"""
Controller pod — HTTP surface (FastAPI).

Front door for the emulation. Materialises templated multi-worker
topologies: POST /templates validates the JSON, then the materialiser
creates one Deployment + ConfigMap + Service per role declared in the
template and writes each role's load formulas + peer list into its
ConfigMap. DELETE /templates/<name> tears the whole topology down.

This module is only the web layer: routing, request-shape validation,
error mapping, and concurrency control. All behaviour lives in the
materialiser / graph / api / prom modules, unchanged.

Why FastAPI (and not the stdlib HTTPServer): sync `def` endpoints run in
an anyio threadpool, so a slow blocking call (materialise waiting on
Endpoints, per-pod scrapes, Prometheus queries) no longer blocks the
liveness probe on /health. The single-threaded HTTPServer used to
serialise everything, so a long materialise could starve /health and get
the pod killed mid-operation.

Endpoints
---------
POST   /templates          materialise a posted template
GET    /templates          list names of currently materialised templates
GET    /templates/<name>   inspect a materialised template (peers, replicas)
PATCH  /templates/<name>   partial update — merge, re-resolve, re-materialise.
                           Change anything: x, a role's cpu/ram/net/count/tier,
                           or edges. Accepts nested JSON or dot-path shorthand,
                           e.g. {"x": 80, "roles.ingest.cpu.a": 6}
DELETE /templates/<name>   tear down a materialised template
GET    /graph/<name>       Grafana Node Graph payload (nodes + measured edges);
                           ?view=pods for per-pod nodes (default: per-role)
GET    /health             liveness for k8s

Unified site API (see api.py / API.md)
--------------------------------------
GET    /api/v1/overview                              site-wide snapshot
GET    /api/v1/templates/<name>/status               fused k8s + live metrics
GET    /api/v1/templates/<name>/summary              CPU/RAM/net averaged over a window

Interactive docs (generated from the code, no hand-maintained spec):
GET    /docs   and   GET /openapi.json
"""

import logging
import os
import threading
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

import api
import graph
import materialiser
import watcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [controller] %(message)s",
)
log = logging.getLogger(__name__)

# Allowed CORS origin for browser clients (Swagger UI, Grafana, etc.). "*"
# is fine for a dev/research tool on a trusted network; set CORS_ALLOW_ORIGIN
# to a specific origin to lock it down.
CORS_ORIGIN = os.environ.get("CORS_ALLOW_ORIGIN", "*")


# ── Per-template lock ───────────────────────────────────────────────────────
# Endpoints run concurrently in the threadpool, so two writes to the SAME
# template could interleave (two POSTs, a PATCH racing the watcher's reconcile
# in watcher.py). Serialise writes per template name; different templates and
# all reads still run fully in parallel. Pure-read endpoints don't take it.
_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_locks_guard = threading.Lock()


def _template_lock(name: str) -> threading.Lock:
    with _locks_guard:
        return _locks[name]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Declarative ingestion: poll labelled ConfigMaps and reconcile them
    # via the same materialiser used by POST /templates. Same daemon thread
    # the old controller started in main(). Keep the server at ONE process
    # (uvicorn --workers 1) so only one reconciler runs.
    watcher.start()
    log.info("Controller started")
    yield


app = FastAPI(
    title="Cloud-Native Emulator Controller",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS_ORIGIN],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type"],
)


# ── Error mapping ───────────────────────────────────────────────────────────
# Replaces the per-handler try/except ladders. The materialiser raises
# ValueError for client-side problems (bad field, cycle, unknown resource) and
# RuntimeError when a k8s API call fails.
@app.exception_handler(ValueError)
async def _value_error(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": str(exc)})


@app.exception_handler(RuntimeError)
async def _runtime_error(request: Request, exc: RuntimeError):
    log.exception("k8s API failure on %s", request.url.path)
    return JSONResponse(status_code=502, content={"error": str(exc)})


# ── Request models ──────────────────────────────────────────────────────────
# Pydantic validates the *shape* and auto-documents it at /docs. The richer
# graph-level checks (cycle detection, edge endpoints exist) stay in
# materialiser.validate(), called explicitly below. extra="allow" preserves
# any unknown fields so the stored template annotation round-trips intact,
# matching the permissive behaviour of the old hand-rolled validator.
class Axis(BaseModel):
    model_config = ConfigDict(extra="allow")
    a: float
    b: float


class Role(BaseModel):
    model_config = ConfigDict(extra="allow")
    count: int = Field(ge=1)
    cpu: Axis
    ram: Axis
    net: Axis
    tier: str | None = None


class Edge(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    from_: str = Field(alias="from")
    to: str


class Template(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    x: float = 0
    roles: dict[str, Role]
    edges: list[Edge] = []


# ── Templates: CRUD ──────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/templates")
def list_templates():
    return {"templates": materialiser.list_managed()}


@app.get("/templates/{name}")
def get_template(name: str):
    info = materialiser.get_managed(name)
    if info is None:
        raise HTTPException(404, f"no template named {name!r}")
    return info["template"]


@app.post("/templates", status_code=201)
def create_template(template: Template):
    body = template.model_dump(by_alias=True)
    # Cycle detection + edge-endpoint checks beyond Pydantic's shape check.
    materialiser.validate(body)
    log.info("Materialising template %s", body.get("name"))
    with _template_lock(body["name"]):
        materialiser.materialise(body)
    return {
        "name": body["name"],
        "roles": list(body["roles"].keys()),
        "peers": materialiser.compute_peers(body),
    }


@app.patch("/templates/{name}")
def patch_template(name: str, patch: dict):
    # Body is free-form: supports nested JSON and dot-path shorthand, so it is
    # not modelled. patch_template raises ValueError (→400) / RuntimeError (→502).
    with _template_lock(name):
        merged = materialiser.patch_template(name, patch)
    if merged is None:
        raise HTTPException(404, f"no template named {name!r}")
    return {
        "name": name,
        "template": merged,
        "peers": materialiser.compute_peers(merged),
    }


@app.delete("/templates/{name}")
def delete_template(name: str):
    log.info("Tearing down template %s", name)
    with _template_lock(name):
        deleted = materialiser.teardown(name)
    return {"name": name, "deleted": deleted}


# ── Graph ─────────────────────────────────────────────────────────────────────
@app.get("/graph/{name}")
def get_graph(name: str, view: str = "role"):
    by_pod = view.lower() in ("pod", "pods")
    payload = graph.build_graph(name, by_pod=by_pod)
    if payload is None:
        raise HTTPException(404, f"no template named {name!r}")
    return payload


# ── Unified /api/v1 site API ───────────────────────────────────────────────────
@app.get("/api/v1/overview")
def overview():
    return api.overview()


@app.get("/api/v1/templates/{name}/status")
def template_status(name: str):
    data = api.template_status(name)
    if data is None:
        raise HTTPException(404, f"no template named {name!r}")
    return data


@app.get("/api/v1/templates/{name}/summary")
def template_summary(
    name: str,
    range: str = "15m",
    resources: str | None = None,
    by_role: bool = False,
    include_x: bool = False,
):
    res = ([r.strip() for r in resources.split(",") if r.strip()]
           if resources else None)
    # template_summary raises ValueError on an unknown resource (→400).
    data = api.template_summary(name, range, resources=res,
                                by_role=by_role, include_x=include_x)
    if data is None:
        raise HTTPException(404, f"no template named {name!r}")
    return data
