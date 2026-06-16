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

One controller manages exactly ONE template (this site's topology), so the
template routes are singular and take no name — the template's `name` field
still exists internally because the k8s resource names are derived from it.

Endpoints
---------
POST   /template           materialise the posted template. Re-POSTing the
                           same name re-materialises idempotently; 409 if a
                           different template is already materialised
GET    /template           the materialised template, in full (404 if none)
PATCH  /template           partial update — merge, re-resolve, re-materialise.
                           Change anything: x, a role's cpu/ram/net/count/tier,
                           edges, or inter-tier latency. Accepts nested JSON or
                           dot-path shorthand, e.g. {"x": 80,
                           "latency.edge.cloud": "150ms"}
DELETE /template           tear down the materialised template
GET    /graph              Grafana Node Graph payload (nodes + measured edges);
                           ?view=pods for per-pod nodes (default: per-role)

Observability (see api.py / API.md)
-----------------------------------
GET    /overview               site-wide snapshot; subsumes the old /health
                               ("ok": true). The k8s readiness probe uses a
                               TCP-socket check instead (see controller.yaml)
GET    /measurements/now       fused k8s + live worker metrics, instantaneous
GET    /measurements/range     CPU/RAM/net aggregated between ?start and ?end
                               (ISO 8601 or unix; default: the last 15 min)
GET    /measurements/periods   the same range split into consecutive
                               ?chunk-sized periods, each aggregated separately

Interactive docs (generated from the code, no hand-maintained spec):
GET    /docs   and   GET /openapi.json
"""

import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field

import api
import chaos
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


# ── Write lock ──────────────────────────────────────────────────────────────
# Endpoints run concurrently in the threadpool, so two writes could
# interleave (POST racing PATCH, double POST). One controller manages one
# template, so a single lock serialises all writers; reads don't take it.
_write_lock = threading.Lock()


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
    docs_url=None,  # served by the dark-mode /docs route below
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS_ORIGIN],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type"],
)


# ── Dark-mode Swagger UI ─────────────────────────────────────────────────────
# Swagger UI has no built-in dark theme, so we serve FastAPI's stock /docs
# page with an inversion filter appended: flip the whole UI to dark, then
# flip syntax-highlighted code blocks back so they keep their own colours.
# hue-rotate(180deg) restores the original hues (greens stay green, etc.).
_DARK_CSS = """<style>
  body { background-color: #0f1217; }
  .swagger-ui { filter: invert(88%) hue-rotate(180deg); }
  .swagger-ui .microlight { filter: invert(100%) hue-rotate(180deg); }
</style>"""


@app.get("/docs", include_in_schema=False)
def swagger_docs() -> HTMLResponse:
    html = get_swagger_ui_html(
        openapi_url="/openapi.json",
        title=f"{app.title} — docs",
    ).body.decode()
    return HTMLResponse(html.replace("</head>", _DARK_CSS + "</head>"))


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
    latency: dict[str, dict[str, str]] | None = Field(
        None,
        description="Optional inter-tier round-trip times, e.g. "
                    '{"edge": {"fog": "30ms", "cloud": "120ms"}}. Each pair '
                    "is rendered into a Chaos Mesh NetworkChaos (half the "
                    "RTT injected per direction), reconciled across pod "
                    "churn, and removed on DELETE. Pairs are symmetric — "
                    "specify each once. Requires Chaos Mesh.",
        examples=[{"edge": {"fog": "30ms", "cloud": "120ms"},
                   "fog": {"cloud": "60ms"}}],
    )


def _stamp(payload: dict) -> dict:
    """Prepend a generation timestamp (controller-local ISO 8601) to a JSON
    response. Every endpoint except /graph returns through this — /graph's
    shape is dictated by the Grafana Node Graph panel, which rejects unknown
    top-level keys."""
    return {"timestamp": api.timestamp(), **payload}


# ── The template: singular CRUD ──────────────────────────────────────────────
def _single_template_name() -> str:
    """Name of the one materialised template. 404 if none. 409 if the cluster
    somehow holds more than one (e.g. legacy state from the old plural API, or
    several labelled ConfigMaps picked up by the watcher) — surfacing that
    beats silently picking one."""
    names = materialiser.list_managed()
    if not names:
        raise HTTPException(404, "no template materialised")
    if len(names) > 1:
        raise HTTPException(
            409, f"multiple templates materialised ({', '.join(names)}); "
                 "tear down the extras first")
    return names[0]


@app.get("/template")
def get_template():
    info = materialiser.get_managed(_single_template_name())
    if info is None or not info.get("template"):
        raise HTTPException(404, "no template materialised")
    return _stamp(info["template"])


@app.post("/template", status_code=201)
def create_template(template: Template):
    body = template.model_dump(by_alias=True)
    # GET /template responses carry an injected `timestamp`; strip it here so
    # re-POSTing a GET round-trips cleanly instead of storing the stamp as
    # template content (extra="allow" would otherwise keep it).
    body.pop("timestamp", None)
    # Cycle detection + edge-endpoint checks beyond Pydantic's shape check.
    materialiser.validate(body)
    with _write_lock:
        # Singleton invariant: re-POSTing the same name re-materialises
        # idempotently; a different name while one exists is a conflict.
        existing = materialiser.list_managed()
        if existing and existing != [body["name"]]:
            raise HTTPException(
                409, f"template {existing[0]!r} is already materialised; "
                     "PATCH it, or DELETE it before posting a new one")
        log.info("Materialising template %s", body.get("name"))
        materialiser.materialise(body)
    return _stamp({
        "name": body["name"],
        "roles": list(body["roles"].keys()),
        "peers": materialiser.compute_peers(body),
    })


@app.patch("/template")
def patch_template(patch: dict):
    # Body is free-form: supports nested JSON and dot-path shorthand, so it is
    # not modelled. patch_template raises ValueError (→400) / RuntimeError (→502).
    patch.pop("timestamp", None)  # injected by GET /template; not content
    with _write_lock:
        name = _single_template_name()
        merged = materialiser.patch_template(name, patch)
    if merged is None:
        raise HTTPException(404, "no template materialised")
    return _stamp({
        "name": name,
        "template": merged,
        "peers": materialiser.compute_peers(merged),
    })


@app.delete("/template")
def delete_template():
    # _single_template_name 404s when nothing is materialised, so "deleted
    # nothing because it was already gone" stays distinguishable from a real
    # teardown; resolving it under the lock keeps check-then-delete atomic.
    with _write_lock:
        name = _single_template_name()
        log.info("Tearing down template %s", name)
        deleted = materialiser.teardown(name)
    return _stamp({"name": name, "deleted": deleted})


# ── Graph ─────────────────────────────────────────────────────────────────────
@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    # Publish the materialised template's CONFIGURED inter-tier RTTs for
    # Prometheus (the dashboard's "what latency is meant to be" panel).
    # Recomputed each scrape from the live template, so a PATCH retune shows
    # up at once; cleared when there's no template or no latency field.
    pairs: dict = {}
    names = materialiser.list_managed()
    if names:
        info = materialiser.get_managed(names[0])
        latency = ((info or {}).get("template") or {}).get("latency")
        try:
            pairs = chaos.validate_latency(latency)
        except ValueError:
            pairs = {}
    chaos.set_configured_rtt(pairs)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/graph")
def get_graph(
    view: str = Query(
        "role",
        description="`role` (default): one node per role, stats summed "
                    "across its pods. `pods`: one node per pod with raw "
                    "pod→pod edges.",
        examples=["pods"],
    ),
):
    """Grafana Node Graph payload (nodes + measured edges) for the
    materialised template. No timestamp — the panel rejects unknown keys."""
    by_pod = view.lower() in ("pod", "pods")
    payload = graph.build_graph(_single_template_name(), by_pod=by_pod)
    if payload is None:
        raise HTTPException(404, "no template materialised")
    return payload


# ── Observability ─────────────────────────────────────────────────────────────
@app.get("/overview")
def overview():
    # "ok" subsumes the old /health endpoint: if this handler runs, the web
    # layer is alive. The rest is the site snapshot from api.overview().
    return _stamp({"ok": True, **api.overview()})


@app.get("/measurements/now")
def measurements_now():
    data = api.template_status(_single_template_name())
    if data is None:
        raise HTTPException(404, "no template materialised")
    return _stamp(data)


@app.get("/measurements/range")
def measurements_range(
    start: datetime | None = Query(
        None,
        description="Interval start — ISO 8601 (`2026-06-10T11:00:00`) or "
                    "unix epoch seconds (`1781089200`). A timestamp without "
                    "an offset is the controller's local time (TZ env, e.g. "
                    "Europe/London); `Z` / explicit offsets are honoured. "
                    "Defaults to `end` − 15 minutes.",
        examples=["2026-06-10T11:00:00"],
    ),
    end: datetime | None = Query(
        None,
        description="Interval end, same formats as `start`. Defaults to now.",
        examples=["2026-06-10T12:00:00"],
    ),
    resources: str | None = Query(
        None,
        description="Comma-separated subset of: `cpu`, `ram`, `net`. "
                    "Defaults to all three.",
        examples=["cpu,net"],
    ),
):
    """CPU / RAM / network aggregated between `start` and `end`.

    Per requested resource: template-wide target/actual averages plus actual
    min/max, summed across pods first, with a per-role breakdown and each
    role's resolved input x averaged over the interval. Omit both timestamps
    for the last 15 minutes."""
    res = ([r.strip() for r in resources.split(",") if r.strip()]
           if resources else None)
    # template_range raises ValueError on an unknown resource, start >= end,
    # or an oversized window (→400).
    data = api.template_range(_single_template_name(), start=start, end=end,
                              resources=res)
    if data is None:
        raise HTTPException(404, "no template materialised")
    return _stamp(data)


@app.get("/measurements/periods")
def measurements_periods(
    start: datetime | None = Query(
        None,
        description="Range start — same formats and defaults as "
                    "`/measurements/range`.",
        examples=["2026-06-10T11:00:00"],
    ),
    end: datetime | None = Query(
        None,
        description="Range end, same formats as `start`. Defaults to now.",
        examples=["2026-06-10T12:00:00"],
    ),
    chunk: str = Query(
        ...,
        description="Length of each chunk — e.g. `90s`, `10m`, `1h` (min "
                    "`30s`, the scrape interval). The range is split into as "
                    "many FULL chunks as fit; a trailing remainder is "
                    "dropped and reported as `remainder_s`.",
        examples=["10m"],
    ),
    resources: str | None = Query(
        None,
        description="Comma-separated subset of: `cpu`, `ram`, `net`. "
                    "Defaults to all three.",
        examples=["cpu,net"],
    ),
):
    """The range split into consecutive `chunk`-sized periods, each
    aggregated separately.

    e.g. `?start=11:00&end=12:00&chunk=10m` → 6 ten-minute periods, each with
    the same totals/roles/x blocks as `/measurements/range`."""
    res = ([r.strip() for r in resources.split(",") if r.strip()]
           if resources else None)
    # template_periods raises ValueError on bad chunking/resources/interval (→400).
    data = api.template_periods(_single_template_name(), start=start, end=end,
                                chunk=chunk, resources=res)
    if data is None:
        raise HTTPException(404, "no template materialised")
    return _stamp(data)
