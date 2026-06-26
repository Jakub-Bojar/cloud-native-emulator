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
                           Change anything: x, a role's cpu/ram/net/count/tier/node,
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
GET    /measurements/periods   the last ?count chunks of ?chunk each, ending
                               now, each aggregated separately

Interactive docs (generated from the code, no hand-maintained spec):
GET    /docs   and   GET /openapi.json
"""

import logging
import os
import shutil
import sys
import threading
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field

import api
import graph
import linkspec
import materialiser
import runner

# Provisioning is merged into the controller: when it runs on the host it can
# drive Multipass itself, so POST /template provisions the sites' VMs before
# materialising the apps. provision.py lives one dir up under provision/.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "provision"))
import provision  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [controller] %(message)s",
)
log = logging.getLogger(__name__)

# Allowed CORS origin for browser clients (Swagger UI, Grafana, etc.). "*"
# is fine for a dev/research tool on a trusted network; set CORS_ALLOW_ORIGIN
# to a specific origin to lock it down.
CORS_ORIGIN = os.environ.get("CORS_ALLOW_ORIGIN", "*")

# When the controller runs on the host (off-cluster), POST /template provisions
# the sites' VMs via Multipass before materialising the apps.
CONTROL_PLANE_VM = os.environ.get("CONTROL_PLANE_VM", "microk8s-vm")
NODE_IMAGE = os.environ.get("NODE_IMAGE", "22.04")


# ── Write lock ──────────────────────────────────────────────────────────────
# Endpoints run concurrently in the threadpool, so two writes could
# interleave (POST racing PATCH, double POST). One controller manages one
# template, so a single lock serialises all writers; reads don't take it.
_write_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
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


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    # There's no content at the root; send browsers straight to the API docs so
    # hitting the bare host:port doesn't 404 with {"detail":"Not Found"}.
    return RedirectResponse(url="/docs")


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
    tier: str | None = Field(
        None, description="Pin pods to nodes labelled tier=<value> "
                          "(e.g. edge/fog/cloud). Absent = schedule anywhere.")
    node: str | None = Field(
        None, description="Pin pods to one specific node by name (its "
                          "kubernetes.io/hostname). Must be a node of `tier` "
                          "if both set. Empty string unpins. PATCH this to "
                          "move a deployment between nodes.")


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
        description="Optional inter-tier ONE-WAY latency, e.g. "
                    '{"edge": {"fog": "30ms", "cloud": "120ms"}} means a '
                    "packet takes 30ms edge→fog (and 30ms back — symmetric). "
                    "Applied as link-level tc shaping on the tier nodes "
                    "(injects the full value on each direction), so pods "
                    "inherit it and scaling never disturbs it; reconciled each "
                    "materialise and removed on DELETE. Pairs are symmetric — "
                    "specify each once.",
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
    somehow holds more than one (e.g. legacy state from the old plural API) —
    surfacing that beats silently picking one."""
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
def create_template(payload: dict):
    """Materialise a posted template.

    Two schemas are accepted, distinguished by the `schema_version` field:

    - Present → the new topology schema (`sites`, `network_links`, `apps`,
      `app_edges`, `k8s_app_description`, `runtime_scenarios`). The controller
      validates it,
      confirms the tier nodes the apps need already exist (it does NOT
      provision them — that's the host-side `provision.py`), then materialises
      each app's cpu/ram/net load from its `apps[].load` block (idle when it has
      none). The signal `x` comes from the first `runtime_scenarios` phase (else
      the doc baseline, default 0) and propagates down the role graph. A phase's
      `x` is either a number (system-wide — every DAG starts together) or an
      object keyed by DAG source app_id (per-DAG — each pipeline starts at its
      own input). Later phases are time windows the scenario runner steps
      through. Pins each app to its tier via the `topology.tier_id` node label.

    - Absent → the legacy role/edge schema (`name`, `x`, `roles`, `edges`,
      `latency`), materialised exactly as before.

    Re-POSTing the same name re-materialises idempotently; 409 if a *different*
    template is already materialised.
    """
    # GET responses carry an injected `timestamp`; strip it so re-POSTing a GET
    # round-trips cleanly instead of storing the stamp as template content.
    payload.pop("timestamp", None)

    # ── New topology schema ──────────────────────────────────────────────
    if payload.get("schema_version"):
        materialiser.validate_topology(payload)
        with _write_lock:
            name = materialiser.topology_name(payload)
            existing = materialiser.list_managed()
            if existing and existing != [name]:
                raise HTTPException(
                    409, f"template {existing[0]!r} is already materialised; "
                         "DELETE it before posting a new one")
            # 1. Provision the sites' VMs — only when running on a host with
            #    Multipass. Off a host, skip and rely on pre-provisioned nodes;
            #    materialise_topology's check_sites validates they exist.
            provisioned = None
            if shutil.which("multipass"):
                log.info("Provisioning sites for topology %s", name)
                provisioned = provision.up(payload,
                                           control_plane=CONTROL_PLANE_VM,
                                           image=NODE_IMAGE)
            # 2. Materialise the apps at baseline (check_sites runs first).
            log.info("Materialising topology %s", name)
            report = materialiser.materialise_topology(payload)
        # 3. Start the scenario runner if the topology declares phases.
        runner.start(payload, name)
        if provisioned is not None:
            report = {"provision": provisioned, **report}
        return _stamp(report)

    # ── Legacy role/edge schema ──────────────────────────────────────────
    body = Template(**payload).model_dump(by_alias=True)
    body.pop("timestamp", None)
    # Cycle detection + edge-endpoint checks beyond Pydantic's shape check.
    materialiser.validate_template(body)
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
    #
    # Symmetric with POST: tears down the apps + link shaping AND, for a
    # topology, strips the infrastructure too — drains/deletes the nodes the
    # provisioner created for it (found by marker label) and deletes their VMs.
    with _write_lock:
        name = _single_template_name()
        topo = materialiser.get_topology_doc(name)
        node_names = materialiser.topology_node_names(topo) if topo else []
        runner.stop()
        log.info("Tearing down template %s", name)
        deleted = materialiser.teardown(name)
        deprovisioned = None
        if node_names and shutil.which("multipass"):
            log.info("Stripping %d node(s) for %s: %s",
                     len(node_names), name, ", ".join(node_names))
            deprovisioned = provision.down_nodes(node_names,
                                                 control_plane=CONTROL_PLANE_VM)
    out = {"name": name, "deleted": deleted}
    if deprovisioned is not None:
        out["deprovisioned"] = deprovisioned
    return _stamp(out)


# ── Graph ─────────────────────────────────────────────────────────────────────
@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    # Publish the materialised template's CONFIGURED inter-tier latency AND
    # bandwidth for Prometheus (the dashboard's "what it's meant to be" panels).
    # Recomputed each scrape from the live template, so a PATCH retune shows up
    # at once; cleared when there's no template or no latency/bandwidth field.
    rtt_pairs: dict = {}
    bw_pairs: dict = {}
    names = materialiser.list_managed()
    if names:
        template = (materialiser.get_managed(names[0]) or {}).get("template") or {}
        try:
            rtt_pairs = linkspec.validate_latency(template.get("latency"))
        except ValueError:
            rtt_pairs = {}
        try:
            bw_pairs = linkspec.validate_bandwidth(template.get("bandwidth"))
        except ValueError:
            bw_pairs = {}
    linkspec.set_configured_rtt(rtt_pairs)
    linkspec.set_configured_bw(bw_pairs)
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
    count: int = Query(
        ...,
        ge=1,
        description="How many chunks to return, counting back from now. e.g. "
                    "`?count=4&chunk=11m` → the last 44 minutes as 4 "
                    "eleven-minute periods.",
        examples=[4],
    ),
    chunk: str = Query(
        ...,
        description="Length of each chunk — e.g. `90s`, `10m`, `1h` (min "
                    "`30s`, the scrape interval).",
        examples=["10m"],
    ),
    resources: str | None = Query(
        None,
        description="Comma-separated subset of: `cpu`, `ram`, `net`. "
                    "Defaults to all three.",
        examples=["cpu,net"],
    ),
):
    """The last `count` chunks of `chunk` each, ending now, aggregated
    separately.

    e.g. `?count=4&chunk=11m` → the last 44 minutes as 4 eleven-minute
    periods. Each period carries the same totals/roles/x blocks as
    `/measurements/range`."""
    res = ([r.strip() for r in resources.split(",") if r.strip()]
           if resources else None)
    # template_periods raises ValueError on a bad chunk/count/resources (→400).
    data = api.template_periods(_single_template_name(),
                                chunk=chunk, count=count, resources=res)
    if data is None:
        raise HTTPException(404, "no template materialised")
    return _stamp(data)
