# API Reference

HTTP endpoints exposed by the controller and worker pods.

> A machine-readable OpenAPI 3.0 spec of the controller API lives in
> [`openapi.yaml`](openapi.yaml). Load it into Swagger UI / Redoc, or
> generate a typed client from it (see the header of that file). This
> document is the human-readable companion with worked examples.

The controller is reachable via the NodePort Service in
`manifests/controller.yaml` (default `30081`). Workers are not normally
exposed externally — their endpoints are documented here for
diagnostics, Prometheus scraping, and direct `kubectl port-forward`.

Examples use `192.168.2.2:30081` for the controller (replace with your
MicroK8s host IP) and `localhost:8080` for the worker (after a
`kubectl port-forward`). Placeholders like `<name>` and `<role>` refer
to whatever template / role name you're working with.

The controller serves three independent API surfaces:

- **Templated mode** (`/templates*`) — the primary write API.
  Materialises whole topologies and resolves x propagation through the
  role graph. This is what you'll use to create/patch/delete topologies.
- **Unified site API** (`/api/v1/*`) — the read + scale API. One clean
  JSON surface that fuses Kubernetes state, live worker gauges, and
  Prometheus-backed windowed averages, plus role scaling. Every response
  is tagged with a `site` block for fleet-wide federation. Use this to
  observe and to add/remove pods.
- **Health** (`/health`) — controller liveness.

---

# Templated mode (primary API)

## `POST /templates`

Materialise a new topology. Validates the template (including cycle
detection on the role graph), computes the resolved `x` for each role
via topological propagation, and creates one Deployment + ConfigMap +
Service per role.

`materialise()` is idempotent: POSTing the same `name` again updates
existing resources via PATCH. You can use this as a "reapply" if you
ever lose track of state. For surgical updates that only change a few
fields, prefer `PATCH /templates/<name>` (below).

**Body schema:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | k8s name (lowercase + digits + `-`). Becomes the suffix of every resource: `wt-<name>-<role>` |
| `x` | number | yes | The signal that drives the topology. Sources use it directly; downstream roles use the resolved cascade |
| `roles` | object | yes | Map of `role_name → role_spec`. See below |
| `edges` | array | optional | List of `{"from": role, "to": role}` |

**Role spec:**

```json
{
  "count": 2,
  "cpu": {"a": 0, "b": 100},
  "ram": {"a": 0, "b": 128},
  "net": {"a": 0.2, "b": 0}
}
```

Targets are computed per pod as `value = a * x_role + b`, where
`x_role` is the resolved x for that role (see "x propagation" below).

**Example:**

```bash
curl -X POST http://192.168.2.2:30081/templates \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "<name>",
    "x": 10,
    "roles": {
      "<source-role>": {
        "count": 2,
        "cpu": {"a": 0, "b": 50},
        "ram": {"a": 0, "b": 64},
        "net": {"a": 0.2, "b": 0}
      },
      "<sink-role>": {
        "count": 1,
        "cpu": {"a": 2, "b": 20},
        "ram": {"a": 5, "b": 256},
        "net": {"a": 0, "b": 0}
      }
    },
    "edges": [
      {"from": "<source-role>", "to": "<sink-role>"}
    ]
  }'
```

**Response (201):**
```json
{
  "name": "<name>",
  "roles": ["<source-role>", "<sink-role>"],
  "peers": {
    "<source-role>": ["wt-<name>-<sink-role>"],
    "<sink-role>": []
  }
}
```

**Status codes:**

| Code | Meaning |
|------|---------|
| 201 | Materialised |
| 400 | Invalid JSON, validation failure, or cycle in the role graph |
| 502 | A k8s API call failed mid-materialisation. Partial resources may exist — re-POST or DELETE to clean up |
| 500 | Unexpected error |

## `GET /templates`

List the names of all currently materialised templates.

```bash
curl http://192.168.2.2:30081/templates
# → {"templates":["<name-1>","<name-2>", ...]}
```

## `GET /templates/<name>`

Inspect a materialised template. The state is reconstructed from
ConfigMap annotations and live Deployment replica counts — there's no
in-memory state on the controller.

```bash
curl http://192.168.2.2:30081/templates/<name> | python3 -m json.tool
```

Response (200):

```json
{
  "name": "<name>",
  "source": "http",
  "template": { /* original POST body */ },
  "peers": { /* role → list of Service names */ },
  "replicas": { "<role>": <count>, ... },
  "configmaps": ["wt-<name>-<role>-config", ...]
}
```

`source` is either `"http"` (created via POST) or `"watch"` (created
by the declarative ConfigMap reconciler — see below).

`404` if no template by that name exists.

## `PATCH /templates/<name>`

Apply a partial update to a running template. Deep-merges the patch
body into the existing template, re-runs validation (including cycle
detection), and re-materialises. Workers pick up the new ConfigMap
within ~60s (kubelet sync) — **no pod restart**.

**Merge semantics:**

- Dicts merge recursively. `{"roles": {"<role>": {"net": {"a": 0.3}}}}`
  changes only that role's `net.a`; `net.b` and every other field stay
  as they were.
- Scalars and lists in the patch **replace** the existing value. To
  change one edge, send the whole new `edges` list.
- The `name` field in the body is ignored — the URL is the source of truth.
- The `source` annotation (`http` vs `watch`) is preserved.

**Common patches:**

```bash
# Change x — the whole topology recascades
curl -X PATCH http://192.168.2.2:30081/templates/<name> \
  -H 'Content-Type: application/json' \
  -d '{"x": 20}'

# Bump one role's network — downstream x values re-resolve
curl -X PATCH http://192.168.2.2:30081/templates/<name> \
  -H 'Content-Type: application/json' \
  -d '{"roles":{"<role>":{"net":{"a":0.30}}}}'

# Tune one role's RAM (no cascade; only that role changes)
curl -X PATCH http://192.168.2.2:30081/templates/<name> \
  -H 'Content-Type: application/json' \
  -d '{"roles":{"<role>":{"ram":{"b":512}}}}'

# Scale a role
curl -X PATCH http://192.168.2.2:30081/templates/<name> \
  -H 'Content-Type: application/json' \
  -d '{"roles":{"<role>":{"count":4}}}'

# Replace the edge list
curl -X PATCH http://192.168.2.2:30081/templates/<name> \
  -H 'Content-Type: application/json' \
  -d '{"edges":[{"from":"<role-a>","to":"<role-b>"}]}'
```

**Response (200):**

```json
{
  "name": "<name>",
  "template": { /* full merged template */ },
  "peers": { /* role → list of Service names */ }
}
```

**Status codes:**

| Code | Meaning |
|------|---------|
| 200 | Patched |
| 400 | Invalid JSON, validation failure on the merged result, or cycle |
| 404 | No template by that name |
| 502 | k8s API failure during re-materialisation |
| 500 | Unexpected error |

**Caveat for watch-managed templates:** if `source == "watch"`, the
controller's labelled-ConfigMap watcher (10s poll) will overwrite your
PATCH with whatever the labelled CM still says. To make a sticky
change to a watch-managed template, edit the labelled CM instead
(`kubectl edit configmap <name>`) — the watcher re-materialises from
that.

## `DELETE /templates/<name>`

Tear down every resource labelled with this template name. Deployments
are deleted first (so pods stop using the ConfigMap), then Services,
then ConfigMaps.

```bash
curl -X DELETE http://192.168.2.2:30081/templates/<name>
# → {"name":"<name>","deleted":<count>}
```

`deleted` is the number of Kubernetes objects removed (rough count).
Returns 404 if no template with that name is materialised; during the
teardown itself, a kind that is already absent is treated as
already-gone rather than an error.

---

# Unified site API (`/api/v1`)

The `/api/v1` surface is the **single place to query everything this
controller knows and to scale roles up/down**, in clean JSON. It fuses
three data sources behind one envelope:

- **Kubernetes live state** — Deployments (desired replicas) and Pods
  (phase, readiness, restarts, node, age) via the k8s API.
- **Instantaneous worker gauges** — scraped straight from each pod's
  `/metrics` (no Prometheus dependency).
- **Windowed averages** — via the Prometheus HTTP API (`PROM_URL`),
  for CPU/RAM/net averaged over a time window (the `/summary` endpoint).

### Site identity & federation

Every `/api/v1` response carries a `site` block:

```json
"site": {"id": "<SITE_ID>", "tier": "<SITE_TIER>"}
```

One controller runs per VM, and each VM plays a part in a larger cloud
system (edge / fog / cloud). `SITE_ID` / `SITE_TIER` are set per
controller via env (`manifests/controller.yaml`; default
`"local"` / `"unknown"`). The block lets a future federation gateway
fan out to many controllers and merge responses by site with no schema
change here.

## `GET /api/v1/overview`

Site-wide snapshot: every materialised template with per-role desired
vs ready replica counts and a pod-health rollup. The "what is running
here" endpoint.

```bash
curl http://192.168.2.2:30081/api/v1/overview | python3 -m json.tool
```

**Response (200):**

```json
{
  "site": {"id": "local", "tier": "unknown"},
  "namespace": "default",
  "templates": [
    {
      "name": "<name>",
      "source": "http",
      "roles": {
        "<role>": {"desired": 2, "ready": 2}
      },
      "pods": {"total": 3, "ready": 3}
    }
  ],
  "prometheus": {"available": true, "url": "http://…:9090"}
}
```

## `GET /api/v1/templates/<name>/status`

Rich per-template status. For each role: desired/ready replicas, the
resolved `x`, role-level target and actual sums (CPU/RAM/NET), and a
per-pod list fusing k8s state with that pod's current worker gauges.
Also returns measured role→role edge traffic.

Pods that exist in k8s but aren't in Endpoints yet (starting up, not
Ready) are surfaced with an empty `metrics: {}` so an in-progress
scale-up is visible rather than silently missing.

```bash
curl http://192.168.2.2:30081/api/v1/templates/<name>/status | python3 -m json.tool
```

**Response (200):**

```json
{
  "site": {"id": "local", "tier": "unknown"},
  "name": "<name>",
  "source": "http",
  "roles": {
    "<role>": {
      "desired": 2,
      "ready": 2,
      "x": 10.0,
      "targets": {"cpu_millicores": 100.0, "ram_mb": 128.0, "net_mbps": 4.0},
      "actuals": {"cpu_millicores": 98.2, "ram_mb": 130.1, "net_mbps": 3.9},
      "pods": [
        {
          "name": "wt-<name>-<role>-abc123",
          "ip": "10.1.0.42",
          "node": "node-1",
          "phase": "Running",
          "ready": true,
          "restarts": 0,
          "age_seconds": 312,
          "metrics": {
            "x": 10.0,
            "target_cpu_millicores": 50.0, "actual_cpu_millicores": 49.1,
            "target_ram_mb": 64.0,         "actual_ram_mb": 65.0,
            "target_net_mbps": 2.0,        "actual_net_mbps": 1.95
          }
        }
      ]
    }
  },
  "edges": [{"from": "<role-a>", "to": "<role-b>", "mbps": 3.912}],
  "prometheus": {"available": null}
}
```

`prometheus.available` is `null` here — this endpoint reads live
scrapes, not Prometheus. Use `/summary` (below) for windowed averages.

`404` if no template by that name exists.

## `GET /api/v1/templates/<name>/summary`

A compact, human-readable digest of **CPU, RAM, and network** for a
template, **averaged over a window** — the "just tell me the numbers"
endpoint. Rather than returning full per-pod time series, it returns a
handful of scalars: the template-wide target and actual *summed across
pods* and then reduced over the window.

**Query params:**

| Param | Default | Notes |
|-------|---------|-------|
| `range` | `15m` | Window length. Bare Prometheus duration (`s`/`m`/`h`/`d`); anything else falls back to 15m |
| `resources` | `cpu,ram,net` | Comma-separated subset to include. Unknown values → 400 |
| `by_role` | `false` | `true` adds a per-role breakdown (avg only) |
| `include_x` | `false` | `true` adds an `x` block: each role → its resolved input `x` |

For each resource the totals carry `target_avg`, `actual_avg`,
`actual_min`, and `actual_max` (the per-role breakdown carries the two
averages only). Values are summed across all of the template's pods,
then averaged / min'd / max'd over the window via a Prometheus
subquery, and rounded to 3 dp. The `x` block (when requested) is
averaged — not summed — across each role's pods, since every pod of a
role shares the same resolved `x`.

```bash
# Whole-template digest over the last hour
curl -s "http://192.168.2.2:30081/api/v1/templates/<name>/summary?range=1h" \
  | python3 -m json.tool

# Just CPU, broken down by role, over 30 minutes
curl -s "http://192.168.2.2:30081/api/v1/templates/<name>/summary?range=30m&resources=cpu&by_role=true" \
  | python3 -m json.tool

# Include the resolved x per role
curl -s "http://192.168.2.2:30081/api/v1/templates/<name>/summary?range=1h&include_x=true" \
  | python3 -m json.tool
```

**Response (200):**

```json
{
  "site": {"id": "local", "tier": "unknown"},
  "name": "<name>",
  "range": "1h",
  "totals": {
    "cpu_millicores": {"target_avg": 980.0, "actual_avg": 951.2, "actual_min": 902.0, "actual_max": 1010.5},
    "ram_mb":         {"target_avg": 707.8, "actual_avg": 712.4, "actual_min": 690.1, "actual_max": 730.9},
    "net_mbps":       {"target_avg": 15.6,  "actual_avg": 14.9,  "actual_min": 0.0,   "actual_max": 16.2}
  },
  "roles": {
    "api": {
      "cpu_millicores": {"target_avg": 484.8, "actual_avg": 470.1},
      "ram_mb":         {"target_avg": 231.6, "actual_avg": 233.0},
      "net_mbps":       {"target_avg": 9.12,  "actual_avg": 8.7}
    }
  },
  "x": {"gateway": 12.0, "auth": 6.8, "api": 6.8, "queue": 9.12, "cache": 9.12, "db": 6.92},
  "prometheus": {"available": true, "url": "http://…:9090"}
}
```

`roles` is present only when `by_role=true`; `x` is present only when
`include_x=true`.

**Graceful degradation:** if Prometheus is unreachable, returns 200 with
`prometheus.available: false`, `totals: {}` (and `roles: {}` when
requested). For a live, no-Prometheus alternative use `…/status`, whose
per-role `targets`/`actuals` are the current (instantaneous) sums.

`404` if no template by that name exists.

## `POST /api/v1/templates/<name>/roles/<role>/scale`

Add or remove pods for one role. Implemented over
`PATCH /templates/<name>` (`materialiser.patch_template`), so scaling
**re-resolves x through the role graph and re-runs peer/port-offset
assignment** — freshly added pods are fully wired into the topology,
not just bare Deployment replicas.

**Body:**

| Field | Type | Notes |
|-------|------|-------|
| `replicas` | integer | Required. Absolute target count |

Target must be `>= 1` and `<= MAX_REPLICAS_PER_ROLE` (default 20;
overridable via env on the controller).

```bash
# Set <role> to 4 pods
curl -X POST http://192.168.2.2:30081/api/v1/templates/<name>/roles/<role>/scale \
  -H 'Content-Type: application/json' -d '{"replicas": 4}'
```

**Response (200):**

```json
{
  "site": {"id": "local", "tier": "unknown"},
  "name": "<name>",
  "role": "<role>",
  "previous": 2,
  "replicas": 4,
  "peers": { "<role>": ["wt-<name>-<peer>"], … }
}
```

**Status codes:**

| Code | Meaning |
|------|---------|
| 200 | Scaled |
| 400 | Invalid JSON, unknown role, missing/non-integer `replicas`, or out-of-range target |
| 404 | No template by that name |
| 502 | k8s API failure during re-materialisation |
| 500 | Unexpected error |

> Same watch-managed caveat as `PATCH`: if `source == "watch"`, the
> ConfigMap watcher will overwrite a scale within ~10s. Edit the
> labelled ConfigMap's `count` instead for a sticky change. Check with
> `curl -s http://192.168.2.2:30081/templates/<name> | jq .source`.

**Verifying a scale took effect:**

```bash
curl -s http://192.168.2.2:30081/api/v1/templates/<name>/status \
  | jq '.roles.<role> | {desired, ready, x, pods: (.pods | length)}'
```

Freshly added pods appear with `"metrics": {}` until they are Ready and
land in Endpoints (Phase-2 wiring) — an in-progress scale-up is visible
rather than silently missing.

Scaling a **sender** role re-resolves `x` for everything downstream
(it's a full re-materialise, not a Deployment replica edit). Watch every
role's resolved `x` shift after scaling a source:

```bash
curl -s http://192.168.2.2:30081/api/v1/templates/<name>/status \
  | jq '.roles | map_values(.x)'
```

**Validation — these all return `400`** (append `-w '\n%{http_code}\n'`
to see the status):

```bash
S=http://192.168.2.2:30081/api/v1/templates/<name>/roles/<role>/scale
curl -s -X POST $S -H 'Content-Type: application/json' -d '{}'                       # missing replicas
curl -s -X POST $S -H 'Content-Type: application/json' -d '{"replicas":0}'           # below 1
curl -s -X POST $S -H 'Content-Type: application/json' -d '{"replicas":21}'          # above MAX_REPLICAS_PER_ROLE
curl -s -X POST $S -H 'Content-Type: application/json' -d '{"replicas":2.5}'         # non-integer
curl -s -X POST $S -H 'Content-Type: application/json' -d '{"replicas":true}'        # boolean
```

**Unknown role (`400`) vs unknown template (`404`):**

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  http://192.168.2.2:30081/api/v1/templates/<name>/roles/nope/scale \
  -H 'Content-Type: application/json' -d '{"replicas":2}'      # 400
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  http://192.168.2.2:30081/api/v1/templates/ghost/roles/<role>/scale \
  -H 'Content-Type: application/json' -d '{"replicas":2}'      # 404
```

---

## x propagation — the model behind templated mode

The template's `x` is a *signal* that flows through the role graph,
not a global constant shared by every role.

```
template.x   ──▶  source roles (no inbound edges)
                  │
                  │ NET formula:  per_pod_egress = max(0, net.a * x + net.b)
                  │ role total:   count * per_pod_egress
                  ▼
                  downstream role's x = Σ (upstream role totals)
                  │ same formula again with this role's net coefficients
                  ▼
                  …
```

For each role, the controller computes `x_role` via a topological pass
(Kahn's algorithm). A source role's x is `template.x`; a downstream
role's x is the sum of upstream role-total egress. All three of CPU,
RAM, and NET formulas evaluate at this resolved x.

Self-edges (intra-role mesh, `from == to`) are legal traffic-wise but
ignored for x-resolution — a role can't feed its own x without
circularity.

Cycles in the role graph are rejected at validate time (400 with
`role graph has a cycle involving: …`).

The resolved x per role is logged at materialise time:

```
[controller] Template <name>: resolved x per role = {'<role-a>': 10.0, '<role-b>': 8.0, ...}
```

It's also visible per pod in Grafana as the `worker_input_x` gauge.

---

## Declarative ingestion (no HTTP needed)

The controller also reconciles ConfigMaps labelled with
`emulator.local/template: "true"`. A polling watcher
(`controller/watcher.py`, 10s interval) calls into the same
`materialise()` / `teardown()` codepaths used by `POST` and `DELETE`.

```yaml
# <name>-template.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: <name>
  labels:
    emulator.local/template: "true"
data:
  template.json: |
    {
      "x": 10,
      "roles": { ... },
      "edges": [ ... ]
    }
```

```bash
microk8s kubectl apply -f <name>-template.yaml   # materialises within ~15s
microk8s kubectl edit configmap <name>           # live edit → re-materialise
microk8s kubectl delete configmap <name>         # tears the whole topology down
```

The template's `name` comes from `metadata.name`; any `name` inside
`template.json` is overwritten.

HTTP and watch sources coexist: templates carry a `source` annotation
and the watcher only tears down templates it created.

---

# Health

## `GET /health`

Liveness probe. Always returns 200 as long as the HTTP server is up.

```bash
curl http://192.168.2.2:30081/health
# → {"ok":true}
```

---

# Worker

Workers expose three endpoints on port `8080`. They're not exposed via
NodePort by default. Reach them with:

```bash
microk8s kubectl port-forward <pod-name> 8080:8080
```

## `GET /health`

```bash
curl http://localhost:8080/health
# → {"ok":true}
```

## `GET /status`

Returns the worker's full `STATE` dict — what it's currently doing.

```bash
curl http://localhost:8080/status | python3 -m json.tool
```

```json
{
  "running": true,
  "x": <resolved-x-for-this-role>,
  "cpu_millicores": <cpu.a * x + cpu.b>,
  "ram_mb": <ram.a * x + ram.b>,
  "net_mbps": <net.a * x + net.b>,
  "formulas": {
    "cpu": {"a": <a>, "b": <b>},
    "ram": {"a": <a>, "b": <b>},
    "net": {"a": <a>, "b": <b>}
  },
  "peers": ["<peer-ip-1>", "<peer-ip-2>", ...]
}
```

For a templated worker, `x` here is the *resolved* x for this pod's
role, and `peers` is the list of concrete pod IPs (not Service names).

## `GET /metrics`

Prometheus text-format scrape endpoint. Gauges exposed:

| Metric | Meaning |
|--------|---------|
| `worker_input_x` | Resolved x for this role |
| `worker_target_cpu_millicores` | `cpu.a * x + cpu.b` |
| `worker_target_ram_mb` | `ram.a * x + ram.b` |
| `worker_target_net_mbps` | `net.a * x + net.b` |
| `worker_actual_cpu_millicores` | 15s rolling average from cgroup `cpu.stat` |
| `worker_actual_ram_mb` | cgroup working set (matches cAdvisor / Grafana) |
| `worker_actual_net_mbps` | 15s rolling egress rate from `psutil.net_io_counters` |

All gauges are scraped by Prometheus via the `worker-templates`
PodMonitor in `manifests/monitoring.yaml`.

```bash
curl -s http://localhost:8080/metrics | grep -E "^worker_"
```

---

# Quick reference

## Templated mode (primary)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/templates` | Create a topology |
| GET | `/templates` | List topologies |
| GET | `/templates/<name>` | Inspect a topology |
| PATCH | `/templates/<name>` | Partial update — merges, re-resolves, re-materialises |
| DELETE | `/templates/<name>` | Tear a topology down |

## Unified site API (`/api/v1`)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/overview` | Site-wide snapshot of all templates |
| GET | `/api/v1/templates/<name>/status` | Rich per-role state + live gauges + edges |
| GET | `/api/v1/templates/<name>/summary` | CPU/RAM/net averaged over a window (`?range=&resources=&by_role=&include_x=`) |
| POST | `/api/v1/templates/<name>/roles/<role>/scale` | Scale a role to `replicas` pods |

## Health

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Controller liveness |

## Worker (port-forward to access)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Worker liveness |
| GET | `/status` | Worker state (resolved x, formulas, peers) |
| GET | `/metrics` | Prometheus scrape |
