# API Reference

HTTP endpoints exposed by the controller and worker pods.

> The controller serves a **live, generated OpenAPI spec** at
> `GET /openapi.json` and an interactive (dark-mode) Swagger UI at
> `GET /docs` — both always match the running code. This document is the
> human-readable companion with worked examples. (The checked-in
> `openapi.yaml` is hand-maintained and may lag; prefer `/openapi.json`.)

The controller is reachable via the NodePort Service in
`manifests/controller.yaml` (default `30081`). Workers are not normally
exposed externally — their endpoints are documented here for
diagnostics, Prometheus scraping, and direct `kubectl port-forward`.

Examples use `192.168.2.2:30081` for the controller (replace with your
MicroK8s host IP) and `localhost:8080` for the worker (after a
`kubectl port-forward`). Placeholders like `<role>` refer to whatever
role name you're working with.

## One controller, one template

A controller manages **exactly one template** — the topology for its
site/VM. The template routes are therefore **singular and take no name**
(`/template`, not `/templates/<name>`). The template's `name` field still
exists internally because every Kubernetes resource is named
`wt-<name>-<role>`. POSTing a second, differently-named template while one
is materialised returns `409`; re-POSTing the same name re-materialises
idempotently.

Every JSON response carries a top-level **`timestamp`** (controller-local
ISO 8601, with offset) recording when it was generated — except `/graph`,
whose shape is dictated by the Grafana Node Graph panel.

The controller serves three groups of endpoints:

- **Template** (`/template`) — create, inspect, patch, tear down the topology.
- **Observability** (`/overview`, `/measurements/*`, `/graph`) — query
  Kubernetes state, live worker gauges, and Prometheus-backed windows.
- **Worker** (port 8080) — per-pod status and metrics (not on the controller).

---

# Template

## `POST /template`

Materialise the topology. Validates the template (shape, cycle detection
on the role graph, latency durations), computes the resolved `x` for each
role via topological propagation, and creates one Deployment + ConfigMap +
Service per role. After the pods come up it resolves peer IPs (Phase 2) and
reconciles any inter-tier latency (Phase 3).

Idempotent for the **same** `name`: re-POSTing updates the existing
resources via PATCH (use it as a "reapply" if you lose track of state). For
surgical changes prefer `PATCH /template`.

**Body schema:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | k8s name (lowercase + digits + `-`). Becomes the resource prefix `wt-<name>-<role>` |
| `x` | number | yes | The signal that drives the topology. Sources use it directly; downstream roles use the resolved cascade |
| `roles` | object | yes | Map of `role_name → role_spec`. See below |
| `edges` | array | optional | List of `{"from": role, "to": role}` |
| `latency` | object | optional | Inter-tier one-way latency, applied as `tc` shaping on the tier nodes. See "Inter-tier latency" below |

**Role spec:**

```json
{
  "count": 2,
  "tier": "edge",
  "node": "site-edge-01-1",
  "cpu": {"a": 0, "b": 100},
  "ram": {"a": 0, "b": 128},
  "net": {"a": 0.2, "b": 0}
}
```

Targets are computed per pod as `value = a * x_role + b`, where `x_role` is
the resolved x for that role (see "x propagation" below). `tier` is optional
and pins the role's pods to nodes labelled `tier=<value>` (e.g.
edge / fog / cloud); absent = schedule anywhere.

`node` is optional and pins the role's pods to **one specific node** by name
(its `kubernetes.io/hostname`, which equals the node/VM name) via
`nodeSelector: {kubernetes.io/hostname: <node>}`. If combined with `tier` the
node must belong to that tier, else the pods stay `Pending`. All replicas of a
node-pinned role co-locate on that node. Change it live to **move** a
deployment — `PATCH {"roles.<role>.node": "site-edge-01-1"}` reschedules onto
that node; `PATCH {"roles.<role>.node": ""}` unpins it back to tier-level
placement. In the topology schema, set it per deployment as
`k8s_app_description[].node_name`.

**Inter-tier latency:**

```json
"latency": {
  "edge": { "fog": "30ms", "cloud": "120ms" },
  "fog":  { "cloud": "60ms" }
}
```

Each value is the **one-way latency** you want between pods of the two
tiers — `"edge": {"fog": "30ms"}` means a packet takes 30ms edge→fog (and
30ms fog→edge, since latency is symmetric). The controller applies it as
**link-level `tc` shaping on the nodes** (`controller/netem.py`): an `htb`
class + `netem` delay on each tier node's NIC, matched on the **peer tier's
node IP**, injecting the **full** value on each direction (so a round trip is
~2× it). Because the rule lives on the inter-node link, pods inherit it
automatically — scaling a tier never disturbs it. Reconciled on every
materialise and removed on DELETE. Pairs are symmetric — give each once, in
either orientation. Durations accept `us` / `ms` / `s`; same-tier pairs,
duplicate pairs, and values above 10s are rejected with a 400. Requires the
controller to run on the host with `multipass` access to the nodes (no Chaos
Mesh needed). Retune live with a dot-path PATCH: `{"latency.edge.fog":
"50ms"}`. The measured one-way value is visible per link as
`worker_peer_rtt_ms / 2` (the TCP round-trip halved) in the Latency section
of `grafana/grafana-network.json`, next to the configured
`emulator_configured_one_way_ms`; measured throughput per link is in that
dashboard's Bandwidth section (`worker_peer_egress_mbps`).

**Example:**

```bash
curl -X POST http://192.168.2.2:30081/template \
  -H 'Content-Type: application/json' \
  -d @templates/iot-pipeline.json
```

**Response (201):**
```json
{
  "timestamp": "2026-06-12T14:00:00+01:00",
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
| 400 | Invalid JSON, validation failure, cycle in the role graph, or bad latency |
| 409 | A *different* template is already materialised — PATCH or DELETE it first |
| 502 | A k8s API call failed mid-materialisation. Partial resources may exist — re-POST or DELETE to clean up |

## `GET /template`

The materialised template, in full (the original POST body, reconstructed
from ConfigMap annotations — there's no in-memory state on the controller),
with a `timestamp` prepended.

```bash
curl http://192.168.2.2:30081/template | python3 -m json.tool
```

`404` if nothing is materialised. (A re-POST of the GET response round-trips
cleanly — the controller strips the injected `timestamp` on the way in.)

## `PATCH /template`

Apply a partial update. Deep-merges the patch body into the existing
template, re-runs validation (including cycle detection), and
re-materialises. Workers pick up the new ConfigMap within ~60s (kubelet
sync) — **no pod restart** unless the change adds/removes pods.

**Merge semantics:**

- Dicts merge recursively. `{"roles": {"<role>": {"net": {"a": 0.3}}}}`
  changes only that role's `net.a`.
- Scalars and lists in the patch **replace** the existing value. To change
  one edge, send the whole new `edges` list. (`latency` is an object, so a
  single pair merges in place.)
- **Dot-path shorthand**: a flat key with dots expands to nested JSON, e.g.
  `{"latency.edge.cloud": "150ms"}` ≡ `{"latency": {"edge": {"cloud": "150ms"}}}`.
- The `name` and any injected `timestamp` in the body are ignored.

**Common patches:**

```bash
# Change x — the whole topology recascades
curl -X PATCH http://192.168.2.2:30081/template -d '{"x": 20}'

# Bump one role's network — downstream x values re-resolve
curl -X PATCH http://192.168.2.2:30081/template -d '{"roles.web.net.a": 0.30}'

# Scale a role (adds/removes pods; re-resolves x + re-wires peers)
curl -X PATCH http://192.168.2.2:30081/template -d '{"roles.api.count": 4}'

# Retune an inter-tier latency live
curl -X PATCH http://192.168.2.2:30081/template -d '{"latency.edge.cloud": "150ms"}'
```

**Response (200):** `{"timestamp", "name", "template": {…merged…}, "peers": {…}}`

**Status codes:** `200` patched · `400` validation/cycle/bad latency ·
`404` no template materialised · `502` k8s failure.

## `DELETE /template`

Tear down every resource for the materialised template. Inter-tier link
shaping (`tc` on the tier nodes) is removed first, along with any leftover
Chaos Mesh `NetworkChaos`, then Deployments, Services, and ConfigMaps.

```bash
curl -X DELETE http://192.168.2.2:30081/template
# → {"timestamp":"…","name":"<name>","deleted":<count>}
```

`404` if nothing is materialised. `deleted` counts the Kubernetes objects
removed.

---

# Observability

Read endpoints fuse three data sources behind one envelope:

- **Kubernetes live state** — Deployments (desired replicas) and Pods
  (phase, readiness, restarts, node, age) via the k8s API.
- **Instantaneous worker gauges** — scraped straight from each pod's
  `/metrics` (no Prometheus dependency).
- **Windowed aggregates** — via the Prometheus HTTP API (`PROM_URL`), for
  the `/measurements/range` and `/measurements/periods` endpoints.

### Site identity & federation

Every observability response carries a `site` block:

```json
"site": {"id": "<SITE_ID>", "tier": "<SITE_TIER>"}
```

One controller runs per VM; `SITE_ID` / `SITE_TIER` are set per controller
via env (`manifests/controller.yaml`; default `"local"` / `"unknown"`). The
block lets a future federation gateway fan out to many controllers and merge
responses by site with no schema change.

### Timezone

Timestamps in responses (and naive request timestamps to
`/measurements/*`) use the controller's local timezone, set via the `TZ`
env var (default `Europe/London`). Internals compute in absolute unix time,
so this is presentation only; rendered values carry their UTC offset so
they stay unambiguous.

## `GET /overview`

Site-wide snapshot — every materialised template with per-role desired vs
ready replica counts and a pod-health rollup. **Subsumes the old `/health`
endpoint**: the `"ok": true` field is present whenever the web layer is up
(the k8s readiness probe itself is now a cheap TCP-socket check, not an HTTP
hit on this heavier endpoint).

```bash
curl http://192.168.2.2:30081/overview | python3 -m json.tool
```

**Response (200):**
```json
{
  "timestamp": "2026-06-12T14:00:00+01:00",
  "ok": true,
  "site": {"id": "local", "tier": "cloud"},
  "namespace": "cloud-native-emulator",
  "templates": [
    {"name": "<name>", "source": "http",
     "roles": {"<role>": {"desired": 2, "ready": 2}},
     "pods": {"total": 3, "ready": 3}}
  ],
  "prometheus": {"available": true, "url": "http://…:9090"}
}
```

## `GET /measurements/now`

Rich, instantaneous per-role status. For each role: desired/ready replicas,
the resolved `x`, role-level target and actual sums (CPU/RAM/NET), and a
per-pod list fusing k8s state with that pod's current worker gauges. Also
returns measured role→role edge traffic. Live scrapes only — no Prometheus.

Pods that exist in k8s but aren't in Endpoints yet (starting up, not Ready)
are surfaced with an empty `metrics: {}`, so an in-progress scale-up is
visible rather than silently missing.

```bash
curl http://192.168.2.2:30081/measurements/now | python3 -m json.tool
```

**Response (200, abridged):**
```json
{
  "timestamp": "2026-06-12T14:00:00+01:00",
  "site": {"id": "local", "tier": "cloud"},
  "name": "<name>",
  "roles": {
    "<role>": {
      "desired": 2, "ready": 2, "x": 10.0,
      "targets": {"cpu_millicores": 100.0, "ram_mb": 128.0, "net_mbps": 4.0},
      "actuals": {"cpu_millicores": 98.2, "ram_mb": 130.1, "net_mbps": 3.9},
      "pods": [{"name": "wt-<name>-<role>-abc123", "ip": "10.1.0.42",
                "node": "node-1", "phase": "Running", "ready": true,
                "restarts": 0, "age_seconds": 312,
                "metrics": {"x": 10.0, "target_cpu_millicores": 50.0, "...": "..."}}]
    }
  },
  "edges": [{"from": "<role-a>", "to": "<role-b>", "mbps": 3.912}],
  "prometheus": {"available": null}
}
```

`404` if nothing is materialised.

## `GET /measurements/range`

CPU/RAM/network for the template, **aggregated between two points in
time** — the "just tell me the numbers" endpoint. Returns scalars: the
template-wide target and actual summed across pods then reduced over the
interval, **always** broken down by role and accompanied by an `x` block.

**Query params:**

| Param | Default | Notes |
|-------|---------|-------|
| `start` | `end` − 15m | ISO 8601 (`2026-06-10T11:00:00`) or unix epoch seconds. No offset ⇒ controller-local time; `Z`/offset honoured |
| `end` | now | Same formats as `start` |
| `resources` | `cpu,ram,net` | Comma-separated subset. Unknown values → 400 |

`totals` carries `target_avg`, `actual_avg`, `actual_min`, `actual_max`;
the per-role breakdown carries the two averages. The `x` block is split by
provenance: **`input`** (source roles, whose x *is* the template's x) and
**`derived`** (downstream roles, x propagated from upstream egress). Both
are averaged — not summed — across each role's pods.

```bash
# Last 15 minutes (defaults)
curl -s "http://192.168.2.2:30081/measurements/range" | python3 -m json.tool

# A fixed hour, CPU + net only
curl -s "http://192.168.2.2:30081/measurements/range?start=2026-06-10T11:00:00&end=2026-06-10T12:00:00&resources=cpu,net" \
  | python3 -m json.tool
```

**Response (200, abridged):**
```json
{
  "timestamp": "2026-06-12T14:00:00+01:00",
  "site": {"id": "local", "tier": "cloud"},
  "name": "<name>",
  "start": "2026-06-10T11:00:00+01:00",
  "end":   "2026-06-10T12:00:00+01:00",
  "window": "3600s",
  "totals": {"cpu_millicores": {"target_avg": 980.0, "actual_avg": 951.2,
                                "actual_min": 902.0, "actual_max": 1010.5}, "...": "..."},
  "roles":  {"<role>": {"cpu_millicores": {"target_avg": 484.8, "actual_avg": 470.1}, "...": "..."}},
  "x": {"input":   {"<source>": 40.0},
        "derived": {"<downstream>": 31.5}},
  "prometheus": {"available": true, "url": "http://…:9090"}
}
```

**Errors:** `400` on an unknown resource, `start >= end`, or a window over
31 days. `404` if nothing is materialised. If Prometheus is unreachable,
returns 200 with `prometheus.available: false` and empty
`totals`/`roles`/`x` — use `/measurements/now` for a live, no-Prometheus
alternative.

## `GET /measurements/periods`

The **last `count` chunks of `chunk` each, ending now**, aggregated
separately — for seeing how the numbers move over a run rather than one
flattened average.

**Query params:** `resources` as above, plus:

| Param | Required | Notes |
|-------|----------|-------|
| `count` | yes | How many chunks to return, counting back from now |
| `chunk` | yes | Length of each chunk, e.g. `90s`, `10m`, `1h` (min `30s`, the scrape interval) |

You say how many chunks and how long each one is; the range is exactly
`count × chunk`, ending now. e.g. `count=4&chunk=11m` → the last 44 minutes
as 4 eleven-minute periods.

```bash
# The last 44 minutes as 4 eleven-minute periods
curl -s "http://192.168.2.2:30081/measurements/periods?count=4&chunk=11m" \
  | python3 -m json.tool
```

**Response (200, abridged):**
```json
{
  "timestamp": "…", "site": {"…": "…"}, "name": "<name>",
  "start": "…", "end": "…", "window": "2640s",
  "chunk": "660s", "count": 4,
  "periods": [
    {"start": "…", "end": "…",
     "totals": {"…": "…"}, "roles": {"…": "…"},
     "x": {"input": {"…": "…"}, "derived": {"…": "…"}}}
  ],
  "prometheus": {"available": true, "url": "http://…:9090"}
}
```

**Errors:** `400` for a missing/malformed `chunk` or `count`, or more than
100 chunks. `404` if nothing is materialised.

## `GET /graph`

Grafana Node Graph payload (nodes + measured edges) for the materialised
template — the data source behind `grafana/grafana-nodegraph.json`. **No
`timestamp`** — the panel rejects unknown top-level keys.

| Param | Default | Notes |
|-------|---------|-------|
| `view` | `role` | `role`: one node per role, stats summed across its pods, edges role→role. `pods`: one node per pod, raw pod→pod edges |

```bash
curl http://192.168.2.2:30081/graph | python3 -m json.tool
curl "http://192.168.2.2:30081/graph?view=pods" | python3 -m json.tool
```

`404` if nothing is materialised.

---

## x propagation — the model behind the template

The template's `x` is a *signal* that flows through the role graph, not a
global constant shared by every role.

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
(Kahn's algorithm). A source role's x is `template.x`; a downstream role's x
is the sum of upstream role-total egress. All three of CPU, RAM, and NET
formulas evaluate at this resolved x. The `/measurements/range` and
`/periods` responses split these into `x.input` (sources) and `x.derived`
(downstream).

Self-edges (intra-role mesh, `from == to`) are legal traffic-wise but
ignored for x-resolution. Cycles in the role graph are rejected at validate
time (`400`, `role graph has a cycle involving: …`). The resolved x per role
is logged at materialise time and visible per pod as the `worker_input_x`
gauge.

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

The worker's full `STATE` dict — what it's currently doing.

```json
{
  "running": true,
  "x": <resolved-x-for-this-role>,
  "cpu_millicores": <cpu.a * x + cpu.b>,
  "ram_mb": <ram.a * x + ram.b>,
  "net_mbps": <net.a * x + net.b>,
  "formulas": {"cpu": {"a": …, "b": …}, "ram": {"a": …, "b": …}, "net": {"a": …, "b": …}},
  "peers": ["<peer-ip-1>", "<peer-ip-2>", …]
}
```

For a templated worker, `x` is the *resolved* x for this pod's role, and
`peers` is the list of concrete pod IPs (not Service names).

## `GET /metrics`

Prometheus text-format scrape endpoint. Gauges exposed:

| Metric | Meaning |
|--------|---------|
| `worker_input_x` | Resolved x for this role |
| `worker_target_cpu_millicores` / `_ram_mb` / `_net_mbps` | `a * x + b` per resource |
| `worker_actual_cpu_millicores` | 15s rolling average from cgroup `cpu.stat` |
| `worker_actual_ram_mb` | cgroup working set (matches cAdvisor / Grafana) |
| `worker_actual_net_mbps` | 15s rolling egress rate from `psutil.net_io_counters` |
| `worker_cpu_stress_millicores` | CPU the feedback loop currently asks stress-ng to generate |
| `worker_peer_egress_mbps{peer,peer_name}` | Measured egress to a specific peer IP (from iperf3 interval reports). `peer` is the pod IP; `peer_name` is the destination role |
| `worker_peer_rtt_ms{peer,peer_name}` | TCP-handshake RTT to a peer pod, probed every 15s — includes injected inter-tier latency. `peer` is the pod IP; `peer_name` is the destination role |

All gauges are scraped via the PodMonitor in `manifests/monitoring.yaml`.

```bash
curl -s http://localhost:8080/metrics | grep -E "^worker_"
```

---

# Quick reference

## Template

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/template` | Materialise the topology (409 if a different one exists) |
| GET | `/template` | The materialised template, in full |
| PATCH | `/template` | Partial update — merges, re-resolves, re-materialises |
| DELETE | `/template` | Tear the topology down |

## Observability

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/overview` | Site-wide snapshot (subsumes `/health`) |
| GET | `/measurements/now` | Live per-role state + gauges + edges |
| GET | `/measurements/range` | CPU/RAM/net aggregated over `?start`–`?end` |
| GET | `/measurements/periods` | The last `?count` chunks of `?chunk` each, ending now |
| GET | `/graph` | Grafana Node Graph payload (`?view=role\|pods`) |
| GET | `/docs`, `/openapi.json` | Swagger UI (dark) and the generated spec |

## Worker (port-forward to access)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Worker liveness |
| GET | `/status` | Worker state (resolved x, formulas, peers) |
| GET | `/metrics` | Prometheus scrape |
