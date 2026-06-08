# Cloud-Native Emulator

A resource-emulation framework for the EEECS summer research project
*"High-Fidelity Emulation Framework for Cloud-Native Applications."*

You describe a **topology** of roles (e.g. gateway → auth/api → cache → db)
as JSON; the controller materialises one Kubernetes Deployment + ConfigMap +
Service per role and runs synthetic CPU, RAM, and network load in every pod
sized to per-role formulas. A single input signal `x` propagates through the
role graph, so changing one number recasts load across the whole topology.

Each controller is the **site API for one VM**. Every `/api/v1` response is
tagged with a `{site}` block (edge / fog / cloud), so the same system can be
run on multiple VMs and merged by a federation layer later.

## How it works

- **Template** — a named set of `roles` connected by directional `edges`.
  Each role has `count` replicas and linear formulas for CPU (millicores),
  RAM (MB), and network (Mbps): `value = a·x_role + b`.
- **x propagation** — source roles use the template's `x`; a downstream
  role's `x` is the sum of upstream role-total egress, resolved via a
  topological pass over the graph (cycles are rejected).
- **Workers** generate real load: `stress-ng` (CPU), an anonymous `mmap`
  (RAM, with a feedback nudger), and `iperf3` (per-peer network traffic).
- **Two-phase materialisation** — pods are created first, then their real
  IPs are resolved and written back so iperf3 clients target concrete peers.
- **Observability** — workers export Prometheus gauges (target vs actual per
  resource); the controller fuses k8s state + live scrapes + Prometheus into
  the `/api/v1` endpoints, and serves a Grafana node-graph of measured edges.

```
                POST /templates {x, roles, edges}
   operator ─────────────────────────────────▶ ┌──────────────┐
   GET /api/v1/... (observe / scale)            │ controller   │ NodePort 30081
                                                │ materialiser │
                                                └──────┬───────┘
                              create/patch via k8s API │
                 ┌───────────────────────┬─────────────┴─────────────┐
                 ▼                        ▼                           ▼
         Deployment+CM+Svc        Deployment+CM+Svc            Deployment+CM+Svc
          (role: gateway)          (role: api ×N)                 (role: db)
              worker pods  ◀── iperf3 peer traffic ──▶  worker pods
                 │ /metrics scrape
                 ▼
          Prometheus ──▶ Grafana
```

## Documentation

| Doc | What's in it |
|-----|--------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System diagrams, the x-propagation model, two-phase materialisation, federation roadmap |
| [API.md](API.md) | Full HTTP API reference with worked examples |
| [openapi.yaml](openapi.yaml) | Machine-readable OpenAPI 3.0 spec (load into Swagger UI / generate clients) |
| [RUNBOOK.md](RUNBOOK.md) | Build, deploy, drive, observe, debug, tear down |

## Prerequisites

- Docker + a Docker Hub account (examples use `jp36/…` — swap in your own)
- MicroK8s on a reachable host, with `metrics-server` enabled
- Optional: a Prometheus Operator install (kube-prometheus-stack) for the
  PodMonitor and the windowed `/summary` endpoint

## Quick start

Replace `jp36` with your Docker Hub user and `192.168.2.2` with your
MicroK8s host IP.

```bash
# 1. Build & push. The controller MUST build from the repo root (its
#    Dockerfile copies manifests/worker-template.yaml into the image).
docker build -t jp36/emulator-worker:latest worker/
docker push jp36/emulator-worker:latest
docker build -f controller/Dockerfile -t jp36/emulator-controller:latest .
docker push jp36/emulator-controller:latest

# 2. Deploy the controller (+ monitoring if you use Prometheus Operator).
microk8s kubectl apply -f manifests/controller.yaml
microk8s kubectl apply -f manifests/monitoring.yaml

# 3. Materialise a topology.
curl -X POST http://192.168.2.2:30081/templates \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "fb",
    "x": 10,
    "roles": {
      "frontend": {"count": 1, "cpu":{"a":10,"b":100}, "ram":{"a":2,"b":32}, "net":{"a":0.2,"b":2}},
      "backend":  {"count": 2, "cpu":{"a":5,"b":50},  "ram":{"a":1,"b":16}, "net":{"a":0.1,"b":1}}
    },
    "edges": [{"from":"frontend","to":"backend"}]
  }'

# 4. Observe.
curl -s http://192.168.2.2:30081/api/v1/overview | python3 -m json.tool
curl -s http://192.168.2.2:30081/api/v1/templates/fb/status | python3 -m json.tool
curl -s "http://192.168.2.2:30081/api/v1/templates/fb/summary?range=15m&by_role=true" | python3 -m json.tool

# 5. Scale a role (re-resolves x + re-wires peers).
curl -X POST http://192.168.2.2:30081/api/v1/templates/fb/roles/backend/scale \
  -H 'Content-Type: application/json' -d '{"replicas": 4}'

# 6. Tear down.
curl -X DELETE http://192.168.2.2:30081/templates/fb
```

You can also drive it declaratively — `kubectl apply` a ConfigMap labelled
`emulator.local/template: "true"` and the controller's watcher materialises
it. See [RUNBOOK.md](RUNBOOK.md).

## Project layout

```
cloud-native-emulator/
├── README.md
├── ARCHITECTURE.md · API.md · RUNBOOK.md · openapi.yaml
├── controller/
│   ├── Dockerfile
│   ├── app.py            FastAPI HTTP routes: /templates CRUD + /graph + /api/v1
│   ├── api.py            unified /api/v1 site API (query + scale)
│   ├── prom.py           Prometheus HTTP API client
│   ├── graph.py          topology scrape + edge measurement
│   ├── materialiser.py   template → k8s resources, two-phase create + IP resolve
│   ├── watcher.py        reconciles labelled template ConfigMaps
│   └── k8s.py            shared in-cluster k8s API client
├── worker/
│   ├── Dockerfile
│   ├── worker.py         entrypoint + configure() funnel
│   ├── loads.py          stress-ng / mmap / iperf3 load generators
│   ├── metrics.py        cgroup sampler, Prometheus gauges, RAM nudger
│   ├── state.py          shared state + tunable env constants
│   └── watcher.py        filesystem watchdog on the mounted ConfigMap
└── manifests/
    ├── controller.yaml          ServiceAccount + RBAC + Pod + NodePort Service
    ├── worker-template.yaml      per-role blueprint, baked into the controller image
    ├── monitoring.yaml           PodMonitor for the templated worker pods
    └── grafana-dashboard.json    dashboards
```
