# Runbook

Operational reference for the cloud-native emulator: build, deploy, drive,
observe, debug, and tear down. Two ingestion paths (HTTP + declarative)
share one materialiser; both are covered here.

Run everything from the repo root. Replace `jp36` with your Docker Hub
username and `192.168.2.2` with your MicroK8s host IP throughout. Pods
live in the `cloud-native-emulator` namespace by default — add
`-n cloud-native-emulator` if your kubectl context targets `default`.

---

## Prerequisites

- Docker Desktop (or any Docker daemon)
- A Docker Hub account, repos public or with `imagePullSecret` configured
- MicroK8s running on a host you can reach over the network
- `metrics-server` enabled (`microk8s enable metrics-server`) — required
  for `kubectl top`
- A Prometheus Operator installation if you want the `PodMonitor` in
  `manifests/monitoring.yaml` to be picked up (otherwise the workers'
  annotations let plain Prometheus scrape them via service discovery)
- Optional: Chaos Mesh, if you use the template's `latency` field for
  inter-tier delay. On MicroK8s, install with the containerd socket
  override (`--set chaosDaemon.runtime=containerd --set
  chaosDaemon.socketPath=/var/snap/microk8s/common/run/containerd.sock`)

---

## Build & push images

**Important:** the controller's Dockerfile copies a manifest file from
`manifests/`, so it must be built from the **repo root** with `-f`. The
worker has no such constraint.

```bash
# Worker
docker build -t jp36/emulator-worker:latest worker/
docker push jp36/emulator-worker:latest

# Controller — build context = repo root
docker build -f controller/Dockerfile -t jp36/emulator-controller:latest .
docker push jp36/emulator-controller:latest
```

If the COPY layer is cached and you suspect file changes weren't picked
up, force a fresh build:

```bash
docker build --no-cache -t jp36/emulator-worker:latest worker/
```

---

## Deploy

### First time

```bash
microk8s kubectl apply -f manifests/controller.yaml
microk8s kubectl apply -f manifests/monitoring.yaml   # if using Prometheus Operator
```

> Do **not** `kubectl apply -f manifests/worker-template.yaml`. It's a
> stencil with `__TEMPLATE__` / `__ROLE__` placeholders; the controller
> reads it at runtime and substitutes per role.

### After rebuilding the controller image

The controller is a `Pod` (not a Deployment), so it doesn't auto-restart.
Delete + re-apply to force a pull of the new image:

```bash
microk8s kubectl delete pod controller
microk8s kubectl apply -f manifests/controller.yaml
microk8s kubectl wait --for=condition=Ready pod/controller --timeout=120s
```

### After rebuilding the worker image

Workers run inside Deployments managed by the materialiser. The simplest
way to force a pull is to scale, or trigger a rollout:

```bash
microk8s kubectl rollout restart deployment -l template=<name>
```

Or tear down + re-POST the template (clean slate):

```bash
curl -X DELETE http://192.168.2.2:30081/template
# re-POST
```

---

## Drive the system — two modes

### Mode A: HTTP template (most flexible)

A controller manages **one** template, so the routes are singular and take
no name. POSTing a different name while one exists returns `409` — DELETE
or PATCH the existing one first.

```bash
curl -X POST http://192.168.2.2:30081/template \
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

# Inspect (or load a ready-made one from templates/*.json)
curl -s http://192.168.2.2:30081/template | python3 -m json.tool

# Tear down
curl -X DELETE http://192.168.2.2:30081/template
```

The response from POST returns the resolved peer map (Service names) and
the role counts; verify it matches what you intended before walking away.

### Mode B: Declarative — kubectl apply a labelled ConfigMap

```yaml
# fb-template.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fb
  labels:
    emulator.local/template: "true"     # the watcher's marker
data:
  template.json: |
    {
      "x": 10,
      "roles": {
        "frontend": {"count": 1, "cpu":{"a":10,"b":100}, "ram":{"a":2,"b":32}, "net":{"a":0.2,"b":2}},
        "backend":  {"count": 2, "cpu":{"a":5,"b":50},  "ram":{"a":1,"b":16}, "net":{"a":0.1,"b":1}}
      },
      "edges": [{"from":"frontend","to":"backend"}]
    }
```

```bash
microk8s kubectl apply -f fb-template.yaml      # materialises within ~15s
microk8s kubectl edit configmap fb              # live edit → watcher re-materialises
microk8s kubectl delete configmap fb            # watcher tears down entire topology
```

The template's `name` comes from `metadata.name`. Any `name` field inside
`template.json` is overwritten by the watcher.

---

## Query & scale via the observability endpoints

These are the place to *observe everything* — clean JSON, every response
tagged with this VM's `site` block (`SITE_ID` / `SITE_TIER`) and a
generation `timestamp` in the controller's local timezone. Full schemas in
`API.md`; the day-to-day commands:

```bash
# What is running on this VM right now? (also the liveness "ok": true)
curl -s http://192.168.2.2:30081/overview | python3 -m json.tool

# Live status: per-role desired/ready, resolved x, target vs actual sums,
# per-pod gauges, measured edge traffic.
curl -s http://192.168.2.2:30081/measurements/now | python3 -m json.tool

# CPU/RAM/net aggregated over a time window (defaults to the last 15m).
curl -s "http://192.168.2.2:30081/measurements/range" | python3 -m json.tool
# A fixed past hour, CPU only (timestamps are controller-local; no offset = local):
curl -s "http://192.168.2.2:30081/measurements/range?start=2026-06-10T11:00:00&end=2026-06-10T12:00:00&resources=cpu" \
  | python3 -m json.tool
# That hour sliced into 10-minute periods:
curl -s "http://192.168.2.2:30081/measurements/periods?start=2026-06-10T11:00:00&end=2026-06-10T12:00:00&chunk=10m" \
  | python3 -m json.tool
```

Scaling a role adds/removes pods *and* re-wires the topology (re-resolves
x, re-assigns peers + iperf port offsets) — it is not a bare Deployment
replica bump. There's no dedicated scale endpoint; PATCH the role's count:

```bash
# Set backend to 4 pods
curl -X PATCH http://192.168.2.2:30081/template \
  -H 'Content-Type: application/json' -d '{"roles.backend.count": 4}'
```

After a scale-up, `/measurements/now` shows new pods with empty
`metrics: {}` until they become Ready and land in Endpoints — watch them
fill in there.

> If the template's `source == "watch"`, the ConfigMap watcher will
> revert a PATCH within ~10s. Edit the labelled ConfigMap's `count`
> instead (Mode B) for a sticky change.

> The windowed `/measurements/range` and `/periods` need Prometheus
> reachable at `PROM_URL`. If it isn't, they still return 200 with
> `prometheus.available: false` and empty `totals` — overview /
> measurements/now (live-scrape based) keep working regardless.

---

## Template authoring — math that conserves

Network targets across an edge **must conserve traffic** or the actuals
won't match the configured numbers. If frontend sends 7 Mbps total and
backends each want 2 Mbps, that's only 4 Mbps of demand — the remaining
3 Mbps still lands on the backends, making each one ~75% over target.

Rule of thumb for a single `from → to` edge:

```
sum(from_role.net  ×  from_role.count)
   =
sum(to_role.net    ×  to_role.count)
```

For 1 frontend → 2 backends with each backend at 2 Mbps:
- frontend.net should resolve to `2 × 2 = 4 Mbps`
- formulas: `frontend.net = {a:0.2, b:2}` at `x=10` → 4 ✓

CPU and RAM don't need to conserve — they're independent per role.

---

## Tuning the iperf3 server pool

Each templated worker pod runs N iperf3 servers on consecutive ports
starting at 9999. N is set by the `IPERF_PORT_COUNT` env var (declared
in `manifests/worker-template.yaml`, default `"2"`).

- **N=2** — fits 1-to-many fan-out. ~2.4 MiB RAM baseline for the pool.
- **N=8** — fits many-to-many topologies up to 8 concurrent inbound
  source pods on a single target. ~10 MiB RAM baseline.
- Larger N tolerates more concurrency but raises the RAM floor. For
  roles with low RAM targets (≤30 MB), the baseline can exceed the
  target and the RAM nudger won't be able to converge.

To change it: edit the env value in `manifests/worker-template.yaml`,
rebuild the controller image (the manifest is baked in), redeploy the
controller pod, then re-POST or re-apply each template so new pods come
up with the new env.

---

## Observe — proven verification commands

These are the exact checks used during system validation.

### Confirm the controller is on the latest code

```bash
microk8s kubectl logs controller --tail=20
# Look for:
#   ConfigMap watcher: polling every 10.0s for labelled ConfigMaps
#   Controller listening on 0.0.0.0:8081
```

If the watcher line is missing, you're on a pre-Step-6 image — rebuild.

### Inspect a materialised template

```bash
microk8s kubectl get all,configmap -l template=<name>
curl -s http://192.168.2.2:30081/template | python3 -m json.tool
```

The GET endpoint reconstructs the template from cluster annotations —
this is how you verify the materialiser wrote the right metadata.

### Frontend's peer IPs (post Phase 2)

```bash
microk8s kubectl get configmap wt-<name>-<role>-config \
  -o jsonpath='{.data.config\.json}' | python3 -m json.tool
```

The `peers` field should be a list of concrete pod IPs (not the Service
name). If it's still a Service name like `wt-fb-backend`, Phase 2 hasn't
completed yet (it has a 30s endpoint-readiness timeout).

### Per-pod metrics (truth comparison)

```bash
microk8s kubectl top pod -l template=<name>

for pod in $(microk8s kubectl get pods -l template=<name> -o name); do
  echo "--- $pod ---"
  pf_port=$(( 8000 + RANDOM % 1000 ))
  microk8s kubectl port-forward $pod ${pf_port}:8080 >/dev/null 2>&1 &
  PF=$!
  sleep 2
  curl -s http://localhost:${pf_port}/metrics \
    | grep -E "^worker_(actual|target)_(cpu|ram|net)|^worker_cpu_stress"
  kill $PF 2>/dev/null
  wait $PF 2>/dev/null
done
```

What "good" looks like after ~60s of settle time:
- RAM actual within ~1% of target
- Net actual within ~5% of target on every pod (senders and receivers)
- CPU `kubectl top` near target; `/metrics` may be bursty but rate over
  `[1m]` in Grafana converges. `worker_actual_cpu_millicores` settles within
  the CPU deadband (`max(CPU_TOLERANCE_MC, 5% of target)`) of target ~30-45s
  after a (re)configure. Watch `worker_cpu_stress_millicores` step toward its
  resting value over the first few `CPU_ADJUST_INTERVAL_S` ticks — that's the
  feedback loop converging. Worker logs print a `CPU nudge: …` line on each
  resize.

### Worker startup log

```bash
microk8s kubectl logs deployment/wt-<name>-<role> | head -30
# Look for:
#   spawn: iperf3 -s -p 9999          (and one per IPERF_PORT_COUNT)
#   Configuring x=… peers=[…]         (Phase 2 reconfigure)
#   Sampler heartbeat                 (every 30s)
```

### Confirm sources don't fight (HTTP + Watch coexistence)

```bash
curl -s http://192.168.2.2:30081/overview | python3 -m json.tool   # lists templates + source
curl -s http://192.168.2.2:30081/template | python3 -m json.tool
# the /overview entry carries "source": "http"  or  "source": "watch"
```

---

## Diagnostics — common failures and fixes

### Controller returns "Connection refused"

The controller is a `Pod` (not a Deployment); manual `kubectl delete`
doesn't self-heal. Re-apply:

```bash
microk8s kubectl apply -f manifests/controller.yaml
```

### Pods stuck in `ContainerCreating`

```bash
microk8s kubectl describe pod <pod-name>
```

Usually `ImagePullBackOff`. Confirm the image was pushed and the tag
matches `manifests/worker-template.yaml`'s `__IMAGE__` substitution
(which defaults to `jp36/emulator-worker:latest`).

### `microk8s kubectl exec POD -- COMMAND` fails

The microk8s wrapper sometimes consumes the `--` separator. Workaround:

```bash
# Use port-forward + curl instead of exec for inspection:
microk8s kubectl port-forward <pod> 8080:8080 &
curl localhost:8080/status

# Or copy files out:
microk8s kubectl cp <pod>:/etc/emulator/config.json /tmp/config.json
```

### POST /template returns 502

A k8s API call failed mid-materialisation. Resources created up to the
failure point still exist. Re-POST is idempotent (`_apply` falls back to
PATCH on 409). Or `DELETE` the template to fully clean up.

### Backends show 0 Mbps actual net

Phase 2 hasn't completed yet (the workers are still in their initial
"empty peers" configure). Wait ~30s after POST and re-check. If it's
persistent: check controller logs for `Materialising template <name>`
and `wrote N peer IPs into …`.

### Worker CPU at full pod limit (e.g. 1000m) when target is much lower

Stress-ng can occasionally over-shoot during a burst. `kubectl top` over
a longer window will average out. If it's sustained, check the workers'
`/metrics` `worker_actual_cpu_millicores` against `worker_target_cpu_millicores`
— if both are pegged, the formula is producing a higher target than
intended.

### Grafana panels are empty for new templated pods

The `worker-templates` PodMonitor must be applied:

```bash
microk8s kubectl apply -f manifests/monitoring.yaml
microk8s kubectl get podmonitor      # expect: worker AND worker-templates
```

---

## Cleanup

### Tear down everything materialised

```bash
# The materialised template (one per controller)
curl -X DELETE http://192.168.2.2:30081/template

# Plus any labelled CMs the watcher would otherwise pick up again
microk8s kubectl get configmap -l emulator.local/template=true -o name \
  | xargs -r microk8s kubectl delete

# Belt-and-braces: anything still labelled as ours
microk8s kubectl delete all,configmap -l app.kubernetes.io/managed-by=emulator-controller
```

### Tear down the static infrastructure

```bash
microk8s kubectl delete -f manifests/controller.yaml
microk8s kubectl delete -f manifests/monitoring.yaml
```

---

## Known limitations

- **Controller is a `Pod`** — convert to a Deployment for self-healing.
- **stress-ng CPU is approximate, but now closed-loop** — `--cpu-load`
  accuracy depends on scheduler responsiveness, so actual CPU is somewhat
  bursty. CPU sizing is no longer open-loop: `configure()` only *seeds*
  stress-ng from the one-shot baseline, then `_adjust_cpu` (every
  `CPU_ADJUST_INTERVAL_S`, default 15 s) re-reads the pod's total cgroup CPU
  and resizes stress-ng so total converges on target. A baseline that read
  high during startup churn — which previously baked in a permanently
  under-target pod — now self-corrects within a few steps. Residual error
  settles inside the deadband (`max(CPU_TOLERANCE_MC, CPU_TOLERANCE_FRAC ×
  target)`); tighten `CPU_TOLERANCE_MC`/`CPU_TOLERANCE_FRAC` for a closer hold
  or lower `CPU_GAIN` if you see it hunting. One case the loop cannot fix: if
  the iperf3+python baseline alone exceeds a (low) target, stress-ng goes to 0
  and the pod still reads above target — fix the formula, not the worker. The
  worker passes `--cpu-load-slice` (env
  `CPU_LOAD_SLICE_MS`, default 20ms) to break the duty cycle into fine
  slices, which smooths it and tightens accuracy under contention.
  Default is 40ms (a balance of smoothness and mean accuracy); lower it
  (e.g. 20) for smoother still at a slightly larger under-bias, raise it
  towards stress-ng's coarse default (`0`) for less overhead. For
  steady-state numbers prefer the windowed `/measurements/range` or Grafana
  `rate(…[1m])` over single-second snapshots. Note the target tracks CPU
  *time* (cgroup `usage_usec`), so CPU-frequency scaling doesn't skew it.
  If a node is oversubscribed (sum of CPU targets > node cores), no knob
  can make pods hit target — check `kubectl top node`.
- **Endpoint enumeration is one-shot.** Phase 2 reads pod IPs at
  materialise time. If backend pods restart (e.g. rollout, eviction),
  their new IPs aren't propagated — re-POST or re-apply the template.
  (Inter-tier latency has the same hazard but self-heals: the controller
  re-resolves the Chaos Mesh rules on every materialise — see
  `controller/chaos.py`.)
- **Template formulas must conserve traffic** (see "Template authoring"
  above). The materialiser doesn't validate conservation.
- **`microk8s kubectl exec --`** is broken by the wrapper. Use
  port-forward or `kubectl cp` for in-pod inspection.

---

## Reference: file layout

```
controller/
  app.py            FastAPI HTTP routes: /template CRUD + /graph + observability
  api.py            observability: overview + measurements (k8s + scrape + prom)
  chaos.py          Chaos Mesh: template-defined latency + auto re-resolve on churn
  prom.py           Prometheus HTTP API client (instant queries), graceful
  graph.py          topology scrape + edge measurement (shared by /graph + api)
  materialiser.py   template → resources, two-phase create + IP resolve
  watcher.py        polls labelled CMs, reconciles via materialiser
  k8s.py            shared k8s API client
  Dockerfile        builds from REPO ROOT (needs manifests/)
worker/
  worker.py         entrypoint, configure() funnel
  state.py          STATE dict, POD_NAME, IPERF_PORT_COUNT, CPU_LOAD_SLICE_MS
  loads.py          stress-ng/mmap/iperf3 + per-peer supervisor threads
  metrics.py        cgroup-CPU sampler, prometheus gauges, RAM nudger
  watcher.py        filesystem watchdog on the mounted ConfigMap
manifests/
  controller.yaml         RBAC + ServiceAccount + Pod + NodePort Service
  worker-template.yaml    stencil baked into the controller image
  monitoring.yaml         PodMonitor for the templated worker pods
  grafana-dashboard.json  pod-agnostic dashboard
```
