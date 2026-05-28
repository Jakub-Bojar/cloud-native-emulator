# Runbook

Operational reference for the cloud-native emulator: build, deploy, drive,
observe, debug, and tear down. Two ingestion paths (HTTP + declarative)
share one materializer; both are covered here.

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
# Legacy single-worker is optional — only needed if you're using POST /configure:
# microk8s kubectl apply -f manifests/worker.yaml
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

Workers run inside Deployments managed by the materializer. The simplest
way to force a pull is to scale, or trigger a rollout:

```bash
microk8s kubectl rollout restart deployment -l template=<name>
```

Or tear down + re-POST the template (clean slate):

```bash
curl -X DELETE http://192.168.2.2:30081/templates/<name>
# re-POST
```

---

## Drive the system — three modes

### Mode A: HTTP templates (most flexible)

```bash
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

# Inspect
curl -s http://192.168.2.2:30081/templates
curl -s http://192.168.2.2:30081/templates/fb | python3 -m json.tool

# Tear down
curl -X DELETE http://192.168.2.2:30081/templates/fb
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
microk8s kubectl apply -f fb-template.yaml      # materializes within ~15s
microk8s kubectl edit configmap fb              # live edit → watcher re-materializes
microk8s kubectl delete configmap fb            # watcher tears down entire topology
```

The template's `name` comes from `metadata.name`. Any `name` field inside
`template.json` is overwritten by the watcher.

### Mode C: Legacy single-worker (POST /configure)

Only if you have `manifests/worker.yaml` deployed alongside.

```bash
curl -X POST http://192.168.2.2:30081/configure \
  -H 'Content-Type: application/json' \
  -d '{"x":50,"cpu":{"a":10,"b":100},"ram":{"a":4,"b":64},"net":{"a":0.1,"b":1}}'
curl http://192.168.2.2:30081/status
curl -X POST http://192.168.2.2:30081/stop
```

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

### Inspect a materialized template

```bash
microk8s kubectl get all,configmap -l template=<name>
curl -s http://192.168.2.2:30081/templates/<name> | python3 -m json.tool
```

The GET endpoint reconstructs the template from cluster annotations —
this is how you verify the materializer wrote the right metadata.

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
    | grep -E "^worker_(actual|target)_(cpu|ram|net)"
  kill $PF 2>/dev/null
  wait $PF 2>/dev/null
done
```

What "good" looks like after ~60s of settle time:
- RAM actual within ~1% of target
- Net actual within ~5% of target on every pod (senders and receivers)
- CPU `kubectl top` near target; `/metrics` may be bursty but rate over
  `[1m]` in Grafana converges

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
curl -s http://192.168.2.2:30081/templates              # both should appear
curl -s http://192.168.2.2:30081/templates/<name> | python3 -m json.tool
# "source": "http"  or  "source": "watch"
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

### POST /templates returns 502

A k8s API call failed mid-materialization. Resources created up to the
failure point still exist. Re-POST is idempotent (`_apply` falls back to
PATCH on 409). Or `DELETE` the template to fully clean up.

### Backends show 0 Mbps actual net

Phase 2 hasn't completed yet (the workers are still in their initial
"empty peers" configure). Wait ~30s after POST and re-check. If it's
persistent: check controller logs for `Materializing template <name>`
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

### Tear down everything materialized

```bash
# All templates created via HTTP or watch
for t in $(curl -s http://192.168.2.2:30081/templates | python3 -c "import sys,json; print(' '.join(json.load(sys.stdin)['templates']))"); do
  curl -X DELETE http://192.168.2.2:30081/templates/$t
done

# Plus any labelled CMs the watcher would otherwise pick up again
microk8s kubectl get configmap -l emulator.local/template=true -o name \
  | xargs -r microk8s kubectl delete
```

### Tear down the static infrastructure

```bash
microk8s kubectl delete -f manifests/controller.yaml
microk8s kubectl delete -f manifests/monitoring.yaml
microk8s kubectl delete -f manifests/worker.yaml      # if used
```

---

## Known limitations

- **Controller is a `Pod`** — convert to a Deployment for self-healing.
- **stress-ng at low `--cpu-load`** is bursty by design. Use Grafana
  `rate(…[1m])` for steady-state numbers; single-second snapshots vary.
- **Endpoint enumeration is one-shot.** Phase 2 reads pod IPs at
  materialize time. If backend pods restart (e.g. rollout, eviction),
  their new IPs aren't propagated — re-POST or re-apply the template.
- **Template formulas must conserve traffic** (see "Template authoring"
  above). The materializer doesn't validate conservation.
- **`microk8s kubectl exec --`** is broken by the wrapper. Use
  port-forward or `kubectl cp` for in-pod inspection.

---

## Reference: file layout

```
controller/
  controller.py     HTTP routes; legacy /configure + /templates CRUD
  materializer.py   template → resources, two-phase create + IP resolve
  watcher.py        polls labelled CMs, reconciles via materializer
  k8s.py            shared k8s API client
  Dockerfile        builds from REPO ROOT (needs manifests/)
worker/
  worker.py         entrypoint, configure() funnel
  state.py          STATE dict, POD_NAME, IPERF_PORT_COUNT
  loads.py          stress-ng/mmap/iperf3 + per-peer supervisor threads
  metrics.py        cgroup-CPU sampler, prometheus gauges, RAM nudger
  watcher.py        filesystem watchdog on the mounted ConfigMap
manifests/
  controller.yaml         RBAC + ServiceAccount + Pod + NodePort Service
  worker.yaml             optional legacy single-worker Deployment
  worker-template.yaml    stencil baked into the controller image
  monitoring.yaml         two PodMonitors (legacy + templated)
  grafana-dashboard.json  pod-agnostic dashboard
Emulator-Architecture.docx  the visual walkthrough
```
