# Validating the emulator against a real application

A repeatable procedure for checking that the emulator reproduces a real
application's behaviour: profile the app's CPU / RAM / network as you vary its
load, fit the linear model (`value = a·x + b`) the emulator uses, feed those
coefficients into a template, run it, and compare.

The example below uses `nginx` as a stand-in — **swap in any app**; the steps
are identical. Replace `192.168.2.2` with your MicroK8s host IP throughout.

## The core idea

The emulator models each resource as `value = a·x + b`, where `x` is an input
signal. To fit that for a real app you need **one load variable you can both
vary on the app and set in the emulator**. The universal choice is **offered
request rate (RPS)**. So: `x = achieved RPS`, on both sides.

The loop, for any app:

1. Run the app under a fixed RPS.
2. Sweep RPS across several levels; measure steady-state CPU/RAM/net at each.
3. Linear-fit each resource vs RPS → `a`, `b`, `R²`.
4. Put `a`/`b` into a template role, drive at the same `x`, measure the same way.
5. Compare emulator vs app per resource; within ~±10% → validated.

Measure both sides with the **same instruments** (cgroup / cAdvisor metrics
that Prometheus already scrapes for every pod) so the comparison is like-for-like.

## Prerequisites

- MicroK8s + kube-prometheus-stack + the emulator running.
- A load generator on your laptop: `brew install vegeta` (or `k6` / `hey`).
- Grafana (for running the PromQL below in **Explore**), or a `kubectl
  port-forward` to Prometheus.

---

## 1. Deploy the target app

Label it (so the cAdvisor pod selector works) and give it a CPU/RAM limit that
**matches your worker pods**, for a fair comparison.

```yaml
# target.yaml
apiVersion: apps/v1
kind: Deployment
metadata: {name: target, labels: {app: target}}
spec:
  replicas: 1
  selector: {matchLabels: {app: target}}
  template:
    metadata: {labels: {app: target}}
    spec:
      containers:
        - name: app
          image: nginx:latest                 # ← swap your app here
          ports: [{containerPort: 80}]
          resources:
            requests: {cpu: "100m", memory: "64Mi"}
            limits:   {cpu: "2000m", memory: "1Gi"}
---
apiVersion: v1
kind: Service
metadata: {name: target}
spec:
  type: NodePort
  selector: {app: target}
  ports: [{port: 80, targetPort: 80, nodePort: 30090}]
```

```bash
microk8s kubectl apply -f target.yaml
```

cAdvisor scrapes it automatically — no extra config (these are kubelet metrics).

## 2. Define `x` and the sweep

`x` = request rate (RPS). Pick levels spanning the operating range, e.g.
**25, 50, 100, 200, 400**.

## 3. For each level: drive a fixed rate, measure steady state

Run a constant rate for ~3 min (discard the first minute as warm-up):

```bash
echo "GET http://192.168.2.2:30090/" | vegeta attack -rate=100 -duration=180s | vegeta report
```

Note vegeta's **achieved** rate from the report — use *that* as `x` (if the app
can't keep up, that's its saturation point). While it's in steady state, run
these in **Grafana → Explore** (Prometheus datasource), pointed at the app's
pods — the same windowed-average pattern the `/summary` endpoint uses:

```promql
# CPU (millicores)
avg_over_time( sum(rate(container_cpu_usage_seconds_total{pod=~"target-.*",container!=""}[1m]))[2m:] ) * 1000
# RAM (MB, working set)
avg_over_time( sum(container_memory_working_set_bytes{pod=~"target-.*"})[2m:] ) / 1e6
# Net egress (Mbps)
avg_over_time( sum(rate(container_network_transmit_bytes_total{pod=~"target-.*"}[1m]))[2m:] ) * 8 / 1e6
```

Record `(rps, cpu, ram, net)`. **Repeat each level ~3×** to get mean ± std
(your graph with error bars).

## 4. Fit the lines

```python
import numpy as np
rps = [25, 50, 100, 200, 400]
cpu = [...]; ram = [...]; net = [...]      # your measured means
for name, y in [("cpu", cpu), ("ram", ram), ("net", net)]:
    a, b = np.polyfit(rps, y, 1)
    r2 = np.corrcoef(rps, y)[0, 1] ** 2
    print(f"{name}: a={a:.4f} b={b:.2f} R2={r2:.3f}")
```

Read the **R²**: high → linear, trust the fit; low → the app is nonlinear
(saturating CPU, GC-driven RAM), so restrict the fit to its linear region and
note that as a finding.

## 5. Build the emulator template from the fit

One role = the app. CPU and RAM only need that single role — but **network
needs somewhere to send** (the emulator only generates egress *to peers*), so
add a tiny `sink` role + an edge so the app role actually emits its `net`.

```json
{
  "name": "emu-target",
  "x": 100,
  "roles": {
    "app":  {"count": 1,
             "cpu": {"a": <a_cpu>, "b": <b_cpu>},
             "ram": {"a": <a_ram>, "b": <b_ram>},
             "net": {"a": <a_net>, "b": <b_net>}},
    "sink": {"count": 1, "cpu": {"a": 0, "b": 0}, "ram": {"a": 0, "b": 0}, "net": {"a": 0, "b": 0}}
  },
  "edges": [{"from": "app", "to": "sink"}]
}
```

```bash
curl -X POST http://192.168.2.2:30081/templates \
  -H 'Content-Type: application/json' -d @emu-target.json
```

Set `x` to one of your measured RPS levels (here 100).

## 6. Drive at the same `x` and compare

Wait ~2 min for the emulator to settle, then read the `app` role back with the
same windowing:

```bash
curl -s "http://192.168.2.2:30081/api/v1/templates/emu-target/summary?range=2m&by_role=true&include_x=true" \
  | python3 -m json.tool
```

For each resource compute **relative error** = `|emulator − app| / app` at that
`x`. Within ~±10% → the emulation reproduces the app. Re-check at 2–3 more `x`
levels to confirm it holds across the range, not just one point.

## 7. Repeat for any app

Swap the `image:` in `target.yaml` and the URL/path in the vegeta line —
**everything else is identical**. That sameness *is* the universal method:
always `x` = achieved RPS, always cgroup/cAdvisor metrics, always least-squares
fit, always compare with the same `/summary`-style window.

---

## Validity threats to respect

- **The emulator has a per-pod resource floor — the target must exceed it.**
  Each worker pod consumes resources just to exist: the Python process plus its
  iperf3 server pool sit at roughly **~4 mc CPU and ~18 MB RAM** when idle
  (read the floor off a zero-load role — e.g. a `sink` — in `/summary`'s
  `actual_avg`). The emulator therefore **cannot reproduce a target lighter
  than this floor**, and RAM is the hard wall: a target using *less* RAM than
  the worker needs to run (e.g. idle `nginx` at ~13 MB) is physically
  impossible to emulate, no matter how good the fit. Validate against a target
  whose CPU/RAM/net at the chosen load sit **comfortably above** the floor (rule
  of thumb: RAM well north of ~40–50 MB, CPU in the hundreds of mc) so the
  baseline is negligible. If you must profile a light app, either drive it far
  harder so its footprint clears the floor, or report the floor as a stated
  lower bound on what the emulator can represent.
- **Linearity is an assumption — measure it.** Report R². Real apps are often
  piecewise-linear or saturate (CPU caps at core count; RAM plateaus once
  caches fill). The emulator is linear, so it can only faithfully reproduce the
  **linear operating region** — state that, don't hide it.
- **Use achieved RPS as `x`, not requested.** Past saturation the app can't keep
  up; achieved load keeps the fit honest and shows the knee.
- **RAM is the trickiest resource** — frequently *not* linear in RPS (GC
  sawtooth, caches, pools); often roughly constant. Expect small `a`, large `b`.
- **Net is usually the cleanest** — bytes ≈ payload-size × RPS → linear.
  Compare egress-to-egress (the emulator's net is egress).
- **Same instrument on both sides** — cgroup vs cgroup. Never compare the app's
  cAdvisor CPU against a differently-measured number.
- **Isolation** — one app per node/quota, matched CPU limits, no noisy
  neighbours, or contention pollutes the fit.
- **Repeatability** — pin app version, fixed load-gen seed, k trials, report
  mean ± std.

## Going further: validate the topology, not just one service

Single apps validate the per-role fit. To also exercise `x`-propagation and
`edges`, profile a real microservice benchmark — measure each service's
resource-vs-RPS *and* the inter-service traffic, then build a multi-role
template with edges mirroring the real call graph. Good targets spanning a
range of profiles:

- **Online Boutique** (Google `microservices-demo`) — ~11 services, built-in load generator.
- **DeathStarBench** (social-network / hotel-reservation) — research-grade, ships workload generators.
- **Train-Ticket** — large, Java-heavy.
- Singletons for breadth: the bundled grayscale service in
  [`target-app/`](target-app/) (compute + RAM + net all above the floor —
  GET-driven, no request body), `redis` (`redis-benchmark`), `postgres`
  (`pgbench`). Note `nginx` is a poor choice — it sits *below* the emulator's
  RAM floor (see Validity threats), and `h2non/imaginary` has no ARM64 image.
