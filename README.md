# Cloud-Native Emulator Prototype

A two-pod prototype for the EEECS summer research project
"High-Fidelity Emulation Framework for Cloud-Native Applications".

## Architecture

```
   user / experiment driver
            │  HTTP POST /configure  { x, cpu:{a,b}, ram:{a,b}, net:{a,b} }
            ▼
   ┌──────────────────────┐
   │   controller pod     │  (controller-service, NodePort 30081)
   │  validates + routes  │
   └──────────┬───────────┘
              │  forwards JSON
              ▼
   ┌──────────────────────┐
   │     worker pod       │  (worker-service, ClusterIP 8080)
   │  applies a·x + b for │
   │  CPU, RAM, network   │
   │  and emulates the    │
   │  resulting load      │
   └──────────────────────┘
```

* **Worker** — Computes `cpu_percent = cpu.a·x + cpu.b`, etc., then actually
  burns CPU, allocates RAM, and pushes loopback network traffic at the
  computed rates.
* **Controller** — Front door. Accepts the same JSON, validates it, and
  forwards to the worker via the worker's Kubernetes Service DNS name
  (`worker-service:8080`).

## Request shape

```json
{
  "x":   50,
  "cpu": { "a": 10,  "b": 100 },
  "ram": { "a": 4,   "b": 64  },
  "net": { "a": 0.1, "b": 1   }
}
```

With `x = 50` this produces:

| Resource | Formula        | Target                |
|----------|----------------|-----------------------|
| CPU      | 10·50 + 100    | 600 m (0.6 cores)     |
| RAM      | 4·50 + 64      | 264 MB                |
| Network  | 0.1·50 + 1     | 6 Mbps (loopback)     |

CPU is in **millicores**, matching Kubernetes' native unit: 1000m = 1 full
core. This makes the values directly comparable to pod `resources.limits`.

---

## Project layout

```
k8s-emulator/
├── README.md
├── controller/
│   ├── Dockerfile
│   └── controller.py
├── manifests/
│   └── emulator.yaml
└── worker/
    ├── Dockerfile
    └── worker.py
```

---

## Prerequisites

* Docker
* A Docker Hub account (this README uses `jp36/...` — swap in your own)
* MicroK8s installed and running:
  ```bash
  sudo snap install microk8s --classic
  sudo usermod -a -G microk8s $USER && newgrp microk8s
  microk8s status --wait-ready
  ```
* `metrics-server` enabled (so `kubectl top` works):
  ```bash
  microk8s enable metrics-server
  ```

---

## Step 1 — Build and push the images

Tag both images with your Docker Hub username so `docker push` knows where
to send them. Run from the repo root:

```bash
# Worker
cd worker
docker build -t jp36/emulator-worker:latest .
docker push  jp36/emulator-worker:latest

# Controller
cd ../controller
docker build -t jp36/emulator-controller:latest .
docker push  jp36/emulator-controller:latest

cd ..
```

Make sure both Docker Hub repos are **public**, or set up an
`imagePullSecret` and reference it in the manifest.

If you change your Docker Hub username, also update the two `image:` lines
in `manifests/emulator.yaml`.

---

## Step 2 — Deploy to MicroK8s

```bash
microk8s kubectl apply -f manifests/emulator.yaml
microk8s kubectl get pods -w
```

Wait until both pods read `Running` with `1/1` ready. The manifest sets
`imagePullPolicy: Always`, so MicroK8s will pull the latest image on every
pod restart — important when you rebuild but keep the `:latest` tag.

To check what was deployed:

```bash
microk8s kubectl get pods,svc
```

You should see:

| Name                         | Kind    | Notes                |
|------------------------------|---------|----------------------|
| `worker`                     | Pod     | the workload         |
| `controller`                 | Pod     | the front door       |
| `worker-service`             | Service | ClusterIP, port 8080 |
| `controller-service`         | Service | NodePort, port 30081 |

---

## Step 3 — Drive it

MicroK8s exposes NodePorts on the host directly, so the controller is
reachable at `http://192.168.2.2:30081`.

### Send a configuration

```bash
curl -X POST http://192.168.2.2:30081/configure \
  -H 'Content-Type: application/json' \
  -d '{"x":50,"cpu":{"a":10,"b":100},"ram":{"a":4,"b":64},"net":{"a":0.1,"b":1}}'
```

Response:

```json
{
  "running": true,
  "x": 50.0,
  "cpu_millicores": 600.0,
  "ram_mb": 264.0,
  "net_mbps": 6.0,
  "formulas": { ... }
}
```

### Inspect current state

```bash
curl http://192.168.2.2:30081/status
```

### Reconfigure with a new input

Just POST again — the worker stops the old emulation before starting the
new one.

```bash
curl -X POST http://192.168.2.2:30081/configure \
  -H 'Content-Type: application/json' \
  -d '{"x":10,"cpu":{"a":20,"b":50},"ram":{"a":1,"b":10},"net":{"a":0.05,"b":0}}'
```

### Stop the emulation

```bash
curl -X POST http://192.168.2.2:30081/stop
```

---

## Step 4 — Verify the emulation actually works

Watch the worker pod's real resource usage and confirm it matches the
formula output.

```bash
# in one terminal: tail the worker's logs
microk8s kubectl logs -f worker

# in another: watch actual pod resource usage
watch -n 2 microk8s kubectl top pod worker
```

Send the `x=50` request above (`cpu: {a:10, b:100}` → 600m). Within a few
seconds `kubectl top` should show the worker pod's CPU usage climbing to
roughly **600m** and memory to **~264 MiB**. After `/stop`, both should
drop back near idle.

Note: `kubectl top pod` reports CPU in millicores natively, so the
worker's `cpu_millicores` value should map directly onto what you see
there — the whole point of using this unit.

---

## Endpoints reference

### Controller (NodePort 30081)

| Method | Path         | Body                            | Purpose                      |
|--------|--------------|---------------------------------|------------------------------|
| POST   | `/configure` | full JSON config (see above)    | start / replace emulation    |
| GET    | `/status`    | —                               | fetch worker's current state |
| POST   | `/stop`      | —                               | clear emulation              |
| GET    | `/healthz`   | —                               | liveness                     |

### Worker (ClusterIP, port 8080 — usually accessed via controller)

Same endpoints. Reachable from inside the cluster as
`http://worker-service:8080`. Useful for debugging:

```bash
microk8s kubectl exec controller -- \
  curl -s http://worker-service:8080/status
```

---

## Iterating

When you change the code:

```bash
# rebuild and push
cd worker && docker build -t jp36/emulator-worker:latest . && docker push jp36/emulator-worker:latest && cd ..

# force MicroK8s to pull the new image
microk8s kubectl delete pod worker
microk8s kubectl apply -f manifests/emulator.yaml
```

The `delete pod` step is needed because the `:latest` tag is cached — even
with `imagePullPolicy: Always`, the pod itself has to restart to trigger a
new pull.

---

## Troubleshooting

**`ImagePullBackOff`** — Image isn't on Docker Hub, the repo is private, or
the image name in `manifests/emulator.yaml` doesn't match your Docker Hub
username. Check with `microk8s kubectl describe pod worker`.

**`localhost:30081` connection refused** — Either the pod isn't ready
(`microk8s kubectl get pods`), or you're on a remote MicroK8s host. In that
case use that host's IP instead of `localhost`.

**`kubectl top` reports `metrics not available`** — Enable the addon:
`microk8s enable metrics-server` and wait ~60s.

**CPU readings way below target** — The worker's busy-loop is coarse and
caps at the pod's CPU limit (`2000m` in the manifest). If `kubectl top`
shows much less than your `cpu_millicores` value, either your target
exceeds the limit, or you need higher fidelity — swap the busy-loop for
`stress-ng --cpu <N>` for a more accurate burn.

**Pod restarts during emulation** — Memory limit is `1Gi` by default; if
you ask for more RAM than that the pod will OOM. Raise `resources.limits`
in the manifest, or lower your `ram.a`/`ram.b` values.

---

## Local sanity check (no Kubernetes)

For fast iteration on the application logic:

```bash
# terminal 1 — worker
python3 worker/worker.py

# terminal 2 — controller, pointed at local worker
WORKER_URL=http://127.0.0.1:8080 python3 controller/controller.py

# terminal 3 — drive it
curl -X POST http://127.0.0.1:8081/configure \
  -H 'Content-Type: application/json' \
  -d '{"x":10,"cpu":{"a":20,"b":50},"ram":{"a":1,"b":10},"net":{"a":0.05,"b":0}}'
curl http://127.0.0.1:8081/status
```

---

## What's a prototype here vs. what should grow

* The CPU burner is a coarse busy-loop, accurate to maybe ±5%. For
  publication-grade fidelity, switch to `stress-ng --cpu-load <pct>` or
  drive cgroup `cpu.max` directly.
* RAM is allocated and touched once. For pressure that exercises the
  kernel allocator, churn the buffer periodically.
* "Network" is loopback traffic — fine for measurement, but for real edge
  emulation pair it with `tc qdisc` policies and a remote peer.
* The controller is currently a pass-through. The natural next step is to
  let it hold a *topology* (multiple workers, dependencies) and configure
  them in concert.