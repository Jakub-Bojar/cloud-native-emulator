# Runbook

Operational reference for the cloud-native emulator: build, deploy, drive,
observe, and tear down. Run everything from the repo root unless noted.

Replace `jp36` with your Docker Hub username and `192.168.2.2` with your
MicroK8s host IP throughout.

---

## Build & push images

```bash
# Worker
docker build -t jp36/emulator-worker:latest ./worker
docker push  jp36/emulator-worker:latest

# Controller
docker build -t jp36/emulator-controller:latest ./controller
docker push  jp36/emulator-controller:latest
```

---

## Deploy / redeploy

First-time apply (creates ConfigMap, Deployment, Services, ServiceAccount,
Role, RoleBinding):

```bash
microk8s kubectl apply -f manifests/worker.yaml
microk8s kubectl apply -f manifests/controller.yaml
```

After rebuilding an image, delete the pod(s) so the new image gets pulled:

```bash
kubectl -n cloud-native-emulator rollout restart deploy/worker

microk8s kubectl delete pod -l app=worker
microk8s kubectl delete pod controller --ignore-not-found
```

After editing manifests but keeping the same image:

```bash
microk8s kubectl apply -f manifests/worker.yaml -f manifests/controller.yaml
```

---

## Scale workers

```bash
microk8s kubectl scale deployment/worker --replicas=3
microk8s kubectl get pods -l app=worker
```

---

## Drive the emulator

Apply a config through the controller:

```bash
curl -X POST http://192.168.2.2:30081/configure \
  -H 'Content-Type: application/json' \
  -d '{"x":50,"cpu":{"a":10,"b":100},"ram":{"a":4,"b":64},"net":{"a":0.1,"b":1}}'
```

Stop the load (writes zeros into the ConfigMap):

```bash
curl -X POST http://192.168.2.2:30081/stop
```

Read current state (proxied from one worker):

```bash
curl http://192.168.2.2:30081/status
```

### Declarative alternative (bypass the controller)

```bash
# Edit live
microk8s kubectl edit configmap worker-config

# Or apply from a file
microk8s kubectl apply -f my-config.yaml
```

A `kubectl edit`/`apply` can take up to ~60s to propagate into pods (kubelet's
mount refresh cadence). Going through the controller's `/configure` is faster
in practice.

---

## Observe

```bash
# All pods at a glance
microk8s kubectl get pods,svc,configmap

# Live resource usage per worker replica
microk8s kubectl top pods -l app=worker

# Tail logs from every worker replica at once
microk8s kubectl logs -l app=worker --tail=50 --max-log-requests=10 -f

# Controller logs (single pod)
microk8s kubectl logs -f controller

# After a config change, confirm every replica reacted
for p in $(microk8s kubectl get pods -l app=worker -o name); do
  echo "=== $p ==="
  microk8s kubectl logs "$p" | grep -E "Configuring|Emulation" | tail -3
done
```

---

## Diagnostics

```bash
# Confirm the new image is what's running
microk8s kubectl describe pod -l app=worker | grep -E "Image:|Image ID:"

# Confirm the controller is running as the right ServiceAccount
microk8s kubectl get pod controller -o jsonpath='{.spec.serviceAccountName}'

# Check RBAC permits the ConfigMap patch
microk8s kubectl auth can-i patch configmap/worker-config \
  --as=system:serviceaccount:default:controller-sa

# Pull the mounted file out of a worker (microk8s kubectl exec mishandles `--`)
microk8s kubectl cp <pod-name>:/etc/emulator/config.json /tmp/current-config.json
cat /tmp/current-config.json

# Per-worker /status (round-robin via Service won't tell you which one answered)
microk8s kubectl get pods -l app=worker -o wide   # note each pod IP
microk8s kubectl exec controller -- curl -s http://<pod-ip>:8080/status
```

---

## Cleanup

```bash
microk8s kubectl delete -f manifests/worker.yaml -f manifests/controller.yaml
```
