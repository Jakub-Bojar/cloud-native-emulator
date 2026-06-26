#!/usr/bin/env bash
#
# Run the merged controller ON THE HOST (off-cluster).
#
# This is the single front door: it drives Multipass to provision sites AND
# talks to the cluster (via kubeconfig) to materialise apps. It REPLACES the
# in-cluster controller Deployment, so this script scales that down first to
# avoid two controllers fighting over the same resources.
#
# Prereqs: multipass, the repo's .venv (controller deps installed).
# Usage:   provision/run-controller-host.sh   [--keep-incluster]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
CP="${CONTROL_PLANE_VM:-microk8s-vm}"
NS="${K8S_NAMESPACE:-cloud-native-emulator}"
KCFG="${KUBECONFIG:-$HOME/.kube/emulator-config}"

# 1. Fetch a kubeconfig from the control plane (admin creds → client cert).
echo "Fetching kubeconfig from $CP → $KCFG"
mkdir -p "$(dirname "$KCFG")"
multipass exec "$CP" -- microk8s config > "$KCFG"
chmod 600 "$KCFG"

# 2. Retire the in-cluster controller so they don't conflict (state lives in
#    the cluster, so the host controller picks up any deployed topology).
if [[ "${1:-}" != "--keep-incluster" ]]; then
  echo "Scaling down the in-cluster controller Deployment"
  multipass exec "$CP" -- microk8s kubectl scale deploy/controller \
    -n "$NS" --replicas=0 || true
fi

# 3. Environment for off-cluster operation.
export KUBECONFIG="$KCFG"
export K8S_NAMESPACE="$NS"
# MicroK8s' self-signed CA omits a key-usage extension that modern Python's TLS
# rejects, so skip verification for the local apiserver. Safe on a single
# trusted machine; set K8S_INSECURE=0 if you wire up a proper CA.
export K8S_INSECURE="${K8S_INSECURE:-1}"
export PROM_URL="${PROM_URL:-http://192.168.2.2:30090}"   # Prometheus NodePort
GRAFANA_URL="${GRAFANA_URL:-http://192.168.2.2:30300}"    # Grafana NodePort (banner only)
export SITE_ID="${SITE_ID:-local}"
export SITE_TIER="${SITE_TIER:-cloud}"
export TZ="${TZ:-Europe/London}"
export CONTROL_PLANE_VM="$CP"
export NODE_IMAGE="${NODE_IMAGE:-22.04}"
export BLUEPRINT_PATH="${BLUEPRINT_PATH:-$REPO/manifests/worker-template.yaml}"
export CONTROLLER_PORT="${CONTROLLER_PORT:-8081}"

# 4. Launch. cd into controller/ so its flat intra-package imports resolve.
#    Binds 0.0.0.0 (all interfaces) but browse it via localhost — the bare URL
#    now redirects to /docs instead of 404ing.
echo "Controller (host), namespace $NS — open:"
echo "    API docs    http://localhost:$CONTROLLER_PORT/docs"
echo "    Grafana     $GRAFANA_URL/"
echo "    Prometheus  $PROM_URL/"
cd "$REPO/controller"
exec "$REPO/.venv/bin/uvicorn" app:app --host 0.0.0.0 --port "$CONTROLLER_PORT"
