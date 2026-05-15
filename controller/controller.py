"""
Controller pod.

Acts as the front door for the emulation. The user (or an experiment
driver) POSTs JSON to this pod describing how the worker should behave.
Instead of forwarding directly to the worker over HTTP, the controller
writes the JSON into the `worker-config` ConfigMap via the Kubernetes
API; kubelet then refreshes the mounted file inside the worker pod, and
the worker reacts via its filesystem watcher.

Separating "what to do" (controller) from "how to do it" (worker) lets
many workers be addressed through a single declarative resource, and
makes the configuration trail visible to anything that watches the
cluster (kubectl, GitOps tooling, audit logs, etc).

Endpoints
---------
POST /configure   validate + write JSON into worker-config ConfigMap
GET  /status      proxy worker's current state (still over HTTP)
POST /stop        write zero-load config into worker-config ConfigMap
GET  /healthz     liveness for k8s
"""

import json
import os
import ssl
import logging
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [controller] %(message)s",
)
log = logging.getLogger(__name__)

WORKER_URL = os.environ.get("WORKER_URL", "http://worker-service:8080")
CONFIGMAP_NAME = os.environ.get("CONFIGMAP_NAME", "worker-config")
CONFIGMAP_KEY = os.environ.get("CONFIGMAP_KEY", "config.json")

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
TOKEN_PATH = f"{SA_DIR}/token"
CA_PATH = f"{SA_DIR}/ca.crt"
NS_PATH = f"{SA_DIR}/namespace"

ZERO_CONFIG = {
    "x": 0,
    "cpu": {"a": 0, "b": 0},
    "ram": {"a": 0, "b": 0},
    "net": {"a": 0, "b": 0},
}


def _read_sa_file(path: str) -> str:
    with open(path, "r") as f:
        return f.read().strip()


def _api_base() -> str:
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS",
                          os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
    return f"https://{host}:{port}"


def _ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=CA_PATH)
    return ctx


def patch_configmap(payload: dict) -> tuple[int, bytes]:
    """Strategic-merge-patch the worker ConfigMap with the new config.json."""
    namespace = _read_sa_file(NS_PATH)
    token = _read_sa_file(TOKEN_PATH)
    url = (f"{_api_base()}/api/v1/namespaces/{namespace}"
           f"/configmaps/{CONFIGMAP_NAME}")

    body = json.dumps({
        "data": {CONFIGMAP_KEY: json.dumps(payload)}
    }).encode()

    req = urllib.request.Request(url, data=body, method="PATCH")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/strategic-merge-patch+json")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10, context=_ssl_context()) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError as e:
        return 502, json.dumps({"error": f"k8s api unreachable: {e.reason}"}).encode()


def fetch_worker_status() -> tuple[int, bytes]:
    url = f"{WORKER_URL}/status"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError as e:
        return 502, json.dumps({"error": f"worker unreachable: {e.reason}"}).encode()


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._send(200, b'{"ok":true}')
        elif self.path == "/status":
            code, body = fetch_worker_status()
            self._send(code, body)
        else:
            self._send(404, b'{"error":"not found"}')

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        if self.path == "/configure":
            try:
                payload = json.loads(raw)
                for key in ("x", "cpu", "ram", "net"):
                    if key not in payload:
                        raise KeyError(key)
                for subKey in ("cpu", "ram", "net"):
                    if "a" not in payload[subKey] or "b" not in payload[subKey]:
                        raise KeyError(f"{subKey}.a/b")
            except (json.JSONDecodeError, KeyError) as e:
                self._send(400, json.dumps({"error": f"bad payload: {e}"}).encode())
                return
            log.info("Writing configure into ConfigMap: %s", payload)
            code, body = patch_configmap(payload)
            self._send(code, body)

        elif self.path == "/stop":
            log.info("Writing zero-load config into ConfigMap")
            code, body = patch_configmap(ZERO_CONFIG)
            self._send(code, body)

        else:
            self._send(404, b'{"error":"not found"}')

    def log_message(self, fmt, *args):
        log.info("HTTP %s", fmt % args)


def main():
    port = int(os.environ.get("CONTROLLER_PORT", "8081"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Controller listening on 0.0.0.0:%d (configmap=%s)",
             port, CONFIGMAP_NAME)
    server.serve_forever()


if __name__ == "__main__":
    main()
