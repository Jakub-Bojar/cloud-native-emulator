"""
Controller pod.

Acts as the front door for the emulation. The user (or an experiment
driver) POSTs JSON to this pod describing how the worker should behave;
this pod forwards the request to the worker through its Kubernetes
Service DNS name.

Separating "what to do" (controller) from "how to do it" (worker) lets
many workers be addressed through a single stable endpoint, and lets
the experiment driver run from anywhere on the cluster network.

Endpoints
---------
POST /configure   forward JSON to worker's /configure
GET  /status      fetch worker's current state
POST /stop        tell worker to clear emulation
GET  /healthz     liveness for k8s
"""

import json
import os
import logging
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [controller] %(message)s",
)
log = logging.getLogger(__name__)

# Worker is addressed by its k8s Service DNS name. In-cluster this resolves
# automatically. For local testing override with WORKER_URL.
WORKER_URL = os.environ.get("WORKER_URL", "http://worker-service:8080")


def forwardToWorker(method: str, path: str, body: bytes | None = None):
    url = f"{WORKER_URL}{path}"
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError as e:
        return 502, json.dumps({"error": f"worker unreachable: {e.reason}"}).encode()
    except Exception as e:
        log.exception("forwardToWorker failed")
        return 502, json.dumps({"error": f"worker error: {e}"}).encode()


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
            code, body = forwardToWorker("GET", "/status")
            self._send(code, body)
        else:
            self._send(404, b'{"error":"not found"}')

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        if self.path == "/configure":
            # Light validation before we forward
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
            log.info("Forwarding configure: %s", payload)
            code, body = forwardToWorker("POST", "/configure", raw)
            self._send(code, body)

        elif self.path == "/stop":
            code, body = forwardToWorker("POST", "/stop", b"")
            self._send(code, body)

        else:
            self._send(404, b'{"error":"not found"}')

    def log_message(self, fmt, *args):
        log.info("HTTP %s", fmt % args)


def main():
    port = int(os.environ.get("CONTROLLER_PORT", "8081"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Controller listening on 0.0.0.0:%d (worker=%s)", port, WORKER_URL)
    server.serve_forever()


if __name__ == "__main__":
    main()
