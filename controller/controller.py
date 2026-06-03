"""
Controller pod.

Front door for the emulation. Materialises templated multi-worker
topologies: POST /templates validates the JSON, then the materialiser
creates one Deployment + ConfigMap + Service per role declared in the
template and writes each role's load formulas + peer list into its
ConfigMap. DELETE /templates/<name> tears the whole topology down.

Endpoints
---------
POST   /templates          materialise a posted template
GET    /templates          list names of currently materialised templates
GET    /templates/<name>   inspect a materialised template (peers, replicas)
PATCH  /templates/<name>   partial update — merge, re-resolve, re-materialise
DELETE /templates/<name>   tear down a materialised template
GET    /graph/<name>       Grafana Node Graph payload (nodes + measured edges);
                           ?view=pods for per-pod nodes (default: per-role)
GET    /health            liveness for k8s

Unified site API (see api.py / API.md)
--------------------------------------
GET    /api/v1/overview                              site-wide snapshot
GET    /api/v1/templates/<name>/status               fused k8s + live metrics
GET    /api/v1/templates/<name>/summary              CPU/RAM/net averaged over a window
POST   /api/v1/templates/<name>/roles/<role>/scale   scale a role's replicas
"""

import json
import os
import logging
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

import api
import graph
import k8s
import materialiser
import watcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [controller] %(message)s",
)
log = logging.getLogger(__name__)


# Allowed CORS origin for browser clients (Swagger UI, Grafana, etc.). "*"
# is fine for a dev/research tool on a trusted network; set CORS_ALLOW_ORIGIN
# to a specific origin to lock it down.
CORS_ORIGIN = os.environ.get("CORS_ALLOW_ORIGIN", "*")


class Handler(BaseHTTPRequestHandler):
    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", CORS_ORIGIN)
        self.send_header("Access-Control-Allow-Methods",
                         "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send(self, code, body, content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code: int, obj) -> None:
        self._send(code, json.dumps(obj).encode())

    def do_OPTIONS(self):
        # CORS preflight for non-simple requests (POST/PATCH/DELETE with a
        # JSON body). Browsers send this before the real call.
        self.send_response(204)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self._send(200, b'{"ok":true}')
        elif self.path == "/templates":
            try:
                names = materialiser.list_managed()
            except Exception as e:
                log.exception("list_managed failed")
                self._send_json(502, {"error": f"list failed: {e}"})
                return
            self._send_json(200, {"templates": names})
        elif self.path.startswith("/templates/"):
            name = urllib.parse.urlparse(self.path).path[len("/templates/"):]
            if "/" in name or not name:
                self._send_json(400, {"error": "expected /templates/<name>"})
                return
            try:
                info = materialiser.get_managed(name)
            except Exception as e:
                log.exception("get_managed failed")
                self._send_json(502, {"error": str(e)})
                return
            if info is None:
                self._send_json(404, {"error": f"no template named {name!r}"})
                return
            self._send_json(200, info)
        elif self.path.startswith("/graph/"):
            parsed = urllib.parse.urlparse(self.path)
            name = parsed.path[len("/graph/"):]
            if "/" in name or not name:
                self._send_json(400, {"error": "expected /graph/<name>"})
                return
            view = (urllib.parse.parse_qs(parsed.query).get("view", ["role"])[0] or "role").lower()
            by_pod = view in ("pod", "pods")
            try:
                payload = graph.build_graph(name, by_pod=by_pod)
            except Exception as e:
                log.exception("build_graph failed")
                self._send_json(502, {"error": str(e)})
                return
            if payload is None:
                self._send_json(404, {"error": f"no template named {name!r}"})
                return
            self._send_json(200, payload)
        elif self.path.startswith("/api/v1/"):
            self._handle_api_get(self.path)
        else:
            self._send(404, b'{"error":"not found"}')

    def _handle_api_get(self, raw_path: str) -> None:
        parsed = urllib.parse.urlparse(raw_path)
        parts = [p for p in parsed.path.split("/") if p]  # e.g. ['api','v1',…]
        qs = urllib.parse.parse_qs(parsed.query)
        try:
            if parts == ["api", "v1", "overview"]:
                self._send_json(200, api.overview())
                return
            # /api/v1/templates/<name>/{status,summary}
            if (len(parts) == 5 and parts[:3] == ["api", "v1", "templates"]):
                name, leaf = parts[3], parts[4]
                if leaf == "status":
                    data = api.template_status(name)
                elif leaf == "summary":
                    res_q = qs.get("resources", [None])[0]
                    resources = ([r.strip() for r in res_q.split(",") if r.strip()]
                                 if res_q else None)
                    by_role = qs.get("by_role", ["false"])[0].lower() in (
                        "1", "true", "yes")
                    include_x = qs.get("include_x", ["false"])[0].lower() in (
                        "1", "true", "yes")
                    data = api.template_summary(
                        name,
                        qs.get("range", ["15m"])[0],
                        resources=resources,
                        by_role=by_role,
                        include_x=include_x,
                    )
                else:
                    self._send_json(404, {"error": "not found"})
                    return
                if data is None:
                    self._send_json(404, {"error": f"no template named {name!r}"})
                    return
                self._send_json(200, data)
                return
            self._send_json(404, {"error": "not found"})
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
        except Exception as e:
            log.exception("api GET %s failed", raw_path)
            self._send_json(502, {"error": str(e)})

    def _handle_api_post(self, raw_path: str, raw_body: bytes) -> None:
        parsed = urllib.parse.urlparse(raw_path)
        parts = [p for p in parsed.path.split("/") if p]
        # /api/v1/templates/<name>/roles/<role>/scale
        if (len(parts) == 7 and parts[:3] == ["api", "v1", "templates"]
                and parts[4] == "roles" and parts[6] == "scale"):
            name, role = parts[3], parts[5]
            try:
                body = json.loads(raw_body) if raw_body else {}
            except json.JSONDecodeError as e:
                self._send_json(400, {"error": f"invalid JSON: {e}"})
                return
            if not isinstance(body, dict):
                self._send_json(400, {"error": "body must be a JSON object"})
                return
            try:
                result = api.scale_role(name, role,
                                        replicas=body.get("replicas"))
            except ValueError as e:
                self._send_json(400, {"error": str(e)})
                return
            except RuntimeError as e:
                log.exception("scale: k8s API failure")
                self._send_json(502, {"error": str(e)})
                return
            except Exception as e:
                log.exception("scale raised unexpected error")
                self._send_json(500, {"error": str(e)})
                return
            if result is None:
                self._send_json(404, {"error": f"no template named {name!r}"})
                return
            self._send_json(200, result)
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        if self.path == "/templates":
            try:
                template = json.loads(raw)
            except json.JSONDecodeError as e:
                self._send_json(400, {"error": f"invalid JSON: {e}"})
                return
            try:
                materialiser.validate(template)
            except ValueError as e:
                self._send_json(400, {"error": str(e)})
                return
            log.info("Materialising template %s", template.get("name"))
            try:
                materialiser.materialise(template)
            except RuntimeError as e:
                # k8s API rejected one of the create/patch calls. Partial
                # materialisation is possible; teardown by name to clean up.
                log.exception("materialise failed")
                self._send_json(502, {"error": str(e)})
                return
            except Exception as e:
                log.exception("materialise raised unexpected error")
                self._send_json(500, {"error": str(e)})
                return
            self._send_json(201, {
                "name": template["name"],
                "roles": list(template["roles"].keys()),
                "peers": materialiser.compute_peers(template),
            })

        elif self.path.startswith("/api/v1/"):
            self._handle_api_post(self.path, raw)

        else:
            self._send(404, b'{"error":"not found"}')

    def do_PATCH(self):
        prefix = "/templates/"
        path = urllib.parse.urlparse(self.path).path
        if not (path.startswith(prefix) and len(path) > len(prefix)):
            self._send(404, b'{"error":"not found"}')
            return
        name = path[len(prefix):]
        if "/" in name or not name:
            self._send_json(400, {"error": "expected /templates/<name>"})
            return
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            patch = json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            self._send_json(400, {"error": f"invalid JSON: {e}"})
            return
        if not isinstance(patch, dict):
            self._send_json(400, {"error": "PATCH body must be a JSON object"})
            return
        try:
            merged = materialiser.patch_template(name, patch)
        except ValueError as e:
            # Either the merged template failed validation (bad field,
            # cycle, missing required key) or we surfaced a structural
            # issue from the merge — all client-side problems → 400.
            self._send_json(400, {"error": str(e)})
            return
        except RuntimeError as e:
            log.exception("patch_template: k8s API failure")
            self._send_json(502, {"error": str(e)})
            return
        except Exception as e:
            log.exception("patch_template raised unexpected error")
            self._send_json(500, {"error": str(e)})
            return
        if merged is None:
            self._send_json(404, {"error": f"no template named {name!r}"})
            return
        self._send_json(200, {
            "name": name,
            "template": merged,
            "peers": materialiser.compute_peers(merged),
        })

    def do_DELETE(self):
        prefix = "/templates/"
        path = urllib.parse.urlparse(self.path).path
        if path.startswith(prefix) and len(path) > len(prefix):
            name = path[len(prefix):]
            if "/" in name or not name:
                self._send_json(400, {"error": "expected /templates/<name>"})
                return
            log.info("Tearing down template %s", name)
            try:
                deleted = materialiser.teardown(name)
            except Exception as e:
                log.exception("teardown failed")
                self._send_json(502, {"error": str(e)})
                return
            self._send_json(200, {"name": name, "deleted": deleted})
        else:
            self._send(404, b'{"error":"not found"}')

    def log_message(self, fmt, *args):
        log.info("HTTP %s", fmt % args)


def main():
    port = int(os.environ.get("CONTROLLER_PORT", "8081"))
    # Declarative ingestion: poll labelled ConfigMaps and reconcile them
    # via the same materialiser used by POST /templates.
    watcher.start()
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Controller listening on 0.0.0.0:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
