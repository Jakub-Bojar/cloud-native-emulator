"""
Shared Kubernetes API client.

Used by the template materialiser to create and patch
Deployments/ConfigMaps/Services dynamically from a template, and by the
/api/v1 read endpoints to list pods.

Authenticates via the in-pod ServiceAccount token mounted at
/var/run/secrets/kubernetes.io/serviceaccount/ and trusts the cluster CA
from the same directory. Outside a pod this module won't work — the
controller is not runnable off-cluster.
"""

import json
import logging
import os
import ssl
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
TOKEN_PATH = f"{SA_DIR}/token"
CA_PATH = f"{SA_DIR}/ca.crt"
NS_PATH = f"{SA_DIR}/namespace"

# Cache the namespace and SSL context for the life of the process. The
# token is re-read on every call because the kubelet rotates it.
_NAMESPACE: str | None = None
_SSL_CTX: ssl.SSLContext | None = None


def _read(path: str) -> str:
    with open(path) as f:
        return f.read().strip()


def namespace() -> str:
    global _NAMESPACE
    if _NAMESPACE is None:
        _NAMESPACE = _read(NS_PATH)
    return _NAMESPACE


def ssl_context() -> ssl.SSLContext:
    global _SSL_CTX
    if _SSL_CTX is None:
        _SSL_CTX = ssl.create_default_context(cafile=CA_PATH)
    return _SSL_CTX


def api_base() -> str:
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS",
                          os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
    return f"https://{host}:{port}"


def request(method: str, path: str, body: bytes | None = None,
            content_type: str = "application/json",
            timeout: float = 10.0) -> tuple[int, bytes]:
    """Low-level call. `path` starts with `/api/...` or `/apis/...`."""
    url = f"{api_base()}{path}"
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {_read(TOKEN_PATH)}")
    if body is not None:
        req.add_header("Content-Type", content_type)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context()) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError as e:
        return 502, json.dumps({"error": f"k8s api unreachable: {e.reason}"}).encode()


def get(path: str) -> tuple[int, bytes]:
    return request("GET", path)


def post(path: str, body: dict) -> tuple[int, bytes]:
    return request("POST", path, json.dumps(body).encode())


def patch(path: str, body: dict,
          content_type: str = "application/strategic-merge-patch+json") -> tuple[int, bytes]:
    return request("PATCH", path, json.dumps(body).encode(), content_type=content_type)


def put(path: str, body: dict) -> tuple[int, bytes]:
    return request("PUT", path, json.dumps(body).encode())


def delete(path: str) -> tuple[int, bytes]:
    return request("DELETE", path)
