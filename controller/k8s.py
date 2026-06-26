"""
Shared Kubernetes API client.

Used by the template materialiser to create and patch
Deployments/ConfigMaps/Services dynamically from a template, and by the
/api/v1 read endpoints to list pods.

Two modes, picked automatically:

- In-cluster (running as a pod): authenticate via the in-pod ServiceAccount
  token mounted at /var/run/secrets/kubernetes.io/serviceaccount/ and trust
  the cluster CA from the same directory.
- Off-cluster (running on the host, e.g. the merged controller): authenticate
  via a kubeconfig (KUBECONFIG env or ~/.kube/config) — server URL, CA, and
  either a bearer token or client cert/key. This is what lets the controller
  run on the Mac host so it can also drive Multipass. Set K8S_INSECURE=1 to
  skip TLS verification if the apiserver cert lacks the host IP in its SAN.

Mode is decided by whether the in-pod token file exists.
"""

import base64
import json
import logging
import os
import ssl
import tempfile
import urllib.error
import urllib.request

import yaml

log = logging.getLogger(__name__)

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
TOKEN_PATH = f"{SA_DIR}/token"
CA_PATH = f"{SA_DIR}/ca.crt"
NS_PATH = f"{SA_DIR}/namespace"

# True when running on the host rather than as a pod (no SA token mounted).
OFF_CLUSTER = not os.path.exists(TOKEN_PATH)

# Cache the namespace and SSL context for the life of the process. The
# in-cluster token is re-read on every call because the kubelet rotates it;
# the off-cluster kubeconfig is parsed once.
_NAMESPACE: str | None = None
_SSL_CTX: ssl.SSLContext | None = None
_OFFCONF: dict | None = None


def _read(path: str) -> str:
    with open(path) as f:
        return f.read().strip()


def _offconf() -> dict:
    """Parse the kubeconfig once into {server, ssl, token, namespace}."""
    global _OFFCONF
    if _OFFCONF is not None:
        return _OFFCONF
    path = os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
    with open(path) as f:
        cfg = yaml.safe_load(f)

    def _pick(items: list, key: str, name: str) -> dict:
        for it in items:
            if it.get("name") == name:
                return it[key]
        raise RuntimeError(f"kubeconfig: no {key} named {name!r}")

    ctx = _pick(cfg["contexts"], "context", cfg["current-context"])
    cluster = _pick(cfg["clusters"], "cluster", ctx["cluster"])
    user = _pick(cfg["users"], "user", ctx["user"])

    if os.environ.get("K8S_INSECURE") or cluster.get("insecure-skip-tls-verify"):
        ctx_ssl = ssl.create_default_context()
        ctx_ssl.check_hostname = False
        ctx_ssl.verify_mode = ssl.CERT_NONE
    elif cluster.get("certificate-authority-data"):
        pem = base64.b64decode(cluster["certificate-authority-data"]).decode()
        ctx_ssl = ssl.create_default_context(cadata=pem)
    elif cluster.get("certificate-authority"):
        ctx_ssl = ssl.create_default_context(cafile=cluster["certificate-authority"])
    else:
        ctx_ssl = ssl.create_default_context()

    token = user.get("token")
    if not token and user.get("client-certificate-data") and user.get("client-key-data"):
        # urllib needs cert/key on disk; write them to temp files for the
        # process lifetime (admin creds — keep them out of the repo).
        cert = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
        cert.write(base64.b64decode(user["client-certificate-data"])); cert.close()
        key = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
        key.write(base64.b64decode(user["client-key-data"])); key.close()
        ctx_ssl.load_cert_chain(cert.name, key.name)
    elif not token and user.get("client-certificate") and user.get("client-key"):
        ctx_ssl.load_cert_chain(user["client-certificate"], user["client-key"])

    ns = ctx.get("namespace") or os.environ.get("K8S_NAMESPACE") or "default"
    _OFFCONF = {"server": cluster["server"].rstrip("/"), "ssl": ctx_ssl,
                "token": token, "namespace": ns}
    log.info("k8s: off-cluster mode, apiserver %s (namespace %s)",
             _OFFCONF["server"], ns)
    return _OFFCONF


def namespace() -> str:
    global _NAMESPACE
    if _NAMESPACE is None:
        _NAMESPACE = _offconf()["namespace"] if OFF_CLUSTER else _read(NS_PATH)
    return _NAMESPACE


def ssl_context() -> ssl.SSLContext:
    global _SSL_CTX
    if _SSL_CTX is None:
        _SSL_CTX = (_offconf()["ssl"] if OFF_CLUSTER
                    else ssl.create_default_context(cafile=CA_PATH))
    return _SSL_CTX


def api_base() -> str:
    if OFF_CLUSTER:
        return _offconf()["server"]
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS",
                          os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
    return f"https://{host}:{port}"


def _auth_header() -> str | None:
    """Bearer token for the current mode, or None (client-cert auth)."""
    if OFF_CLUSTER:
        tok = _offconf()["token"]
        return f"Bearer {tok}" if tok else None
    return f"Bearer {_read(TOKEN_PATH)}"


def request(method: str, path: str, body: bytes | None = None,
            content_type: str = "application/json",
            timeout: float = 10.0) -> tuple[int, bytes]:
    """Low-level call. `path` starts with `/api/...` or `/apis/...`."""
    url = f"{api_base()}{path}"
    req = urllib.request.Request(url, data=body, method=method)
    auth = _auth_header()
    if auth:
        req.add_header("Authorization", auth)
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


def delete(path: str) -> tuple[int, bytes]:
    return request("DELETE", path)
