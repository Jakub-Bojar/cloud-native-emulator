"""
Prometheus HTTP API client.

The controller already scrapes worker /metrics directly for instantaneous
values (see graph.py). This module adds the complementary capability:
windowed PromQL instant queries via Prometheus's HTTP API, so the /api/v1
summary endpoint can report CPU/RAM/net averaged over a time window rather
than only the current snapshot.

Prometheus is reached over plain HTTP at PROM_URL. With kube-prometheus-stack
the in-cluster service is typically
`http://<release>-kube-prometheus-stack-prometheus.<ns>.svc:9090`; the exact
name depends on the Helm release, so it is configurable via the PROM_URL env
var. Prometheus has no auth by default in-cluster, so no token is needed —
only network reachability across namespaces (fine on MicroK8s, which has no
default NetworkPolicy).

Every call degrades gracefully: a query against an unreachable or erroring
Prometheus returns None / [] and logs a warning rather than raising, so the
read endpoints can still serve the k8s + live-scrape portion of their
response and simply flag prometheus.available = false.
"""

import json
import logging
import math
import os
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger(__name__)

DEFAULT_URL = ("http://kps-kube-prometheus-stack-prometheus"
               ".monitoring.svc:9090")
TIMEOUT_S = 5.0


def url() -> str:
    return os.environ.get("PROM_URL", DEFAULT_URL)


def _get(path: str, params: dict) -> dict | None:
    full = f"{url()}{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(full, timeout=TIMEOUT_S) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        log.warning("prometheus query failed (%s): %s", path, e)
        return None


def instant(query: str) -> dict | None:
    """Run an instant PromQL query. Returns the raw Prometheus JSON, or None."""
    return _get("/api/v1/query", {"query": query})


def instant_scalar(query: str) -> float | None:
    """Run an instant query expected to reduce to a single number and return
    that number, or None.

    Used for summary aggregations like `avg_over_time(sum(metric)[15m:])`,
    which evaluate to a one-element instant vector. NaN/Inf collapse to None
    so the result is always valid JSON."""
    data = instant(query)
    if not data or data.get("status") != "success":
        return None
    result = data.get("data", {}).get("result", [])
    if not result:
        return None
    try:
        raw = result[0]["value"][1]
    except (KeyError, IndexError, TypeError):
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def available() -> bool:
    """True if Prometheus answers a trivial instant query. Cheap liveness."""
    data = instant("vector(1)")
    return bool(data and data.get("status") == "success")


def health() -> dict:
    """Small block embedded in API responses so callers know whether the
    time-series portion of the data is trustworthy."""
    return {"available": available(), "url": url()}
