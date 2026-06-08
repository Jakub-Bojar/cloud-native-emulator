"""
Unified /api/v1 surface: query + control, in clean JSON.

This is the per-VM "site API". One controller runs per VM, and each VM plays
a role in a larger cloud system (edge / fog / cloud). Every response carries
a `site` block ({"id", "tier"}) so that a future federation gateway can fan
out to many controllers and merge their responses by site without any schema
change here. SITE_ID / SITE_TIER are set per controller via env.

Read endpoints fuse three data sources:
  - Kubernetes live state (Deployments → desired replicas; Pods → phase,
    readiness, restarts, node, age) via the k8s API.
  - Instantaneous worker gauges scraped straight from each pod's /metrics
    (reusing graph.scrape_topology — no Prometheus dependency).
  - Historical time series via the Prometheus HTTP API (prom.py).

The endpoint routing lives in controller.py; this module holds the logic.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import quote

import graph
import k8s
import materialiser
import prom

log = logging.getLogger(__name__)

# Worker Prometheus gauge name → friendly key used in our JSON responses.
_GAUGE_MAP = {
    "worker_input_x": "x",
    "worker_target_cpu_millicores": "target_cpu_millicores",
    "worker_actual_cpu_millicores": "actual_cpu_millicores",
    "worker_target_ram_mb": "target_ram_mb",
    "worker_actual_ram_mb": "actual_ram_mb",
    "worker_target_net_mbps": "target_net_mbps",
    "worker_actual_net_mbps": "actual_net_mbps",
}


def site_block() -> dict:
    """Identity of this controller's site. Tags every response so a fleet-wide
    gateway can attribute and merge data across VMs later."""
    return {
        "id": os.environ.get("SITE_ID", "local"),
        "tier": os.environ.get("SITE_TIER", "unknown"),
    }


# ----------------------------------------------------------------------------
# Kubernetes pod state
# ----------------------------------------------------------------------------

def _age_seconds(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc)
        return int((datetime.now(timezone.utc) - dt).total_seconds())
    except ValueError:
        return None


def _list_pods(name: str) -> dict[str, dict]:
    """Live Pod state for template `name`, keyed by pod name.

    Returns {pod_name: {role, phase, ready, restarts, node, ip, age_seconds}}.
    Requires the controller Role to grant pods get/list (see
    manifests/controller.yaml)."""
    selector = (f"{materialiser.MANAGED_BY_LABEL}={materialiser.MANAGED_BY_VALUE}"
                f",{materialiser.TEMPLATE_LABEL}={name}")
    path = (f"/api/v1/namespaces/{k8s.namespace()}/pods"
            f"?labelSelector={quote(selector)}")
    status, body = k8s.get(path)
    out: dict[str, dict] = {}
    if status != 200:
        log.warning("list pods for %s: status=%s", name, status)
        return out
    for item in json.loads(body).get("items", []):
        meta = item.get("metadata", {})
        pod = meta.get("name")
        if not pod:
            continue
        spec = item.get("spec", {})
        st = item.get("status", {})
        cs = st.get("containerStatuses") or []
        restarts = sum(int(c.get("restartCount", 0)) for c in cs)
        ready = all(c.get("ready", False) for c in cs) if cs else False
        out[pod] = {
            "role": (meta.get("labels", {}) or {}).get(materialiser.ROLE_LABEL),
            "phase": st.get("phase"),
            "ready": ready,
            "restarts": restarts,
            "node": spec.get("nodeName"),
            "ip": st.get("podIP"),
            "age_seconds": _age_seconds(st.get("startTime")),
        }
    return out


# ----------------------------------------------------------------------------
# Read endpoints
# ----------------------------------------------------------------------------

def overview() -> dict:
    """Site-wide snapshot: every materialised template with per-role desired
    vs ready replica counts and a pod health rollup. The single place to see
    'what is running here'."""
    templates: list[dict] = []
    for nm in materialiser.list_managed():
        info = materialiser.get_managed(nm)
        if not info:
            continue
        desired = info.get("replicas", {}) or {}
        pods = _list_pods(nm)
        roles_summary: dict[str, dict] = {
            role: {"desired": count, "ready": 0}
            for role, count in desired.items()
        }
        total = ready_total = 0
        for pd in pods.values():
            total += 1
            if pd["ready"]:
                ready_total += 1
            role = pd.get("role")
            if role is None:
                continue
            roles_summary.setdefault(role, {"desired": 0, "ready": 0})
            if pd["ready"]:
                roles_summary[role]["ready"] += 1
        templates.append({
            "name": nm,
            "source": info.get("source"),
            "roles": roles_summary,
            "pods": {"total": total, "ready": ready_total},
        })
    return {
        "site": site_block(),
        "namespace": k8s.namespace(),
        "templates": templates,
        "prometheus": prom.health(),
    }


def _pod_metrics(rec: dict) -> dict:
    """Friendly-keyed metric snapshot from a scraped pod record."""
    return {friendly: rec.get(gauge) for gauge, friendly in _GAUGE_MAP.items()}


def template_status(name: str) -> dict | None:
    """Rich per-template status: each role's desired/ready replicas, per-pod
    k8s state fused with that pod's current worker gauges, role-level target
    and actual sums, and measured role→role edge traffic. None if no such
    template."""
    topo = graph.scrape_topology(name)
    if topo is None:
        return None
    roles = topo["roles"]
    replicas = topo["replicas"]
    pod_records = topo["pod_records"]
    role_pods = topo["role_pods"]
    k8s_pods = _list_pods(name)

    scrapes_by_role: dict[str, list[dict]] = {r: [] for r in roles}
    for rec in pod_records:
        scrapes_by_role.setdefault(rec["role"], []).append(rec)
    role_ips = {r: {ip for (_, ip) in pods} for r, pods in role_pods.items()}
    edge_records = graph.measure_role_edges(topo["edges_def"],
                                            scrapes_by_role, role_ips)

    roles_out: dict[str, dict] = {}
    for role in roles:
        scrapes = scrapes_by_role.get(role, [])
        scraped_names = {rec["pod"] for rec in scrapes}

        def _sum(gauge: str) -> float:
            return sum(rec.get(gauge, 0.0) or 0.0 for rec in scrapes)

        pods_out: list[dict] = []
        x_val = None
        for rec in scrapes:
            pod = rec["pod"]
            kp = k8s_pods.get(pod, {})
            metrics = _pod_metrics(rec)
            if x_val is None:
                x_val = metrics.get("x")
            pods_out.append({
                "name": pod,
                "ip": rec.get("ip") or kp.get("ip"),
                "node": kp.get("node"),
                "phase": kp.get("phase"),
                "ready": kp.get("ready", True),
                "restarts": kp.get("restarts"),
                "age_seconds": kp.get("age_seconds"),
                "metrics": metrics,
            })
        # Pods that exist in k8s but aren't in Endpoints yet (starting up,
        # not Ready) won't have been scraped — surface them so a scale-up in
        # progress is visible rather than silently missing.
        for pod, kp in k8s_pods.items():
            if kp.get("role") == role and pod not in scraped_names:
                pods_out.append({
                    "name": pod,
                    "ip": kp.get("ip"),
                    "node": kp.get("node"),
                    "phase": kp.get("phase"),
                    "ready": kp.get("ready", False),
                    "restarts": kp.get("restarts"),
                    "age_seconds": kp.get("age_seconds"),
                    "metrics": {},
                })

        roles_out[role] = {
            "desired": int(replicas.get(role, len(pods_out))),
            "ready": sum(1 for p in pods_out if p.get("ready")),
            "x": x_val,
            "targets": {
                "cpu_millicores": _sum("worker_target_cpu_millicores"),
                "ram_mb": _sum("worker_target_ram_mb"),
                "net_mbps": _sum("worker_target_net_mbps"),
            },
            "actuals": {
                "cpu_millicores": _sum("worker_actual_cpu_millicores"),
                "ram_mb": _sum("worker_actual_ram_mb"),
                "net_mbps": _sum("worker_actual_net_mbps"),
            },
            "pods": pods_out,
        }

    return {
        "site": site_block(),
        "name": name,
        "source": (topo["info"] or {}).get("source"),
        "roles": roles_out,
        "edges": [{"from": r["source"], "to": r["target"],
                   "mbps": round(r["mbps"], 3)} for r in edge_records],
        "prometheus": {"available": None},  # not queried on this endpoint
    }


# ----------------------------------------------------------------------------
# Summary: CPU / RAM / network averaged over a window
# ----------------------------------------------------------------------------

# resource key → (friendly output key + unit, actual gauge, target gauge).
_RESOURCE_METRICS = {
    "cpu": ("cpu_millicores", "worker_actual_cpu_millicores",
            "worker_target_cpu_millicores"),
    "ram": ("ram_mb", "worker_actual_ram_mb", "worker_target_ram_mb"),
    "net": ("net_mbps", "worker_actual_net_mbps", "worker_target_net_mbps"),
}
_DURATION_RE = re.compile(r"^\d+[smhd]$")


def _safe_duration(s: str) -> str:
    """Only allow a bare Prometheus duration so it can be embedded in a
    subquery range without injection. Falls back to 15m."""
    return s if isinstance(s, str) and _DURATION_RE.match(s) else "15m"


def _round(v):
    return round(v, 3) if isinstance(v, (int, float)) else v


def template_summary(name: str, range_str: str = "15m", resources=None,
                     by_role: bool = False, include_x: bool = False) -> dict | None:
    """Compact CPU/RAM/network summary for a template, averaged over a window.

    For each requested resource it reports the template-wide target and actual
    *summed across pods*, then reduced over the window: actual gets avg / min /
    max, target gets avg. With by_role=True it also breaks the per-resource
    averages down by role. With include_x=True an `x` block is added mapping
    each role to its resolved input x (averaged over the window). None if no
    such template; graceful degradation if Prometheus is unreachable.

    Raises ValueError on an unknown resource → caller maps to 400.
    """
    info = materialiser.get_managed(name)
    if info is None:
        return None

    if resources:
        unknown = [r for r in resources if r not in _RESOURCE_METRICS]
        if unknown:
            raise ValueError(
                f"unknown resource(s) {unknown}; valid: cpu, ram, net")
        wanted = list(dict.fromkeys(resources))  # de-dupe, keep order
    else:
        wanted = ["cpu", "ram", "net"]

    window = _safe_duration(range_str)
    envelope = {
        "site": site_block(),
        "name": name,
        "range": window,
    }
    roles = (info.get("template") or {}).get("roles") or {}

    if not prom.available():
        envelope["prometheus"] = {"available": False, "url": prom.url()}
        envelope["totals"] = {}
        if by_role:
            envelope["roles"] = {}
        if include_x:
            envelope["x"] = {}
        return envelope

    ns = k8s.namespace()

    def agg(metric: str, scope: str, fn: str):
        # Sum across pods first, then reduce over time via a subquery so the
        # number is "the whole role/template's usage, averaged over the window".
        return _round(prom.instant_scalar(
            f"{fn}(sum({metric}{{{scope}}})[{window}:])"))

    def block(scope: str, full: bool) -> dict:
        out: dict[str, dict] = {}
        for r in wanted:
            key, actual, target = _RESOURCE_METRICS[r]
            entry = {
                "target_avg": agg(target, scope, "avg_over_time"),
                "actual_avg": agg(actual, scope, "avg_over_time"),
            }
            if full:
                entry["actual_min"] = agg(actual, scope, "min_over_time")
                entry["actual_max"] = agg(actual, scope, "max_over_time")
            out[key] = entry
        return out

    envelope["totals"] = block(f'namespace="{ns}",pod=~"wt-{name}-.*"', full=True)
    if by_role:
        envelope["roles"] = {
            role: block(f'namespace="{ns}",pod=~"wt-{name}-{role}-.*"',
                        full=False)
            for role in roles
        }
    if include_x:
        # x is the same on every pod of a role, so average (not sum) across the
        # role's pods to recover that role's resolved input x.
        def x_of(role: str):
            scope = f'namespace="{ns}",pod=~"wt-{name}-{role}-.*"'
            return _round(prom.instant_scalar(
                f"avg_over_time(avg(worker_input_x{{{scope}}})[{window}:])"))
        envelope["x"] = {role: x_of(role) for role in roles}
    envelope["prometheus"] = {"available": True, "url": prom.url()}
    return envelope

