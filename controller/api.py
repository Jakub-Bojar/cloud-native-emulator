"""
Unified api surface: query + control, in clean JSON.

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

The endpoint routing lives in app.py; this module holds the logic.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
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


# Timezone for the human-facing side of /measurements/range: naive request
# timestamps are interpreted in it and response timestamps are rendered in it
# (with their UTC offset, so they stay unambiguous). Set TZ on the controller
# pod, e.g. "Europe/London"; everything internal still computes in absolute
# unix time, so this is presentation only.
try:
    LOCAL_TZ = ZoneInfo(os.environ.get("TZ", "UTC"))
except ZoneInfoNotFoundError:
    log.warning("unknown TZ %r — falling back to UTC", os.environ.get("TZ"))
    LOCAL_TZ = ZoneInfo("UTC")


def site_block() -> dict:
    """Identity of this controller's site. Tags every response so a fleet-wide
    gateway can attribute and merge data across VMs later."""
    return {
        "id": os.environ.get("SITE_ID", "local"),
        "tier": os.environ.get("SITE_TIER", "unknown"),
    }


def timestamp() -> str:
    """Now, in the controller's local timezone, ISO 8601 with offset.
    Stamped onto every JSON response (see app._stamp)."""
    return datetime.now(LOCAL_TZ).isoformat(timespec="seconds")


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
# Range: CPU / RAM / network aggregated between two points in time
# ----------------------------------------------------------------------------

# resource key → (friendly output key + unit, actual gauge, target gauge).
_RESOURCE_METRICS = {
    "cpu": ("cpu_millicores", "worker_actual_cpu_millicores",
            "worker_target_cpu_millicores"),
    "ram": ("ram_mb", "worker_actual_ram_mb", "worker_target_ram_mb"),
    "net": ("net_mbps", "worker_actual_net_mbps", "worker_target_net_mbps"),
}

# Hard cap on end - start. A subquery walks every sample in the window, so an
# unbounded interval would let one request make Prometheus chew through months
# of series. 31 days covers any plausible experiment.
_MAX_WINDOW_S = 31 * 24 * 3600


def _round(v):
    return round(v, 3) if isinstance(v, (int, float)) else v


def _as_local(dt: datetime | None) -> datetime | None:
    """Normalise to aware local time (LOCAL_TZ); a naive datetime is taken
    to already BE local. Aware datetimes (Z / explicit offset) keep their
    instant and are converted for display."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ)


def _wanted_resources(resources) -> list[str]:
    """Validate/normalise the resources filter; defaults to all three."""
    if not resources:
        return ["cpu", "ram", "net"]
    unknown = [r for r in resources if r not in _RESOURCE_METRICS]
    if unknown:
        raise ValueError(f"unknown resource(s) {unknown}; valid: cpu, ram, net")
    return list(dict.fromkeys(resources))  # de-dupe, keep order


def _resolve_interval(start: datetime | None,
                      end: datetime | None) -> tuple[datetime, datetime, int]:
    """Apply defaults (end=now, start=end-15m), validate, return
    (start_dt, end_dt, window_seconds) in local time."""
    end_dt = _as_local(end) or datetime.now(LOCAL_TZ)
    start_dt = _as_local(start) or end_dt - timedelta(minutes=15)
    window_s = int((end_dt - start_dt).total_seconds())
    if window_s <= 0:
        raise ValueError("start must be before end")
    if window_s > _MAX_WINDOW_S:
        raise ValueError(f"window too large: {window_s}s (max {_MAX_WINDOW_S}s)")
    return start_dt, end_dt, window_s


def _source_roles(template: dict) -> set[str]:
    """Roles with no inbound edge (self-edges ignored). These evaluate their
    formulas at the template's own x; every other role's x is derived from
    upstream egress (see materialiser._compute_resolved_x)."""
    roles = set(template.get("roles") or {})
    targets = {e.get("to") for e in (template.get("edges") or [])
               if e.get("from") != e.get("to")}
    return roles - targets


def _interval_stats(name: str, roles: dict, wanted: list[str],
                    window_s: int, at: float,
                    source_roles: set[str]) -> dict:
    """totals / roles / x blocks for ONE interval of `window_s` seconds
    ending at unix time `at`.

    The `x` block is split by provenance: `input` holds the source roles
    (their x IS the template's x) and `derived` holds the downstream roles
    (x propagated from upstream egress).

    The trailing-window subquery `fn(sum(metric)[<w>:])` is evaluated AT
    `at`, which turns "last w from now" into "the w seconds ending at `at`".
    Sum across pods first, then reduce over time, so each number is "the
    whole role/template's usage over the interval". The window string is
    built from a validated int, so it is safe to embed in PromQL.
    """
    ns = k8s.namespace()
    window = f"{int(window_s)}s"

    def agg(metric: str, scope: str, fn: str):
        return _round(prom.instant_scalar(
            f"{fn}(sum({metric}{{{scope}}})[{window}:])", at=at))

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

    # x is the same on every pod of a role, so average (not sum) across the
    # role's pods to recover that role's resolved input x.
    def x_of(role: str):
        scope = f'namespace="{ns}",pod=~"wt-{name}-{role}-.*"'
        return _round(prom.instant_scalar(
            f"avg_over_time(avg(worker_input_x{{{scope}}})[{window}:])",
            at=at))

    x_input: dict = {}
    x_derived: dict = {}
    for role in roles:
        (x_input if role in source_roles else x_derived)[role] = x_of(role)

    return {
        "totals": block(f'namespace="{ns}",pod=~"wt-{name}-.*"', full=True),
        "roles": {role: block(f'namespace="{ns}",pod=~"wt-{name}-{role}-.*"',
                              full=False)
                  for role in roles},
        "x": {"input": x_input, "derived": x_derived},
    }


def template_range(name: str, start: datetime | None = None,
                   end: datetime | None = None,
                   resources=None) -> dict | None:
    """CPU/RAM/network for a template, aggregated between `start` and `end`.

    Defaults give "the last 15 minutes". For each requested resource it
    reports the template-wide target and actual *summed across pods*, then
    reduced over [start, end]: actual gets avg / min / max, target gets avg —
    plus the per-role breakdown and an `x` block averaged over the interval,
    split into `input` (source roles, set by the template's own x) and
    `derived` (downstream roles, computed from upstream egress). None if no
    such template; graceful degradation if Prometheus is unreachable.

    Raises ValueError on an unknown resource, start >= end, or a window
    beyond _MAX_WINDOW_S → caller maps to 400.
    """
    info = materialiser.get_managed(name)
    if info is None:
        return None
    wanted = _wanted_resources(resources)
    start_dt, end_dt, window_s = _resolve_interval(start, end)

    envelope = {
        "site": site_block(),
        "name": name,
        "start": start_dt.isoformat(timespec="seconds"),
        "end": end_dt.isoformat(timespec="seconds"),
        "window": f"{window_s}s",
    }
    roles = (info.get("template") or {}).get("roles") or {}

    if not prom.available():
        envelope.update(totals={}, roles={}, x={"input": {}, "derived": {}})
        envelope["prometheus"] = {"available": False, "url": prom.url()}
        return envelope

    envelope.update(_interval_stats(name, roles, wanted, window_s,
                                    end_dt.timestamp(),
                                    _source_roles(info.get("template") or {})))
    envelope["prometheus"] = {"available": True, "url": prom.url()}
    return envelope


# Cap on chunks per request: each chunk costs its own batch of Prometheus
# queries (~40 for a 4-role template), so 100 chunks ≈ 4k instant queries —
# slow but tolerable; beyond that, refuse rather than melt.
_MAX_PERIODS = 100

_DUR_RE = re.compile(r"^(\d+)(s|m|h|d)?$")


def _parse_duration(s: str) -> int:
    """'90s' / '10m' / '1h' / '2d' (bare number = seconds) → seconds."""
    m = _DUR_RE.match(s.strip())
    if not m:
        raise ValueError(f"bad duration {s!r}; use e.g. 90s, 10m, 1h")
    return int(m.group(1)) * {"s": 1, "m": 60,
                              "h": 3600, "d": 86400}[m.group(2) or "s"]


def template_periods(name: str, chunk: str | None = None,
                     count: int | None = None, resources=None) -> dict | None:
    """The last `count` chunks of `chunk` each, ending now, aggregated
    separately.

    e.g. count=4, chunk=11m → the last 44 minutes as 4 eleven-minute periods.
    Each period carries its own start/end plus the same totals/roles/x blocks
    as template_range. Chunks shorter than 30s (the Prometheus scrape
    interval) are rejected — they would hold at most one sample.

    Returns None if no such template. Raises ValueError on a missing/bad
    chunk or count → caller maps to 400.
    """
    info = materialiser.get_managed(name)
    if info is None:
        return None
    wanted = _wanted_resources(resources)

    if not chunk:
        raise ValueError("chunk is required, e.g. chunk=10m")
    chunk_s = _parse_duration(chunk)
    if chunk_s < 30:
        raise ValueError("chunk must be >= 30s (the scrape interval)")

    if count is None:
        raise ValueError("count is required, e.g. count=4")
    if count < 1:
        raise ValueError("count must be >= 1")
    if count > _MAX_PERIODS:
        raise ValueError(
            f"too many chunks ({count}, max {_MAX_PERIODS}) — use a "
            "larger chunk or a smaller count")
    window_s = count * chunk_s
    if window_s > _MAX_WINDOW_S:
        raise ValueError(f"window too large: {window_s}s "
                         f"(max {_MAX_WINDOW_S}s)")
    end_dt = datetime.now(LOCAL_TZ)
    start_dt = end_dt - timedelta(seconds=window_s)

    envelope = {
        "site": site_block(),
        "name": name,
        "start": start_dt.isoformat(timespec="seconds"),
        "end": end_dt.isoformat(timespec="seconds"),
        "window": f"{window_s}s",
        "chunk": f"{chunk_s}s",
        "count": count,
    }
    roles = (info.get("template") or {}).get("roles") or {}

    if not prom.available():
        envelope["periods"] = []
        envelope["prometheus"] = {"available": False, "url": prom.url()}
        return envelope

    src_roles = _source_roles(info.get("template") or {})
    periods = []
    for i in range(count):
        p_start = start_dt + timedelta(seconds=i * chunk_s)
        p_end = p_start + timedelta(seconds=chunk_s)
        stats = _interval_stats(name, roles, wanted, chunk_s,
                                p_end.timestamp(), src_roles)
        periods.append({
            "start": p_start.isoformat(timespec="seconds"),
            "end": p_end.isoformat(timespec="seconds"),
            **stats,
        })
    envelope["periods"] = periods
    envelope["prometheus"] = {"available": True, "url": prom.url()}
    return envelope

