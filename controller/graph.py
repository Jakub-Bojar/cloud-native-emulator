"""
Topology graph builder for the Grafana Node Graph panel.

Turns a materialized template into a {"nodes": [...], "edges": [...]}
structure that a Grafana Node Graph panel can render (via the Infinity or
JSON API datasource pointed at GET /graph/<name>).

Where the numbers come from
---------------------------
The controller runs in-cluster, so it scrapes each worker pod's /metrics
endpoint directly on port 8080 — no Prometheus dependency, and the worker
gauges are already smoothed (CPU and net over a 15s rolling window, RAM
from the cgroup working set).

  - Node stats  (CPU / RAM / x) are read from the per-pod gauges and summed
    across the role's pods to give a role-level total.
  - Edge weights are measured: for each edge from→to we sum the
    worker_peer_egress_mbps{peer="<ip>"} series reported by every `from`
    pod toward IPs that belong to the `to` role. The IP→role mapping comes
    from each role's Service Endpoints (the same source the materializer
    uses when it resolves peer IPs).
"""

import logging
import re
import urllib.error
import urllib.request

import materializer

log = logging.getLogger(__name__)

# Visually distinct palette (Grafana classic colors). Colors are assigned to
# roles by position within the template (see build_graph), so a single graph
# never reuses a color for up to 10 roles, and a role keeps the same color
# across requests and across the role/pod views.
_PALETTE = [
    "#7EB26D", "#EAB839", "#6ED0E0", "#EF843C", "#E24D42",
    "#1F78C1", "#BA43A9", "#705DA0", "#508642", "#0A437C",
]

WORKER_PORT = 8080
SCRAPE_TIMEOUT = 5.0

# Prometheus text-format line parsers. Labeled lines (metric{...} value) are
# tried first so worker_peer_egress_mbps{peer="ip"} doesn't fall through to
# the bare-metric parser.
_LABELED_RE = re.compile(r'^(\w+)\{([^}]*)\}\s+([-\d.eE+]+)\s*$')
_SIMPLE_RE = re.compile(r'^(\w+)\s+([-\d.eE+]+)\s*$')
_PEER_RE = re.compile(r'peer="([^"]+)"')

# Bare gauges we surface per node.
_NODE_GAUGES = (
    "worker_actual_cpu_millicores", "worker_target_cpu_millicores",
    "worker_actual_ram_mb", "worker_target_ram_mb",
    "worker_actual_net_mbps", "worker_target_net_mbps",
    "worker_input_x",
)


def _scrape_pod(ip: str) -> dict | None:
    """Scrape one worker's /metrics. Returns a dict of the bare gauges plus a
    `peer_egress` sub-dict {peer_ip: mbps}, or None if the pod is unreachable.
    """
    url = f"http://{ip}:{WORKER_PORT}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=SCRAPE_TIMEOUT) as resp:
            text = resp.read().decode()
    except (urllib.error.URLError, OSError) as e:
        log.warning("graph: scrape %s failed: %s", ip, e)
        return None
    out: dict = {"peer_egress": {}}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LABELED_RE.match(line)
        if m:
            metric, labels, value = m.group(1), m.group(2), m.group(3)
            if metric == "worker_peer_egress_mbps":
                pm = _PEER_RE.search(labels)
                if pm:
                    out["peer_egress"][pm.group(1)] = float(value)
            continue
        m = _SIMPLE_RE.match(line)
        if m and m.group(1) in _NODE_GAUGES:
            out[m.group(1)] = float(m.group(2))
    return out


def _arc(act_cpu: float, tgt_cpu: float) -> float:
    """CPU saturation fraction (actual / target), clamped to [0, 1]."""
    return max(0.0, min(1.0, act_cpu / tgt_cpu)) if tgt_cpu > 0 else 0.0


def build_graph(name: str, by_pod: bool = False) -> dict | None:
    """Build the Node Graph payload for template `name`, or None if there is
    no such materialized template.

    by_pod=False → one node per role (stats summed across the role's pods),
    edges are role→role with the measured traffic summed per edge.

    by_pod=True  → one node per pod, edges are the raw pod→pod links straight
    from worker_peer_egress_mbps{peer="<ip>"}.
    """
    info = materializer.get_managed(name)
    if info is None:
        return None
    template = info.get("template") or {}
    roles = template.get("roles", {}) or {}
    edges_def = template.get("edges", []) or []
    replicas = info.get("replicas", {}) or {}

    # Resolve the pods backing each role and a reverse IP → (role, pod) index.
    role_pods: dict[str, list[tuple[str, str]]] = {}
    ip_index: dict[str, tuple[str, str]] = {}
    for role in roles:
        pods = materializer._get_endpoint_pods(f"wt-{name}-{role}")
        role_pods[role] = pods
        for pod_name, ip in pods:
            ip_index[ip] = (role, pod_name)

    # Scrape every pod once; one record per pod carrying its role/name/ip,
    # the parsed gauges, and its peer_egress map.
    pod_records: list[dict] = []
    for role, pods in role_pods.items():
        for pod_name, ip in pods:
            s = _scrape_pod(ip) or {"peer_egress": {}}
            pod_records.append({"role": role, "pod": pod_name, "ip": ip, **s})

    # Assign a palette color per role by position so one graph never reuses a
    # color (up to len(_PALETTE) roles). Stable across requests and views.
    color_by_role = {role: _PALETTE[i % len(_PALETTE)]
                     for i, role in enumerate(roles)}

    if by_pod:
        return _pod_graph(pod_records, ip_index, color_by_role)
    return _role_graph(roles, edges_def, replicas, role_pods, pod_records,
                       color_by_role)


def _role_graph(roles: dict, edges_def: list, replicas: dict,
                role_pods: dict[str, list[tuple[str, str]]],
                pod_records: list[dict],
                color_by_role: dict[str, str]) -> dict:
    scrapes_by_role: dict[str, list[dict]] = {r: [] for r in roles}
    for rec in pod_records:
        scrapes_by_role[rec["role"]].append(rec)
    role_ips = {r: {ip for (_, ip) in pods} for r, pods in role_pods.items()}

    # Edges first (measured from→to) so each node can carry its in/out totals.
    edge_records: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for e in edges_def:
        src, dst = e.get("from"), e.get("to")
        if src is None or dst is None or (src, dst) in seen:
            continue
        seen.add((src, dst))
        dst_ips = role_ips.get(dst, set())
        measured = 0.0
        for s in scrapes_by_role.get(src, []):
            for peer_ip, mbps in s.get("peer_egress", {}).items():
                if peer_ip in dst_ips:
                    measured += mbps
        edge_records.append({"source": src, "target": dst, "mbps": measured})

    out_by_role: dict[str, float] = {r: 0.0 for r in roles}
    in_by_role: dict[str, float] = {r: 0.0 for r in roles}
    for rec in edge_records:
        out_by_role[rec["source"]] += rec["mbps"]
        in_by_role[rec["target"]] += rec["mbps"]

    nodes: list[dict] = []
    for role in roles:
        scrapes = scrapes_by_role.get(role, [])
        n_pods = len(scrapes)

        def _sum(metric: str) -> float:
            return sum(s.get(metric, 0.0) for s in scrapes)

        act_cpu, tgt_cpu = _sum("worker_actual_cpu_millicores"), _sum("worker_target_cpu_millicores")
        act_ram, tgt_ram = _sum("worker_actual_ram_mb"), _sum("worker_target_ram_mb")
        x = scrapes[0].get("worker_input_x", 0.0) if scrapes else 0.0
        want = int(replicas.get(role, n_pods))
        frac = _arc(act_cpu, tgt_cpu)

        nodes.append({
            "id": role,
            "title": role,
            "subTitle": f"{n_pods}/{want} pods",
            "mainStat": f"CPU {act_cpu:.0f}/{tgt_cpu:.0f} m",
            "secondaryStat": f"RAM {act_ram:.0f}/{tgt_ram:.0f} MB",
            "color": color_by_role.get(role, _PALETTE[0]),
            "arc__used": round(frac, 3),
            "arc__free": round(1.0 - frac, 3),
            "detail__cpu_pct": round(frac * 100, 1),
            "detail__x": round(x, 2),
            "detail__net_in_mbps": round(in_by_role.get(role, 0.0), 2),
            "detail__net_out_mbps": round(out_by_role.get(role, 0.0), 2),
            "detail__replicas": want,
        })

    edges = [{
        "id": f"{r['source']}->{r['target']}",
        "source": r["source"],
        "target": r["target"],
        "mainStat": f"{r['mbps']:.2f} Mbps",
    } for r in edge_records]

    return {"nodes": nodes, "edges": edges}


def _pod_graph(pod_records: list[dict],
               ip_index: dict[str, tuple[str, str]],
               color_by_role: dict[str, str]) -> dict:
    # Raw pod→pod edges straight from each pod's peer_egress map.
    edges: list[dict] = []
    in_by_pod: dict[str, float] = {}
    out_by_pod: dict[str, float] = {}
    for rec in pod_records:
        src = rec["pod"]
        for peer_ip, mbps in rec.get("peer_egress", {}).items():
            target = ip_index.get(peer_ip)
            if target is None:
                continue  # peer IP not part of this template (stale/unknown)
            dst = target[1]
            edges.append({
                "id": f"{src}->{dst}",
                "source": src,
                "target": dst,
                "mainStat": f"{mbps:.2f} Mbps",
            })
            out_by_pod[src] = out_by_pod.get(src, 0.0) + mbps
            in_by_pod[dst] = in_by_pod.get(dst, 0.0) + mbps

    nodes: list[dict] = []
    for rec in pod_records:
        pod, role = rec["pod"], rec["role"]
        act_cpu = rec.get("worker_actual_cpu_millicores", 0.0)
        tgt_cpu = rec.get("worker_target_cpu_millicores", 0.0)
        act_ram = rec.get("worker_actual_ram_mb", 0.0)
        tgt_ram = rec.get("worker_target_ram_mb", 0.0)
        x = rec.get("worker_input_x", 0.0)
        frac = _arc(act_cpu, tgt_cpu)
        # Short, readable title: role + the pod's trailing hash segment.
        suffix = pod.rsplit("-", 1)[-1]

        nodes.append({
            "id": pod,
            "title": f"{role}/{suffix}",
            "subTitle": role,
            "mainStat": f"CPU {act_cpu:.0f}/{tgt_cpu:.0f} m",
            "secondaryStat": f"RAM {act_ram:.0f}/{tgt_ram:.0f} MB",
            "color": color_by_role.get(role, _PALETTE[0]),
            "arc__used": round(frac, 3),
            "arc__free": round(1.0 - frac, 3),
            "detail__role": role,
            "detail__cpu_pct": round(frac * 100, 1),
            "detail__x": round(x, 2),
            "detail__net_in_mbps": round(in_by_pod.get(pod, 0.0), 2),
            "detail__net_out_mbps": round(out_by_pod.get(pod, 0.0), 2),
        })

    return {"nodes": nodes, "edges": edges}
