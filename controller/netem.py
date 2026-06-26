"""
Link-level inter-tier shaping (latency + bandwidth) with `tc` on the nodes.

Replaces the Chaos Mesh NetworkChaos approach for inter-tier latency/bandwidth.
Chaos Mesh pins its tc/iptables rules to individual pod IPs, resolved once at
apply time, so scaling a tier (adding pods) races pod-IP propagation and can
leave a new pod with a half- or un-injected delay that Chaos Mesh still reports
as `AllInjected` (the scale-up bug).

Latency/bandwidth are really properties of the inter-NODE link, not of pods:
each tier is a dedicated node (VM) and Calico runs VXLAN-always, so all
inter-tier pod traffic is encapsulated between node InternalIPs and egresses the
node's physical NIC. We therefore shape the link itself — an `htb` class (rate)
with a `netem` child (delay) on each node's NIC, selected by a `tc` filter
matching the PEER NODE IP. Pods inherit the link characteristics automatically,
so scaling never disturbs them and nothing needs re-resolving.

The configured value is the ONE-WAY latency: the full value is applied on each
node toward its peer, so a packet takes that long each way (round trip ~2×),
matching the rest of the system (see linkspec.validate_latency / grafana-network.json).

The controller runs on the host and reaches each node by `multipass exec`
(k8s node name == Multipass VM name). Reconciled on every materialise (a cheap
full rebuild), torn down on DELETE. Never raises — shaping is auxiliary to
materialising the template.
"""

import json
import logging
import os
import subprocess

import k8s
import linkspec

log = logging.getLogger(__name__)

# `multipass exec <vm> -- sudo sh -c <script>` is how we reach each node. The
# VM name equals the k8s node name (see provision.py).
MULTIPASS = os.environ.get("MULTIPASS_BIN", "multipass")
TIER_LABEL = "tier"            # node label set by provision.py
UNSHAPED_RATE = "10gbit"       # htb rate for the default class / delay-only links
EXEC_TIMEOUT_S = 30


def _vm_sh(vm: str, script: str) -> subprocess.CompletedProcess:
    """Run `script` as root inside `vm` via multipass exec."""
    return subprocess.run(
        [MULTIPASS, "exec", vm, "--", "sudo", "sh", "-c", script],
        capture_output=True, text=True, timeout=EXEC_TIMEOUT_S)


def _nodes_by_tier() -> dict[str, list[tuple[str, str]]]:
    """{tier_name: [(node_name, internal_ip), ...]} from the live k8s nodes."""
    status, body = k8s.get("/api/v1/nodes")
    if status != 200:
        log.warning("netem: listing nodes returned %s; skipping shaping", status)
        return {}
    out: dict[str, list[tuple[str, str]]] = {}
    for n in json.loads(body).get("items", []):
        labels = (n.get("metadata", {}).get("labels") or {})
        tier = labels.get(TIER_LABEL)
        if not tier:
            continue  # control plane and unlabelled nodes are never shaped
        ip = next((a["address"]
                   for a in n.get("status", {}).get("addresses", [])
                   if a.get("type") == "InternalIP"), None)
        if ip:
            out.setdefault(tier, []).append((n["metadata"]["name"], ip))
    return out


def _tc_rate(rate_str: str) -> str:
    """'1000mbps' → a tc bit-unit rate string ('1000mbit' / '1gbit')."""
    bits = linkspec._parse_rate(rate_str) * 8
    if bits % 1_000_000_000 == 0:
        return f"{bits // 1_000_000_000}gbit"
    if bits % 1_000_000 == 0:
        return f"{bits // 1_000_000}mbit"
    return f"{bits}bit"


def _iface_line(internal_ip: str) -> str:
    """Shell that sets $IFACE to the NIC holding the node's InternalIP — the
    physical NIC that carries the VXLAN-encapsulated inter-tier traffic."""
    return (f"IFACE=$(ip -o -4 addr show | grep -F '{internal_ip}/' "
            "| awk '{print $2}' | head -1)")


def _clear_script(internal_ip: str) -> str:
    """Remove our root qdisc, restoring the node's default queuing."""
    return (f"{_iface_line(internal_ip)}; "
            '[ -n "$IFACE" ] && tc qdisc del dev "$IFACE" root 2>/dev/null; true')


def _shape_script(internal_ip: str,
                  peers: list[tuple[str, int | None, str | None]]) -> str:
    """Build the tc script: htb root with an unshaped default class, plus one
    class+filter per peer node IP (netem child when the link has a delay)."""
    lines = [
        _iface_line(internal_ip),
        '[ -z "$IFACE" ] && { echo "netem: no NIC found" >&2; exit 1; }',
        'tc qdisc del dev "$IFACE" root 2>/dev/null || true',
        'tc qdisc add dev "$IFACE" root handle 1: htb default 9999',
        f'tc class add dev "$IFACE" parent 1: classid 1:9999 htb rate {UNSHAPED_RATE}',
    ]
    for i, (peer_ip, delay_us, rate_str) in enumerate(peers):
        cid = 10 + i
        rate = _tc_rate(rate_str) if rate_str else UNSHAPED_RATE
        lines.append(f'tc class add dev "$IFACE" parent 1: classid 1:{cid} '
                     f'htb rate {rate} ceil {rate}')
        if delay_us:
            lines.append(f'tc qdisc add dev "$IFACE" parent 1:{cid} '
                         f'handle {cid}0: netem delay {delay_us / 1000.0:g}ms')
        lines.append(f'tc filter add dev "$IFACE" parent 1: protocol ip '
                     f'prio 1 u32 match ip dst {peer_ip}/32 flowid 1:{cid}')
    return "\n".join(lines)


def apply(template: dict) -> None:
    """Reconcile inter-tier link shaping from the template's latency/bandwidth.
    Never raises — shaping is auxiliary."""
    try:
        _apply(template)
    except Exception:
        log.exception("netem: apply failed; continuing without link shaping")


def _apply(template: dict) -> None:
    latency = linkspec.validate_latency(template.get("latency"))      # {(a,b): us}
    bandwidth = linkspec.validate_bandwidth(template.get("bandwidth"))  # {(a,b): rate}
    nodes = _nodes_by_tier()
    if not nodes:
        return
    pairs = set(latency) | set(bandwidth)  # sorted (min,max) tier tuples
    for tier, members in nodes.items():
        # Every node in this tier shapes toward the SAME peer set (the nodes of
        # the other tier on each configured link); the full one-way value is
        # applied on each direction.
        peer_specs: list[tuple[str, int | None, str | None]] = []
        for (a, b) in pairs:
            other = b if tier == a else a if tier == b else None
            if other is None:
                continue
            delay_us, rate = latency.get((a, b)), bandwidth.get((a, b))
            for (_peer_node, peer_ip) in nodes.get(other, []):
                peer_specs.append((peer_ip, delay_us, rate))
        for (node_name, internal_ip) in members:
            script = (_shape_script(internal_ip, peer_specs) if peer_specs
                      else _clear_script(internal_ip))
            try:
                r = _vm_sh(node_name, script)
            except (subprocess.SubprocessError, OSError) as e:
                log.warning("netem: exec on %s failed: %s", node_name, e)
                continue
            if r.returncode != 0:
                log.warning("netem: tc on %s failed (rc=%s): %s", node_name,
                            r.returncode, (r.stderr or "").strip()[:300])
            else:
                log.info("netem: shaped %s — %d peer link(s)",
                         node_name, len(peer_specs))


def teardown() -> None:
    """Remove link shaping from every tier node. Never raises."""
    try:
        nodes = _nodes_by_tier()
        for members in nodes.values():
            for (node_name, internal_ip) in members:
                try:
                    _vm_sh(node_name, _clear_script(internal_ip))
                except (subprocess.SubprocessError, OSError) as e:
                    log.warning("netem: clear on %s failed: %s", node_name, e)
        if nodes:
            log.info("netem: cleared link shaping on %d node(s)",
                     sum(len(m) for m in nodes.values()))
    except Exception:
        log.exception("netem: teardown failed; continuing")
