"""
Inter-tier latency / bandwidth: validation, the configured-RTT metric, and
cleanup of any leftover Chaos Mesh NetworkChaos.

The actual link shaping is done by netem.py (tc on the tier nodes); this module
no longer injects anything. What remains is:

  - validate_latency / validate_bandwidth — parse and shape-check the template's
    latency/bandwidth fields. Also reused by netem.py to get the per-pair values.
  - set_configured_rtt — publish the configured one-way latency per tier pair on
    the controller's /metrics, so a dashboard can show "what the latency is meant
    to be" next to the workers' measured figure (worker_peer_rtt_ms / 2).
  - delete_managed — remove any NetworkChaos left over from the old Chaos Mesh
    approach so it can't double-shape a link netem.py now owns. Called on every
    materialise and on teardown; a no-op once a cluster carries no such objects.
"""

import json
import logging
import re

from prometheus_client import Gauge

import k8s

log = logging.getLogger(__name__)

MANAGED_LABEL = "app.kubernetes.io/managed-by"
MANAGED_VALUE = "emulator-controller"

_RTT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(us|ms|s)\s*$")
_BW_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(kbps|mbps|gbps)\s*$", re.IGNORECASE)
# bytes per second per unit (for limit/buffer sizing)
_BW_UNIT_BYTES = {"kbps": 125, "mbps": 125_000, "gbps": 125_000_000}
_UNIT_US = {"us": 1, "ms": 1_000, "s": 1_000_000}
_MAX_RTT_US = 10 * 1_000_000  # 10s — beyond any plausible WAN emulation


# ── Configured inter-tier latency metric ─────────────────────────────────────
# The template's CONFIGURED inter-tier ONE-WAY latency, exposed on the
# controller's /metrics so dashboards can show "what the latency is meant to
# be" alongside the workers' measured one-way figure (worker_peer_rtt_ms / 2).
# Latency is symmetric, so it's one series per UNORDERED tier pair (label
# `pair`, e.g. "edge ↔ cloud"). Recomputed each scrape from the live template,
# so a PATCH retune shows up immediately.

CONFIGURED_RTT = Gauge(
    "emulator_configured_one_way_ms",
    "Configured inter-tier ONE-WAY latency (ms) from the template's latency "
    "field — a packet takes this long to cross the link each way. One series "
    "per unordered tier pair. Compare against the measured one-way value "
    "(worker_peer_rtt_ms / 2).",
    ["pair"])


def set_configured_rtt(pairs: dict[tuple[str, str], int]) -> None:
    """Publish `pairs` ({(tierA,tierB): one_way_us} from validate_latency) as
    the CONFIGURED_RTT gauge (the one-way latency per tier pair). Each key is
    already a sorted (min,max) tuple, so the `pair` label is deterministic.
    Cleared first so a retune or teardown drops stale series."""
    CONFIGURED_RTT.clear()
    for (a, b), rtt_us in pairs.items():
        CONFIGURED_RTT.labels(pair=f"{a} -> {b}").set(rtt_us / 1000.0)


# The template's CONFIGURED inter-tier bandwidth cap, exposed alongside
# CONFIGURED_RTT so the network dashboard can show "what the bandwidth is meant
# to be" next to the workers' measured throughput (worker_peer_egress_mbps).
# Symmetric, so one series per UNORDERED tier pair (label `pair`).
CONFIGURED_BW = Gauge(
    "emulator_configured_bandwidth_mbps",
    "Configured inter-tier bandwidth cap (Mbps) from the template's bandwidth "
    "field — the tc rate limit applied on the link in each direction. One "
    "series per unordered tier pair. Compare against measured throughput "
    "(worker_peer_egress_mbps).",
    ["pair"])


def set_configured_bw(pairs: dict[tuple[str, str], str]) -> None:
    """Publish `pairs` ({(tierA,tierB): rate_str} from validate_bandwidth) as the
    CONFIGURED_BW gauge (the bandwidth cap per tier pair, in Mbps). The rate
    string (e.g. '1000mbps') is parsed to Mbps. Cleared first so a retune or
    teardown drops stale series."""
    CONFIGURED_BW.clear()
    for (a, b), rate in pairs.items():
        mbps = _parse_rate(rate) * 8 / 1_000_000  # bytes/sec → Mbps
        CONFIGURED_BW.labels(pair=f"{a} -> {b}").set(mbps)


# ── Latency / bandwidth validation ───────────────────────────────────────────
# The template's optional `latency` / `bandwidth` fields declare the ONE-WAY
# values between tier pairs, e.g.
#
#     "latency": { "edge": { "fog": "30ms", "cloud": "120ms" },
#                  "fog":  { "cloud": "60ms" } }
#
# means a packet edge→fog takes 30ms (and fog→edge 30ms — symmetric). These
# validators parse and shape-check those fields; netem.py reuses them to get the
# per-pair values it applies as tc on the nodes.

def _parse_rtt_us(value) -> int:
    if not isinstance(value, str) or not _RTT_RE.match(value):
        raise ValueError(f"latency value {value!r}: use a duration like "
                         "'10ms', '120ms', '1s'")
    m = _RTT_RE.match(value)
    us = int(float(m.group(1)) * _UNIT_US[m.group(2)])
    if us < 2:
        raise ValueError(f"latency {value!r} is below 1us per direction")
    if us > _MAX_RTT_US:
        raise ValueError(f"latency {value!r} exceeds the 10s cap")
    return us


def validate_latency(latency) -> dict[tuple[str, str], int]:
    """template.latency → {(tierA, tierB) sorted: rtt_us}.

    Raises ValueError on bad shape, bad durations, same-tier pairs, or a
    pair specified twice (latency is symmetric — give each pair once,
    in either orientation)."""
    if latency is None:
        return {}
    if not isinstance(latency, dict):
        raise ValueError(
            'latency must be an object: {"tier": {"peerTier": "rtt"}}')
    pairs: dict[tuple[str, str], int] = {}
    for a, peers in latency.items():
        if not isinstance(a, str) or not a:
            raise ValueError("latency keys must be tier names")
        if not isinstance(peers, dict):
            raise ValueError(f"latency.{a} must be an object of peer tiers")
        for b, rtt in peers.items():
            if not isinstance(b, str) or not b:
                raise ValueError(f"latency.{a} keys must be tier names")
            if a == b:
                raise ValueError(
                    f"latency.{a}.{b}: same-tier latency is not supported")
            key = (min(a, b), max(a, b))
            if key in pairs:
                raise ValueError(
                    f"latency between {key[0]} and {key[1]} is specified "
                    "more than once (it is symmetric — give each pair once)")
            pairs[key] = _parse_rtt_us(rtt)
    return pairs


def _parse_rate(value: str) -> int:
    """Parse a bandwidth rate string (e.g. '100mbps') and return bytes/sec.

    Raises ValueError if the string is not a valid rate."""
    if not isinstance(value, str):
        raise ValueError(f"bandwidth rate {value!r}: must be a string like '100mbps'")
    m = _BW_RE.match(value)
    if not m:
        raise ValueError(
            f"bandwidth rate {value!r}: use 'Xkbps', 'Xmbps', or 'Xgbps'")
    num = float(m.group(1))
    if num <= 0:
        raise ValueError(f"bandwidth rate {value!r}: must be > 0")
    return int(num * _BW_UNIT_BYTES[m.group(2).lower()])


def validate_bandwidth(bandwidth) -> dict[tuple[str, str], str]:
    """template.bandwidth → {(tierA, tierB) sorted: rate_str}.

    Raises ValueError on bad shape, bad rate strings, same-tier pairs, or a
    pair specified twice (bandwidth is symmetric — give each pair once,
    in either orientation)."""
    if bandwidth is None:
        return {}
    if not isinstance(bandwidth, dict):
        raise ValueError(
            'bandwidth must be an object: {"tier": {"peerTier": "Xmbps"}}')
    pairs: dict[tuple[str, str], str] = {}
    for a, peers in bandwidth.items():
        if not isinstance(a, str) or not a:
            raise ValueError("bandwidth keys must be tier names")
        if not isinstance(peers, dict):
            raise ValueError(f"bandwidth.{a} must be an object of peer tiers")
        for b, rate in peers.items():
            if not isinstance(b, str) or not b:
                raise ValueError(f"bandwidth.{a} keys must be tier names")
            if a == b:
                raise ValueError(
                    f"bandwidth.{a}.{b}: same-tier bandwidth is not supported")
            key = (min(a, b), max(a, b))
            if key in pairs:
                raise ValueError(
                    f"bandwidth between {key[0]} and {key[1]} is specified "
                    "more than once (it is symmetric — give each pair once)")
            _parse_rate(rate)  # validate format
            pairs[key] = rate
    return pairs


# ── Leftover NetworkChaos cleanup ─────────────────────────────────────────────
# netem.py owns inter-tier shaping now. This removes any NetworkChaos left from
# the old Chaos Mesh approach so the two can't both shape the same link.

def _chaos_path(name: str | None = None) -> str:
    base = (f"/apis/chaos-mesh.org/v1alpha1/namespaces/"
            f"{k8s.namespace()}/networkchaos")
    return f"{base}/{name}" if name else base


def _managed_items() -> list[dict]:
    """Controller-managed NetworkChaos in the namespace, or [{}] if the list
    can't be read (Chaos Mesh not installed / API error) — delete_managed
    treats that sentinel as 'nothing to delete'."""
    status, body = k8s.get(_chaos_path())
    if status != 200:
        return [{}]
    return [i for i in json.loads(body).get("items", [])
            if (i.get("metadata", {}).get("labels") or {})
            .get(MANAGED_LABEL) == MANAGED_VALUE]


def delete_managed() -> int:
    """Remove any leftover controller-managed NetworkChaos (from the old Chaos
    Mesh approach). Called FIRST in teardown, while the worker pods are still
    alive — recovering rules from live pods is fast, whereas records pointing at
    dead pods stall the finalizers. A no-op once a cluster carries none."""
    try:
        mine = _managed_items()
        if mine == [{}] or not mine:
            return 0
        for item in mine:
            k8s.delete(_chaos_path(item["metadata"]["name"]))
        log.info("linkspec: deleted %d leftover NetworkChaos", len(mine))
        return len(mine)
    except Exception:
        log.exception("linkspec: leftover NetworkChaos delete failed; continuing")
        return 0
