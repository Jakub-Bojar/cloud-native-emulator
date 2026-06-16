"""
Chaos Mesh integration: refresh NetworkChaos after the pods it targets change.

Chaos Mesh resolves a NetworkChaos's pod selectors ONCE, at apply time, and
never again — pods created later match the selectors but get no tc rules, so
the injected inter-tier latency silently vanishes for them. The controller
is the one component that knows the exact moment new worker pods exist (the
end of materialise(), which has just finished waiting for their Endpoints),
so it refreshes the chaos there.

Delete+recreate is the ONLY way to make Chaos Mesh re-resolve: an
experiment's containerRecords are fixed for its lifetime, so pause/resume
(tried first — see git history) just re-injects into the original, partly
dead pod set. The risk of delete+recreate is being left with nothing if the
controller dies or Chaos Mesh's finalizers stall between the two steps, so
the specs are snapshotted to a ConfigMap before deleting and the ConfigMap
is only removed once every resource is back; a later refresh finding no
NetworkChaos but a snapshot restores from it.

Refresh is conditional: it compares the pod names in every NetworkChaos's
injection records against the live worker pods and does nothing when they
match. A config-only PATCH (new x, new formulas) churns no pods and so never
blips the latency.

Entirely optional: if Chaos Mesh isn't installed (CRD path 404s) or no
NetworkChaos exist in the namespace, everything here is a no-op. Failures
are logged, never raised — latency injection is auxiliary to materialising
the template itself.
"""

import json
import logging
import re
import time
from urllib.parse import quote

from prometheus_client import Gauge

import k8s

log = logging.getLogger(__name__)

WORKER_SELECTOR = "app.kubernetes.io/managed-by=emulator-controller"

# The snapshot taken before deleting, so an interrupted refresh can restore.
SNAPSHOT_CM = "emulator-chaos-snapshot"

# How long to wait for worker pods to stop churning before touching the
# chaos (racing the chaos-daemon mid-termination corrupts the merged tc
# rules), for deletion finalizers (slow when records reference dead pods —
# exactly what a stale refresh deals in), and for re-injection to complete.
STABLE_TIMEOUT_S = 90
DELETE_TIMEOUT_S = 180
INJECT_TIMEOUT_S = 60


def _chaos_path(name: str | None = None) -> str:
    base = (f"/apis/chaos-mesh.org/v1alpha1/namespaces/"
            f"{k8s.namespace()}/networkchaos")
    return f"{base}/{name}" if name else base


def _live_worker_pods() -> tuple[dict[str, str], bool] | None:
    """({running worker pod name: node name}, any_churning) or None on API
    error."""
    path = (f"/api/v1/namespaces/{k8s.namespace()}/pods"
            f"?labelSelector={quote(WORKER_SELECTOR)}")
    status, body = k8s.get(path)
    if status != 200:
        return None
    running: dict[str, str] = {}
    churning = False
    for pod in json.loads(body).get("items", []):
        meta, st = pod.get("metadata", {}), pod.get("status", {})
        if meta.get("deletionTimestamp") or st.get("phase") == "Pending":
            churning = True
        elif st.get("phase") == "Running":
            running[meta.get("name", "")] = pod.get("spec", {}).get("nodeName", "")
    return running, churning


def _workers_stable(timeout_s: float = STABLE_TIMEOUT_S) -> bool:
    """Wait until no worker pod is Pending/Terminating. Touching the chaos
    mid-churn either bakes in a stale pod list or races the chaos-daemon."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        live = _live_worker_pods()
        if live is not None and not live[1]:
            return True
        time.sleep(3)
    return False


def refresh() -> None:
    """Re-create every NetworkChaos in the namespace iff its injection
    records no longer match the live worker pods. Never raises."""
    try:
        _refresh()
    except Exception:
        log.exception("chaos refresh failed; continuing without it")


def _refresh() -> None:
    status, body = k8s.get(_chaos_path())
    if status == 404:
        return  # Chaos Mesh not installed — nothing to manage
    if status != 200:
        log.warning("chaos refresh: list returned %s; skipping", status)
        return
    items = json.loads(body).get("items", [])
    if not items:
        # A previous refresh may have died between delete and re-create —
        # restore from its snapshot rather than leaving the cluster bare.
        snapshot = _load_snapshot()
        if snapshot:
            log.warning("chaos refresh: no NetworkChaos but a snapshot of %d "
                        "exists (interrupted refresh) — restoring",
                        len(snapshot))
            _create_all(snapshot)
        else:
            log.info("chaos refresh: no NetworkChaos in the namespace — "
                     "apply manifests/network-chaos.yaml if latency "
                     "injection is wanted")
        return

    # Stale check: with tier-pair rules, every worker pod should appear in
    # the union of injection records, and every recorded pod should still
    # exist. Equality of the two sets means nothing changed — don't blip.
    recorded: set[str] = set()
    for item in items:
        recs = ((item.get("status", {}).get("experiment", {}) or {})
                .get("containerRecords") or [])
        recorded.update(r.get("id", "").split("/")[-1] for r in recs)
    live = _live_worker_pods()
    if live is None:
        log.warning("chaos refresh: cannot list worker pods; skipping")
        return
    live_pods = set(live[0])
    if recorded == live_pods:
        log.info("chaos refresh: records match the %d live worker pods — "
                 "nothing to do", len(live_pods))
        return

    if not _workers_stable():
        log.warning("chaos refresh: worker pods still churning after %ss; "
                    "skipping (next materialise will retry)", STABLE_TIMEOUT_S)
        return

    snapshot = [{
        "apiVersion": "chaos-mesh.org/v1alpha1",
        "kind": "NetworkChaos",
        "metadata": {"name": i["metadata"]["name"],
                     "namespace": i["metadata"]["namespace"]},
        "spec": i["spec"],
    } for i in items]
    names = [s["metadata"]["name"] for s in snapshot]

    # Persist the snapshot FIRST: if anything below fails or the controller
    # dies, a later refresh restores from it instead of losing the config.
    if not _save_snapshot(snapshot):
        log.warning("chaos refresh: could not save snapshot ConfigMap; "
                    "refusing to delete NetworkChaos without it")
        return

    log.info("chaos refresh: stale records — re-creating %s so Chaos Mesh "
             "re-resolves pods", names)
    for name in names:
        k8s.delete(_chaos_path(name))
    gone = _wait(lambda: not _list_items(), DELETE_TIMEOUT_S)
    if not gone:
        log.warning("chaos refresh: old NetworkChaos still terminating after "
                    "%ss — snapshot kept, a later refresh will restore",
                    DELETE_TIMEOUT_S)
        return
    _create_all(snapshot)


def _list_items() -> list[dict]:
    status, body = k8s.get(_chaos_path())
    if status != 200:
        return [{}]  # unknown — treat as "still there" so callers keep waiting
    return json.loads(body).get("items", [])


def _create_all(snapshot: list[dict]) -> None:
    """Create every snapshotted NetworkChaos (retrying webhook blips); drop
    the snapshot ConfigMap only once all of them exist again."""
    failed = []
    for spec in snapshot:
        name = spec["metadata"]["name"]
        for attempt in range(3):
            status, resp = k8s.post(_chaos_path(), spec)
            if status in (200, 201, 409):  # 409: already restored
                break
            log.warning("chaos refresh: create %s failed (try %d/3): %s %s",
                        name, attempt + 1, status, resp[:200])
            time.sleep(2)
        else:
            failed.append(name)
    if failed:
        log.warning("chaos refresh: %s not re-created — snapshot kept for "
                    "the next refresh", failed)
        return
    _drop_snapshot()
    if _wait(_all_injected, INJECT_TIMEOUT_S):
        log.info("chaos refresh: %d NetworkChaos re-injected against the "
                 "current pods", len(snapshot))
    else:
        log.warning("chaos refresh: not AllInjected after %ss — check "
                    "worker_peer_rtt_ms / kubectl describe networkchaos",
                    INJECT_TIMEOUT_S)


# ── Snapshot ConfigMap plumbing ──────────────────────────────────────────────

def _cm_path(name: str | None = None) -> str:
    base = f"/api/v1/namespaces/{k8s.namespace()}/configmaps"
    return f"{base}/{name}" if name else base


def _save_snapshot(snapshot: list[dict]) -> bool:
    payload = json.dumps(snapshot, separators=(",", ":"))
    cm = {"apiVersion": "v1", "kind": "ConfigMap",
          "metadata": {"name": SNAPSHOT_CM},
          "data": {"snapshot.json": payload}}
    status, _ = k8s.post(_cm_path(), cm)
    if status == 409:
        status, _ = k8s.patch(_cm_path(SNAPSHOT_CM),
                              {"data": {"snapshot.json": payload}})
    return 200 <= status < 300


def _load_snapshot() -> list[dict] | None:
    status, body = k8s.get(_cm_path(SNAPSHOT_CM))
    if status != 200:
        return None
    try:
        return json.loads(json.loads(body)["data"]["snapshot.json"])
    except (KeyError, json.JSONDecodeError):
        log.warning("chaos refresh: snapshot ConfigMap is malformed")
        return None


def _drop_snapshot() -> None:
    k8s.delete(_cm_path(SNAPSHOT_CM))


def _wait(cond, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _item_injected(item: dict) -> bool:
    """True only when an AllInjected=True condition is PRESENT. A freshly
    created NetworkChaos has no conditions at all — treating that as success
    (vacuous all()) made the controller report injection prematurely."""
    return any(c.get("type") == "AllInjected" and c.get("status") == "True"
               for c in item.get("status", {}).get("conditions", []))


def _all_injected() -> bool:
    status, body = k8s.get(_chaos_path())
    if status != 200:
        return False
    items = json.loads(body).get("items", [])
    return bool(items) and all(_item_injected(i) for i in items)


# ─────────────────────────────────────────────────────────────────────────────
# Template-defined latency: the template's optional `latency` field declares
# RTTs between tier pairs, e.g.
#
#     "latency": { "edge": { "fog": "30ms", "cloud": "120ms" },
#                  "fog":  { "cloud": "60ms" } }
#
# The controller renders one NetworkChaos per pair (selector = pods on nodes
# of one tier, target = the other, direction both, delay = HALF the RTT so
# the two ends sum to it), labels them as controller-managed, and reconciles
# them on every materialise: spec drift or a stale pod list → replace; in
# sync → untouched, so a numbers-only PATCH never blips the latency. The
# desired state is always derivable from the template, so no snapshot is
# needed: an interrupted replace is simply completed by the next materialise.
# ─────────────────────────────────────────────────────────────────────────────

MANAGED_LABEL = "app.kubernetes.io/managed-by"
MANAGED_VALUE = "emulator-controller"

_RTT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(us|ms|s)\s*$")
_UNIT_US = {"us": 1, "ms": 1_000, "s": 1_000_000}
_MAX_RTT_US = 10 * 1_000_000  # 10s — beyond any plausible WAN emulation

# The template's CONFIGURED inter-tier RTTs, exposed on the controller's
# /metrics so dashboards can show "what the latency is meant to be" alongside
# the workers' measured worker_peer_rtt_ms. Latency is symmetric, so it's one
# series per UNORDERED tier pair (label `pair`, e.g. "edge ↔ cloud").
# Recomputed each scrape from the live template, so a PATCH retune shows up
# immediately.
CONFIGURED_RTT = Gauge(
    "emulator_configured_rtt_ms",
    "Configured inter-tier round-trip latency (ms) from the template's "
    "latency field, one series per unordered tier pair. Compare against "
    "measured worker_peer_rtt_ms.",
    ["pair"])


def set_configured_rtt(pairs: dict[tuple[str, str], int]) -> None:
    """Publish `pairs` ({(tierA,tierB): rtt_us} from validate_latency) as the
    CONFIGURED_RTT gauge. Each key is already a sorted (min,max) tuple, so the
    `pair` label is deterministic. Cleared first so a retune or teardown drops
    stale series."""
    CONFIGURED_RTT.clear()
    for (a, b), rtt_us in pairs.items():
        CONFIGURED_RTT.labels(pair=f"{a} ↔ {b}").set(rtt_us / 1000.0)


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


def _latency_specs(pairs: dict[tuple[str, str], int]) -> list[dict]:
    ns = k8s.namespace()
    specs = []
    for (a, b), rtt_us in sorted(pairs.items()):
        each_way_us = max(1, rtt_us // 2)
        specs.append({
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {
                "name": f"wt-latency-{a}-{b}",
                "namespace": ns,
                "labels": {MANAGED_LABEL: MANAGED_VALUE},
            },
            "spec": {
                "action": "delay",
                "mode": "all",
                "selector": {"namespaces": [ns],
                             "nodeSelectors": {"tier": a}},
                "target": {"mode": "all",
                           "selector": {"namespaces": [ns],
                                        "nodeSelectors": {"tier": b}}},
                "direction": "both",
                "delay": {"latency": f"{each_way_us}us"},
            },
        })
    return specs


def _sig(item: dict) -> tuple:
    """The fields we own, for drift detection. Never compare full specs —
    the API server adds defaults that would read as permanent drift."""
    spec = item.get("spec", {})
    return (
        item.get("metadata", {}).get("name"),
        spec.get("action"),
        spec.get("direction"),
        (spec.get("delay") or {}).get("latency"),
        (spec.get("selector") or {}).get("nodeSelectors", {}).get("tier"),
        ((spec.get("target") or {}).get("selector") or {})
        .get("nodeSelectors", {}).get("tier"),
    )


def _records(item: dict) -> set[str]:
    return {r.get("id", "").split("/")[-1]
            for r in ((item.get("status", {}).get("experiment", {}) or {})
                      .get("containerRecords") or [])}


def _node_tiers() -> dict[str, str]:
    status, body = k8s.get("/api/v1/nodes")
    if status != 200:
        return {}
    return {n["metadata"]["name"]: n["metadata"].get("labels", {}).get("tier")
            for n in json.loads(body).get("items", [])
            if n["metadata"].get("labels", {}).get("tier")}


def _managed_stale(mine: list[dict]) -> bool:
    """True when any managed NetworkChaos's records diverge from the running
    worker pods on the tiers it covers (pod churn since injection)."""
    live = _live_worker_pods()
    if live is None:
        return False  # can't tell — don't churn the chaos on a blind guess
    pod_node, _ = live
    tiers = _node_tiers()
    pod_tier = {p: tiers.get(n) for p, n in pod_node.items()}
    for item in mine:
        sig = _sig(item)
        a, b = sig[4], sig[5]
        expected = {p for p, t in pod_tier.items() if t in (a, b)}
        if expected != _records(item):
            return True
    return False


def _managed_items() -> list[dict]:
    status, body = k8s.get(_chaos_path())
    if status != 200:
        return [{}]  # unknown — read as "still there" so waiters keep waiting
    return [i for i in json.loads(body).get("items", [])
            if (i.get("metadata", {}).get("labels") or {})
            .get(MANAGED_LABEL) == MANAGED_VALUE]


def apply_template_latency(template: dict) -> None:
    """Reconcile the controller-managed NetworkChaos with the template's
    `latency` field. Never raises — latency is auxiliary."""
    try:
        _apply_template_latency(template)
    except Exception:
        log.exception("chaos latency: apply failed; continuing")


def _apply_template_latency(template: dict) -> None:
    pairs = validate_latency(template.get("latency"))
    status, body = k8s.get(_chaos_path())
    if status == 404:
        if pairs:
            log.warning("chaos latency: template defines latency but Chaos "
                        "Mesh is not installed — skipping")
        return
    if status != 200:
        log.warning("chaos latency: list returned %s; skipping", status)
        return
    items = json.loads(body).get("items", [])
    mine = [i for i in items
            if (i.get("metadata", {}).get("labels") or {})
            .get(MANAGED_LABEL) == MANAGED_VALUE]
    others = [i["metadata"]["name"] for i in items if i not in mine]
    if pairs and others:
        log.warning("chaos latency: unmanaged NetworkChaos %s exist alongside "
                    "the template-defined latency — delays may stack; remove "
                    "them (e.g. kubectl delete -f manifests/network-chaos.yaml)",
                    others)

    desired = _latency_specs(pairs)
    in_sync = ({_sig(i) for i in mine} == {_sig(d) for d in desired})
    if in_sync and not (mine and _managed_stale(mine)):
        if mine:
            log.info("chaos latency: %d template-defined NetworkChaos in "
                     "sync — nothing to do", len(mine))
        return

    if not _workers_stable():
        log.warning("chaos latency: worker pods still churning after %ss; "
                    "skipping (next materialise will retry)", STABLE_TIMEOUT_S)
        return

    if mine:
        log.info("chaos latency: replacing %d managed NetworkChaos",
                 len(mine))
        for item in mine:
            k8s.delete(_chaos_path(item["metadata"]["name"]))
        if not _wait(lambda: not _managed_items(), DELETE_TIMEOUT_S):
            log.warning("chaos latency: old managed NetworkChaos still "
                        "terminating after %ss — the next materialise will "
                        "finish the replacement", DELETE_TIMEOUT_S)
            return
    if not desired:
        log.info("chaos latency: template defines no latency — managed "
                 "NetworkChaos removed")
        return

    for spec in desired:
        name = spec["metadata"]["name"]
        for attempt in range(3):
            status, resp = k8s.post(_chaos_path(), spec)
            if status in (200, 201, 409):
                break
            log.warning("chaos latency: create %s failed (try %d/3): %s %s",
                        name, attempt + 1, status, resp[:200])
            time.sleep(2)
    def _managed_injected() -> bool:
        mine_now = _managed_items()
        return (bool(mine_now) and mine_now != [{}]
                and all(_item_injected(i) for i in mine_now))

    if _wait(_managed_injected, INJECT_TIMEOUT_S):
        log.info("chaos latency: %d NetworkChaos applied from the template "
                 "and injected", len(desired))
    else:
        log.warning("chaos latency: not AllInjected after %ss — check "
                    "worker_peer_rtt_ms / kubectl describe networkchaos",
                    INJECT_TIMEOUT_S)


def delete_managed() -> int:
    """Remove the template-defined NetworkChaos. Called FIRST in teardown,
    while the worker pods are still alive — recovering rules from live pods
    is fast, whereas records pointing at dead pods stall the finalizers."""
    try:
        mine = _managed_items()
        if mine == [{}] or not mine:
            return 0
        for item in mine:
            k8s.delete(_chaos_path(item["metadata"]["name"]))
        log.info("chaos latency: deleted %d managed NetworkChaos", len(mine))
        return len(mine)
    except Exception:
        log.exception("chaos latency: delete failed; continuing")
        return 0
