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
import time
from urllib.parse import quote

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


def _live_worker_pods() -> tuple[set[str], bool] | None:
    """(running worker pod names, any_churning) or None on API error."""
    path = (f"/api/v1/namespaces/{k8s.namespace()}/pods"
            f"?labelSelector={quote(WORKER_SELECTOR)}")
    status, body = k8s.get(path)
    if status != 200:
        return None
    running: set[str] = set()
    churning = False
    for pod in json.loads(body).get("items", []):
        meta, st = pod.get("metadata", {}), pod.get("status", {})
        if meta.get("deletionTimestamp") or st.get("phase") == "Pending":
            churning = True
        elif st.get("phase") == "Running":
            running.add(meta.get("name", ""))
    return running, churning


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
    live_pods, _ = live
    if recorded == live_pods:
        log.info("chaos refresh: records match the %d live worker pods — "
                 "nothing to do", len(live_pods))
        return

    # Wait for the pod set to stop churning; a refresh mid-churn would just
    # be stale again (and racing the chaos-daemon is what corrupts the
    # merged tc rules).
    deadline = time.monotonic() + STABLE_TIMEOUT_S
    while time.monotonic() < deadline:
        live = _live_worker_pods()
        if live is not None and not live[1]:
            break
        time.sleep(3)
    else:
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


def _all_injected() -> bool:
    status, body = k8s.get(_chaos_path())
    if status != 200:
        return False
    items = json.loads(body).get("items", [])
    return bool(items) and all(
        c.get("status") == "True"
        for item in items
        for c in item.get("status", {}).get("conditions", [])
        if c.get("type") == "AllInjected")
