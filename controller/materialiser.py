"""
Template materialiser.

Takes a parsed template (a dict matching the schema documented in
manifests/worker-template.yaml) and turns it into a set of Kubernetes
resources: one ConfigMap + Deployment + Service per role.

This module is the single source of truth for "what does a template
become in the cluster". The HTTP POST /template handler (app.py) calls into
materialise() / teardown() here.
"""

import json
import logging
import os
import re
import time

import yaml

import k8s
import linkspec
import netem

log = logging.getLogger(__name__)

BLUEPRINT_PATH = os.environ.get("BLUEPRINT_PATH",
                                "/etc/emulator/worker-template.yaml")
WORKER_IMAGE = os.environ.get("WORKER_IMAGE",
                              "jp36/emulator-worker:latest")

# Must match IPERF_BASE_PORT in worker/state.py. The controller uses this
# when assigning explicit ip:port peer entries so source pods land on a
# port that is actually running in the target's iperf3 server pool.
IPERF_BASE_PORT = 9999

# Every resource we create is labelled so we can find/list/delete them
# later by label selector instead of by tracking names in memory.
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "emulator-controller"
TEMPLATE_LABEL = "template"
ROLE_LABEL = "role"

# Each managed ConfigMap carries the original template JSON as an
# annotation so GET /templates/<name> can reconstruct the template from
# the cluster without the controller maintaining in-memory state.
TEMPLATE_ANNOTATION = "emulator.local/template"

# Which ingestion path created the template. Currently always "http" (POST
# /template); stamped on every managed ConfigMap and surfaced by the read
# endpoints (api.overview / measurements). Kept as a labelled field so a future
# second ingestion path can be told apart without a schema change.
SOURCE_ANNOTATION = "emulator.local/source"
SOURCE_HTTP = "http"

# New-schema ("topology") ingestion. Worker nodes are labelled with the tier
# id by the host-side provisioner (provision/provision.py); apps pin to a tier
# via this label. The original posted document is stored under
# TOPOLOGY_ANNOTATION so it can be read back even though the topology is
# materialised through the internal role model (see translate_topology).
TIER_ID_LABEL = "topology.tier_id"
TOPOLOGY_ANNOTATION = "emulator.local/topology"
# Marker labels the host-side provisioner stamps on nodes it creates/adopts,
# so DELETE can find this topology's nodes to strip them.
PROVISIONED_NODE_LABEL = "emulator.local/provisioned"
SITE_NODE_LABEL = "emulator.local/site"


# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------

def validate_template(template: dict) -> None:
    """Raise ValueError on any structural problem in the template."""
    if not isinstance(template, dict):
        raise ValueError("template must be a JSON object")
    name = template.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("template.name must be a non-empty string")
    # k8s names: lowercase alphanumeric + '-', 1-63 chars after the
    # 'wt-' + '-' overhead we add per role.
    if not all(c.islower() or c.isdigit() or c == "-" for c in name):
        raise ValueError(f"template.name {name!r}: lowercase alphanumeric + '-' only")
    roles = template.get("roles")
    if not isinstance(roles, dict) or not roles:
        raise ValueError("template.roles must be a non-empty object")
    for role_name, role in roles.items():
        if not isinstance(role, dict):
            raise ValueError(f"role {role_name!r} must be an object")
        if not isinstance(role.get("count"), int) or role["count"] < 1:
            raise ValueError(f"role {role_name!r}.count must be a positive int")
        for axis in ("cpu", "ram", "net"):
            sub = role.get(axis)
            if not isinstance(sub, dict) or "a" not in sub or "b" not in sub:
                raise ValueError(f"role {role_name!r}.{axis} must have 'a' and 'b'")
        # Optional: pin this role's pods to a tier node (matches the node
        # label `tier=<value>`, e.g. edge/fog/cloud). Absent = schedule anywhere.
        tier = role.get("tier")
        if tier is not None and (not isinstance(tier, str) or not tier):
            raise ValueError(f"role {role_name!r}.tier must be a non-empty string")
        # Optional: pin to ONE specific node by name (matches the node's
        # kubernetes.io/hostname). Must be a node of the role's tier if both
        # are set, else the pod stays Pending. Shape-checked here; node
        # existence is left to the scheduler (Pending surfaces a bad name).
        node = role.get("node")
        if node is not None and not isinstance(node, str):
            raise ValueError(f"role {role_name!r}.node must be a string")
    for edge in template.get("edges", []) or []:
        if edge.get("from") not in roles:
            raise ValueError(f"edge.from {edge.get('from')!r} not in roles")
        if edge.get("to") not in roles:
            raise ValueError(f"edge.to {edge.get('to')!r} not in roles")

    # Optional inter-tier latency declaration; shape and durations are
    # validated here so a bad field 400s instead of half-materialising.
    linkspec.validate_latency(template.get("latency"))

    # Cycle detection.  x is resolved per role by a topological pass over
    # the role graph (see _compute_resolved_x); a cycle would make the
    # system underdetermined, so reject it here so the caller gets a clean
    # 400 instead of a half-materialised template.
    _compute_resolved_x(template)


# ----------------------------------------------------------------------------
# Blueprint rendering
# ----------------------------------------------------------------------------

def _load_blueprint() -> str:
    with open(BLUEPRINT_PATH) as f:
        return f.read()


def _render_role(blueprint: str, template_name: str, role_name: str,
                 count: int, image: str, tier: str | None = None,
                 node: str | None = None) -> list[dict]:
    """Substitute placeholders in the blueprint and parse out the docs.

    Placement is set on the Deployment's pod spec via `nodeSelector` (done on
    the parsed dict rather than string templating so YAML indentation can't
    break):
      - `tier`: pin to any node of that tier (`nodeSelector: {tier: <tier>}`).
      - `node`: pin to one specific node by name
        (`nodeSelector: {kubernetes.io/hostname: <node>}`) — k8s node name ==
        the hostname label here. Combined with `tier` it must be a node of
        that tier, else the pod stays Pending (no node matches both).

    Note on unpinning: clearing `node` (back to tier-level) re-renders without
    the hostname key, but a strategic-merge patch won't drop a key it doesn't
    mention — so materialise() removes a stale pin explicitly (see
    _unpin_node)."""
    rendered = (blueprint
                .replace("__TEMPLATE__", template_name)
                .replace("__ROLE__", role_name)
                .replace("__COUNT__", str(count))
                .replace("__IMAGE__", image))
    docs = [doc for doc in yaml.safe_load_all(rendered) if doc]
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        pod_spec = doc["spec"]["template"]["spec"]
        if tier:
            pod_spec.setdefault("nodeSelector", {})["tier"] = tier
        if node:
            pod_spec.setdefault("nodeSelector", {})["kubernetes.io/hostname"] = node
    return docs


def compute_peers(template: dict) -> dict[str, list[str]]:
    """For each role, the list of Service DNS names of its outbound peers.

    This is the *intent* — what the template says — without resolving to
    pod IPs. Used by GET /templates/<name> and as the input to peer
    resolution at materialise time. The actual peer addresses written
    into worker ConfigMaps are produced by _resolve_peer_ips() below."""
    name = template["name"]
    roles = template["roles"]
    peers: dict[str, list[str]] = {role: [] for role in roles}
    for edge in template.get("edges", []) or []:
        src, dst = edge["from"], edge["to"]
        dst_service = f"wt-{name}-{dst}"
        # Self-edge with count == 1 means the pod targets its own
        # single-pod Service — would route back to the same pod. Allow
        # it (intra-role mesh with count > 1 is the legitimate case)
        # but log so the surprise is visible.
        if src == dst and int(roles[dst].get("count", 1)) == 1:
            log.warning("template %s: self-edge on role %r with count=1 "
                        "will route back to the same pod", name, src)
        if dst_service not in peers[src]:
            peers[src].append(dst_service)
    return peers


def _get_endpoint_pods(service_name: str) -> list[tuple[str, str]]:
    """Return (pod_name, pod_ip) for each pod backing this Service.

    Uses the Endpoints `targetRef.name` field which k8s populates from
    the pod's own metadata.name — stable across re-lists."""
    ns = k8s.namespace()
    status, body = k8s.get(f"/api/v1/namespaces/{ns}/endpoints/{service_name}")
    if status != 200:
        return []
    data = json.loads(body)
    result: list[tuple[str, str]] = []
    for subset in data.get("subsets") or []:
        for addr in subset.get("addresses") or []:
            ip = addr.get("ip")
            ref = addr.get("targetRef") or {}
            pod_name = ref.get("name", "")
            if ip and pod_name:
                result.append((pod_name, ip))
            elif ip:
                result.append(("", ip))
    return result


def _wait_for_endpoint_pods(service_name: str, want_count: int,
                             timeout: float = 30.0) -> list[tuple[str, str]]:
    """Poll until at least `want_count` (pod_name, ip) pairs are Ready."""
    deadline = time.monotonic() + timeout
    last: list[tuple[str, str]] = []
    while time.monotonic() < deadline:
        last = _get_endpoint_pods(service_name)
        if len(last) >= want_count:
            return last
        time.sleep(1.0)
    if last:
        log.warning("endpoints for %s: got %d of %d expected within %.0fs",
                    service_name, len(last), want_count, timeout)
    else:
        log.warning("endpoints for %s: none ready after %.0fs",
                    service_name, timeout)
    return last


def _resolve_peer_ips(
        template: dict,
) -> tuple[dict[str, list[str]], dict[str, dict[str, int]], dict[str, int],
           dict[str, dict[str, str]]]:
    """Expand each role's peer Services into bare pod IPs and assign each
    source pod a port offset so it lands on a unique iperf3 server port on
    every target it connects to.

    A source pod uses a single offset (IPERF_BASE_PORT + offset) on *all* of
    its target pods, so any two source pods that share a target must get
    distinct offsets — otherwise they collide on that target's single-session
    iperf3 server port. Offsets are assigned by greedy colouring: each source
    pod takes the smallest offset not already claimed by another pod it shares
    a target role with. This is correct for arbitrary fan-out/fan-in — a
    source feeding several shared sinks — unlike a per-first-target counter,
    which lets offsets overlap on a sink that isn't a source's first target.

    Returns
    -------
    peers_by_role : dict[role, list[ip]]
        Bare pod IPs the worker should connect to (no port — the port is
        derived from the pod's own assigned offset).
    port_offset_by_pod_by_role : dict[role, dict[pod_name, offset]]
        Each source pod's port offset (0-based). The worker connects to
        IPERF_BASE_PORT + offset on every peer IP.
    effective_server_count : dict[role, int]
        iperf3 server slots each target role needs: (highest offset of any
        pod connecting to it) + 1. May exceed the raw fanin since greedy
        colouring isn't guaranteed minimal, but it never collides.
    peer_names_by_role : dict[role, dict[ip, target_role]]
        For each source role, the destination role name each peer IP belongs
        to. Written into the ConfigMap so the worker can label its per-peer
        metrics with a human-readable role instead of a bare pod IP.
    """
    name = template["name"]
    prefix = f"wt-{name}-"
    intended = compute_peers(template)
    resolved: dict[str, list[str]] = {role: [] for role in template["roles"]}
    offsets: dict[str, dict[str, int]] = {role: {} for role in template["roles"]}
    peer_names: dict[str, dict[str, str]] = {role: {} for role in template["roles"]}

    # Resolve each target Service → pod IPs once, and collect each source
    # role's pod list (sorted by name for deterministic, stable offsets).
    ip_cache: dict[str, list[str]] = {}
    src_pods_by_role: dict[str, list[tuple[str, str]]] = {}
    for src_role, services in intended.items():
        if not services:
            continue
        src_count = int(template["roles"][src_role].get("count", 1))
        src_pods_by_role[src_role] = sorted(
            _wait_for_endpoint_pods(prefix + src_role, src_count),
            key=lambda t: t[0])
        for svc in services:
            trole = svc[len(prefix):]
            if svc not in ip_cache:
                want = int(template["roles"][trole].get("count", 1))
                ip_cache[svc] = [ip for (_, ip)
                                 in _wait_for_endpoint_pods(svc, want)]
            for ip in ip_cache[svc]:
                if ip not in resolved[src_role]:
                    resolved[src_role].append(ip)
                # One pod backs exactly one Service, so an IP maps to a single
                # destination role — safe to assign unconditionally.
                peer_names[src_role][ip] = trole

    # Greedy colouring. used_by_target[role] is the set of offsets already
    # claimed by source pods connecting to that target role. A source pod
    # connects to *every* pod of each target role it points at, so all pods
    # of a target role share one offset namespace.
    used_by_target: dict[str, set[int]] = {r: set() for r in template["roles"]}
    for src_role in sorted(src_pods_by_role):
        target_roles = [svc[len(prefix):] for svc in intended[src_role]]
        for pod_name, _ in src_pods_by_role[src_role]:
            if not pod_name:
                continue
            offset = 0
            while any(offset in used_by_target[t] for t in target_roles):
                offset += 1
            offsets[src_role][pod_name] = offset
            for t in target_roles:
                used_by_target[t].add(offset)

    effective_server_count: dict[str, int] = {
        r: (max(used) + 1 if used else 0)
        for r, used in used_by_target.items()
    }

    return resolved, offsets, effective_server_count, peer_names


def _compute_fanin(template: dict) -> dict[str, int]:
    """Number of source pods that will open inbound iperf3 connections to each role.

    The worker uses this to size its iperf3 server pool exactly, rather than
    relying on the static IPERF_PORT_COUNT env-var guess."""
    roles = template["roles"]
    fanin: dict[str, int] = {role: 0 for role in roles}
    for edge in template.get("edges", []) or []:
        src, dst = edge["from"], edge["to"]
        fanin[dst] += int(roles[src].get("count", 1))
    return fanin


def _source_x_fn(template: dict):
    """Resolve the template's `x` spec into a `role -> input x` function for
    source roles (those that start a DAG).

    `x` may be either:
      - a single number — the system-wide signal every DAG source starts at
        (the original design), or
      - an object `{role: x}` — a per-source signal so each DAG's starting
        role gets its own input. A source role absent from the map starts at
        0 (idle), symmetric with a scalar applying to all sources.

    The map is keyed by ROLE here (the topology layer rekeys the author's
    source app_ids to roles before this point — see _x_to_roles)."""
    x_spec = template.get("x", 0)
    if isinstance(x_spec, dict):
        return lambda r: float(x_spec.get(r, 0) or 0)
    sx = float(x_spec or 0)
    return lambda r: sx


def _compute_resolved_x(template: dict) -> dict[str, float]:
    """For each role, the x value its load formulas should evaluate at.

    The template's `x` is treated as a *signal* that propagates through
    the role graph rather than a global constant every role shares:

      - A source role (no inbound edges) uses the template's `x`. `x` may be
        a single number shared by every DAG source, or a per-source map so
        each DAG's starting role gets its own input (see _source_x_fn).
      - A downstream role's `x` is the sum of upstream role-total egress.
        For an upstream role U with count N and net coefficients (a, b),
        U contributes  N * max(0, a * x_U + b)  Mbps to each downstream's x.

    This lets a sink's CPU/RAM/NET formulas scale with the *actual* traffic
    arriving at it instead of the raw input, which matches real distributed
    systems where downstream cost is driven by upstream load.

    Self-edges (intra-role mesh with count > 1) are legal traffic-wise but
    are skipped for x-resolution — a role can't feed its own x without
    making the system circular.

    Raises ValueError if the role graph contains a cycle.
    """
    roles = template["roles"]
    source_x = _source_x_fn(template)

    # Build dedup'd upstream / downstream adjacency over role pairs.
    upstreams: dict[str, set[str]] = {r: set() for r in roles}
    downstreams: dict[str, set[str]] = {r: set() for r in roles}
    for edge in template.get("edges", []) or []:
        src, dst = edge["from"], edge["to"]
        if src == dst:
            continue
        upstreams[dst].add(src)
        downstreams[src].add(dst)

    # Kahn's algorithm.  A role is ready to compute once every upstream
    # has produced its own x; at that point its x is the accumulated
    # sum of upstream role-total egress.
    resolved: dict[str, float] = {}
    accum: dict[str, float] = {r: 0.0 for r in roles}
    remaining: dict[str, int] = {r: len(upstreams[r]) for r in roles}

    queue: list[str] = []
    for r in roles:
        if remaining[r] == 0:
            resolved[r] = source_x(r)
            queue.append(r)

    head = 0
    while head < len(queue):
        r = queue[head]
        head += 1
        role = roles[r]
        per_pod_egress = max(0.0,
                             float(role["net"]["a"]) * resolved[r]
                             + float(role["net"]["b"]))
        role_total_egress = int(role["count"]) * per_pod_egress
        for dn in downstreams[r]:
            accum[dn] += role_total_egress
            remaining[dn] -= 1
            if remaining[dn] == 0:
                resolved[dn] = accum[dn]
                queue.append(dn)

    if len(resolved) != len(roles):
        unresolved = sorted(r for r in roles if r not in resolved)
        raise ValueError(
            f"role graph has a cycle involving: {', '.join(unresolved)}"
        )
    return resolved


def _role_config(template: dict, role_name: str, peers: list[str],
                 resolved_x: float,
                 server_count: int | None = None,
                 port_offset_by_pod: dict[str, int] | None = None,
                 peer_names: dict[str, str] | None = None) -> dict:
    """The config.json payload that goes into a role's ConfigMap.

    resolved_x is the x value this role's formulas should evaluate at.
    It's the template's x for source roles and the sum of upstream
    role-total egress for downstream roles (see _compute_resolved_x).

    port_offset_by_pod maps each source pod's name to the iperf3 port
    offset it should use when connecting: actual_port = BASE + offset.
    Assigning unique offsets across source pods avoids the iperf3
    single-session limit — two pods never fight over the same server port.

    peer_names maps each peer IP to the destination role it belongs to, so
    the worker can label its per-peer metrics with a readable role name.
    """
    role = template["roles"][role_name]
    cfg: dict = {
        "x": resolved_x,
        "cpu": role["cpu"],
        "ram": role["ram"],
        "net": role["net"],
        "peers": peers,
    }
    if server_count is not None:
        cfg["server_count"] = server_count
    if port_offset_by_pod:
        cfg["port_offset_by_pod"] = port_offset_by_pod
    if peer_names:
        cfg["peer_names"] = peer_names
    return cfg


# ----------------------------------------------------------------------------
# Kubernetes API plumbing
# ----------------------------------------------------------------------------

def _kind_path(kind: str, name: str | None = None,
               label_selector: str | None = None) -> str:
    ns = k8s.namespace()
    if kind == "Deployment":
        base = f"/apis/apps/v1/namespaces/{ns}/deployments"
    elif kind == "ConfigMap":
        base = f"/api/v1/namespaces/{ns}/configmaps"
    elif kind == "Service":
        base = f"/api/v1/namespaces/{ns}/services"
    else:
        raise ValueError(f"unsupported kind {kind!r}")
    if name is not None:
        return f"{base}/{name}"
    if label_selector:
        from urllib.parse import quote
        return f"{base}?labelSelector={quote(label_selector)}"
    return base


def _apply(resource: dict) -> None:
    """Idempotent create-or-update for a single resource dict."""
    kind = resource["kind"]
    name = resource["metadata"]["name"]
    status, body = k8s.post(_kind_path(kind), resource)
    if status in (200, 201):
        log.info("created %s/%s", kind, name)
        return
    if status == 409:
        # Already exists: strategic merge patch with the new fields.
        status, body = k8s.patch(_kind_path(kind, name), resource)
        if 200 <= status < 300:
            log.info("patched %s/%s", kind, name)
            return
    log.error("apply %s/%s failed: status=%s body=%s",
              kind, name, status, body[:300])
    raise RuntimeError(f"k8s API error applying {kind}/{name}: {status}")


def _unpin_node(deployment: str) -> None:
    """Drop a kubernetes.io/hostname node pin from a Deployment if present.

    A strategic-merge patch with the key set to null deletes it (valid because
    this only ever runs as an update, never a create). No-op when there's no
    pin. Lets a role move back to tier-level placement after being node-pinned,
    since a normal patch won't remove a nodeSelector key it doesn't mention."""
    patch = {"spec": {"template": {"spec": {"nodeSelector": {
        "kubernetes.io/hostname": None}}}}}
    status, _ = k8s.patch(_kind_path("Deployment", deployment), patch)
    if not (200 <= status < 300) and status != 404:
        log.warning("unpin node on %s: status=%s", deployment, status)


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

def materialise(template: dict, source: str = SOURCE_HTTP,
                extra_annotations: dict | None = None) -> None:
    """Create (or update) all Kubernetes resources for the template.

    Two phases:
      1. Apply every Deployment/Service/ConfigMap with an empty peers
         list. Workers come up with their HTTP servers + iperf3 server
         pool but no outbound peer traffic.
      2. Wait for each Service's Endpoints to populate, then patch the
         per-role ConfigMap's `peers` field with the concrete pod IPs
         that are now backing each target Service. Workers re-read the
         file (via their watchdog) and spawn one iperf3 client per
         resolved peer IP.

    The two-phase approach avoids a startup race: if we wrote peer IPs
    in phase 1 they'd be empty (Endpoints haven't populated yet) and we'd
    have no way to recover without an external reconciler.

    `source` records which ingestion path triggered this call (currently
    always SOURCE_HTTP). Stamped into the SOURCE_ANNOTATION on every managed
    ConfigMap and surfaced by the read endpoints.
    """
    validate_template(template)
    name = template["name"]
    log.info("Materialising template %s (source=%s)", name, source)
    blueprint = _load_blueprint()
    template_annotation = json.dumps(template, separators=(",", ":"))
    fanin = _compute_fanin(template)
    # x propagates through the role graph: source roles evaluate their
    # formulas at the template's x, downstream roles at the sum of
    # upstream role-total egress.  Computed once here and threaded
    # through both phases so Phase 1's initial config and Phase 2's
    # peer-IP patch agree.  validate() above already guarantees this
    # won't raise (cycles are rejected there).
    resolved_x = _compute_resolved_x(template)
    log.info("Template %s: resolved x per role = %s",
             name, {r: round(v, 3) for r, v in resolved_x.items()})

    # ─── Phase 1: create resources with empty peers ─────────────────────
    for role_name, role in template["roles"].items():
        count = int(role["count"])
        docs = _render_role(blueprint, name, role_name, count, WORKER_IMAGE,
                            tier=role.get("tier"), node=role.get("node"))
        config_payload = json.dumps(
            _role_config(template, role_name, [],
                         resolved_x=resolved_x[role_name],
                         server_count=fanin[role_name] or None),
            indent=2,
        )
        for doc in docs:
            if doc.get("kind") == "ConfigMap":
                doc.setdefault("data", {})["config.json"] = config_payload
                meta = doc.setdefault("metadata", {})
                ann = meta.setdefault("annotations", {})
                ann[TEMPLATE_ANNOTATION] = template_annotation
                ann[SOURCE_ANNOTATION] = source
                if extra_annotations:
                    ann.update(extra_annotations)
        for doc in docs:
            _apply(doc)
        # If the role isn't pinned to a node, clear any leftover pin from a
        # previous materialise (strategic-merge won't drop an unmentioned key,
        # so null it explicitly — this only runs as an update, where null is
        # the delete directive).
        if not role.get("node"):
            _unpin_node(f"wt-{name}-{role_name}")

    # ─── Phase 2: resolve peers to pod IPs and patch ConfigMaps ─────────
    log.info("Template %s: waiting for endpoints to populate…", name)
    peers_by_role, offsets_by_role, effective_sc, peer_names_by_role = \
        _resolve_peer_ips(template)
    for role_name in template["roles"]:
        peer_ips = peers_by_role[role_name]
        sc = effective_sc.get(role_name) or None

        # Patch roles that have outbound peers (the common case) OR roles
        # that are pure sinks whose effective server count differs from the
        # Phase 1 fanin estimate. The latter happens when a source role has
        # an inflated offset (because it shares a target with another source
        # role), causing it to connect to a higher-numbered port on the sink
        # than the sink's raw fanin would suggest — e.g. batch→cache AND
        # batch→storage where batch gets offsets 2,3 (not 0,1) because web
        # was processed first for cache, so storage also needs 4 servers
        # even though only 2 pods connect to it.
        needs_sc_update = sc is not None and sc != (fanin[role_name] or None)
        if not peer_ips and not needs_sc_update:
            continue

        cm_name = f"wt-{name}-{role_name}-config"
        config_payload = json.dumps(
            _role_config(template, role_name, peer_ips,
                         resolved_x=resolved_x[role_name],
                         server_count=sc,
                         port_offset_by_pod=offsets_by_role[role_name] or None,
                         peer_names=peer_names_by_role[role_name] or None),
            indent=2,
        )
        patch_body = {"data": {"config.json": config_payload}}
        status, body = k8s.patch(_kind_path("ConfigMap", cm_name), patch_body)
        if 200 <= status < 300:
            log.info("Template %s: wrote %d peer IPs + offsets %s (server_count=%s) into %s",
                     name, len(peer_ips), offsets_by_role[role_name], sc, cm_name)
        else:
            log.warning("Template %s: peer-IP patch on %s failed: %s",
                        name, cm_name, status)

    # ─── Phase 3: reconcile inter-tier link shaping (latency + bandwidth) ──
    # Latency/bandwidth are properties of the inter-NODE link, applied with tc
    # on each node's NIC (see netem.py): pods inherit them automatically, so
    # scaling a tier never disturbs the injection — unlike the old Chaos Mesh
    # per-pod approach, which raced pod-IP propagation and left scaled-in pods
    # half-shaped. Remove any leftover managed NetworkChaos from that approach
    # so the two mechanisms don't both shape the same links. Never fails the
    # materialise.
    linkspec.delete_managed()
    netem.apply(template)


def _deep_merge(base: dict, patch: dict) -> dict:
    """Recursively merge `patch` into a copy of `base`.

    Dict values merge recursively; scalars and lists in `patch` replace
    whatever's in `base`.  Neither input is mutated.  This is the merge
    semantics used by PATCH /templates/<name>: a partial template like
    `{"roles": {"middle": {"net": {"a": 0.3}}}}` updates just middle's
    net.a while leaving net.b and every other field untouched.
    """
    result = dict(base)
    for k, v in patch.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _expand_dot_keys(obj):
    """Expand dot-path keys in a patch into nested dicts, recursively.

    Lets callers write a flat shorthand instead of deeply nested JSON, e.g.
        {"x": 80, "roles.ingest.cpu.a": 5, "roles.ingest.cpu.b": 100}
    expands to
        {"x": 80, "roles": {"ingest": {"cpu": {"a": 5, "b": 100}}}}
    which then goes through the normal _deep_merge.  Role/field names never
    contain dots (validate() restricts role names to [a-z0-9-]), so a dot in
    a key is unambiguously a path separator.  Non-dict values and dot-free
    keys pass through unchanged, so existing nested patches still work."""
    if not isinstance(obj, dict):
        return obj
    result: dict = {}
    for key, value in obj.items():
        value = _expand_dot_keys(value)
        parts = key.split(".") if isinstance(key, str) else [key]
        cursor = result
        for part in parts[:-1]:
            nxt = cursor.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cursor[part] = nxt
            cursor = nxt
        leaf = parts[-1]
        if isinstance(cursor.get(leaf), dict) and isinstance(value, dict):
            cursor[leaf] = _deep_merge(cursor[leaf], value)
        else:
            cursor[leaf] = value
    return result


def patch_template(name: str, patch: dict) -> dict | None:
    """Merge `patch` into the existing template `name` and re-materialise.

    The merge is deep — see `_deep_merge` — so a patch only needs to
    specify the fields that actually change.  Patch keys may also use
    dot-path shorthand (see `_expand_dot_keys`): `{"roles.ingest.cpu.a": 5}`
    is equivalent to the fully-nested form.  The template's `name`
    field is always taken from the URL parameter (any `name` in the
    patch body is ignored).  The source annotation is preserved across
    the re-materialise.

    Returns the merged template on success, or None if no template
    named `name` exists.  Raises ValueError if the merged template
    fails validation (including cycle detection), and RuntimeError if a
    Kubernetes API call fails during re-materialisation.  materialise()
    is idempotent, so a partial failure leaves the cluster in a
    well-defined state that a retry can recover.
    """
    info = get_managed(name)
    if info is None or not info.get("template"):
        return None
    existing = info["template"]
    source = info.get("source") or SOURCE_HTTP
    # Expand dot-path shorthand, then strip name (the URL is the source of
    # truth), then deep-merge onto the existing template.
    expanded = _expand_dot_keys(patch)
    sanitized = {k: v for k, v in expanded.items() if k != "name"}
    merged = _deep_merge(existing, sanitized)
    merged["name"] = name
    validate_template(merged)
    log.info("Patching template %s (source=%s) with: %s", name, source,
             json.dumps(sanitized, separators=(",", ":")))
    materialise(merged, source=source)
    return merged


def teardown(name: str) -> int:
    """Delete every resource we created for `name`. Returns count deleted."""
    log.info("Tearing down template %s", name)
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{TEMPLATE_LABEL}={name}"
    # Remove inter-tier link shaping (tc on the nodes) and any leftover
    # Chaos Mesh NetworkChaos from the previous approach.
    netem.teardown()
    deleted = linkspec.delete_managed()
    # Deployments first so pods stop using the ConfigMap before it goes.
    for kind in ("Deployment", "Service", "ConfigMap"):
        path = _kind_path(kind, label_selector=selector)
        status, body = k8s.delete(path)
        if status in (200, 202):
            try:
                obj = json.loads(body)
                if obj.get("kind", "").endswith("List"):
                    deleted += len(obj.get("items", []))
                else:
                    deleted += 1
            except json.JSONDecodeError:
                deleted += 1
        elif status == 404:
            continue
        else:
            log.warning("delete %s for template %s: status=%s body=%s",
                        kind, name, status, body[:200])
    return deleted


def list_managed() -> list[str]:
    """Names of templates currently materialised in the cluster."""
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
    status, body = k8s.get(_kind_path("ConfigMap", label_selector=selector))
    if status != 200:
        return []
    names: set[str] = set()
    for item in json.loads(body).get("items", []):
        labels = item.get("metadata", {}).get("labels", {})
        if TEMPLATE_LABEL in labels:
            names.add(labels[TEMPLATE_LABEL])
    return sorted(names)


def get_managed(name: str) -> dict | None:
    """Reconstruct a materialised template's full state from the cluster.

    Returns None if no resources are labelled with the given template
    name. Otherwise returns a dict combining:
      - the original template (read from the annotation we stamped at
        materialise time),
      - the resolved peers per role (recomputed from the template's
        edges so it stays in sync if the annotation lags),
      - the current replica counts as reported by each role's Deployment
        (so a `kubectl scale` is visible here),
      - the names of the resources we created.
    """
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{TEMPLATE_LABEL}={name}"

    cm_status, cm_body = k8s.get(_kind_path("ConfigMap", label_selector=selector))
    if cm_status != 200:
        return None
    cms = json.loads(cm_body).get("items", [])
    if not cms:
        return None

    # All ConfigMaps for one template carry the same annotations.
    template_json = None
    source = None
    for cm in cms:
        ann = cm.get("metadata", {}).get("annotations", {}) or {}
        template_json = template_json or ann.get(TEMPLATE_ANNOTATION)
        source = source or ann.get(SOURCE_ANNOTATION)
        if template_json and source:
            break
    template: dict | None = None
    if template_json:
        try:
            template = json.loads(template_json)
        except json.JSONDecodeError:
            log.warning("template %s: annotation is not valid JSON", name)

    dep_status, dep_body = k8s.get(_kind_path("Deployment", label_selector=selector))
    replicas: dict[str, int] = {}
    if dep_status == 200:
        for dep in json.loads(dep_body).get("items", []):
            labels = dep.get("metadata", {}).get("labels", {})
            role = labels.get(ROLE_LABEL)
            if role:
                replicas[role] = int(dep.get("spec", {}).get("replicas", 0))

    return {
        "name": name,
        "source": source,
        "template": template,
        "peers": compute_peers(template) if template else {},
        "replicas": replicas,
        "configmaps": sorted(cm.get("metadata", {}).get("name") for cm in cms),
    }


# ----------------------------------------------------------------------------
# New-schema (topology) ingestion
# ----------------------------------------------------------------------------
#
# The controller is an in-cluster pod and cannot provision VMs, so it does NOT
# create the `sites` — that is the host-side provisioner's job
# (provision/provision.py). When the full topology document is POSTed here the
# controller instead: validates the document, confirms the tier nodes the apps
# need already exist (check_sites), translates the apps into the internal role
# model, and reuses materialise() so all existing observability keeps working.
# Each app's load function (cpu/ram/net a/b) comes from its apps[].load block;
# an app with no load block stays idle. The app-to-app traffic graph comes from
# app_edges (translated to role edges), so inter-app network load and x
# propagation work exactly as in the legacy schema. The signal `x` is injected
# at the DAG sources and propagated down that graph (the original role-model
# design); a phase sets it either once for the whole system or once per DAG
# (keyed by source app_id). runtime_scenarios is the timeline that moves x over
# time. The system materialises at the first scenario phase's x (else the doc
# baseline `x`, default 0). The remaining phases are time windows carried in the
# stored document for the scenario runner — a background stepper that re-patches
# x on a clock.

def _sanitize_name(value: str) -> str:
    """Lowercase + DNS-label-safe ([a-z0-9-]) for use as a k8s resource name."""
    out = re.sub(r"[^a-z0-9-]+", "-", str(value).lower()).strip("-")
    return out or "topology"


def _default_name(doc: dict) -> str:
    """Fallback template name when the document omits a top-level `name`:
    the first deployment's namespace, else 'topology'."""
    for desc in doc.get("k8s_app_description", []) or []:
        ns = desc.get("namespace")
        if ns:
            return ns
    return "topology"


def topology_name(doc: dict) -> str:
    """The internal template name a topology document materialises under."""
    return _sanitize_name(doc.get("name") or _default_name(doc))


def validate_topology(doc: dict) -> None:
    """Raise ValueError on any structural problem in a new-schema document.

    Checks the `sites`, `network_links`, `k8s_app_description` and `apps`
    sections and their cross-references (link endpoints, deployment_ref, the
    node_selector tier id). Does NOT touch the cluster — node readiness is
    checked separately by check_sites at materialise time."""
    if not isinstance(doc, dict):
        raise ValueError("topology must be a JSON object")

    sites = doc.get("sites")
    if not isinstance(sites, list) or not sites:
        raise ValueError("topology.sites must be a non-empty array")
    tier_ids: set[str] = set()
    site_ids: set[str] = set()
    for site in sites:
        if not isinstance(site, dict):
            raise ValueError("each site must be an object")
        sid, tid = site.get("site_id"), site.get("tier_id")
        if not isinstance(sid, str) or not sid:
            raise ValueError("site.site_id must be a non-empty string")
        if not isinstance(tid, str) or not tid:
            raise ValueError(f"site {sid!r}.tier_id must be a non-empty string")
        site_ids.add(sid)
        tier_ids.add(tid)

    for link in doc.get("network_links", []) or []:
        for end in ("source_site_id", "target_site_id"):
            if link.get(end) not in site_ids:
                raise ValueError(f"network_link.{end} {link.get(end)!r} not in sites")

    descs = doc.get("k8s_app_description")
    if not isinstance(descs, list) or not descs:
        raise ValueError("topology.k8s_app_description must be a non-empty array")
    dep_ids: set[str] = set()
    for desc in descs:
        if not isinstance(desc, dict):
            raise ValueError("each k8s_app_description must be an object")
        did, dname = desc.get("deployment_id"), desc.get("deployment_name")
        if not isinstance(did, str) or not did:
            raise ValueError("k8s_app_description.deployment_id must be a non-empty string")
        if not isinstance(dname, str) or not dname:
            raise ValueError(f"k8s_app_description {did!r}.deployment_name must be a non-empty string")
        dep_ids.add(did)
        tid = (desc.get("node_selector") or {}).get(TIER_ID_LABEL)
        if tid is not None and tid not in tier_ids:
            raise ValueError(
                f"k8s_app_description {did!r}: node_selector "
                f"{TIER_ID_LABEL}={tid!r} has no matching site")
        replicas = desc.get("replicas", 1)
        if not isinstance(replicas, int) or replicas < 1:
            raise ValueError(f"k8s_app_description {did!r}.replicas must be a positive int")

    app_ids: set[str] = set()
    for app in doc.get("apps", []) or []:
        aid = app.get("app_id")
        app_ids.add(aid)
        ref = (app.get("app") or {}).get("deployment_ref")
        if ref is not None and ref not in dep_ids:
            raise ValueError(
                f"app {app.get('app_id')!r}: deployment_ref {ref!r} "
                "matches no k8s_app_description")
        # An app's `load` block defines the load FUNCTION it generates: the
        # cpu/ram/net coefficients {a, b} of `load = max(0, a*x + b)`. Optional —
        # an app with no load block stays idle. The signal `x` is NOT per-app; a
        # phase sets it system-wide or per-DAG-source (see runtime_scenarios).
        _validate_app_load(aid, app.get("load"))

    # app_edges: the app-to-app traffic graph (re-added for the new schema — the
    # legacy role/edge model's `edges`). Each endpoint must be an app that
    # deploys a workload (has a deployment_ref), so the source pods have a
    # concrete target Service to open iperf3 connections to. Translated into
    # role edges by translate_topology; resolved-x propagation, iperf peer
    # wiring, and fan-in sizing then all work exactly as in the legacy schema.
    role_by_app = _role_by_app_id(doc)
    for e in doc.get("app_edges", []) or []:
        if not isinstance(e, dict):
            raise ValueError("each app_edge must be an object")
        for end in ("source_app_id", "target_app_id"):
            v = e.get(end)
            if v not in app_ids:
                raise ValueError(f"app_edge.{end} {v!r} matches no app")
            if v not in role_by_app:
                raise ValueError(
                    f"app_edge.{end} {v!r}: app has no deployment_ref, so there "
                    "is no workload to connect")

    # runtime_scenarios drive `x` over time (system-wide or per-DAG). Validated
    # here so a bad scenario 400s instead of half-materialising.
    _validate_runtime_scenarios(doc)

    # Validate against the translated template so a bad graph 400s before any
    # provisioning: the inter-tier latency/bandwidth values must parse as Chaos
    # Mesh durations/rates, and the app-edge graph must be acyclic (x
    # propagation is underdetermined on a cycle — _compute_resolved_x raises).
    translated = translate_topology(doc)
    linkspec.validate_latency(translated.get("latency"))
    linkspec.validate_bandwidth(translated.get("bandwidth"))
    _compute_resolved_x(translated)


def _is_number(value) -> bool:
    """True for an int/float that is not a bool (bools are ints in Python)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_app_load(app_id, load) -> None:
    """Validate an app's optional `load` block: the `a`/`b` of any of cpu/ram/net
    (each defaulting to 0) for `load = max(0, a*x + b)`. Absent → the app is
    idle. Raises ValueError if malformed. The signal `x` is system-wide
    (runtime_scenarios), not per-app, so it does NOT live in an app's load."""
    if load is None:
        return
    if not isinstance(load, dict):
        raise ValueError(f"app {app_id!r}: load must be an object")
    for ax in ("cpu", "ram", "net"):
        sub = load.get(ax)
        if sub is None:
            continue
        if not isinstance(sub, dict):
            raise ValueError(f"app {app_id!r}: load.{ax} must be an object with a/b")
        for k in ("a", "b"):
            if k in sub and not _is_number(sub[k]):
                raise ValueError(f"app {app_id!r}: load.{ax}.{k} must be a number")


def _validate_runtime_scenarios(doc: dict) -> None:
    """Validate the optional runtime_scenarios timeline.

    runtime_scenarios drives the signal `x` over time: an ordered list of
    phases, each setting `x` over a `[start_min, end_min)` window in minutes.
    The windows form one contiguous timeline — the first starts at 0 and each
    next picks up where the previous ended (0–X, X–Y, Y–Z …).

    A phase's `x` is either:
      - a number — the system-wide signal every DAG source starts at, or
      - an object `{source_app_id: number}` — a per-DAG signal, so each DAG's
        starting app gets its own input. Keys must be DAG *source* apps (apps
        with a workload and no inbound app_edge); setting x on a downstream app
        has no effect since its load is derived from upstream traffic. A source
        omitted from the map starts that phase at 0 (idle).

    The cpu/ram/net coefficients live on apps[].load, so a phase may only set x
    (plus its window / phase_id)."""
    phases = doc.get("runtime_scenarios")
    if phases is None:
        return
    if not isinstance(phases, list):
        raise ValueError("runtime_scenarios must be an array of phases")
    sources = _source_app_ids(doc)
    prev_end = 0.0
    for i, ph in enumerate(phases):
        if not isinstance(ph, dict):
            raise ValueError("each runtime_scenarios phase must be an object")
        for ax in ("cpu", "ram", "net"):
            if ax in ph:
                raise ValueError(
                    f"runtime_scenarios phase must not set {ax} — cpu/ram/net "
                    "coefficients live on apps[].load; a phase only sets x")
        start, end = ph.get("start_min"), ph.get("end_min")
        if not _is_number(start) or not _is_number(end):
            raise ValueError(
                "each runtime_scenarios phase needs numeric start_min and "
                "end_min (minutes)")
        if end <= start:
            raise ValueError(
                f"runtime_scenarios phase end_min ({end}) must be greater than "
                f"start_min ({start})")
        # Contiguous timeline: phase i begins exactly where phase i-1 ended; the
        # first begins at 0. prev_end starts at 0, so the same check does both.
        if start != prev_end:
            if i == 0:
                raise ValueError(
                    f"runtime_scenarios: first phase must start at start_min 0 "
                    f"(got {start})")
            raise ValueError(
                f"runtime_scenarios phase start_min ({start}) must equal the "
                f"previous phase's end_min ({prev_end}) — phases must be "
                "contiguous (0–X, X–Y, Y–Z …)")
        prev_end = end
        _validate_phase_x(ph.get("x"), sources)


def _validate_phase_x(x, sources: set[str]) -> None:
    """Validate one runtime_scenarios phase's `x`: a number (system-wide), or a
    non-empty object mapping DAG source app_ids to numbers (per-DAG)."""
    if _is_number(x):
        return
    if not isinstance(x, dict):
        raise ValueError(
            "each runtime_scenarios phase must set x to a number (system-wide) "
            "or an object mapping source app_id → number (per-DAG)")
    if not x:
        raise ValueError("runtime_scenarios phase x map must not be empty")
    for app_id, val in x.items():
        if app_id not in sources:
            raise ValueError(
                f"runtime_scenarios phase x: {app_id!r} is not a DAG source app "
                f"(sources: {sorted(sources)}) — x can only start a DAG at its "
                "source app; downstream load is derived from upstream traffic")
        if not _is_number(val):
            raise ValueError(
                f"runtime_scenarios phase x[{app_id!r}] must be a number")


# ── App load + runtime scenarios ─────────────────────────────────────────────
# An app's load FUNCTION lives on the app (apps[].load): the cpu/ram/net
# coefficients {a, b} of the worker's `load = max(0, a*x + b)` formula (cpu in
# millicores, ram in MB, net in Mbps — the same units as the role model). The
# signal `x` is injected at the DAG sources and propagates down the role graph
# (see _compute_resolved_x). It is NOT per-app: a phase sets x either once for
# the whole system, or once per DAG (keyed by the DAG's source app_id) so each
# pipeline starts at its own input. runtime_scenarios is the timeline that moves
# x over time: an ordered list of phases, each setting `x` over a contiguous
# `[start_min, end_min)` window in minutes (0–X, X–Y, Y–Z …). The FIRST phase's
# x is what the system is materialised at (else the doc's baseline `x`, default
# 0); the scenario runner advances through the rest on a clock. The a/b never
# change — only x moves.

def _role_by_app_id(doc: dict) -> dict[str, str]:
    """Map each app_id to the role it deploys as (the sanitised
    deployment_name), following app_id → app.deployment_ref →
    k8s_app_description.deployment_name. Apps with no resolvable deployment
    are omitted."""
    role_by_dep = {
        desc["deployment_id"]: _sanitize_name(desc["deployment_name"])
        for desc in doc.get("k8s_app_description", []) or []
        if desc.get("deployment_id") and desc.get("deployment_name")
    }
    out: dict[str, str] = {}
    for app in doc.get("apps", []) or []:
        aid = app.get("app_id")
        ref = (app.get("app") or {}).get("deployment_ref")
        if aid and ref in role_by_dep:
            out[aid] = role_by_dep[ref]
    return out


def _source_app_ids(doc: dict) -> set[str]:
    """App ids that start a DAG: apps that deploy a workload (resolve to a role)
    and have no inbound app_edge. Their `x` is the input that propagates down
    the graph (see _compute_resolved_x); these are the only apps a phase's x map
    may key on, since a downstream app's load is derived, not set."""
    targets = {e.get("target_app_id")
               for e in (doc.get("app_edges") or []) if isinstance(e, dict)}
    return {aid for aid in _role_by_app_id(doc) if aid not in targets}


def _x_to_roles(x, role_by_app: dict[str, str]):
    """Normalise a phase/baseline `x` spec into the template's role space.

    A number passes through unchanged (system-wide x, applied to every DAG
    source). An object keyed by source app_id is rekeyed to the role each app
    deploys as, so _compute_resolved_x can look it up by role. app_ids with no
    resolvable workload are dropped — validation rejects those up front."""
    if not isinstance(x, dict):
        return float(x or 0)
    out: dict[str, float] = {}
    for app_id, val in x.items():
        role = role_by_app.get(app_id)
        if role is not None:
            out[role] = float(val or 0)
    return out


def _app_load(app: dict) -> dict | None:
    """The {cpu, ram, net} an app's `load` block resolves to (absent axes /
    coefficients default to 0), or None if the app declares no load. The signal
    `x` is system-wide (not per-app), so it is not part of an app's load."""
    load = app.get("load")
    if not isinstance(load, dict):
        return None
    def axis(name: str) -> dict:
        sub = load.get(name) or {}
        return {"a": float(sub.get("a", 0)), "b": float(sub.get("b", 0))}
    return {"cpu": axis("cpu"), "ram": axis("ram"), "net": axis("net")}


def _app_loads(doc: dict) -> dict[str, dict]:
    """Per-role load function resolved from apps[].load, keyed by role. Apps
    with no load block (or no resolvable deployment) are omitted."""
    role_by_app = _role_by_app_id(doc)
    out: dict[str, dict] = {}
    for app in doc.get("apps", []) or []:
        role = role_by_app.get(app.get("app_id"))
        load = _app_load(app)
        if role and load is not None:
            out[role] = load
    return out


def scenario_x_timeline(doc: dict) -> list[dict]:
    """The x timeline resolved from runtime_scenarios: an ordered list of phases
    — each `{phase_id, start_min, end_min, x}` — over which the signal `x` moves
    (the role a/b come from apps[].load and don't change). Each phase's `x` is
    normalised into role space: a number stays a number (system-wide), a
    per-DAG map authored by source app_id is rekeyed to roles (see _x_to_roles)
    so the runner can hand it straight to patch_system_x. The first phase
    supplies the initial x picked up by translate_topology."""
    role_by_app = _role_by_app_id(doc)
    phases: list[dict] = []
    for ph in doc.get("runtime_scenarios", []) or []:
        end = ph.get("end_min")
        phases.append({
            "phase_id": ph.get("phase_id"),
            "start_min": float(ph.get("start_min", 0)),
            "end_min": float(end) if end is not None else None,
            "x": _x_to_roles(ph.get("x", 0), role_by_app),
        })
    return phases


def _initial_x(doc: dict):
    """The system x at t=0: the first runtime_scenarios phase's x if any, else
    the doc's baseline `x` (default 0). Either a number (system-wide) or a
    role-keyed map (per-DAG), already normalised into role space."""
    phases = scenario_x_timeline(doc)
    if phases:
        return phases[0]["x"]
    return _x_to_roles(doc.get("x", 0), _role_by_app_id(doc))


def translate_topology(doc: dict) -> dict:
    """Translate a new-schema document into the internal role-model template.

    Each k8s_app_description becomes a role: count = replicas (default 1),
    pinned to its tier, with its cpu/ram/net {a, b} from the matching app's
    `load` block (idle if the app declares no load). The signal `x` is set on
    the template — the doc's t=0 value, i.e. the first runtime_scenarios phase
    if any, else the doc baseline `x`. It is a number (system-wide) or, for a
    per-DAG document, a map rekeyed from source app_id to role; either way it
    propagates down the role graph via _compute_resolved_x.
    network_links become inter-tier latency and bandwidth: each link's
    `one_way_ms` is the ONE-WAY latency between the two tiers (injected in
    full on each direction, so a packet takes that long each way) and
    `bandwidth_mbps` caps the link. app_edges become role edges (the app-to-app
    traffic graph), so
    source apps open iperf3 connections to their targets and `x`
    propagates down the graph — a source app evaluates at its input x, a
    downstream app at the traffic arriving from its upstreams (see
    _compute_resolved_x) — exactly as in the legacy schema."""
    tier_name_by_id = {s["tier_id"]: s.get("tier_name") for s in doc["sites"]}
    tier_name_by_site = {s["site_id"]: s.get("tier_name") for s in doc["sites"]}
    app_loads = _app_loads(doc)
    role_by_app = _role_by_app_id(doc)

    roles: dict[str, dict] = {}
    for desc in doc.get("k8s_app_description", []) or []:
        role = _sanitize_name(desc["deployment_name"])
        load = app_loads.get(role)
        role_def: dict = {
            "count": int(desc.get("replicas", 1)),
            "cpu": load["cpu"] if load else {"a": 0, "b": 0},
            "ram": load["ram"] if load else {"a": 0, "b": 0},
            "net": load["net"] if load else {"a": 0, "b": 0},
        }
        tid = (desc.get("node_selector") or {}).get(TIER_ID_LABEL)
        tier = tier_name_by_id.get(tid) if tid else None
        if tier:
            role_def["tier"] = tier
        # Optional: pin to one specific node by name (the k8s node / VM name).
        node = desc.get("node_name")
        if node:
            role_def["node"] = node
        roles[role] = role_def

    latency: dict[str, dict[str, str]] = {}
    bandwidth: dict[str, dict[str, str]] = {}
    for link in doc.get("network_links", []) or []:
        src = tier_name_by_site.get(link.get("source_site_id"))
        dst = tier_name_by_site.get(link.get("target_site_id"))
        if not src or not dst or src == dst:
            continue  # shaping is inter-tier; skip same-tier / unmapped links
        # one_way_ms is the ONE-WAY latency between the two tiers; it is
        # injected in full on each direction (rtt_ms accepted as the old name).
        one_way = link.get("one_way_ms", link.get("rtt_ms", 0))
        latency.setdefault(src, {})[dst] = f"{int(round(one_way))}ms"
        bw = link.get("bandwidth_mbps")
        if bw is not None:
            bandwidth.setdefault(src, {})[dst] = f"{bw}mbps"

    # app_edges → role edges. Endpoints already validated to resolve to roles;
    # any that don't (e.g. translate called on an unvalidated doc) are skipped.
    edges: list[dict] = []
    for e in doc.get("app_edges", []) or []:
        src = role_by_app.get(e.get("source_app_id"))
        dst = role_by_app.get(e.get("target_app_id"))
        if src and dst:
            edges.append({"from": src, "to": dst})

    template: dict = {"name": topology_name(doc), "x": _initial_x(doc),
                      "roles": roles, "edges": edges}
    if latency:
        template["latency"] = latency
    if bandwidth:
        template["bandwidth"] = bandwidth
    return template


def _ready_nodes_for_tier(tier_id: str) -> list[str]:
    """Names of Ready nodes carrying topology.tier_id=<tier_id>."""
    from urllib.parse import quote
    selector = quote(f"{TIER_ID_LABEL}={tier_id}")
    status, body = k8s.get(f"/api/v1/nodes?labelSelector={selector}")
    if status != 200:
        return []
    ready: list[str] = []
    for node in json.loads(body).get("items", []):
        conds = node.get("status", {}).get("conditions", []) or []
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds):
            ready.append(node.get("metadata", {}).get("name", ""))
    return ready


def check_sites(doc: dict) -> dict:
    """Confirm the tiers the apps target have Ready nodes in the cluster.

    The controller can't provision sites, so this verifies the host-side
    provisioner has already created them. Returns a per-tier readiness report;
    raises ValueError if any tier an app is pinned to has no Ready node."""
    tier_name_by_id = {s["tier_id"]: s.get("tier_name") for s in doc["sites"]}
    required = {
        (desc.get("node_selector") or {}).get(TIER_ID_LABEL)
        for desc in doc.get("k8s_app_description", []) or []
    }
    required.discard(None)

    report: dict[str, dict] = {}
    unsatisfied: list[str] = []
    for tid in sorted({s["tier_id"] for s in doc["sites"]}):
        nodes = _ready_nodes_for_tier(tid)
        report[tid] = {
            "tier_name": tier_name_by_id.get(tid),
            "ready_nodes": nodes,
            "ready": bool(nodes),
        }
        if tid in required and not nodes:
            unsatisfied.append(tid)

    if unsatisfied:
        raise ValueError(
            "no Ready node for tier id(s) " + ", ".join(sorted(unsatisfied)) +
            f" — provision them first (host-side: `provision.py up`); nodes "
            f"must carry the label {TIER_ID_LABEL}=<id>")
    return report


def _scenario_report(doc: dict) -> dict | None:
    """Summary of the x timeline for the POST response: phase count, the span in
    minutes (last phase's end_min), and the x range. x_min/x_max flatten across
    every DAG source for per-DAG phases. x_start is the first phase's x as set
    (a number, or a role-keyed map for a per-DAG phase). None when no
    runtime_scenarios are declared."""
    phases = scenario_x_timeline(doc)
    if not phases:
        return None
    ends = [p["end_min"] for p in phases if p.get("end_min") is not None]
    # A phase's x is a number (system-wide) or a role-keyed map (per-DAG);
    # flatten both into a flat list of scalars for the min/max range.
    flat = [v for p in phases
            for v in (p["x"].values() if isinstance(p["x"], dict) else [p["x"]])]
    return {
        "phases": len(phases),
        "span_min": max(ends) if ends else None,
        "x_start": phases[0]["x"],
        "x_min": min(flat) if flat else None,
        "x_max": max(flat) if flat else None,
    }


def materialise_topology(doc: dict, source: str = SOURCE_HTTP) -> dict:
    """Ingest a full new-schema document: validate, confirm sites exist, then
    materialise each app's cpu/ram/net load at the initial `x` (the first
    runtime_scenarios phase, else the doc baseline; a number for system-wide x
    or a per-DAG map keyed by source app; apps with no load block stay idle).
    Returns a summary report. The original document is stored under
    TOPOLOGY_ANNOTATION so it round-trips — including the full x-timeline, so the
    runner can read it back from the cluster."""
    validate_topology(doc)
    sites_report = check_sites(doc)
    translated = translate_topology(doc)
    x0 = translated["x"]
    loaded = sum(1 for rd in translated["roles"].values()
                 if any(rd[ax]["a"] or rd[ax]["b"] for ax in ("cpu", "ram", "net")))
    log.info("Materialising topology %s: %d app(s) (%d with load), %d app-edge(s) "
             "at system x=%s", translated["name"], len(translated["roles"]),
             loaded, len(translated["edges"]), x0)
    materialise(translated, source=source,
                extra_annotations={TOPOLOGY_ANNOTATION:
                                   json.dumps(doc, separators=(",", ":"))})
    return {
        "name": translated["name"],
        "sites": sites_report,
        "deployments": sorted(translated["roles"].keys()),
        "network_links": len(doc.get("network_links", []) or []),
        "app_edges": len(translated["edges"]),
        "peers": compute_peers(translated),
        "system_x_start": x0,
        "runtime_scenarios": _scenario_report(doc),
    }


def patch_system_x(name: str, new_x) -> None:
    """Re-patch every role's ConfigMap with a new x.

    `new_x` is either a number (system-wide — every DAG source moves together)
    or a role-keyed map `{role: x}` (per-DAG — each DAG's source moves on its
    own). The scenario runner hands over whatever the active phase declared,
    already rekeyed to roles by scenario_x_timeline.

    Lighter than patch_template({"x": …}) — reads the stored template once,
    recomputes per-role resolved x via the graph, then patches only the `x`
    field in each role's config.json. All other fields (peers, server_count,
    port_offset_by_pod) are preserved. Used by the scenario runner to advance
    x on a clock without touching Deployments, Services, or Chaos Mesh.

    Raises RuntimeError if no template named `name` is materialised.
    Per-role patch failures are logged as warnings and skipped (runner retries
    on the next tick).
    """
    info = get_managed(name)
    if info is None or not info.get("template"):
        raise RuntimeError(f"no template named {name!r} is materialised")
    template = dict(info["template"])
    template["x"] = new_x
    resolved_x = _compute_resolved_x(template)
    log.debug("patch_system_x %s x=%s → per-role %s",
              name, new_x, {r: round(v, 3) for r, v in resolved_x.items()})
    for role_name in template["roles"]:
        cm_name = f"wt-{name}-{role_name}-config"
        r_status, r_body = k8s.get(_kind_path("ConfigMap", cm_name))
        if r_status != 200:
            log.warning("patch_system_x: ConfigMap %s not found (status %s)",
                        cm_name, r_status)
            continue
        try:
            config = json.loads(
                json.loads(r_body).get("data", {}).get("config.json", "{}"))
        except json.JSONDecodeError:
            log.warning("patch_system_x: could not parse config.json in %s", cm_name)
            continue
        config["x"] = resolved_x[role_name]
        patch_body = {"data": {"config.json": json.dumps(config, indent=2)}}
        p_status, _ = k8s.patch(_kind_path("ConfigMap", cm_name), patch_body)
        if 200 <= p_status < 300:
            log.debug("patch_system_x: x=%.3f → %s", resolved_x[role_name], cm_name)
        else:
            log.warning("patch_system_x: patch on %s failed (status %s)",
                        cm_name, p_status)


def get_topology_doc(name: str) -> dict | None:
    """The original new-schema document stored at materialise time, or None for
    a legacy template. Read from the TOPOLOGY_ANNOTATION on a managed ConfigMap."""
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{TEMPLATE_LABEL}={name}"
    status, body = k8s.get(_kind_path("ConfigMap", label_selector=selector))
    if status != 200:
        return None
    for cm in json.loads(body).get("items", []):
        tj = (cm.get("metadata", {}).get("annotations", {}) or {}).get(TOPOLOGY_ANNOTATION)
        if tj:
            try:
                return json.loads(tj)
            except json.JSONDecodeError:
                return None
    return None


def topology_node_names(doc: dict) -> list[str]:
    """Cluster node names provisioned for this topology, found by the provisioner's
    marker labels. Used by DELETE to strip the infrastructure. Matches a node when
    its emulator.local/site label is one of the topology's (sanitised) site ids."""
    from urllib.parse import quote
    wanted = {_sanitize_name(s["site_id"]) for s in doc.get("sites", []) if s.get("site_id")}
    if not wanted:
        return []
    status, body = k8s.get(f"/api/v1/nodes?labelSelector={quote(PROVISIONED_NODE_LABEL + '=true')}")
    if status != 200:
        return []
    names: list[str] = []
    for node in json.loads(body).get("items", []):
        labels = node.get("metadata", {}).get("labels", {}) or {}
        if labels.get(SITE_NODE_LABEL) in wanted:
            names.append(node.get("metadata", {}).get("name", ""))
    return [n for n in names if n]
