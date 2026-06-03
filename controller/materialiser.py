"""
Template materialiser.

Takes a parsed template (a dict matching the schema documented in
manifests/worker-template.yaml) and turns it into a set of Kubernetes
resources: one ConfigMap + Deployment + Service per role.

This module is the single source of truth for "what does a template
become in the cluster". Both ingestion paths added in later steps —
the HTTP /templates endpoint and the ConfigMap watcher — call into
materialise() / teardown() here.

Step 2 only adds this module; nothing imports it yet. Step 3 wires it
up to the controller's HTTP handler.
"""

import json
import logging
import os
import time

import yaml

import k8s

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

# Which ingestion path created the template: "http" (POST /templates) or
# "watch" (declarative ConfigMap labelled with TEMPLATE_LABEL_FILTER, see
# watcher.py). The ConfigMap-watch reconciler only tears down templates
# whose source is "watch", so an HTTP-created template is never killed
# by a missing labelled ConfigMap.
SOURCE_ANNOTATION = "emulator.local/source"
SOURCE_HTTP = "http"
SOURCE_WATCH = "watch"


# ----------------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------------

def validate(template: dict) -> None:
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
    for edge in template.get("edges", []) or []:
        if edge.get("from") not in roles:
            raise ValueError(f"edge.from {edge.get('from')!r} not in roles")
        if edge.get("to") not in roles:
            raise ValueError(f"edge.to {edge.get('to')!r} not in roles")

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
                 count: int, image: str) -> list[dict]:
    """Substitute placeholders in the blueprint and parse out the docs."""
    rendered = (blueprint
                .replace("__TEMPLATE__", template_name)
                .replace("__ROLE__", role_name)
                .replace("__COUNT__", str(count))
                .replace("__IMAGE__", image))
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


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
) -> tuple[dict[str, list[str]], dict[str, dict[str, int]], dict[str, int]]:
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
    """
    name = template["name"]
    prefix = f"wt-{name}-"
    intended = compute_peers(template)
    resolved: dict[str, list[str]] = {role: [] for role in template["roles"]}
    offsets: dict[str, dict[str, int]] = {role: {} for role in template["roles"]}

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
            if svc not in ip_cache:
                trole = svc[len(prefix):]
                want = int(template["roles"][trole].get("count", 1))
                ip_cache[svc] = [ip for (_, ip)
                                 in _wait_for_endpoint_pods(svc, want)]
            for ip in ip_cache[svc]:
                if ip not in resolved[src_role]:
                    resolved[src_role].append(ip)

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

    return resolved, offsets, effective_server_count


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


def _compute_resolved_x(template: dict) -> dict[str, float]:
    """For each role, the x value its load formulas should evaluate at.

    The template's `x` is treated as a *signal* that propagates through
    the role graph rather than a global constant every role shares:

      - A source role (no inbound edges) uses the template's `x`.
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
    template_x = float(template.get("x", 0))

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
            resolved[r] = template_x
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
                 port_offset_by_pod: dict[str, int] | None = None) -> dict:
    """The config.json payload that goes into a role's ConfigMap.

    resolved_x is the x value this role's formulas should evaluate at.
    It's the template's x for source roles and the sum of upstream
    role-total egress for downstream roles (see _compute_resolved_x).

    port_offset_by_pod maps each source pod's name to the iperf3 port
    offset it should use when connecting: actual_port = BASE + offset.
    Assigning unique offsets across source pods avoids the iperf3
    single-session limit — two pods never fight over the same server port.
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


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

def materialise(template: dict, source: str = SOURCE_HTTP) -> None:
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

    `source` records which ingestion path triggered this call. Stamped
    into the SOURCE_ANNOTATION on every managed ConfigMap so the
    declarative watcher can tell HTTP-managed templates apart from its
    own and never tear those down.
    """
    validate(template)
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
        docs = _render_role(blueprint, name, role_name, count, WORKER_IMAGE)
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
        for doc in docs:
            _apply(doc)

    # ─── Phase 2: resolve peers to pod IPs and patch ConfigMaps ─────────
    log.info("Template %s: waiting for endpoints to populate…", name)
    peers_by_role, offsets_by_role, effective_sc = _resolve_peer_ips(template)
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
                         port_offset_by_pod=offsets_by_role[role_name] or None),
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


def patch_template(name: str, patch: dict) -> dict | None:
    """Merge `patch` into the existing template `name` and re-materialise.

    The merge is deep — see `_deep_merge` — so a patch only needs to
    specify the fields that actually change.  The template's `name`
    field is always taken from the URL parameter (any `name` in the
    patch body is ignored).  The source annotation (http vs watch) is
    preserved so a watch-managed template stays under watcher control
    after a PATCH; the next labelled-ConfigMap reconciliation will still
    overwrite it from the labelled-CM content, so callers PATCHing a
    watch-managed template should usually also update the labelled CM.

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
    # Strip name from the patch — the URL is the source of truth.
    sanitized = {k: v for k, v in patch.items() if k != "name"}
    merged = _deep_merge(existing, sanitized)
    merged["name"] = name
    validate(merged)
    log.info("Patching template %s (source=%s) with: %s", name, source,
             json.dumps(sanitized, separators=(",", ":")))
    materialise(merged, source=source)
    return merged


def teardown(name: str) -> int:
    """Delete every resource we created for `name`. Returns count deleted."""
    log.info("Tearing down template %s", name)
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{TEMPLATE_LABEL}={name}"
    deleted = 0
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


def list_managed_with_source(source: str) -> dict[str, dict]:
    """Map of template name → parsed template, for templates created by
    the given ingestion path. Used by the watcher to find templates it
    owns so it can compare against the labelled-CM expected set."""
    selector = f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}"
    status, body = k8s.get(_kind_path("ConfigMap", label_selector=selector))
    if status != 200:
        return {}
    result: dict[str, dict] = {}
    for cm in json.loads(body).get("items", []):
        labels = cm.get("metadata", {}).get("labels", {}) or {}
        ann = cm.get("metadata", {}).get("annotations", {}) or {}
        if ann.get(SOURCE_ANNOTATION) != source:
            continue
        name = labels.get(TEMPLATE_LABEL)
        tj = ann.get(TEMPLATE_ANNOTATION)
        if not name or not tj or name in result:
            continue
        try:
            result[name] = json.loads(tj)
        except json.JSONDecodeError:
            log.warning("template %s: annotation is not valid JSON", name)
    return result
