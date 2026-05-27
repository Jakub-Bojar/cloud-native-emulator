"""
Template materializer.

Takes a parsed template (a dict matching the schema documented in
manifests/worker-template.yaml) and turns it into a set of Kubernetes
resources: one ConfigMap + Deployment + Service per role.

This module is the single source of truth for "what does a template
become in the cluster". Both ingestion paths added in later steps —
the HTTP /templates endpoint and the ConfigMap watcher — call into
materialize() / teardown() here.

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
TEMPLATE_ANNOTATION = "emulator.anthropic.dev/template"

# Which ingestion path created the template: "http" (POST /templates) or
# "watch" (declarative ConfigMap labelled with TEMPLATE_LABEL_FILTER, see
# watcher.py). The ConfigMap-watch reconciler only tears down templates
# whose source is "watch", so an HTTP-created template is never killed
# by a missing labelled ConfigMap.
SOURCE_ANNOTATION = "emulator.anthropic.dev/source"
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
    resolution at materialize time. The actual peer addresses written
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
    source pod a deterministic port offset so it lands on a unique iperf3
    server port on every target pod.

    Returns
    -------
    peers_by_role : dict[role, list[ip]]
        Bare pod IPs the worker should connect to (no port — the port is
        derived from the pod's own assigned offset).
    port_offset_by_pod_by_role : dict[role, dict[pod_name, offset]]
        Each source pod's port offset (0-based). The worker connects to
        IPERF_BASE_PORT + offset on every peer IP.  Offsets are assigned
        globally across *all* source roles that connect to a given target:
        the first source role gets 0, 1, …, N-1; the next source role
        picks up at N, N+1, …; and so on.  This guarantees no two pods
        ever land on the same port on the same target, even when multiple
        source roles share a target (e.g. A→C and B→C).
    effective_server_count : dict[role, int]
        The number of iperf3 server slots each role actually needs.  Equal
        to (max offset assigned to any pod connecting to that role) + 1.
        May exceed the raw fanin when a source role's offset was "inflated"
        by a shared target it also connects to.
    """
    name = template["name"]
    intended = compute_peers(template)
    fanin = _compute_fanin(template)
    resolved: dict[str, list[str]] = {role: [] for role in template["roles"]}
    offsets: dict[str, dict[str, int]] = {role: {} for role in template["roles"]}

    # Cache target Service → pod IPs.
    ip_cache: dict[str, list[str]] = {}

    # Global offset counter per target role.  Tracks how many offset slots
    # have already been consumed across all source roles that connect to a
    # given target.  Each new source role picks up from here so offsets are
    # globally unique — no two pods land on the same server port on the same
    # target, regardless of how many different source roles point to it.
    next_offset: dict[str, int] = {}

    for src_role, services in intended.items():
        if not services:
            continue

        # Determine port offset for each source pod.
        # Sort by pod name for determinism (endpoint order is not guaranteed).
        src_svc = f"wt-{name}-{src_role}"
        src_count = int(template["roles"][src_role].get("count", 1))
        src_pods = _wait_for_endpoint_pods(src_svc, src_count)
        src_pods_sorted = sorted(src_pods, key=lambda t: t[0])

        # Derive the base offset for this source role from the first target
        # role it connects to (all targets share the same source-pod list so
        # the base only needs to be anchored to one of them).
        first_target_role = services[0][len(f"wt-{name}-"):]
        base = next_offset.get(first_target_role, 0)
        for idx, (pod_name, _) in enumerate(src_pods_sorted):
            if pod_name:
                offsets[src_role][pod_name] = base + idx
        # Advance the counter so the next source role connecting to this
        # target starts immediately after the last offset we just assigned.
        next_offset[first_target_role] = base + len(src_pods_sorted)

        for svc in services:
            if svc not in ip_cache:
                role_name = svc[len(f"wt-{name}-"):]
                want = int(template["roles"][role_name].get("count", 1))
                target_pods = _wait_for_endpoint_pods(svc, want)
                ip_cache[svc] = [ip for (_, ip) in target_pods]
            for ip in ip_cache[svc]:
                if ip not in resolved[src_role]:
                    resolved[src_role].append(ip)

    # Compute the effective server-pool size required by each target role.
    # Raw fanin is enough when all sources start from 0, but when a source
    # role's base offset was inflated (because it shares a target with
    # another role that was processed first), the target it *also* connects
    # to may need more server slots than its own fanin implies.
    effective_server_count: dict[str, int] = {r: 0 for r in template["roles"]}
    for src_role, pod_offsets in offsets.items():
        if not pod_offsets:
            continue
        max_offset = max(pod_offsets.values())
        for svc in intended.get(src_role, []):
            target_role = svc[len(f"wt-{name}-"):]
            effective_server_count[target_role] = max(
                effective_server_count[target_role], max_offset + 1
            )

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


def _role_config(template: dict, role_name: str, peers: list[str],
                 server_count: int | None = None,
                 port_offset_by_pod: dict[str, int] | None = None) -> dict:
    """The config.json payload that goes into a role's ConfigMap.

    port_offset_by_pod maps each source pod's name to the iperf3 port
    offset it should use when connecting: actual_port = BASE + offset.
    Assigning unique offsets across source pods avoids the iperf3
    single-session limit — two pods never fight over the same server port.
    """
    role = template["roles"][role_name]
    cfg: dict = {
        "x": template.get("x", 0),
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

def materialize(template: dict, source: str = SOURCE_HTTP) -> None:
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
    log.info("Materializing template %s (source=%s)", name, source)
    blueprint = _load_blueprint()
    template_annotation = json.dumps(template, separators=(",", ":"))
    fanin = _compute_fanin(template)

    # ─── Phase 1: create resources with empty peers ─────────────────────
    for role_name, role in template["roles"].items():
        count = int(role["count"])
        docs = _render_role(blueprint, name, role_name, count, WORKER_IMAGE)
        config_payload = json.dumps(
            _role_config(template, role_name, [],
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
    """Names of templates currently materialized in the cluster."""
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
    """Reconstruct a materialized template's full state from the cluster.

    Returns None if no resources are labelled with the given template
    name. Otherwise returns a dict combining:
      - the original template (read from the annotation we stamped at
        materialize time),
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
