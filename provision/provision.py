"""
Host-side infrastructure provisioner (Phase 1).

Reads the `sites` + `network_links` sections of a template and turns them
into joined, labelled MicroK8s worker nodes backed by Multipass VMs. This
runs on the Mac *host* — not inside the controller pod — because a pod
cannot drive the host's `multipass` daemon. It is the only genuinely new
component in the Multipass provisioning design; everything downstream
(workload materialisation, Chaos Mesh, monitoring) is unchanged.

It talks to the existing control plane by `multipass exec`-ing into the
control-plane VM and running `microk8s` there, so the host needs no kubectl
config of its own. The seam to the rest of the system is the node label
`topology.tier_id=<id>` (plus `tier=<name>` for the current materialiser's
nodeSelector); once a node carries it, workloads schedule onto it with no
controller changes.

Commands:
    provision.py up    <template.json>   # reconcile: create missing nodes, label all
    provision.py down  <template.json>   # drain + delete nodes, delete VMs
    provision.py status <template.json>  # show desired vs actual

Phase 1 only provisions nodes. It validates `network_links` but does not
inject latency/bandwidth — that is Chaos Mesh's job and only applies once
workload pods exist (Phase 3).
"""

# Lazy annotations so the modern `X | None` syntax works on the host's
# python3 (3.9), not just the 3.12 the controller runs in its container.
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time

log = logging.getLogger("provision")

# Control-plane VM that hosts the only apiserver. `microk8s add-node` and
# all kubectl calls are run inside it via `multipass exec`.
DEFAULT_CONTROL_PLANE = "microk8s-vm"
# Ubuntu image Multipass launches each worker from.
DEFAULT_IMAGE = "22.04"
# How long to wait for a freshly joined node to report Ready.
DEFAULT_READY_TIMEOUT = 300

# The seam: workloads land on a node because of these labels. tier_id is the
# forward-looking key the new schema's node_selector uses; tier_name/tier are
# kept so the current materialiser (nodeSelector {tier: <name>}) still works.
TIER_ID_LABEL = "topology.tier_id"
TIER_NAME_LABEL = "topology.tier_name"
COMPAT_TIER_LABEL = "tier"
# Marker labels so teardown can find what we created and never touch the
# control-plane node.
PROVISIONED_LABEL = "emulator.local/provisioned"
SITE_LABEL = "emulator.local/site"

# Installs MicroK8s and joins as a worker-only node. --worker keeps the node
# out of the HA control plane, which matters given the single control plane.
# The channel is pinned to the control plane's so a fresh node doesn't grab a
# newer MicroK8s than the apiserver (kubelet must not be newer than the
# control plane — see Kubernetes version-skew policy).
CLOUD_INIT = """#cloud-config
package_update: false
runcmd:
  - snap install microk8s --classic --channel={channel}
  - microk8s status --wait-ready
  - {join} --worker
"""


# ----------------------------------------------------------------------------
# Shell helpers
# ----------------------------------------------------------------------------

def run(cmd: list[str], *, check: bool = False,
        timeout: float | None = None) -> subprocess.CompletedProcess:
    """Run a command, capturing output. Raises RuntimeError if check and rc!=0."""
    log.debug("exec: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed (rc={proc.returncode}): {' '.join(cmd)}\n"
            f"{proc.stderr.strip()}")
    return proc


def kubectl(cp: str, *args: str, check: bool = False) -> subprocess.CompletedProcess:
    """Run `microk8s kubectl ...` inside the control-plane VM."""
    return run(["multipass", "exec", cp, "--", "microk8s", "kubectl", *args],
               check=check)


def sanitise(name: str) -> str:
    """Make a string a valid VM/host/k8s-node name (lowercase, '-', digits)."""
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


# ----------------------------------------------------------------------------
# Template handling
# ----------------------------------------------------------------------------

def load_template(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def validate_infrastructure(template: dict) -> None:
    """Raise ValueError on any problem in the `sites`/`network_links` sections."""
    sites = template.get("sites")
    if not isinstance(sites, list) or not sites:
        raise ValueError("template.sites must be a non-empty array")

    ids: set[str] = set()
    for site in sites:
        if not isinstance(site, dict):
            raise ValueError("each site must be an object")
        sid = site.get("site_id")
        if not isinstance(sid, str) or not sid:
            raise ValueError("site.site_id must be a non-empty string")
        if sid in ids:
            raise ValueError(f"duplicate site_id {sid!r}")
        ids.add(sid)
        if not isinstance(site.get("tier_id"), str) or not site["tier_id"]:
            raise ValueError(f"site {sid!r}.tier_id must be a non-empty string")
        cap = site.get("capacity")
        if cap is not None:
            if not isinstance(cap, dict):
                raise ValueError(f"site {sid!r}.capacity must be an object")
            for field in ("cpu_vcpu", "memory_gib"):
                val = cap.get(field)
                if val is not None and (not isinstance(val, (int, float)) or val <= 0):
                    raise ValueError(
                        f"site {sid!r}.capacity.{field} must be a positive number")
        prov = site.get("provision", {})
        if not isinstance(prov, dict):
            raise ValueError(f"site {sid!r}.provision must be an object")
        names = prov.get("node_names")
        if names is not None:
            # Explicit names map a site to specific existing/desired VMs (e.g.
            # your edge/fog/cloud). When present they are authoritative.
            if (not isinstance(names, list) or not names
                    or not all(isinstance(x, str) and x for x in names)):
                raise ValueError(
                    f"site {sid!r}.provision.node_names must be a non-empty list of strings")
            nc = prov.get("node_count")
            if nc is not None and nc != len(names):
                raise ValueError(
                    f"site {sid!r}: node_count ({nc}) != len(node_names) ({len(names)})")
        else:
            n = prov.get("node_count", 1)
            if not isinstance(n, int) or n < 1:
                raise ValueError(f"site {sid!r}.provision.node_count must be a positive int")

    for link in template.get("network_links", []) or []:
        for end in ("source_site_id", "target_site_id"):
            if link.get(end) not in ids:
                raise ValueError(f"network_link.{end} {link.get(end)!r} not in sites")
        # one_way_ms is the one-way latency between the two tiers (rtt_ms is
        # the deprecated name, still accepted).
        one_way = link.get("one_way_ms", link.get("rtt_ms", 0))
        if not isinstance(one_way, (int, float)) or one_way < 0:
            raise ValueError(
                f"network_link {link.get('link_id')!r}.one_way_ms must be >= 0")
        bw = link.get("bandwidth_mbps")
        if bw is not None and (not isinstance(bw, (int, float)) or bw <= 0):
            raise ValueError(f"network_link {link.get('link_id')!r}.bandwidth_mbps must be > 0")


def desired_nodes(template: dict) -> list[tuple[str, dict]]:
    """Expand sites into a flat [(node_name, site), ...] list.

    If a site's provision block lists `node_names`, those exact names are
    used (so a site can adopt existing VMs like edge/fog/cloud). Otherwise
    names are derived as `<sanitized site_id>-<i>`."""
    out: list[tuple[str, dict]] = []
    for site in template["sites"]:
        prov = site.get("provision", {})
        names = prov.get("node_names")
        if names:
            for name in names:
                out.append((name, site))
        else:
            base = sanitise(site["site_id"])
            for i in range(int(prov.get("node_count", 1))):
                out.append((f"{base}-{i}", site))
    return out


# ----------------------------------------------------------------------------
# Multipass / MicroK8s operations
# ----------------------------------------------------------------------------

def multipass_instances() -> set[str]:
    """Names of all existing Multipass VMs (empty set if multipass errors)."""
    proc = run(["multipass", "list", "--format", "json"])
    if proc.returncode != 0:
        return set()
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return set()
    return {inst["name"] for inst in data.get("list", [])}


def mint_join_command(cp: str) -> str:
    """Get a fresh one-time join command from the control plane.

    add-node tokens are single-use, so this is called once per node right
    before launch rather than reused across nodes."""
    proc = run(["multipass", "exec", cp, "--",
                "microk8s", "add-node", "--token-ttl", "3600", "--format", "short"],
               check=True)
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith("microk8s join"):
            return line
    raise RuntimeError(f"could not parse a join command from add-node:\n{proc.stdout}")


def detect_channel(cp: str) -> str:
    """Read the control plane's MicroK8s snap channel (e.g. '1.32/stable').

    Fresh worker nodes install this exact channel so kubelet matches the
    apiserver instead of jumping to the latest stable."""
    proc = run(["multipass", "exec", cp, "--", "snap", "list", "microk8s"],
               check=True)
    for line in proc.stdout.splitlines():
        parts = line.split()
        # Columns: Name Version Rev Tracking Publisher Notes
        if parts and parts[0] == "microk8s" and len(parts) >= 4:
            return parts[3]
    raise RuntimeError("could not detect microk8s channel on the control plane")


def render_cloud_init(join_cmd: str, channel: str) -> str:
    """Write a per-node cloud-init file with the join command + channel injected."""
    fd, path = tempfile.mkstemp(prefix="cloud-init-", suffix=".yaml")
    with os.fdopen(fd, "w") as f:
        f.write(CLOUD_INIT.format(join=join_cmd, channel=channel))
    return path


def _vm_size(site: dict) -> tuple[str, str, str]:
    """Resolve (cpus, memory, disk) for a site's VM.

    `capacity.cpu_vcpu` and `capacity.memory_gib` set the VM size — these are
    what you actually get. An optional `provision` block overrides any of
    cpus/memory/disk (and carries node_names); hard defaults apply otherwise."""
    cap = site.get("capacity") or {}
    prov = site.get("provision") or {}
    cpus = int(prov.get("cpus", cap.get("cpu_vcpu", 2)))
    memory = prov.get("memory") or f"{cap.get('memory_gib', 4)}G"
    disk = prov.get("disk") or f"{cap.get('disk_gib', 20)}G"
    return str(cpus), str(memory), str(disk)


def launch_node(name: str, site: dict, cloud_init_path: str, image: str) -> None:
    cpus, memory, disk = _vm_size(site)
    log.info("launching %s with %s vCPU / %s RAM / %s disk", name, cpus, memory, disk)
    run(["multipass", "launch", image,
         "--name", name,
         "--cpus", cpus,
         "--memory", memory,
         "--disk", disk,
         "--cloud-init", cloud_init_path,
         "--timeout", "600"],
        check=True, timeout=900)


def wait_ready(cp: str, node: str, timeout: float) -> None:
    """Poll until the node reports Ready=True, or raise TimeoutError."""
    jsonpath = ('jsonpath={.status.conditions[?(@.type=="Ready")].status}')
    deadline = time.time() + timeout
    while time.time() < deadline:
        proc = kubectl(cp, "get", "node", node, "-o", jsonpath)
        if proc.returncode == 0 and proc.stdout.strip() == "True":
            return
        time.sleep(5)
    raise TimeoutError(f"node {node} did not become Ready within {timeout:.0f}s")


def label_node(cp: str, node: str, site: dict) -> None:
    """Apply tier + marker labels (idempotent via --overwrite)."""
    labels = [
        f"{TIER_ID_LABEL}={site['tier_id']}",
        f"{PROVISIONED_LABEL}=true",
        f"{SITE_LABEL}={sanitise(site['site_id'])}",
    ]
    if site.get("tier_name"):
        labels.append(f"{TIER_NAME_LABEL}={site['tier_name']}")
        labels.append(f"{COMPAT_TIER_LABEL}={site['tier_name']}")
    kubectl(cp, "label", "node", node, *labels, "--overwrite", check=True)


def remove_node(cp: str, name: str) -> None:
    """Drain + delete the node, then delete and purge the VM. Best-effort."""
    log.info("draining %s", name)
    kubectl(cp, "drain", name, "--ignore-daemonsets",
            "--delete-emptydir-data", "--force", "--timeout=120s")
    kubectl(cp, "delete", "node", name)
    log.info("deleting VM %s", name)
    run(["multipass", "delete", name])
    run(["multipass", "purge"])


# ----------------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------------

def up(template: dict, *, control_plane: str = DEFAULT_CONTROL_PLANE,
       image: str = DEFAULT_IMAGE, channel: str | None = None,
       timeout: float = DEFAULT_READY_TIMEOUT) -> dict:
    """Reconcile the template's `sites` into joined, labelled nodes.

    Idempotent: existing VMs are adopted (skip launch), labels re-applied.
    Importable so the host-side service (server.py) can provision straight from
    a POST. Returns a summary dict."""
    validate_infrastructure(template)
    cp = control_plane
    nodes = desired_nodes(template)
    existing = multipass_instances()
    chan = channel or detect_channel(cp)
    for node, site in nodes:
        if node in existing:
            log.info("node %s already exists — skipping launch", node)
        else:
            log.info("provisioning node %s (site %s, tier %s, channel %s)",
                     node, site["site_id"], site["tier_id"], chan)
            join = mint_join_command(cp)
            cloud_init = render_cloud_init(join, chan)
            try:
                launch_node(node, site, cloud_init, image)
            finally:
                os.unlink(cloud_init)
        log.info("waiting for %s to become Ready", node)
        wait_ready(cp, node, timeout)
        label_node(cp, node, site)
        log.info("node %s Ready and labelled", node)

    return {
        "nodes": [{"node": n, "site_id": s["site_id"], "tier_id": s["tier_id"]}
                  for n, s in nodes],
        "channel": chan,
        "network_links": len(template.get("network_links", []) or []),
    }


def cmd_up(args: argparse.Namespace) -> int:
    template = load_template(args.template)
    summary = up(template, control_plane=args.control_plane, image=args.image,
                 channel=args.channel, timeout=args.timeout)
    log.info("provisioning complete: %d node(s); %d network_link(s) validated "
             "(injection deferred to Chaos Mesh)",
             len(summary["nodes"]), summary["network_links"])
    return 0


def down_nodes(names: list[str], *,
               control_plane: str = DEFAULT_CONTROL_PLANE) -> dict:
    """Drain + delete the named nodes and their VMs (best-effort, idempotent).

    Importable so the controller's DELETE can strip the infrastructure. Unlike
    cmd_down it takes explicit node names (the controller resolves them from the
    nodes' marker labels) rather than re-deriving them from a template."""
    existing = multipass_instances()
    removed: list[str] = []
    for name in names:
        if name in existing:
            remove_node(control_plane, name)
            removed.append(name)
        else:
            log.info("node %s not present — skipping", name)
    return {"removed": removed}


def cmd_down(args: argparse.Namespace) -> int:
    template = load_template(args.template)
    validate_infrastructure(template)
    cp = args.control_plane

    existing = multipass_instances()
    # Reverse so dependent/later nodes go first; purely cosmetic here.
    targets = [n for n, _ in reversed(desired_nodes(template)) if n in existing]
    if not targets:
        log.info("nothing to tear down")
        return 0
    if not args.yes:
        print("About to DRAIN + DELETE these nodes and delete their VMs:")
        for t in targets:
            print(f"  - {t}")
        if input("Continue? [y/N] ").strip().lower() not in ("y", "yes"):
            log.info("aborted")
            return 0
    for node in targets:
        remove_node(cp, node)
    log.info("teardown complete")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    template = load_template(args.template)
    validate_infrastructure(template)
    existing = multipass_instances()
    print(f"{'NODE':<24}{'TIER':<12}{'VM':<10}")
    for node, site in desired_nodes(template):
        present = "present" if node in existing else "missing"
        print(f"{node:<24}{site['tier_id']:<12}{present:<10}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Multipass infrastructure provisioner")
    parser.add_argument("--control-plane", default=DEFAULT_CONTROL_PLANE,
                        help=f"control-plane VM name (default {DEFAULT_CONTROL_PLANE})")
    parser.add_argument("--image", default=DEFAULT_IMAGE,
                        help=f"Multipass image (default {DEFAULT_IMAGE})")
    parser.add_argument("--channel", default=None,
                        help="MicroK8s snap channel for new nodes "
                             "(default: match the control plane)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_READY_TIMEOUT,
                        help="seconds to wait for a node to become Ready")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)
    for name, fn, help_ in (("up", cmd_up, "create missing nodes and label all"),
                            ("down", cmd_down, "drain + delete nodes and VMs"),
                            ("status", cmd_status, "show desired vs actual")):
        p = sub.add_parser(name, help=help_)
        p.add_argument("template", help="path to the template JSON")
        if name == "down":
            p.add_argument("-y", "--yes", action="store_true",
                           help="skip the confirmation prompt")
        p.set_defaults(func=fn)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s")

    try:
        return args.func(args)
    except (ValueError, RuntimeError, TimeoutError, FileNotFoundError) as e:
        log.error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
