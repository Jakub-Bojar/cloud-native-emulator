"""
Worker pod entry point.

Reads its desired behaviour from a JSON file on disk (sourced from a
mounted Kubernetes ConfigMap) and reacts to changes via watchdog. The
JSON specifies coefficients (a, b) for the linear function `a*x + b`
for each of CPU, RAM, and Network, plus the input value x.

The worker shells out to standard load generators:
  - CPU:     stress-ng
  - RAM:     anonymous mmap inside this process (deterministic RSS)
  - Network: iperf3 (server pool + per-peer rate-limited clients)

A /status endpoint reports what the worker is currently doing, and
/metrics exposes Prometheus gauges that Grafana scrapes.

Requires `stress-ng` and `iperf3` to be present in the container image.
"""

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

import loads
import metrics
import watcher
from state import CONFIG_PATH, POD_NAME, STATE, STATE_LOCK, linear

log = logging.getLogger(__name__)


def configure(payload: dict) -> None:
    """
    Apply a new configuration:
      1. Stop whatever was running.
      2. Bring up the network load.
      3. Seed the CPU load at the raw target; RAM starts unallocated.

    No baseline is measured here: the sampler's feedback loops
    (_adjust_cpu / _adjust_ram) measure the pod's real totals and resize
    stress-ng / the mmap until they converge on target, so the seed error
    (the iperf3 + python footprint) is corrected within a few cycles.

    payload shape:
        { "x": 50,
          "cpu": {"a": 10,  "b": 100},   # millicores
          "ram": {"a": 4,   "b": 64},    # MB
          "net": {"a": 0.1, "b": 1} }    # Mbps

    A payload with x=0 and all-zero coefficients is treated as a stop.
    """
    loads.stop_current()

    x = float(payload["x"])
    cpu_a, cpu_b = float(payload["cpu"]["a"]), float(payload["cpu"]["b"])
    ram_a, ram_b = float(payload["ram"]["a"]), float(payload["ram"]["b"])
    net_a, net_b = float(payload["net"]["a"]), float(payload["net"]["b"])
    # A role's peers are the concrete pod IPs it sends iperf3 traffic to,
    # written by the controller. Empty (or absent) means no outbound edges:
    # the iperf3 server pool stays up for inbound peer connections, no client.
    raw_peers = payload.get("peers")
    if raw_peers is not None and (
            not isinstance(raw_peers, list)
            or not all(isinstance(p, str) for p in raw_peers)):
        log.warning("peers field is not a list[str], ignoring: %r", raw_peers)
        raw_peers = None
    peers = raw_peers or []

    # Map of peer IP → destination role name, used only to label per-peer
    # metrics with a readable name. Optional; unmapped IPs fall back to the IP.
    raw_peer_names = payload.get("peer_names")
    if raw_peer_names is not None and not isinstance(raw_peer_names, dict):
        log.warning("peer_names field is not a dict, ignoring: %r",
                    raw_peer_names)
        raw_peer_names = None
    peer_names = raw_peer_names or {}

    cpu_millicores = linear(cpu_a, cpu_b, x)
    ram_mb         = linear(ram_a, ram_b, x)
    net_mbps       = linear(net_a, net_b, x)

    server_count = payload.get("server_count") or 0

    if cpu_millicores == 0 and ram_mb == 0 and net_mbps == 0 and not server_count:
        log.info("Configured zero load — staying stopped")
        return

    mode_str = f"peers={peers}" if peers else "peers=[] (server-only)"
    log.info("Configuring x=%.2f → CPU=%.0fm, RAM=%.1fMB, NET=%.2fMbps %s",
             x, cpu_millicores, ram_mb, net_mbps, mode_str)

    # The controller assigns each source pod a unique port offset so it
    # connects to a distinct iperf3 server port on every target pod.
    # This avoids the iperf3 single-session limit when multiple source
    # pods would otherwise all try the same port simultaneously.
    port_offset_by_pod: dict = payload.get("port_offset_by_pod") or {}
    my_port_offset: int = int(port_offset_by_pod.get(POD_NAME, 0))
    if port_offset_by_pod:
        log.info("Port offset for this pod (%s): %d", POD_NAME, my_port_offset)
    loads.start_network(net_mbps, peers,
                        server_count=server_count,
                        my_port_offset=my_port_offset)

    # Seed stress-ng at the full CPU target. The pod total briefly overshoots
    # by the iperf3 + python footprint; the CPU feedback loop (_adjust_cpu)
    # re-reads the pod's total CPU every CPU_ADJUST_INTERVAL_S and resizes
    # stress-ng until the total converges on target.
    loads.start_cpu(cpu_millicores)

    # RAM starts unallocated: the nudger (_adjust_ram) sizes the mmap against
    # the live cgroup working set within one 5 s cycle. Seeding from below
    # also keeps a target near the pod's memory limit from overshooting.

    with STATE_LOCK:
        STATE.update({
            "running": True,
            "x": x,
            "cpu_millicores": cpu_millicores,
            "ram_mb": ram_mb,
            "net_mbps": net_mbps,
            "formulas": {
                "cpu": {"a": cpu_a, "b": cpu_b},
                "ram": {"a": ram_a, "b": ram_b},
                "net": {"a": net_a, "b": net_b},
            },
            "peers": peers,
            "peer_names": peer_names,
        })

    metrics.TARGET_CPU.set(cpu_millicores)
    metrics.TARGET_RAM.set(ram_mb)
    metrics.TARGET_NET.set(net_mbps)
    metrics.INPUT_X.set(x)


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/status":
            with STATE_LOCK:
                self._send_json(200, dict(STATE))
        elif self.path == "/health":
            self._send_json(200, {"ok": True})
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._send_json(404, {"error": "not found"})

    def log_message(self, fmt, *args) -> None:
        log.info("HTTP %s", fmt % args)


def main() -> None:
    port = int(os.environ.get("WORKER_PORT", "8080"))
    threading.Thread(target=metrics.sampler_loop, daemon=True).start()
    watcher.start_config_watcher(CONFIG_PATH, configure)
    # Apply whatever config is already on disk at startup (the mounted
    # ConfigMap is present before the container starts).
    watcher.load_initial(CONFIG_PATH, configure)
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Worker listening on 0.0.0.0:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
