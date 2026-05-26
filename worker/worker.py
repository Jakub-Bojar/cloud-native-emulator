"""
Worker pod entry point.

Reads its desired behaviour from a JSON file on disk (sourced from a
mounted Kubernetes ConfigMap) and reacts to changes via watchdog. The
JSON specifies coefficients (a, b) for the linear function `a*x + b`
for each of CPU, RAM, and Network, plus the input value x.

The worker shells out to standard load generators:
  - CPU:     stress-ng
  - RAM:     anonymous mmap inside this process (deterministic RSS)
  - Network: iperf3 (loopback server + rate-limited client)

A /status endpoint reports what the worker is currently doing, and
/metrics exposes Prometheus gauges that Grafana scrapes.

Requires `stress-ng` and `iperf3` to be present in the container image.
"""

import json
import logging
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import psutil
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
      2. Bring up the network load (it has a non-trivial baseline footprint).
      3. Measure baseline cgroup memory + CPU.
      4. Start CPU and RAM loads sized to bring pod totals to target.

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
    # Distinguish "peers field absent" (legacy single-worker mode → loopback
    # iperf3) from "peers field present but empty" (templated role with no
    # outbound edges → keep the iperf3 server slot free for inbound peer
    # connections from other roles; spawning a loopback client here would
    # occupy the server and block all inbound traffic).
    raw_peers = payload.get("peers")
    templated_mode = raw_peers is not None
    if raw_peers is not None and (
            not isinstance(raw_peers, list)
            or not all(isinstance(p, str) for p in raw_peers)):
        log.warning("peers field is not a list[str], ignoring: %r", raw_peers)
        raw_peers = []
        templated_mode = False
    peers = raw_peers or []

    cpu_millicores = linear(cpu_a, cpu_b, x)
    ram_mb         = linear(ram_a, ram_b, x)
    net_mbps       = linear(net_a, net_b, x)

    if cpu_millicores == 0 and ram_mb == 0 and net_mbps == 0:
        log.info("Configured zero load — staying stopped")
        return

    if peers:
        mode_str = f"peers={peers}"
    elif templated_mode:
        mode_str = "peers=[] (templated, server-only)"
    else:
        mode_str = "peers=<loopback>"
    log.info("Configuring x=%.2f → CPU=%.0fm, RAM=%.1fMB, NET=%.2fMbps %s",
             x, cpu_millicores, ram_mb, net_mbps, mode_str)

    # Network first so iperf3 is part of the baseline that gets subtracted
    # from the CPU and RAM targets.
    server_count = payload.get("server_count")
    # The controller assigns each source pod a unique port offset so it
    # connects to a distinct iperf3 server port on every target pod.
    # This avoids the iperf3 single-session limit when multiple source
    # pods would otherwise all try the same port simultaneously.
    port_offset_by_pod: dict = payload.get("port_offset_by_pod") or {}
    my_port_offset: int = int(port_offset_by_pod.get(POD_NAME, 0))
    if port_offset_by_pod:
        log.info("Port offset for this pod (%s): %d", POD_NAME, my_port_offset)
    loads.start_network(net_mbps, peers, legacy_loopback=not templated_mode,
                        server_count=server_count,
                        my_port_offset=my_port_offset)
    # Loopback iperf3 reaches steady state within ~1 s. In peer mode the
    # supervisor threads have just spawned their clients but the peer pods
    # may still be coming up — we sleep the same 1 s anyway so the
    # baseline measurement window below includes whatever traffic we can
    # generate immediately; CPU subtraction stays correct as more peers
    # come online (the sampler thread keeps the actual gauge live).
    time.sleep(1.0)

    # Sample CPU over a 1 s window to get a meaningful baseline_mc.
    self_proc = psutil.Process(os.getpid())
    self_proc.cpu_percent(None)
    for c in self_proc.children(recursive=True):
        try:
            c.cpu_percent(None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    time.sleep(1.0)

    baseline_cpu_pct = self_proc.cpu_percent(None)
    for c in self_proc.children(recursive=True):
        try:
            baseline_cpu_pct += c.cpu_percent(None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # cgroup working set matches what Grafana plots; psutil sum is just
    # logged for comparison (it double-counts shared library pages).
    cgroup_baseline = metrics.read_cgroup_working_set_mb()
    psutil_baseline = metrics.sum_rss_mb(self_proc)
    baseline_rss_mb = cgroup_baseline if cgroup_baseline is not None else psutil_baseline
    baseline_mc = baseline_cpu_pct * 10.0
    log.info("Baseline (net+python): RAM=%.1fMB (cgroup=%s psutil=%.1f) CPU=%.0fm",
             baseline_rss_mb,
             f"{cgroup_baseline:.1f}" if cgroup_baseline is not None else "n/a",
             psutil_baseline, baseline_mc)

    loads.start_cpu(max(0.0, cpu_millicores - baseline_mc))

    # Rough initial RAM allocation. The sampler nudges this every 5 s to
    # close the remaining gap.
    loads.start_ram(max(0.0, ram_mb - baseline_rss_mb))

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
        elif self.path == "/healthz":
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
