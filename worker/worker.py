"""
Worker pod.

Reads its desired behaviour from a JSON file on disk (sourced from a
mounted Kubernetes ConfigMap) and reacts to changes via watchdog. The
JSON specifies coefficients (a, b) for the linear function `a*x + b`
for each of CPU, RAM, and Network, plus the input network value x.

The worker shells out to standard load generators:
  - CPU + RAM: stress-ng
  - Network:   iperf3 (loopback server + rate-limited client)

A /status endpoint reports what the worker is currently doing.

Requires `stress-ng` and `iperf3` to be present in the container image.
"""

import json
import math
import os
import shlex
import signal
import socket
import subprocess
import threading
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

import psutil
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(message)s",
)
log = logging.getLogger(__name__)

IPERF_PORT = 9999

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/etc/emulator/config.json")

STATE_LOCK = threading.Lock()
STATE = {
    "running": False,
    "x": None,
    "cpu_millicores": 0.0,
    "ram_mb": 0.0,
    "net_mbps": 0.0,
    "formulas": {},
}

PROCS: list[subprocess.Popen] = []

TARGET_CPU = Gauge("worker_target_cpu_millicores", "Configured CPU target (millicores)")
TARGET_RAM = Gauge("worker_target_ram_mb",         "Configured RAM target (MB)")
TARGET_NET = Gauge("worker_target_net_mbps",       "Configured network target (Mbps)")

ACTUAL_CPU = Gauge("worker_actual_cpu_millicores", "Measured CPU usage (millicores)")
ACTUAL_RAM = Gauge("worker_actual_ram_mb",         "Measured RAM usage (MB)")
ACTUAL_NET = Gauge("worker_actual_net_mbps",       "Measured loopback throughput (Mbps)")

INPUT_X = Gauge("worker_input_x", "Current network input value x")


def linear(a: float, b: float, x: float) -> float:
    return max(0.0, a * x + b)


def spawn(cmd: list[str]) -> subprocess.Popen:
    log.info("spawn: %s", shlex.join(cmd))
    log_path = f"/tmp/spawn-{cmd[0]}-{int(time.time()*1000)}.log"
    log_fh = open(log_path, "wb")
    p = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT)
    log.info("  -> pid=%d log=%s", p.pid, log_path)
    PROCS.append(p)
    return p


# CPU: stress-ng --cpu N --cpu-load P runs N workers each busy P% of the time.
# Total CPU% across the pod = N * P. We want total% = millicores / 10.
# kubectl top reflects this because the kernel accounts busy-loop time to the
# pod's cgroup. For exact usage, set the pod's CPU *limit* equal to the target.
def start_cpu(millicores: float) -> None:
    if millicores <= 0:
        return
    cores = os.cpu_count() or 1
    millicores = min(millicores, cores * 1000.0)
    # Pick the fewest workers that can absorb the requested load at <=100%
    # each, then use an integer --cpu-load (some stress-ng builds reject
    # fractional values).
    workers = max(1, math.ceil(millicores / 1000.0))
    load_per_worker = int(round(millicores / (10.0 * workers)))
    load_per_worker = max(1, min(100, load_per_worker))
    spawn([
        "stress-ng",
        "--cpu", str(workers),
        "--cpu-load", str(load_per_worker),
        "--cpu-method", "matrixprod",
    ])


# RAM: --vm-keep + --vm-hang 0 makes the worker write the buffer once then
# sleep, holding the allocation without burning CPU.
def start_ram(ram_mb: float) -> None:
    if ram_mb <= 0:
        return
    spawn([
        "stress-ng",
        "--vm", "1",
        "--vm-bytes", f"{int(ram_mb)}M",
        "--vm-keep",
        "--vm-hang", "0",
    ])


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            try:
                s.connect((host, port))
                return True
            except OSError:
                time.sleep(0.05)
    return False


def start_network(mbps: float) -> None:
    if mbps <= 0:
        return
    spawn(["iperf3", "-s", "-p", str(IPERF_PORT)])
    # iperf3's --connect-timeout caps a single connect() syscall but does
    # not retry on ECONNREFUSED, so the client races the server's bind().
    # Block until the server is actually listening before spawning the client.
    if not _wait_for_port("127.0.0.1", IPERF_PORT):
        log.warning("iperf3 server did not bind on :%d in time", IPERF_PORT)
        return
    # iperf3 caps -t at 86400s (24h). For longer runs, re-trigger configure.
    spawn([
        "iperf3", "-c", "127.0.0.1",
        "-p", str(IPERF_PORT),
        "-b", f"{mbps}M",
        "-t", "86400",
    ])


def stop_current() -> None:
    for p in PROCS:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in PROCS:
        try:
            p.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            p.kill()
    PROCS.clear()
    with STATE_LOCK:
        STATE.update({
            "running": False,
            "x": None,
            "cpu_millicores": 0.0,
            "ram_mb": 0.0,
            "net_mbps": 0.0,
            "formulas": {},
        })
    TARGET_CPU.set(0)
    TARGET_RAM.set(0)
    TARGET_NET.set(0)
    INPUT_X.set(0)
    log.info("Emulation stopped")


def configure(payload: dict) -> None:
    """
    payload shape:
    {
      "x": 50,
      "cpu": {"a": 10,  "b": 100},   # millicores
      "ram": {"a": 4,   "b": 64},    # MB
      "net": {"a": 0.1, "b": 1}      # Mbps
    }

    A payload with x=0 and all-zero coefficients is treated as a stop.
    """
    stop_current()

    x = float(payload["x"])
    cpu_a = float(payload["cpu"]["a"]); cpu_b = float(payload["cpu"]["b"])
    ram_a = float(payload["ram"]["a"]); ram_b = float(payload["ram"]["b"])
    net_a = float(payload["net"]["a"]); net_b = float(payload["net"]["b"])

    cpu_millicores = linear(cpu_a, cpu_b, x)
    ram_mb         = linear(ram_a, ram_b, x)
    net_mbps       = linear(net_a, net_b, x)

    if cpu_millicores == 0 and ram_mb == 0 and net_mbps == 0:
        log.info("Configured zero load — staying stopped")
        return

    log.info("Configuring x=%.2f → CPU=%.0fm, RAM=%.1fMB, NET=%.2fMbps",
             x, cpu_millicores, ram_mb, net_mbps)

    # Bring up network and any baseline-affecting work first so we can
    # measure the pod's pre-stressor RAM/CPU and size the stress-ng
    # allocations to fill the *remaining* gap to target — that way pod-total
    # usage lands on the target value, not target + baseline.
    start_network(net_mbps)
    time.sleep(1.0)  # let iperf3 reach steady state

    self_proc = psutil.Process(os.getpid())
    self_proc.cpu_percent(None)  # prime; first reading is 0
    children_for_baseline = list(self_proc.children(recursive=True))
    for c in children_for_baseline:
        try:
            c.cpu_percent(None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    time.sleep(1.0)  # accumulate a meaningful CPU-percent window

    baseline_rss_bytes = self_proc.memory_info().rss
    baseline_cpu_pct = self_proc.cpu_percent(None)
    for c in self_proc.children(recursive=True):
        try:
            baseline_rss_bytes += c.memory_info().rss
            baseline_cpu_pct += c.cpu_percent(None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    baseline_rss_mb = baseline_rss_bytes / (1024 * 1024)
    baseline_mc = baseline_cpu_pct * 10.0
    log.info("Baseline (net+python): RAM=%.1fMB CPU=%.0fm", baseline_rss_mb, baseline_mc)

    cpu_to_emit = max(0.0, cpu_millicores - baseline_mc)
    ram_to_emit = max(0.0, ram_mb - baseline_rss_mb)
    start_cpu(cpu_to_emit)
    start_ram(ram_to_emit)

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
        })

    TARGET_CPU.set(cpu_millicores)
    TARGET_RAM.set(ram_mb)
    TARGET_NET.set(net_mbps)
    INPUT_X.set(x)


def _validate(payload: dict) -> None:
    for key in ("x", "cpu", "ram", "net"):
        if key not in payload:
            raise KeyError(key)
    for sub in ("cpu", "ram", "net"):
        if "a" not in payload[sub] or "b" not in payload[sub]:
            raise KeyError(f"{sub}.a/b")


def _load_and_apply(path: str) -> None:
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        log.info("Config file %s missing — staying idle", path)
        return
    if not raw.strip():
        log.info("Config file %s empty — staying idle", path)
        return
    try:
        payload = json.loads(raw)
        _validate(payload)
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        log.error("Bad config in %s: %s", path, e)
        return
    try:
        configure(payload)
    except Exception:
        log.exception("configure() failed")


# Kubernetes refreshes a mounted ConfigMap by swapping the parent directory's
# `..data` symlink atomically, so an inotify watch on the file itself misses
# updates. Watch the *directory* and trigger on any event whose final path
# resolves to our config file.
class ConfigWatcher(FileSystemEventHandler):
    def __init__(self, path: str):
        self.path = os.path.realpath(path)
        self.dir = os.path.dirname(self.path)
        self._lock = threading.Lock()
        self._debounce_until = 0.0

    def _maybe_reload(self) -> None:
        # k8s' atomic swap produces a flurry of events in quick succession;
        # debounce so we only re-apply once per real change.
        now = time.monotonic()
        with self._lock:
            if now < self._debounce_until:
                return
            self._debounce_until = now + 0.5
        time.sleep(0.2)
        _load_and_apply(self.path)

    def on_any_event(self, event) -> None:
        if event.is_directory:
            self._maybe_reload()
            return
        try:
            event_path = os.path.realpath(event.src_path)
        except OSError:
            return
        if event_path == self.path or os.path.dirname(event_path) == self.dir:
            self._maybe_reload()


def start_config_watcher(path: str) -> Observer:
    watch_dir = os.path.dirname(path) or "."
    os.makedirs(watch_dir, exist_ok=True)
    handler = ConfigWatcher(path)
    observer = Observer()
    observer.schedule(handler, watch_dir, recursive=False)
    observer.daemon = True
    observer.start()
    log.info("Watching %s for config changes", path)
    return observer


# Sample once per second. CPU is measured over the elapsed interval so the
# first reading is meaningful; RAM is summed across the stress-ng tree; net
# is diffed on the loopback interface (iperf3 runs client+server there).
def sampler_loop(interval: float = 1.0) -> None:
    self_proc = psutil.Process(os.getpid())
    self_proc.cpu_percent(None)  # prime
    last_net = psutil.net_io_counters(pernic=True).get("lo")
    last_t = time.monotonic()

    # cpu_percent(None) returns CPU since the *previous* call on the same
    # Process object — the first call always returns 0.0. Cache children
    # across iterations so each one accumulates real samples.
    child_cache: dict[int, psutil.Process] = {}

    while True:
        time.sleep(interval)

        live_pids = set()
        # Pod-total usage: python itself + every descendant. start_cpu and
        # start_ram size their stressors to make this sum land on the target.
        cpu_pct = self_proc.cpu_percent(None)
        rss_bytes = self_proc.memory_info().rss
        for child in self_proc.children(recursive=True):
            live_pids.add(child.pid)
            proc = child_cache.get(child.pid)
            if proc is None:
                child_cache[child.pid] = child
                try:
                    child.cpu_percent(None)  # prime; first reading is 0
                    rss_bytes += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                continue
            try:
                cpu_pct += proc.cpu_percent(None)
                rss_bytes += proc.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        for pid in list(child_cache.keys()):
            if pid not in live_pids:
                del child_cache[pid]

        ACTUAL_CPU.set(cpu_pct * 10.0)  # 100% of one core == 1000 millicores
        ACTUAL_RAM.set(rss_bytes / (1024 * 1024))

        now = time.monotonic()
        cur_net = psutil.net_io_counters(pernic=True).get("lo")
        if cur_net and last_net:
            elapsed = max(now - last_t, 1e-6)
            bytes_delta = (cur_net.bytes_sent + cur_net.bytes_recv) \
                        - (last_net.bytes_sent + last_net.bytes_recv)
            # loopback counts each byte twice (sent + recv on same iface)
            mbps = (bytes_delta / 2) * 8 / 1e6 / elapsed
            ACTUAL_NET.set(max(0.0, mbps))
        last_net, last_t = cur_net, now


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
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

    def log_message(self, fmt, *args):
        log.info("HTTP %s", fmt % args)


def main():
    port = int(os.environ.get("WORKER_PORT", "8080"))
    threading.Thread(target=sampler_loop, daemon=True).start()
    start_config_watcher(CONFIG_PATH)
    # Apply whatever config is already on disk at startup (the mounted
    # ConfigMap is present before the container starts).
    _load_and_apply(CONFIG_PATH)
    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("Worker listening on 0.0.0.0:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
