"""
Measurement + Prometheus gauges + RAM feedback control.

The sampler thread takes a measurement every second:
  - CPU: psutil per-process sum (the kernel attributes busy-loop time
    cleanly to children of this process, so psutil's view here is
    accurate).
  - RAM: read directly from the pod's cgroup `memory.current` minus
    `inactive_file`. This is what cAdvisor exports as
    `container_memory_working_set_bytes` and is what Grafana plots —
    importantly, it counts shared library pages once per cgroup rather
    than once per process.
  - Net: byte deltas on the loopback interface (iperf3 runs entirely
    on lo, so this is the load we generated).

Every 5 s, _adjust_ram() resizes the mmap held by loads.RAM_BUFFER so
the cgroup working set converges on STATE['ram_mb']. Every 30 s, a
heartbeat log line confirms the thread is alive and shows what it's
seeing — invaluable when diagnosing drift or a stalled sampler.
"""

import logging
import os
import time

import psutil
from prometheus_client import Gauge

import loads
from state import RAM_TOLERANCE_MB, STATE, STATE_LOCK

log = logging.getLogger(__name__)


TARGET_CPU = Gauge("worker_target_cpu_millicores", "Configured CPU target (millicores)")
TARGET_RAM = Gauge("worker_target_ram_mb",         "Configured RAM target (MB)")
TARGET_NET = Gauge("worker_target_net_mbps",       "Configured network target (Mbps)")

ACTUAL_CPU = Gauge("worker_actual_cpu_millicores", "Measured CPU usage (millicores)")
ACTUAL_RAM = Gauge("worker_actual_ram_mb",         "Measured RAM usage (MB)")
ACTUAL_NET = Gauge("worker_actual_net_mbps",       "Measured loopback throughput (Mbps)")

INPUT_X = Gauge("worker_input_x", "Current network input value x")


def sum_rss_mb(proc: psutil.Process) -> float:
    """Sum process + descendant RSS. Diagnostic only — for the gauge/nudger
    we use the cgroup working set because psutil double-counts shared pages."""
    rss = proc.memory_info().rss
    for c in proc.children(recursive=True):
        try:
            rss += c.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return rss / (1024 * 1024)


_CGROUP_PATHS: tuple[str, str, str] | None = None


def _detect_cgroup_paths() -> tuple[str, str, str] | None:
    # cgroup v2: unified hierarchy.
    if os.path.exists("/sys/fs/cgroup/memory.current"):
        return ("/sys/fs/cgroup/memory.current",
                "/sys/fs/cgroup/memory.stat",
                "inactive_file")
    # cgroup v1: per-controller mounts.
    if os.path.exists("/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        return ("/sys/fs/cgroup/memory/memory.usage_in_bytes",
                "/sys/fs/cgroup/memory/memory.stat",
                "total_inactive_file")
    return None


def read_cgroup_working_set_mb() -> float | None:
    """Return the cgroup working set in MiB, matching what cAdvisor /
    Grafana display. Returns None if cgroup files aren't accessible."""
    global _CGROUP_PATHS
    if _CGROUP_PATHS is None:
        _CGROUP_PATHS = _detect_cgroup_paths()
    if _CGROUP_PATHS is None:
        return None
    current_file, stat_file, inactive_key = _CGROUP_PATHS
    try:
        with open(current_file) as f:
            current = int(f.read().strip())
        inactive = 0
        with open(stat_file) as f:
            for line in f:
                if line.startswith(inactive_key + " "):
                    inactive = int(line.split()[1])
                    break
        return max(0, current - inactive) / (1024 * 1024)
    except (OSError, ValueError):
        return None


def _adjust_ram(actual_mb: float) -> None:
    """One step of the RAM feedback loop: compare cgroup working set to
    STATE['ram_mb'] target and resize the mmap to close the gap."""
    with STATE_LOCK:
        target_mb = STATE.get("ram_mb", 0.0)
        running = STATE.get("running", False)
    if not running or target_mb <= 0:
        return
    buf = loads.RAM_BUFFER
    if buf is None:
        return
    error_mb = target_mb - actual_mb
    if abs(error_mb) <= RAM_TOLERANCE_MB:
        return
    current_mb = len(buf) / (1024 * 1024)
    new_mb = max(0.0, current_mb + error_mb)
    log.info("RAM nudge: actual=%.1fMB target=%.1fMB → resize mmap %.1f→%.1fMB",
             actual_mb, target_mb, current_mb, new_mb)
    loads.start_ram(new_mb)


def sampler_loop(interval: float = 1.0) -> None:
    self_proc = psutil.Process(os.getpid())
    self_proc.cpu_percent(None)  # prime
    last_net = psutil.net_io_counters(pernic=True).get("lo")
    last_t = time.monotonic()
    last_ram_adjust = 0.0
    last_heartbeat = 0.0
    RAM_ADJUST_INTERVAL_S = 5.0
    HEARTBEAT_INTERVAL_S = 30.0

    # cpu_percent(None) returns CPU since the *previous* call on the same
    # Process object — the first call always returns 0.0. Cache children
    # across iterations so each one accumulates real samples.
    child_cache: dict[int, psutil.Process] = {}

    while True:
        time.sleep(interval)
        # An uncaught exception in this body would silently kill the daemon
        # thread — the pod would stay Ready but ACTUAL_RAM would freeze
        # and the RAM nudger would stop firing. Catch and continue.
        try:
            live_pids = set()
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

            psutil_rss_mb = rss_bytes / (1024 * 1024)
            cgroup_mb = read_cgroup_working_set_mb()
            actual_ram_mb = cgroup_mb if cgroup_mb is not None else psutil_rss_mb
            ACTUAL_CPU.set(cpu_pct * 10.0)  # 100% of one core == 1000 millicores
            ACTUAL_RAM.set(actual_ram_mb)

            now = time.monotonic()
            if now - last_ram_adjust >= RAM_ADJUST_INTERVAL_S:
                last_ram_adjust = now
                _adjust_ram(actual_ram_mb)

            if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                last_heartbeat = now
                with STATE_LOCK:
                    s_target = STATE.get("ram_mb", 0.0)
                    s_running = STATE.get("running", False)
                buf_mb = (len(loads.RAM_BUFFER) / (1024 * 1024)
                          if loads.RAM_BUFFER is not None else 0.0)
                log.info("Sampler heartbeat: actual=%.1fMB (cgroup=%s psutil=%.1f) "
                         "target=%.1fMB running=%s buf=%.1fMB",
                         actual_ram_mb,
                         f"{cgroup_mb:.1f}" if cgroup_mb is not None else "n/a",
                         psutil_rss_mb, s_target, s_running, buf_mb)

            cur_net = psutil.net_io_counters(pernic=True).get("lo")
            if cur_net and last_net:
                elapsed = max(now - last_t, 1e-6)
                bytes_delta = (cur_net.bytes_sent + cur_net.bytes_recv) \
                            - (last_net.bytes_sent + last_net.bytes_recv)
                # loopback counts each byte twice (sent + recv on same iface)
                mbps = (bytes_delta / 2) * 8 / 1e6 / elapsed
                ACTUAL_NET.set(max(0.0, mbps))
            last_net, last_t = cur_net, now
        except Exception:
            log.exception("sampler iteration failed; continuing")
