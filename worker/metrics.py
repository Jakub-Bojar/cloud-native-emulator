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
  - Net: bytes_sent delta summed across all interfaces. This is
    correct in both modes: on loopback every send is also a recv on
    the same iface, so counting sent-only avoids the double-count;
    on the pod interface, sent is true egress and recv is from peers
    (not relevant to "what this pod generated").

Every 5 s, _adjust_ram() resizes the mmap held by loads.RAM_BUFFER so
the cgroup working set converges on STATE['ram_mb']. Every 30 s, a
heartbeat log line confirms the thread is alive and shows what it's
seeing — invaluable when diagnosing drift or a stalled sampler.
"""

import logging
import os
import time
from collections import deque

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
ACTUAL_NET = Gauge("worker_actual_net_mbps",
                   "Measured egress throughput (Mbps). Counts bytes_sent on "
                   "every interface (loopback counted once). Matches what the "
                   "net formula targets — pure egress — so actual tracks "
                   "target correctly for all roles including middle-tier ones.")

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


def read_cgroup_cpu_usec() -> int | None:
    """Cumulative CPU time consumed by the pod's cgroup, in microseconds.

    This is the *truth* metric — the same source cAdvisor and kubectl
    top use. Comparing this against psutil's sum gives us the gap
    between what the worker thinks it's doing and what the kernel
    actually attributed to the cgroup.

    cgroup v2: /sys/fs/cgroup/cpu.stat -> usage_usec (microseconds)
    cgroup v1: /sys/fs/cgroup/cpuacct/cpuacct.usage (nanoseconds)
    """
    try:
        with open("/sys/fs/cgroup/cpu.stat") as f:
            for line in f:
                if line.startswith("usage_usec "):
                    return int(line.split()[1])
    except OSError:
        pass
    try:
        with open("/sys/fs/cgroup/cpuacct/cpuacct.usage") as f:
            return int(f.read().strip()) // 1000  # ns -> us
    except OSError:
        pass
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
    last_ram_adjust = 0.0
    last_heartbeat = 0.0
    RAM_ADJUST_INTERVAL_S = 5.0
    HEARTBEAT_INTERVAL_S = 30.0

    # cpu_percent(None) returns CPU since the *previous* call on the same
    # Process object — the first call always returns 0.0. Cache children
    # across iterations so each one accumulates real samples.
    child_cache: dict[int, psutil.Process] = {}

    # Cgroup CPU is averaged over a rolling 15s window so the gauge
    # matches what kubectl top (and the Grafana dashboard's rate()[1m])
    # show, instead of bouncing with stress-ng's sub-second bursts.
    # Each entry is (wall_clock_s, cumulative_usage_usec).
    ROLLING_CPU_WINDOW_S = 15.0
    cgroup_cpu_samples: deque[tuple[float, int]] = deque()
    cur0 = read_cgroup_cpu_usec()
    if cur0 is not None:
        cgroup_cpu_samples.append((time.monotonic(), cur0))
    last_per_proc_breakdown: list[tuple[int, str, float]] = []
    last_cgroup_cpu_mc: float | None = None

    # Network egress is also averaged over a 15s rolling window.
    # At low rates (e.g. 0.5 Mbps per iperf3 connection) the token-bucket
    # delivery is bursty enough that a 1s delta window swings from near-0
    # to 2× the average depending on burst phase.  Keeping cumulative byte
    # counts and computing the rate over the oldest-to-newest span gives
    # a stable reading that matches the true average rate.
    # Each entry is (wall_clock_s, cumulative_bytes_sent).
    ROLLING_NET_WINDOW_S = 15.0
    net_samples: deque[tuple[float, int]] = deque()
    _init_net = psutil.net_io_counters()
    net_samples.append((time.monotonic(), _init_net.bytes_sent))

    while True:
        time.sleep(interval)
        # An uncaught exception in this body would silently kill the daemon
        # thread — the pod would stay Ready but ACTUAL_RAM would freeze
        # and the RAM nudger would stop firing. Catch and continue.
        try:
            live_pids = set()
            cpu_pct = self_proc.cpu_percent(None)
            rss_bytes = self_proc.memory_info().rss
            per_proc: list[tuple[int, str, float]] = [
                (self_proc.pid, "python", cpu_pct),
            ]
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
                    c_pct = proc.cpu_percent(None)
                    cpu_pct += c_pct
                    rss_bytes += proc.memory_info().rss
                    try:
                        per_proc.append((proc.pid, proc.name()[:16], c_pct))
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        per_proc.append((proc.pid, "?", c_pct))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            for pid in list(child_cache.keys()):
                if pid not in live_pids:
                    del child_cache[pid]

            psutil_rss_mb = rss_bytes / (1024 * 1024)
            cgroup_mb = read_cgroup_working_set_mb()
            actual_ram_mb = cgroup_mb if cgroup_mb is not None else psutil_rss_mb
            ACTUAL_RAM.set(actual_ram_mb)

            now = time.monotonic()

            # Cgroup CPU is the truth — same source cAdvisor and kubectl top
            # use. We previously set ACTUAL_CPU from a psutil sum across child
            # processes, but that approach systematically misses stress-ng's
            # ephemeral worker children. Here we keep a 15s sliding window of
            # cumulative usage_usec readings and compute the gauge as
            # (newest - oldest) / wall-clock — i.e. a rolling 15s average,
            # which smooths over stress-ng's bursty load cycles.
            cur_cgroup_cpu_usec = read_cgroup_cpu_usec()
            if cur_cgroup_cpu_usec is not None:
                cgroup_cpu_samples.append((now, cur_cgroup_cpu_usec))
                # Drop samples older than the rolling window. Always keep
                # at least one prior sample so we can still compute a delta
                # in the first 15s after startup.
                cutoff = now - ROLLING_CPU_WINDOW_S
                while len(cgroup_cpu_samples) > 2 and cgroup_cpu_samples[0][0] < cutoff:
                    cgroup_cpu_samples.popleft()
                if len(cgroup_cpu_samples) >= 2:
                    t0, u0 = cgroup_cpu_samples[0]
                    t1, u1 = cgroup_cpu_samples[-1]
                    if t1 > t0:
                        last_cgroup_cpu_mc = (u1 - u0) / (t1 - t0) / 1000.0
            last_per_proc_breakdown = per_proc

            # Prefer cgroup; fall back to psutil only if cgroup isn't readable.
            if last_cgroup_cpu_mc is not None:
                ACTUAL_CPU.set(last_cgroup_cpu_mc)
            else:
                ACTUAL_CPU.set(cpu_pct * 10.0)

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
                # Per-process CPU breakdown + cgroup truth. Diagnostic.
                breakdown_str = ", ".join(
                    f"{name}({pid})={pct:.0f}%"
                    for (pid, name, pct) in last_per_proc_breakdown
                    if pct > 0.1 or name in ("python", "stress-ng")
                )
                log.info("CPU debug: psutil_total=%.0fm cgroup=%s breakdown=[%s]",
                         cpu_pct * 10,
                         (f"{last_cgroup_cpu_mc:.0f}m"
                          if last_cgroup_cpu_mc is not None else "n/a"),
                         breakdown_str)

            # Egress-only accounting so ACTUAL_NET matches what the net
            # formula targets. The formula controls how much traffic this
            # pod *sends*; it has no control over inbound traffic arriving
            # from upstream roles. Counting recv on top would cause middle-
            # tier pods to always read higher than their target (they send
            # their full budget AND receive from whoever is upstream).
            # On loopback sent == recv, so counting sent once is still correct.
            #
            # We use a 15s rolling window (oldest-to-newest cumulative byte
            # delta / elapsed time) instead of a 1s delta to suppress the
            # burst-phase noise that iperf3's token-bucket emits at low rates.
            cur_net = psutil.net_io_counters()
            net_samples.append((now, cur_net.bytes_sent))
            cutoff = now - ROLLING_NET_WINDOW_S
            while len(net_samples) > 2 and net_samples[0][0] < cutoff:
                net_samples.popleft()
            if len(net_samples) >= 2:
                nt0, nb0 = net_samples[0]
                nt1, nb1 = net_samples[-1]
                net_elapsed = max(nt1 - nt0, 1e-6)
                mbps = max(0.0, (nb1 - nb0) * 8 / 1e6 / net_elapsed)
            else:
                mbps = 0.0
            ACTUAL_NET.set(mbps)
        except Exception:
            log.exception("sampler iteration failed; continuing")
