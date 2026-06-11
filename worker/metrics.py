"""
Measurement + Prometheus gauges + RAM feedback control.

The sampler thread takes a measurement every second, all from cgroup
files — the same sources cAdvisor and kubectl top read:
  - CPU: the cgroup's cumulative usage (cpu.stat usage_usec) averaged
    over a 15 s rolling window. A per-process psutil sum would miss
    stress-ng's ephemeral worker children and under-read.
  - RAM: the cgroup working set (`memory.current` minus `inactive_file`).
    This is what cAdvisor exports as
    `container_memory_working_set_bytes` and is what Grafana plots —
    importantly, it counts shared library pages once per cgroup rather
    than once per process.
  - Net: bytes_sent delta summed across all interfaces — true egress.
    recv is inbound traffic from peers and is not counted (it isn't
    "what this pod generated").

Every 5 s, _adjust_ram() resizes the mmap held by loads.RAM_BUFFER so
the cgroup working set converges on STATE['ram_mb']. Every 30 s, a
heartbeat log line confirms the thread is alive and shows what it's
seeing — invaluable when diagnosing drift or a stalled sampler.
"""

import logging
import os
import socket
import time
from collections import deque

import psutil
from prometheus_client import Gauge

import loads
from state import (RAM_TOLERANCE_MB, STATE, STATE_LOCK,
                   PEER_EGRESS_MBPS, PEER_EGRESS_LOCK,
                   CPU_GAIN, CPU_TOLERANCE_FRAC, CPU_TOLERANCE_MC,
                   CPU_ADJUST_INTERVAL_S)

log = logging.getLogger(__name__)


TARGET_CPU = Gauge("worker_target_cpu_millicores", "Configured CPU target (millicores)")
TARGET_RAM = Gauge("worker_target_ram_mb",         "Configured RAM target (MB)")
TARGET_NET = Gauge("worker_target_net_mbps",       "Configured network target (Mbps)")

ACTUAL_CPU = Gauge("worker_actual_cpu_millicores", "Measured CPU usage (millicores)")
ACTUAL_RAM = Gauge("worker_actual_ram_mb",         "Measured RAM usage (MB)")
ACTUAL_NET = Gauge("worker_actual_net_mbps",
                   "Measured egress throughput (Mbps), bytes_sent across all "
                   "interfaces. Matches what the net formula targets — pure "
                   "egress — so actual tracks target for all roles including "
                   "middle-tier ones.")

INPUT_X = Gauge("worker_input_x", "Current network input value x")

STRESS_CPU = Gauge("worker_cpu_stress_millicores",
                   "CPU the feedback loop currently asks stress-ng to generate "
                   "(millicores). The pod total is this plus the iperf3+python "
                   "baseline; the loop adjusts this so total converges on "
                   "worker_target_cpu_millicores. Watch it move after a "
                   "reconfigure to see the loop working.")

PEER_EGRESS = Gauge("worker_peer_egress_mbps",
                    "Measured egress to a specific peer IP (Mbps), parsed from "
                    "the iperf3 client's per-second interval reports. One "
                    "series per peer IP this pod is sending to.",
                    ["peer"])

PEER_RTT = Gauge("worker_peer_rtt_ms",
                 "TCP-handshake round-trip time to a peer pod (ms), probed "
                 "against the peer's HTTP port every 15s. Measured pod-to-pod, "
                 "so it includes injected inter-tier latency (Chaos Mesh / tc "
                 "netem). One series per peer IP this pod sends to.",
                 ["peer"])


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


def _probe_rtt_ms(ip: str, port: int = 8080, timeout: float = 1.0) -> float | None:
    """One TCP handshake to the peer's always-listening HTTP port ≈ one
    network round trip. None if the peer is unreachable within `timeout`."""
    try:
        t0 = time.monotonic()
        with socket.create_connection((ip, port), timeout=timeout):
            return (time.monotonic() - t0) * 1000.0
    except OSError:
        return None


def _adjust_ram(actual_mb: float) -> None:
    """One step of the RAM feedback loop: compare cgroup working set to
    STATE['ram_mb'] target and resize the mmap to close the gap.

    configure() never allocates the buffer — RAM_BUFFER is None after every
    reconfigure. Treating None as a 0 MB buffer means this loop also performs
    the initial allocation, sized against the live cgroup working set, so the
    python + iperf3 footprint is accounted for without being measured
    explicitly."""
    with STATE_LOCK:
        target_mb = STATE.get("ram_mb", 0.0)
        running = STATE.get("running", False)
    if not running or target_mb <= 0:
        return
    error_mb = target_mb - actual_mb
    if abs(error_mb) <= RAM_TOLERANCE_MB:
        return
    buf = loads.RAM_BUFFER
    current_mb = len(buf) / (1024 * 1024) if buf is not None else 0.0
    new_mb = max(0.0, current_mb + error_mb)
    log.info("RAM nudge: actual=%.1fMB target=%.1fMB → resize mmap %.1f→%.1fMB",
             actual_mb, target_mb, current_mb, new_mb)
    loads.start_ram(new_mb)


def _adjust_cpu(actual_total_mc: float) -> None:
    """One step of the CPU feedback loop: compare the pod's *total* measured
    CPU to STATE['cpu_millicores'] and resize stress-ng to close the gap.

    Because actual_total_mc already includes the iperf3 + python baseline, the
    loop sizes stress-ng so total → target without ever measuring the baseline
    directly — a bad initial guess from configure() self-corrects within a few
    steps. This is the CPU analogue of _adjust_ram; the differences are forced
    by CPU not being RAM:
      - stress-ng can't be resized in place, so a change means kill+respawn
        (start_cpu handles that, and skips the respawn when the new size
        quantises to the running command).
      - actual_total_mc is a 15 s rolling average, so we apply a sub-unity gain
        and run on an interval >= that window (see state.CPU_*); full-gain
        correction against a lagged signal would oscillate."""
    with STATE_LOCK:
        target_mc = STATE.get("cpu_millicores", 0.0)
        running = STATE.get("running", False)
    if not running or target_mc <= 0:
        return
    tol = max(CPU_TOLERANCE_MC, CPU_TOLERANCE_FRAC * target_mc)
    error_mc = target_mc - actual_total_mc
    if abs(error_mc) <= tol:
        return
    current_stress = loads.CPU_STRESS_MC
    new_stress = max(0.0, current_stress + CPU_GAIN * error_mc)
    if new_stress <= 0 and current_stress <= 0:
        # Over target with stress-ng already at zero: the baseline (iperf3 +
        # python) alone meets or exceeds the target, so there's nothing left to
        # give up. Unfixable from the worker — don't churn. (Quiet to avoid
        # spamming every interval; the gap is visible on the gauges.)
        return
    changed = loads.start_cpu(new_stress)
    if changed:
        log.info("CPU nudge: actual=%.0fm target=%.0fm err=%+.0fm → "
                 "stress %.0f→%.0fm", actual_total_mc, target_mc, error_mc,
                 current_stress, new_stress)


def sampler_loop(interval: float = 1.0) -> None:
    last_ram_adjust = 0.0
    # Seed the CPU corrector's clock to "now" so the first correction waits a
    # full interval — by then the rolling CPU average has filled with real data
    # and reflects configure()'s initial stress-ng size rather than startup 0s.
    last_cpu_adjust = time.monotonic()
    last_heartbeat = 0.0
    last_rtt_probe = 0.0
    RAM_ADJUST_INTERVAL_S = 5.0
    HEARTBEAT_INTERVAL_S = 30.0
    RTT_PROBE_INTERVAL_S = 15.0

    # Peer IPs we currently have a worker_peer_egress_mbps series for, so we
    # can remove series for peers that vanish after a reconfigure.
    published_peers: set[str] = set()
    # Same bookkeeping for the worker_peer_rtt_ms series.
    published_rtt_peers: set[str] = set()

    # Cgroup CPU is averaged over a rolling 15s window so the gauge
    # matches what kubectl top (and the Grafana dashboard's rate()[1m])
    # show, instead of bouncing with stress-ng's sub-second bursts.
    # Each entry is (wall_clock_s, cumulative_usage_usec).
    ROLLING_CPU_WINDOW_S = 15.0
    cgroup_cpu_samples: deque[tuple[float, int]] = deque()
    cur0 = read_cgroup_cpu_usec()
    if cur0 is not None:
        cgroup_cpu_samples.append((time.monotonic(), cur0))
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
            now = time.monotonic()

            # RAM: the cgroup working set — what cAdvisor exports and Grafana
            # plots. If the cgroup files aren't readable (never the case
            # inside a k8s pod) skip the gauge and the nudger this cycle
            # rather than publish a wrong number.
            cgroup_mb = read_cgroup_working_set_mb()
            if cgroup_mb is not None:
                ACTUAL_RAM.set(cgroup_mb)

            # CPU: keep a 15s sliding window of cumulative usage_usec readings
            # and compute the gauge as (newest - oldest) / wall-clock — a
            # rolling 15s average, which smooths over stress-ng's bursty load
            # cycles. (A psutil per-process sum would miss stress-ng's
            # ephemeral worker children and systematically under-read.)
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
            if last_cgroup_cpu_mc is not None:
                ACTUAL_CPU.set(last_cgroup_cpu_mc)
            STRESS_CPU.set(loads.CPU_STRESS_MC)

            if cgroup_mb is not None and now - last_ram_adjust >= RAM_ADJUST_INTERVAL_S:
                last_ram_adjust = now
                _adjust_ram(cgroup_mb)

            # CPU feedback step. The interval is >= the rolling window so each
            # step reads a settled measurement of the previous change (see
            # _adjust_cpu).
            if (last_cgroup_cpu_mc is not None
                    and now - last_cpu_adjust >= CPU_ADJUST_INTERVAL_S):
                last_cpu_adjust = now
                _adjust_cpu(last_cgroup_cpu_mc)

            if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                last_heartbeat = now
                with STATE_LOCK:
                    s_target = STATE.get("ram_mb", 0.0)
                    s_running = STATE.get("running", False)
                buf_mb = (len(loads.RAM_BUFFER) / (1024 * 1024)
                          if loads.RAM_BUFFER is not None else 0.0)
                log.info("Sampler heartbeat: ram=%sMB target=%.1fMB cpu=%s "
                         "stress=%.0fm running=%s buf=%.1fMB",
                         f"{cgroup_mb:.1f}" if cgroup_mb is not None else "n/a",
                         s_target,
                         (f"{last_cgroup_cpu_mc:.0f}m"
                          if last_cgroup_cpu_mc is not None else "n/a"),
                         loads.CPU_STRESS_MC, s_running, buf_mb)

            # Egress-only accounting so ACTUAL_NET matches what the net
            # formula targets. The formula controls how much traffic this
            # pod *sends*; it has no control over inbound traffic arriving
            # from upstream roles. Counting recv on top would cause middle-
            # tier pods to always read higher than their target (they send
            # their full budget AND receive from whoever is upstream).
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

            # Publish measured per-peer egress and prune series for peers that
            # disappeared (e.g. after a reconfigure changed the peer set).
            with PEER_EGRESS_LOCK:
                peer_snapshot = dict(PEER_EGRESS_MBPS)
            for peer_ip, peer_mbps in peer_snapshot.items():
                PEER_EGRESS.labels(peer=peer_ip).set(peer_mbps)
            for stale in published_peers - peer_snapshot.keys():
                try:
                    PEER_EGRESS.remove(stale)
                except KeyError:
                    pass
            published_peers = set(peer_snapshot.keys())

            # Probe per-peer RTT. Blocking, so on its own slow cadence with a
            # 1s timeout — worst case (every peer down) stalls one sampler
            # iteration by len(peers) seconds once per probe interval.
            if now - last_rtt_probe >= RTT_PROBE_INTERVAL_S:
                last_rtt_probe = now
                with STATE_LOCK:
                    rtt_peers = list(STATE.get("peers") or [])
                for ip in rtt_peers:
                    ms = _probe_rtt_ms(ip)
                    if ms is not None:
                        PEER_RTT.labels(peer=ip).set(ms)
                for stale in published_rtt_peers - set(rtt_peers):
                    try:
                        PEER_RTT.remove(stale)
                    except KeyError:
                        pass
                published_rtt_peers = set(rtt_peers)
        except Exception:
            log.exception("sampler iteration failed; continuing")
