"""
Shared mutable state, constants, and the linear-function helper used
across the worker modules.

STATE describes what the worker is currently doing. It's read by /status,
by the RAM nudger to know the target, and updated by configure() at the
end of a successful reconfigure.
"""

import logging
import os
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(message)s",
)

IPERF_BASE_PORT = 9999
IPERF_PORT_COUNT = int(os.environ.get("IPERF_PORT_COUNT", "8"))
# Back-compat alias; some callers still want a single canonical port.
IPERF_PORT = IPERF_BASE_PORT

# --- iperf3 server-pool self-recycling -------------------------------------
# An iperf3 server (`-s -p PORT`) hosts ONE session at a time. If the client
# of a session dies uncleanly (SIGKILL during a reconfigure, a pod eviction,
# a node blip), the server can be left stuck "busy running a test" with no
# live connection — a poisoned port that refuses every new client. The next
# client then loops forever on "the server is busy", so that link sits at 0
# and the role's egress never smooths out.
#
# The recycler thread (loads._server_recycler) clears this: every
# RECYCLE_INTERVAL_S it checks each server port for an ESTABLISHED inbound
# connection. A port that previously had a connection but has been
# connection-less for RECYCLE_GRACE_S is treated as poisoned and the server
# is killed + respawned, freeing the port for the waiting client. A port with
# a live connection (an active test) is never touched, and a port that has
# never had a client (idle, peer not up yet) is left alone — so a healthy
# pool is never churned. GRACE must comfortably exceed a normal client
# reconnect gap (~2 s) so legitimate reconnects don't trip it.
IPERF_SERVER_RECYCLE_INTERVAL_S = float(
    os.environ.get("IPERF_SERVER_RECYCLE_INTERVAL_S", "10"))
IPERF_SERVER_RECYCLE_GRACE_S = float(
    os.environ.get("IPERF_SERVER_RECYCLE_GRACE_S", "30"))
# stress-ng --cpu-load duty-cycle slice (ms). Small values break the busy/idle
# loading into fine slices, giving the scheduler frequent yield points → much
# smoother CPU and better load accuracy when several pods share a node. Without
# it stress-ng cycles in coarse chunks (up to ~0.5s), which is bursty. 0 uses
# stress-ng's default. Only applies when the computed --cpu-load is < 100%.
CPU_LOAD_SLICE_MS = int(os.environ.get("CPU_LOAD_SLICE_MS", "40"))
PAGE_SIZE = 4096
RAM_TOLERANCE_MB = 0.5

# --- CPU feedback loop (closed-loop, mirrors the RAM nudger) ---------------
# CPU used to be open-loop: stress-ng was sized ONCE at configure() time as
# target - baseline, using a single 1 s baseline sample taken during startup
# churn. A transiently-high baseline baked in a permanently under-sized
# stress-ng. These knobs drive a periodic corrector that re-measures the pod's
# *total* CPU and resizes stress-ng until total converges on the target — so a
# bad initial guess self-heals, exactly like RAM.
#
# CPU_GAIN: fraction of the error closed per correction step. RAM can use 1.0
#   because its measurement updates instantly; CPU's measured value is a 15 s
#   rolling average (see metrics.ROLLING_CPU_WINDOW_S), so acting at full gain
#   against that lag would oscillate. 0.6 converges in ~2-3 steps without
#   overshoot.
# CPU_ADJUST_INTERVAL_S: seconds between corrections. MUST be >= the rolling
#   CPU window (15 s) so each correction reads a measurement that reflects the
#   previous change rather than stale pre-change samples.
# CPU_TOLERANCE_*: deadband. We skip a correction when |error| is within
#   max(CPU_TOLERANCE_MC, CPU_TOLERANCE_FRAC * target). The absolute floor must
#   be at least one stress-ng quantisation step (~10 mc per worker) so small
#   targets don't churn the process for sub-step deltas.
CPU_GAIN = float(os.environ.get("CPU_GAIN", "0.6"))
CPU_ADJUST_INTERVAL_S = float(os.environ.get("CPU_ADJUST_INTERVAL_S", "15"))
CPU_TOLERANCE_FRAC = float(os.environ.get("CPU_TOLERANCE_FRAC", "0.05"))
CPU_TOLERANCE_MC = float(os.environ.get("CPU_TOLERANCE_MC", "15"))
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/etc/emulator/config.json")

# Pod name from the DownwardAPI env injected in the worker template.
# Used to deterministically pick which server port this pod's outbound
# iperf3 clients connect to on each peer — so multiple source pods
# sending to the same target distribute across the target's port pool
# instead of all colliding on port 9999.
POD_NAME = os.environ.get("POD_NAME", "worker-unknown")

STATE_LOCK = threading.Lock()
STATE = {
    "running": False,
    "x": None,
    "cpu_millicores": 0.0,
    "ram_mb": 0.0,
    "net_mbps": 0.0,
    "formulas": {},
    # Concrete peer pod IPs this worker sends iperf3 traffic to, written by
    # the controller's materialiser. Empty for a role with no outbound edges.
    "peers": [],
}

# Measured per-peer egress (Mbps), keyed by peer IP. The iperf3 client
# supervisors in loads.py write here as they parse interval reports; the
# metrics sampler publishes it to the worker_peer_egress_mbps gauge. Guarded
# by a lock because one supervisor thread writes per peer concurrently.
PEER_EGRESS_MBPS: dict[str, float] = {}
PEER_EGRESS_LOCK = threading.Lock()


def linear(a: float, b: float, x: float) -> float:
    return max(0.0, a * x + b)
