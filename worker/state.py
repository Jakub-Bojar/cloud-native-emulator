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
# stress-ng --cpu-load duty-cycle slice (ms). Small values break the busy/idle
# loading into fine slices, giving the scheduler frequent yield points → much
# smoother CPU and better load accuracy when several pods share a node. Without
# it stress-ng cycles in coarse chunks (up to ~0.5s), which is bursty. 0 uses
# stress-ng's default. Only applies when the computed --cpu-load is < 100%.
CPU_LOAD_SLICE_MS = int(os.environ.get("CPU_LOAD_SLICE_MS", "40"))
PAGE_SIZE = 4096
RAM_TOLERANCE_MB = 0.5
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
