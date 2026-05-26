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
    # Peer Service DNS names this worker is sending iperf3 traffic to.
    # Empty when the worker is in legacy loopback mode (no peers in the
    # mounted config). Populated by the controller's template materializer.
    "peers": [],
}


def linear(a: float, b: float, x: float) -> float:
    return max(0.0, a * x + b)
