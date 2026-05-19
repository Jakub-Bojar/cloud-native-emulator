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

IPERF_PORT = 9999
PAGE_SIZE = 4096
RAM_TOLERANCE_MB = 2.0
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


def linear(a: float, b: float, x: float) -> float:
    return max(0.0, a * x + b)
