"""
Load generators: CPU, RAM, network.

Each `start_*` function brings its load to (approximately) the requested
level. start_cpu/start_network do their work by spawning child processes
(stress-ng, iperf3) which the kernel automatically counts against the
pod's cgroup. start_ram allocates an anonymous mmap inside *this* python
process and explicitly faults every page in.

PROCS holds the child processes so stop_current() can terminate them.
RAM_BUFFER holds the current mmap so metrics.py can resize it.
"""

import logging
import math
import mmap
import os
import shlex
import signal
import socket
import subprocess
import time

from state import IPERF_PORT, PAGE_SIZE, STATE, STATE_LOCK

log = logging.getLogger(__name__)

PROCS: list[subprocess.Popen] = []
RAM_BUFFER: mmap.mmap | None = None


def spawn(cmd: list[str]) -> subprocess.Popen:
    # Drop child stdout/stderr to /dev/null. iperf3 prints a stats line per
    # second and over a long run the page-cache pages backing those logs
    # add up to a slow upward drift in the cgroup working set.
    log.info("spawn: %s", shlex.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    log.info("  -> pid=%d", p.pid)
    PROCS.append(p)
    return p


# stress-ng --cpu N --cpu-load P runs N workers each busy P% of the time.
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


# A private anonymous mmap pinned by writing one byte per page. Using mmap
# (not bytearray) makes the RSS contribution deterministic: .close() returns
# pages to the kernel immediately, and the next allocation always maps fresh
# pages, so glibc's malloc arena can't quietly hold onto freed memory.
def start_ram(ram_mb: float) -> None:
    global RAM_BUFFER
    old = RAM_BUFFER
    RAM_BUFFER = None
    if old is not None:
        try:
            old.close()
        except Exception:
            log.exception("failed to close previous RAM_BUFFER")
    if ram_mb <= 0:
        return
    n_bytes = int(ram_mb * 1024 * 1024)
    if n_bytes < PAGE_SIZE:
        return
    n_bytes = ((n_bytes + PAGE_SIZE - 1) // PAGE_SIZE) * PAGE_SIZE
    buf = mmap.mmap(-1, n_bytes)
    # Anonymous mappings start as CoW-zero; writing one byte per page forces
    # the kernel to allocate a real page, putting it in RSS.
    for off in range(0, n_bytes, PAGE_SIZE):
        buf[off] = 0
    RAM_BUFFER = buf


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
    # iperf3's --connect-timeout caps a single connect() syscall but does not
    # retry on ECONNREFUSED, so the client races the server's bind(). Block
    # until the server is actually listening before spawning the client.
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
    global RAM_BUFFER
    for p in PROCS:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in PROCS:
        try:
            p.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            p.kill()
    PROCS.clear()
    if RAM_BUFFER is not None:
        try:
            RAM_BUFFER.close()
        except Exception:
            log.exception("failed to close RAM_BUFFER on stop")
    RAM_BUFFER = None
    with STATE_LOCK:
        STATE.update({
            "running": False,
            "x": None,
            "cpu_millicores": 0.0,
            "ram_mb": 0.0,
            "net_mbps": 0.0,
            "formulas": {},
        })
    log.info("Emulation stopped")
