"""
Load generators: CPU, RAM, network.

Each `start_*` function brings its load to (approximately) the requested
level. start_cpu does its work by spawning child processes (stress-ng)
which the kernel automatically counts against the pod's cgroup. start_ram
allocates an anonymous mmap inside *this* python process and explicitly
faults every page in.

start_network has two modes:

  - Loopback (peers == []): one iperf3 server + one iperf3 client both
    on 127.0.0.1:9999. This is the legacy single-worker behaviour.
  - Peer-to-peer (peers != []): one iperf3 server on 0.0.0.0:9999
    (accepts inbound connections from other pods), plus one supervisor
    thread per declared peer. Each supervisor keeps an iperf3 client
    targeting `<peer>:9999` alive — if the peer isn't Ready yet the
    iperf3 client exits, the supervisor sleeps briefly and respawns it.
    The bandwidth target is divided evenly across peers so the pod's
    total egress matches the configured `net_mbps`.

PROCS holds child iperf3 / stress-ng processes so stop_current() can
terminate them. PEER_SUPS holds the per-peer supervisor threads, paired
with the Event that tells them to stop.
"""

import hashlib
import logging
import math
import mmap
import os
import shlex
import signal
import socket
import subprocess
import threading
import time

from state import (IPERF_BASE_PORT, IPERF_PORT_COUNT, PAGE_SIZE,
                   POD_NAME, STATE, STATE_LOCK)

log = logging.getLogger(__name__)

PROCS: list[subprocess.Popen] = []
RAM_BUFFER: mmap.mmap | None = None

# Per-peer iperf3 client supervisors. Each entry is (thread, stop_event).
# The supervisor owns its child iperf3 process; it is NOT added to PROCS
# (which is reserved for processes whose lifetime is bounded by a single
# start_*/stop_current cycle).
PEER_SUPS: list[tuple[threading.Thread, threading.Event]] = []


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


def _port_for_pod(pod_name: str) -> int:
    """Pick a stable iperf3 server port for this pod's outbound clients.

    Hash the pod name into the [BASE_PORT, BASE_PORT+PORT_COUNT) range.
    Different source pods → different ports → can hit the same target
    pod's port pool simultaneously without colliding on the single-
    session iperf3 server limit. Uses md5 (not Python's hash()) so the
    choice is stable across pod restarts / different processes."""
    h = int(hashlib.md5(pod_name.encode()).hexdigest()[:8], 16)
    return IPERF_BASE_PORT + (h % IPERF_PORT_COUNT)


def _parse_peer(peer: str) -> tuple[str, int]:
    """Accept 'host' or 'host:port'. When no port is given, choose one
    deterministically from our pod-name hash so the load distributes
    across the target's port pool."""
    host, sep, port_str = peer.partition(":")
    if sep and port_str:
        try:
            return host, int(port_str)
        except ValueError:
            log.warning("invalid port in peer %r — falling back to hash", peer)
    return host, _port_for_pod(POD_NAME)


def _peer_client_supervisor(peer: str, mbps_per_peer: float,
                            port: int,
                            stop: threading.Event) -> None:
    """Keep an iperf3 client running against `<peer>:<port>`.

    iperf3 client exits if the connection can't be established or if it
    completes its -t window. This loop restarts it until `stop` is set
    so that:
      - a peer pod that wasn't Ready at configure time eventually gets
        traffic once it does come up,
      - a peer pod restarting (rolling update, crash, reschedule) does
        not silently drop the link.

    Backoff is fixed at 2s — short enough that startup feels immediate,
    long enough to avoid hammering an unreachable peer.

    `port` is IPERF_BASE_PORT + the pod's assigned offset (supplied by
    the controller via port_offset_by_pod). Using a controller-assigned
    offset guarantees that two source pods never connect to the same
    iperf3 server port on the same target pod simultaneously (which would
    trigger iperf3's single-session refusal and leave one pod at 0 Mbps).
    """
    # Strip any legacy ":port" or "@pool" suffix that may appear in
    # ConfigMaps written by an older controller version.
    host = peer.split(":")[0].split("@")[0]
    cmd = [
        "iperf3", "-c", host,
        "-p", str(port),
        "-b", f"{mbps_per_peer}M",
        "-t", "86400",                # iperf3's hard cap (24h)
        "--connect-timeout", "3000",  # ms; fail fast so the loop can retry
    ]
    while not stop.is_set():
        log.info("peer iperf3 spawn: %s", shlex.join(cmd))
        try:
            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.STDOUT)
        except FileNotFoundError:
            log.error("iperf3 binary not found — peer supervisor exiting")
            return
        # Poll for either the process to exit or the stop signal to fire.
        while not stop.is_set():
            if proc.poll() is not None:
                break
            stop.wait(0.5)
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        if stop.is_set():
            return
        # iperf3 client exited on its own (peer unreachable / 24h reached).
        # Back off briefly before retrying so a missing peer doesn't pin a CPU.
        stop.wait(2.0)


def start_network(mbps: float, peers: list[str] | None = None,
                  legacy_loopback: bool = True,
                  server_count: int | None = None,
                  my_port_offset: int = 0) -> None:
    """Bring the network load up to `mbps` Mbps total egress.

    Four operating modes, decided by the caller:

      - peers non-empty: spawn one supervisor thread per peer; each keeps
        an iperf3 client streaming at `mbps / len(peers)` Mbps to that
        peer. iperf3 server stays running for inbound peer traffic.

      - peers empty, `legacy_loopback=True` (default): legacy single-worker
        mode. iperf3 server + a loopback iperf3 client at `mbps`. The
        client stays connected to the local server for the full 24h cap,
        so the server is effectively single-tenant.

      - peers empty, `legacy_loopback=False`: templated role with no
        outbound edges. iperf3 server runs but NO client is spawned. The
        server slot stays free for inbound connections from other roles
        in the topology. This pod's self-generated net traffic is zero.

      - `mbps == 0` and `server_count > 0`: pure receive-only role. No
        outbound traffic, but iperf3 servers still need to run so that
        upstream pods can connect. Spawns servers then returns without
        starting a client.
    """
    # Decide server pool size. A pod spawns servers if:
    #   (a) it has outbound traffic (mbps > 0), OR
    #   (b) it is a declared target (server_count > 0) — even if its own
    #       net formula evaluates to 0, upstream pods will still connect.
    if server_count is not None and server_count > 0:
        pool_size = server_count
    elif mbps > 0:
        pool_size = IPERF_PORT_COUNT
    else:
        # Neither outbound traffic nor declared inbound — nothing to do.
        return

    # Spawn a pool of iperf3 servers, one per port in our range. iperf3
    # in default mode can only host ONE active session per server
    # process — so to support multiple simultaneous inbound clients
    # (from different source pods in a templated topology), we run
    # `pool_size` servers on consecutive ports.
    for offset in range(pool_size):
        port = IPERF_BASE_PORT + offset
        spawn(["iperf3", "-s", "-p", str(port)])
    # Wait until at least the base port is bound; the rest come up
    # within a few ms of that.
    if not _wait_for_port("127.0.0.1", IPERF_BASE_PORT):
        log.warning("iperf3 server pool did not bind on :%d in time",
                    IPERF_BASE_PORT)
        return

    if mbps <= 0:
        # Pure receive-only role: servers are up, no outbound client needed.
        log.info("net: receive-only mode (server_count=%d, mbps=0)", pool_size)
        return

    peers = peers or []
    if not peers:
        if not legacy_loopback:
            # Templated role with no outbound edges. Don't run a loopback
            # client — server pool stays free for inbound peer traffic.
            log.info("net: server-only mode (templated role, no peers)")
            return
        # Legacy path. iperf3 caps -t at 86400s (24h).
        spawn([
            "iperf3", "-c", "127.0.0.1",
            "-p", str(IPERF_BASE_PORT),
            "-b", f"{mbps}M",
            "-t", "86400",
        ])
        return

    # Peer path: divide bandwidth budget evenly, one supervisor per peer.
    mbps_per_peer = mbps / len(peers)
    target_port = IPERF_BASE_PORT + my_port_offset
    log.info("Starting %d peer iperf3 supervisor(s) at %.2f Mbps each "
             "(port=%d, offset=%d)",
             len(peers), mbps_per_peer, target_port, my_port_offset)
    for peer in peers:
        stop = threading.Event()
        t = threading.Thread(
            target=_peer_client_supervisor,
            args=(peer, mbps_per_peer, target_port, stop),
            name=f"peer-{peer}",
            daemon=True,
        )
        PEER_SUPS.append((t, stop))
        t.start()


def _stop_peer_supervisors() -> None:
    """Signal every peer supervisor to exit and join them."""
    for _, stop in PEER_SUPS:
        stop.set()
    for t, _ in PEER_SUPS:
        # Each supervisor's inner loop checks `stop` every ~0.5s and then
        # SIGTERMs its iperf3 child (3s grace, then SIGKILL). Worst case
        # ~5s per thread; join with a generous timeout per thread.
        t.join(timeout=6.0)
        if t.is_alive():
            log.warning("peer supervisor %s did not exit cleanly", t.name)
    PEER_SUPS.clear()


def stop_current() -> None:
    global RAM_BUFFER
    _stop_peer_supervisors()
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
            "peers": [],
        })
    log.info("Emulation stopped")
