#!/usr/bin/env python3
"""
Synthetic load generator: CPU %, RAM MB, network Mbps.
Open-loop CPU (fixed duty cycle) + burst-paced network for steady, predictable load.
Exposes Prometheus metrics on :8000/metrics.
"""
import os
import time
import threading
import socket
import psutil
from prometheus_client import start_http_server, Gauge, Counter

CPU_TARGET_PCT = float(os.getenv("CPU_TARGET_PCT", "30"))
RAM_TARGET_MB  = int(os.getenv("RAM_TARGET_MB",  "256"))
NET_TARGET_MBPS = float(os.getenv("NET_TARGET_MBPS", "10"))
NET_TARGET_HOST = os.getenv("NET_TARGET_HOST", "10.0.0.1")
NET_TARGET_PORT = int(os.getenv("NET_TARGET_PORT", "9999"))
METRICS_PORT   = int(os.getenv("METRICS_PORT",   "8000"))

proc = psutil.Process(os.getpid())

g_cpu_target   = Gauge("synthload_cpu_target_pct",   "Configured CPU target (% of one core)")
g_cpu_measured = Gauge("synthload_cpu_measured_pct", "Measured process CPU (% of one core)")
g_cpu_duty     = Gauge("synthload_cpu_duty",         "Configured burn duty cycle (0..1)")
g_ram_target   = Gauge("synthload_ram_target_bytes", "Configured RAM target (bytes)")
g_ram_rss      = Gauge("synthload_ram_rss_bytes",    "Measured RSS of this process (bytes)")
g_net_target   = Gauge("synthload_net_target_mbps",  "Configured outbound network target (Mbps)")
c_net_bytes    = Counter("synthload_net_sent_bytes_total",   "Total bytes successfully sent")
c_net_packets  = Counter("synthload_net_sent_packets_total", "Total packets successfully sent")
c_net_errors   = Counter("synthload_net_send_errors_total",  "Total send errors (OSError)")

g_cpu_target.set(CPU_TARGET_PCT)
g_ram_target.set(RAM_TARGET_MB * 1024 * 1024)
g_net_target.set(NET_TARGET_MBPS)


def hold_ram(mb):
    blob = bytearray(mb * 1024 * 1024)
    for i in range(0, len(blob), 4096):
        blob[i] = 1
    return blob

ram_blob = hold_ram(RAM_TARGET_MB)


def network_loop():
    """
    Send NET_TARGET_MBPS as 10ms bursts. The kernel handles bursts more
    accurately than 1ms sleeps; pacing drift is bounded to one burst window.
    """
    payload = b"x" * 1400
    bytes_per_burst = NET_TARGET_MBPS * 1_000_000 / 8 / 100  # per 10ms
    packets_per_burst = max(1, int(bytes_per_burst / len(payload)))
    burst_interval = 0.01  # 10ms

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    next_burst = time.perf_counter()
    while True:
        for _ in range(packets_per_burst):
            try:
                sock.sendto(payload, (NET_TARGET_HOST, NET_TARGET_PORT))
                c_net_bytes.inc(len(payload))
                c_net_packets.inc()
            except OSError:
                c_net_errors.inc()
        next_burst += burst_interval
        sleep_for = next_burst - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_burst = time.perf_counter()


def cpu_loop():
    """
    Fixed-duty open-loop CPU burner. Burns CPU_TARGET_PCT% of each window.
    Reports its own observed CPU via a wall-clock measurement (not psutil's
    drift-prone cpu_percent), purely for visibility.
    """
    duty = CPU_TARGET_PCT / 100.0
    g_cpu_duty.set(duty)
    window = 0.1  # 100ms

    last_report = time.perf_counter()
    last_cpu = sum(proc.cpu_times()[:2])  # user + system seconds

    while True:
        burn_time = window * duty
        end = time.perf_counter() + burn_time
        while time.perf_counter() < end:
            pass
        rest = window - burn_time
        if rest > 0:
            time.sleep(rest)

        now = time.perf_counter()
        if now - last_report >= 1.0:
            cpu_now = sum(proc.cpu_times()[:2])
            measured_pct = (cpu_now - last_cpu) / (now - last_report) * 100.0
            g_cpu_measured.set(measured_pct)
            g_ram_rss.set(proc.memory_info().rss)
            last_report = now
            last_cpu = cpu_now


start_http_server(METRICS_PORT)
threading.Thread(target=network_loop, daemon=True).start()
cpu_loop()
