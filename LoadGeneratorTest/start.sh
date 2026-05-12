#!/bin/sh

# Default to 0 if not provided
CPU_PCT=${CPU_TARGET_PCT:-0}
RAM_MB=${RAM_TARGET_MB:-0}
NET_MBPS=${NET_TARGET_MBPS:-0}

# Default iperf3 port is 5201
HOST=${NET_TARGET_HOST:-"127.0.0.1"}
PORT=${NET_TARGET_PORT:-5201}

echo "[Load Generator] Starting up..."

# 1. Handle CPU & Memory (stress-ng)
if [ "$CPU_PCT" -gt 0 ] || [ "$RAM_MB" -gt 0 ]; then
  STRESS_ARGS=""
  
  if [ "$CPU_PCT" -gt 0 ]; then
    echo "[Load] CPU Target: ${CPU_PCT}% across all available cores"
    # --cpu 0 uses all available cores. --cpu-load throttles them.
    STRESS_ARGS="--cpu 0 --cpu-load $CPU_PCT"
  fi
  
  if [ "$RAM_MB" -gt 0 ]; then
    echo "[Load] RAM Target: ${RAM_MB}MB"
    # --vm 1 creates a single memory worker. --vm-bytes allocates the exact amount.
    STRESS_ARGS="$STRESS_ARGS --vm 1 --vm-bytes ${RAM_MB}M"
  fi
  
  # Run stress-ng in the background (&)
  stress-ng $STRESS_ARGS &
fi

# 2. Handle Network (iperf3)
if [ "$NET_MBPS" -gt 0 ]; then
  echo "[Load] Network Target: ${NET_MBPS}Mbps UDP to ${HOST}:${PORT}"
  # Run iperf3 in the foreground to keep the container alive
  # -c (client) -u (UDP) -b (bandwidth) -t 0 (run forever)
  iperf3 -c "$HOST" -p "$PORT" -u -b "${NET_MBPS}M" -t 0
else
  # If we aren't generating network load, wait for stress-ng so the container doesn't exit
  wait
fi