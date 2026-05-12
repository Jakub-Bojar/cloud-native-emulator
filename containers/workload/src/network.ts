import { createSocket } from "node:dgram";
import type { LoadConfig } from "./config.js";
import type { Metrics } from "./metrics.js";

const PAYLOAD_SIZE = 1400;
const BURST_INTERVAL_MS = 10;

export function startNetworkLoop(cfg: LoadConfig, metrics: Metrics): void {
  const payload = Buffer.alloc(PAYLOAD_SIZE, 0x78); // 'x'
  const bytesPerBurst = (cfg.netTargetMbps * 1_000_000) / 8 / 100;
  const packetsPerBurst = Math.max(1, Math.floor(bytesPerBurst / payload.length));

  const sock = createSocket("udp4");
  sock.on("error", () => {
    metrics.netErrors.inc();
  });

  let nextBurst = performance.now();
  const burst = (): void => {
    for (let i = 0; i < packetsPerBurst; i++) {
      sock.send(payload, cfg.netTargetPort, cfg.netTargetHost, (err) => {
        if (err) {
          metrics.netErrors.inc();
          return;
        }
        metrics.netBytes.inc(payload.length);
        metrics.netPackets.inc();
      });
    }
    nextBurst += BURST_INTERVAL_MS;
    const sleepFor = nextBurst - performance.now();
    if (sleepFor > 0) setTimeout(burst, sleepFor);
    else {
      nextBurst = performance.now();
      setImmediate(burst);
    }
  };
  burst();
}
