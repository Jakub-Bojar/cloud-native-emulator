import { isMainThread, Worker, workerData } from "node:worker_threads";
import { loadConfig } from "./config.js";
import { detectAllocatedCores } from "./cpu.js";
import { holdRam } from "./memory.js";
import { createMetrics, startCpuRssReporter, startMetricsServer } from "./metrics.js";
import { startNetworkLoop } from "./network.js";

if (!isMainThread) {
  const duty = Math.max(0, Math.min(1, (workerData as { duty: number }).duty));
  const windowMs = 100;
  const burnMs = windowMs * duty;
  const restMs = windowMs - burnMs;

  const busyUntil = (deadline: number): void => {
    while (performance.now() < deadline) {
      // tight spin — this is the whole point
    }
  };

  const tick = (): void => {
    if (burnMs > 0) busyUntil(performance.now() + burnMs);
    if (restMs > 0) setTimeout(tick, restMs);
    else setImmediate(tick);
  };
  tick();
} else {
  const cfg = loadConfig();
  const metrics = createMetrics(cfg);

  const ramBlob = holdRam(cfg.ramTargetMb);
  void ramBlob;

  const { cores, fractional, source } = detectAllocatedCores();
  // CPU_TARGET_PCT is "percent of total pod CPU". Total work = fractional * (pct/100) cores.
  // Spread across `cores` workers, each runs at (totalWork / cores) duty.
  const totalWorkCores = fractional * (cfg.cpuTargetPct / 100);
  const perWorkerDuty = Math.min(1, totalWorkCores / cores);

  const workers: Worker[] = [];
  for (let i = 0; i < cores; i++) {
    const w = new Worker(__filename, { workerData: { duty: perWorkerDuty } });
    w.on("error", (err) => {
      console.error(`[cpu-worker ${i}!] ${err.stack ?? err.message}`);
    });
    workers.push(w);
  }

  startCpuRssReporter(metrics);
  startNetworkLoop(cfg, metrics);

  const server = startMetricsServer(cfg.metricsPort, metrics.register);
  server.on("listening", () => {
    console.log(
      `[load] metrics on :${cfg.metricsPort}/metrics, target ${cfg.cpuTargetPct}% CPU / ${cfg.ramTargetMb}MB / ${cfg.netTargetMbps}Mbps -> ${cfg.netTargetHost}:${cfg.netTargetPort}`,
    );
    console.log(
      `[load] cpu: ${fractional.toFixed(2)} cores allocated (${source}), spawned ${cores} workers at ${(perWorkerDuty * 100).toFixed(1)}% duty each`,
    );
  });

  const shutdown = (sig: NodeJS.Signals): void => {
    console.log(`[load] received ${sig}, shutting down`);
    Promise.all(workers.map((w) => w.terminate())).finally(() => {
      server.close(() => process.exit(0));
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
