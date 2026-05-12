import { createServer, Server } from "node:http";
import { collectDefaultMetrics, Counter, Gauge, Registry } from "prom-client";
import type { LoadConfig } from "./config.js";

export type Metrics = {
  register: Registry;
  cpuTarget: Gauge;
  cpuMeasured: Gauge;
  cpuDuty: Gauge;
  ramTarget: Gauge;
  ramRss: Gauge;
  netTarget: Gauge;
  netBytes: Counter;
  netPackets: Counter;
  netErrors: Counter;
};

export function createMetrics(cfg: LoadConfig): Metrics {
  const register = new Registry();
  collectDefaultMetrics({ register });

  const cpuTarget = new Gauge({
    name: "synthload_cpu_target_pct",
    help: "Configured CPU target (% of one core)",
    registers: [register],
  });
  const cpuMeasured = new Gauge({
    name: "synthload_cpu_measured_pct",
    help: "Measured process CPU (% of one core)",
    registers: [register],
  });
  const cpuDuty = new Gauge({
    name: "synthload_cpu_duty",
    help: "Configured burn duty cycle (0..1)",
    registers: [register],
  });
  const ramTarget = new Gauge({
    name: "synthload_ram_target_bytes",
    help: "Configured RAM target (bytes)",
    registers: [register],
  });
  const ramRss = new Gauge({
    name: "synthload_ram_rss_bytes",
    help: "Measured RSS of this process (bytes)",
    registers: [register],
  });
  const netTarget = new Gauge({
    name: "synthload_net_target_mbps",
    help: "Configured outbound network target (Mbps)",
    registers: [register],
  });
  const netBytes = new Counter({
    name: "synthload_net_sent_bytes_total",
    help: "Total bytes successfully sent",
    registers: [register],
  });
  const netPackets = new Counter({
    name: "synthload_net_sent_packets_total",
    help: "Total packets successfully sent",
    registers: [register],
  });
  const netErrors = new Counter({
    name: "synthload_net_send_errors_total",
    help: "Total send errors",
    registers: [register],
  });

  cpuTarget.set(cfg.cpuTargetPct);
  cpuDuty.set(cfg.cpuTargetPct / 100);
  ramTarget.set(cfg.ramTargetMb * 1024 * 1024);
  netTarget.set(cfg.netTargetMbps);

  return {
    register,
    cpuTarget,
    cpuMeasured,
    cpuDuty,
    ramTarget,
    ramRss,
    netTarget,
    netBytes,
    netPackets,
    netErrors,
  };
}

export function startMetricsServer(port: number, register: Registry): Server {
  const server = createServer((req, res) => {
    if (req.url === "/metrics") {
      register
        .metrics()
        .then((body) => {
          res.setHeader("Content-Type", register.contentType);
          res.end(body);
        })
        .catch((err: unknown) => {
          res.statusCode = 500;
          res.end(err instanceof Error ? err.message : String(err));
        });
      return;
    }
    if (req.url === "/healthz") {
      res.statusCode = 200;
      res.end("ok");
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  server.listen(port);
  return server;
}

export function startCpuRssReporter(metrics: Metrics): NodeJS.Timeout {
  let lastReportMs = performance.now();
  let lastCpu = process.cpuUsage();
  return setInterval(() => {
    const nowMs = performance.now();
    const cpuNow = process.cpuUsage();
    const elapsedUs = (nowMs - lastReportMs) * 1000;
    const cpuDeltaUs = cpuNow.user - lastCpu.user + (cpuNow.system - lastCpu.system);
    if (elapsedUs > 0) {
      metrics.cpuMeasured.set((cpuDeltaUs / elapsedUs) * 100);
    }
    metrics.ramRss.set(process.memoryUsage().rss);
    lastReportMs = nowMs;
    lastCpu = cpuNow;
  }, 1000);
}
