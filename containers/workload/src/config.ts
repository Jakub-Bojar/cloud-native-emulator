export type LoadConfig = {
  cpuTargetPct: number;
  ramTargetMb: number;
  netTargetMbps: number;
  netTargetHost: string;
  netTargetPort: number;
  metricsPort: number;
};

export function loadConfig(): LoadConfig {
  return {
    cpuTargetPct: Number.parseFloat(process.env.CPU_TARGET_PCT ?? "30"),
    ramTargetMb: Number.parseInt(process.env.RAM_TARGET_MB ?? "256", 10),
    netTargetMbps: Number.parseFloat(process.env.NET_TARGET_MBPS ?? "10"),
    netTargetHost: process.env.NET_TARGET_HOST ?? "10.0.0.1",
    netTargetPort: Number.parseInt(process.env.NET_TARGET_PORT ?? "9999", 10),
    metricsPort: Number.parseInt(process.env.METRICS_PORT ?? "8000", 10),
  };
}
