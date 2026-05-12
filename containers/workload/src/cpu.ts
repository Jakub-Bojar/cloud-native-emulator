import { readFileSync } from "node:fs";
import { cpus } from "node:os";

// Returns the number of CPU cores allocated to this container by the cgroup,
// rounded up. Falls back to the host core count if no cgroup limit is set.
// Why rounded up: a 1.5-core limit needs 2 workers to actually reach 1.5 cores
// of work; the per-worker duty cycle is then scaled to compensate.
export function detectAllocatedCores(): { cores: number; fractional: number; source: string } {
  // cgroup v2: single file with "quota period" (or "max period" for unlimited)
  try {
    const raw = readFileSync("/sys/fs/cgroup/cpu.max", "utf8").trim();
    const [quotaStr, periodStr] = raw.split(/\s+/);
    if (quotaStr && periodStr && quotaStr !== "max") {
      const quota = Number.parseInt(quotaStr, 10);
      const period = Number.parseInt(periodStr, 10);
      if (quota > 0 && period > 0) {
        const fractional = quota / period;
        return { cores: Math.ceil(fractional), fractional, source: "cgroup v2" };
      }
    }
  } catch {
    // not v2 or not in a container
  }

  // cgroup v1: two separate files
  try {
    const quota = Number.parseInt(readFileSync("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "utf8").trim(), 10);
    const period = Number.parseInt(readFileSync("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "utf8").trim(), 10);
    if (quota > 0 && period > 0) {
      const fractional = quota / period;
      return { cores: Math.ceil(fractional), fractional, source: "cgroup v1" };
    }
  } catch {
    // no v1 cpu controller
  }

  const host = cpus().length;
  return { cores: host, fractional: host, source: "os.cpus()" };
}
