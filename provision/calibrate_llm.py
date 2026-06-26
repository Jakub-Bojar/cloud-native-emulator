#!/usr/bin/env python3
"""
LLM load calibrator — derive the emulator's (a, b) coefficients from a real model.

Drives any OpenAI-compatible LLM endpoint (Ollama, vLLM, llama.cpp server,
LM Studio, TGI, …) at a sweep of concurrency levels with k6, measures the CPU,
RAM and network footprint at each level, then least-squares fits the worker's
load function

    load = max(0, a * x + b)        (cpu→millicores, ram→MB, net→Mbps)

so the resulting {a, b} can be pasted straight into a template's `apps[].load`.
`x` here is the SOURCE signal — concurrent in-flight requests (= k6 VUs) — the
same unit runtime_scenarios inject at a DAG's source app. (Downstream apps see x
in Mbps of inbound traffic, a separate calibration; see ARCHITECTURE.md.)

Division of labour
------------------
- k6 owns the load: VUs = concurrency, and its built-in `data_sent` /
  `data_received` counters give the network bytes directly (NET), plus request
  count and a custom token counter for a throughput sanity check.
- Python owns the server-side resource sampling that k6 can't see: the model
  server's CPU-time delta (→ millicores) and RSS (→ MB), matched by process name
  (`--proc-match`, so it works for whatever server you run), then the linear fit
  and the report.

Universal across servers
------------------------
- Endpoint: POST `/v1/chat/completions` (override with `--endpoint`). Every common
  local server speaks this, including Ollama.
- Process match: `--proc-match` is a case-insensitive regex over each process's
  command line; the default covers the usual suspects. Set it to your server's
  binary if it's something else.

No third-party Python deps — pure stdlib. Requires the `k6` binary on PATH
(`brew install k6`).

Example
-------
    brew install k6
    OLLAMA_NUM_PARALLEL=8 ollama serve &        # any OpenAI-compatible server
    ollama pull llama3.2:3b

    python3 provision/calibrate_llm.py \
        --model llama3.2:3b --x 0 1 2 4 8 \
        --duration 40 --out calibration/llm-3b.csv
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


# k6 script: each VU posts a chat completion in a loop for the run's duration.
# data_sent/data_received are built-in; we add token counters from the response
# `usage` block. handleSummary writes just the numbers we need to $K6_SUMMARY.
K6_SCRIPT = r"""
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const completionTokens = new Counter('completion_tokens');
const promptTokens = new Counter('prompt_tokens');

export const options = {
  vus: Number(__ENV.VUS),
  duration: __ENV.DURATION,
};

export default function () {
  const payload = JSON.stringify({
    model: __ENV.MODEL,
    messages: [{ role: 'user', content: __ENV.PROMPT }],
    max_tokens: Number(__ENV.MAX_TOKENS),
    stream: false,
  });
  const res = http.post(__ENV.HOST + __ENV.ENDPOINT, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '600s',
  });
  try {
    const u = res.json('usage');
    if (u) {
      completionTokens.add(u.completion_tokens || 0);
      promptTokens.add(u.prompt_tokens || 0);
    }
  } catch (e) { /* non-JSON / error response */ }
}

export function handleSummary(data) {
  const m = data.metrics;
  const count = (k) => (m[k] && m[k].values ? (m[k].values.count || 0) : 0);
  const out = {
    http_reqs: count('http_reqs'),
    data_sent: count('data_sent'),
    data_received: count('data_received'),
    completion_tokens: count('completion_tokens'),
    prompt_tokens: count('prompt_tokens'),
    failed_rate: m.http_req_failed ? m.http_req_failed.values.rate : 0,
    latency_avg_ms: m.http_req_duration ? m.http_req_duration.values.avg : 0,
    duration_s: data.state.testRunDurationMs / 1000,
  };
  return { [__ENV.K6_SUMMARY]: JSON.stringify(out) };
}
"""


# ── HTTP: preload so the model is resident before the x=0 idle baseline ──────

def preload(args) -> None:
    body = json.dumps({
        "model": args.model,
        "messages": [{"role": "user", "content": "ready?"}],
        "max_tokens": 8,
        "stream": False,
    }).encode()
    req = urllib.request.Request(
        args.host.rstrip("/") + args.endpoint, data=body,
        headers={"Content-Type": "application/json"})
    print(f"preloading {args.model} ...", flush=True)
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            resp.read()
    except urllib.error.URLError as exc:
        sys.exit(f"\nCannot reach the model at {args.host}{args.endpoint} ({exc}).\n"
                 f"Start an OpenAI-compatible server, e.g.:\n"
                 f"  OLLAMA_NUM_PARALLEL=8 ollama serve   &&   ollama pull {args.model}")


# ── Process sampling: the model server's CPU time + RSS ──────────────────────

def _parse_ps_time(s: str) -> float:
    """macOS `ps -o time` -> seconds. Format is [[dd-]hh:]mm:ss.ss."""
    days = 0
    if "-" in s:
        d, s = s.split("-", 1)
        days = int(d)
    parts = [float(p) for p in s.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    h, m, sec = parts[-3], parts[-2], parts[-1]
    return days * 86400 + h * 3600 + m * 60 + sec


def sample_server(proc_re: "re.Pattern") -> tuple[float, float]:
    """(total RSS in MB, total CPU-seconds) summed across every process whose
    command matches `proc_re` — captures the server plus any inference child.
    Our own tooling (this script, k6) is excluded."""
    out = subprocess.run(
        ["ps", "-axo", "rss=,time=,command="],
        capture_output=True, text=True).stdout
    rss_kb = 0
    cpu_s = 0.0
    for line in out.splitlines():
        if "calibrate_llm" in line or "/k6 " in line or line.endswith("k6"):
            continue
        if not proc_re.search(line):
            continue
        parts = line.split(None, 2)  # rss, time, command
        if len(parts) < 3:
            continue
        try:
            rss_kb += int(parts[0])
            cpu_s += _parse_ps_time(parts[1])
        except ValueError:
            continue
    return rss_kb / 1024.0, cpu_s


def _collect(proc: "subprocess.Popen | None", duration: float,
             proc_re: "re.Pattern") -> tuple[float, float]:
    """Sample server CPU-time delta + mean RSS over a window. The window is the
    lifetime of `proc` (a k6 run), or `duration` seconds when proc is None (the
    x=0 idle baseline)."""
    _, cpu0 = sample_server(proc_re)
    t0 = time.monotonic()
    rss: list[float] = []
    while True:
        time.sleep(1.0)
        mb, _ = sample_server(proc_re)
        rss.append(mb)
        if proc is not None:
            if proc.poll() is not None:
                break
        elif time.monotonic() - t0 >= duration:
            break
    _, cpu1 = sample_server(proc_re)
    wall = max(1e-6, time.monotonic() - t0)
    cpu_millicores = (cpu1 - cpu0) / wall * 1000.0
    ram_mb = statistics.fmean(rss) if rss else 0.0
    return cpu_millicores, ram_mb


# ── One concurrency level ────────────────────────────────────────────────────

def measure_level(x: int, args, script_path: str,
                  proc_re: "re.Pattern") -> dict:
    """Drive the model at concurrency `x` (k6 VUs) and return measured CPU/RAM/net.
    x=0 measures the idle baseline with no load."""
    if x == 0:
        cpu, ram = _collect(None, args.duration, proc_re)
        return {"x": 0, "cpu_millicores": round(cpu, 1), "ram_mb": round(ram, 1),
                "net_mbps": 0.0, "tokens_per_s": 0.0, "requests": 0,
                "failed_rate": 0.0, "latency_ms": 0.0}

    summary_path = tempfile.mktemp(prefix="k6-summary-", suffix=".json")
    env = dict(os.environ,
               HOST=args.host.rstrip("/"), ENDPOINT=args.endpoint,
               MODEL=args.model, PROMPT=args.prompt,
               MAX_TOKENS=str(args.max_tokens),
               VUS=str(x), DURATION=f"{int(args.duration)}s",
               K6_SUMMARY=summary_path)
    proc = subprocess.Popen(["k6", "run", "--quiet", script_path], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cpu, ram = _collect(proc, args.duration, proc_re)
    proc.wait()

    with open(summary_path) as f:
        s = json.load(f)
    os.remove(summary_path)
    dur = s["duration_s"] or args.duration
    net_mbps = (s["data_sent"] + s["data_received"]) * 8 / 1e6 / dur
    return {
        "x": x,
        "cpu_millicores": round(cpu, 1),
        "ram_mb": round(ram, 1),
        "net_mbps": round(net_mbps, 4),
        "tokens_per_s": round(s["completion_tokens"] / dur, 1),
        "requests": int(s["http_reqs"]),
        "failed_rate": round(s["failed_rate"], 3),
        "latency_ms": round(s["latency_avg_ms"], 1),
    }


# ── Linear fit ───────────────────────────────────────────────────────────────

def fit(points: list[tuple[float, float]]) -> tuple[float, float, float]:
    """Ordinary least squares y = a*x + b over (x, y) points; returns (a, b, R²)."""
    n = len(points)
    if n == 0:
        return 0.0, 0.0, 0.0
    sx = sum(p[0] for p in points)
    sy = sum(p[1] for p in points)
    sxx = sum(p[0] * p[0] for p in points)
    sxy = sum(p[0] * p[1] for p in points)
    denom = n * sxx - sx * sx
    if denom == 0:  # only one distinct x — can't fit a slope
        return 0.0, sy / n, 0.0
    a = (n * sxy - sx * sy) / denom
    b = (sy - a * sx) / n
    ybar = sy / n
    ss_tot = sum((p[1] - ybar) ** 2 for p in points)
    ss_res = sum((p[1] - (a * p[0] + b)) ** 2 for p in points)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return a, b, r2


# ── Standalone HTML report (no third-party deps) ─────────────────────────────

def _svg_panel(title: str, unit: str, points: list[tuple[float, float]],
               a: float, b: float, r2: float,
               w: int = 380, h: int = 260) -> str:
    """One inline-SVG scatter panel: measured points + the fitted a*x+b line."""
    xs = [p[0] for p in points] or [0]
    ys = [p[1] for p in points] or [0]
    xmax = max(xs + [1])
    ymax = max(ys + [a * xmax + b, 1]) * 1.12
    pl, pr, pt, pb = 52, 14, 34, 34

    def sx(x: float) -> float:
        return pl + (x / xmax) * (w - pl - pr)

    def sy(y: float) -> float:
        return h - pb - (y / ymax) * (h - pt - pb)

    parts = [f'<text x="{pl}" y="20" class="t">{title}</text>']
    for i in range(5):
        gy = ymax * i / 4
        y = sy(gy)
        parts.append(f'<line x1="{pl}" y1="{y:.1f}" x2="{w-pr}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{pl-6}" y="{y+3:.1f}" class="yl">{gy:.0f}</text>')
    for x in sorted(set(xs)):
        parts.append(f'<text x="{sx(x):.1f}" y="{h-pb+16:.0f}" class="xl">{x:g}</text>')
    parts.append(
        f'<line x1="{sx(0):.1f}" y1="{sy(max(0,b)):.1f}" '
        f'x2="{sx(xmax):.1f}" y2="{sy(max(0,a*xmax+b)):.1f}" class="fit"/>')
    for x, y in points:
        parts.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4.5" class="pt"/>')
    parts.append(
        f'<text x="{w-pr}" y="{pt+4}" class="eq">load = {a:g}·x + {b:g} '
        f'&#160;({unit}, R²={r2:g})</text>')
    return f'<svg viewBox="0 0 {w} {h}" class="panel">{"".join(parts)}</svg>'


def write_html_report(rows: list[dict], fits: dict, model: str, path: str) -> None:
    """Self-contained HTML report — open in any browser, no dependencies."""
    panels = "".join(
        _svg_panel(
            {"cpu": "CPU", "ram": "RAM", "net": "Network"}[ax],
            {"cpu": "millicores", "ram": "MB", "net": "Mbps"}[ax],
            [(r["x"], r[key]) for r in rows],
            fits[ax]["a"], fits[ax]["b"], fits[ax]["r2"])
        for ax, key in (("cpu", "cpu_millicores"),
                        ("ram", "ram_mb"),
                        ("net", "net_mbps")))
    cols = ("x", "cpu_millicores", "ram_mb", "net_mbps",
            "tokens_per_s", "requests", "failed_rate", "latency_ms")
    trows = "".join(
        "<tr>" + "".join(f"<td>{r[c]}</td>" for c in cols) + "</tr>"
        for r in rows)
    load_block = json.dumps(
        {ax: {"a": fits[ax]["a"], "b": fits[ax]["b"]}
         for ax in ("cpu", "ram", "net")}, separators=(",", ":"))
    html = f"""<!doctype html><meta charset="utf-8">
<title>LLM calibration — {model}</title>
<style>
 body{{font:14px -apple-system,system-ui,sans-serif;margin:24px;color:#1a1a1a}}
 h1{{font-size:18px}} code{{background:#f2f2f2;padding:2px 6px;border-radius:4px}}
 .panels{{display:flex;flex-wrap:wrap;gap:12px}}
 .panel{{width:380px;border:1px solid #e3e3e3;border-radius:8px;background:#fff}}
 .t{{font-size:13px;font-weight:600;fill:#1a1a1a}}
 .grid{{stroke:#eee;stroke-width:1}}
 .fit{{stroke:#2563eb;stroke-width:2}} .pt{{fill:#ef4444}}
 .yl{{fill:#888;font-size:10px;text-anchor:end}}
 .xl{{fill:#888;font-size:10px;text-anchor:middle}}
 .eq{{fill:#2563eb;font-size:11px;text-anchor:end;font-weight:600}}
 table{{border-collapse:collapse;margin-top:16px;font-size:12px}}
 td,th{{border:1px solid #e3e3e3;padding:4px 8px;text-align:right}}
</style>
<h1>LLM load calibration — <code>{model}</code></h1>
<p>x = concurrent requests (k6 VUs). Points = measured; line = least-squares fit.</p>
<div class="panels">{panels}</div>
<p>Paste into a template app's load:</p>
<pre><code>{{"load":{load_block}}}</code></pre>
<table><tr><th>x</th><th>cpu (mc)</th><th>ram (MB)</th><th>net (Mbps)</th>
<th>tok/s</th><th>reqs</th><th>fail</th><th>lat (ms)</th></tr>{trows}</table>
"""
    with open(path, "w") as f:
        f.write(html)


# ── Orchestration ────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="http://localhost:11434",
                    help="server base URL (Ollama 11434, vLLM/llama.cpp 8000, …)")
    ap.add_argument("--endpoint", default="/v1/chat/completions",
                    help="OpenAI-compatible chat endpoint")
    ap.add_argument("--model", default="llama3.2:3b")
    ap.add_argument("--x", type=int, nargs="+", default=[0, 1, 2, 4, 8],
                    help="concurrency levels (k6 VUs); include 0 for the idle baseline")
    ap.add_argument("--duration", type=float, default=40.0,
                    help="measured window per level, seconds")
    ap.add_argument("--max-tokens", type=int, default=200)
    ap.add_argument("--prompt",
                    default="Explain how a CPU pipeline works, step by step, in detail.")
    ap.add_argument("--proc-match", default=r"ollama|llama[-_.]?(server|cpp)|vllm|mlx|lm[-_ ]?studio|text-generation",
                    help="case-insensitive regex matching the model server's process")
    ap.add_argument("--out", default="calibration/llm-calibration.csv")
    args = ap.parse_args()

    if not _have_k6():
        sys.exit("k6 not found on PATH. Install it with:  brew install k6")
    proc_re = re.compile(args.proc_match, re.IGNORECASE)

    script_path = tempfile.mktemp(prefix="calib-k6-", suffix=".js")
    with open(script_path, "w") as f:
        f.write(K6_SCRIPT)
    preload(args)

    rows: list[dict] = []
    try:
        for x in args.x:
            print(f"\n── level x={x} (k6 {int(args.duration)}s) ──", flush=True)
            row = measure_level(x, args, script_path, proc_re)
            rows.append(row)
            print(f"  cpu={row['cpu_millicores']}mc  ram={row['ram_mb']}MB  "
                  f"net={row['net_mbps']}Mbps  {row['tokens_per_s']}tok/s  "
                  f"reqs={row['requests']}  fail={row['failed_rate']}  "
                  f"lat={row['latency_ms']}ms", flush=True)
    finally:
        os.path.exists(script_path) and os.remove(script_path)

    fits = {}
    for axis, key in (("cpu", "cpu_millicores"),
                      ("ram", "ram_mb"), ("net", "net_mbps")):
        a, b, r2 = fit([(r["x"], r[key]) for r in rows])
        fits[axis] = {"a": round(a, 4), "b": round(b, 2), "r2": round(r2, 4)}

    out_path = args.out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    cols = ["x", "cpu_millicores", "ram_mb", "net_mbps",
            "tokens_per_s", "requests", "failed_rate", "latency_ms"]
    with open(out_path, "w") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(str(r[c]) for c in cols) + "\n")

    load_block = {ax: {"a": fits[ax]["a"], "b": fits[ax]["b"]}
                  for ax in ("cpu", "ram", "net")}
    json_path = os.path.splitext(out_path)[0] + ".fit.json"
    with open(json_path, "w") as f:
        json.dump({"model": args.model, "x": args.x,
                   "rows": rows, "fits": fits, "load": load_block}, f, indent=2)
    html_path = os.path.splitext(out_path)[0] + ".html"
    write_html_report(rows, fits, args.model, html_path)

    print("\n" + "=" * 64)
    print(f"wrote {out_path}\nwrote {json_path}\nwrote {html_path}  (open to visualise)")
    print("\nfit  load = a*x + b   (R² in parens)")
    for ax in ("cpu", "ram", "net"):
        unit = {"cpu": "mc", "ram": "MB", "net": "Mbps"}[ax]
        print(f"  {ax:<3} a={fits[ax]['a']:<10} b={fits[ax]['b']:<10} "
              f"({unit})  R²={fits[ax]['r2']}")
    print("\npaste into a template app's load:")
    print("  " + json.dumps({"load": load_block}, separators=(",", ":")))


def _have_k6() -> bool:
    try:
        subprocess.run(["k6", "version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


if __name__ == "__main__":
    main()
