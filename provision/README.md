# provision — host-side infrastructure provisioner (Phase 1)

Turns the `sites` + `network_links` sections of a template into joined,
labelled MicroK8s worker nodes backed by Multipass VMs. Runs on the **Mac
host**, not in the controller pod (a pod can't drive `multipass`).

This is Phase 1 of the Multipass design: it creates the nodes. Phases 2–3
(deploying workloads via the existing controller, and Chaos Mesh link
injection) reuse the existing system and are not part of this tool.

## Prerequisites

- `multipass` installed on the host
- An existing MicroK8s control-plane VM (default name `microk8s-vm`)
- The control-plane VM reachable from new worker VMs (same Multipass network)

## Usage

```bash
# Show desired nodes vs what currently exists
python3 provision/provision.py status provision/example-infra.json

# Create missing nodes, join them, label them (idempotent — safe to re-run)
python3 provision/provision.py up provision/example-infra.json

# Drain + delete nodes and their VMs (prompts first; -y to skip)
python3 provision/provision.py down provision/example-infra.json
```

## Adopting existing nodes

If a VM named in the template already exists, `up` does **not** recreate it —
it skips the launch, waits for Ready, and just (re)applies the labels. This is
how you adopt a hand-built cluster: point a site at the existing VM with
`provision.node_names` and `up` will only add the missing `topology.tier_id`
label. `example-infra.json` is wired this way for the existing edge/fog/cloud
nodes, so running `up` against it is non-destructive (label-only).

```jsonc
"provision": { "node_names": ["edge"], "cpus": 2, "memory": "4G" }
```

Without `node_names`, node names are derived as `<site_id>-<i>` and the VMs are
created from scratch. `down` (with confirmation) drains and deletes whatever
the template resolves to — including adopted nodes, so be careful pointing it
at a cluster you want to keep.

Flags: `--control-plane <vm>` (default `microk8s-vm`), `--image <img>`
(default `22.04`), `--timeout <s>` (Ready wait, default 300), `-v` (debug).

## What `up` does per node

1. `microk8s add-node` on the control plane → fresh one-time join command.
2. Render cloud-init with that command and `multipass launch` the VM.
3. cloud-init installs MicroK8s and joins as a `--worker` node.
4. Wait until the node is `Ready`, then label it:
   - `topology.tier_id=<tier_id>` — the seam workloads schedule against
   - `tier=<tier_name>` — back-compat with the current materialiser's nodeSelector
   - `emulator.local/provisioned=true`, `emulator.local/site=<site_id>` — markers

It's idempotent: existing VMs are skipped, labels are re-applied every run.

## Calibrating load coefficients from a real LLM (`calibrate_llm.py`)

`calibrate_llm.py` derives a template's `(a, b)` coefficients from a real model
instead of guessing them. `k6` drives a sweep of concurrency levels against the
model's OpenAI-compatible endpoint; the script samples the server's CPU/RAM at
each level and least-squares fits the worker's `load = max(0, a*x + b)` per axis.
Pure stdlib on the Python side (no pip); needs the `k6` binary.

Works with **any OpenAI-compatible server** — Ollama, vLLM, llama.cpp server,
LM Studio, TGI — via `POST /v1/chat/completions`. Example with Ollama:

```bash
brew install k6 ollama
OLLAMA_NUM_PARALLEL=8 ollama serve          # leave running; parallel slots so high x isn't queued
ollama pull llama3.2:3b

python3 provision/calibrate_llm.py \
    --model llama3.2:3b --x 0 1 2 4 8 \
    --duration 40 --out calibration/llm-3b.csv
open calibration/llm-3b.html                # standalone visualisation (scatter + fit line per axis)
```

For a different server, point `--host` at it and set `--proc-match` to its
process name so CPU/RAM sampling finds it, e.g. vLLM:

```bash
python3 provision/calibrate_llm.py --host http://localhost:8000 \
    --model meta-llama/Llama-3.1-8B-Instruct --proc-match vllm \
    --x 0 2 4 8 16 --out calibration/vllm-8b.csv
```

Outputs alongside `--out`: the CSV, a `.fit.json` (rows + fits + a ready-to-paste
`load` block), and a self-contained `.html` report.

What it measures, and why:
- `x` = concurrent requests = k6 VUs — the SOURCE signal, same unit
  `runtime_scenarios` inject at a DAG's source app. `x=0` is the idle baseline (→ `b`).
- CPU (millicores): the server process-tree's CPU-time delta / wall time (exact;
  macOS `ps %cpu` is a decayed average and unreliable for short windows).
- RAM (MB): mean RSS of the server tree — resident weights (`b`) + KV cache that
  grows with concurrency (`a`).
- NET (Mbps): k6's own `data_sent` + `data_received` over the run. A localhost
  model's NIC traffic is loopback, so these payload bytes (prompt in + tokens out)
  stand in for what would cross a link when the tiers are distributed — which is
  what the emulator's net axis represents.

The fitted `(a, b)` are SOURCE-app coefficients (resource per request) — paste
them onto a DAG's entry app. Downstream apps take `x` in inbound Mbps, so those
are a separate calibration (see ARCHITECTURE.md / `_compute_resolved_x`).

## Notes / gotchas

- VM size comes from each site's `capacity`: `cpu_vcpu` = cores, `memory_gib`
  = RAM (and optional `disk_gib`). These are what the VM actually gets. An
  optional `provision` block overrides `cpus`/`memory`/`disk` and carries
  `node_names`/`node_count`. Keep capacities small — they must fit your host.
- Keep node counts small — each VM wants ~2–4 GB of host RAM, and more nodes
  add join/heartbeat load to the single control plane.
- After the host sleeps, joined nodes can hit Calico `Unauthorized`; restart
  the affected node's `calico-node` pod (known issue).
