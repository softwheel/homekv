# HomeKV benchmarks

This directory implements Spec 0002 (M0 prototype baseline).

## Current slice

Implemented:

- BENCH-T1 — deterministic config/result schema and percentile helpers
- BENCH-T2 — storage-only `BTreeStore` + `Mvcc` baseline
- BENCH-T3 — existing Tonic/Tokio server/RPC baseline with client concurrency 1/8/32
- BENCH-T5 — bounded CI smoke mode

Still required before Spec 0002 can become Verified:

- BENCH-T4 — any additional memory/accounting metrics that are practical
- BENCH-T6 — immutable full prototype result capture
- BENCH-T7 — baseline analysis/summary

No M1 storage optimization belongs in M0.

## Storage smoke

```bash
cargo run --bin hkvbench -- \
  --config benchmarks/configs/smoke-storage.json \
  --output target/hkvbench-smoke.json
```

Smoke mode exists to validate the harness in CI. It is not an authoritative performance result.

## Full storage baseline

Use a release build for recorded results:

```bash
cargo run --release --bin hkvbench -- \
  --config benchmarks/configs/baseline-storage.json \
  --output benchmarks/results/m0/storage-run-1.json
```

Repeat the full run at least three times on the same controlled host before drawing conclusions. Preserve every run rather than selecting only the fastest one.

## Full server/RPC baseline

BENCH-T3 deliberately uses the existing public Tonic/Tokio server path. Start the prototype server unchanged in one shell:

```bash
cargo run --release --bin homekv -- \
  --host 127.0.0.1 \
  --port 20001 \
  --public_host 127.0.0.1 \
  --gossip_port 20002
```

Then run the checked-in server matrix from another shell:

```bash
cargo run --release --bin hkvbench -- \
  --config benchmarks/configs/baseline-server.json \
  --output benchmarks/results/m0/server-run-1.json
```

The server matrix covers:

- 16-byte keys / 64-byte values at 1,000, 10,000, and 50,000 keys;
- client concurrency 1, 8, and 32 for each primary dataset size;
- GET, SET, DELETE, and deterministic 80% GET / 20% SET workloads;
- 16-byte keys / 256-byte values at 10,000 keys and concurrency 8;
- 32-byte keys / 1 KiB values at 10,000 keys and concurrency 8;
- a 5-second target measured window per server workload cell.

The harness preloads deterministic data through the public SET RPC before measurements. Preload is outside the measured window. Tonic clients are cloned per worker, each worker uses a deterministic seed derived from the global seed and worker id, and successful request latency samples are aggregated across workers.

RPC failures and Tokio task/runtime failures are reported separately. `measured_operations` counts successful measured RPCs, while `attempted_operations` includes measured RPC attempts that returned an RPC failure. A worker task failure is reported in `runtime_failures`.

The prototype server's existing per-request logging remains enabled. BENCH-T3 must preserve the server path rather than optimize it, so those costs are intentionally part of this historical baseline. The `process_rss_bytes` value in a server-layer result describes the `hkvbench` client process; server-process memory evidence belongs to BENCH-T4.

Repeat the full server run at least three times on the same controlled host and retain every run.

## What is measured

The storage baseline intentionally exercises `Mvcc<BTreeStore>` exactly as the prototype uses it:

- GET includes acquisition of the current MVCC read snapshot and a B-tree lookup.
- SET updates an existing key and includes the first-write whole-store COW clone.
- DELETE targets a guaranteed-missing key. This keeps dataset cardinality constant while still triggering the current write transaction's COW clone.
- `read80_write20` deterministically mixes 80% GET and 20% SET.

Storage concurrency is reported as 1 because the prototype serializes writers. Concurrent client behavior is measured separately at the RPC layer.

For the server baseline, DELETE also targets a guaranteed-missing key so the dataset cardinality remains stable across concurrency/workload cells.

## Result semantics

Every result bundle is labeled:

```text
"authoritative_performance_result": false
```

M0 is a single-node prototype baseline, not a distributed or strong-consistency benchmark. Public comparative HomeKV claims require the later distributed correctness and benchmark specs.

The result includes the git SHA, Rust version, OS/kernel, CPU, logical CPU count, host memory where available, benchmark-process RSS where available, workload parameters, attempted/successful operation counts, throughput, p50/p95/p99 latency, and failure counters.
