# HomeKV benchmarks

This directory implements Spec 0002 (M0 prototype baseline).

## Current slice

Implemented in the first M0 slice:

- BENCH-T1 — deterministic config/result schema and percentile helpers
- BENCH-T2 — storage-only `BTreeStore` + `Mvcc` baseline
- BENCH-T5 — bounded CI smoke mode

Still required before Spec 0002 can become Verified:

- BENCH-T3 — existing Tonic/Tokio server/RPC baseline with client concurrency 1/8/32
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

## What is measured

The current storage baseline intentionally exercises `Mvcc<BTreeStore>` exactly as the prototype uses it:

- GET includes acquisition of the current MVCC read snapshot and a B-tree lookup.
- SET updates an existing key and includes the first-write whole-store COW clone.
- DELETE targets a guaranteed-missing key. This keeps dataset cardinality constant while still triggering the current write transaction's COW clone.
- `read80_write20` deterministically mixes 80% GET and 20% SET.

Storage concurrency is reported as 1 because the prototype serializes writers. Concurrent client behavior is measured separately by BENCH-T3 at the RPC layer.

## Result semantics

Every result bundle is labeled:

```text
"authoritative_performance_result": false
```

M0 is a single-node prototype baseline, not a distributed or strong-consistency benchmark. Public comparative HomeKV claims require the later distributed correctness and benchmark specs.

The result includes the git SHA, Rust version, OS/kernel, CPU, logical CPU count, host memory where available, process RSS where available, workload parameters, measured operation count, throughput, and p50/p95/p99 latency.
