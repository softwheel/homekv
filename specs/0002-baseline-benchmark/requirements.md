# Spec 0002 — Baseline Benchmark Requirements

- Status: Draft
- Parent spec: `../0001-homekv-v1/`
- Tracking issue: #8

## Purpose

Capture a reproducible performance baseline of the current HomeKV prototype before storage-engine changes.

## Requirements

**REQ-BENCH-001** — The benchmark MUST measure GET, SET, DELETE, and an 80/20 read/write mix.

**REQ-BENCH-002** — The benchmark MUST sweep at least three dataset cardinalities and multiple client concurrency levels.

**REQ-BENCH-003** — Results MUST report throughput plus p50/p95/p99 latency.

**REQ-BENCH-004** — Results MUST record key/value sizes, workload mix, dataset size, concurrency, HomeKV commit SHA, Rust toolchain, OS/kernel, CPU, and RAM.

**REQ-BENCH-005** — The harness SHOULD report allocations/op and memory/key where practical.

**REQ-BENCH-006** — The baseline MUST be captured against the current COW/BTree prototype before M1 removes whole-store copy-on-write.

**REQ-BENCH-007** — The benchmark MUST include a dataset-size sweep intended to reveal whether write cost grows with the size of the cloned underlying store.

**REQ-BENCH-008** — Benchmark setup and commands MUST be version-controlled and runnable by another developer without unpublished manual steps.

**REQ-BENCH-009** — Results MUST be labeled as single-node prototype results and MUST NOT be presented as distributed/strong-consistency performance.

## Default workload matrix

At minimum:

- 16-byte keys / 64-byte values
- 16-byte keys / 256-byte values
- 32-byte keys / 1 KiB values
- concurrency: 1, 8, 32 (higher levels may be added)
- small, medium, and large in-memory datasets sized relative to the benchmark host

## Non-goals

- comparing HomeKV against Redis/Valkey/Dragonfly yet
- distributed Raft measurements
- optimizing the implementation as part of baseline capture
- choosing Rust vs Zig
