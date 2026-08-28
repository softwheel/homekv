# Spec 0002 — Baseline Benchmark Requirements

- Status: Verified
- Parent spec: `../0001-homekv-v1/`
- Tracking issue: #8

## Purpose

Capture a reproducible performance baseline of the current HomeKV prototype before storage-engine changes.

## Requirements

**REQ-BENCH-001** — The benchmark MUST measure GET, SET, DELETE, and an 80/20 read/write mix.

**REQ-BENCH-002** — The full baseline MUST sweep at least three dataset cardinalities and client concurrency levels 1, 8, and 32 where the measured path supports concurrent clients.

**REQ-BENCH-003** — Results MUST report throughput plus p50/p95/p99 latency and total measured operation count.

**REQ-BENCH-004** — Results MUST record key/value sizes, workload mix, dataset size, concurrency, HomeKV commit SHA, Rust toolchain, OS/kernel, CPU, logical CPU count, and RAM when available.

**REQ-BENCH-005** — The harness SHOULD report process memory and memory/key; allocations/op MAY be deferred if a reliable low-intrusion measurement is not practical.

**REQ-BENCH-006** — The baseline MUST be captured against the current COW/BTree prototype before M1 removes whole-store copy-on-write.

**REQ-BENCH-007** — The benchmark MUST include a dataset-size sweep that can reveal whether write cost grows with the size of the cloned underlying store.

**REQ-BENCH-008** — Benchmark setup and commands MUST be version-controlled and runnable by another developer without unpublished manual steps.

**REQ-BENCH-009** — Results MUST be labeled as single-node prototype results and MUST NOT be presented as distributed/strong-consistency performance.

**REQ-BENCH-010** — Benchmark datasets and operation selection MUST be deterministic from an explicit seed so runs are comparable.

**REQ-BENCH-011** — CI MUST run a bounded smoke benchmark sufficient to catch harness breakage, while the full baseline is a separate recorded run so normal PR CI is not dominated by benchmarking.

**REQ-BENCH-012** — Each full baseline configuration SHOULD be repeated at least three times. Variance or run-to-run spread MUST be retained rather than selecting only the best run.

## Accepted workload matrix

### Primary scaling profile

- key size: 16 bytes
- value size: 64 bytes
- dataset cardinalities: 1,000 / 10,000 / 50,000 keys
- concurrency: 1 / 8 / 32 for the server/client path
- workloads: GET / SET / DELETE / 80% GET + 20% SET

The storage-only MVCC path is serialized by design, so its primary concurrency value is 1; concurrency behavior is measured at the existing server path separately.

### Payload sensitivity profiles

At 10,000 keys and server concurrency 8, also measure:

- 16-byte keys / 256-byte values
- 32-byte keys / 1 KiB values

### Sampling

- warm-up before measured samples
- target measured window: 5 seconds for full server workload cells unless an accepted implementation note justifies an operation-count mode
- storage COW write-size sweep MAY use a bounded operation count to avoid pathological benchmark duration, but must record sample count and use the same count across compared dataset sizes
- smoke mode uses substantially smaller cardinality/duration and is not an authoritative performance result

## Non-goals

- comparing HomeKV against Redis/Valkey/Dragonfly yet
- distributed Raft measurements
- optimizing the implementation as part of baseline capture
- choosing Rust vs Zig
- treating GitHub-hosted CI numbers as release performance claims
