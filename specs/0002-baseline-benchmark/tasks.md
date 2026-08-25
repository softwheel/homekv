# Spec 0002 — Baseline Benchmark Tasks

- Status: Draft
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #8

Implementation begins only after this spec is Accepted.

## BENCH-T1 — Select benchmark dependencies and result format

Requirements: `REQ-BENCH-003`, `REQ-BENCH-004`, `REQ-BENCH-008`

- select latency histogram / benchmark libraries
- define machine-readable configuration schema
- define result metadata schema
- document benchmark commands

## BENCH-T2 — Storage-engine baseline harness

Requirements: `REQ-BENCH-001` through `REQ-BENCH-007`

- populate deterministic datasets
- GET benchmark
- SET benchmark
- DELETE benchmark
- 80/20 workload
- dataset-size sweep
- concurrency sweep where storage API semantics permit

## BENCH-T3 — Existing server/RPC baseline

Requirements: `REQ-BENCH-001`, `REQ-BENCH-002`, `REQ-BENCH-003`, `REQ-BENCH-004`, `REQ-BENCH-009`

Measure the existing Tonic/Tokio path separately from the storage-only baseline.

## BENCH-T4 — Environment and memory metrics

Requirements: `REQ-BENCH-004`, `REQ-BENCH-005`

- capture git/toolchain/system metadata
- collect process memory
- add allocation metrics if a reliable low-intrusion method is practical

## BENCH-T5 — Capture immutable prototype baseline

Requirements: `REQ-BENCH-006`, `REQ-BENCH-007`, `REQ-BENCH-009`

Run the accepted workload matrix against the pre-M1 prototype and preserve the results with the exact commit/configuration.

## BENCH-T6 — Publish baseline summary

Summarize what dominates the current read/write path, explicitly separating observations from hypotheses for M1.

No storage optimization is part of this task.
