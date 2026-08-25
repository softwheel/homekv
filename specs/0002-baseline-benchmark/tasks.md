# Spec 0002 — Baseline Benchmark Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #8

Implementation may begin because this spec is Accepted.

## BENCH-T1 — Result/config schema

Requirements: `REQ-BENCH-003/004/008/010/011`

- define JSON configuration schema
- define machine-readable result schema
- implement deterministic key/value/workload generation
- implement percentile calculation with unit tests
- document smoke/full commands

Completion: a clean checkout can parse the checked-in smoke config and emit a schema-valid result.

## BENCH-T2 — Storage-engine baseline harness

Requirements: `REQ-BENCH-001` through `REQ-BENCH-007`, `REQ-BENCH-010`

- populate deterministic datasets
- GET benchmark
- SET benchmark
- DELETE benchmark
- 80/20 GET/SET workload
- dataset-size sweep
- bounded operation-count mode for COW-heavy writes

Completion: all primary storage workload cells emit valid results without changing production storage semantics.

## BENCH-T3 — Existing server/RPC baseline

Requirements: `REQ-BENCH-001/002/003/004/009/010`

- exercise existing public Tonic/Tokio server path
- concurrency 1/8/32
- no server optimization
- report RPC/runtime failures separately

Completion: server layer results are clearly separated from storage-only results.

## BENCH-T4 — Environment and memory metadata

Requirements: `REQ-BENCH-004/005`

- git SHA
- Rust version
- OS/kernel
- CPU/logical CPU count
- memory where available
- process memory/memory-key where practical

Unknown data is reported as unknown/null rather than guessed.

## BENCH-T5 — CI smoke mode

Requirements: `REQ-BENCH-008/011`

- add checked-in smoke config
- wire smoke run into CI after normal tests/build
- keep runtime bounded
- ensure smoke output is explicitly non-authoritative

## BENCH-T6 — Capture immutable prototype baseline

Requirements: `REQ-BENCH-002/003/004/006/007/009/012`

- run accepted full matrix against the pre-M1 prototype
- repeat configurations where practical
- retain raw result bundle/artifact
- preserve exact commit/configuration metadata
- do not modify M1 storage code in the baseline commit

## BENCH-T7 — Publish M0 summary

- summarize observed scaling/latency trends
- separate observations from hypotheses
- identify likely M1 bottlenecks without implementing fixes
- link raw artifacts/results

## Verification handoff

When BENCH-T1..T7 are complete, run `verification.md`. Spec 0002 moves to `Verified` only when the mandatory result cells and metadata pass that gate.
