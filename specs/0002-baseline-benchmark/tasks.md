# Spec 0002 — Baseline Benchmark Tasks

- Status: Verified
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #8

Implementation is complete and the verification gate has passed.

## BENCH-T1 — Result/config schema — Complete

Requirements: `REQ-BENCH-003/004/008/010/011`

- define JSON configuration schema
- define machine-readable result schema
- implement deterministic key/value/workload generation
- implement percentile calculation with unit tests
- document smoke/full commands

Completion: a clean checkout can parse the checked-in smoke config and emit a schema-valid result.

## BENCH-T2 — Storage-engine baseline harness — Complete

Requirements: `REQ-BENCH-001` through `REQ-BENCH-007`, `REQ-BENCH-010`

- populate deterministic datasets
- GET benchmark
- SET benchmark
- DELETE benchmark
- 80/20 GET/SET workload
- dataset-size sweep
- bounded operation-count mode for COW-heavy writes

Completion: all primary storage workload cells emit valid results without changing production storage semantics.

## BENCH-T3 — Existing server/RPC baseline — Complete

Requirements: `REQ-BENCH-001/002/003/004/009/010`

- exercise existing public Tonic/Tokio server path
- concurrency 1/8/32
- no server optimization
- report RPC/runtime failures separately

Completion: server layer results are clearly separated from storage-only results.

## BENCH-T4 — Environment and memory metadata — Complete

Requirements: `REQ-BENCH-004/005`

- git SHA
- Rust version
- OS/kernel
- CPU/logical CPU count
- memory where available
- process memory/memory-key where practical

Unknown data is reported as unknown/null rather than guessed.

## BENCH-T5 — CI smoke mode — Complete

Requirements: `REQ-BENCH-008/011`

- add checked-in smoke config
- wire smoke run into CI after normal tests/build
- keep runtime bounded
- ensure smoke output is explicitly non-authoritative

## BENCH-T6 — Capture immutable prototype baseline — Complete

Requirements: `REQ-BENCH-002/003/004/006/007/009/012`

- run accepted full matrix against the pre-M1 prototype
- repeat configurations where practical
- retain raw result bundle/artifact
- preserve exact commit/configuration metadata
- do not modify M1 storage code in the baseline commit

Evidence:

- frozen pre-M1 SHA: `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`
- workflow run: `33080513696`
- artifact id: `9650486609`
- artifact digest: `sha256:015faf8d87c8d197457ca9c6fd2f2c8d1ab07dc0bc16a2e62a2517335331680c`
- retained repetitions: 3 storage + 3 server, plus storage/server memory bundles

## BENCH-T7 — Publish M0 summary — Complete

- summarize observed scaling/latency trends
- separate observations from hypotheses
- identify likely M1 bottlenecks without implementing fixes
- link raw artifacts/results
- mechanically validate mandatory result cells and p99 sample support
- publish a reproducible retained-artifact analyzer

Evidence:

- durable analysis: `m0-analysis.md`
- deterministic analyzer: `benchmarks/analyze_m0.py`
- retained-artifact verifier: `.github/workflows/m0-verification.yml`
- executed requirement/gate record: `verification.md`

## Verification handoff

BENCH-T1..T7 are complete. `verification.md` records PASS for REQ-BENCH-001..012 and the M0 acceptance gate. Spec 0002 is **Verified** and M1/#9 may proceed under Spec 0003.
