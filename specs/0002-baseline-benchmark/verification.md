# Spec 0002 — Baseline Benchmark Verification

- Status: Verified
- Requirements: `requirements.md`
- Analysis: `m0-analysis.md`
- Tracking issue: #8

## Verification result

**PASS.** Spec 0002 satisfies the M0 acceptance gate against the immutable pre-M1 baseline commit `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`.

The retained BENCH-T6 evidence was captured by workflow run [33080513696](https://github.com/softwheel/homekv/actions/runs/33080513696) as artifact `9650486609`, named `homekv-m0-baseline-bc613b74e8c718a7d002f1cacbd8d51cddbf3067`, with digest:

```text
sha256:015faf8d87c8d197457ca9c6fd2f2c8d1ab07dc0bc16a2e62a2517335331680c
```

BENCH-T7 independently downloaded that artifact and confirmed the archive SHA-256 exactly before analyzing the retained JSON bundles. The durable result interpretation is in `m0-analysis.md`; `benchmarks/analyze_m0.py` and the `M0 Verification` workflow make the retained-artifact analysis reproducible.

## Verification matrix

| Requirement | Result | Proof |
| --- | --- | --- |
| REQ-BENCH-001 | PASS | storage and server bundles contain GET, SET, DELETE, and deterministic 80/20 GET/SET cells |
| REQ-BENCH-002 | PASS | primary results contain 1k/10k/50k datasets; server path contains concurrency 1/8/32 |
| REQ-BENCH-003 | PASS | every performance cell records throughput, p50/p95/p99, and measured operation count |
| REQ-BENCH-004 | PASS | retained rows record key/value sizes, workload, cardinality, concurrency, exact HomeKV SHA, Rust version, OS/kernel, CPU model, logical CPU count, and RAM |
| REQ-BENCH-005 | PASS | storage/server RSS evidence and approximate bytes/key are retained; allocations/op remain explicitly deferred as permitted |
| REQ-BENCH-006 | PASS | every retained result identifies `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`; PR #18 only corrected benchmark preload batching outside the measured window and contains no M1 storage optimization |
| REQ-BENCH-007 | PASS | `m0-analysis.md` documents the 1k/10k/50k storage mutation-cost trend and ratios |
| REQ-BENCH-008 | PASS | configs, benchmark binaries, smoke/full commands, immutable capture workflow, analyzer, and verification workflow are version-controlled |
| REQ-BENCH-009 | PASS | all bundles set `authoritative_performance_result=false` and label results as single-node prototype/non-distributed |
| REQ-BENCH-010 | PASS | checked-in seed/config generation is deterministic; generator golden-vector and percentile/config tests were exercised by Rust CI before capture |
| REQ-BENCH-011 | PASS | normal Rust CI runs bounded smoke benchmarks; the full M0 capture is a separate workflow/run |
| REQ-BENCH-012 | PASS | three complete storage repetitions and three complete server repetitions are retained; run-to-run spread is reported rather than selecting a best run |

## Harness and CI evidence

The implementation slices were merged only after their required checks passed:

- PR #11 — deterministic/storage benchmark harness
- PR #15 — existing Tonic/Tokio server benchmark
- PR #16 — isolated storage/server memory evidence
- PR #18 — preload batch-size correction for the 32B-key/1KiB-value sensitivity dataset
- PR #17 — immutable repeated BENCH-T6 capture

The retained capture workflow itself completed successfully and validated exactly eight JSON result bundles: three storage repetitions, three server repetitions, one storage-memory bundle, and one server-memory bundle.

The BENCH-T7 verification workflow adds a second guard: it checks the retained artifact id/name/digest/run identity, downloads that exact T6 artifact, validates mandatory cells and repetitions with `benchmarks/analyze_m0.py`, and emits a derived Markdown report without rerunning the historical benchmark.

## M0 acceptance gate

| Gate | Result | Evidence |
| --- | --- | --- |
| 1. harness/configs merged | PASS | BENCH-T1..T5 merged before capture |
| 2. smoke mode passes CI | PASS | Rust workflow on the implementation/capture PRs |
| 3. exact pre-M1 baseline captured | PASS | frozen SHA `bc613b74e8c718a7d002f1cacbd8d51cddbf3067` |
| 4. mandatory primary cells have valid throughput/p50/p95/p99 | PASS | all required storage/server cells present with zero measured/RPC/runtime failures |
| 5. payload-sensitivity cells captured | PASS | 16B/256B and 32B/1KiB at 10k keys/concurrency 8 retained for all workloads |
| 6. environment metadata complete where exposed | PASS | Rust/OS/kernel/CPU/logical CPUs/RAM all present and consistent across repetitions |
| 7. dataset-size write-cost trend documented | PASS | `m0-analysis.md` |
| 8. results explicitly non-distributed | PASS | bundle flag + per-result notes + analysis labeling |
| 9. no M1 storage optimization in baseline | PASS | frozen commit is the corrected M0 prototype; M1/#9 remained blocked during capture and analysis |

## Stability and interpretation checks

- warm-up is part of every measured path before recorded operations;
- all three repetitions are retained and summarized by median plus run spread;
- every storage cell uses the accepted bounded 200-operation mode, so its p99 has only about two top-1% observations and is explicitly flagged as a **low-support tail estimate**;
- the slow 32B-key/1KiB-value server SET sensitivity cell has a minimum 386 successful samples in a repetition, or about four top-1% observations, and is also flagged low-support;
- the other server cells have at least five approximate top-1% observations in their smallest repetition;
- no CPU-utilization or lock-profile data was captured, so concurrency-related queueing is recorded as an observation and causal attribution is left to M1 profiling;
- the largest 50k COW-heavy storage cells show high run-to-run spread on the GitHub-hosted runner; those numbers are retained but remain non-authoritative.

Low p99 sample support does **not** fail gate 4: the accepted contract requires recorded valid p50/p95/p99 and sample counts, plus explicit flagging when the tail sample is too small for strong interpretation. That condition is satisfied.

## Verified M0 observations

The baseline establishes a clear comparison point for Spec 0003:

- storage SET median throughput falls from about 16.5k ops/s at 1k keys to about 113 ops/s at 50k keys, while p50 grows from about 59 µs to 7.2 ms;
- DELETE shows nearly the same dataset-size mutation cliff;
- storage GET degrades much more gradually across the same cardinalities;
- server GET throughput benefits from concurrency, while mutation throughput saturates early and mutation latency grows sharply;
- larger stored values have a modest effect on GET but a dramatic effect on mutation throughput/tails;
- primary isolated storage RSS is about 188 bytes/key at 10k/50k; fresh server-process RSS is about 370–377 bytes/key for the same 16B/64B logical payload.

These are observations. The hypothesis that whole-store COW is the dominant mutation bottleneck is deferred to M1 comparison/profiling for causal confirmation.

## Residual limitations

- GitHub-hosted runner numbers are not release or comparative performance claims.
- M0 is single-node and does not measure Raft, durability, or distributed strong consistency.
- storage p99 sample support is intentionally limited by the bounded COW-heavy operation count.
- RSS delta is approximate process-level evidence, not allocator-precise bytes/op.
- the large raw Actions artifact has finite retention; the immutable run/id/digest, durable analysis, and reproducible retained-artifact verifier preserve traceability while the artifact exists.

## Handoff

Spec 0002 is **Verified**. M0/#8 may close and M1/#9 may begin under Spec 0003. The M1 comparison must reuse the accepted M0 dimensions and must not credit unrelated RPC/network changes as storage-engine improvements.
