# Spec 0002 — Baseline Benchmark Verification

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #8

## Verification matrix

| Requirement | Proof |
| --- | --- |
| REQ-BENCH-001 | result bundle includes GET, SET, DELETE, 80/20 GET/SET |
| REQ-BENCH-002 | full results include 1k/10k/50k datasets and server concurrency 1/8/32 |
| REQ-BENCH-003 | every result includes throughput + p50/p95/p99 + sample count |
| REQ-BENCH-004 | result metadata contains commit/toolchain/OS/kernel/CPU/RAM fields |
| REQ-BENCH-005 | memory metrics included where practical; limitations documented |
| REQ-BENCH-006 | archived result identifies an exact pre-M1 prototype commit |
| REQ-BENCH-007 | storage write results are tabulated across dataset cardinality |
| REQ-BENCH-008 | clean checkout can run documented smoke/full commands |
| REQ-BENCH-009 | result/summary explicitly labels single-node prototype semantics |
| REQ-BENCH-010 | same seed/config produces identical generated keys/workload decisions |
| REQ-BENCH-011 | normal CI runs bounded smoke mode, not the full authoritative matrix |
| REQ-BENCH-012 | full result bundle retains repeated runs/variance where practical |

## Harness tests

Before baseline capture, verify:

- percentile helper against known ordered samples;
- deterministic generator golden vectors;
- invalid config rejection;
- zero/empty sample handling;
- result JSON round-trip/parseability;
- smoke config exits successfully on a clean checkout.

## Stability checks

- warm each measured path;
- retain all repeated runs, not only the best;
- flag cells with too few samples for useful p99 interpretation;
- record obvious CPU saturation/queueing conditions;
- do not compare results across unrecorded configuration changes.

## M0 acceptance gate

Spec 0002 becomes `Verified` when:

1. benchmark harness/configs are merged;
2. smoke mode passes CI;
3. the prototype baseline is captured against an exact pre-M1 commit;
4. all mandatory primary workload cells have valid p50/p95/p99 and throughput results;
5. payload-sensitivity cells are captured or a documented host-resource limitation is accepted by a spec amendment;
6. environment metadata is complete to the extent exposed by the host;
7. dataset-size write-cost trend is documented;
8. results are explicitly labeled single-node prototype/non-distributed;
9. no M1 storage optimization is present in the baseline commit.

The verified baseline becomes the comparison point for Spec 0003 (shard-owned memory engine).
