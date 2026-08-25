# Spec 0002 — Baseline Benchmark Verification

- Status: Draft
- Requirements: `requirements.md`
- Tracking issue: #8

## Verification matrix

| Requirement | Proof |
| --- | --- |
| REQ-BENCH-001 | benchmark configs/results include GET, SET, DELETE, 80/20 mix |
| REQ-BENCH-002 | results include >=3 dataset sizes and multiple concurrency levels |
| REQ-BENCH-003 | result schema contains throughput + p50/p95/p99 |
| REQ-BENCH-004 | each run contains required environment/workload metadata |
| REQ-BENCH-005 | allocation and memory/key metrics included, or limitation documented |
| REQ-BENCH-006 | archived result identifies a pre-M1 prototype commit |
| REQ-BENCH-007 | write results plotted/tabulated across dataset cardinality |
| REQ-BENCH-008 | clean checkout can run documented benchmark command/config |
| REQ-BENCH-009 | summary labels results as single-node prototype baseline |

## Stability checks

- run each key configuration multiple times;
- record variance rather than selecting only the best run;
- include warm-up;
- detect obvious CPU saturation or queueing artifacts;
- keep machine background load reasonably controlled for authoritative results.

## Acceptance gate

Spec 0002 becomes Verified when:

1. the harness and configs are merged;
2. the prototype baseline is captured against an exact commit;
3. all mandatory workload cells have valid p50/p95/p99 and throughput results;
4. environment metadata is complete;
5. the dataset-size write-cost trend is documented;
6. no M1 storage-engine optimization was mixed into the baseline commit.

The verified baseline becomes the comparison point for Spec 0003 (shard-owned memory engine).
