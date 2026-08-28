# Spec 0002 — M0 Baseline Analysis

This document is the durable BENCH-T7 summary derived from the immutable BENCH-T6 artifact.

## Provenance

- frozen pre-M1 HomeKV commit: `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`
- capture workflow run: [33080513696](https://github.com/softwheel/homekv/actions/runs/33080513696)
- retained artifact: `homekv-m0-baseline-bc613b74e8c718a7d002f1cacbd8d51cddbf3067`
- artifact id: `9650486609`
- artifact SHA-256: `015faf8d87c8d197457ca9c6fd2f2c8d1ab07dc0bc16a2e62a2517335331680c`
- capture contents: 3 storage repetitions, 3 server repetitions, storage memory evidence, server memory evidence, and three server logs
- aggregation below: median across the three retained repetitions; throughput spread is `(max - min) / median`
- semantics: **single-node prototype baseline only; not distributed/strong-consistency performance**

The artifact archive was independently downloaded during BENCH-T7 and its SHA-256 matched the recorded digest exactly before analysis.

## Result completeness

The retained performance data contains 56 unique measured cells per repetition pair:

- 12 storage cells: 4 workloads × 3 dataset sizes
- 36 primary server cells: 4 workloads × 3 dataset sizes × concurrency 1/8/32
- 8 additional server payload-sensitivity cells beyond the overlapping 16B/64B primary cell

Every performance cell is present in each of the three repetitions. Across the retained performance rows there are zero measured-operation failures, zero RPC failures, and zero runtime failures.

All performance repetitions record the same environment:

- Rust: `rustc 1.98.0 (88d9e12ae 2026-08-18)`
- OS: Linux
- kernel: `6.17.0-1022-azure`
- CPU: AMD EPYC 7763 64-Core Processor
- logical CPUs exposed to the runner: 4
- RAM: 16,770,744,320 bytes

These are GitHub-hosted-runner results and remain explicitly non-authoritative.

## Primary storage baseline — 16B keys / 64B values

| workload | keys | median ops/s | p50 µs | p95 µs | p99 µs | run spread |
| --- | --- | --- | --- | --- | --- | --- |
| GET | 1,000 | 3,749,531 | 0.2 | 0.3 | 0.4 | 3.3% |
| SET | 1,000 | 16,498 | 58.9 | 79.5 | 88.5 | 3.1% |
| DELETE | 1,000 | 16,542 | 59.0 | 70.4 | 84.8 | 14.9% |
| 80/20 | 1,000 | 66,775 | 0.3 | 59.3 | 77.8 | 18.6% |
| GET | 10,000 | 2,551,118 | 0.3 | 0.5 | 0.5 | 9.1% |
| SET | 10,000 | 1,428 | 685.5 | 757.7 | 794.5 | 3.0% |
| DELETE | 10,000 | 1,259 | 790.2 | 851.4 | 938.2 | 1.6% |
| 80/20 | 10,000 | 6,365 | 0.5 | 801.2 | 837.0 | 4.8% |
| GET | 50,000 | 1,300,585 | 0.7 | 1.0 | 1.3 | 11.0% |
| SET | 50,000 | 113 | 7,210.1 | 15,737.0 | 18,329.3 | 100.9% |
| DELETE | 50,000 | 132 | 6,606.7 | 13,379.9 | 14,409.9 | 87.6% |
| 80/20 | 50,000 | 398 | 2.5 | 14,553.9 | 15,873.4 | 131.5% |

### Dataset-size write-cost trend

The primary storage sweep strongly separates reads from mutations.

- SET throughput is `1.000x / 0.087x / 0.0069x` at 1k / 10k / 50k keys. Median SET latency grows `1.0x / 11.6x / 122.5x`.
- DELETE throughput is `1.000x / 0.076x / 0.0080x`. Median DELETE latency grows `1.0x / 13.4x / 111.9x`.
- The 80/20 workload falls from 66,775 ops/s at 1k keys to 398 ops/s at 50k. Its p50 remains read-dominated, while p95/p99 expose the mutation stalls: p99 rises from 77.8 µs to 15.9 ms.
- GET changes much more gradually: 3.75M ops/s at 1k keys to 1.30M ops/s at 50k, with p50 moving from roughly 0.2 µs to 0.7 µs.

The 50k mutation cells also show high GitHub-runner variance. This is retained evidence, not hidden by selecting the best run.

## Existing Tonic/Tokio server path

At the 10k primary dataset, GET throughput scales with client concurrency while mutation throughput scales only modestly and mutation latency grows sharply.

| workload | concurrency | median ops/s | p50 µs | p95 µs | p99 µs | run spread |
| --- | --- | --- | --- | --- | --- | --- |
| GET | 1 | 5,606 | 161.6 | 229.1 | 576.9 | 5.1% |
| SET | 1 | 733 | 1,311.3 | 1,464.4 | 2,407.7 | 7.6% |
| DELETE | 1 | 730 | 1,249.1 | 1,460.3 | 3,313.1 | 18.8% |
| 80/20 | 1 | 2,339 | 166.2 | 1,337.8 | 1,540.2 | 12.3% |
| GET | 8 | 15,783 | 469.4 | 842.1 | 1,317.7 | 2.6% |
| SET | 8 | 976 | 7,457.0 | 12,466.6 | 15,434.3 | 6.1% |
| DELETE | 8 | 1,052 | 7,041.7 | 10,882.1 | 15,328.3 | 13.5% |
| 80/20 | 8 | 4,113 | 210.5 | 8,744.6 | 9,906.0 | 9.4% |
| GET | 32 | 26,880 | 1,124.9 | 1,923.2 | 2,484.4 | 1.6% |
| SET | 32 | 1,100 | 27,611.4 | 36,006.7 | 45,233.7 | 5.1% |
| DELETE | 32 | 1,157 | 26,935.6 | 30,532.7 | 48,161.7 | 9.4% |
| 80/20 | 32 | 4,225 | 215.6 | 38,023.5 | 43,745.5 | 20.4% |

The same pattern becomes more pronounced at 50k keys. GET reaches about 26.1k ops/s at concurrency 32, while SET remains about 173 ops/s and its p50 reaches roughly 175 ms. The 50k 80/20 workload at concurrency 32 records p99 around 268 ms.

This is an observation about the current prototype path, not a claim that CPU saturation or a particular lock is the cause. CPU utilization and lock profiling were not captured in M0.

## Payload sensitivity — 10k keys / concurrency 8

| workload | key/value | median ops/s | p50 µs | p99 µs | throughput vs 16B/64B |
| --- | --- | --- | --- | --- | --- |
| GET | 16B/64B | 15,783 | 469.4 | 1,317.7 | 1.000x |
| GET | 16B/256B | 13,706 | 532.5 | 1,752.3 | 0.868x |
| GET | 32B/1024B | 13,892 | 533.4 | 1,533.8 | 0.880x |
| SET | 16B/64B | 976 | 7,457.0 | 15,434.3 | 1.000x |
| SET | 16B/256B | 155 | 46,817.1 | 83,828.4 | 0.159x |
| SET | 32B/1024B | 84 | 90,155.2 | 138,070.9 | 0.087x |
| DELETE | 16B/64B | 1,052 | 7,041.7 | 15,328.3 | 1.000x |
| DELETE | 16B/256B | 153 | 50,459.2 | 81,865.2 | 0.145x |
| DELETE | 32B/1024B | 93 | 83,650.3 | 132,950.1 | 0.088x |
| 80/20 | 16B/64B | 4,113 | 210.5 | 9,906.0 | 1.000x |
| 80/20 | 16B/256B | 597 | 210.8 | 88,335.3 | 0.145x |
| 80/20 | 32B/1024B | 397 | 239.4 | 114,330.8 | 0.096x |

GET is comparatively insensitive to the larger stored values in this matrix, while mutations become dramatically more expensive as the populated dataset contains more bytes. DELETE targets a guaranteed-missing key, so its payload sensitivity is especially useful evidence that mutation cost is associated with the existing populated-store representation rather than the returned value size of the DELETE operation itself.

## Memory evidence

The memory measurements are process-RSS deltas, not exact heap-allocation accounting.

| layer | key/value | keys | approx RSS B/key | RSS/logical payload |
| --- | --- | --- | --- | --- |
| storage | 16B/64B | 1,000 | 180.2 | 2.25x |
| storage | 16B/64B | 10,000 | 188.4 | 2.36x |
| storage | 16B/64B | 50,000 | 188.3 | 2.35x |
| server | 16B/64B | 1,000 | 405.5 | 5.07x |
| server | 16B/64B | 10,000 | 370.3 | 4.63x |
| server | 16B/64B | 50,000 | 377.3 | 4.72x |
| server | 16B/256B | 10,000 | 724.6 | 2.66x |
| server | 32B/1024B | 10,000 | 2,143.4 | 2.03x |

For the primary storage profile, the approximate isolated RSS delta stabilizes near 188 bytes/key at 10k and 50k keys. The fresh server-process measurement is roughly 370–405 bytes/key for the same logical payload. These figures include allocator/runtime effects and are only a baseline for relative M1 comparison.

## p99 sample sufficiency and stability

M0 records a p99 for every mandatory cell, but not every p99 has equal statistical support.

- Every storage cell intentionally uses a bounded 200-operation measured window because the current COW path becomes pathological at larger datasets. Roughly two observations therefore occupy the top 1%; storage p99 values are **low-support tail estimates** and must not be over-interpreted.
- The slowest 32B-key/1KiB-value server SET cell has a minimum of 386 successful samples in a repetition, giving roughly four observations in the top 1%; its p99 is also flagged low-support.
- Other server cells have at least five approximate top-1% observations in the smallest repetition.
- The largest run-to-run spreads occur in the 50k storage mutation cells: roughly 100.9% for SET, 87.6% for DELETE, and 131.5% for 80/20.

These limitations do not invalidate the mandatory result cells: each contains a measured throughput, p50, p95, p99, and sample count. They do prevent treating the GitHub-hosted M0 tail values as release-quality performance claims.

## Observations vs. hypotheses

### Measured observations

1. Storage mutation cost increases by roughly two orders of magnitude in median latency between the 1k and 50k primary datasets, while GET degrades far less.
2. Server GET throughput benefits substantially from additional client concurrency; mutation throughput does not scale comparably and mutation tail latency grows with concurrency.
3. Larger stored values have modest effect on GET throughput but very large effect on SET/DELETE and mixed-workload mutation tails.
4. The current prototype has substantial run-to-run variance in the largest COW-heavy storage cells.
5. The retained result set has zero measured/RPC/runtime failures.

### Hypotheses to test in M1, not claims proved by M0

1. The current `Mvcc<BTreeStore>` whole-store copy-on-write behavior is the dominant source of dataset-size and payload-size mutation scaling.
2. The serialized writer path compounds server mutation latency under concurrency through queueing.
3. Historical per-request server logging and Tonic/Tokio overhead contribute to server-path latency, but M0 does not isolate their costs.

The first hypothesis is strongly motivated by the code structure and the shape of the storage/payload results, but causal attribution still belongs to M1 comparison/profiling rather than M0 alone.

## M1 comparison contract

Spec 0003 should repeat the same primary storage/server matrix after the shard-owned storage engine is implemented. The highest-value success signal is not an absolute GitHub-runner number; it is the removal of the dataset-size-dependent mutation cliff while preserving semantics.

At minimum, compare:

- SET/DELETE p50/p95/p99 and throughput at 1k/10k/50k keys
- 80/20 p95/p99, where write stalls are visible despite read-dominated p50
- concurrency 1/8/32 server behavior
- 16B/64B vs 16B/256B vs 32B/1KiB payload sensitivity
- approximate RSS bytes/key
- run-to-run spread

No RPC/network optimization should be credited as a storage-engine win unless the comparison isolates that change.
