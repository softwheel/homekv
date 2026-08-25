# Spec 0002 — Baseline Benchmark Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #8

## 1. Harness shape

Use a Rust benchmark/load-generation binary checked into the repository. It exercises two layers separately:

1. **storage baseline** — current `BTreeStore` + `Mvcc` path;
2. **server baseline** — current Tonic/Tokio request path.

Preferred layout:

```text
src/bin/hkvbench.rs
benchmarks/
  configs/
    smoke.json
    baseline.json
  results/
    .gitkeep
  README.md
```

The benchmark binary accepts a config file and emits newline-delimited or single-document JSON results suitable for archival and later analysis.

## 2. Benchmark modes

### Smoke

Small deterministic configuration run by normal CI. Purpose: prove the harness builds/runs and result schema remains valid. Smoke numbers are not performance claims.

### Full baseline

Runs the accepted workload matrix from `requirements.md`, repeated at least three times per configuration where practical. It is captured before M1 and retained as the immutable prototype comparison point.

## 3. Measurement lifecycle

For each configuration:

1. initialize/populate deterministic dataset from an explicit seed;
2. warm the measured path;
3. run the configured duration or operation count;
4. collect per-operation latency samples in nanoseconds;
5. sort samples and calculate p50/p95/p99;
6. record successful operation count, failures, elapsed time and throughput;
7. record environment metadata;
8. emit machine-readable result.

The baseline intentionally favors simple transparent measurement code over a complicated benchmarking framework.

## 4. Storage baseline

The storage path directly exercises `Mvcc<BTreeStore>` and the existing `Store` operations.

The key diagnostic is the SET/DELETE dataset-size sweep. Because the current first mutation clones the underlying store, a bounded operation-count mode is used for large datasets so the benchmark itself remains tractable while preserving comparable sample counts.

Storage concurrency is reported as 1 because writes are serialized by the current MVCC design. Concurrent request behavior belongs to the server benchmark.

## 5. Server baseline

The server/client benchmark exercises the existing Tonic/Tokio RPC path and sweeps client concurrency 1/8/32. This separates storage algorithm cost from RPC/runtime/queueing overhead.

If implementing the full network load generator inside M0 would materially change the server code, it MUST instead use the existing public client API unchanged; M0 may add benchmark-only client code but may not optimize the server.

## 6. Result schema

Each result contains at least:

```text
schema_version
mode                    # smoke | baseline
layer                   # storage | server
workload                # get | set | delete | read80_write20
seed
key_size
value_size
dataset_cardinality
concurrency
warmup_ops or warmup_ms
measured_ops
elapsed_ns
throughput_ops_sec
latency_ns.p50
latency_ns.p95
latency_ns.p99
failures
homekv_git_sha
rustc_version
os
kernel
cpu_model
logical_cpus
memory_bytes
notes
```

Unknown environment fields may be `null`/`unknown` when the host does not expose them, but the harness must not fabricate values.

## 7. Determinism

Keys and values are generated from `(seed, ordinal)` without external randomness during a measured run. Mixed workloads use a deterministic PRNG/sequence initialized by the configured seed.

Each mutation targets existing/pre-generated keys so payload generation does not dominate measured latency.

## 8. Percentiles

Percentiles are computed from the complete latency sample vector for the measured window. Index selection is documented and covered by unit tests.

The result records sample count so users can judge whether p99 is statistically meaningful.

## 9. Environment metadata

Capture where available:

- git SHA
- timestamp
- Rust version
- OS/kernel
- CPU model and logical CPU count
- total memory

Host metadata is evidence, not a claim that CI is controlled hardware.

## 10. Result retention

- smoke results may remain CI artifacts/log output;
- the accepted full M0 baseline is committed under `benchmarks/results/m0/` if reasonably small, otherwise attached as a GitHub Actions artifact with a checked-in summary and artifact/run reference;
- summary documents observations separately from M1 hypotheses.

## 11. CI strategy

Normal PR/push CI runs the smoke config after tests/build checks. The full baseline configuration is run on the dedicated M0 implementation branch/PR once the harness is stable; subsequent milestones do not overwrite its result bundle.

## 12. Prohibited M0 changes

M0 MUST NOT:

- replace `BTreeStore`;
- alter MVCC copy-on-write behavior;
- change server concurrency semantics for performance;
- add batching/caching to improve measurements;
- change consistency semantics.

Only benchmark/support code and non-semantic instrumentation are allowed.
