# Spec 0002 — Baseline Benchmark Design

- Status: Draft
- Requirements: `requirements.md`
- Tracking issue: #8

## Harness

Use a Rust benchmark/load-generation harness checked into the repository. It should exercise both storage-engine-level operations and, where useful, the existing server path so local engine cost can be separated from RPC overhead.

Preferred layout:

```text
benches/
  storage_baseline.rs
  server_baseline.rs
benchmarks/
  configs/
  results/
```

Exact paths may change during review, but commands/configuration and result metadata must remain version controlled.

## Measurement approach

For each workload/configuration:

1. create/populate the target dataset;
2. warm the workload;
3. run a fixed-duration or statistically sufficient sample window;
4. collect latency histogram and operation count;
5. record environment metadata;
6. repeat enough times to identify unstable runs.

Use a histogram implementation appropriate for p50/p95/p99 rather than relying only on average timings.

## Storage baseline

The storage baseline directly exercises the current `BTreeStore` + MVCC transaction path.

The dataset-size sweep is central: SET/DELETE workloads should demonstrate how first-mutation COW behaves as the underlying tree grows.

## Server baseline

The server baseline measures the existing Tonic/Tokio request path separately so RPC/runtime overhead is visible rather than conflated with storage cost.

## Reproducibility metadata

Every result set should include a machine-readable metadata record with:

- git SHA
- benchmark config name
- timestamp
- Rust version
- OS/kernel
- CPU model/core count
- memory
- operation mix
- key/value sizes
- dataset cardinality
- concurrency

## Results policy

Raw or minimally processed outputs should be retained under a results/artifact convention or attached to GitHub Actions/release artifacts if repository size becomes a concern.

The baseline should not be tuned after observing results; optimizations belong to subsequent specs so the before/after comparison remains meaningful.
