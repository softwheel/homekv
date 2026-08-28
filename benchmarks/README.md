# HomeKV benchmarks

This directory implements Spec 0002 (M0 prototype baseline).

## M0 status

Spec 0002 is **Verified** on the BENCH-T7 verification branch pending merge.

Completed:

- BENCH-T1 — deterministic config/result schema and percentile helpers
- BENCH-T2 — storage-only `BTreeStore` + `Mvcc` baseline
- BENCH-T3 — existing Tonic/Tokio server/RPC baseline with client concurrency 1/8/32
- BENCH-T4 — low-intrusion process RSS / approximate bytes-per-key accounting
- BENCH-T5 — bounded CI smoke mode
- BENCH-T6 — immutable repeated full prototype capture
- BENCH-T7 — retained-artifact analysis and verification handoff

The durable analysis and caveats live in [`specs/0002-baseline-benchmark/m0-analysis.md`](../specs/0002-baseline-benchmark/m0-analysis.md). The exact retained artifact is revalidated by `.github/workflows/m0-verification.yml` and analyzed by `benchmarks/analyze_m0.py`.

No M1 storage optimization belongs in M0.

## Storage smoke

```bash
cargo run --bin hkvbench -- \
  --config benchmarks/configs/smoke-storage.json \
  --output target/hkvbench-smoke.json
```

Smoke mode exists to validate the harness in CI. It is not an authoritative performance result.

## Full storage baseline

Use a release build for recorded results:

```bash
cargo run --release --bin hkvbench -- \
  --config benchmarks/configs/baseline-storage.json \
  --output benchmarks/results/m0/storage-run-1.json
```

Repeat the full run at least three times on the same controlled host before drawing conclusions. Preserve every run rather than selecting only the fastest one.

## Full server/RPC baseline

BENCH-T3 deliberately uses the existing public Tonic/Tokio server path. Start the prototype server unchanged in one shell:

```bash
cargo run --release --bin homekv -- \
  --host 127.0.0.1 \
  --port 20001 \
  --public_host 127.0.0.1 \
  --gossip_port 20002
```

Then run the checked-in server matrix from another shell:

```bash
cargo run --release --bin hkvbench -- \
  --config benchmarks/configs/baseline-server.json \
  --output benchmarks/results/m0/server-run-1.json
```

The server matrix covers:

- 16-byte keys / 64-byte values at 1,000, 10,000, and 50,000 keys;
- client concurrency 1, 8, and 32 for each primary dataset size;
- GET, SET, DELETE, and deterministic 80% GET / 20% SET workloads;
- 16-byte keys / 256-byte values at 10,000 keys and concurrency 8;
- 32-byte keys / 1 KiB values at 10,000 keys and concurrency 8;
- a 5-second target measured window per server workload cell.

The harness preloads deterministic data through the public SET RPC before measurements. Preload is outside the measured window. Tonic clients are cloned per worker, each worker uses a deterministic seed derived from the global seed and worker id, and successful request latency samples are aggregated across workers.

RPC failures and Tokio task/runtime failures are reported separately. `measured_operations` counts successful measured RPCs, while `attempted_operations` includes measured RPC attempts that returned an RPC failure. A worker task failure is reported in `runtime_failures`.

The prototype server's existing per-request logging remains enabled. BENCH-T3 must preserve the server path rather than optimize it, so those costs are intentionally part of this historical baseline. The `process_rss_bytes` value in a server-layer result describes the `hkvbench` client process; server-process memory is measured separately by BENCH-T4.

Repeat the full server run at least three times on the same controlled host and retain every run.

## BENCH-T4 memory accounting

`hkvmem` is a companion measurement probe. It deliberately uses process RSS rather than adding allocator hooks to the production server. Its output is evidence for memory scaling, not an exact heap-allocation profile.

### Storage memory matrix

```bash
cargo run --release --bin hkvmem -- \
  --config benchmarks/configs/baseline-storage.json \
  --output benchmarks/results/m0/storage-memory.json
```

Each storage cell runs in a fresh `hkvmem` worker process. The worker samples its RSS before constructing the dataset and after the populated `Mvcc<BTreeStore>` is resident. This avoids carrying the allocator high-water mark from a larger preceding dataset into a smaller one.

The result includes:

- RSS before population;
- RSS after population;
- signed RSS delta;
- approximate RSS bytes/key when the observed delta is positive;
- logical key+value payload bytes;
- RSS-delta / logical-payload ratio.

Temporary deterministic key/value generation and allocator retention can affect the RSS delta. The probe preserves the observed value rather than fabricating an exact allocation number.

### Server-process memory matrix

Build the probe and server first so `hkvmem` can launch a fresh historical HomeKV server for every unique dataset:

```bash
cargo build --release --bin homekv --bin hkvmem

target/release/hkvmem \
  --config benchmarks/configs/baseline-server.json \
  --homekv-bin target/release/homekv \
  --output benchmarks/results/m0/server-memory.json
```

For every unique `(key_size, value_size, dataset_cardinality)` in the accepted server matrix, `hkvmem`:

1. starts a fresh unmodified HomeKV Tonic/Tokio process on temporary loopback ports;
2. waits for the public RPC endpoint to become ready;
3. samples server-process RSS;
4. preloads the deterministic dataset through the public SET RPC;
5. samples server-process RSS again;
6. terminates the isolated server before moving to the next dataset.

Concurrency variants are intentionally deduplicated for this probe because they share the same resident dataset. Server stdout/stderr are redirected only so the prototype's historical per-request `println!` output cannot fill the measurement driver's pipe; the server implementation itself is not modified.

On hosts without Linux `/proc/<pid>/status`, RSS fields are reported as `null` rather than guessed. Allocations/op remain deferred under REQ-BENCH-005 because adding a reliable low-intrusion allocator profiler is not necessary for the M0 acceptance gate.

## BENCH-T6 retained baseline

The immutable M0 comparison point is:

- baseline SHA: `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`
- workflow run: `33080513696`
- artifact id: `9650486609`
- artifact name: `homekv-m0-baseline-bc613b74e8c718a7d002f1cacbd8d51cddbf3067`
- digest: `sha256:015faf8d87c8d197457ca9c6fd2f2c8d1ab07dc0bc16a2e62a2517335331680c`

The capture contains three complete storage runs, three complete server runs, storage/server memory evidence, and the historical server logs. It must not be replaced by a later M1 run.

## BENCH-T7 analysis

To analyze the exact retained T6 result bundles rather than recapturing on a different runner:

```bash
python3 benchmarks/analyze_m0.py \
  --input <downloaded-artifact>/benchmarks/results/m0 \
  --output target/m0-analysis.md \
  --baseline-sha bc613b74e8c718a7d002f1cacbd8d51cddbf3067
```

The `M0 Verification` GitHub Actions workflow verifies the retained artifact id/name/digest/run identity, downloads that exact artifact, executes the analyzer, and publishes the derived report to the job summary.

## What is measured

The storage baseline intentionally exercises `Mvcc<BTreeStore>` exactly as the prototype uses it:

- GET includes acquisition of the current MVCC read snapshot and a B-tree lookup.
- SET updates an existing key and includes the first-write whole-store COW clone.
- DELETE targets a guaranteed-missing key. This keeps dataset cardinality constant while still triggering the current write transaction's COW clone.
- `read80_write20` deterministically mixes 80% GET and 20% SET.

Storage concurrency is reported as 1 because the prototype serializes writers. Concurrent client behavior is measured separately at the RPC layer.

For the server baseline, DELETE also targets a guaranteed-missing key so the dataset cardinality remains stable across concurrency/workload cells.

## Result semantics

Every result bundle is labeled:

```text
"authoritative_performance_result": false
```

M0 is a single-node prototype baseline, not a distributed or strong-consistency benchmark. Public comparative HomeKV claims require the later distributed correctness and benchmark specs.

The latency/throughput bundle includes the git SHA, Rust version, OS/kernel, CPU, logical CPU count, host memory where available, benchmark-process RSS where available, workload parameters, attempted/successful operation counts, throughput, p50/p95/p99 latency, and failure counters.

The BENCH-T4 bundle separately records isolated storage/server RSS deltas and approximate bytes/key with explicit measurement limitations.
