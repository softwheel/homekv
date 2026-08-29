# Spec 0003 — Shard-Owned In-Memory Engine Verification

- Status: Verified
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #9
- Verification implementation merge: `d10cebfc4d19731d6ad747fd996c289450514543`
- Benchmark implementation head: `8b5bde7c104c264a040ed51f424894ffbeedd841`

## Verification conclusion

Candidate A satisfies every mandatory Spec 0003 requirement. M1 removes the verified M0 whole-dataset mutation cliff while preserving deterministic single-shard semantics, bounded foreground admission, explicit cancellation/lifecycle behavior, exact logical memory accounting, and the existing public GET/SET/DELETE contract.

Candidate B / Crossbeam was not attempted. M1-T8 is explicitly optional when Candidate A satisfies all mandatory M1 gates, so Candidate A remains the primary M1 architecture. This is a verification decision, not a claim that future profiling cannot justify a separate read-optimization experiment.

M0 remains immutable at pre-M1 SHA `bc613b74e8c718a7d002f1cacbd8d51cddbf3067`.

## Requirement matrix

| Requirement | Verification evidence | Result |
| --- | --- | --- |
| REQ-M1-SHARD-001/002 | `shard_id_bounds_are_fixed_to_1024`, `xxh3_mapping_golden_vectors_are_stable`, `shard_mapping_is_deterministic_and_bounded` in `src/storage/shard_engine.rs` | PASS |
| REQ-M1-SHARD-003/004 | `ShardEngine::spawn` creates one bounded Tokio MPSC + one owner task per shard; `ShardStore::spawn` creates 1,024 independent engines; foreground state is owner-local `HashMap` | PASS |
| REQ-M1-DATA-001/002/003 | `owner_engine_put_get_delete_round_trip` plus public adapter integration tests | PASS |
| REQ-M1-DATA-004/005 | `batch_constructor_rejects_empty_and_cross_shard_commands`; `concurrent_snapshots_never_see_partial_batch` | PASS |
| REQ-M1-DATA-006 | deterministic owner-ordered mutation representation and repeated-key batch/accounting tests | PASS |
| REQ-M1-PERF-001 | source review shows in-place owner-local mutation with no whole-map clone; repeated scaling benchmark below | PASS |
| REQ-M1-PERF-002/003 | retained three-run M1 benchmark and frozen M0 comparison, workflow `33261555887` | PASS |
| REQ-M1-PERF-004 | 50k local GET median p99 ~50.8 µs, below 1 ms target | PASS |
| REQ-M1-PERF-005 | checked-in `baseline-shard.json`, `smoke-shard.json`, result schema/harness, and analyzer | PASS |
| REQ-M1-QUEUE-001/002 | bounded `mpsc::channel(queue_capacity)`; `try_admission_reports_queue_full_and_counts_rejection` | PASS |
| REQ-M1-QUEUE-003 | `cancellation_before_admission_does_not_apply_mutation`; `cancellation_after_admission_still_applies_exactly_once` | PASS |
| REQ-M1-QUEUE-004 | queue capacity/depth and overload counters exposed via `ShardMetrics` and saturation tests | PASS |
| REQ-M1-MEM-001/002 | `logical_memory_accounting_tracks_replacements_deletes_and_batches`; aggregate store accounting test | PASS |
| REQ-M1-MEM-003 | T7 records process RSS plus exact logical key/value bytes for every benchmark cell | PASS |
| REQ-M1-LIFE-001/002 | explicit `shutdown` closes admission, enqueues shutdown boundary, drains accepted work, joins owner; lifecycle tests in `shard_engine.rs` | PASS |
| REQ-M1-LIFE-003 | source/repository review finds no M1 consensus, WAL/snapshot durability, online migration, placement, or rebalance implementation | PASS |

## V1 — Shard mapping compatibility

Verified on the merged Candidate A implementation:

- `ShardId::new(0)` and `ShardId::new(1023)` succeed;
- `ShardId::new(1024)` returns `InvalidShard`;
- raw-byte XXH3 golden vectors are checked in (`b"" -> 194`, `b"abc" -> 336`);
- 10,000 generated keys are checked for deterministic mapping and `< 1024` bounds;
- mapping is a pure function of raw key bytes: `XXH3_64(raw_key) & 1023`.

Result: **PASS**.

## V2 — Candidate A state-machine semantics

`OwnerState` owns a shard-local `HashMap<Vec<u8>, Vec<u8>>` and applies GET/PUT/DELETE/batches only in the single owner loop. Tests cover absent/present GET, insert, overwrite, delete present/absent, repeated mutations, and repeated-key batches while checking visible values and accounting.

Result: **PASS**.

## V3 — Atomic batch visibility

`ShardBatch::new` rejects cross-shard commands before admission. Accepted batches are applied synchronously in one owner-loop request with no await/interleaving point. `concurrent_snapshots_never_see_partial_batch` repeatedly alternates correlated multi-key states while an owner-side snapshot observer verifies every observation is internally uniform.

Result: **PASS**.

## V4 — Queue, cancellation, and lifecycle

Candidate A uses an explicitly bounded Tokio MPSC for each shard. Verification tests prove:

- a full queue returns `QueueFull` on `try_*` and increments overload rejection accounting;
- cancellation before successful admission cannot mutate state;
- cancellation after successful admission does not revoke accepted work and the mutation is applied exactly once in owner order;
- shutdown takes an exclusive admission boundary, rejects later admission, drains accepted commands up to the shutdown marker, and joins the owner task before returning.

Result: **PASS**.

## V5 — Memory accounting

Owner-local accounting is updated in-place on insert/replace/delete and through batches. Tests cover replacement growth, delete-present/delete-absent, multi-key batches, repeated keys within a batch, key count, logical bytes, and mutation counters. The T7 harness separately records process RSS and logical bytes for benchmark evidence.

Result: **PASS**.

## V6 — No whole-dataset mutation copy

Source review confirms Candidate A mutates its owner-local `HashMap` in place. No `HashMap::clone` or whole-dataset copy occurs on PUT/DELETE.

Frozen three-run M1 gate results for 16B keys / 64B values:

- 1k SET median p50: ~31.1 µs
- 10k SET median p50: ~30.6 µs
- 50k SET median p50: ~31.2 µs
- 50k/1k SET p50 ratio: **1.002** (required <= 4.0)
- 50k SET p50 improvement vs verified M0 ~7.2 ms: **230.92x** (required >= 10x)
- 50k SET throughput: ~30.1k ops/s
- 50k SET throughput improvement vs verified M0 ~113 ops/s: **266.36x** (required >= 10x)

Result: **PASS**.

## V7 — Repeated M1 benchmark

Exact retained evidence from PR #25:

- benchmark implementation head: `8b5bde7c104c264a040ed51f424894ffbeedd841`
- merge to main: `d10cebfc4d19731d6ad747fd996c289450514543`
- normal Rust workflow: `33261555875` — success
- M1 Shard Benchmark workflow: `33261555887` — success
- artifact id: `9717413412`
- artifact name: `homekv-m1-shard-6ca8d1bf76cdf79fcdfe286aaa9474e078317ff0`
- artifact digest: `sha256:dc735edb4e5e83e37538f825eaddb1f8669cdf58a60f5e9b418d11c8d711c096`
- retained evidence: smoke result, three complete Candidate A repetitions, host/toolchain metadata, RSS/logical-memory evidence, and frozen gate analysis

The matrix is 16B key / 64B value, 1k/10k/50k cardinality, GET/SET/DELETE/80-20, with 10,000 measured operations per baseline cell and deterministic seed 42.

Median 50k GET p99 is ~50.8 µs (~0.051 ms).

Result: **PASS**.

These are engineering comparison measurements from GitHub-hosted CI and are not public release performance claims.

## V8 — Existing API integration

`ShardStore` is the narrow adapter between the existing service path and the 1,024 Candidate A engines. `existing_api_semantics_route_through_shards` verifies set/get/delete behavior, `same_shard_set_request_uses_atomic_batch_accounting` verifies same-shard grouped mutation behavior, and aggregate metrics remain compatible with the existing server counters. PR #24 changed no external gRPC wire contract and introduced no M2 protocol.

Result: **PASS**.

## V9 — Candidate decision

**Candidate A retained; Candidate B not attempted.** The optional Crossbeam promotion task is unnecessary for M1 because Candidate A passes all mandatory correctness, latency, throughput, scaling, memory-accounting, admission, lifecycle, and integration gates. Crossbeam remains eligible for a later profiling-driven optimization spec.

Result: **PASS / not required for M1**.

## M1 acceptance checklist

- [x] mapping compatibility verified
- [x] Candidate A GET/PUT/DELETE semantics verified
- [x] single-shard atomic batch visibility verified
- [x] cross-shard batches rejected
- [x] bounded queue/backpressure verified
- [x] cancellation/shutdown semantics verified
- [x] logical memory accounting verified
- [x] no whole-dataset mutation copy verified
- [x] existing API migrated/integrated without M2/M3 scope creep
- [x] 3-run M1 benchmark captured against exact commit
- [x] M1 performance gates pass
- [x] Candidate A/B decision recorded
- [x] no consensus/WAL/new-protocol code mixed into M1

## Verification decision

**Spec 0003 is Verified.** M1-T1 through M1-T7 are complete; M1-T8 is explicitly skipped under its accepted optional rule; M1-T9 is this verification handoff. After this verification change merges, issue #9 may close and M2 may begin under its own Accepted child spec. No M2 semantic implementation is authorized by this verification alone.
