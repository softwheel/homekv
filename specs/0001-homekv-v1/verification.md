# Spec 0001 — HomeKV v1 Verification Plan

- Status: Draft
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #7

Spec 0001 becomes `Verified` only after its child specs are verified and the end-to-end requirement matrix below passes.

## Requirement-to-verification matrix

| Requirement | Verification |
| --- | --- |
| REQ-DATA-001/002/003 | deterministic API unit/integration tests across restarts and leader changes |
| REQ-DATA-004 | concurrent single-shard batch histories; no partial batch visibility |
| REQ-DATA-005 | API rejects/does not advertise cross-shard atomicity |
| REQ-SHARD-001 | deterministic mapping property tests |
| REQ-SHARD-002 | membership-view divergence tests; ownership changes only via authoritative config |
| REQ-SHARD-003 | stale client routing integration tests with epoch/leader redirect |
| REQ-CONS-001 | linearizability checker over concurrent generated histories |
| REQ-CONS-002 | stale-leader/partition tests proving rejected writes after authority loss |
| REQ-CONS-003/004 | follower/stale-state tests; read-barrier/lease safety tests |
| REQ-CONS-005 | minority partition cannot acknowledge strongly consistent writes |
| REQ-DUR-001/002 | leader loss after acknowledged write; documented durability boundary verified |
| REQ-DUR-003/004 | crash/replay and corrupted WAL/snapshot tests |
| REQ-FAIL-001 | gossip suspicion cannot directly mutate consensus authority |
| REQ-FAIL-002/003 | leader kill/election/stale request tests |
| REQ-FAIL-004 | membership-change tests under concurrent traffic |
| REQ-FAIL-005 | crash injection during WAL/snapshot operations |
| REQ-PERF-001 | controlled healthy-path GET benchmark; target reported with workload/hardware |
| REQ-PERF-002/003 | core/shard scaling + lock/contention profiling |
| REQ-PERF-004 | mutation cost does not scale by copying entire dataset; allocation/copy profiling |
| REQ-PERF-005/006 | benchmark artifact review against benchmarking contract |
| REQ-OPS-001/002 | metrics/topology inspection integration tests |
| REQ-OPS-003 | bounded-queue overload/backpressure test |
| REQ-LANG-001 | ADR/benchmark evidence required before any Zig production hot-path adoption |

## Correctness suites

### C1 — deterministic state-machine tests

Generate command sequences and compare the shard state machine against a simple reference model.

Cover:

- absent/present keys
- overwrite/delete cycles
- atomic batches
- duplicate/retried idempotent PUT/DELETE
- snapshot + replay equivalence

### C2 — concurrent linearizability histories

Run multiple clients issuing GET/PUT/DELETE/batches while recording invocation/completion times and results.

Validate histories against a linearizable per-shard model.

Run under:

- no faults
- elections
- process pauses
- partitions
- reconnects
- stale routing

### C3 — consensus authority tests

Verify that:

- one authoritative leader exists per term/configuration under quorum assumptions;
- minority replicas cannot acknowledge writes;
- old leaders reject/fail after losing authority;
- reconfiguration preserves safety.

### C4 — durability/recovery tests

Inject crashes:

- before WAL append
- during record append
- after local append but before quorum
- after quorum/commit but before response
- during snapshot creation
- during snapshot installation
- during log truncation

After recovery, compare visible state to the committed-history model.

### C5 — corruption tests

Mutate/truncate WAL and snapshot artifacts and verify detection/fail-safe recovery behavior.

## Performance suites

### P0 — prototype baseline

Defined by child Spec 0002 and captured before storage-engine replacement.

### P1 — shard-owned engine

Repeat P0 workloads after M1 and compare:

- throughput/core
- p50/p95/p99
- allocations/op
- memory/key
- scaling with dataset size
- scaling with shard/worker count

### P2 — replicated shard

Three replicas:

- linearizable GET
- durable PUT/DELETE
- various pipeline depths
- batching/group commit sweep
- follower lag

### P3 — cluster scaling

Measure nodes/cores/shards under uniform and skewed workloads.

### P4 — failure performance

Measure foreground latency/availability during:

- leader loss
- replica catch-up
- snapshot transfer
- rebalance
- one unavailable replica

## Verification artifacts

Each verified child spec should retain or link:

- exact commit SHA
- test commands/configuration
- benchmark configuration
- raw/summary results
- failure matrix results
- known limitations

## Release gate for strong performance claims

A result may be described as a HomeKV strong-consistency performance result only if:

1. the tested code passes the relevant correctness suite;
2. consistency/durability/replication settings are stated;
3. workload/hardware/toolchain are stated;
4. tail latency and throughput are reported together;
5. no unresolved spec deviation affects the measured semantics.
