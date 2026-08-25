# Spec 0001 — HomeKV v1 Verification Plan

- Status: Accepted
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
| REQ-SHARD-001/004/005 | deterministic XXH3 mapping property/golden-vector tests over 1,024-shard space |
| REQ-SHARD-002 | membership-view divergence tests; ownership changes only via authoritative config |
| REQ-SHARD-003 | stale client routing integration tests with epoch/leader redirect |
| REQ-CONS-001 | linearizability checker over concurrent generated histories |
| REQ-CONS-002 | stale-leader/partition tests proving rejected writes after authority loss |
| REQ-CONS-003/004/006 | stale-state tests; safe ReadIndex barrier and apply-index verification |
| REQ-CONS-005 | minority partition cannot acknowledge strongly consistent writes |
| REQ-DUR-001/002/005 | leader loss after acknowledged write; quorum durable boundary verified |
| REQ-DUR-003/004 | crash/replay and corrupted WAL/snapshot tests |
| REQ-DUR-006 | relaxed mode, if added, is explicitly named and benchmark-labeled |
| REQ-FAIL-001 | gossip suspicion cannot directly mutate consensus authority |
| REQ-FAIL-002/003 | leader kill/election/stale request tests |
| REQ-FAIL-004 | membership-change tests under concurrent traffic |
| REQ-FAIL-005 | crash injection during WAL/snapshot operations |
| REQ-RETRY-001 | repeated PUT/DELETE/deterministic batches converge to one semantic result |
| REQ-RETRY-002/003 | API/docs do not promise general exactly-once semantics |
| REQ-PERF-001 | controlled healthy-path GET benchmark; target reported with workload/hardware |
| REQ-PERF-002/003 | core/shard scaling + lock/contention profiling |
| REQ-PERF-004 | mutation cost does not copy entire dataset; allocation/copy profiling |
| REQ-PERF-005/006 | benchmark artifact review against benchmarking contract |
| REQ-OPS-001/002 | metrics/topology inspection integration tests |
| REQ-OPS-003 | bounded-queue overload/backpressure test |
| REQ-LANG-001 | benchmark/ADR evidence before any Zig production hot-path adoption |
| REQ-BENCH-AUTH-001/002 | public result bundle contains frozen dedicated-host/network metadata |
| REQ-RAFT-001/002 | dependency/integration review confirms raft-rs core + HomeKV-owned WAL/transport/state machine |
| REQ-SDD-001..005 | PR/spec history proves milestone boundaries and verification gates were followed |

## C1 — Deterministic state-machine tests

Generate command sequences and compare the shard state machine against a simple reference model. Cover absent/present keys, overwrite/delete cycles, atomic batches, duplicate/retried idempotent commands, and snapshot/replay equivalence.

## C2 — Concurrent linearizability histories

Run clients issuing GET/PUT/DELETE/batches while recording invocation/completion times and results. Validate histories against a linearizable per-shard model under no faults, elections, process pauses, partitions, reconnects, and stale routing.

## C3 — Consensus authority tests

Verify that one authoritative leader exists per term/configuration under quorum assumptions, minority replicas cannot acknowledge writes, old leaders fail safely after losing authority, safe ReadIndex is respected, and reconfiguration preserves safety.

## C4 — Durability/recovery tests

Inject crashes before/during WAL persistence, after local persistence but before quorum, after quorum/commit but before response, during snapshot generation/install, and during truncation. After recovery, compare visible state to committed history.

A distributed success response is not considered verified durable unless a subsequent leader loss/restart test preserves it.

## C5 — Corruption tests

Mutate/truncate WAL and snapshot artifacts and verify detection/fail-safe behavior.

## Performance suites

### P0 — prototype baseline

Defined by Spec 0002 and captured before storage-engine replacement.

### P1 — shard-owned engine

Repeat P0 workloads after M1 and compare throughput/core, p50/p95/p99, allocation/memory cost, dataset-size scaling, and shard/worker scaling.

### P2 — one replicated shard

Three replicas: safe linearizable GET, durable PUT/DELETE, pipeline depth, WAL sync/group-commit behavior, follower lag, leader failover, and restart recovery.

### P3 — Multi-Raft cluster scaling

Measure nodes/cores/groups under uniform and skewed workloads and verify the 1,024-shard mapping/placement behavior.

### P4 — failure performance

Measure foreground latency/availability during leader loss, replica catch-up, snapshot transfer, rebalance, and one unavailable replica.

## Verification artifacts

Each verified child spec retains or links:

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
5. no unresolved spec deviation affects measured semantics;
6. public comparative claims use the dedicated benchmark profile frozen by the M9 spec.
