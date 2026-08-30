# Spec 0005 — One-Shard Three-Node OpenRaft Requirements

- Status: Accepted
- Parent: `specs/0001-homekv-v1/requirements.md`
- Tracking issue: #38

## 1. Purpose

M3 proves the first distributed HomeKV shard: one logical shard replicated across exactly three voting nodes using OpenRaft, with leader-authoritative writes, quorum-backed linearizable reads, explicit durable acknowledgement, failover, restart/recovery, and snapshot transfer sufficient to establish the correctness boundary before M4 scales the design to many Raft groups.

M3 MUST preserve the Verified M0/M1/M2 contracts. Performance work MUST NOT weaken consistency, durability, backpressure, routing, or admitted-work semantics.

## 2. Scope

M3 covers:

- exactly one HomeKV logical shard replicated by three voting replicas;
- OpenRaft 0.9.25 pinned exactly for the first implementation;
- HomeKV-owned Raft command/response types, log storage, state machine, network adapter, snapshot encoding/install path, and database-oriented observability;
- deterministic three-node bootstrap for tests and local deployments;
- strongly consistent SET/PUT, DELETE, deterministic single-shard BATCH, and GET;
- quorum-durable mutation acknowledgement plus leader-local application;
- safe linearizable reads through OpenRaft's quorum-backed read barrier;
- stale/non-leader rejection or redirect without executing a write outside Raft authority;
- leader failover, minority isolation, process restart, log replay, and snapshot installation/recovery;
- bounded foreground admission and bounded Raft transport queues.

M3 excludes:

- 1,024-shard placement and Multi-Raft scheduling (M4);
- online shard movement/rebalancing (M4);
- lease-based read optimization;
- relaxed/memory-only distributed durability;
- cross-shard transactions;
- public release benchmark claims;
- advanced WAL/group-commit tuning beyond what is required to prove the durable boundary;
- production TLS/authentication policy.

## 3. Consensus dependency and compatibility

**REQ-M3-RAFT-001** — M3 MUST use OpenRaft `=0.9.25`; Cargo resolution MUST NOT float to another OpenRaft minor/patch during M3 verification. Any version change requires an accepted Spec 0005 amendment because OpenRaft documents its pre-1.0 API as unstable.

**REQ-M3-RAFT-002** — HomeKV MUST own its `RaftTypeConfig`, application command/response types, `RaftLogStorage`, `RaftStateMachine`, snapshot format, Raft network adapter/factory, connection management, durability boundary, observability mapping, and request routing integration. OpenRaft is the consensus engine, not HomeKV's database contract.

**REQ-M3-RAFT-003** — M3 MUST NOT introduce a second consensus implementation or bypass OpenRaft ordering/leadership for foreground strongly consistent operations.

## 4. Replicated state machine

**REQ-M3-SM-001** — A client mutation accepted for the replicated shard MUST be represented as a deterministic Raft application command and applied only from committed Raft log entries.

**REQ-M3-SM-002** — The state machine MUST preserve M1 SET, DELETE, and deterministic single-shard BATCH semantics, including batch atomicity and deterministic delete-of-absent behavior.

**REQ-M3-SM-003** — Application order MUST equal committed Raft log order. A committed application command MUST be applied at most once for a given log identity during one coherent state-machine history; restart/snapshot replay MUST reconstruct the same logical state.

**REQ-M3-SM-004** — Membership entries MUST update the persisted/applied membership metadata required by OpenRaft and MUST NOT be interpreted as HomeKV data mutations.

**REQ-M3-SM-005** — The state-machine adapter MUST expose the last applied log identity and current membership consistently with the state it serves.

## 5. Write consistency and authority

**REQ-M3-WRITE-001** — SET/PUT, DELETE, and BATCH MUST enter the replicated state machine through OpenRaft's leader-authoritative client-write path or the version-equivalent API.

**REQ-M3-WRITE-002** — A non-leader or node that cannot establish current write authority MUST NOT apply or acknowledge the mutation locally. It MUST return a safe not-leader/stale-route/unavailable result, optionally with an advisory leader hint.

**REQ-M3-WRITE-003** — A minority partition MUST NOT acknowledge successful strongly consistent writes.

**REQ-M3-WRITE-004** — A successful mutation response MUST be emitted only after the command is committed by a quorum, every replica counted toward the durable quorum has completed the accepted persistent-log boundary for that entry, and the current leader has applied the committed entry locally.

**REQ-M3-WRITE-005** — Client transport cancellation after successful Raft admission MUST NOT revoke a command that later commits; cancellation before admission MUST NOT cause application. Existing M1/M2 retry/idempotence semantics remain unchanged.

## 6. Durable Raft storage

**REQ-M3-DUR-001** — `save_vote()` MUST not return success until the vote is durable on disk, matching OpenRaft's storage correctness contract.

**REQ-M3-DUR-002** — `append()` MUST make appended entries readable before returning and MUST signal OpenRaft's log-flushed callback only after the corresponding log bytes and required persistent metadata are durably flushed with `fdatasync`, `fsync`, or a verified equivalent.

**REQ-M3-DUR-003** — Log append/truncate/purge operations MUST preserve a hole-free Raft log and MUST reject or surface storage corruption/I/O failure rather than silently continuing.

**REQ-M3-DUR-004** — Raft log records and snapshot payloads MUST include integrity protection sufficient to detect corrupted/truncated persisted content before it is applied.

**REQ-M3-DUR-005** — The durable log format MUST be versioned. Unknown incompatible format versions MUST fail closed during recovery.

**REQ-M3-DUR-006** — Recovery MUST never expose an uncommitted/speculative log entry as applied HomeKV state.

**REQ-M3-DUR-007** — If the in-memory state machine is not itself persisted on every apply, M3 MUST retain enough durable committed/snapshot/log metadata to reconstruct the state justified by committed Raft history after restart.

## 7. Linearizable reads

**REQ-M3-READ-001** — Default GET MUST be served by the authoritative leader only after a quorum-backed OpenRaft linearizability barrier (`ensure_linearizable()`, `get_read_log_id()` plus applied wait, or the version-equivalent safe API) succeeds.

**REQ-M3-READ-002** — A GET MUST observe a state-machine applied position at least as new as the read barrier returned/established by OpenRaft before reading the key.

**REQ-M3-READ-003** — Followers MUST NOT serve default GET from local state as if it were strongly consistent.

**REQ-M3-READ-004** — Lease-only leader-local reads are forbidden in M3 even if OpenRaft exposes lease-related machinery.

## 8. Three-node membership and bootstrap

**REQ-M3-MEM-001** — M3's verified topology MUST contain exactly three voting nodes with stable node IDs and explicit Raft endpoints.

**REQ-M3-MEM-002** — Cluster initialization MUST be deterministic and idempotent for the same bootstrap configuration; concurrent/repeated bootstrap attempts MUST NOT create split authoritative clusters.

**REQ-M3-MEM-003** — Consensus membership is authoritative. Existing gossip/failure detection MAY provide health observations but MUST NOT grant leadership, mutate Raft membership, or authorize writes.

**REQ-M3-MEM-004** — General online add/remove/rebalance flows are deferred to M4; M3 MAY exercise membership APIs only as required for safe initial formation and snapshot/follower catch-up tests.

## 9. Network and backpressure

**REQ-M3-NET-001** — HomeKV's Raft network adapter MUST implement the OpenRaft RPC surface required by the pinned version, including vote, append-entries, and snapshot transfer paths.

**REQ-M3-NET-002** — Raft RPC connections and request queues MUST be bounded/configurable. Network failure or a slow replica MUST NOT cause unbounded memory growth.

**REQ-M3-NET-003** — Transport failure MUST surface to OpenRaft as transport/RPC failure; it MUST NOT be converted into false success or local authority.

**REQ-M3-NET-004** — Test transport MUST support deterministic partition/drop/delay controls sufficient for verification without altering consensus semantics.

## 10. Snapshot and recovery

**REQ-M3-SNAP-001** — Snapshot metadata MUST identify at least format version, last included/applied log identity, membership state, shard identity, payload length, and integrity checksum/digest.

**REQ-M3-SNAP-002** — A snapshot MUST represent a coherent state-machine point. Snapshot creation MUST NOT expose a mixture of application positions.

**REQ-M3-SNAP-003** — Snapshot installation MUST validate metadata/integrity before making the new state visible, then atomically replace the receiving state-machine image from HomeKV's perspective.

**REQ-M3-SNAP-004** — A restarted or lagging replica MUST be able to recover from a valid snapshot plus subsequent durable Raft log state and rejoin without serving speculative state.

**REQ-M3-SNAP-005** — Truncated/corrupted/incompatible snapshots MUST fail closed and MUST NOT be partially installed as authoritative state.

## 11. Failure semantics

**REQ-M3-FAIL-001** — Loss/isolation of the current leader MUST eventually allow the remaining healthy quorum to elect a new leader and resume strongly consistent operations.

**REQ-M3-FAIL-002** — The isolated old leader MUST not acknowledge successful writes while unable to contact a quorum, and after reconnection its conflicting uncommitted suffix MUST not become applied application state.

**REQ-M3-FAIL-003** — Loss of quorum MUST fail or block strongly consistent operations within configured request/election time bounds rather than silently degrading consistency.

**REQ-M3-FAIL-004** — Restart of one replica after acknowledged durable writes MUST recover those writes when justified by the surviving committed replicated state.

## 12. Operability

**REQ-M3-OPS-001** — HomeKV MUST expose per-replica database-oriented state sufficient to inspect node ID, role, current leader, vote/term-equivalent identity, last log, committed progress, applied progress, membership, snapshot state, and replica health.

**REQ-M3-OPS-002** — Counters/histograms MUST cover client write/read outcomes, not-leader/unavailable responses, elections/leadership changes, Raft RPC failures, replication lag/progress, WAL append/flush latency, snapshot build/install, queue/backpressure, and recovery failures.

**REQ-M3-OPS-003** — Public HomeKV management/metrics types MUST NOT expose OpenRaft-specific Rust types as a compatibility contract.

## 13. Performance constraints

**REQ-M3-PERF-001** — M3 benchmarks are engineering evidence only. They MUST state replication factor 3, linearizable-read mode, durable-write mode, workload, payload sizes, concurrency, host/toolchain, p50/p95/p99, throughput, and failures.

**REQ-M3-PERF-002** — No M3 optimization may acknowledge before `REQ-M3-WRITE-004`, bypass the safe read barrier, create unbounded queues, or weaken verified M0/M1/M2 behavior.

**REQ-M3-PERF-003** — Group commit, lease reads, Multi-Raft coalescing, and other semantic/performance optimizations require the milestone/spec that explicitly owns them.

## 14. Verification acceptance

Spec 0005 may become Verified only when retained automated evidence establishes all mandatory requirements, including at minimum:

1. deterministic state-machine command tests and three-node replicated CRUD/batch tests;
2. explicit persistence-boundary tests proving vote durability and log-flush callback ordering;
3. leader isolation/minority-write rejection and new-leader failover tests;
4. linearizable-read histories under concurrent writes and leadership change, checked by a model/history checker rather than timing assertions alone;
5. restart/replay tests after acknowledged writes;
6. snapshot build/install/catch-up plus corruption/truncation rejection tests;
7. bounded-network/backpressure tests;
8. preservation of all required M0/M1/M2 CI gates;
9. a three-run replicated engineering benchmark with all semantics recorded.

Passing M3 proves one replicated shard. It MUST NOT be interpreted as proving M4's 1,024-group placement/scaling behavior or public release performance.