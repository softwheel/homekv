# Spec 0001 — HomeKV v1 Requirements

- Status: Accepted (amended for OpenRaft selection)
- Tracking issue: #7

## 1. Purpose

HomeKV v1 evolves the current prototype into a memory-first, strongly consistent distributed key-value database designed for low tail latency, high throughput, and rigorous failure semantics.

This document is normative for externally observable system behavior. Implementation choices belong in `design.md`.

## 2. Scope

HomeKV v1 supports:

- byte-string keys and values
- GET
- PUT/SET
- DELETE
- atomic batches whose keys belong to one logical shard
- partitioned scale-out using stable logical shards
- replicated fault tolerance for each shard
- durable recovery for acknowledged durable mutations

## 3. Functional requirements

### Data API

**REQ-DATA-001** — GET MUST return the most recent value visible under the requested consistency contract or indicate that the key does not exist.

**REQ-DATA-002** — PUT MUST atomically replace or create one key/value pair within its shard.

**REQ-DATA-003** — DELETE MUST atomically remove one key within its shard and be deterministic when the key is already absent.

**REQ-DATA-004** — A single-shard batch MUST commit atomically with respect to other commands in the same shard.

**REQ-DATA-005** — Cross-shard atomic transactions MUST NOT be exposed as a v1 guarantee.

### Partitioning and routing

**REQ-SHARD-001** — Every key MUST map deterministically to one stable logical shard ID for a given shard-map version.

**REQ-SHARD-002** — Physical node membership changes MUST NOT redefine key ownership without an authoritative placement/configuration transition.

**REQ-SHARD-003** — Clients MAY cache shard routing metadata, but stale routing MUST fail safely through redirect/retry behavior rather than accepting writes under stale authority.

**REQ-SHARD-004** — The v1 default shard space MUST contain 1,024 logical shards. The shard count is fixed at cluster creation for v1 and is not changed online.

**REQ-SHARD-005** — v1 key-to-shard mapping MUST use XXH3-64 over raw key bytes and select the low 10 bits of the hash. Any future hash/shard-space change is a compatibility change and requires a new accepted spec.

## 4. Consistency requirements

**REQ-CONS-001** — The default API MUST provide linearizability per shard.

**REQ-CONS-002** — A node MUST NOT acknowledge a strongly consistent write after it has lost authoritative leadership for that shard.

**REQ-CONS-003** — A default GET MUST NOT silently return a stale follower value.

**REQ-CONS-004** — A read optimization MAY avoid a quorum round trip only when the implementation can establish that the result remains linearizable.

**REQ-CONS-005** — During a network partition, a minority replica set MUST NOT independently become writable outside the consensus protocol.

**REQ-CONS-006** — The first replicated v1 implementation MUST use a quorum-backed safe read barrier (`read_index`, `get_read_log_id`, or equivalent). Lease-based reads are deferred until a later accepted optimization spec verifies leadership/timing assumptions.

## 5. Durability requirements

**REQ-DUR-001** — For the durable write mode, an acknowledged mutation MUST survive loss/restart of the current leader subject to the configured replication/durability assumptions.

**REQ-DUR-002** — The acknowledgement point for durable writes MUST be explicitly documented and measurable.

**REQ-DUR-003** — Recovery MUST reconstruct a state no newer than what is justified by committed replicated log/snapshot state.

**REQ-DUR-004** — WAL and snapshot corruption/integrity failures MUST be detectable rather than silently applied.

**REQ-DUR-005** — The v1 default distributed write mode MUST acknowledge only after the entry is committed by a quorum whose Raft persistence boundary has been durably flushed, and after the leader has applied the committed entry locally.

**REQ-DUR-006** — A relaxed memory-only mode MAY be introduced later, but it MUST be explicitly named and MUST NOT be used for default strong/durable benchmark claims.

## 6. Failure and reconfiguration requirements

**REQ-FAIL-001** — Gossip/failure detection MAY trigger suspicion or reconfiguration proposals but MUST NOT directly grant authoritative shard leadership or write ownership.

**REQ-FAIL-002** — Leader failure MUST eventually permit a healthy quorum to establish a new authoritative leader.

**REQ-FAIL-003** — Requests sent to stale leaders MUST fail or redirect safely.

**REQ-FAIL-004** — Replica addition/removal MUST preserve the consistency contract during reconfiguration.

**REQ-FAIL-005** — A process crash during WAL append, snapshot generation, or snapshot installation MUST not produce silently inconsistent recovered state.

## 7. Retry semantics

**REQ-RETRY-001** — v1 PUT, DELETE, and deterministic single-shard batches MUST be safe for client retry after transport/routing failures because their command semantics are idempotent.

**REQ-RETRY-002** — Request IDs MAY be used for correlation, but v1 does not promise general exactly-once execution for future non-idempotent commands.

**REQ-RETRY-003** — Any future non-idempotent mutation API MUST define replicated deduplication/exactly-once semantics in a new accepted spec before release.

## 8. Performance requirements

These are engineering targets rather than release claims until verified on controlled hardware.

**REQ-PERF-001** — The healthy-path in-memory GET SHOULD target sub-millisecond p99 latency under a documented non-saturated workload.

**REQ-PERF-002** — Throughput MUST scale with independent shards/cores until a documented shared resource becomes the bottleneck.

**REQ-PERF-003** — The normal data path SHOULD avoid global locks shared by independent shards.

**REQ-PERF-004** — A single mutation MUST NOT require copying the entire logical dataset.

**REQ-PERF-005** — Benchmarks MUST report p50/p95/p99, throughput, workload, concurrency, consistency mode, durability mode, replication factor, key/value sizes, and hardware/toolchain context.

**REQ-PERF-006** — Performance comparisons MUST NOT imply equivalence when consistency or durability settings differ materially.

## 9. Operability requirements

**REQ-OPS-001** — The system MUST expose enough state to determine shard placement, current leader, term/epoch, commit/apply progress, and replica health.

**REQ-OPS-002** — The system SHOULD expose latency, throughput, queue depth, replication lag, WAL/snapshot, memory, and failure-detection metrics.

**REQ-OPS-003** — Backpressure MUST be explicit; overload MUST NOT grow unbounded request queues indefinitely.

## 10. Language requirement

**REQ-LANG-001** — HomeKV v1 remains Rust-first unless controlled benchmark evidence demonstrates that a Zig component materially improves a stable hotspot while preserving equivalent semantics.

## 11. Benchmark authority

**REQ-BENCH-AUTH-001** — Development/CI benchmarks MAY run on any recorded host, but public release performance claims require a dedicated, reproducible Linux benchmark profile with CPU frequency/power settings, topology, kernel, memory and network details recorded.

**REQ-BENCH-AUTH-002** — The exact release benchmark machine is intentionally deferred to the comparative benchmark/release spec; this does not block M0/M1 engineering measurements as long as each result records its host metadata.

## 12. Consensus dependency

**REQ-RAFT-001** — The production v1 M3 consensus integration MUST use OpenRaft rather than a bespoke Raft implementation. The exact pinned OpenRaft version and integration contract are owned by the M3 child spec.

**REQ-RAFT-002** — HomeKV MUST retain ownership of its database-specific Raft storage implementation, state machine, network transport/connection management, durability boundary, observability, scheduling, and placement integration through OpenRaft's extension interfaces.

**REQ-RAFT-003** — M4 MUST benchmark and verify the many-group cost of the chosen OpenRaft integration, including per-group task/runtime overhead, connection sharing, memory/group, throughput/core, and tail latency. `openraft-multi` MAY be used, but its alpha/pre-1.0 status MUST NOT bypass this verification gate.

**REQ-RAFT-004** — If the M4 scaling gate demonstrates that OpenRaft cannot meet HomeKV's accepted performance requirements after reasonable integration optimization, a new accepted consensus-adapter amendment MAY replace the crate without changing the externally visible consistency/durability requirements.

## 13. Milestone boundaries

**REQ-SDD-001** — M0 captures the immutable prototype benchmark baseline and MUST NOT optimize storage.

**REQ-SDD-002** — M1 implements local shard-owned memory execution and MUST NOT introduce distributed consensus.

**REQ-SDD-003** — M2 defines/implements the low-overhead data-plane protocol and routing semantics without making Multi-Raft a dependency.

**REQ-SDD-004** — M3 proves one 3-replica shard with OpenRaft, WAL durability, safe linearizable reads, failover and recovery.

**REQ-SDD-005** — M4 scales the proven M3 machinery to the 1,024-shard placement/Multi-Raft architecture and verifies OpenRaft's many-group cost.

## 14. Non-goals

HomeKV v1 does not require:

- SQL
- secondary indexes
- arbitrary range scans
- cross-shard ACID transactions
- multi-master conflict resolution
- follower reads under the default strong-consistency API
- a full Redis-compatible command surface
- custom production Raft
- lease-based read optimization in the first replicated milestone

## 15. Acceptance decisions

The acceptance-time decisions are:

1. **Logical shard space:** 1,024 shards; `XXH3_64(key) & 1023`; fixed at cluster bootstrap in v1.
2. **Durable acknowledgement:** quorum-durable Raft persistence plus local leader apply before response.
3. **Consensus integration:** OpenRaft for M3; exact pin/integration details belong to M3. M4 includes a mandatory many-group scaling gate and may trigger a later adapter amendment if evidence requires it.
4. **Benchmark authority:** M0 records actual host metadata; release-claim hardware is selected and frozen by the release/comparison spec.
5. **Retries:** unconditional PUT/DELETE/deterministic batches are idempotent and retriable; general exactly-once semantics are out of v1 scope.
