# Spec 0001 — HomeKV v1 Requirements

- Status: Draft
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

## 4. Consistency requirements

**REQ-CONS-001** — The default API MUST provide linearizability per shard.

**REQ-CONS-002** — A node MUST NOT acknowledge a strongly consistent write after it has lost authoritative leadership for that shard.

**REQ-CONS-003** — A default GET MUST NOT silently return a stale follower value.

**REQ-CONS-004** — A read optimization MAY avoid a quorum round trip only when the implementation can establish that the result remains linearizable.

**REQ-CONS-005** — During a network partition, a minority replica set MUST NOT independently become writable outside the consensus protocol.

## 5. Durability requirements

**REQ-DUR-001** — For the durable write mode, an acknowledged mutation MUST survive loss/restart of the current leader subject to the configured replication/durability assumptions.

**REQ-DUR-002** — The acknowledgement point for durable writes MUST be explicitly documented and measurable.

**REQ-DUR-003** — Recovery MUST reconstruct a state no newer than what is justified by committed replicated log/snapshot state.

**REQ-DUR-004** — WAL and snapshot corruption/integrity failures MUST be detectable rather than silently applied.

## 6. Failure and reconfiguration requirements

**REQ-FAIL-001** — Gossip/failure detection MAY trigger suspicion or reconfiguration proposals but MUST NOT directly grant authoritative shard leadership or write ownership.

**REQ-FAIL-002** — Leader failure MUST eventually permit a healthy quorum to elect/establish a new authoritative leader.

**REQ-FAIL-003** — Requests sent to stale leaders MUST fail or redirect safely.

**REQ-FAIL-004** — Replica addition/removal MUST preserve the consistency contract during reconfiguration.

**REQ-FAIL-005** — A process crash during WAL append, snapshot generation, or snapshot installation MUST not produce silently inconsistent recovered state.

## 7. Performance requirements

These are engineering targets rather than release claims until verified on controlled hardware.

**REQ-PERF-001** — The healthy-path in-memory GET SHOULD target sub-millisecond p99 latency under a documented non-saturated workload.

**REQ-PERF-002** — Throughput MUST scale with independent shards/cores until a documented shared resource becomes the bottleneck.

**REQ-PERF-003** — The normal data path SHOULD avoid global locks shared by independent shards.

**REQ-PERF-004** — A single mutation MUST NOT require copying the entire logical dataset.

**REQ-PERF-005** — Benchmarks MUST report p50/p95/p99, throughput, workload, concurrency, consistency mode, durability mode, replication factor, key/value sizes, and hardware/toolchain context.

**REQ-PERF-006** — Performance comparisons MUST NOT imply equivalence when consistency or durability settings differ materially.

## 8. Operability requirements

**REQ-OPS-001** — The system MUST expose enough state to determine shard placement, current leader, term/epoch, commit/apply progress, and replica health.

**REQ-OPS-002** — The system SHOULD expose latency, throughput, queue depth, replication lag, WAL/snapshot, memory, and failure-detection metrics.

**REQ-OPS-003** — Backpressure MUST be explicit; overload MUST NOT grow unbounded request queues indefinitely.

## 9. Language requirement

**REQ-LANG-001** — HomeKV v1 remains Rust-first unless controlled benchmark evidence demonstrates that a Zig component materially improves a stable hotspot while preserving equivalent semantics.

## 10. Non-goals

HomeKV v1 does not require:

- SQL
- secondary indexes
- arbitrary range scans
- cross-shard ACID transactions
- multi-master conflict resolution
- follower reads under the default strong-consistency API
- a full Redis-compatible command surface

## 11. Open questions before acceptance

1. What exact logical shard count/range representation should v1 adopt?
2. What durable acknowledgement contract should be the default: quorum WAL persistence, quorum memory + configurable persistence, or another explicit mode?
3. Should the initial consensus implementation use an existing Rust Raft library or a minimal HomeKV implementation for educational/control reasons?
4. What are the first authoritative benchmark hardware and workload profiles?
5. Which client protocol semantics are required for redirects, retries, and request deduplication?

These must be resolved or deliberately deferred in `design.md` before this spec moves to Accepted.
