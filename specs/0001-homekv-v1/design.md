# Spec 0001 — HomeKV v1 Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #7

## 1. Architecture summary

HomeKV v1 uses stable logical shards, shard-owned execution, and a replicated state machine per shard.

```text
client
  |
  | shard map + leader hint
  v
node ingress
  |
  +--> shard router --> shard worker --> raft-rs group --> WAL
                           |                 |
                           |                 +--> followers
                           v
                     in-memory state
```

The design optimizes the healthy path without weakening the consistency contract.

## 2. Requirement mapping

- `REQ-SHARD-*` -> fixed logical shard space + versioned placement metadata
- `REQ-CONS-*` -> leader-authoritative replicated state machine + safe ReadIndex path
- `REQ-DUR-*` -> quorum-persisted Raft WAL + snapshots + deterministic replay
- `REQ-FAIL-*` -> consensus-controlled leadership/membership; gossip remains advisory
- `REQ-PERF-*` -> shard-owned execution, no whole-store COW, batching, compact data plane
- `REQ-OPS-*` -> per-shard consensus/data-plane metrics and topology inspection
- `REQ-RAFT-*` -> TiKV `raft-rs` consensus core with HomeKV-owned integration

## 3. Logical shards

The v1 shard space contains **1,024 logical shards**. Keys map with:

```text
shard_id = XXH3_64(key_bytes) & 1023
```

The count is fixed at cluster bootstrap in v1. Changing the hash or shard count is a compatibility/migration event and requires a new accepted spec.

Logical shard ID is independent of current node membership. A separate versioned placement map assigns each shard to a replication group and identifies replicas plus leader hints.

## 4. Execution ownership

A shard has one execution owner on a node. A worker may own multiple shards, but each shard's state-machine mutations execute serially on one worker.

Cross-worker communication uses bounded queues. Queue depth is observable and overload is rejected/backpressured before unbounded memory growth.

M1 owns the exact worker/shard scheduling and queue design. It must remain local/single-node; consensus integration begins in M3.

## 5. In-memory storage

The primary v1 engine is point-operation oriented:

- hash-indexed keys
- owned byte strings initially
- no whole-store clone on mutation
- explicit memory accounting
- allocator/arena specialization only after profiling

The existing BTree/COW implementation remains the immutable M0 benchmark/control implementation until M1 is verified.

MVCC is not required for the default v1 contract. Future multi-version semantics require a dedicated spec.

## 6. Consensus core

Production v1 uses **TiKV `raft-rs`** as the Raft algorithm core. HomeKV intentionally owns the surrounding database machinery:

- persistent Raft log/WAL
- transport
- state machine
- scheduling and batching
- snapshots/recovery
- metrics
- logical-shard placement and routing

The exact pinned crate/git revision and adapter APIs belong to Spec M3, so the dependency can be pinned reproducibly at implementation time.

Why `raft-rs`:

- it is a low-level consensus core rather than a complete database framework;
- it is used by TiKV and is suitable for many independent Raft groups;
- HomeKV retains control over the hot replication, persistence and scheduling paths;
- it avoids making bespoke consensus correctness a prerequisite for the database project.

A from-scratch Raft may exist as a research subproject but is not the v1 correctness foundation.

## 7. Mutation path

For a replicated shard:

1. client routes to the leader or receives a structured redirect;
2. leader validates current authority/placement epoch;
3. deterministic command is proposed to the `raft-rs` group;
4. replicas persist the Raft Ready state before acknowledging replication success;
5. quorum persistence allows the entry to become committed;
6. leader applies the committed entry to its shard state machine;
7. leader returns success.

The initial durable path favors correctness and measurable semantics over minimum fsync latency. Group commit/batching optimizations are added only after this baseline path is verified.

## 8. Read path

Default GET is linearizable.

M3 starts with **safe quorum-backed ReadIndex/equivalent behavior**. A read is served only after the leader establishes the required read barrier and its local state machine has applied through that barrier.

Lease-based leader-local reads are explicitly deferred. They may be introduced by a later performance spec only after assumptions about leadership, clocks/timing, pause behavior and failure tests are explicit.

Follower reads are not part of the default v1 consistency API.

## 9. Linux durability boundary

The v1 durable acknowledgement contract is:

> A successful distributed mutation response requires quorum-persisted Raft state and local leader application of the committed entry.

For the first Linux implementation, a replica counts an entry as persisted only after required log bytes and Raft hard-state metadata reach the configured durable WAL boundary and the corresponding durability operation (`fdatasync`, `fsync`, or verified equivalent) completes successfully.

The `raft-rs` Ready processing order must preserve its persistence-before-message safety requirements. WAL records are checksummed. Snapshots include last-included index/term plus integrity metadata.

A future relaxed/memory-only mode must be explicitly named and cannot be compared as equivalent to the default durable mode.

## 10. Membership and gossip

The existing gossip/phi detector becomes a health-observation subsystem. It may discover nodes, surface suspicion, emit metrics, or trigger a reconfiguration proposal.

It may not elect a leader, promote a replica, mutate authoritative membership, or allow writes from local suspicion alone.

Replica membership changes use Raft-safe configuration transitions. Placement metadata carries a monotonically increasing epoch/version.

## 11. Client routing and retries

Clients may cache shard-map version, shard -> replication group mapping and leader hints.

A stale/non-leader node returns a structured retry/redirect result containing the best-known placement epoch and leader hint.

The v1 mutation surface is deliberately idempotent: unconditional PUT, DELETE and deterministic batches can be retried after an uncertain transport/routing result. Request IDs are correlation metadata, not a general exactly-once guarantee.

Non-idempotent commands require a later spec with replicated deduplication semantics.

## 12. Data-plane protocol

Tonic/gRPC remains useful for administrative/control APIs.

The hot data plane evolves in M2 toward a compact framed protocol with request ID, command, routing epoch, lengths, pipelining/multiplexing, batch representation, explicit redirect/status responses and bounded frame size.

M2 owns the exact wire format and compatibility rules.

## 13. Snapshots and recovery

Snapshot creation must avoid long global pauses and be anchored at a committed/applied log position.

Recovery sequence:

1. validate/load latest complete snapshot;
2. restore shard state and included log metadata;
3. replay valid subsequent WAL/log entries;
4. participate in consensus only after state is coherent;
5. never expose speculative/uncommitted entries as applied state.

Exact snapshot/WAL segmentation details are specified in the persistence work under M3/M5.

## 14. Rebalancing

Safe shard movement under M4:

1. choose target replica placement;
2. add learner/non-voting replica where supported;
3. transfer snapshot/log until caught up;
4. commit membership change;
5. optionally transfer leadership;
6. remove old replica;
7. publish the newer placement epoch.

Foreground latency impact is part of verification.

## 15. Observability

Per node/shard metrics should include requests/latency by operation, queue depth/backpressure, leader/role, term/epoch, commit/applied index, replication lag, batching, WAL sync latency, snapshot bytes/time, memory/key/value bytes, elections/leadership changes and gossip suspicion state.

## 16. Milestone boundaries

### M0 — Baseline

Benchmark only. No storage optimization.

### M1 — Local shard-owned engine

Owns storage layout, shard/worker ownership, local queues/backpressure, memory accounting and single-shard atomic application. No Raft.

### M2 — Data plane

Owns framed wire protocol, pipelining, client/server routing metadata and redirect/error semantics. It can run against local shards without Multi-Raft.

### M3 — One replicated shard

Owns `raft-rs` adapter, three-replica transport, WAL persistence, safe ReadIndex, failover, snapshots sufficient for one group and recovery verification.

### M4 — Multi-Raft / placement

Owns 1,024 logical shard placement, many Raft-group scheduling, rebalancing, placement epochs, shard-aware client routing and multi-group operational behavior.

### M5+

Owns persistence/operability hardening and performance optimization after the basic distributed architecture is proven.

## 17. Benchmark authority

M0/M1 results are valid engineering evidence on any host whose metadata is recorded. CI results are regression signals, not release claims.

The exact dedicated hardware/network profile for public comparative claims is deliberately selected and frozen by the release/comparison benchmark spec. This deferred machine selection does not weaken the correctness or architecture acceptance gates.

## 18. Alternatives considered

### Direct consistent-hash ownership

Rejected for authoritative placement because local membership views can disagree during partitions.

### Global shared concurrent map

Rejected as the primary architecture because it couples independent shards through shared synchronization and makes CPU-locality control harder.

### Keep copy-on-write MVCC

Rejected for the main write path because first mutation can clone the entire underlying store.

### Rewrite everything in Zig first

Rejected by ADR 0001. Architecture is the higher-leverage bottleneck; Zig remains a measured hot-path option.

### OpenRaft

A credible alternative with higher-level application APIs and documented leader-lease support. HomeKV selects `raft-rs` because v1 intentionally wants lower-level control of WAL, transport, scheduling and eventual Multi-Raft behavior. The initial implementation still uses safe ReadIndex rather than chasing a lease fast path.

### Build custom Raft first

Rejected for production v1 because correctness risk and verification cost are high relative to HomeKV's primary goals.

## 19. Accepted decisions

All acceptance-blocking design items are resolved or deliberately delegated to child specs:

1. consensus core: TiKV `raft-rs`;
2. shard space: 1,024 shards, XXH3-64 low 10 bits;
3. durability: quorum durable WAL persistence + leader apply before success;
4. M1/M2/M3/M4 boundaries: fixed in section 16;
5. benchmark authority: host metadata for development; dedicated release machine frozen later.
