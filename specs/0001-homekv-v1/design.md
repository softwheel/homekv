# Spec 0001 — HomeKV v1 Design

- Status: Draft
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
  +--> shard router --> shard worker --> consensus group --> WAL
                           |                |
                           |                +--> followers
                           v
                     in-memory state
```

The design optimizes the healthy path without weakening the consistency contract.

## 2. Requirement mapping

- `REQ-SHARD-*` -> stable logical shard IDs + versioned placement metadata
- `REQ-CONS-*` -> leader-authoritative replicated state machine + safe linearizable read path
- `REQ-DUR-*` -> replicated WAL + snapshots + deterministic replay
- `REQ-FAIL-*` -> consensus-controlled leadership/membership; gossip remains advisory
- `REQ-PERF-*` -> shard-owned execution, no whole-store COW, batching, compact data plane
- `REQ-OPS-*` -> per-shard consensus/data-plane metrics and topology inspection

## 3. Logical shards

Keys map to stable logical shard IDs using a deterministic hash function over key bytes and a configured shard space.

The logical shard ID is independent of current node membership. A separate versioned placement map assigns each shard to a replication group and identifies current replicas/leader hints.

This satisfies `REQ-SHARD-001` and prevents failure detector disagreement from redefining ownership (`REQ-SHARD-002`).

### Initial choice

For v1, prefer a fixed power-of-two shard space so shard selection can be implemented with a stable hash + mask/modulo operation. The exact initial count remains an acceptance-time parameter rather than a semantic guarantee.

## 4. Execution ownership

A shard has one execution owner on a node. Workers own multiple shards, but a single shard's state-machine mutations execute serially on one worker.

Benefits:

- no global data lock across independent shards
- deterministic apply order
- reduced synchronization on point operations
- natural backpressure point per worker/shard

Cross-worker communication uses bounded queues. Queue depth is observable and overload is rejected/backpressured before unbounded memory growth (`REQ-OPS-003`).

## 5. In-memory storage

The primary v1 engine is point-operation oriented.

Direction:

- hash-indexed keys
- owned byte strings for keys/values initially
- no whole-store clone on mutation
- explicit memory accounting
- allocator/arena experiments only after baseline profiling

The existing BTree/COW implementation remains a benchmark/control implementation until M1 is verified.

MVCC is not a required primitive for the default single-key/single-shard v1 contract. If future snapshot/read semantics need multiple visible versions, they should be added by a dedicated spec rather than retained implicitly.

## 6. Replicated state machine

Each logical shard is backed by a consensus group, initially three replicas in distributed benchmarks.

### Mutation path

1. client routes to leader or any node that can redirect;
2. leader validates current authority/epoch;
3. command is proposed into the shard log;
4. entry is replicated;
5. quorum/durability condition is satisfied;
6. entry becomes committed;
7. state machine applies it in log order;
8. response is returned according to the configured acknowledgement contract.

The command representation contains enough information for deterministic application.

### Consensus implementation choice

The first production-quality v1 path should strongly prefer an existing, battle-tested Rust Raft implementation unless a separate accepted spec justifies implementing Raft itself. HomeKV's differentiating work is the data plane, memory engine, replication integration, and performance behavior—not proving that a bespoke consensus implementation can reach parity.

A from-scratch Raft implementation may exist as an educational/research subproject, but must not silently become the correctness foundation without equivalent verification.

## 7. Read path

Default GET is linearizable.

Read hierarchy:

1. leader-local fast path only when leadership safety can be established under the chosen consensus/lease mechanism;
2. otherwise ReadIndex/equivalent quorum-backed barrier;
3. stale follower reads, if ever added, require a separate explicit consistency mode/spec.

The initial implementation may use the safer barrier path first and add leader-lease optimization later behind benchmark + correctness evidence.

## 8. Durability model

HomeKV is memory-first but supports durable acknowledged writes.

Initial persistence design:

- append-only segmented WAL
- checksummed records/frames
- group commit capability
- consensus log metadata sufficient for replay
- snapshots with last-included log index/term and integrity metadata
- replay committed entries after restart

### Proposed default acknowledgement contract

For the v1 durable mode, a write response should require the entry to be committed by a quorum whose acknowledgement includes persistence to the configured durable WAL boundary.

A future explicitly named memory-only/relaxed durability mode may trade durability for lower latency, but benchmark output must label it separately. The default strong benchmark should use the durable contract.

This resolves the intended direction for `REQ-DUR-001/002`, subject to spec review.

## 9. Membership, gossip, and reconfiguration

The existing gossip/phi detector becomes a health-observation subsystem.

It can:

- discover nodes
- surface suspicion
- emit metrics
- trigger a proposal to move/replace a replica

It cannot:

- directly elect a shard leader
- directly promote a replica
- change authoritative membership
- allow writes based on local suspicion

Replica movement uses consensus-aware membership transitions. Placement metadata receives a monotonically increasing epoch/version.

## 10. Client routing

Clients may cache:

- shard-map version
- shard -> replication group
- leader hint

If a request reaches a stale/non-leader node, that node returns a structured redirect/retry response including the best-known newer epoch/leader information.

The protocol must distinguish retriable routing errors from application results.

Mutation request IDs are recommended before adding non-idempotent operations beyond PUT/DELETE; v1 PUT/DELETE retries can be made idempotent at the command semantics level.

## 11. Data-plane protocol

Tonic/gRPC remains available for administrative/control APIs.

The hot data plane evolves toward a compact framed binary protocol with:

- request ID
- command type
- shard-map epoch or optional routing metadata
- key/value lengths
- pipelining/multiplexing
- batch representation
- explicit status/redirect responses
- bounded frame size

M2 will define the wire format in its own child spec before implementation.

## 12. Snapshots and recovery

Snapshot creation should avoid long global pauses. The exact implementation is deferred to the persistence child spec, but the state machine must support a consistent snapshot boundary at a committed/applied log position.

Recovery sequence:

1. validate/load latest complete snapshot;
2. restore shard state and included log metadata;
3. replay valid subsequent WAL/log entries;
4. participate in consensus only after state is coherent;
5. never expose speculative/uncommitted entries as applied state.

## 13. Rebalancing

Safe shard movement:

1. choose target replica placement;
2. add learner/non-voting replica where supported;
3. transfer snapshot/log until caught up;
4. commit membership change;
5. optionally transfer leadership;
6. remove old replica;
7. publish placement epoch.

Foreground performance impact is part of verification.

## 14. Observability

Per node/shard metrics should eventually include:

- requests + latency by operation
- queue depth/backpressure events
- shard leader/role
- term/epoch
- commit index/applied index
- replication lag
- proposal batching
- WAL append/fsync/group-commit latency
- snapshot bytes/time
- memory used / key count / value bytes
- elections / leadership changes
- gossip suspicion state

## 15. Alternatives considered

### Direct consistent-hash ownership

Rejected for authoritative placement because local membership views can disagree during partitions and violate strong consistency.

### Global shared concurrent map

Rejected as the primary architecture because it couples independent shards through shared synchronization and makes CPU-locality control harder.

### Keep copy-on-write MVCC

Rejected for the main write path because first mutation can clone the entire underlying store, violating `REQ-PERF-004`.

### Rewrite everything in Zig first

Rejected by ADR 0001. Architecture is the higher-leverage bottleneck; Zig remains a measured hot-path option.

### Build custom Raft first

Not preferred for the production v1 path. Correctness risk and verification cost are high relative to HomeKV's primary goals.

## 16. Open design items before Accepted

1. Select the Rust consensus library and document its read/lease semantics.
2. Fix the initial logical shard count and hash algorithm for v1 compatibility.
3. Define exact WAL persistence acknowledgement semantics on Linux.
4. Define the child-spec boundary for M1 memory engine vs M2 protocol vs M3 replication.
5. Establish authoritative benchmark hardware/profile for release claims.

Implementation tasks remain blocked until these are resolved or explicitly deferred in an accepted revision.
