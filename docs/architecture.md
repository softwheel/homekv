# HomeKV v1 Architecture

## Goals

HomeKV v1 is a memory-first distributed key-value database with:

- linearizable single-key reads and writes
- predictable low tail latency on the healthy path
- horizontal scale through logical shards
- per-shard fault tolerance through consensus replication
- durable recovery through WAL + snapshots
- failure semantics that remain correct under partitions and stale routing

## Non-goals for v1

- arbitrary cross-shard ACID transactions
- SQL or secondary indexes
- multi-master conflict resolution
- eventual-consistency-first replication
- transparent use of gossip to reassign authoritative ownership

## Data model

The initial model is intentionally small:

- byte-string keys
- byte-string values
- GET
- PUT/SET
- DELETE
- batched operations constrained to one shard

A mutation receives a monotonically ordered position inside its shard's replicated log. The state machine applies committed commands deterministically.

## Partitioning

Keys map to a stable logical shard ID. Logical shards are then placed on nodes.

This indirection is important: process liveness must not directly redefine key ownership. A client may cache a shard map and route directly to the current leader, but leadership and membership changes are authoritative only after they are committed by the relevant consensus/control-plane state.

The existing consistent-hash implementation can remain useful as an experiment or placement helper, but v1 routing should ultimately be based on stable shard identifiers and versioned placement metadata.

## Execution model

Each shard has one logical execution owner at a time on a node. The preferred implementation model is shared-nothing:

```text
network ingress
     |
     v
shard router
  |   |   |
  v   v   v
 W0  W1  W2 ...
```

Each worker owns one or more shard state machines. Commands for a shard are serialized by that shard's execution context, avoiding a global lock around the in-memory map.

This model is intended to make the common path:

1. parse request
2. identify shard
3. enqueue/dispatch to shard owner
4. perform local lookup or propose mutation
5. return response

rather than acquiring multiple shared locks for each operation.

## In-memory engine

The v1 engine should optimize for point lookups, updates, and memory efficiency rather than ordered scans.

Initial direction:

- hash-table-oriented index
- compact key/value ownership
- slab/arena allocation experiments
- explicit accounting for memory/key
- no whole-dataset copy-on-write on mutation

MVCC should only be introduced where a concrete semantic requirement demands multiple visible versions. Replication-log ordering already provides the primary serialization mechanism for v1 writes.

## Replication

Each shard is a replicated state machine, initially with three replicas.

Write path:

```text
client
  |
  v
leader
  |
  +--> append/propose
  |
  +--> replicate to followers
  |
  +--> quorum commit
  |
  +--> apply to in-memory state
  v
response
```

Implementation should evolve from one Raft group to many independent groups per process. Batching and transport multiplexing are expected to be major performance levers.

## Reads

Linearizable reads should avoid unnecessary quorum round trips on the healthy path while remaining safe under leader changes.

Preferred hierarchy:

1. safe leader-local read under a valid leadership/lease condition
2. ReadIndex or equivalent quorum-backed barrier when local certainty is insufficient
3. explicit optional stale/replica read mode only if later added as a separate API contract

The system must never silently serve a potentially stale follower read as a strongly consistent read.

## Membership and failure detection

HomeKV's gossip and phi-accrual failure detector are advisory components.

They may help with:

- discovery
- health suspicion
- observability
- triggering reconfiguration proposals

They must not independently:

- elect a leader
- promote a replica
- transfer shard ownership
- permit writes under an uncommitted topology

This separation prevents failure-detector disagreement from becoming split brain.

## Durability

HomeKV is memory-first, not memory-only.

The durable path should eventually include:

- segmented WAL
- checksummed records
- batched/group commit
- snapshot creation
- snapshot installation
- restart replay
- log truncation after safe snapshot advancement

The exact acknowledgement contract must be explicit. A durable write should not be reported successful until the configured replicated-durability condition has been satisfied.

## Protocols

### Data plane

The long-term data plane should use a compact binary framing protocol with:

- pipelining
- batched operations
- request IDs for multiplexing
- minimal copies
- bounded parsing
- explicit backpressure

### Control plane

Tonic/gRPC remains appropriate for:

- administrative operations
- metadata inspection
- health endpoints
- cluster management
- debugging APIs

This keeps ergonomics where latency is not dominant while allowing the hot path to remain small.

## Rebalancing

Rebalancing should move logical shard replicas through consensus-aware membership changes rather than by instantly changing a hash ring.

A safe high-level sequence is:

1. add target replica
2. catch up log/snapshot state
3. commit membership transition
4. optionally transfer leadership
5. remove old replica
6. publish new placement epoch

Clients with stale routing metadata receive redirects containing a newer epoch/leader hint.

## Performance model

The performance budget should be decomposed into:

- client encoding/queueing
- network transit
- server parsing
- shard dispatch
- consensus proposal
- follower replication
- durable log cost
- apply cost
- response encoding

Optimization work should identify which component dominates p99 before introducing complexity.

## Evolution

After v1 correctness and performance are established, possible research directions include:

- separating durable log service from memory compute
- RDMA or specialized transports
- io_uring-based transport/WAL paths
- hardware-aware NUMA placement
- custom allocators
- value log / tiered persistence
- cross-shard transactions
- Zig implementations of selected hot components
