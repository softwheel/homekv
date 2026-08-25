# HomeKV Consistency Contract

## Default contract

HomeKV's default data API targets **linearizability per shard**.

For a successfully completed operation, the system should behave as if operations on the same shard occurred in a single real-time-respecting order.

This document is a contract for implementation and testing; optimizations are valid only when they preserve it.

## Writes

A successful durable write must satisfy all of the following:

1. it was accepted by the authoritative leader for the shard and configuration epoch;
2. it was ordered in the shard's replicated log;
3. it reached the configured quorum/durability condition;
4. its log position is committed;
5. the response cannot be produced by a stale leader after it has lost authority.

The exact relationship between durable log commit and in-memory apply may be optimized, but externally observable behavior must remain linearizable.

## Reads

A default GET is a strongly consistent read.

A node may serve it locally only when it can prove that doing so is safe for the current leadership term/epoch. Otherwise it must perform a quorum-backed read barrier such as ReadIndex or redirect the client to a node that can satisfy the contract.

Follower reads are not strongly consistent merely because the follower is healthy.

If HomeKV later exposes replica/stale reads, they must use a separate explicit API or consistency option.

## Failed and ambiguous requests

Distributed failures can make the client uncertain whether a mutation committed.

HomeKV must distinguish where practical between:

- definitely rejected before proposal;
- definitely committed;
- result unknown to the client because the response was lost after possible commit.

Clients must not be encouraged to blindly retry non-idempotent operations. Mutation request IDs / deduplication should be considered when richer commands are introduced.

## Single-shard atomic batches

For v1, a transactional batch is allowed only when all keys map to the same logical shard.

The batch is represented as one replicated state-machine command and therefore commits atomically relative to other operations on that shard.

Cross-shard atomic transactions are out of scope for v1.

## Reconfiguration

Topology changes must be versioned and consensus-controlled.

A stale client may route to an old leader or old replica. The receiving node must not accept a write merely because its local failure detector believes it should own the shard.

Expected behavior is redirect/retry with a newer configuration epoch or leader hint.

## Network partitions

During a partition, only a replica set containing a valid quorum may make progress on strongly consistent writes.

Minority partitions must not elect or self-promote an independently writable primary outside the consensus protocol.

Availability may be sacrificed to preserve the consistency contract.

## Leader leases

Leader leases may optimize reads only if their safety assumptions are explicit and tested.

Implementation must account for:

- term changes;
- clock assumptions, if any;
- delayed messages;
- paused processes;
- lease expiration;
- leadership transfer.

If safety cannot be established, fall back to quorum-backed ReadIndex/barrier behavior.

## Snapshots and recovery

A recovered replica must never expose state beyond what is justified by its committed log/snapshot state.

Snapshot metadata should include enough information to establish at least:

- shard/group identity;
- last included log index;
- last included term/epoch;
- integrity/checksum information.

Recovery tests must include process death during WAL writes and snapshot generation/installation.

## Correctness testing

HomeKV should eventually run generated concurrent histories under injected failures and verify linearizability using a history checker.

The fault matrix should include:

- leader kill/restart;
- follower kill/restart;
- asymmetric partitions;
- delayed/dropped/reordered packets;
- stale client routing;
- repeated leader elections;
- snapshot transfer during churn;
- disk/WAL interruption;
- slow or paused replicas.

Performance results are publishable only after the tested configuration passes the corresponding correctness suite.
