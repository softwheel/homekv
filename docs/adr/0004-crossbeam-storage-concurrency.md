# ADR 0004 — Crossbeam for HomeKV storage concurrency

- Status: Proposed
- Scope: M1 shard-owned in-memory engine
- Tracking issue: #9

## Context

HomeKV v1 needs a storage engine that removes the prototype's whole-store copy-on-write cost while preserving predictable tail latency, single-shard atomic batches, and a clean path to replicated linearizable reads.

Prior Softwheel LSM work explored `crossbeam-skiplist::SkipMap`, atomics, and message passing for a lock-free MemTable. The key HomeKV question is not simply whether a lock-free skip list is faster than a hash map; it is whether Crossbeam lets the read path scale across many request-processing cores without forcing all GETs through the single shard mutation owner.

Crossbeam currently provides useful primitives for this design:

- `crossbeam-queue::ArrayQueue` — bounded MPMC queue
- `crossbeam-skiplist::SkipMap` — concurrent lock-free ordered map
- `crossbeam-epoch` — epoch-based reclamation for lock-free structures
- `crossbeam-utils::CachePadded` — cache-line padding for hot per-core/per-shard metadata
- `crossbeam-utils::Backoff` — bounded spin/backoff utility

## Decision

Crossbeam becomes a **mandatory M1 benchmark/design candidate**, not an assumed winner.

M1 will compare two storage/concurrency architectures under identical semantics.

### Candidate A — owner-only hash table

```text
request reactor(s)
      |
      v
bounded dispatch queue
      |
      v
single shard owner
      |
      v
HashMap<Key, Value>
```

All GET/PUT/DELETE/batch operations execute on the shard owner. This provides excellent locality, simple atomic batches, and no internal data-structure synchronization.

This is the correctness/performance baseline for M1.

### Candidate B — single-writer + Crossbeam lock-free direct reads

```text
                      +---------------------------+
GET reactor ----------> lock-free SkipMap read    |
                      |        ^                  |
                      |        |                  |
mutation reactor(s) --+--> bounded queue          |
                               |                  |
                               v                  |
                         single shard writer ------+
```

Mutations remain serialized by one shard owner, preserving deterministic apply order and compatibility with the future Raft state-machine apply path.

GETs may bypass the mutation queue and read a shared `crossbeam_skiplist::SkipMap` directly when the shard's read/version guards establish a valid observation point.

This is the main Crossbeam hypothesis: remove queueing/scheduling from the dominant read path while keeping writes single-writer.

## Atomic batch visibility

A naive lock-free map is insufficient because a reader could observe a partially applied multi-key batch.

Each shard therefore has a sequence counter:

```rust
struct ShardVersion {
    seq: AtomicU64,
}
```

Writer protocol:

1. transition `seq` from even to odd;
2. apply every command in the atomic batch;
3. publish `seq` as the next even value with release semantics;
4. only then publish the new applied state/index.

Reader protocol:

1. load `seq` with acquire semantics;
2. retry if it is odd;
3. read the requested key from the lock-free map;
4. load `seq` again with acquire semantics;
5. accept the result only when both sequence values are equal and even; otherwise retry.

The exact memory orderings and overflow policy must be verified with model/concurrency tests before acceptance.

This is conceptually similar to a sequence-lock validation layer while Crossbeam handles concurrent map traversal and reclamation.

## Future linearizable replicated reads

M3/OpenRaft can layer its read barrier on top of the same structure:

1. establish a linearizable read barrier / ReadIndex-equivalent;
2. wait until the local state machine's `applied_index` reaches the barrier;
3. acquire the published applied state;
4. perform the sequence-validated lock-free lookup.

The shard owner publishes `applied_index` only after the corresponding map mutation/batch is fully visible.

This keeps the Crossbeam optimization below the consensus contract rather than weakening it.

## Queues and backpressure

`ArrayQueue` is the preferred Crossbeam candidate for bounded cross-thread mutation dispatch because HomeKV explicitly forbids unbounded foreground request queues.

`SegQueue` is not the default foreground dispatch primitive because it is unbounded.

M1 must benchmark `ArrayQueue` against any alternative Tokio/channel or custom SPSC/MPSC queue using identical workloads.

## Why SkipMap is not automatically the primary storage structure

`SkipMap` provides safe concurrent access, but HomeKV is primarily a point-KV database:

- skip-list lookup is expected O(log n), whereas a well-designed hash table is expected O(1);
- skip lists involve pointer traversal and reclamation metadata;
- Crossbeam epoch pinning/reclamation has a non-zero per-operation cost;
- ordered/range behavior is not a v1 requirement;
- the single-owner architecture already eliminates the need for a lock inside the mutation path.

Therefore the expected win, if any, comes from **read parallelism and queue bypass**, not from the skip-list asymptotic lookup itself.

## Memory accounting

HomeKV needs exact-enough memory accounting for admission/backpressure. A simple `fetch_add(key.len() + value.len())` on every insert is insufficient because replacements and deletes must account for previous values.

M1 must define accounting for:

- new inserts
- replacements
- deletes
- deferred reclamation
- map/index/node overhead where measurable

Epoch-deferred objects count toward resident memory until they are actually reclaimable.

## Custom crossbeam-epoch hash table

A custom lock-free hash table using `crossbeam-epoch` may offer better point-lookup locality than a SkipMap, but it introduces unsafe memory-reclamation and concurrent-algorithm proof burden.

It is **not** a first M1 implementation target. It requires a separate accepted optimization spec after the safe M1 candidates are benchmarked.

## Verification

The Crossbeam direct-read candidate must prove:

- no partial visibility of atomic batches;
- correct GET/PUT/DELETE semantics under concurrency;
- no use-after-free or stale-reference behavior;
- bounded retry behavior under sustained writes;
- safe interaction between mutation publication and future applied-index publication;
- no unbounded request queue growth.

The sequence/version layer should receive Loom-style concurrency/model tests where practical.

## Benchmark matrix

Compare Candidate A and Candidate B with the same keys, values, dataset, operation generator, CPU affinity, and result schema.

Required workload dimensions:

- read/write mixes: 100/0, 95/5, 80/20, 50/50
- uniform and skewed/hot-key distributions
- multiple dataset cardinalities from the verified M0 baseline
- multiple reader/request threads per shard
- single-key operations and atomic write batches
- saturated and non-saturated load points

Report at least:

- throughput/core
- p50/p95/p99/p99.9
- CPU/op or cycles/op where practical
- allocations/op
- resident memory/key
- dispatch queue depth/wait time
- read retry count due to sequence changes
- epoch/deferred-reclamation behavior where observable

## Promotion rule

M1 must not choose the Crossbeam direct-read design merely because it wins a microbenchmark.

It should become the primary HomeKV read architecture only if it shows a material repeatable end-to-end advantage in the target read-heavy workloads while preserving batch atomicity, memory bounds, mixed-workload performance, and tail latency.

The exact numeric promotion threshold is frozen in Spec 0003 after the M0 baseline is Verified.

## Consequences

This design preserves the simple single-writer state-machine model while giving HomeKV a credible path to horizontally scale reads across request-processing cores.

It also cleanly separates three optimizations:

1. Crossbeam bounded queues for mutation dispatch;
2. Crossbeam SkipMap for safe concurrent/direct read experiments;
3. lower-level `crossbeam-epoch` custom structures only after profiling proves they are justified.
