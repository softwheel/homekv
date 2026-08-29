# Spec 0003 — Shard-Owned In-Memory Engine Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #9

## 1. Design summary

M1 introduces a local shard engine with one execution owner per logical shard. The first accepted implementation is Candidate A: a bounded async dispatch queue feeding one owner task that alone mutates and reads a shard-local `HashMap<Vec<u8>, Vec<u8>>`.

```text
caller tasks
    |
    | bounded request channel
    v
+---------------- shard N ----------------+
| owner task                              |
|   HashMap<Vec<u8>, Vec<u8>>             |
|   logical-byte accounting               |
|   queue/operation counters              |
+-----------------------------------------+
```

No foreground storage lock is shared across shards. Creating a second shard creates another map, queue, and owner task.

Candidate B (Crossbeam direct reads) remains an optional later M1 experiment. It is not needed to establish the new correctness boundary.

## 2. Types and module boundary

Preferred module:

```text
src/storage/shard_engine.rs
```

Core public types:

```rust
pub const LOGICAL_SHARD_COUNT: u16 = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ShardId(u16);

pub fn shard_for_key(key: &[u8]) -> ShardId;

pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct ShardBatch {
    pub shard_id: ShardId,
    pub mutations: Vec<Mutation>,
}

pub struct ShardEngine { /* sender + lifecycle */ }

pub struct ShardMetrics {
    pub shard_id: ShardId,
    pub key_count: usize,
    pub logical_bytes: usize,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub overload_rejections: u64,
    pub applied_mutations: u64,
}
```

`ShardId::new` rejects values >= 1024. `shard_for_key` hashes the raw key bytes with XXH3-64 and masks the low 10 bits.

The M1 engine API returns owned values. Internal map references never cross the owner boundary.

## 3. Request protocol

The owner queue carries envelopes:

```rust
enum Request {
    Get { key, reply },
    Put { key, value, reply },
    Delete { key, reply },
    Batch { batch, reply },
    Metrics { reply },
    Shutdown { reply },
}
```

A bounded `tokio::sync::mpsc` channel is the initial dispatch implementation because it integrates cleanly with the current Tokio server/runtime and gives explicit capacity. `crossbeam::queue::ArrayQueue` is benchmarked later as an independent queue candidate.

Two admission forms are useful:

- normal async admission waits for bounded capacity;
- `try_*` admission returns an explicit overload error when the queue is full.

The queue is never unbounded.

### Cancellation boundary

The semantic acceptance point is successful enqueue:

- caller cancellation before enqueue: command is not accepted and cannot be applied;
- caller cancellation after enqueue: the owner still applies the accepted command in order; the dropped oneshot only discards the response;
- owner shutdown closes admission first, then drains accepted work before completing shutdown.

This makes cancellation behavior deterministic and compatible with later replicated apply.

## 4. Shard mapping

Use `xxhash_rust::xxh3::xxh3_64(raw_key)` and:

```text
shard = hash & 0x3ff
```

The hash operates on raw bytes, not UTF-8 text or serialized wrappers.

Golden vectors are checked in so accidental hash/library/encoding changes are caught as compatibility failures.

## 5. Candidate A state and execution

Owner state:

```rust
struct OwnerState {
    data: HashMap<Vec<u8>, Vec<u8>>,
    logical_bytes: usize,
    applied_mutations: u64,
}
```

Only the owner task can access `OwnerState`. No mutex is required around `data`.

### GET

A GET is processed in queue order and clones only the requested value into the reply. It does not snapshot or clone the map.

### PUT

For `Put { key, value }`:

1. look up the existing value;
2. if absent, logical-byte delta is `key.len() + value.len()`;
3. if present, logical-byte delta replaces only the old value contribution (`new_value.len() - old_value.len()`);
4. insert/replace the entry;
5. update counters;
6. reply.

No whole-map clone occurs.

### DELETE

For DELETE:

1. remove the key if present;
2. if present, subtract `key.len() + old_value.len()` from logical bytes;
3. if absent, leave state unchanged and report success;
4. update mutation counter and reply.

### Atomic batch

A `ShardBatch` contains only PUT/DELETE mutations and cannot nest another batch.

Before enqueue, validate:

- batch is non-empty where the API requires useful work;
- every mutation key hashes to `batch.shard_id`;
- no unsupported command is present.

After successful validation/enqueue, Candidate A applies every mutation synchronously in the owner loop without awaiting or yielding between mutation steps. Since every GET/mutation for Candidate A also executes on the same owner, no observer can interleave with the batch. Batch atomicity therefore follows directly from single-owner execution.

The individual operations are memory-resident/infallible after validation. Logical byte accounting uses checked debug assertions around map-derived deltas; production counters are derived from actual replaced/removed entries rather than caller estimates.

## 6. Error model

Introduce an engine-local error enum (names may vary without changing semantics):

```text
InvalidShard
CrossShardBatch
QueueFull
Closed
OwnerStopped
```

M1 does not define transport status codes. M2/server integration maps these errors to the external protocol.

## 7. Lifecycle

`ShardEngine::spawn(shard_id, queue_capacity)` creates the bounded channel and owner task.

Shutdown protocol:

1. mark/close new admission;
2. enqueue or signal shutdown through the owned sender lifecycle;
3. owner drains commands already accepted before the shutdown boundary;
4. owner returns final metrics/state summary if useful for tests;
5. `shutdown().await` returns only after the owner task exits.

Dropping all handles also eventually closes the queue and lets the owner exit, but explicit shutdown is the verified path.

## 8. Metrics

Candidate A exposes at least:

- shard ID
- key count
- logical key/value bytes
- configured queue capacity
- current queue depth where Tokio exposes it reliably (`capacity` can be converted to used slots)
- overload rejections from `try_*` calls
- accepted/applied mutation count

Metrics are snapshots and do not create correctness semantics.

## 9. Benchmark integration

Add an M1 benchmark layer to the existing harness rather than overwriting M0 results.

Preferred configuration/result distinction:

```text
layer = "shard"
engine = "owner_hashmap" | "crossbeam_skipmap"
```

M1 results use the same deterministic key/value/workload generator and primary dimensions as M0. The new local engine is fast enough that the measured operation count increases from M0's bounded 200 COW-heavy operations to >= 10,000 operations per primary cell for useful p99 support.

Capture at least three repeated full Candidate A runs before Spec 0003 verification.

The benchmark summary computes:

- M1 median throughput/p50/p95/p99;
- 50k/1k mutation p50 scaling ratio;
- M1/M0 improvement ratio for SET throughput and p50;
- run-to-run spread;
- process RSS/logical bytes per key.

## 10. Candidate B design hook

Candidate B, if implemented, keeps mutations on one owner but stores data in `crossbeam_skiplist::SkipMap`. Direct GET uses a per-shard sequence counter:

Writer:

1. publish odd sequence;
2. apply full mutation/batch;
3. publish next even sequence with release ordering.

Reader:

1. acquire-load sequence; retry if odd;
2. read key;
3. acquire-load sequence again;
4. accept only when both observations match and are even.

The exact memory-ordering implementation requires concurrency/model tests before Candidate B can be promoted. Future OpenRaft applied-index publication occurs only after the even/post-mutation state is visible.

## 11. Alternatives considered

### `RwLock<HashMap<...>>`

Rejected as the primary M1 architecture because it preserves a shared lock and does not create the intended single-owner state-machine boundary.

### Continue COW MVCC with a different tree

Rejected because the verified M0 evidence shows dataset-size-dependent mutation cost and `REQ-PERF-004` forbids whole-dataset copying for a mutation.

### Crossbeam SkipMap first

Rejected as the correctness baseline. Point lookups do not require ordering, and batch visibility needs an additional version protocol. Candidate A gives a simpler state-machine foundation and a fair baseline for the direct-read hypothesis.

### Custom `crossbeam-epoch` hash table

Deferred. The unsafe/reclamation proof burden is not justified until profiling shows the safe candidates leave a stable hotspot.

## 12. Invariants

1. One mutation owner exists per live shard engine.
2. Only the owner accesses Candidate A's map.
3. Accepted mutation order equals owner apply order.
4. A Candidate A batch has no await/yield/interleaving point while it mutates state.
5. No mutation clones the complete shard map.
6. Queue capacity is finite and observable.
7. Logical-byte accounting equals the sum of resident key lengths plus resident value lengths.
8. Key-to-shard mapping is XXH3-64 low 10 bits and never depends on node membership.
9. M1 introduces no consensus/durability semantics.
