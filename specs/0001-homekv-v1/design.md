# Spec 0001 — HomeKV v1 Design

- Status: Accepted (amended for OpenRaft selection)
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
  +--> shard router --> shard worker --> OpenRaft group --> WAL
                           |               |
                           |               +--> followers
                           v
                     in-memory state
```

The design optimizes the healthy path without weakening the consistency contract.

## 2. Requirement mapping

- `REQ-SHARD-*` -> fixed logical shard space + versioned placement metadata
- `REQ-CONS-*` -> leader-authoritative replicated state machine + safe OpenRaft read barrier
- `REQ-DUR-*` -> quorum-durable Raft log storage + snapshots + deterministic replay
- `REQ-FAIL-*` -> consensus-controlled leadership/membership; gossip remains advisory
- `REQ-PERF-*` -> shard-owned execution, no whole-store COW, batching, compact data plane
- `REQ-OPS-*` -> per-shard consensus/data-plane metrics and topology inspection
- `REQ-RAFT-*` -> OpenRaft M3 integration with a mandatory M4 many-group scaling gate

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

## 6. Consensus integration

Production v1 M3 uses **OpenRaft** as the Raft implementation.

HomeKV still owns the database-specific surrounding machinery through OpenRaft's application interfaces:

- `RaftLogStorage` implementation backed by HomeKV's durable WAL/log format
- `RaftStateMachine` implementation backed by the shard-owned in-memory engine
- `RaftNetwork`/network-factory integration using HomeKV's connection management and transport
- snapshot encoding/storage/installation
- request routing and redirect semantics
- metrics and topology reporting
- shard placement and Multi-Raft scheduling policy

The exact pinned OpenRaft version, feature flags, storage adapter and network adapter belong to Spec M3 so the dependency can be frozen reproducibly at implementation time.

### Why OpenRaft for M3

OpenRaft better matches HomeKV's v1 development objective than using the lower-level `raft-rs::RawNode` API directly:

- storage, state-machine and network responsibilities are explicit application interfaces;
- linearizable read APIs are documented as part of the framework;
- membership changes, learner handling and snapshots are integrated rather than reconstructed around a raw consensus core;
- it is async/event-driven and can batch internal work without requiring HomeKV to hand-roll the full Ready/persist/send/apply orchestration correctly;
- it lets v1 spend engineering effort on database data-path behavior instead of consensus plumbing while retaining ownership of HomeKV-specific WAL/network/state-machine code.

This is an integration choice, not a permanent performance assumption. Section 16 defines the M4 scaling gate.

## 7. Mutation path

For a replicated shard:

1. client routes to the leader or receives a structured redirect;
2. leader validates current authority/placement epoch;
3. deterministic command is submitted through the OpenRaft client-write path;
4. HomeKV's OpenRaft log store persists required Raft log/vote/committed state according to the accepted durability boundary;
5. Raft establishes commitment through the configured quorum;
6. OpenRaft invokes HomeKV's state machine to apply the committed entry;
7. HomeKV returns success only after the accepted durable/apply boundary is satisfied.

The M3 spec must explicitly verify that the chosen storage callback/flush semantics satisfy `REQ-DUR-005`; framework-level completion alone is not assumed to imply HomeKV's durable success contract.

Group commit/batching optimizations are added only after the baseline durable path is verified.

## 8. Read path

Default GET is linearizable.

M3 starts with OpenRaft's safe linearizable read path using `read_index`, `get_read_log_id`, or the version-appropriate equivalent. A read is served only after the leader establishes the required read barrier and its local state machine has applied through that barrier.

Lease-based leader-local reads are explicitly deferred. OpenRaft has leader-lease machinery, but HomeKV will not enable a lease fast path until a later performance spec defines and verifies its timing/leadership/pause assumptions.

Follower reads are not part of the default v1 consistency API.

## 9. Linux durability boundary

The v1 durable acknowledgement contract is:

> A successful distributed mutation response requires quorum-durable Raft persistence and local leader application of the committed entry.

For the first Linux implementation, a replica counts an entry as durable only after required log bytes and Raft persistent metadata reach the configured durable WAL boundary and the corresponding durability operation (`fdatasync`, `fsync`, or verified equivalent) completes successfully.

The M3 OpenRaft storage adapter must map OpenRaft's log/vote/commit persistence callbacks to this boundary explicitly. WAL records are checksummed. Snapshots include last-included log identity plus integrity metadata.

A future relaxed/memory-only mode must be explicitly named and cannot be compared as equivalent to the default durable mode.

## 10. Membership and gossip

The existing gossip/phi detector becomes a health-observation subsystem. It may discover nodes, surface suspicion, emit metrics, or trigger a reconfiguration proposal.

It may not elect a leader, promote a replica, mutate authoritative membership, or allow writes from local suspicion alone.

Replica membership changes use OpenRaft's Raft-safe membership transition APIs. Placement metadata carries a monotonically increasing epoch/version.

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
3. restore OpenRaft persistent state/log state;
4. replay valid subsequent durable entries;
5. participate in consensus only after state is coherent;
6. never expose speculative/uncommitted entries as applied state.

Exact snapshot/WAL segmentation details are specified in the persistence work under M3/M5.

## 14. Rebalancing

Safe shard movement under M4:

1. choose target replica placement;
2. add learner/non-voting replica through OpenRaft membership APIs;
3. transfer snapshot/log until caught up;
4. commit membership change;
5. optionally transfer leadership;
6. remove old replica;
7. publish the newer placement epoch.

Foreground latency impact is part of verification.

## 15. Observability

Per node/shard metrics should include requests/latency by operation, queue depth/backpressure, leader/role, term/epoch, commit/applied progress, replication lag, batching, WAL sync latency, snapshot bytes/time, memory/key/value bytes, elections/leadership changes and gossip suspicion state.

OpenRaft metrics may feed these signals, but HomeKV exposes a database-oriented metrics contract rather than leaking framework-specific types as its public management API.

## 16. Milestone boundaries and OpenRaft scaling gate

### M0 — Baseline

Benchmark only. No storage optimization.

### M1 — Local shard-owned engine

Owns storage layout, shard/worker ownership, local queues/backpressure, memory accounting and single-shard atomic application. No Raft.

### M2 — Data plane

Owns framed wire protocol, pipelining, client/server routing metadata and redirect/error semantics. It can run against local shards without Multi-Raft.

### M3 — One replicated shard

Owns the OpenRaft adapter, three-replica transport, durable log storage, safe linearizable reads, membership basics, failover, snapshots sufficient for one group and recovery verification.

### M4 — Multi-Raft / placement

Owns 1,024 logical shard placement, many-group scheduling, rebalancing, placement epochs, shard-aware client routing and multi-group operational behavior.

M4 MUST benchmark:

- memory per Raft group
- tasks/futures/timers per group
- connection count and connection sharing
- idle-group overhead
- active groups per core
- throughput/core and p99/p99.9 as group count grows
- scheduler/runtime contention

`openraft-multi` is a candidate connection-sharing adapter, not an assumed production dependency. Its current pre-1.0/alpha state requires explicit pinning and verification.

If OpenRaft cannot meet the accepted M4 scaling/performance requirements after reasonable integration optimization, HomeKV may replace the consensus adapter through a new accepted spec amendment while keeping the same consistency/durability semantics.

### M5+

Owns persistence/operability hardening and performance optimization after the basic distributed architecture is proven.

## 17. Benchmark authority

M0/M1 results are valid engineering evidence on any host whose metadata is recorded. CI results are regression signals, not release claims.

The exact dedicated hardware/network profile for public comparative claims is deliberately selected and frozen by the release/comparison benchmark spec.

## 18. Alternatives considered

### Direct consistent-hash ownership

Rejected for authoritative placement because local membership views can disagree during partitions.

### Global shared concurrent map

Rejected as the primary architecture because it couples independent shards through shared synchronization and makes CPU-locality control harder.

### Keep copy-on-write MVCC

Rejected for the main write path because first mutation can clone the entire underlying store.

### Rewrite everything in Zig first

Rejected by ADR 0001. Architecture is the higher-leverage bottleneck; Zig remains a measured hot-path option.

### TiKV `raft-rs`

Still a strong alternative. Its `RawNode`/`Ready` model gives very low-level control and it is proven in TiKV. HomeKV does not select it for M3 because that control also makes HomeKV responsible for more consensus orchestration and persistence/message ordering glue. The M4 scaling gate preserves the option to reconsider the adapter if OpenRaft framework overhead becomes a proven bottleneck.

### Build custom Raft first

Rejected for production v1 because correctness risk and verification cost are high relative to HomeKV's primary goals.

## 19. Accepted decisions

All acceptance-blocking design items are resolved or deliberately delegated to child specs:

1. consensus integration: OpenRaft for M3, with a mandatory M4 many-group scaling gate;
2. shard space: 1,024 shards, XXH3-64 low 10 bits;
3. durability: quorum-durable Raft persistence + leader apply before success;
4. M1/M2/M3/M4 boundaries: fixed in section 16;
5. benchmark authority: host metadata for development; dedicated release machine frozen later.

See `docs/adr/0002-openraft-consensus.md` for the crate-selection rationale and escape criteria.
