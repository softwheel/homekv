# ADR 0002 — Select OpenRaft for HomeKV v1 M3

- Status: Accepted by Spec 0001 amendment
- Date: 2026-08-25
- Supersedes: the `raft-rs` implementation choice in the original Spec 0001 acceptance

## Context

HomeKV v1 needs a production-quality Raft implementation while keeping the project focused on database/data-plane engineering: memory layout, shard ownership, WAL/durability, networking, routing, failure behavior, snapshots, and eventually many Raft groups.

The original v1 spec selected TiKV `raft-rs` because it is low-level, proven in TiKV, and gives the application precise control over persistence, messages, apply ordering, and Multi-Raft integration.

During spec review we revisited OpenRaft as the alternative.

## Decision

Use **OpenRaft** for the M3 single-replicated-shard implementation.

HomeKV will implement and own:

- OpenRaft `RaftLogStorage` backed by HomeKV's WAL/log persistence
- OpenRaft `RaftStateMachine` backed by the shard-owned in-memory engine
- OpenRaft network integration backed by HomeKV transport/connection management
- HomeKV durability acknowledgement rules
- snapshot representation/integrity
- routing, placement, observability, and database APIs

The M3 child spec must pin the exact OpenRaft version and features.

## Why OpenRaft

### 1. Better v1 correctness/velocity boundary

OpenRaft exposes storage, state-machine, and networking as explicit application extension points while handling more of the Raft orchestration itself. This leaves HomeKV substantial control over database-specific components without requiring HomeKV to reconstruct every `RawNode::Ready` persistence/send/apply sequence correctly.

### 2. Linearizable reads are a first-class documented path

OpenRaft documents `read_index` / read-log-id behavior for linearizable reads. HomeKV will start with that safe path and keep lease-based optimization disabled until separately specified and verified.

### 3. Membership and snapshot plumbing are integrated

Dynamic membership, learners, snapshot building/installation, and lifecycle behavior are closer to application-level APIs, reducing the amount of consensus plumbing HomeKV must build before it can test the actual database system.

### 4. Still allows a custom data plane

Selecting OpenRaft does not require adopting a generic KV storage engine or generic network protocol. HomeKV still controls the hot storage and transport implementations.

## Why not `raft-rs` for M3

`raft-rs` remains an excellent low-level consensus core and is proven by TiKV. Its `RawNode` / `Ready` model gives excellent control, but that control increases integration surface and correctness burden in M3.

HomeKV's v1 differentiator is not the Raft algorithm itself. M3 should establish a correct, durable replicated shard as efficiently as possible so engineering effort can move to the memory engine, networking, Multi-Raft scheduling, and performance behavior.

## Risks

### OpenRaft API stability

OpenRaft is still pre-1.0 and documents its API as unstable. HomeKV must pin versions and treat upgrades as reviewed changes.

### Multi-Raft maturity

`openraft-multi` exists for sharing routing/connections across groups, but the current line is pre-1.0/alpha. HomeKV will not assume it is production-ready merely because it exists.

### Framework overhead

A higher-level framework may add per-group tasks, channels, allocations, timers, or scheduling overhead that becomes visible at hundreds/thousands of groups.

## Mandatory M4 escape gate

M4 must measure the OpenRaft integration as the number of groups grows. At minimum:

- bytes/group at idle and under load
- task/future/timer count per group
- idle CPU cost
- active groups per core
- transport connection sharing
- throughput/core
- p99 and p99.9 latency as group count grows
- runtime/scheduler contention

If OpenRaft cannot satisfy HomeKV's accepted scaling/performance requirements after reasonable integration optimization, the consensus adapter may be replaced through a new accepted spec amendment. That replacement must preserve the same consistency, durability, failure, and client-visible semantics.

## Consequences

- M3 implementation gets a smaller consensus-integration surface.
- HomeKV can focus earlier on its database-specific durability/state/network design.
- OpenRaft upgrades are pinned/reviewed rather than floated.
- M4 explicitly becomes the point where OpenRaft's Multi-Raft suitability is proven rather than assumed.
- `raft-rs` remains the primary fallback if low-level control becomes necessary based on measurement.

## References

- OpenRaft docs: https://docs.rs/openraft/
- OpenRaft getting started/storage APIs: https://docs.rs/openraft/latest/openraft/docs/getting_started/
- OpenRaft linearizable reads: https://docs.rs/openraft/latest/openraft/docs/protocol/read/
- OpenRaft Multi-Raft adapter: https://docs.rs/openraft-multi/
- TiKV raft-rs: https://github.com/tikv/raft-rs
- raft-rs `RawNode`: https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html
