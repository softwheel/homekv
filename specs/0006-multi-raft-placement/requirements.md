# Spec 0006 — Multi-Raft Placement and Rebalancing Requirements

- Status: Accepted
- Parent: `specs/0001-homekv-v1/requirements.md`
- Depends on: Verified Spec 0005
- Tracking issue: #70

## 1. Purpose

M4 scales the Verified M3 one-shard, three-voter OpenRaft contract to the fixed 1,024-shard HomeKV v1 architecture. It adds authoritative placement, bounded many-group execution, shard-aware routing, safe membership changes, movement/rebalancing, and an explicit OpenRaft scaling gate.

M4 MUST preserve the Verified M0-M3 consistency, quorum-durability, failure, resource-bound, protocol-compatibility, and benchmark-comparability contracts. Performance work MUST NOT weaken those semantics.

## 2. Scope and inherited contract

M4 covers exactly 1,024 data shards, each represented by an independent RF=3 OpenRaft group, plus one RF=3 system placement-catalog group. It covers deterministic bootstrap, node/group lifecycle, route resolution, movement and equal-weight balancing, aggregate operations, and many-group measurement.

M4 excludes online shard-count/hash changes, cross-shard transactions, heterogeneous capacity weights, automatic failure-detector-driven membership mutation, segmented/group-commit persistence optimization, lease/follower strong reads, public comparative claims, TLS/authentication policy, and replacing OpenRaft without a separately Accepted amendment.

**REQ-M4-BASE-001** — M4 MUST reuse the exact Verified M3 write acknowledgement boundary: quorum-durable Raft persistence plus current-leader local apply before success.

**REQ-M4-BASE-002** — M4 default GET MUST remain quorum-barrier-backed and linearizable per shard. Lease and follower strong-read shortcuts remain forbidden.

**REQ-M4-BASE-003** — Each data group MUST retain the Verified M3 log, state-machine, snapshot, recovery, corruption, and fail-closed behavior.

**REQ-M4-BASE-004** — The M2 compact protocol framing/status compatibility surface and all M0-M3 regression gates MUST remain intact.

## 3. Shard identity and placement authority

**REQ-M4-MAP-001** — The data shard space MUST contain exactly 1,024 shard IDs `0..1024`; `shard_id = XXH3_64(raw_key_bytes) & 1023`. Node membership MUST NOT alter this mapping.

**REQ-M4-MAP-002** — The system placement catalog MUST be an independent three-voter OpenRaft group using the same durable acknowledgement and safe-read contract as M3. Gossip, local health views, cached routes, and leader hints MUST NOT mutate or supersede catalog authority.

**REQ-M4-MAP-003** — A committed catalog snapshot MUST identify cluster identity, global monotonically increasing placement epoch, and for every shard: group identity, committed replica membership, desired leader hint, and any pending movement operation.

**REQ-M4-MAP-004** — Every committed stable data-shard placement MUST contain exactly three distinct voter node IDs and no duplicate failure-domain identity when three eligible domains are configured.

**REQ-M4-MAP-005** — Cluster bootstrap MUST deterministically create all 1,024 data-group identities and balanced RF=3 placements from a validated eligible-node set. Repeating the same bootstrap is idempotent; conflicting cluster identity or topology fails closed.

**REQ-M4-MAP-006** — Placement catalog snapshots and replay MUST be versioned and integrity checked. Recovery MUST expose no placement state newer than committed catalog state.

## 4. Many-group lifecycle and bounds

**REQ-M4-GROUP-001** — A node MUST maintain a bounded registry admitting at most the configured data-group capacity plus the catalog group. Unknown or excess groups MUST fail explicitly before unbounded allocation.

**REQ-M4-GROUP-002** — Groups MUST share a bounded async runtime/worker pool. M4 MUST NOT allocate a dedicated OS thread per group.

**REQ-M4-GROUP-003** — Raft traffic for different groups between the same node pair MUST use shared bounded transport/connection management. Connection growth MUST be a function of peers/configured pool width, not shard count.

**REQ-M4-GROUP-004** — Foreground operations, per-peer RPCs, group events, snapshot reception, snapshot transfer, and concurrent movement work MUST each have explicit configurable count and/or byte bounds with observable overload/rejection.

**REQ-M4-GROUP-005** — Activation, shutdown, restart, snapshot/log recovery, and removal MUST be idempotent per group and MUST NOT expose a partially recovered group for service.

**REQ-M4-GROUP-006** — A hot or unavailable group MUST NOT create an unbounded queue or prevent unrelated healthy groups from making progress.

## 5. Routing and authority

**REQ-M4-ROUTE-001** — The ingress path MUST recompute shard identity from every raw key, validate the supplied M2 `shard_id`, and resolve the current committed placement before dispatch.

**REQ-M4-ROUTE-002** — A node not in the committed placement, or not currently authoritative leader for the requested strong operation, MUST return the Verified M2 `STALE_ROUTE_OR_NOT_OWNER` result with the best known committed placement epoch and optional endpoint/leader hint. It MUST NOT apply or acknowledge the operation locally.

**REQ-M4-ROUTE-003** — Route versions and endpoint/leader hints are cache hints only. Receiving a newer-looking uncommitted hint MUST NOT grant write or read authority.

**REQ-M4-ROUTE-004** — During a placement transition, exactly the membership committed by the data group controls consensus authority. Catalog pending intent MUST NOT make the target writable before Raft membership admits it.

**REQ-M4-ROUTE-005** — PUT, DELETE, and deterministic single-shard batches remain retry-safe idempotent commands. M4 MUST NOT claim general exactly-once or cross-shard atomic behavior.

## 6. Membership change and movement

**REQ-M4-MOVE-001** — Each shard movement MUST have a durable unique operation ID, source/final membership, catalog epoch, phase, and retry state. At most one membership-changing operation may be active per shard.

**REQ-M4-MOVE-002** — Movement MUST follow: commit pending catalog intent; add target as learner/non-voter; catch it up from valid snapshot/log state; use OpenRaft's safe membership API to commit the voter transition; optionally transfer leadership; remove the old replica through safe membership transition; then atomically publish the new stable catalog placement and higher epoch.

**REQ-M4-MOVE-003** — A target MUST NOT become a voter until its data group proves the catch-up precondition required by the pinned OpenRaft API. An old replica MUST NOT be destroyed until removal is committed and cleanup is safe.

**REQ-M4-MOVE-004** — Retrying after controller/process crash at every phase MUST reconcile catalog intent with the data group's committed membership and converge idempotently. Ambiguity MUST pause/fail closed, never invent authority.

**REQ-M4-MOVE-005** — A failed or cancelled pre-membership movement MUST leave the old stable placement authoritative. Cancellation after a committed membership change MUST finish reconciliation rather than pretending the old placement remains authoritative.

**REQ-M4-MOVE-006** — Acknowledged writes and linearizable reads MUST remain safe under concurrent traffic throughout learner catch-up, membership transition, leadership transfer, removal, crash, restart, and retry.

## 7. Placement and rebalancing policy

**REQ-M4-BAL-001** — The M4 policy is equal-weight and failure-domain-aware. Stable voter counts across eligible nodes and stable desired-leader counts MUST differ by at most one when topology permits.

**REQ-M4-BAL-002** — Rebalancing plans MUST be deterministic for the same committed catalog/topology, avoid unnecessary moves, and never schedule two replicas of one shard onto one node.

**REQ-M4-BAL-003** — Rebalancing execution MUST enforce configured cluster-wide, per-node, and snapshot-byte concurrency limits and expose queued/running/blocked/failed/completed counts.

**REQ-M4-BAL-004** — Gossip/failure detection MAY propose or prioritize a plan but MUST NOT directly change placement, Raft membership, or leadership authority.

**REQ-M4-BAL-005** — Loss of catalog quorum MUST block new placement mutations while allowing existing data groups to continue only under their last committed membership and normal per-group quorum rules.

## 8. Failure behavior

**REQ-M4-FAIL-001** — One unavailable replica in any data group MUST preserve progress through the remaining quorum; loss of that group's quorum MUST fail strong operations without stale fallback and MUST NOT block unrelated healthy groups.

**REQ-M4-FAIL-002** — Catalog leader failure with catalog quorum intact MUST permit safe control-plane failover without duplicate/conflicting movement.

**REQ-M4-FAIL-003** — Node restart MUST recover each hosted group from valid snapshot/log state before admission and reconcile catalog/membership state without applying speculative data.

**REQ-M4-FAIL-004** — Corrupt placement, data-group log, or snapshot state MUST fail closed for the affected authority; it MUST NOT be repaired by accepting cached client or gossip claims.

## 9. Observability

**REQ-M4-OPS-001** — HomeKV MUST expose a stable serializable topology view containing catalog epoch/health, all shard placements, per-group leader/term/config, commit/apply/snapshot progress, and replica health without exposing OpenRaft Rust types as the contract.

**REQ-M4-OPS-002** — Metrics MUST cover loaded/active/idle groups, runtime tasks/timers/workers, peer connections, queue depth/rejection, memory by group and aggregate, routing redirects, movement phase/bytes/duration/failures, placement skew, leader skew, and existing M3 metrics.

**REQ-M4-OPS-003** — Metrics cardinality MUST be bounded/configurable. Required aggregate health MUST remain available even if per-shard labels are limited.

## 10. Scaling and benchmark requirements

**REQ-M4-PERF-001** — The many-group harness MUST exercise 1, 64, 256, and 1,024 data groups under idle, uniform, and skewed activity and report memory/group and total memory; tasks/futures/timers/group; runtime workers; peer connections; active groups/core; throughput/core; p50/p95/p99/p99.9; failures; and scheduler contention.

**REQ-M4-PERF-002** — The 1,024-group gate MUST use RF=3, quorum-durable writes, quorum-backed linearizable reads, exact toolchain/dependency lock, and documented hardware/topology. Every retained run MUST complete with zero unexpected operation failures.

**REQ-M4-PERF-003** — At least three complete runs MUST cover GET, SET, DELETE, and 80/20 at low and moderate concurrency under uniform and skewed shard selection. Tail latency and throughput MUST be reported together.

**REQ-M4-PERF-004** — The harness MUST prove shared connection count does not grow with group count for a fixed node topology, configured resource ceilings are not exceeded, all 1,024 groups can recover/serve, and unrelated groups make progress while one group is delayed/unavailable or moving.

**REQ-M4-PERF-005** — M4 results are engineering evidence, not public release claims. Comparisons MUST preserve consistency/durability/RF semantics and MUST NOT use a faster weakened mode.

**REQ-M4-PERF-006** — If the accepted gate cannot pass after reasonable documented integration optimization, implementation MUST stop and open an Accepted consensus-adapter amendment before replacing OpenRaft or weakening any requirement.

## 11. Acceptance

Spec 0006 is Verified only after every mandatory verification row is PASS on one exact code identity, the retained correctness/failure/reconfiguration evidence and three complete many-group benchmark runs are archived, and all required Rust, RF=3, and preserved M0-M3 gates pass unchanged.
