# Spec 0006 — Multi-Raft Placement and Rebalancing Verification

- Status: Verified (2026-09-20, identity `3033c18d07273ef63b3b656bbd51b43c9d822ea7`)
- Requirements: `requirements.md`
- Design: `design.md`
- Tasks: `tasks.md`
- Tracking issue: #70

## 1. Verification rule

M4 becomes Verified only when every mandatory row below is PASS on one exact implementation identity. Tests must inspect committed catalog state, committed OpenRaft membership, operation histories and persisted recovery state; timing-only success is insufficient.

All evidence inherits RF=3 quorum-durable writes plus leader apply and quorum-backed linearizable reads. M0-M3 gates remain mandatory and unchanged.

## 2. Required environment record

Retained evidence records:

- exact HomeKV commit, branch head/base and tested merge checkout;
- exact Rust/OpenRaft/Cargo.lock identities;
- OS/kernel, CPU, memory, filesystem and network topology;
- node/controller topology and failure-domain labels;
- catalog/data-group election, heartbeat and request timeouts;
- runtime worker and group-registry capacities;
- foreground, group-event, RPC, connection, snapshot and movement bounds;
- group counts, active-group/core matrix and connection-pool width;
- key/value sizes, keyspace, shard selection, concurrency and mixes;
- RF=3, quorum-durable + leader-apply writes, quorum-backed reads;
- raw artifact identity/digest and all test/benchmark commands.

## 3. Requirement-to-evidence matrix

| Requirement | Required evidence | State |
|---|---|---|
| `REQ-M4-BASE-001..004` | per-group M3 boundary tests + complete unchanged M0-M3 regression gates | pass (335/335 on `3033c18`) |
| `REQ-M4-MAP-001` | XXH3 golden/property tests across all 1,024 shards and restart | pass (`m4_mapping_catalog_matrix.rs`, 11 tests) |
| `REQ-M4-MAP-002..003` | real catalog-group quorum/write/read/failover and authoritative-state tests | pass (M4-T1 `m4_catalog_group.rs`) |
| `REQ-M4-MAP-004..006` | RF=3/domain/skew/bootstrap/replay/corruption model tests | pass (`m4_mapping_catalog_matrix.rs`; items 6/9 trace to `m4_catalog_group.rs`) |
| `REQ-M4-GROUP-001..006` | registry/runtime/transport bounds, recovery, fairness and isolation tests | pass (`m4_group_runtime_matrix.rs`, 10 tests) |
| `REQ-M4-ROUTE-001..005` | wrong-shard/member/leader, stale cached route and retry histories | pass (`m4_routing_histories.rs`, 12 tests) |
| `REQ-M4-MOVE-001..006` | phase, catch-up, membership, crash/retry/cancel and traffic histories | pass (`m4_movement_matrix.rs`, 31 tests; `m4_placement_node.rs` end-to-end) |
| `REQ-M4-BAL-001..005` | deterministic planner, skew, bounded execution, gossip and catalog-loss tests | pass (BAL-001 skew ≤ 1 on resulting plans; BAL-002..005 in `m4_failure_matrix.rs`) |
| `REQ-M4-FAIL-001..004` | per-group/catalog/node loss, restart and corruption matrix | pass (FAIL-001..003 in `m4_failure_matrix.rs`; FAIL-004 in `m4_data_group_corruption.rs`, 5 tests) |
| `REQ-M4-OPS-001..003` | stable topology/metrics/cardinality assertions during transitions | pass (`m4_observability_matrices.rs`, 11 tests; production `PlacementMetrics` incl. `node_timers`; honest scoped definitions in `docs/m4t7-verification-evidence.md`) |
| `REQ-M4-PERF-001..006` | retained complete many-group scaling bundles and adapter decision | pass (three 1,024-group runs re-executed on `3033c18`, 0 failures each; digests in `docs/m4t7-verification-evidence.md`) |

## 4. Mapping and catalog verification

Mandatory tests:

1. parent/M1 XXH3 golden vectors remain identical;
2. arbitrary byte keys always map within `0..1024`;
3. all 1,024 group IDs are unique and stable across restart;
4. deterministic bootstrap produces RF=3, no duplicate voter, permitted-domain spread, and voter/leader skew at most one;
5. repeated identical bootstrap is a no-op; conflicting identity/topology fails closed;
6. catalog writes/reads require normal OpenRaft authority;
7. catalog quorum loss cannot publish a placement;
8. catalog leader failover preserves committed epoch and pending movement;
9. snapshot + log replay reproduces the reference catalog;
10. corrupt/version-incompatible catalog state fails closed.

## 5. Group/runtime/transport verification

With small deterministic capacities and the full 1,024-group case, prove:

- registry refuses capacity + 1 without partial allocation;
- no group serves before coherent recovery;
- runtime OS-worker count stays at configured bound as groups grow;
- fixed node topology uses shared peer connections independent of group count;
- per-group/per-peer/global RPC and byte saturation returns explicit bounded failure/backpressure;
- snapshot and movement bounds remain enforced;
- cancellation releases permits;
- one hot/delayed/unavailable group does not starve independent healthy groups;
- group stop/restart/remove is idempotent;
- aggregate accounting returns to baseline after cleanup.

## 6. Routing and consistency histories

Mandatory histories cover:

- stale client routes to a former replica;
- correct shard but wrong member;
- correct member but stale/non-leader endpoint;
- route cache older/newer than locally committed catalog view;
- placement pending while old stable membership serves;
- membership committed before catalog publication;
- catalog publication and old-replica cleanup;
- concurrent GET/SET/DELETE/batch on multiple shards during route refresh;
- redirect/retry after uncertain transport completion.

Every completed per-shard history must pass the linearizability model and real-time order. No wrong-owner/stale leader may acknowledge or apply a strong operation. Cross-shard atomic behavior must not be advertised.

## 7. Movement phase matrix

For every phase (Intent, Learner, CatchUp, Promote, Lead, Remove, Publish, Cleanup), inject controller crash/restart and operation retry. Additionally cover:

- target unavailable before promotion;
- target snapshot corruption/truncation;
- source unavailable before and after promotion;
- catalog leader change;
- data-group leader change;
- cancellation before membership change;
- cancellation request after membership change;
- duplicate reconciler invocation;
- stale operation ID/epoch;
- concurrent foreground traffic;
- acknowledged write immediately before each fault;
- subsequent restart of source and target.

Expected invariants:

- at most one active transition per shard;
- no premature target authority;
- no destroyed still-authoritative replica;
- no lost acknowledged write;
- no stale strong read;
- committed membership is reconciled forward;
- final catalog placement equals data-group membership;
- all retained replicas converge.

## 8. Rebalancing and failure matrix

| Fault/scenario | Required invariant |
|---|---|
| one data replica unavailable | shard quorum progresses; other groups unaffected |
| one data group loses quorum | that shard rejects strong operations; other groups progress |
| catalog leader killed | catalog quorum elects replacement; one movement identity persists |
| catalog loses quorum | no new placement; stable data groups continue normally |
| node with many groups restarts | groups recover independently before service |
| planner/reconciler restart | deterministic plan/idempotent phase continuation |
| gossip divergence | no direct authority or membership change |
| movement target slow | configured move/snapshot bounds hold; foreground isolation measured |
| corrupt data/catalog artifact | affected authority fails closed |
| heal after partial transition | catalog and committed membership converge |

## 9. Observability verification

Assert stable HomeKV-owned representations during bootstrap, elections, route errors, saturation, movement and failures for:

- catalog epoch/leader/health;
- placements and desired/current leaders;
- group loaded/active/idle/error counts;
- per-group role/term/config/commit/apply/snapshot/lag;
- runtime workers and task/timer counts;
- connection and RPC counts/bytes/rejections;
- memory by group and aggregate;
- route redirects by cause;
- movement phase/bytes/duration/result;
- voter/leader skew;
- bounded metric cardinality and paginated per-shard inspection.

## 10. Many-group benchmark gate

Run at 1, 64, 256 and 1,024 data groups:

- idle characterization;
- uniform GET, SET, DELETE, 80/20;
- skewed GET, SET, DELETE, 80/20;
- low and moderate concurrency;
- one delayed/unavailable group;
- one bounded movement with catch-up.

At 1,024 groups retain three complete runs. Every required workload cell reports operations attempted/succeeded/failed, throughput/core, p50/p95/p99/p99.9, memory total/per group, task/future/timer counts, runtime workers, peer connections, active groups/core, queue/saturation metrics and scheduler contention.

Gate conditions:

1. zero unexpected operation failures in retained healthy cells;
2. all completed histories pass correctness checks;
3. configured memory/admission/byte/movement ceilings are respected;
4. connection count for fixed topology does not increase with group count;
5. all 1,024 groups recover and serve;
6. unrelated healthy groups progress during the fault/movement cells;
7. no unexplained resource, throughput or tail-latency collapse remains;
8. the OpenRaft suitability decision and any bottleneck are documented.

These results are engineering evidence only. No fixed public latency/throughput claim is created.

## 11. Regression and final handoff

Every implementation/verification PR runs the locked Rust build, complete tests, M3 RF=3 benchmark, and all three preserved M0 smoke gates. M4-T6/T7 additionally run the many-group benchmark and retain artifacts.

Final verification records the exact candidate head/base, workflow/run/jobs, artifact ID/digest, benchmark summary, requirement evidence, PR/workflow ledger, tested merge checkout, merged commit, tree/parent identity, and residual risks. Every mandatory row is PASS on the single exact implementation identity `3033c18d07273ef63b3b656bbd51b43c9d822ea7` (2026-09-20); Spec 0006 is Verified and M5 is unblocked.
