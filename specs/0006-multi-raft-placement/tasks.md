# Spec 0006 — Multi-Raft Placement and Rebalancing Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #70
- Dependency: Spec 0005 Verified

Every semantic slice requires its own focused PR, requirement/task traceability, complete required CI, and review of exact tested head/base/merge identity. A later task may begin only after its dependency is merged. No task may weaken a Verified M0-M3 assertion.

## M4-S0 — Accept child spec

- Author/review `requirements.md`, `design.md`, `tasks.md`, and `verification.md` together.
- Confirm the parent T-0005 boundary and Spec 0005 residual handoff.
- Record Accepted state and tracker traceability.
- No semantic implementation.

Completion: all four documents are internally consistent, testable, Accepted, and merged with green repository gates.

## M4-T1 — Placement model and bootstrap

Requirements: `REQ-M4-MAP-*`, `REQ-M4-BAL-001/002`, `REQ-M4-OPS-001`

Deliver:

- catalog command/state/snapshot types;
- dedicated three-voter catalog-group bootstrap;
- immutable shard-map/cluster identity validation;
- deterministic RF=3 placement for all 1,024 shards;
- versioned catalog persistence/replay;
- reference-model, golden, skew, idempotence and corruption tests.

No data-group movement or background rebalancing in T1.

## M4-T2 — Bounded many-group runtime and shared transport

Depends on: T1

Requirements: `REQ-M4-BASE-*`, `REQ-M4-GROUP-*`, `REQ-M4-FAIL-001/003/004`

Deliver:

- bounded group registry and recover-before-serve lifecycle;
- shared configured runtime/worker ownership;
- group-aware multiplexed peer transport;
- per-group/per-peer/global admission and byte bounds;
- fairness and isolation tests;
- restart/recovery tests for multiple groups.

Preserve exact M3 storage/state-machine/read/write behavior per group.

## M4-T3 — Shard-aware routing

Depends on: T1, T2

Requirements: `REQ-M4-ROUTE-*`, `REQ-M4-MAP-001..003`

Deliver:

- committed catalog resolver behind the existing M2 adapter;
- key/shard/placement/member/leader validation;
- route-version and endpoint-hint response integration without wire change;
- cached/stale/divergent route tests;
- multi-shard concurrent linearizability histories;
- retry and wrong-owner safety evidence.

## M4-T4 — Membership and movement reconciler

Depends on: T1-T3

Requirements: `REQ-M4-MOVE-*`, `REQ-M4-FAIL-*`

Deliver:

- durable movement operations and phase machine;
- learner creation/catch-up;
- OpenRaft safe promotion/removal;
- optional leadership-transfer observation;
- epoch publication and safe cleanup;
- crash/restart/retry/cancel hooks at every phase;
- concurrent traffic and acknowledged-write preservation tests.

## M4-T5 — Rebalancing and failure recovery

Depends on: T4

Requirements: `REQ-M4-BAL-*`, `REQ-M4-FAIL-*`, `REQ-M4-OPS-*`

Deliver:

- deterministic equal-weight/failure-domain planner;
- bounded cluster/per-node movement scheduler;
- leader distribution;
- catalog failover/quorum-loss behavior;
- unavailable node and partial-plan recovery;
- stable topology/movement management surfaces.

## M4-T6 — Resource and many-group verification

Depends on: T1-T5

Requirements: `REQ-M4-GROUP-*`, `REQ-M4-OPS-*`, `REQ-M4-PERF-*`

Deliver:

- bounded-cardinality aggregate/per-group metrics;
- instrumentation for memory, tasks/timers/workers, queues, connections, movement and contention;
- 1/64/256/1,024 group harness;
- idle/uniform/skew and delayed/unavailable/moving-group cells;
- three complete RF=3 durable/linearizable runs;
- retained raw/summary artifacts and exact environment identity;
- documented OpenRaft scaling decision.

## M4-T7 — Verification handoff

Depends on: every prior task

Reconcile every requirement, task, test, benchmark and residual risk. Run the complete Rust suite, M3 RF=3 benchmark, M4 many-group benchmark, and all preserved M0-M2 gates on one exact candidate.

Promote Spec 0006 to Verified only when every mandatory matrix row is PASS and tested/merged identity is proven. Otherwise keep it Accepted, record the precise blocker and next action, and do not start M5.
