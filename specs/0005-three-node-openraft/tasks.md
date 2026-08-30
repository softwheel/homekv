# Spec 0005 — One-Shard Three-Node OpenRaft Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #38

Each task is a bounded implementation/review slice. No task may silently change Spec 0005 semantics; amend and re-accept the spec first.

## M3-S0 — Accept the child spec

Requirements: all

Completion criteria:

- requirements, design, tasks, and verification plan are reviewed together;
- OpenRaft version is pinned by the spec;
- durability acknowledgement and read semantics are falsifiable;
- M4/M5 boundaries are explicit;
- CI preserves the Verified M0/M1/M2 baseline;
- spec PR merges before any M3 semantic implementation.

## M3-T1 — OpenRaft types + deterministic state-machine adapter

Requirements: `REQ-M3-RAFT-001..003`, `REQ-M3-SM-001..005`

Prerequisite: M3-S0

Scope:

- add exact `openraft = "=0.9.25"` dependency;
- define HomeKV Raft type config, node metadata, deterministic command/response types;
- implement the one-shard `RaftStateMachine` application boundary against M1-compatible state;
- retain applied log identity and membership metadata;
- add deterministic command/application tests.

Must not include durable WAL, production Raft transport, cluster bootstrap, or serving replicated client traffic.

Completion criteria:

- all command variants deterministically apply in committed order;
- membership entries do not mutate KV state;
- repeated/recovery-oriented application tests establish stable logical state;
- existing M0/M1/M2 gates pass.

## M3-T2 — Durable Raft log/vote store

Requirements: `REQ-M3-DUR-001..007`

Prerequisite: M3-T1

Scope:

- implement versioned/checksummed log/vote persistence;
- implement `RaftLogReader`/`RaftLogStorage` for the pinned API;
- make append entries readable before append returns;
- fire `LogFlushed` only after durable flush;
- persist votes before `save_vote` returns;
- implement hole-free truncate/purge and recovery validation;
- retain committed-progress metadata if required by the chosen recovery construction.

Must not introduce group-commit optimization beyond a simple correctness-first serialized flush path.

Completion criteria:

- crash/reopen tests for votes/logs/truncate/purge;
- injected I/O failure never yields false flush success;
- checksum/truncation/version corruption fails closed;
- callback-ordering tests establish the accepted persistence boundary;
- existing gates pass.

## M3-T3 — Bounded three-node Raft transport and bootstrap

Requirements: `REQ-M3-MEM-001..004`, `REQ-M3-NET-001..004`, parts of `REQ-M3-WRITE-001..003`

Prerequisite: M3-T2

Scope:

- implement the OpenRaft network adapter/factory required by 0.9.25;
- bounded per-peer connection/outstanding RPC behavior;
- vote, append-entries/heartbeat, and snapshot RPC plumbing;
- deterministic three-voter bootstrap for one shard;
- test transport facade with link partition/drop/delay controls.

Must not integrate 1,024-shard placement or general online rebalancing.

Completion criteria:

- exactly one leader emerges in healthy three-node tests;
- repeated compatible bootstrap is safe/idempotent;
- incompatible cluster identity/membership bootstrap fails closed;
- link failures surface as Raft transport failures;
- queue/resource bounds are tested;
- existing gates pass.

## M3-T4 — Replicated writes + linearizable GET

Requirements: `REQ-M3-WRITE-001..005`, `REQ-M3-READ-001..004`, `REQ-M3-PERF-002`

Prerequisite: M3-T3

Scope:

- route SET/DELETE/BATCH through leader-authoritative `client_write`;
- hold external success until durable quorum commitment plus leader apply is established;
- implement GET through `ensure_linearizable()` or the accepted equivalent barrier and applied wait;
- translate not-leader/stale/unavailable/storage outcomes to the M2 result vocabulary;
- preserve admitted-work cancellation and retry semantics.

Must not enable lease reads or follower strong reads.

Completion criteria:

- three-node replicated CRUD/batch tests;
- follower/stale-leader requests do not apply locally;
- minority partition cannot acknowledge writes;
- GET history is checked against a linearizable model under concurrent writes;
- existing gates pass.

## M3-T5 — Snapshot, failover, restart and recovery

Requirements: `REQ-M3-SNAP-001..005`, `REQ-M3-FAIL-001..004`, remaining `REQ-M3-DUR-*`

Prerequisite: M3-T4

Scope:

- coherent versioned/checksummed snapshot build;
- bounded snapshot transfer/install;
- atomic state-machine replacement on validated install;
- restart from durable snapshot/log state;
- leader kill/isolation/rejoin scenarios;
- lagging follower catch-up via snapshot when needed.

Must not add M5 storage-layout optimizations.

Completion criteria:

- acknowledged writes survive leader loss/restart subject to quorum assumptions;
- healthy quorum elects a new leader and resumes;
- old isolated leader cannot acknowledge writes;
- corrupted/truncated/incompatible snapshot is rejected;
- recovered state equals committed model state;
- existing gates pass.

## M3-T6 — Observability, bounded admission, fault matrix and engineering benchmark

Requirements: `REQ-M3-OPS-001..003`, `REQ-M3-NET-002`, `REQ-M3-PERF-001..003`

Prerequisite: M3-T5

Scope:

- expose HomeKV-oriented role/leader/log/commit/apply/membership/snapshot/replication signals;
- metrics for reads/writes, errors, elections, RPCs, durable flush, apply, snapshots and queue/backpressure;
- bounded foreground replicated-operation admission;
- complete deterministic one-node/minority/quorum-loss/network fault matrix;
- retained three-run engineering benchmark under RF=3 durable/linearizable semantics.

Completion criteria:

- saturation is bounded and observable;
- metrics reflect tested state transitions without becoming a compatibility dependency on OpenRaft Rust types;
- benchmark retains host/toolchain/workload/latency/throughput/failure evidence;
- no benchmark result is presented as a release claim;
- existing gates pass.

## M3-T7 — Verification handoff

Requirements: all mandatory Spec 0005 requirements

Prerequisite: M3-T6

Scope:

- execute and retain the full requirement-to-evidence matrix in `verification.md`;
- review every mandatory requirement independently;
- confirm M0/M1/M2 regression gates still pass;
- record exact tested commit, workflow runs/artifacts, toolchain and known residual risks;
- change Spec 0005 state to Verified only if all mandatory gates pass.

Completion criteria:

- no unresolved mandatory requirement;
- verification record is reproducible and traceable;
- issue #38 is closed only after the verification PR merges;
- v1 tracker #7 marks M3 Verified and identifies M4 spec acceptance as the next SDD boundary.