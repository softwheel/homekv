# Spec 0001 — HomeKV v1 Task Plan

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #7

## Dependency graph

```text
0002 M0 baseline benchmark
        |
        v
0003 M1 shard-owned engine
        |
        v
0004 M2 data protocol
        |
        v
0005 M3 one replicated shard + WAL/read/recovery
        |
        v
0006 M4 Multi-Raft placement/rebalancing
        |
        v
0007 M5 persistence/snapshot hardening
        |
        v
0008 M6 correctness/fault verification
        |
        v
0009 M7 performance optimization
        |
        v
0010 M8 Rust/Zig experiments
        |
        v
0011 M9 published benchmark report
```

Each child spec is written and accepted before its implementation begins.

## T-0001 — M0 prototype baseline

- Child spec: `specs/0002-baseline-benchmark/`
- GitHub issue: #8
- Requirements: `REQ-PERF-005`, `REQ-PERF-006`, `REQ-SDD-001`

Deliver a reproducible benchmark harness and preserve the current COW/BTree prototype baseline before M1.

Completion requires Spec 0002 to reach `Verified`.

## T-0002 — M1 shard-owned memory engine

- Planned child spec: `0003-shard-owned-engine`
- GitHub issue: #9
- Requirements: `REQ-DATA-*`, `REQ-SHARD-001`, `REQ-PERF-002/003/004`, `REQ-OPS-003`, `REQ-SDD-002`
- Depends on: T-0001

Define and implement local logical shards, worker ownership, point storage, bounded dispatch/backpressure, explicit memory accounting, and single-shard atomic batches. No Raft in M1.

## T-0003 — M2 native data plane

- Planned child spec: `0004-data-plane`
- Requirements: `REQ-DATA-*`, `REQ-SHARD-003`, `REQ-RETRY-*`, `REQ-OPS-003`, `REQ-PERF-*`, `REQ-SDD-003`
- Depends on: T-0002

Specify and implement framing, pipelining, request IDs, redirects, bounds, backpressure, and error semantics. Keep control/admin APIs on gRPC where useful.

## T-0004 — M3 one correct replicated shard

- Planned child spec: `0005-replicated-shard`
- Requirements: `REQ-RAFT-*`, `REQ-CONS-*`, `REQ-DUR-*`, `REQ-FAIL-001/002/003/005`, `REQ-OPS-001`, `REQ-SDD-004`
- Depends on: T-0002; M2 protocol may be integrated but is not a safety prerequisite

Integrate TiKV `raft-rs` for a three-replica shard. Implement persistent Ready handling, durable WAL boundary, safe ReadIndex, deterministic apply, leader failover, basic snapshot/recovery, and fault tests sufficient to prove one group.

## T-0005 — M4 Multi-Raft placement and rebalancing

- Planned child spec: `0006-multi-raft-placement`
- Requirements: `REQ-SHARD-*`, `REQ-FAIL-004`, `REQ-OPS-001`, `REQ-SDD-005`
- Depends on: T-0004

Scale the proven M3 mechanics to the 1,024-shard space: placement metadata, many-group scheduling, stale-route handling, membership changes, shard movement, leader distribution and rebalancing.

## T-0006 — M5 persistence/snapshot hardening

- Planned child spec: `0007-persistence-hardening`
- Requirements: `REQ-DUR-*`, `REQ-FAIL-005`
- Depends on: T-0004 and T-0005 where multi-group behavior matters

Harden segmented WAL, group commit, snapshot generation/install, log truncation, corruption detection, recovery time and operational tooling. M3 includes only the persistence needed to prove one replicated group correctly.

## T-0007 — M6 distributed correctness verification

- Planned child spec: `0008-correctness-faults`
- Requirements: all `REQ-CONS-*`, `REQ-DUR-*`, `REQ-FAIL-*`
- Depends on: T-0004/T-0005/T-0006

Build generated concurrent histories, linearizability checking, network/process fault injection, restart/corruption testing and long-running randomized verification.

## T-0008 — M7 performance optimization campaign

- Planned child spec: `0009-performance`
- Requirements: `REQ-PERF-*`
- Depends on: a verified correctness baseline

Profile first. Optimize identified bottlenecks in batching, queues, allocation, CPU/NUMA locality, network I/O, Raft scheduling, WAL, snapshots and routing. Significant architecture/semantic changes require an amendment or child spec.

## T-0009 — M8 Rust vs Zig controlled experiments

- Planned child spec: `0010-rust-zig-experiments`
- Requirement: `REQ-LANG-001`
- Depends on: T-0008 profiling evidence

Implement equivalent isolated hotspots in Rust/Zig and decide using p99, cycles/op, throughput/core, memory and integration/maintenance cost.

## T-0010 — M9 publish reproducible results

- Planned child spec: `0011-published-benchmarks`
- Requirements: `REQ-PERF-005/006`, `REQ-BENCH-AUTH-*`
- Depends on: verified system and stable benchmark setup

Freeze benchmark hardware/network, publish configs/semantics/toolchain/raw artifacts, and compare systems only under explicitly documented consistency and durability modes.

## Task policy

A task may move to implementation only when:

1. its child spec is `Accepted`;
2. dependencies are `Verified` or explicitly waived by an accepted amendment;
3. acceptance/verification criteria are testable;
4. the implementation PR links the spec and requirement/task IDs.
