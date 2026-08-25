# Spec 0001 — HomeKV v1 Task Plan

- Status: Draft
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #7

This file is a decomposition plan, not permission to implement while Spec 0001 remains Draft.

## Dependency graph

```text
0002 M0 benchmark baseline
        |
        v
0003 M1 shard-owned engine
        |
        +----------> 0004 M2 data protocol
        |
        v
0005 M3 replicated shard
        |
        +----------> 0006 M5 persistence/recovery
        |
        v
0007 M4 multi-shard placement/rebalancing
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

Numbers after 0001 are reserved here as a planning convention; each child spec is created/reviewed before implementation begins.

## T-0001 — Establish benchmark baseline

- Child spec: `specs/0002-baseline-benchmark/`
- GitHub issue: #8
- Requirements: `REQ-PERF-005`, `REQ-PERF-006`

Deliver a reproducible benchmark harness and capture the current prototype baseline before changing the storage engine.

Completion requires the child spec to reach Verified.

## T-0002 — Replace whole-store COW with shard-owned engine

- Planned child spec: 0003
- GitHub issue: #9
- Requirements: `REQ-DATA-*`, `REQ-SHARD-001`, `REQ-PERF-002`, `REQ-PERF-003`, `REQ-PERF-004`, `REQ-OPS-003`
- Depends on: T-0001

Define and implement stable logical shards, worker ownership, point storage, bounded dispatch, and single-shard atomic batches.

## T-0003 — Define and implement native data-plane protocol

- Planned child spec: 0004
- Requirements: `REQ-DATA-*`, `REQ-SHARD-003`, `REQ-OPS-003`, `REQ-PERF-*`
- Depends on: T-0002

Specify framing, pipelining, request IDs, redirects, bounds, backpressure, and error semantics before replacing the hot-path transport.

## T-0004 — Implement correct replicated shard

- Planned child spec: 0005
- Requirements: `REQ-CONS-*`, `REQ-FAIL-001/002/003`, `REQ-OPS-001`
- Depends on: T-0002

Select/integrate a consensus implementation and deliver a three-replica shard with linearizable mutation ordering and a safe read path.

## T-0005 — Add WAL, group commit, snapshot, recovery

- Planned child spec: 0006
- Requirements: `REQ-DUR-*`, `REQ-FAIL-005`
- Depends on: T-0004

Define exact acknowledgement semantics and persistence behavior before implementation.

## T-0006 — Multi-shard cluster placement and rebalancing

- Planned child spec: 0007
- Requirements: `REQ-SHARD-*`, `REQ-FAIL-004`, `REQ-OPS-001`
- Depends on: T-0004

Introduce placement metadata, stale-route handling, membership changes, shard movement, and leader distribution.

## T-0007 — Distributed correctness verification

- Planned child spec: 0008
- Requirements: all `REQ-CONS-*`, `REQ-DUR-*`, `REQ-FAIL-*`
- Depends on: T-0004, T-0005, T-0006

Build generated concurrent histories, linearizability checking, network/process fault injection, and recovery verification.

## T-0008 — Performance optimization campaign

- Planned child spec: 0009
- Requirements: `REQ-PERF-*`
- Depends on: verified correctness baseline

Profile first; optimize identified bottlenecks in batching, queues, allocation, CPU locality, network I/O, WAL, and snapshot paths. Each significant optimization should have its own small spec/ADR when it changes architecture or semantics.

## T-0009 — Rust vs. Zig controlled experiments

- Planned child spec: 0010
- Requirement: `REQ-LANG-001`
- Depends on: T-0008 profiling evidence

Implement equivalent isolated hot components in Rust/Zig and decide based on controlled results plus maintenance/integration cost.

## T-0010 — Publish reproducible results

- Planned child spec: 0011
- Requirements: `REQ-PERF-005`, `REQ-PERF-006`
- Depends on: verified system and stable benchmark setup

Publish workload configs, semantics, hardware/toolchain, raw artifacts, and trade-off analysis. Do not claim semantic equivalence where comparison systems run weaker durability or consistency modes.

## Task policy

A task may move to implementation only when:

1. its child spec is `Accepted`;
2. dependencies are satisfied or explicitly waived by an accepted spec amendment;
3. acceptance/verification criteria are testable;
4. the implementation PR links the spec and requirement/task IDs.
