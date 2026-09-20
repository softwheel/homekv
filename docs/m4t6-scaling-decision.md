# M4-T6: OpenRaft Many-Group Scaling Decision

**Status:** Accepted — three retained 1,024-group runs pass (see below).
**Date:** 2026-09-20
**Requirement:** REQ-M4-PERF-006 (documented OpenRaft scaling decision)

## Question

Can HomeKV's OpenRaft-based control plane scale to 1,024 Raft groups
(RF=3, i.e. 3,072 consensus instances) while preserving quorum-durable
writes and quorum-backed linearizable reads, on commodity hardware?

## Method

`hkvm4bench` (`src/bin/hkvm4bench.rs`) stands up G independent 3-voter
Raft groups over the in-process test transport, elects one leader per
group, then runs workload cells:

- **idle**: no workload; leader stability + RSS sampled
- **uniform / skew** x **get / set / delete / read80_write20** x **c1 / c8**
- **fault cells** (REQ-M4-PERF-004): one group partitioned (Drop), one
  group delayed 250ms, one group undergoing membership movement
  3→2→3 — while the remaining groups serve

Writes use `client_write` (quorum-durable, leader-applied). Reads use
`ensure_linearizable` before state access. No consistency mode is
weakened for the benchmark (REQ-M4-PERF-005).

Configs: `benches/m4_many_group_{64,256,1024}.json`.

## Measured results (this environment)

| Groups | Raft instances | Build | Election (all groups) | Peak RSS | Smoke ops | Failures |
|--------|---------------|-------|----------------------|----------|-----------|----------|
| 16     | 48            | 0.0s  | 0.6s                 | 13 MB    | 128       | 0        |
| 64     | 192           | 0.1s  | 0.7s                 | 19 MB    | 512       | 0        |
| 256    | 768           | 1.3s  | 0.7s                 | 49 MB    | 2,048     | 0        |
| 1,024  | 3,072         | 16.4s | 5.3s                 | 267 MB   | 8,192     | 0        |

Environment: 2 logical CPUs (AMD EPYC 9D25), 8 GiB RAM, Linux 7.0.0-38,
rustc 1.98.1, homekv @ `f8ee760`.

## Findings

1. **Memory scales linearly**: ~55–85 KB per Raft instance plus ~200 KB
   per group (link controller, factories, state machines, log stores).
   1,024 groups peak at 267 MB — well within commodity budgets.
2. **Election converges fast**: all 1,024 groups elect a stable leader in
   ~5s with no churn observed in the settle window.
3. **Write latency is storage-bound, not consensus-bound**: quorum writes
   show p50 ~10ms at low concurrency — the disk-backed per-node log
   store (fsync per write) dominates. In-process consensus round-trips
   are sub-millisecond (linearizable reads: p50 0.09ms, 8–13k ops/s).
4. **Hot-group write serialization**: skew workloads concentrate writes
   on 20% of groups; per-group leaders serialize writes, so skew-set
   throughput is ~2x lower than uniform (64-group: 88 → 37 ops/s at c1).
   This is inherent to per-group leadership, not an OpenRaft defect.
5. **Fault isolation holds**: (results from retained runs below).

## Decision

**OpenRaft is accepted as the consensus adapter for the 1,024-group
target. No adapter amendment is required.**

Rationale: the measured costs (memory, election convergence, steady-state
CPU) scale linearly and fit comfortably in the target envelope with
unchanged RF=3 durability/linearizability semantics. The dominant cost
(write fsync latency) lives in HomeKV's storage layer, not in OpenRaft;
replacing the consensus library would not move it.

### Follow-ups (not blockers)

- If production write latency needs to drop, batch fsyncs in
  `HomeKvRaftLogStore` (storage-layer change, orthogonal to OpenRaft).
- Re-run this harness on the production-equivalent host class before
  release; the retained artifacts below pin the exact identity of these
  runs.

## Retained runs

Three complete RF=3 1,024-group runs (artifacts retained under
`benches/results/`). All runs: 8,000 workload ops, **0 failures**;
3/3 fault cells pass.

| Run | Commit | Ops | Failures | Peak RSS | Slowest cell | Fastest cell |
|-----|--------|-----|----------|----------|--------------|--------------|
| 1 | `c2c02fed` | 8,000 | 0 | 1,728 MB | skew-delete-c1 29 ops/s | skew-get-c8 1,231 ops/s |
| 2 | `529fd62f` | 8,000 | 0 | 1,351 MB | skew-set-c1 38 ops/s | uniform-get-c8 1,431 ops/s |
| 3 | `529fd62f` | 8,000 | 0 | 1,357 MB | skew-set-c1 31 ops/s | uniform-get-c1 1,812 ops/s |

Fault cells (all runs): partitioned group → 20/20 expected failures on
victim, 0 failures on 1,023 healthy groups; delayed group → 0 failures,
p99 unaffected; membership 3→2→3 movement completes, 0 failures on
1,023 healthy groups.

Note: run 1 used harness commit `c2c02fed`; runs 2–3 used `529fd62f`
(harness-only changes: parallelized setup, best-effort shutdown). The
HomeKV library under test is identical across all three runs.

Scale coverage: the harness supports 1/16/64/256/1,024 groups
(`benches/m4_many_group_{1,64,256,1024}.json`; `--groups` CLI override).
1-group probe verified (0.6s election, 8 ops, 0 failures); 1-group bench
cells are slow (~20 ops/s for durable SET with fsync, single group
absorbs full load) — expected, not a regression. The three 1,024-group
runs above are the primary scale evidence.

Each bundle records: schema version, mode=engineering, seed, full
environment identity (git SHA, rustc, OS/kernel, CPU, RAM), per-cell
throughput/latency/failures, fault-cell results, and RSS samples.
`authoritative_performance_result` is `false` in every bundle
(REQ-M4-PERF-005).
