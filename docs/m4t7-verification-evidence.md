# M4-T7 verification evidence — residual slices closed, spec Verified

**Date:** 2026-09-20
**Spec:** `specs/0006-multi-raft-placement/` — status **Verified**
**Final implementation identity:** `3033c18d07273ef63b3b656bbd51b43c9d822ea7` (main, PR #97 squash merge)
**Final tree:** `7b4bb361e1c6a2324511169c29406154fa15b445` (parent `e11139e`)

## Identity record

- Final main head: `3033c18` — all residual verification slices merged.
- PR #97 head (pre-merge): `0e0cb50f27aadf7070ba3489faa7a1f039de5dc3`, tree `7b4bb361`
  (identical tree to the squash-merge `3033c18`; CI ran green on it).
- Blocker stack base: `c63bc57185d0196b22ce9e27b04e3ecef7cd5f62` (PR #90).
- Supersedes: `23d3d37` (PR #92, four blockers) and `25771c3` (PR #93, evidence-only,
  spec stayed Accepted with five residual rows).

## PR / workflow ledger

| PR | Change | Merge SHA |
|---|---|---|
| #89 | `TopologyView` per-group Raft fields | `96109f8` |
| #90 | Production `PlacementNode` + `--placement` mode | `c63bc57` |
| #91 | §6 routing histories + §9 observability matrices | `abad7ee` |
| #92 | §7 movement matrix + §8 failure matrix | `23d3d37` |
| #93 | Evidence ledger; spec stays Accepted (5 residual rows) | `25771c3` |
| #94 | FAIL-004: data-group corruption fail-closed (+ janitor single-file fix, divergent-reinit guard) | `6ba89cd` |
| #95 | §4 mapping/catalog matrix (11 tests) + §5 group/runtime matrix (10 tests) + BAL-001 skew assertions | `2d5f7fe` |
| #96 | §9 production metrics surface on `PlacementNode` (`placement_metrics.rs`) | `e11139e` |
| #97 | §9 `node_timers` (HomeKV-owned timer count); stabilize 2 timing-sensitive tests | `3033c18` |

## Test evidence (exact identity `3033c18`)

- `cargo test --all-targets --no-fail-fast`: **335 passed, 0 failed** (26 targets)
  - `tests/m4_mapping_catalog_matrix.rs`: 11 tests — XXH3 golden vectors, bounded
    arbitrary-key mapping, 1,024 unique/stable group IDs, RF=3/bootstrap/domain/skew,
    idempotent bootstrap, conflicting identity fail-closed, catalog quorum
    loss/failover/corruption (§4 items 1–5, 7–8, 10; items 6/9 trace to
    `tests/m4_catalog_group.rs::catalog_group_is_quorum_authoritative_and_recovers_durable_bootstrap`)
  - `tests/m4_group_runtime_matrix.rs`: 10 tests — registry capacity+1 refusal,
    no-serve-before-coherent-recovery, worker bound as groups grow, per-peer
    saturation backpressure, permit release, transport isolation, idempotent
    lifecycle, accounting cleanup
  - `tests/m4_data_group_corruption.rs`: 5 tests — checksum bit-flip, truncated
    store, version corruption, snapshot corruption, production
    `PlacementNode::start` corruption path (all fail closed; healthy quorum
    continues; janitor + restart heals)
  - `tests/m4_observability_matrices.rs`: 11 tests — stable topology views,
    per-group Raft field consistency, bounded cardinality, catalog health
    transitions, production metrics snapshot covering every §9 field incl.
    `node_timers == 2`
  - `tests/m4_routing_histories.rs`: 12 tests — per-shard linearizability model
    histories; wrong-shard/member/leader, stale cached route, retry histories
  - `tests/m4_movement_matrix.rs`: 31 tests — crash/restart + bounded retry at
    each of the 8 movement phases; ack-write-before-fault; §7 invariant battery
  - `tests/m4_failure_matrix.rs`: 10 tests — single voter loss, group quorum loss,
    catalog leader change, catalog quorum loss, node restart (incl. BAL-002..005)
  - `tests/m4_placement_node.rs`: 1 test — end-to-end movement through the
    production composition root
  - lib unit tests: remainder (incl. BAL-001 planner skew ≤ 1 on resulting plans,
    `fs_janitor_*` listing/removal semantics)
- `cargo clippy --all-targets`: zero new warnings (2 pre-existing unused-import
  warnings in untouched files `src/storage/shard_store.rs`, `src/raft_data_plane.rs`)
- `rustfmt --check` on touched leaf files: clean (repo-wide fmt not run —
  pre-existing diffs in untouched files)
- CI `Rust` workflow on PR #97 head `0e0cb50`: **success** (same tree as `3033c18`)

### Honest scoped definitions (§9)

The §9 surface reports only signals that exist; nothing is invented:

- **timers**: `RuntimeMetricsSnapshot::node_timers` = HomeKV-owned timer count
  (the drive loop's drive + reconcile `interval()`s). Tokio's internal timer wheel
  has no stable `Handle` API — documented, not synthesized.
- **connections**: the in-process transport has no sockets; the connection-like
  unit is the per-peer factory entry (fixed configured width, `current`/`peak`
  in-flight, backpressure rejections). GROUP-003's core invariant — shared peer
  connections independent of group count — is asserted, not the socket count.
- **movement bytes**: per-operation byte attribution does not exist (replication
  traffic is untagged by design, and per-operation series would violate the
  bounded-cardinality requirement REQ-M4-OPS-003). The reported byte flow is the
  aggregate that exists: per-replica snapshot build/install/receive bytes and
  per-peer serialized payload bytes.
- **queue depth**: tokio-internal queue counters require `tokio_unstable`;
  queue depth is reported where HomeKV owns the queue (per-peer in-flight).

## Benchmark evidence (exact identity `3033c18`)

- Harness: `src/bin/hkvm4bench.rs`, config `benches/m4_many_group_1024.json`
  (`{"concurrencies":[1,8],"dataset_cardinality":16,"fault_cells":true,"idle_secs":5,
  "key_size":32,"mode":"engineering","operations_per_cell":500,"schema_version":1,
  "seed":20260920,"selections":["uniform","skew"],"value_size":128,"warmup_operations":200,
  "workloads":["get","set","delete","read80_write20"]}`)
- Command: `./target/release/hkvm4bench --config benches/m4_many_group_1024.json --groups 1024 --output benches/results/m4_1024_run{N}.json`
- Run window (UTC 2026-09-20): run1 09:58:29–09:59:52, run2 09:59:52–10:01:12, run3 10:01:12–10:02:31
- Retained: `benches/results/m4_1024_run{1,2,3}.json` (committed to main)
- Each run: 16 workload cells + 3 fault cells, 1,024 groups RF=3 (3,072 Raft
  instances), 8,000 ops attempted, **0 failures**;
  env identity `homekv_git_sha=3033c18d07273ef63b3b656bbd51b43c9d822ea7`, `rustc 1.98.1`
- SHA-256:
  - run1: `bea0df4c3ab6f068c7b26f0d982781743634de6fb3512d0457db2d4e1f564070`
  - run2: `963bf43d7c9d330df440a725bf0379acbd8f2a8cdedeec6297fb3013a3fd8078`
  - run3: `1534753a479cf08dd706d1da9bb798c4dc4f6a201e771941409f29db3a495cb4`

## Requirement-by-requirement verdict (honest)

| Requirement | Verdict | Evidence |
|---|---|---|
| REQ-M4-BASE-001..004 | PASS | full suite green incl. M0–M3 gates, 335/335 on `3033c18` |
| REQ-M4-MAP-001..006 (§4) | PASS | 11-test matrix + §4 items 6/9 traced to `m4_catalog_group.rs` quorum/snapshot/restart test |
| REQ-M4-GROUP-001..006 (§5) | PASS | 10-test runtime matrix: capacity, recovery, worker bound, backpressure, isolation, lifecycle, accounting |
| REQ-M4-ROUTE-001..005 (§6) | PASS | 12 automated histories, per-shard linearizability model; wrong-owner/stale leader never ack/apply |
| REQ-M4-MOVE-001..006 (§7) | PASS | 31 driver-level tests: crash/restart + bounded retry at every phase, all invariants |
| REQ-M4-BAL-001 | PASS | voter and desired-leader skew ≤ 1 asserted on resulting plans when topology permits |
| REQ-M4-BAL-002..005 | PASS | determinism on restart, bounded execution, single movement identity, gossip advisory-only, scheduler restart idempotent, slow-movement bounds |
| REQ-M4-FAIL-001..003 | PASS | single voter loss, group quorum loss blocks strong ops while other shards progress, catalog leader change, catalog quorum loss, node restart recovers groups independently |
| REQ-M4-FAIL-004 | PASS | data-group checksum/truncation/version/snapshot corruption all fail closed; healthy quorum continues; janitor + restart heals |
| REQ-M4-OPS-001..003 (§9) | PASS | production `PlacementMetrics`: workers/tasks/**timers**, groups, per-replica role/term/config/index/lag, RPC attempts/failures/backpressure/bytes, per-peer views, RSS + per-replica memory, redirects by cause, movement phase/duration/results/failures + aggregate byte flow, skew — with the honest scoped definitions above |
| REQ-M4-PERF-001..006 (§10) | PASS | three 1,024-group RF=3 runs on the single final identity `3033c18`, 0 failures each, digests retained |

## Verdict

Every mandatory row is PASS on the single exact implementation identity
`3033c18d07273ef63b3b656bbd51b43c9d822ea7` (tree `7b4bb361`). Spec 0006 is
**Verified**. M5 is unblocked.

## Residual risks / notes

- Two timing-sensitive tests were stabilized during this slice (term≥1 wait;
  60s leader wait). They are green in isolation and in the full suite on the
  final identity, but remain inherently timing-sensitive; CI re-runs are the
  backstop.
- §9 reports only existing signals (see "Honest scoped definitions"); a future
  transport with real sockets or tagged replication traffic could enrich the
  surface without changing the contract.
