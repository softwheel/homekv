# M4-T7 verification evidence — post-blocker reconciliation

**Date:** 2026-09-20
**Spec:** `specs/0006-multi-raft-placement/` — status **Accepted** (not Verified)
**Candidate identity:** `23d3d37b1c47b06a16b4c9ca4bf73797a224d169` (main, PR #92 squash merge)

## Identity record

- Final main head: `23d3d37` (all four blockers merged: #89 observability, #90 production wiring, #91 §6+§9 matrices, #92 §7+§8 matrices)
- Base of blocker stack: `c63bc57185d0196b22ce9e27b04e3ecef7cd5f62` (PR #90)
- Previous reconciliation head: `e7950ae` (M4-T7, PR #88)

## PR / workflow ledger

| PR | Change | Merge SHA |
|---|---|---|
| #89 | `TopologyView` per-group Raft fields (blocker 2) | `96109f8` |
| #90 | Production `PlacementNode` + `--placement` mode (blocker 3) | `c63bc57` |
| #91 | §6 routing histories + §9 observability matrices (blocker 4a) | `abad7ee` |
| #92 | §7 movement matrix + §8 failure matrix + placement-node leadership-wait fix (blocker 4b) | `23d3d37` |

## Test evidence (exact identity `23d3d37`)

- `cargo test --all-targets`: **302 passed, 0 failed**
  - `tests/m4_movement_matrix.rs`: 31 tests (crash/restart + bounded retry at each of the 8 movement phases; ack-write-before-fault; §7 invariant battery per test)
  - `tests/m4_failure_matrix.rs`: 10 tests (3 real `PlacementNode` clusters + 7 model-level)
  - `tests/m4_routing_histories.rs`: 12 tests (per-shard linearizability model histories)
  - `tests/m4_observability_matrices.rs`: 6 tests (stable topology views; per-group Raft field consistency; bounded cardinality)
  - `tests/m4_placement_node.rs`: 1 test (end-to-end movement through the production composition root)
- `cargo clippy --all-targets`: zero warnings in touched files
- `rustfmt --check` on touched leaf files: clean
- CI `Rust` workflow run `35496156736` on main `23d3d37`: **success** (attempt 3; attempt 1 failed at `Install Protoc` — infra; attempt 2 failed at `Run tests` — the pre-existing `data_plane_adapter` timing flake; local full suite green 302/302 on the same identity, so attempt 3 rerun passed)

## Benchmark evidence (exact identity `23d3d37`)

- Harness: `src/bin/hkvm4bench.rs`, config `benches/m4_many_group_1024.json`
  (`{"concurrencies":[1,8],"dataset_cardinality":16,"fault_cells":true,"idle_secs":5,
  "key_size":32,"mode":"engineering","operations_per_cell":500,"schema_version":1,
  "seed":20260920,"selections":["uniform","skew"],"value_size":128,"warmup_operations":200,
  "workloads":["get","set","delete","read80_write20"]}`)
- Command: `./target/release/hkvm4bench --config benches/m4_many_group_1024.json --groups 1024 --output benches/results/m4_1024_run{N}.json`
- Run window (UTC): run1 07:16:02–07:17:23, run2 07:17:23–07:18:40, run3 07:18:40–07:19:58 (2026-09-20)
- Retained: `benches/results/m4_1024_run{1,2,3}.json` (committed to main)
- Each run: 16 workload cells + 3 fault cells, 8,000 ops attempted, **0 failures**; env identity `homekv_git_sha=23d3d37b1c47`, `rustc 1.98.1`
- SHA-256:
  - run1: `28d39be22c966dc2768716d40c77572a3a9c554bb9a0958fe1455995807f332c`
  - run2: `93c374742b55f5db36f445d46ec7df83c1caad737d3bc377302660dded371e3a`
  - run3: `d38132bc8702dd29f07c99dee36e074669dca7fdf6100a052ed2d19ba0916c0d`

## Requirement-by-requirement verdict (honest)

| Requirement | Verdict | Evidence / residual gap |
|---|---|---|
| REQ-M4-BASE-001..004 | PASS | full suite green incl. M0–M3 gates, 302/302 |
| REQ-M4-MAP-001..006 (§4) | **PARTIAL** | catalog consensus, mapping, bootstrap bounds covered; the §4 golden-vector/restart/conflicting-identity/corruption automated matrix is still not implemented |
| REQ-M4-GROUP-001..006 (§5) | **PARTIAL** | capacity+1 refusal, no-serve-before-recovery, runtime worker bound as groups grow, per-peer saturation backpressure, idempotent stop/restart, accounting baseline — not automated |
| REQ-M4-ROUTE-001..005 (§6) | PASS | 12 automated histories, per-shard linearizability model; wrong-owner/stale leader never ack/apply |
| REQ-M4-MOVE-001..006 (§7) | PASS | 31 driver-level tests: crash/restart + bounded retry at every phase, all invariants; §7 scenarios covered (target unavailable, snapshot corruption, source unavailable ±promotion, leader changes, cancellation, duplicate reconciler, stale op/epoch, 20 concurrent writes, source+target restart). Model-level via deterministic fakes + real-catalog persistence; end-to-end movement proven by `m4_placement_node.rs` |
| REQ-M4-BAL-001 | **gap** | planner skew ≤ 1 on resulting plans not asserted (bootstrap skew ≤ 1 is asserted) |
| REQ-M4-BAL-002..005 | PASS | determinism on restart, bounded execution, single movement identity, gossip advisory-only, scheduler restart idempotent, slow-movement bounds |
| REQ-M4-FAIL-001..003 | PASS | single voter loss, group quorum loss blocks strong ops while other shards progress, catalog leader change, catalog quorum loss (write pending, never acked/applied; reads of committed view continue), node restart recovers groups independently (2-group real cluster) |
| REQ-M4-FAIL-004 | **PARTIAL** | corrupt/version-incompatible *catalog* snapshots fail closed; data-group log corruption not covered |
| REQ-M4-OPS-001..003 (§9) | **PARTIAL** | per-group Raft fields, stable views, bounded cardinality, health gate asserted; runtime workers, task/timer counts, connection/RPC metrics, memory accounting live outside the library observation surface — not asserted |
| REQ-M4-PERF-001..006 (§10) | PASS (blocker fixed) | three 1,024-group runs re-executed on the single final identity, 0 unexpected failures |

## Verdict

Spec 0006 remains **Accepted**. The four authorized blockers are closed, but
mandatory rows in §4, §5, BAL-001, FAIL-004, and §9 remain PARTIAL/gapped, so
the spec cannot be promoted to Verified under its own rule ("every mandatory
row below is PASS on one exact implementation identity").

## Recommended next authorization (not started)

1. §4 mapping/catalog automated matrix (golden vectors, restart stability, conflicting identity fail-closed, corruption fail-closed)
2. §5 group/runtime/transport automated matrix (capacity+1 refusal, no-serve-before-recovery, worker bounds, saturation backpressure, idempotent lifecycle, accounting baseline)
3. BAL-001 planner skew ≤ 1 assertion on resulting plans
4. FAIL-004 data-group log corruption fail-closed
5. §9 runtime/connection/memory observation surface (or a documented spec carve-out)
6. Re-run matrices + benchmarks on the then-final identity; promote only if every row is PASS

M5 remains blocked until Verified.
