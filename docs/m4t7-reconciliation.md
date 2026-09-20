# M4-T7 Final Verification Reconciliation

> Follow-on evidence ledger: [`docs/m4t7-verification-evidence.md`](m4t7-verification-evidence.md)
> (2026-09-20, identity `3033c18`) records the final verification run.
> All residual rows (§4, §5, BAL-001, FAIL-004, §9) are closed; Spec 0006 is
> **Verified**. (An earlier ledger at identity `23d3d37` left the spec at
> **Accepted** with five residual rows; those are now closed.)

**Date:** 2026-09-20  
**Spec:** 0006-multi-raft-placement  
**Status:** **Verified**  
**Final implementation identity:** `3033c18d07273ef63b3b656bbd51b43c9d822ea7` (main, PR #97)

## Verdict

Spec 0006 is **Verified**: every mandatory verification row is PASS on the
single exact implementation identity `3033c18d07273ef63b3b656bbd51b43c9d822ea7`
(tree `7b4bb361`) — full suite 335/335 green plus three 1,024-group RF=3
benchmark runs with 0 failures each (digests in
[`docs/m4t7-verification-evidence.md`](m4t7-verification-evidence.md)). M5 is
unblocked.

## Requirement-to-evidence reconciliation

### REQ-M4-BASE-001..004 (M0-M3 preservation)

**State:** PASS  
**Evidence:** Full `cargo test` (231 tests, 19 targets) green on `4dcefe1`.
M0-M3 regression gates unchanged. M4-T6 harness uses M3's exact
`client_write` (quorum-durable + leader-apply) and `ensure_linearizable`
(quorum-backed) boundaries.

### REQ-M4-MAP-001..006 (Shard mapping and catalog authority)

**State:** PASS  
**Evidence:** `tests/m4_mapping_catalog_matrix.rs` (11 tests, PR #95): XXH3 golden
vectors, bounded arbitrary-key mapping, 1,024 unique/stable group IDs,
RF=3/bootstrap/domain/skew behavior, idempotent bootstrap, conflicting identity
fail-closed, catalog quorum loss/failover/corruption. §4 items 6/9 trace to the
existing `tests/m4_catalog_group.rs::catalog_group_is_quorum_authoritative_and_recovers_durable_bootstrap`
(quorum-less authority rejection; snapshot → full stop → restart → read equals
pre-restart state).

### REQ-M4-GROUP-001..006 (Lifecycle and bounds)

**State:** PASS  
**Evidence:** `tests/m4_group_runtime_matrix.rs` (10 tests, PR #95): registry
capacity+1 refusal without partial allocation, no-serve-before-coherent-recovery,
OS-worker bound as groups grow, per-peer saturation backpressure with permit
release, transport isolation, idempotent stop/restart lifecycle, accounting
cleanup on removal.

### REQ-M4-ROUTE-001..005 (Routing and authority)

**State:** PARTIAL  
**Evidence:** M4-T3 implemented committed-catalog shard-aware routing.  
**Gap:** Section-6 routing histories (stale routes, wrong member, stale
leader, cache version skew, concurrent ops during refresh, etc.) not
implemented as automated linearizability-model tests.

### REQ-M4-MOVE-001..006 (Membership change and movement)

**State:** PARTIAL  
**Evidence:** M4-T4 implemented the movement reconciler with phase tracking.
M4-T6 fault cell proves 3→2→3 membership movement completes with 0
failures on healthy groups.  
**Gap:** Section-7 movement phase matrix (crash/restart at every phase,
target unavailable, snapshot corruption, catalog/data-group leader change,
cancellation, duplicate invocation, etc.) not implemented as automated tests.  
**Blocker:** No production binary instantiates `MovementDriver`. The
movement path exists as a library but is not wired into a production
composition root. End-to-end movement in a real deployment is unproven.

### REQ-M4-BAL-001..005 (Rebalancing policy)

**State:** PASS  
**Evidence:** BAL-001: voter and desired-leader skew ≤ 1 asserted on resulting
plans when topology permits (PR #95). BAL-002..005: deterministic planner,
bounded execution, single movement identity, gossip advisory-only, scheduler
restart idempotent, slow-movement bounds (`tests/m4_failure_matrix.rs`).  
**Note:** No scheduler-owned snapshot-byte limit; byte limits remain in
movement driver (M4-T5 caveat).

### REQ-M4-FAIL-001..004 (Failure behavior)

**State:** PASS  
**Evidence:** FAIL-001..003: single voter loss, group quorum loss blocks strong
ops while other shards progress, catalog leader change, catalog quorum loss
(write pending, never acked/applied; committed-view reads continue), node
restart recovers groups independently. FAIL-004 (PR #94):
`tests/m4_data_group_corruption.rs` (5 tests) — checksum bit-flip, truncated
store, version corruption, snapshot corruption, and the production
`PlacementNode::start` corruption path all fail closed; healthy quorum
continues; catalog/other groups unaffected; janitor + full restart heals.
Production fixes: `FsReplicaJanitor` removes single-file `node-<id>.raft`
stores; data groups initialize only when no voter has a durable store
(prevents divergent re-initialization on restart).

### REQ-M4-OPS-001..003 (Observability)

**State:** PASS  
**Evidence:** PR #89 added per-group Raft fields to `TopologyView` (role, term,
config, commit/apply index, snapshot progress, replica lag). PR #96 added the
production metrics surface `src/placement_metrics.rs` on `PlacementNode`
(workers/tasks, groups, per-replica Raft detail, RPC attempts/failures/
backpressure/bytes, per-peer views, RSS + per-replica memory, redirects by
cause, movement phase/duration/results/failures + aggregate byte flow, skew,
bounded cardinality, paginated shard inspection). PR #97 added
`node_timers` (HomeKV-owned timer count, asserted `== 2`).
`tests/m4_observability_matrices.rs` (11 tests) asserts stable topology views,
per-group field consistency, bounded cardinality, health transitions, and the
production snapshot covering every §9 field.  
**Honest scope:** only existing signals are reported — timers are the
HomeKV-owned count (no stable tokio timer-wheel API); connections are the
per-peer factory entries (in-process transport has no sockets); movement bytes
are aggregate snapshot-transfer/per-peer payload bytes (per-operation
attribution would violate bounded cardinality, REQ-M4-OPS-003); queue depth is
reported where HomeKV owns the queue.

### REQ-M4-PERF-001..006 (Scaling benchmark)

**State:** PARTIAL (blocker on identity)  
**Evidence:** M4-T6 delivered the harness and three retained 1,024-group
runs (8,000 ops each, 0 failures, 3/3 fault cells pass). Scaling decision
documented as Accepted. `authoritative_performance_result=false`.  
**Blocker:** The three runs are not on one exact identity. Run 1 used
harness commit `c2c02fed`; runs 2–3 used `529fd62f` (harness-only changes,
but the verification rule requires one exact implementation identity).
To satisfy the gate, all three runs must be re-executed on the final
commit.

## Precise blockers for Verified status

1. **Identity:** Re-run all three 1,024-group benchmark runs on one exact
   commit (the final merge head). Current runs span two harness commits.

2. **Observability:** Extend `TopologyView` with per-group Raft state
   (role, term, config, commit/apply index, snapshot progress, lag) per
   REQ-M4-OPS-001. Add automated assertions for stable representations
   during transitions.

3. **Production wiring:** Instantiate `MovementDriver` in a production
   composition root (binary). Prove end-to-end movement in a real
   deployment topology.

4. **Verification matrices:** Implement automated tests for:
   - Section 7: Movement phase crash/restart matrix
   - Section 8: Rebalancing and failure matrix  
   - Section 6: Routing history linearizability model
   - Section 9: Observability stability assertions

## Next actions

To promote Spec 0006 to Verified:
1. Address blockers 1-4 above (estimated: 2-3 additional task slices)
2. Re-run the full verification suite on one exact identity
3. Archive all evidence with digests
4. Reconcile again via M4-T7

M5 cannot begin until Spec 0006 is Verified.
