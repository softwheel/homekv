# M4-T7 Final Verification Reconciliation

> Follow-on evidence ledger: [`docs/m4t7-verification-evidence.md`](m4t7-verification-evidence.md)
> (2026-09-20, identity `23d3d37`) records the post-blocker verification run.
> It closes all four authorized blockers but leaves Spec 0006 at **Accepted**
> with precise residual blockers (§4, §5, BAL-001, FAIL-004, §9).

**Date:** 2026-09-20  
**Spec:** 0006-multi-raft-placement  
**Status:** **Accepted** (not Verified)  
**Merged head:** `4dcefe1` (PR #86, M4-T6)

## Verdict

Spec 0006 remains **Accepted**. It cannot be promoted to Verified because
mandatory verification rows are not PASS on one exact identity. The
blockers are precise and actionable (see below).

## Requirement-to-evidence reconciliation

### REQ-M4-BASE-001..004 (M0-M3 preservation)

**State:** PASS  
**Evidence:** Full `cargo test` (231 tests, 19 targets) green on `4dcefe1`.
M0-M3 regression gates unchanged. M4-T6 harness uses M3's exact
`client_write` (quorum-durable + leader-apply) and `ensure_linearizable`
(quorum-backed) boundaries.

### REQ-M4-MAP-001..006 (Shard mapping and catalog authority)

**State:** PARTIAL  
**Evidence:** M4-T1 implemented the placement catalog (authoritative model,
consensus adapter, recovery). `tests/m4_catalog_group.rs` proves catalog
consensus recovery. XXH3 mapping to 1,024 shards implemented.  
**Gap:** The full section-4 verification matrix (golden vectors across
restart, conflicting identity fail-closed, corruption fail-closed, etc.)
was not implemented as automated tests.

### REQ-M4-GROUP-001..006 (Lifecycle and bounds)

**State:** PARTIAL  
**Evidence:** M4-T2 implemented bounded registry, shared runtime, shared
transport, admission bounds. Multi-group recovery test exists.  
**Gap:** Section-5 verification (capacity+1 refusal, no-serve-before-recovery,
worker bound as groups grow, per-peer saturation backpressure, etc.) not
fully covered by automated tests.

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

**State:** PARTIAL  
**Evidence:** M4-T5 implemented deterministic planner and bounded scheduler
with unit tests.  
**Gap:** Section-8 rebalancing matrix (planner restart determinism,
gossip divergence isolation, movement target slow bounds, catalog-loss
behavior, etc.) not fully covered by automated tests.  
**Note:** No scheduler-owned snapshot-byte limit; byte limits remain in
movement driver (M4-T5 caveat).

### REQ-M4-FAIL-001..004 (Failure behavior)

**State:** PARTIAL  
**Evidence:** M4-T6 fault cells prove: one unavailable replica preserves
quorum progress; one delayed group doesn't starve healthy groups;
membership movement completes.  
**Gap:** Section-8 full failure matrix (catalog leader kill, catalog quorum
loss, node restart with many groups, corrupt artifact fail-closed, heal
after partial transition, etc.) not implemented as automated tests.

### REQ-M4-OPS-001..003 (Observability)

**State:** FAIL  
**Blocker:** `TopologyView` (src/rebalance.rs:1287) lacks per-group Raft
fields required by REQ-M4-OPS-001: per-group role, term, config,
commit/apply index, snapshot progress, and replica lag. The struct exposes
cluster-level aggregates (voter_skew, leader_skew) but not the per-group
detail the verification spec mandates.  
**Gap:** Section-9 observability assertions (stable representations during
bootstrap/elections/movement/failures, bounded cardinality, etc.) not
implemented as automated tests.

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
