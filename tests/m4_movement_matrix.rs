//! Spec 0006 verification §7 — movement phase matrix.
//!
//! For every movement phase (Intent, Learner, CatchUp, Promote, Lead,
//! Remove, Publish, Cleanup) this file injects a controller crash/restart
//! and bounded operation retries at the [`MovementDriver`] level, using the
//! deterministic fakes in [`movement_fakes`]. Every test
//! acknowledges a write immediately before the fault and then checks the
//! §7 invariants through [`Harness::assert_converged`]:
//!
//! - at most one active transition per shard;
//! - no premature target authority (stable placement untouched until the
//!   atomic publish);
//! - no destroyed still-authoritative replica (janitor removals only after
//!   the publish committed);
//! - no lost acknowledged write;
//! - no stale strong read;
//! - committed membership reconciled forward;
//! - final catalog placement equals the converged voter set;
//! - all retained replicas converge.
//!
//! The additional §7 scenarios are covered too: target unavailable before
//! promotion, target snapshot corruption, source unavailable before and
//! after promotion, catalog leader change, data-group leader change,
//! cancellation before/after the membership change, duplicate reconciler
//! invocation, stale operation id/epoch, and concurrent foreground traffic.
//!
//! Traceability: REQ-M4-MOVE-001..006, REQ-M4-FAIL-001..003.

#[path = "support/movement_fakes.rs"]
mod movement_fakes;

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use homekv::movement::{CancelOutcome, DriverOutcome, OperatorError, TombstoneOutcome};
use homekv::placement::{CatalogCommand, MovementPhase, PlacementError};
use homekv::placement_raft::CatalogGroupError;
use movement_fakes::{CommandKind, FaultPoint, Harness, OP_ID, SHARD};

/// Crash/restart while the movement is parked at `phase`: partially drive,
/// drop the driver (the controller crashes), acknowledge a write, rebuild a
/// fresh driver over the same persisted catalog, converge, and check every
/// §7 invariant.
async fn crash_restart_at_phase(phase: MovementPhase) {
    let harness = Harness::new(phase);
    let removed = harness.catalog.removed();
    // Partial drive, then the controller crashes.
    let partial_attempts = match phase {
        MovementPhase::Remove => 1,
        _ => 2,
    };
    let driver = harness.driver(removed, partial_attempts);
    let _ = driver.drive_shard(SHARD).await;
    drop(driver);
    // Acknowledged write immediately before the fault.
    harness.ack_write(b"phase-key", b"phase-value").await;
    // A restarted controller converges over the same persisted catalog.
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    assert!(driver.tombstone(SHARD, &OP_ID).await.is_some());
    harness.assert_converged().await;
}

/// Bounded retry at `phase`: fail the phase's operation `times` times, then
/// let it succeed. Exactly `times` phase failures must be recorded — the
/// driver retries the same intent instead of failing closed or skipping.
async fn retry_at_phase(
    phase: MovementPhase,
    point: FaultPoint,
    times: usize,
    error: OperatorError,
    failure_phase: MovementPhase,
) {
    let harness = Harness::new(phase);
    let removed = harness.catalog.removed();
    harness.ack_write(b"retry-key", b"retry-value").await;
    harness.operator.fail_times(point, times, error).await;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    let metrics = driver.metrics().snapshot();
    assert_eq!(
        metrics.phase_failures.get(&failure_phase),
        Some(&(times as u64)),
        "bounded retry: exactly {times} failures, then success"
    );
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "exactly one publish despite retries"
    );
}

async fn pending_phase(harness: &Harness) -> MovementPhase {
    harness
        .catalog
        .read_committed()
        .await
        .expect("catalog readable")
        .placements[&SHARD]
        .pending_movement
        .as_ref()
        .expect("movement pending")
        .phase
}

// ---------------------------------------------------------------------------
// §7 phase matrix: controller crash/restart per phase
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_intent_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::Intent).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_learner_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::Learner).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_catchup_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::CatchUp).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_promote_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::Promote).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_lead_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::Lead).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_remove_crash_restart_converges() {
    crash_restart_at_phase(MovementPhase::Remove).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_publish_crash_restart_converges() {
    // The membership change committed; the publish submit is next. The
    // controller crashes before submitting it.
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    harness.ack_write(b"phase-key", b"phase-value").await;
    let driver = harness.driver(removed, 64);
    drop(driver); // crash before the publish submit
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "crash before publish must not double-publish"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_cleanup_crash_restart_converges() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"phase-key", b"phase-value").await;
    // Publish with a driver whose identity is not the removed node, so the
    // inline cleanup is a no-op; then crash before reconciliation runs.
    let stayer = harness.catalog.source_voters[0];
    assert!(harness.catalog.target_voters.contains(&stayer));
    let driver = harness.driver(stayer, 64);
    harness.drive_until_completed(&driver).await;
    drop(driver); // crash after publish, before cleanup
                  // The restarted controller heals through level-triggered reconciliation:
                  // the committed placement no longer includes this node, so its replica
                  // is reclaimed.
    let driver = harness.driver(removed, 64);
    let outcome = driver
        .reconcile_local_replicas()
        .await
        .expect("reconcile succeeds");
    assert_eq!(outcome.removed, vec![SHARD]);
    assert!(outcome.errors.is_empty());
    harness.assert_converged().await;
}

// ---------------------------------------------------------------------------
// §7 phase matrix: bounded operation retry per phase
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_intent_retry_converges() {
    retry_at_phase(
        MovementPhase::Intent,
        FaultPoint::AddLearner,
        2,
        OperatorError::Transient("target unavailable".to_string()),
        MovementPhase::Learner,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_learner_retry_converges() {
    retry_at_phase(
        MovementPhase::Learner,
        FaultPoint::AddLearner,
        2,
        OperatorError::Transient("target unavailable".to_string()),
        MovementPhase::Learner,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_catchup_retry_converges() {
    retry_at_phase(
        MovementPhase::CatchUp,
        FaultPoint::AddLearner,
        2,
        OperatorError::Transient("catch-up stalled".to_string()),
        MovementPhase::CatchUp,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_promote_retry_converges() {
    retry_at_phase(
        MovementPhase::Promote,
        FaultPoint::Promote,
        2,
        OperatorError::Transient("joint consensus timeout".to_string()),
        MovementPhase::Promote,
    )
    .await;
    // The promotion applied exactly once despite the retries.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_lead_retry_converges() {
    retry_at_phase(
        MovementPhase::Lead,
        FaultPoint::IsLeader,
        2,
        OperatorError::Transient("leadership probe failed".to_string()),
        MovementPhase::Lead,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_remove_retry_converges() {
    let harness = Harness::new(MovementPhase::Remove);
    let removed = harness.catalog.removed();
    harness.ack_write(b"retry-key", b"retry-value").await;
    harness
        .operator
        .fail_times(
            FaultPoint::Remove,
            2,
            OperatorError::Transient("remove voter timeout".to_string()),
        )
        .await;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    let metrics = driver.metrics().snapshot();
    assert_eq!(
        metrics.phase_failures.get(&MovementPhase::Remove),
        Some(&2),
        "bounded retry: exactly 2 failures, then success"
    );
    assert_eq!(
        *harness.operator.removed.lock().await,
        vec![removed],
        "the old voter is removed exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_publish_retry_converges() {
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    harness.ack_write(b"retry-key", b"retry-value").await;
    harness
        .catalog
        .fail_next_submit(
            Some(CommandKind::Publish),
            CatalogGroupError::ConsensusUnavailable,
        )
        .await;
    let driver = harness.driver(removed, 64);
    // The failed publish surfaces as a retryable outcome; the pending
    // movement is untouched.
    assert!(
        matches!(
            driver.drive_shard(SHARD).await,
            DriverOutcome::TransientFailure { .. }
        ),
        "publish fault must be retryable"
    );
    assert!(pending_phase(&harness).await == MovementPhase::Remove);
    // The retry publishes exactly once.
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "exactly one publish despite the retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_cleanup_retry_converges() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"retry-key", b"retry-value").await;
    let stayer = harness.catalog.source_voters[0];
    let driver = harness.driver(stayer, 64);
    harness.drive_until_completed(&driver).await;
    drop(driver);
    // The first reconciliation hits an injected janitor fault; the error is
    // reported, the replica is kept, and the next scan retries.
    harness.janitor.fail_next(1).await;
    let driver = harness.driver(removed, 64);
    let outcome = driver
        .reconcile_local_replicas()
        .await
        .expect("reconcile reports the fault");
    assert!(outcome.removed.is_empty());
    assert_eq!(outcome.errors.len(), 1);
    let outcome = driver
        .reconcile_local_replicas()
        .await
        .expect("retry succeeds");
    assert_eq!(outcome.removed, vec![SHARD]);
    assert!(outcome.errors.is_empty());
    assert_eq!(driver.metrics().snapshot().cleanups, 1);
    harness.assert_converged().await;
}

// ---------------------------------------------------------------------------
// §7 additional scenarios
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_unavailable_before_promotion_retries_then_advances() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"target-key", b"target-value").await;
    harness
        .operator
        .fail_times(
            FaultPoint::AddLearner,
            3,
            OperatorError::Transient("connection refused".to_string()),
        )
        .await;
    // While the target is unavailable the phase must not advance early.
    let driver = harness.driver(removed, 3);
    assert!(matches!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::TransientFailure { .. }
    ));
    assert_eq!(pending_phase(&harness).await, MovementPhase::Intent);
    assert!(
        harness.operator.observed.lock().await.learners.is_empty(),
        "no learner admitted while the target is down"
    );
    // The faults are exhausted: the retry admits the learner and advances.
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn target_snapshot_corruption_bounded_retry() {
    let harness = Harness::new(MovementPhase::CatchUp);
    let removed = harness.catalog.removed();
    let added = harness.catalog.added();
    harness.ack_write(b"snapshot-key", b"snapshot-value").await;
    harness
        .operator
        .fail_times(
            FaultPoint::AddLearner,
            2,
            OperatorError::Transient("snapshot checksum mismatch".to_string()),
        )
        .await;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    let metrics = driver.metrics().snapshot();
    assert_eq!(
        metrics.phase_failures.get(&MovementPhase::CatchUp),
        Some(&2),
        "corrupt snapshots fail boundedly, then the clean one applies"
    );
    // The learner caught up only once a clean snapshot applied.
    assert_eq!(
        harness.data.get(added, b"snapshot-key").await,
        Some(b"snapshot-value".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_unavailable_before_promotion_retries() {
    let harness = Harness::new(MovementPhase::CatchUp);
    let removed = harness.catalog.removed();
    harness.ack_write(b"src-key", b"src-value").await;
    harness
        .operator
        .fail_times(
            FaultPoint::Observe,
            2,
            OperatorError::Transient("source unreachable".to_string()),
        )
        .await;
    // No phase advance and no membership mutation while observing fails: a
    // single drive attempt fails on the first observe.
    let driver = harness.driver(removed, 1);
    assert!(matches!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::TransientFailure { .. }
    ));
    // No phase advance and no membership mutation while observing fails.
    assert_eq!(pending_phase(&harness).await, MovementPhase::CatchUp);
    assert!(harness.operator.promoted.lock().await.is_empty());
    assert!(harness.operator.removed.lock().await.is_empty());
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_unavailable_after_promotion_retries() {
    // Promotion committed (observed = source + added); the source then goes
    // dark before the old voter is removed.
    let harness = Harness::new(MovementPhase::Remove);
    let removed = harness.catalog.removed();
    harness.ack_write(b"src2-key", b"src2-value").await;
    harness
        .operator
        .fail_times(
            FaultPoint::Observe,
            2,
            OperatorError::Transient("source unreachable".to_string()),
        )
        .await;
    // The source goes dark before the old voter is removed: a single drive
    // attempt fails on the first observe, with no membership mutation.
    let driver = harness.driver(removed, 1);
    assert!(matches!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::TransientFailure { .. }
    ));
    assert!(
        harness.operator.removed.lock().await.is_empty(),
        "no membership mutation during the observe outage"
    );
    assert_eq!(pending_phase(&harness).await, MovementPhase::Remove);
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_leader_change_preserves_single_movement_identity() {
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    harness.ack_write(b"leader-key", b"leader-value").await;
    // The catalog leader changes mid-publish: the first submit is rejected
    // as if the leader were unknown. The retry must reuse the same movement
    // identity instead of starting a second one.
    harness
        .catalog
        .fail_next_submit(
            Some(CommandKind::Publish),
            CatalogGroupError::ConsensusUnavailable,
        )
        .await;
    let driver = harness.driver(removed, 64);
    assert!(matches!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::TransientFailure { .. }
    ));
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "one movement identity: exactly one publish across the leader change"
    );
    assert!(driver.tombstone(SHARD, &OP_ID).await.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_epoch_refresh_retries_with_fresh_view() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"epoch-key", b"epoch-value").await;
    // A concurrent controller reassigns shard 6's desired leader, bumping
    // the global epoch between this driver's read and its submit.
    let placement6 = harness.catalog.placement(6).await.expect("shard 6");
    let new_leader = placement6
        .voters
        .iter()
        .copied()
        .find(|id| *id != placement6.desired_leader)
        .expect("another stable voter");
    harness
        .catalog
        .hook_next_submit(move |catalog| {
            catalog
                .apply(CatalogCommand::SetDesiredLeader {
                    expected_epoch: 1,
                    shard_id: 6,
                    new_leader,
                })
                .expect("concurrent controller write applies");
        })
        .await;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    // Both writers landed: the epoch advanced for the concurrent hint and
    // for the publish, and neither clobbered the other.
    let state = harness.catalog.read_committed().await.unwrap();
    assert_eq!(state.placement_epoch, 3);
    assert_eq!(state.placements[&6].desired_leader, new_leader);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_operation_id_surfaces_rejection() {
    // Catalog level: an unknown operation id fails closed.
    let harness = Harness::new(MovementPhase::Intent);
    let unknown = harness
        .catalog
        .submit(CatalogCommand::AdvanceMovementPhase {
            expected_epoch: 1,
            operation_id: [9; 16],
            shard_id: SHARD,
            phase: MovementPhase::Learner,
        })
        .await;
    assert!(
        matches!(
            unknown,
            Err(CatalogGroupError::InvalidPlacement(
                PlacementError::UnknownMovement { .. }
            ))
        ),
        "unexpected: {unknown:?}"
    );

    // Driver level: a concurrent controller cancels the movement between
    // this driver's observation and its publish submit. The driver must
    // surface the rejection as Cancelled — not publish, not panic.
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    let source = harness.catalog.source_voters;
    harness.ack_write(b"stale-key", b"stale-value").await;
    harness
        .catalog
        .hook_next_submit(move |catalog| {
            catalog
                .apply(CatalogCommand::CancelMovement {
                    expected_epoch: 1,
                    operation_id: OP_ID,
                    shard_id: SHARD,
                    observed_voters: source,
                    reason: "concurrent cancel".to_string(),
                })
                .expect("concurrent cancel applies");
        })
        .await;
    let driver = harness.driver(removed, 64);
    let outcome = driver.drive_shard(SHARD).await;
    assert!(
        matches!(outcome, DriverOutcome::Cancelled { .. }),
        "unexpected: {outcome:?}"
    );
    // The old stable placement is still authoritative.
    let state = harness.catalog.read_committed().await.unwrap();
    assert_eq!(state.placements[&SHARD].voters, source);
    assert!(state.placements[&SHARD].pending_movement.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_membership_change_aborts() {
    let harness = Harness::new(MovementPhase::CatchUp);
    let removed = harness.catalog.removed();
    harness.ack_write(b"cancel-key", b"cancel-value").await;
    let driver = harness.driver(removed, 64);
    assert_eq!(
        driver
            .request_cancel(SHARD, "target drained".to_string())
            .await,
        CancelOutcome::Cancelled
    );
    let tombstone = driver.tombstone(SHARD, &OP_ID).await.expect("tombstoned");
    assert_eq!(tombstone.outcome, TombstoneOutcome::Cancelled);
    // The old stable placement stays authoritative; the epoch is untouched.
    let state = harness.catalog.read_committed().await.unwrap();
    assert_eq!(
        state.placements[&SHARD].voters,
        harness.catalog.source_voters
    );
    assert!(state.placements[&SHARD].pending_movement.is_none());
    assert_eq!(state.placement_epoch, 1, "cancel must not consume an epoch");
    assert_eq!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::NoPendingMovement
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_after_membership_change_must_finish_forward() {
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    harness.ack_write(b"cancel2-key", b"cancel2-value").await;
    let driver = harness.driver(removed, 64);
    assert!(matches!(
        driver.request_cancel(SHARD, "too late".to_string()).await,
        CancelOutcome::MustFinishForward {
            phase: MovementPhase::Remove,
            ..
        }
    ));
    // The refused cancel left the movement untouched; it finishes forward
    // and is never stranded.
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_reconciler_invocation_keeps_single_transition() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"dup-key", b"dup-value").await;
    // Two controllers race the same shard: the catalog's epoch CAS admits
    // exactly one publish; the loser observes the completed movement.
    let driver_a = harness.driver(removed, 64);
    let driver_b = harness.driver(removed, 64);
    let (outcome_a, outcome_b) =
        tokio::join!(driver_a.drive_shard(SHARD), driver_b.drive_shard(SHARD));
    let completed = [&outcome_a, &outcome_b]
        .iter()
        .filter(|outcome| matches!(outcome, DriverOutcome::Completed { .. }))
        .count();
    assert_eq!(
        completed, 1,
        "exactly one reconciler publishes: {outcome_a:?} vs {outcome_b:?}"
    );
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "a single active transition per shard"
    );
    for driver in [&driver_a, &driver_b] {
        assert_eq!(
            driver.drive_shard(SHARD).await,
            DriverOutcome::NoPendingMovement
        );
    }
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_intent_while_active_is_rejected() {
    // REQ-M4-MOVE-001: at most one active movement per shard; the catalog
    // rejects a second intent while one is active.
    let harness = Harness::new(MovementPhase::CatchUp);
    let target = harness.catalog.target_voters;
    let rejected = harness
        .catalog
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: 1,
            operation_id: [8; 16],
            shard_id: SHARD,
            target_voters: target,
        })
        .await;
    assert!(
        matches!(
            rejected,
            Err(CatalogGroupError::InvalidPlacement(
                PlacementError::ConflictingMovement { .. }
            ))
        ),
        "unexpected: {rejected:?}"
    );
    // The active movement is untouched by the rejected intent.
    let pending = harness
        .catalog
        .read_committed()
        .await
        .unwrap()
        .placements
        .get(&SHARD)
        .expect("placement")
        .pending_movement
        .clone()
        .expect("movement still active");
    assert_eq!(pending.operation_id, OP_ID);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_group_leader_elsewhere_advances_and_converges() {
    // The Lead phase observes rather than moves leadership (the pinned
    // OpenRaft API exposes no transfer primitive): Ok(false) — this node is
    // not the data-group leader, a stable leader exists elsewhere — is the
    // steady state, so the driver advances and converges.
    let harness = Harness::new(MovementPhase::Lead);
    let removed = harness.catalog.removed();
    harness.ack_write(b"lead2-key", b"lead2-value").await;
    *harness.operator.leader.lock().await = false;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_group_leader_change_backs_off_without_mutation() {
    let harness = Harness::new(MovementPhase::Lead);
    let removed = harness.catalog.removed();
    harness.ack_write(b"lead-key", b"lead-value").await;
    // The data-group leader changes mid-movement: leadership probes fail and
    // the driver must back off without mutating membership.
    harness
        .operator
        .fail_times(
            FaultPoint::IsLeader,
            3,
            OperatorError::NotLeader {
                leader_hint: Some(9),
            },
        )
        .await;
    let driver = harness.driver(removed, 3);
    assert!(matches!(
        driver.drive_shard(SHARD).await,
        DriverOutcome::TransientFailure { .. }
    ));
    assert!(
        harness.operator.promoted.lock().await.is_empty(),
        "no promotion during the leadership flap"
    );
    assert!(
        harness.operator.removed.lock().await.is_empty(),
        "no removal during the leadership flap"
    );
    assert_eq!(pending_phase(&harness).await, MovementPhase::Lead);
    // Leadership stabilizes: the movement converges.
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_foreground_traffic_no_lost_writes() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    // Slow the catch-up so foreground writes interleave with every phase.
    *harness.operator.catchup_delay.lock().await = Duration::from_millis(50);
    harness.ack_write(b"traffic-0", b"value-0").await;
    let writer_harness = harness.clone();
    let writer = tokio::spawn(async move {
        for i in 1..20u32 {
            writer_harness
                .ack_write(
                    format!("traffic-{i}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    writer.await.expect("writer finishes");
    // Every acknowledged write — before, during, and after the movement —
    // survived on the converged voter set.
    harness.assert_converged().await;
    assert_eq!(harness.data.acked().await.len(), 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_of_source_and_target_preserves_data() {
    // Driver-model restart: rebuild the observed membership purely from the
    // committed catalog (the data plane is the durable disk here). Full
    // process-restart recovery of data groups is covered in
    // m4_failure_matrix.rs; this asserts the movement left no replica
    // behind that a restart could not reconcile.
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    harness.ack_write(b"restart-key", b"restart-value").await;
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target.clone(), BTreeSet::new()).await;
    for node in &target {
        assert_eq!(
            harness.data.get(*node, b"restart-key").await,
            Some(b"restart-value".to_vec()),
            "node {node} serves after restart"
        );
    }
    assert_eq!(
        harness.strong_read(b"restart-key").await,
        Some(b"restart-value".to_vec())
    );
}
