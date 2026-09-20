//! Movement reconciler: carries a shard voter movement through the design's
//! movement phases to completion.
//!
//! Traceability:
//! - REQ-M4-MOVE-001: at most one active movement per shard; the catalog
//!   rejects a second intent while one is active.
//! - REQ-M4-MOVE-002: movement is staged `Intent → Learner → CatchUp → Promote
//!   → Lead → Remove → Publish → Cleanup`; stable placement stays unchanged
//!   until the atomic publish.
//! - REQ-M4-MOVE-003: membership changes use OpenRaft joint consensus via
//!   `change_membership(ReplaceAllVoters(..))`.
//! - REQ-M4-MOVE-004: retry-safe: every transition is idempotent or retries
//!   the same intent; reconciliation is level-triggered and fails closed on
//!   ambiguous membership. A tombstone prevents re-driving a cancelled or
//!   completed operation after restart.
//! - REQ-M4-MOVE-005: cancellation is safe before committed membership
//!   changes; after that the movement must finish forward.
//! - REQ-M4-MOVE-006: the removed node's local state is reclaimed only after
//!   publication and the safety checks pass.
//! - REQ-M4-FAIL-001: learner/catch-up faults surface as retryable errors.
//! - REQ-M4-FAIL-002: target unavailability surfaces as a retryable error.
//! - REQ-M4-FAIL-003: transient faults retry within bounded attempts; a stale
//!   catalog view re-reads and retries on the fresh epoch.
//! - REQ-M4-FAIL-004: movement outcomes (retries, durations, phases) are
//!   exposed as metrics.
//!
//! The reconciler never invents membership: it observes the *committed*
//! membership from the data-group state machine and only advances the catalog
//! when the observation matches what the phase expects. Anything else fails
//! closed (no publish, no cancel, no guess).
//!
//! Note on leadership: the pinned OpenRaft API exposes no leadership-transfer
//! primitive, so the `Lead` phase observes leadership and treats a stable
//! remote leader as the steady state rather than blocking forever.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{ChangeMembers, Membership, Raft};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::movement_admission::MovementWorkAdmission;
use crate::placement::{
    next_phase, CatalogCommand, CatalogResponse, CatalogState, EligibleNode, MovementPhase,
    PendingMovement, PlacementEpoch, PlacementError, MAX_MOVEMENT_NOTE_BYTES,
};
use crate::placement_raft::{CatalogGroupError, PlacementCatalogGroup};
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode, RaftNodeId};

/// Committed membership observed from a data group's state machine.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObservedMembership {
    /// Committed voters: authoritative for membership decisions.
    pub voters: BTreeSet<RaftNodeId>,
    /// Committed learners.
    pub learners: BTreeSet<RaftNodeId>,
    /// Whether a joint (in-transition) configuration is in effect.
    pub membership_changing: bool,
    /// The Raft term that admitted the observed membership.
    pub term: u64,
}

/// Decision the reconciler takes for one drive step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcilerAction {
    /// Admit the pending learner replica into the OpenRaft group.
    AddLearner { node: RaftNodeId },
    /// Wait until the committed membership admits the new node.
    WaitForLearnerCatchUp { node: RaftNodeId },
    /// Promote the caught-up learner to voter via joint consensus.
    PromoteLearner { node: RaftNodeId },
    /// Observe leadership on the data group before removing a voter.
    WaitForLeadership,
    /// Remove the old voter via joint consensus.
    RemoveVoter { node: RaftNodeId },
    /// Publish the new stable placement to the catalog.
    Publish,
    /// Re-check the observed state after a settle delay.
    WaitForSettle,
    /// The observation contradicts the intent: stop and surface it.
    FailClosed { reason: String },
}

/// Pure transition decision: (intent phase, observation) → action.
///
/// This function is deliberately free of side effects so every branch is
/// unit-testable (REQ-M4-MOVE-004).
pub fn decide(pending: &PendingMovement, observed: &ObservedMembership) -> ReconcilerAction {
    let source: BTreeSet<RaftNodeId> = pending.source_voters.iter().copied().collect();
    let target: BTreeSet<RaftNodeId> = pending.target_voters.iter().copied().collect();
    let added: Vec<RaftNodeId> = target.difference(&source).copied().collect();
    let removed: Vec<RaftNodeId> = source.difference(&target).copied().collect();

    // The design moves one voter per operation.
    if added.len() != 1 || removed.len() != 1 {
        return ReconcilerAction::FailClosed {
            reason: format!(
                "movement intent changes {} voters ({} added, {} removed); expected exactly one",
                added.len() + removed.len(),
                added.len(),
                removed.len()
            ),
        };
    }
    let added = added[0];
    let removed = removed[0];

    // A joint configuration means a membership change is still committing.
    if observed.membership_changing {
        return ReconcilerAction::WaitForSettle;
    }
    // Committed target membership: the movement is ready to publish,
    // regardless of which phase the catalog recorded.
    if observed.voters == target {
        return ReconcilerAction::Publish;
    }

    let fail_closed = |why: &str| ReconcilerAction::FailClosed {
        reason: format!(
            "{why}; phase {:?}, observed voters {:?}, expected source {:?} or target {:?}",
            pending.phase, observed.voters, source, target
        ),
    };

    match pending.phase {
        MovementPhase::Intent | MovementPhase::Learner | MovementPhase::CatchUp => {
            if observed.voters != source {
                return fail_closed("voters left the source set before promotion");
            }
            if observed.learners.contains(&added) {
                ReconcilerAction::WaitForLearnerCatchUp { node: added }
            } else {
                ReconcilerAction::AddLearner { node: added }
            }
        }
        MovementPhase::Promote => {
            let mut promoted = source.clone();
            promoted.insert(added);
            if observed.voters == source {
                ReconcilerAction::PromoteLearner { node: added }
            } else if observed.voters == promoted {
                // Promotion committed; observe leadership before removal.
                ReconcilerAction::WaitForLeadership
            } else {
                fail_closed("unexpected voters during promotion")
            }
        }
        MovementPhase::Lead | MovementPhase::Remove => {
            let mut promoted = source.clone();
            promoted.insert(added);
            if observed.voters == promoted {
                if pending.phase == MovementPhase::Lead {
                    ReconcilerAction::WaitForLeadership
                } else {
                    ReconcilerAction::RemoveVoter { node: removed }
                }
            } else {
                fail_closed("unexpected voters during removal")
            }
        }
        // Publish/Cleanup never drive membership; reaching them here means
        // the observation contradicts the recorded phase.
        MovementPhase::Publish | MovementPhase::Cleanup => {
            fail_closed("phase expects publication but voters do not match the target")
        }
    }
}

/// The new node and its endpoint for learner admission.
#[derive(Clone, Debug)]
pub struct LearnerEndpoint {
    pub id: RaftNodeId,
    pub node: RaftNode,
}

/// Errors from the membership operator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperatorError {
    /// This replica is not the data-group leader.
    NotLeader { leader_hint: Option<RaftNodeId> },
    /// OpenRaft rejected the membership change.
    Rejected(String),
    /// Retryable failure (target unavailable, transport fault, ...).
    Transient(String),
    /// The operation is unavailable (no consensus, storage fault, ...).
    Unavailable(String),
    /// Non-retryable failure.
    Fatal(String),
}

impl std::fmt::Display for OperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => {
                write!(f, "not the data-group leader (hint: {leader_hint:?})")
            }
            Self::Rejected(reason) => write!(f, "membership change rejected: {reason}"),
            Self::Transient(reason) => write!(f, "transient membership fault: {reason}"),
            Self::Unavailable(reason) => write!(f, "membership unavailable: {reason}"),
            Self::Fatal(reason) => write!(f, "fatal membership fault: {reason}"),
        }
    }
}

/// Effect side of the reconciler, abstracted for testing.
#[async_trait]
pub trait MembershipOperator: Send + Sync {
    /// Observe the committed membership from the state machine.
    async fn observe(&self) -> Result<ObservedMembership, OperatorError>;
    /// Admit a learner; `blocking=true` waits for catch-up proof.
    async fn add_learner(
        &self,
        learner: LearnerEndpoint,
        blocking: bool,
    ) -> Result<(), OperatorError>;
    /// Promote a learner to voter via joint consensus (idempotent).
    async fn promote_learner(&self, voter: RaftNodeId) -> Result<(), OperatorError>;
    /// Remove a voter via joint consensus (idempotent).
    async fn remove_voter(&self, voter: RaftNodeId) -> Result<(), OperatorError>;
    /// Whether this replica currently leads the data group.
    async fn is_leader(&self) -> Result<bool, OperatorError>;
}

/// Read path into the committed placement catalog.
#[async_trait]
pub trait CatalogPort: Send + Sync {
    async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError>;
    async fn submit(&self, command: CatalogCommand) -> Result<CatalogResponse, CatalogGroupError>;
}

#[async_trait]
impl CatalogPort for PlacementCatalogGroup {
    async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
        self.committed_state().await
    }

    async fn submit(&self, command: CatalogCommand) -> Result<CatalogResponse, CatalogGroupError> {
        PlacementCatalogGroup::submit(self, command).await
    }
}

/// Reclaims the removed node's local replica directory.
#[async_trait]
pub trait LocalReplicaJanitor: Send + Sync {
    async fn remove_local_replica(&self, shard_id: u16) -> Result<(), String>;
    /// List the shard ids that currently hold a local replica on this node.
    async fn list_local_replicas(&self) -> Result<Vec<u16>, String>;
}

/// The voter a movement adds: present in the target, absent from the source.
fn added_voter(pending: &PendingMovement) -> Option<RaftNodeId> {
    pending
        .target_voters
        .iter()
        .copied()
        .find(|node| !pending.source_voters.contains(node))
}

/// Live operator backed by the data-group OpenRaft handle.
pub struct LiveMembershipOperator {
    raft: Raft<HomeKvRaftConfig>,
    state_machine: Arc<HomeKvStateMachine>,
}

impl LiveMembershipOperator {
    pub fn new(raft: Raft<HomeKvRaftConfig>, state_machine: Arc<HomeKvStateMachine>) -> Self {
        Self {
            raft,
            state_machine,
        }
    }

    fn map_error(
        error: RaftError<RaftNodeId, ClientWriteError<RaftNodeId, RaftNode>>,
    ) -> OperatorError {
        match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                OperatorError::NotLeader {
                    leader_hint: forward.leader_id,
                }
            }
            RaftError::APIError(ClientWriteError::ChangeMembershipError(change)) => {
                OperatorError::Rejected(change.to_string())
            }
            RaftError::Fatal(fatal) => OperatorError::Fatal(fatal.to_string()),
        }
    }
}

#[async_trait]
impl MembershipOperator for LiveMembershipOperator {
    async fn observe(&self) -> Result<ObservedMembership, OperatorError> {
        let metrics = self.raft.metrics().borrow().clone();
        // Committed membership comes from the state machine's applied view,
        // not from the advisory metrics.
        let view = self.state_machine.view().await;
        let term = view
            .last_applied
            .map(|log_id| log_id.leader_id.term)
            .unwrap_or(0);
        let voters: BTreeSet<RaftNodeId> = view.membership.voter_ids().collect();
        let learners: BTreeSet<RaftNodeId> = view.membership.membership().learner_ids().collect();
        // Advisory only: the effective (possibly joint) membership tells us
        // whether a change is still committing; the committed voters above
        // are authoritative for decisions.
        let effective: &Membership<RaftNodeId, RaftNode> = metrics.membership_config.membership();
        let effective_voters: BTreeSet<RaftNodeId> = effective.voter_ids().collect();
        let joint = effective.get_joint_config().len() > 1;
        let membership_changing = joint || effective_voters != voters;
        Ok(ObservedMembership {
            voters,
            learners,
            membership_changing,
            term,
        })
    }

    async fn add_learner(
        &self,
        learner: LearnerEndpoint,
        blocking: bool,
    ) -> Result<(), OperatorError> {
        self.raft
            .add_learner(learner.id, learner.node, blocking)
            .await
            .map_err(Self::map_error)
            .map(|_| ())
    }

    async fn promote_learner(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
        let observed = self.observe().await?;
        let mut next: BTreeSet<RaftNodeId> = observed.voters;
        next.insert(voter);
        // Joint consensus converges this to the uniform set; re-issuing the
        // same set is a no-op, so retries are safe.
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(next), false)
            .await
            .map_err(Self::map_error)
            .map(|_| ())
    }

    async fn remove_voter(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
        let observed = self.observe().await?;
        let mut next: BTreeSet<RaftNodeId> = observed.voters;
        next.remove(&voter);
        self.raft
            .change_membership(ChangeMembers::ReplaceAllVoters(next), false)
            .await
            .map_err(Self::map_error)
            .map(|_| ())
    }

    async fn is_leader(&self) -> Result<bool, OperatorError> {
        // Leadership check via a linearizable round: Ok iff this replica
        // currently leads the data group.
        Ok(self.raft.ensure_linearizable().await.is_ok())
    }
}

/// Bounded, idempotent record of movements that were cancelled, completed, or
/// abandoned.
///
/// Tombstoning prevents a restarted reconciler from re-issuing a movement
/// that was already cancelled or published (REQ-M4-MOVE-004).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementTombstone {
    pub operation_id: [u8; 16],
    pub shard_id: u16,
    pub outcome: TombstoneOutcome,
    pub note: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TombstoneOutcome {
    Completed,
    Cancelled,
    Abandoned,
}

/// In-memory tombstone store; callers persist it alongside the catalog.
#[derive(Debug)]
pub struct TombstoneStore {
    inner: tokio::sync::Mutex<BTreeMap<([u8; 16], u16), MovementTombstone>>,
    capacity: usize,
}

impl TombstoneStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(BTreeMap::new()),
            capacity: capacity.max(1),
        }
    }

    pub async fn tombstone(
        &self,
        shard_id: u16,
        operation_id: &[u8; 16],
        outcome: TombstoneOutcome,
        note: impl Into<String>,
    ) {
        let note = truncate_note(note.into());
        let mut inner = self.inner.lock().await;
        inner.insert(
            (*operation_id, shard_id),
            MovementTombstone {
                operation_id: *operation_id,
                shard_id,
                outcome,
                note,
            },
        );
        while inner.len() > self.capacity {
            let Some(oldest) = inner.keys().next().copied() else {
                break;
            };
            inner.remove(&oldest);
        }
    }

    pub async fn get(&self, shard_id: u16, operation_id: &[u8; 16]) -> Option<MovementTombstone> {
        self.inner
            .lock()
            .await
            .get(&(*operation_id, shard_id))
            .cloned()
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

fn truncate_note(note: String) -> String {
    if note.len() <= MAX_MOVEMENT_NOTE_BYTES {
        return note;
    }
    let mut end = MAX_MOVEMENT_NOTE_BYTES;
    while end > 0 && !note.is_char_boundary(end) {
        end -= 1;
    }
    note[..end].to_string()
}

/// Terminal outcome of a reconciled movement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MovementOutcome {
    Completed,
    Cancelled,
    FailClosed,
    TransientFailure,
}

/// Bounded summary of one reconciled movement (REQ-M4-FAIL-004).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationSummary {
    pub operation_id: [u8; 16],
    pub shard_id: u16,
    pub outcome: MovementOutcome,
    pub phases_advanced: u32,
    pub attempts: u32,
    pub duration_millis: u64,
}

const MAX_RECENT_OPERATIONS: usize = 64;

/// Movement metrics snapshot; serializable for the operator API.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementMetricsSnapshot {
    pub operations_started: u64,
    pub operations_completed: u64,
    pub operations_fail_closed: u64,
    pub publishes: u64,
    pub cancels: u64,
    pub cleanups: u64,
    pub admission_deferrals: u64,
    pub phase_attempts: BTreeMap<MovementPhase, u64>,
    pub phase_successes: BTreeMap<MovementPhase, u64>,
    pub phase_failures: BTreeMap<MovementPhase, u64>,
    pub recent: Vec<OperationSummary>,
}

#[derive(Debug, Default)]
struct MovementMetricsInner {
    operations_started: u64,
    operations_completed: u64,
    operations_fail_closed: u64,
    publishes: u64,
    cancels: u64,
    cleanups: u64,
    admission_deferrals: u64,
    phase_attempts: BTreeMap<MovementPhase, u64>,
    phase_successes: BTreeMap<MovementPhase, u64>,
    phase_failures: BTreeMap<MovementPhase, u64>,
    recent: VecDeque<OperationSummary>,
}

/// Thread-safe movement metrics (REQ-M4-FAIL-004).
#[derive(Debug, Clone, Default)]
pub struct MovementMetrics {
    inner: Arc<Mutex<MovementMetricsInner>>,
}

impl MovementMetrics {
    fn lock(&self) -> std::sync::MutexGuard<'_, MovementMetricsInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record_start(&self) {
        self.lock().operations_started += 1;
    }

    fn record_phase_attempt(&self, phase: MovementPhase) {
        let mut inner = self.lock();
        *inner.phase_attempts.entry(phase).or_insert(0) += 1;
    }

    fn record_phase_success(&self, phase: MovementPhase) {
        let mut inner = self.lock();
        *inner.phase_successes.entry(phase).or_insert(0) += 1;
    }

    fn record_phase_failure(&self, phase: MovementPhase) {
        let mut inner = self.lock();
        *inner.phase_failures.entry(phase).or_insert(0) += 1;
    }

    fn record_publish(&self) {
        self.lock().publishes += 1;
    }

    fn record_cancel(&self) {
        self.lock().cancels += 1;
    }

    fn record_cleanup(&self) {
        self.lock().cleanups += 1;
    }

    fn record_deferral(&self) {
        self.lock().admission_deferrals += 1;
    }

    fn record_terminal(&self, summary: OperationSummary) {
        let mut inner = self.lock();
        match summary.outcome {
            MovementOutcome::Completed => inner.operations_completed += 1,
            MovementOutcome::FailClosed => inner.operations_fail_closed += 1,
            _ => {}
        }
        inner.recent.push_back(summary);
        while inner.recent.len() > MAX_RECENT_OPERATIONS {
            inner.recent.pop_front();
        }
    }

    pub fn snapshot(&self) -> MovementMetricsSnapshot {
        let inner = self.lock();
        MovementMetricsSnapshot {
            operations_started: inner.operations_started,
            operations_completed: inner.operations_completed,
            operations_fail_closed: inner.operations_fail_closed,
            publishes: inner.publishes,
            cancels: inner.cancels,
            cleanups: inner.cleanups,
            admission_deferrals: inner.admission_deferrals,
            phase_attempts: inner.phase_attempts.clone(),
            phase_successes: inner.phase_successes.clone(),
            phase_failures: inner.phase_failures.clone(),
            recent: inner.recent.iter().cloned().collect(),
        }
    }
}

/// Configuration for the movement driver.
#[derive(Clone, Debug)]
pub struct MovementDriverConfig {
    /// This node's Raft id (used for the local cleanup safety check).
    pub local_node_id: RaftNodeId,
    /// The node id movement work is admitted against.
    pub work_node_id: RaftNodeId,
    /// Maximum drive-loop iterations per `drive_shard` call.
    pub max_attempts_per_drive: u32,
    /// Delay between retryable failures.
    pub retry_delay: Duration,
    /// Delay after observing a settle state.
    pub settle_delay: Duration,
    /// Tombstone store capacity.
    pub tombstone_capacity: usize,
}

impl Default for MovementDriverConfig {
    fn default() -> Self {
        Self {
            local_node_id: 0,
            work_node_id: 0,
            max_attempts_per_drive: 128,
            retry_delay: Duration::from_millis(250),
            settle_delay: Duration::from_millis(500),
            tombstone_capacity: 256,
        }
    }
}

/// Outcome of one `drive_shard` call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DriverOutcome {
    NoPendingMovement,
    Completed {
        operation_id: [u8; 16],
        phases_advanced: u32,
    },
    Cancelled {
        operation_id: [u8; 16],
    },
    DeferredAdmission,
    FailClosed {
        operation_id: [u8; 16],
        reason: String,
    },
    TransientFailure {
        reason: String,
    },
}

/// Outcome of a cancel request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    NothingToCancel,
    Cancelled,
    MustFinishForward {
        operation_id: [u8; 16],
        phase: MovementPhase,
    },
    AmbiguousMembership {
        operation_id: [u8; 16],
    },
    CatalogUnavailable(String),
}

/// Durable, level-triggered movement reconciler.
///
/// One `drive_shard` call advances the shard's pending movement as far as the
/// current observation allows, then returns; the caller (operator loop or
/// on-demand request) re-invokes it until the movement is gone. Every state
/// change goes through the catalog's expected-epoch compare-and-swap, so a
/// crash or a concurrent controller can only ever retry the same intent.
pub struct MovementDriver<O, C> {
    config: MovementDriverConfig,
    operator: O,
    catalog: C,
    janitor: Option<Arc<dyn LocalReplicaJanitor>>,
    admission: MovementWorkAdmission,
    tombstones: TombstoneStore,
    metrics: MovementMetrics,
}

impl<O, C> MovementDriver<O, C>
where
    O: MembershipOperator,
    C: CatalogPort,
{
    pub fn new(
        config: MovementDriverConfig,
        operator: O,
        catalog: C,
        janitor: Option<Arc<dyn LocalReplicaJanitor>>,
        admission: MovementWorkAdmission,
    ) -> Self {
        let tombstones = TombstoneStore::new(config.tombstone_capacity);
        Self {
            config,
            operator,
            catalog,
            janitor,
            admission,
            tombstones,
            metrics: MovementMetrics::default(),
        }
    }

    pub fn metrics(&self) -> MovementMetrics {
        self.metrics.clone()
    }

    /// Borrow the catalog port. The M4-T5 rebalancing scheduler commits
    /// movement intents through the same port the driver reconciles, so a
    /// single controller owns both sides of the intent lifecycle.
    pub fn catalog(&self) -> &C {
        &self.catalog
    }

    pub async fn tombstone(
        &self,
        shard_id: u16,
        operation_id: &[u8; 16],
    ) -> Option<MovementTombstone> {
        self.tombstones.get(shard_id, operation_id).await
    }

    /// Drive one shard's pending movement forward.
    pub async fn drive_shard(&self, shard_id: u16) -> DriverOutcome {
        let started = Instant::now();
        let outcome = self.drive_shard_inner(shard_id, started).await;
        if let DriverOutcome::Completed { operation_id, .. } = &outcome {
            self.tombstones
                .tombstone(
                    shard_id,
                    operation_id,
                    TombstoneOutcome::Completed,
                    "movement published",
                )
                .await;
        }
        outcome
    }

    async fn drive_shard_inner(&self, shard_id: u16, started: Instant) -> DriverOutcome {
        let state = match self.catalog.read_committed().await {
            Ok(state) => state,
            Err(error) => {
                return DriverOutcome::TransientFailure {
                    reason: format!("catalog read failed: {error}"),
                };
            }
        };
        let placement = match state.placements.get(&shard_id) {
            Some(placement) => placement.clone(),
            None => {
                return DriverOutcome::TransientFailure {
                    reason: format!("shard {shard_id} has no committed placement"),
                };
            }
        };
        let mut pending = match placement.pending_movement.clone() {
            Some(pending) => pending,
            None => return DriverOutcome::NoPendingMovement,
        };
        if self
            .tombstones
            .get(shard_id, &pending.operation_id)
            .await
            .is_some()
        {
            // A tombstoned operation must never be re-driven; if it is still
            // pending in the catalog, fail closed loudly.
            return self
                .fail_closed(
                    shard_id,
                    &pending,
                    state.placement_epoch,
                    0,
                    started,
                    "operation is tombstoned but still pending in the catalog".to_string(),
                )
                .await;
        }
        let eligible = state.eligible_nodes.clone();
        let mut epoch = state.placement_epoch;

        let _permit = match self.admission.try_admit(self.config.work_node_id) {
            Ok(permit) => permit,
            Err(_) => {
                self.metrics.record_deferral();
                return DriverOutcome::DeferredAdmission;
            }
        };

        self.metrics.record_start();
        let operation_id = pending.operation_id;
        let mut phases_advanced = 0u32;
        let mut attempts = 0u32;

        loop {
            if attempts >= self.config.max_attempts_per_drive {
                let reason = "attempt budget exhausted".to_string();
                self.record_note(shard_id, &pending, epoch, false, &reason)
                    .await;
                return self.transient(
                    shard_id,
                    operation_id,
                    phases_advanced,
                    attempts,
                    started,
                    reason,
                );
            }
            attempts += 1;

            let observed = match self.operator.observe().await {
                Ok(observed) => observed,
                Err(error) => {
                    self.metrics.record_phase_failure(pending.phase);
                    self.record_note(
                        shard_id,
                        &pending,
                        epoch,
                        false,
                        &format!("observe failed: {error}"),
                    )
                    .await;
                    sleep(self.config.retry_delay).await;
                    continue;
                }
            };

            let action = decide(&pending, &observed);
            match action {
                ReconcilerAction::AddLearner { node } => {
                    let Some(endpoint) = learner_endpoint(&eligible, node) else {
                        return self
                            .fail_closed(
                                shard_id,
                                &pending,
                                epoch,
                                attempts,
                                started,
                                format!("learner node {node} has no catalog endpoint"),
                            )
                            .await;
                    };
                    self.metrics.record_phase_attempt(MovementPhase::Learner);
                    match self.operator.add_learner(endpoint, false).await {
                        Ok(()) => {
                            self.record_note(shard_id, &pending, epoch, true, "learner admitted")
                                .await;
                            match self
                                .advance_toward(
                                    shard_id,
                                    &mut pending,
                                    &mut epoch,
                                    MovementPhase::Learner,
                                )
                                .await
                            {
                                Ok(advanced) => phases_advanced += advanced,
                                Err(outcome) => return outcome,
                            }
                        }
                        Err(error) => {
                            self.on_operator_error(
                                shard_id,
                                &pending,
                                epoch,
                                MovementPhase::Learner,
                                &error,
                            )
                            .await;
                            sleep(self.config.retry_delay).await;
                        }
                    }
                }
                ReconcilerAction::WaitForLearnerCatchUp { node } => {
                    let Some(endpoint) = learner_endpoint(&eligible, node) else {
                        return self
                            .fail_closed(
                                shard_id,
                                &pending,
                                epoch,
                                attempts,
                                started,
                                format!("learner node {node} has no catalog endpoint"),
                            )
                            .await;
                    };
                    self.metrics.record_phase_attempt(MovementPhase::CatchUp);
                    // Blocking call: OpenRaft waits until the learner has
                    // caught up with the log; only then is promotion safe.
                    match self.operator.add_learner(endpoint, true).await {
                        Ok(()) => {
                            self.record_note(
                                shard_id,
                                &pending,
                                epoch,
                                true,
                                "learner caught up with the log",
                            )
                            .await;
                            // The blocking proof completes the CatchUp phase;
                            // if we are already there, move on to Promote.
                            let target = if pending.phase == MovementPhase::CatchUp {
                                MovementPhase::Promote
                            } else {
                                MovementPhase::CatchUp
                            };
                            match self
                                .advance_toward(shard_id, &mut pending, &mut epoch, target)
                                .await
                            {
                                Ok(advanced) => phases_advanced += advanced,
                                Err(outcome) => return outcome,
                            }
                        }
                        Err(error) => {
                            self.on_operator_error(
                                shard_id,
                                &pending,
                                epoch,
                                MovementPhase::CatchUp,
                                &error,
                            )
                            .await;
                            sleep(self.config.retry_delay).await;
                        }
                    }
                }
                ReconcilerAction::PromoteLearner { node } => {
                    self.metrics.record_phase_attempt(MovementPhase::Promote);
                    match self.operator.promote_learner(node).await {
                        Ok(()) => {
                            self.record_note(
                                shard_id,
                                &pending,
                                epoch,
                                true,
                                "learner promoted to voter",
                            )
                            .await;
                            match self
                                .advance_toward(
                                    shard_id,
                                    &mut pending,
                                    &mut epoch,
                                    MovementPhase::Promote,
                                )
                                .await
                            {
                                Ok(advanced) => phases_advanced += advanced,
                                Err(outcome) => return outcome,
                            }
                        }
                        Err(error) => {
                            self.on_operator_error(
                                shard_id,
                                &pending,
                                epoch,
                                MovementPhase::Promote,
                                &error,
                            )
                            .await;
                            sleep(self.config.retry_delay).await;
                        }
                    }
                }
                ReconcilerAction::WaitForLeadership => {
                    self.metrics.record_phase_attempt(MovementPhase::Lead);
                    match self.operator.is_leader().await {
                        Ok(_) => {
                            // The pinned OpenRaft API exposes no leadership
                            // transfer primitive, so the Lead phase observes
                            // rather than moves leadership: a stable leader
                            // anywhere is the steady state.
                            self.record_note(
                                shard_id,
                                &pending,
                                epoch,
                                true,
                                "leadership observed; no transfer primitive available",
                            )
                            .await;
                            // One step: Promote -> Lead, or Lead -> Remove.
                            let target = match pending.phase {
                                MovementPhase::Promote => MovementPhase::Lead,
                                MovementPhase::Lead => MovementPhase::Remove,
                                other => {
                                    return self
                                        .fail_closed(
                                            shard_id,
                                            &pending,
                                            epoch,
                                            attempts,
                                            started,
                                            format!(
                                                "leadership observed in unexpected phase {other:?}"
                                            ),
                                        )
                                        .await;
                                }
                            };
                            match self
                                .advance_toward(shard_id, &mut pending, &mut epoch, target)
                                .await
                            {
                                Ok(advanced) => phases_advanced += advanced,
                                Err(outcome) => return outcome,
                            }
                        }
                        Err(error) => {
                            self.on_operator_error(
                                shard_id,
                                &pending,
                                epoch,
                                MovementPhase::Lead,
                                &error,
                            )
                            .await;
                            sleep(self.config.retry_delay).await;
                        }
                    }
                }
                ReconcilerAction::RemoveVoter { node } => {
                    self.metrics.record_phase_attempt(MovementPhase::Remove);
                    match self.operator.remove_voter(node).await {
                        Ok(()) => {
                            self.record_note(shard_id, &pending, epoch, true, "old voter removed")
                                .await;
                            // No catalog phase advance: removal commits the
                            // target membership, so the next observation
                            // decides Publish.
                        }
                        Err(error) => {
                            self.on_operator_error(
                                shard_id,
                                &pending,
                                epoch,
                                MovementPhase::Remove,
                                &error,
                            )
                            .await;
                            sleep(self.config.retry_delay).await;
                        }
                    }
                }
                ReconcilerAction::Publish => {
                    self.metrics.record_phase_attempt(MovementPhase::Publish);
                    let operation_id = pending.operation_id;
                    let target_voters = pending.target_voters;
                    let command = |epoch: PlacementEpoch| CatalogCommand::PublishMovement {
                        expected_epoch: epoch,
                        operation_id,
                        shard_id,
                        observed_voters: target_voters,
                    };
                    match self.submit_with_refresh(command, epoch, operation_id).await {
                        Ok(CatalogResponse::MovementPublished { .. }) => {
                            self.metrics.record_publish();
                            self.metrics.record_phase_success(MovementPhase::Publish);
                            self.cleanup_local_replica(shard_id, &pending).await;
                            return self.completed(
                                shard_id,
                                operation_id,
                                phases_advanced,
                                attempts,
                                started,
                            );
                        }
                        Ok(unexpected) => {
                            self.metrics.record_phase_failure(MovementPhase::Publish);
                            return self.transient(
                                shard_id,
                                operation_id,
                                phases_advanced,
                                attempts,
                                started,
                                format!("publish returned unexpected response: {unexpected:?}"),
                            );
                        }
                        Err(outcome) => return outcome,
                    }
                }
                ReconcilerAction::WaitForSettle => {
                    sleep(self.config.settle_delay).await;
                }
                ReconcilerAction::FailClosed { reason } => {
                    return self
                        .fail_closed(shard_id, &pending, epoch, attempts, started, reason)
                        .await;
                }
            }
        }
    }

    /// Request cancellation of the shard's pending movement.
    ///
    /// Safe only while the observed committed voters still equal the source
    /// membership; after the membership committed, the movement must finish
    /// forward (REQ-M4-MOVE-005).
    pub async fn request_cancel(&self, shard_id: u16, reason: String) -> CancelOutcome {
        let state = match self.catalog.read_committed().await {
            Ok(state) => state,
            Err(error) => return CancelOutcome::CatalogUnavailable(error.to_string()),
        };
        let placement = match state.placements.get(&shard_id) {
            Some(placement) => placement.clone(),
            None => return CancelOutcome::CatalogUnavailable(format!("shard {shard_id} unknown")),
        };
        let pending = match placement.pending_movement.clone() {
            Some(pending) => pending,
            None => return CancelOutcome::NothingToCancel,
        };
        let observed = match self.operator.observe().await {
            Ok(observed) => observed,
            Err(error) => return CancelOutcome::CatalogUnavailable(error.to_string()),
        };
        let source: BTreeSet<RaftNodeId> = pending.source_voters.iter().copied().collect();
        let target: BTreeSet<RaftNodeId> = pending.target_voters.iter().copied().collect();
        let epoch = state.placement_epoch;

        if observed.voters == source && !observed.membership_changing {
            let operation_id = pending.operation_id;
            let source_voters = pending.source_voters;
            let command = |epoch: PlacementEpoch| CatalogCommand::CancelMovement {
                expected_epoch: epoch,
                operation_id,
                shard_id,
                observed_voters: source_voters,
                reason: truncate_note(reason.clone()),
            };
            match self.submit_with_refresh(command, epoch, operation_id).await {
                Ok(CatalogResponse::MovementCancelled) => {
                    self.metrics.record_cancel();
                    self.tombstones
                        .tombstone(
                            shard_id,
                            &pending.operation_id,
                            TombstoneOutcome::Cancelled,
                            reason,
                        )
                        .await;
                    CancelOutcome::Cancelled
                }
                Err(DriverOutcome::Cancelled { .. }) => CancelOutcome::NothingToCancel,
                Err(DriverOutcome::TransientFailure { reason }) => {
                    CancelOutcome::CatalogUnavailable(reason)
                }
                Err(_) | Ok(_) => CancelOutcome::CatalogUnavailable(
                    "cancel returned an unexpected result".to_string(),
                ),
            }
        } else if observed.voters == target {
            CancelOutcome::MustFinishForward {
                operation_id: pending.operation_id,
                phase: pending.phase,
            }
        } else if added_voter(&pending).is_some_and(|added| {
            let mut promoted = source.clone();
            promoted.insert(added);
            observed.voters == promoted
        }) {
            // Promotion already committed: the added voter is authoritative
            // in the Raft group, so cancellation must finish forward
            // (remove the old voter, then publish) rather than unwind.
            CancelOutcome::MustFinishForward {
                operation_id: pending.operation_id,
                phase: pending.phase,
            }
        } else {
            CancelOutcome::AmbiguousMembership {
                operation_id: pending.operation_id,
            }
        }
    }

    /// Best-effort retry/error note; never fails the drive loop.
    async fn record_note(
        &self,
        shard_id: u16,
        pending: &PendingMovement,
        epoch: PlacementEpoch,
        succeeded: bool,
        note: &str,
    ) {
        let operation_id = pending.operation_id;
        let command = |epoch: PlacementEpoch| CatalogCommand::RecordMovementAttempt {
            expected_epoch: epoch,
            operation_id,
            shard_id,
            succeeded,
            note: Some(truncate_note(note.to_string())),
        };
        let _ = self.submit_with_refresh(command, epoch, operation_id).await;
    }

    /// Advance the catalog phase step by step toward `target`, returning the
    /// number of committed phase transitions. Never moves backward and never
    /// overshoots: both fail closed instead of recording a phase whose work
    /// was not done.
    async fn advance_toward(
        &self,
        shard_id: u16,
        pending: &mut PendingMovement,
        epoch: &mut PlacementEpoch,
        target: MovementPhase,
    ) -> Result<u32, DriverOutcome> {
        let mut advanced = 0u32;
        while pending.phase != target {
            let Some(next) = next_phase(pending.phase) else {
                return Err(DriverOutcome::FailClosed {
                    operation_id: pending.operation_id,
                    reason: format!("cannot advance from {:?} toward {target:?}", pending.phase),
                });
            };
            if next > target {
                return Err(DriverOutcome::FailClosed {
                    operation_id: pending.operation_id,
                    reason: format!(
                        "advancing from {:?} would overshoot target {target:?}",
                        pending.phase
                    ),
                });
            }
            self.advance_one(shard_id, pending, *epoch, next).await?;
            advanced += 1;
        }
        Ok(advanced)
    }

    async fn advance_one(
        &self,
        shard_id: u16,
        pending: &mut PendingMovement,
        epoch: PlacementEpoch,
        phase: MovementPhase,
    ) -> Result<(), DriverOutcome> {
        let operation_id = pending.operation_id;
        let command = |epoch: PlacementEpoch| CatalogCommand::AdvanceMovementPhase {
            expected_epoch: epoch,
            operation_id,
            shard_id,
            phase,
        };
        match self.submit_with_refresh(command, epoch, operation_id).await {
            Ok(CatalogResponse::PhaseAdvanced { phase: advanced }) => {
                pending.phase = advanced;
                self.metrics.record_phase_success(phase);
                Ok(())
            }
            Ok(unexpected) => Err(DriverOutcome::TransientFailure {
                reason: format!("advance returned unexpected response: {unexpected:?}"),
            }),
            Err(outcome) => Err(outcome),
        }
    }

    async fn on_operator_error(
        &self,
        shard_id: u16,
        pending: &PendingMovement,
        epoch: PlacementEpoch,
        phase: MovementPhase,
        error: &OperatorError,
    ) {
        self.metrics.record_phase_failure(phase);
        let note = match error {
            OperatorError::Fatal(_) | OperatorError::Rejected(_) => {
                // Non-retryable: still surfaced as a note; the level-triggered
                // loop retries on the next drive unless cancelled.
                format!("{phase:?} failed: {error}")
            }
            _ => format!("{phase:?} retryable fault: {error}"),
        };
        self.record_note(shard_id, pending, epoch, false, &note)
            .await;
    }

    async fn fail_closed(
        &self,
        shard_id: u16,
        pending: &PendingMovement,
        epoch: PlacementEpoch,
        attempts: u32,
        started: Instant,
        reason: String,
    ) -> DriverOutcome {
        self.record_note(shard_id, pending, epoch, false, &reason)
            .await;
        self.tombstones
            .tombstone(
                shard_id,
                &pending.operation_id,
                TombstoneOutcome::Abandoned,
                reason.clone(),
            )
            .await;
        self.metrics.record_terminal(OperationSummary {
            operation_id: pending.operation_id,
            shard_id,
            outcome: MovementOutcome::FailClosed,
            phases_advanced: 0,
            attempts,
            duration_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
        DriverOutcome::FailClosed {
            operation_id: pending.operation_id,
            reason,
        }
    }

    fn completed(
        &self,
        shard_id: u16,
        operation_id: [u8; 16],
        phases_advanced: u32,
        attempts: u32,
        started: Instant,
    ) -> DriverOutcome {
        self.metrics.record_terminal(OperationSummary {
            operation_id,
            shard_id,
            outcome: MovementOutcome::Completed,
            phases_advanced,
            attempts,
            duration_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
        DriverOutcome::Completed {
            operation_id,
            phases_advanced,
        }
    }

    fn transient(
        &self,
        shard_id: u16,
        operation_id: [u8; 16],
        phases_advanced: u32,
        attempts: u32,
        started: Instant,
        reason: String,
    ) -> DriverOutcome {
        self.metrics.record_terminal(OperationSummary {
            operation_id,
            shard_id,
            outcome: MovementOutcome::TransientFailure,
            phases_advanced,
            attempts,
            duration_millis: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
        DriverOutcome::TransientFailure { reason }
    }

    /// Submit a catalog command; on a stale-epoch rejection re-read the
    /// committed epoch and retry with it (REQ-M4-FAIL-003).
    async fn submit_with_refresh(
        &self,
        command: impl Fn(PlacementEpoch) -> CatalogCommand,
        mut epoch: PlacementEpoch,
        operation_id: [u8; 16],
    ) -> Result<CatalogResponse, DriverOutcome> {
        for _ in 0..2 {
            match self.catalog.submit(command(epoch)).await {
                Err(CatalogGroupError::InvalidPlacement(PlacementError::StaleCatalogView {
                    actual,
                    ..
                })) => {
                    epoch = actual;
                }
                result => return self.map_submit_result(result, operation_id),
            }
        }
        self.map_submit_result(self.catalog.submit(command(epoch)).await, operation_id)
    }

    fn map_submit_result(
        &self,
        result: Result<CatalogResponse, CatalogGroupError>,
        operation_id: [u8; 16],
    ) -> Result<CatalogResponse, DriverOutcome> {
        match result {
            Ok(response) => Ok(response),
            Err(CatalogGroupError::InvalidPlacement(PlacementError::UnknownMovement {
                ..
            })) => Err(DriverOutcome::Cancelled { operation_id }),
            Err(CatalogGroupError::InvalidPlacement(
                PlacementError::MembershipAlreadyCommitted { .. },
            )) => Err(DriverOutcome::TransientFailure {
                reason: "membership already committed; movement must finish forward".to_string(),
            }),
            Err(error) => Err(DriverOutcome::TransientFailure {
                reason: format!("catalog submit failed: {error}"),
            }),
        }
    }

    /// Cleanup phase: reclaim the removed node's local state only after the
    /// publish is committed and the safety checks pass (REQ-M4-MOVE-006).
    async fn cleanup_local_replica(&self, shard_id: u16, pending: &PendingMovement) {
        let removed: Vec<RaftNodeId> = pending
            .source_voters
            .iter()
            .copied()
            .filter(|node| !pending.target_voters.contains(node))
            .collect();
        if removed.len() != 1 || removed[0] != self.config.local_node_id {
            return;
        }
        // Re-read the committed catalog: the publish must be visible, the
        // movement must be gone, and this node must not be in the new stable
        // voters before any local state is touched.
        let Ok(state) = self.catalog.read_committed().await else {
            return;
        };
        let Some(placement) = state.placements.get(&shard_id) else {
            return;
        };
        if placement.pending_movement.is_some() {
            return;
        }
        if placement.voters.contains(&self.config.local_node_id) {
            return;
        }
        if placement.voters != pending.target_voters {
            return;
        }
        self.tombstones
            .tombstone(
                shard_id,
                &pending.operation_id,
                TombstoneOutcome::Completed,
                format!("published; local replica for shard {shard_id} safe to reclaim"),
            )
            .await;
        if let Some(janitor) = &self.janitor {
            if janitor.remove_local_replica(shard_id).await.is_ok() {
                self.metrics.record_cleanup();
            }
        } else {
            self.metrics.record_cleanup();
        }
    }

    /// Level-triggered replica reconciliation: remove local replicas for
    /// shards whose committed placement no longer includes this node.
    ///
    /// This is the crash-safety backstop for `Publish -> Cleanup`
    /// (REQ-M4-MOVE-006): if the process crashes after publish cleared the
    /// pending movement but before the local replica was reclaimed, no
    /// durable intent remains — but the committed placement still shows this
    /// node is not a voter, so the next scan repairs it. It derives purely
    /// from committed placement plus local state, so it is idempotent and
    /// safe to run on startup and periodically.
    ///
    /// A replica is kept when this node is a stable voter, when a pending
    /// movement targets this node (the replica is a learner catching up),
    /// or when the shard has no committed placement (fail closed).
    pub async fn reconcile_local_replicas(&self) -> Result<ReplicaReconciliation, String> {
        let mut outcome = ReplicaReconciliation::default();
        let Some(janitor) = &self.janitor else {
            return Ok(outcome);
        };
        let state = self
            .catalog
            .read_committed()
            .await
            .map_err(|error| format!("replica scan: catalog read failed: {error}"))?;
        let local_replicas = janitor
            .list_local_replicas()
            .await
            .map_err(|error| format!("replica scan: list failed: {error}"))?;
        outcome.scanned = local_replicas.len();
        for shard_id in local_replicas {
            let Some(placement) = state.placements.get(&shard_id) else {
                outcome.kept.push(shard_id);
                continue;
            };
            if placement.voters.contains(&self.config.local_node_id) {
                outcome.kept.push(shard_id);
                continue;
            }
            let is_movement_target = placement
                .pending_movement
                .as_ref()
                .is_some_and(|pending| added_voter(pending) == Some(self.config.local_node_id));
            if is_movement_target {
                outcome.kept.push(shard_id);
                continue;
            }
            match janitor.remove_local_replica(shard_id).await {
                Ok(()) => {
                    outcome.removed.push(shard_id);
                    self.metrics.record_cleanup();
                }
                Err(error) => outcome.errors.push((shard_id, error)),
            }
        }
        Ok(outcome)
    }
}

/// Outcome of one [`MovementDriver::reconcile_local_replicas`] scan.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaReconciliation {
    pub scanned: usize,
    pub removed: Vec<u16>,
    pub kept: Vec<u16>,
    pub errors: Vec<(u16, String)>,
}

fn learner_endpoint(
    eligible: &BTreeMap<RaftNodeId, EligibleNode>,
    node: RaftNodeId,
) -> Option<LearnerEndpoint> {
    eligible.get(&node).map(|entry| LearnerEndpoint {
        id: node,
        node: RaftNode {
            addr: entry.raft_endpoint.clone(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_movement() -> PendingMovement {
        PendingMovement {
            operation_id: [7; 16],
            source_epoch: 1,
            source_voters: [1, 2, 3],
            target_voters: [1, 2, 4],
            phase: MovementPhase::Intent,
            retries: 0,
            last_error: None,
        }
    }

    fn with_phase(mut pending: PendingMovement, phase: MovementPhase) -> PendingMovement {
        pending.phase = phase;
        pending
    }

    fn observed(voters: &[RaftNodeId], learners: &[RaftNodeId]) -> ObservedMembership {
        ObservedMembership {
            voters: voters.iter().copied().collect(),
            learners: learners.iter().copied().collect(),
            membership_changing: false,
            term: 3,
        }
    }

    #[test]
    fn decide_admits_learner_when_membership_matches_source() {
        assert_eq!(
            decide(&pending_movement(), &observed(&[1, 2, 3], &[])),
            ReconcilerAction::AddLearner { node: 4 }
        );
    }

    #[test]
    fn decide_waits_for_learner_catch_up_once_admitted() {
        assert_eq!(
            decide(&pending_movement(), &observed(&[1, 2, 3], &[4])),
            ReconcilerAction::WaitForLearnerCatchUp { node: 4 }
        );
    }

    #[test]
    fn decide_promotes_once_learner_caught_up() {
        let pending = with_phase(pending_movement(), MovementPhase::Promote);
        assert_eq!(
            decide(&pending, &observed(&[1, 2, 3], &[4])),
            ReconcilerAction::PromoteLearner { node: 4 }
        );
    }

    #[test]
    fn decide_observes_leadership_after_promotion_committed() {
        let pending = with_phase(pending_movement(), MovementPhase::Promote);
        assert_eq!(
            decide(&pending, &observed(&[1, 2, 3, 4], &[])),
            ReconcilerAction::WaitForLeadership
        );
    }

    #[test]
    fn decide_removes_old_voter_after_leadership() {
        let pending = with_phase(pending_movement(), MovementPhase::Remove);
        assert_eq!(
            decide(&pending, &observed(&[1, 2, 3, 4], &[])),
            ReconcilerAction::RemoveVoter { node: 3 }
        );
    }

    #[test]
    fn decide_publishes_once_target_is_committed() {
        for phase in [
            MovementPhase::Intent,
            MovementPhase::Promote,
            MovementPhase::Remove,
        ] {
            let pending = with_phase(pending_movement(), phase);
            assert_eq!(
                decide(&pending, &observed(&[1, 2, 4], &[])),
                ReconcilerAction::Publish,
                "phase {phase:?}"
            );
        }
    }

    #[test]
    fn decide_settles_while_joint_config_is_in_effect() {
        let mut membership = observed(&[1, 2, 3, 4], &[]);
        membership.membership_changing = true;
        assert_eq!(
            decide(&pending_movement(), &membership),
            ReconcilerAction::WaitForSettle
        );
    }

    #[test]
    fn decide_fails_closed_on_ambiguous_membership() {
        let action = decide(&pending_movement(), &observed(&[1, 9, 9], &[]));
        assert!(matches!(action, ReconcilerAction::FailClosed { .. }));
    }

    #[test]
    fn decide_fails_closed_when_voters_left_source_early() {
        let pending = with_phase(pending_movement(), MovementPhase::Learner);
        let action = decide(&pending, &observed(&[1, 2, 3, 4], &[]));
        assert!(matches!(action, ReconcilerAction::FailClosed { .. }));
    }

    #[test]
    fn decide_fails_closed_on_multi_voter_intent() {
        let mut pending = pending_movement();
        pending.target_voters = [1, 5, 6];
        let action = decide(&pending, &observed(&[1, 2, 3], &[]));
        assert!(matches!(action, ReconcilerAction::FailClosed { .. }));
    }

    #[test]
    fn tombstone_store_is_bounded() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = TombstoneStore::new(2);
            store
                .tombstone(1, &[1; 16], TombstoneOutcome::Cancelled, "a")
                .await;
            store
                .tombstone(1, &[2; 16], TombstoneOutcome::Completed, "b")
                .await;
            store
                .tombstone(1, &[3; 16], TombstoneOutcome::Abandoned, "c")
                .await;
            assert_eq!(store.len().await, 2);
            assert!(store.get(1, &[1; 16]).await.is_none());
            let tombstone = store.get(1, &[2; 16]).await.unwrap();
            assert_eq!(tombstone.outcome, TombstoneOutcome::Completed);
        });
    }

    #[test]
    fn truncate_note_respects_byte_bound() {
        assert_eq!(truncate_note("ok".to_string()), "ok");
        let long = "é".repeat(MAX_MOVEMENT_NOTE_BYTES);
        let truncated = truncate_note(long);
        assert!(truncated.len() <= MAX_MOVEMENT_NOTE_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    fn eligible_map() -> BTreeMap<RaftNodeId, EligibleNode> {
        (1..=4)
            .map(|id| {
                (
                    id,
                    EligibleNode {
                        node_id: id,
                        raft_endpoint: format!("node{id}:9000"),
                        failure_domain: format!("d{id}"),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn learner_endpoint_resolves_from_catalog() {
        let map = eligible_map();
        let endpoint = learner_endpoint(&map, 4).unwrap();
        assert_eq!(endpoint.id, 4);
        assert_eq!(endpoint.node.addr, "node4:9000");
        assert!(learner_endpoint(&map, 9).is_none());
    }

    // --- Fake operator/catalog driver tests ---------------------------------

    #[derive(Default)]
    struct FakeOperator {
        observed: tokio::sync::Mutex<ObservedMembership>,
        fail_observe: tokio::sync::Mutex<Option<OperatorError>>,
        added_learners: tokio::sync::Mutex<Vec<(RaftNodeId, bool)>>,
        promoted: tokio::sync::Mutex<Vec<RaftNodeId>>,
        removed: tokio::sync::Mutex<Vec<RaftNodeId>>,
        leader: tokio::sync::Mutex<bool>,
    }

    #[async_trait]
    impl MembershipOperator for FakeOperator {
        async fn observe(&self) -> Result<ObservedMembership, OperatorError> {
            if let Some(error) = self.fail_observe.lock().await.clone() {
                return Err(error);
            }
            Ok(self.observed.lock().await.clone())
        }

        async fn add_learner(
            &self,
            learner: LearnerEndpoint,
            blocking: bool,
        ) -> Result<(), OperatorError> {
            self.added_learners
                .lock()
                .await
                .push((learner.id, blocking));
            // Admission: the learner joins the committed learner set.
            self.observed.lock().await.learners.insert(learner.id);
            Ok(())
        }

        async fn promote_learner(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
            let mut observed = self.observed.lock().await;
            observed.learners.remove(&voter);
            observed.voters.insert(voter);
            self.promoted.lock().await.push(voter);
            Ok(())
        }

        async fn remove_voter(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
            self.observed.lock().await.voters.remove(&voter);
            self.removed.lock().await.push(voter);
            Ok(())
        }

        async fn is_leader(&self) -> Result<bool, OperatorError> {
            Ok(*self.leader.lock().await)
        }
    }

    struct FakeCatalog {
        catalog: tokio::sync::Mutex<crate::placement::PlacementCatalog>,
        source_voters: [RaftNodeId; 3],
        target_voters: [RaftNodeId; 3],
    }

    impl FakeCatalog {
        fn with_movement(phase: MovementPhase) -> Self {
            let mut catalog = crate::placement::PlacementCatalog::default();
            catalog
                .apply(CatalogCommand::Bootstrap {
                    cluster_id: *b"movement-test01!",
                    eligible_nodes: vec![
                        EligibleNode {
                            node_id: 1,
                            raft_endpoint: "node1:9000".to_string(),
                            failure_domain: "a".to_string(),
                        },
                        EligibleNode {
                            node_id: 2,
                            raft_endpoint: "node2:9000".to_string(),
                            failure_domain: "b".to_string(),
                        },
                        EligibleNode {
                            node_id: 3,
                            raft_endpoint: "node3:9000".to_string(),
                            failure_domain: "c".to_string(),
                        },
                        EligibleNode {
                            node_id: 4,
                            raft_endpoint: "node4:9000".to_string(),
                            failure_domain: "d".to_string(),
                        },
                    ],
                })
                .unwrap();
            // Derive a valid intent from the actual golden placement: swap
            // the highest stable voter for the spare eligible node.
            let state = catalog.state().unwrap();
            let source_voters: [RaftNodeId; 3] = state.placements.get(&5).unwrap().voters;
            let removed = source_voters[2];
            let spare = state
                .eligible_nodes
                .keys()
                .copied()
                .find(|node| !source_voters.contains(node))
                .unwrap();
            let mut target: Vec<RaftNodeId> = source_voters
                .iter()
                .copied()
                .filter(|node| *node != removed)
                .chain(std::iter::once(spare))
                .collect();
            target.sort_unstable();
            let target_voters: [RaftNodeId; 3] = target.try_into().ok().unwrap();
            catalog
                .apply(CatalogCommand::BeginMovement {
                    expected_epoch: 1,
                    operation_id: [7; 16],
                    shard_id: 5,
                    target_voters,
                })
                .unwrap();
            for next in phases_up_to(phase) {
                let epoch = catalog.state().unwrap().placement_epoch;
                catalog
                    .apply(CatalogCommand::AdvanceMovementPhase {
                        expected_epoch: epoch,
                        operation_id: [7; 16],
                        shard_id: 5,
                        phase: next,
                    })
                    .unwrap();
            }
            Self {
                catalog: tokio::sync::Mutex::new(catalog),
                source_voters,
                target_voters,
            }
        }

        fn added(&self) -> RaftNodeId {
            self.target_voters
                .iter()
                .copied()
                .find(|node| !self.source_voters.contains(node))
                .unwrap()
        }

        fn removed(&self) -> RaftNodeId {
            self.source_voters
                .iter()
                .copied()
                .find(|node| !self.target_voters.contains(node))
                .unwrap()
        }
    }

    fn phases_up_to(phase: MovementPhase) -> Vec<MovementPhase> {
        let order = [
            MovementPhase::Learner,
            MovementPhase::CatchUp,
            MovementPhase::Promote,
            MovementPhase::Lead,
            MovementPhase::Remove,
        ];
        match order.iter().position(|candidate| *candidate == phase) {
            Some(pos) => order[..=pos].to_vec(),
            // Intent/Publish/Cleanup: no advancement from the initial intent.
            None => Vec::new(),
        }
    }

    #[async_trait]
    impl CatalogPort for FakeCatalog {
        async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
            self.catalog
                .lock()
                .await
                .state()
                .cloned()
                .ok_or(CatalogGroupError::MissingCommittedState)
        }

        async fn submit(
            &self,
            command: CatalogCommand,
        ) -> Result<CatalogResponse, CatalogGroupError> {
            let mut catalog = self.catalog.lock().await;
            catalog.apply(command).map_err(CatalogGroupError::from)
        }
    }

    struct FakeJanitor {
        removed: tokio::sync::Mutex<Vec<u16>>,
        local_replicas: tokio::sync::Mutex<Vec<u16>>,
    }

    impl FakeJanitor {
        fn with_replicas(replicas: Vec<u16>) -> Self {
            Self {
                removed: tokio::sync::Mutex::new(Vec::new()),
                local_replicas: tokio::sync::Mutex::new(replicas),
            }
        }
    }

    #[async_trait]
    impl LocalReplicaJanitor for FakeJanitor {
        async fn remove_local_replica(&self, shard_id: u16) -> Result<(), String> {
            self.removed.lock().await.push(shard_id);
            self.local_replicas.lock().await.retain(|s| *s != shard_id);
            Ok(())
        }

        async fn list_local_replicas(&self) -> Result<Vec<u16>, String> {
            Ok(self.local_replicas.lock().await.clone())
        }
    }

    fn driver_config(local: RaftNodeId) -> MovementDriverConfig {
        MovementDriverConfig {
            local_node_id: local,
            work_node_id: local,
            max_attempts_per_drive: 64,
            retry_delay: Duration::from_millis(1),
            settle_delay: Duration::from_millis(1),
            tombstone_capacity: 16,
        }
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn admission() -> MovementWorkAdmission {
        MovementWorkAdmission::new(crate::movement_admission::MovementWorkConfig {
            cluster_max_concurrent: 4,
            per_node_max_concurrent: 2,
        })
        .unwrap()
    }

    #[test]
    fn driver_runs_full_movement_to_publish() {
        test_runtime().block_on(async {
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let source = catalog.source_voters;
            let target = catalog.target_voters;
            let added = catalog.added();
            let removed = catalog.removed();
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&source, &[])),
                leader: tokio::sync::Mutex::new(true),
                ..FakeOperator::default()
            };
            let janitor = Arc::new(FakeJanitor::with_replicas(vec![5]));
            // Local node is the removed voter: its replica must be reclaimed
            // only after publication.
            let driver = MovementDriver::new(
                driver_config(removed),
                operator,
                catalog,
                Some(janitor.clone()),
                admission(),
            );

            let outcome = driver.drive_shard(5).await;
            assert!(
                matches!(
                    outcome,
                    DriverOutcome::Completed { phases_advanced, .. } if phases_advanced >= 4
                ),
                "unexpected outcome: {outcome:?}"
            );

            // Publish replaced the stable placement and bumped the epoch.
            let state = driver.catalog.read_committed().await.unwrap();
            assert_eq!(state.placement_epoch, 2);
            let placement = state.placements.get(&5).unwrap();
            assert_eq!(placement.voters, target);
            assert!(placement.pending_movement.is_none());

            // The fake operator saw the full phase sequence.
            let added_calls = driver.operator.added_learners.lock().await.clone();
            assert!(added_calls.contains(&(added, false)));
            assert!(added_calls.contains(&(added, true)));
            assert_eq!(*driver.operator.promoted.lock().await, vec![added]);
            assert_eq!(*driver.operator.removed.lock().await, vec![removed]);

            // The removed local replica was reclaimed after publication.
            assert_eq!(*janitor.removed.lock().await, vec![5]);
            assert!(driver.tombstone(5, &[7; 16]).await.is_some());

            let metrics = driver.metrics().snapshot();
            assert_eq!(metrics.publishes, 1);
            assert_eq!(metrics.operations_completed, 1);
            assert_eq!(metrics.cleanups, 1);
            assert_eq!(metrics.recent.len(), 1);
            assert_eq!(metrics.recent[0].outcome, MovementOutcome::Completed);
        });
    }

    #[test]
    fn driver_does_not_reclaim_remote_replica() {
        test_runtime().block_on(async {
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let target = catalog.target_voters;
            // Local node stays a voter: no cleanup must happen.
            let stayer = catalog.source_voters[0];
            assert!(target.contains(&stayer));
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&target, &[])),
                ..FakeOperator::default()
            };
            let janitor = Arc::new(FakeJanitor::with_replicas(vec![5]));
            let driver = MovementDriver::new(
                driver_config(stayer),
                operator,
                catalog,
                Some(janitor.clone()),
                admission(),
            );

            let outcome = driver.drive_shard(5).await;
            assert!(matches!(outcome, DriverOutcome::Completed { .. }));
            assert!(janitor.removed.lock().await.is_empty());
            assert_eq!(driver.metrics().snapshot().cleanups, 0);
        });
    }

    #[test]
    fn driver_fails_closed_on_ambiguous_membership() {
        test_runtime().block_on(async {
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&[9], &[])),
                ..FakeOperator::default()
            };
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());

            let outcome = driver.drive_shard(5).await;
            assert!(matches!(outcome, DriverOutcome::FailClosed { .. }));
            // Nothing was published: the stable placement is untouched.
            let state = driver.catalog.read_committed().await.unwrap();
            assert_eq!(state.placement_epoch, 1);
            let source = driver.catalog.source_voters;
            assert_eq!(state.placements.get(&5).unwrap().voters, source);
            assert!(driver.tombstone(5, &[7; 16]).await.is_some());
            assert_eq!(driver.metrics().snapshot().operations_fail_closed, 1);
        });
    }

    #[test]
    fn driver_retries_transient_observe_failures() {
        test_runtime().block_on(async {
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let target = catalog.target_voters;
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&target, &[])),
                fail_observe: tokio::sync::Mutex::new(Some(OperatorError::Transient(
                    "flaky".to_string(),
                ))),
                ..FakeOperator::default()
            };
            // First drive fails transiently; clear the fault and retry.
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());
            assert!(matches!(
                driver.drive_shard(5).await,
                DriverOutcome::TransientFailure { .. }
            ));
            *driver.operator.fail_observe.lock().await = None;
            assert!(matches!(
                driver.drive_shard(5).await,
                DriverOutcome::Completed { .. }
            ));
        });
    }

    #[test]
    fn driver_reports_no_pending_movement() {
        test_runtime().block_on(async {
            let operator = FakeOperator::default();
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let target = catalog.target_voters;
            // Publish the movement away first.
            catalog
                .submit(CatalogCommand::PublishMovement {
                    expected_epoch: 1,
                    operation_id: [7; 16],
                    shard_id: 5,
                    observed_voters: target,
                })
                .await
                .unwrap();
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());
            assert_eq!(
                driver.drive_shard(5).await,
                DriverOutcome::NoPendingMovement
            );
        });
    }

    #[test]
    fn driver_defers_when_admission_is_saturated() {
        test_runtime().block_on(async {
            let operator = FakeOperator::default();
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let saturated =
                MovementWorkAdmission::new(crate::movement_admission::MovementWorkConfig {
                    cluster_max_concurrent: 1,
                    per_node_max_concurrent: 1,
                })
                .unwrap();
            let _held = saturated.try_admit(1).expect("first permit");
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, saturated);
            assert_eq!(
                driver.drive_shard(5).await,
                DriverOutcome::DeferredAdmission
            );
            assert_eq!(driver.metrics().snapshot().admission_deferrals, 1);
        });
    }

    #[test]
    fn cancel_before_commit_leaves_stable_authoritative() {
        test_runtime().block_on(async {
            let catalog = FakeCatalog::with_movement(MovementPhase::CatchUp);
            let source = catalog.source_voters;
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&source, &[])),
                ..FakeOperator::default()
            };
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());

            assert_eq!(
                driver.request_cancel(5, "target drained".to_string()).await,
                CancelOutcome::Cancelled
            );
            let state = driver.catalog.read_committed().await.unwrap();
            assert_eq!(state.placement_epoch, 1);
            assert!(state.placements.get(&5).unwrap().pending_movement.is_none());
            let tombstone = driver.tombstone(5, &[7; 16]).await.unwrap();
            assert_eq!(tombstone.outcome, TombstoneOutcome::Cancelled);
            assert_eq!(driver.metrics().snapshot().cancels, 1);
        });
    }

    #[test]
    fn cancel_after_commit_must_finish_forward() {
        test_runtime().block_on(async {
            let catalog = FakeCatalog::with_movement(MovementPhase::Promote);
            let target = catalog.target_voters;
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&target, &[])),
                ..FakeOperator::default()
            };
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());

            assert!(matches!(
                driver.request_cancel(5, "too late".to_string()).await,
                CancelOutcome::MustFinishForward {
                    phase: MovementPhase::Promote,
                    ..
                }
            ));
            // The movement is still pending: cancellation did not touch it.
            let state = driver.catalog.read_committed().await.unwrap();
            assert!(state.placements.get(&5).unwrap().pending_movement.is_some());
        });
    }

    #[test]
    fn cancel_with_ambiguous_observation_is_rejected() {
        test_runtime().block_on(async {
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&[9], &[])),
                ..FakeOperator::default()
            };
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());
            assert!(matches!(
                driver.request_cancel(5, "confused".to_string()).await,
                CancelOutcome::AmbiguousMembership { .. }
            ));
        });
    }

    #[test]
    fn cancel_after_promote_must_finish_forward() {
        test_runtime().block_on(async {
            // Promotion committed: the Raft group has source + added as
            // voters. Cancelling must finish forward, not unwind.
            let catalog = FakeCatalog::with_movement(MovementPhase::Promote);
            let source: Vec<RaftNodeId> = catalog.source_voters.to_vec();
            let added = catalog
                .target_voters
                .iter()
                .copied()
                .find(|node| !catalog.source_voters.contains(node))
                .unwrap();
            let mut promoted = source.clone();
            promoted.push(added);
            let operator = FakeOperator {
                observed: tokio::sync::Mutex::new(observed(&promoted, &[])),
                ..FakeOperator::default()
            };
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());
            assert!(matches!(
                driver.request_cancel(5, "too late".to_string()).await,
                CancelOutcome::MustFinishForward { .. }
            ));
        });
    }

    #[test]
    fn cancel_with_nothing_pending_is_a_no_op() {
        test_runtime().block_on(async {
            let operator = FakeOperator::default();
            let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
            let target = catalog.target_voters;
            catalog
                .submit(CatalogCommand::PublishMovement {
                    expected_epoch: 1,
                    operation_id: [7; 16],
                    shard_id: 5,
                    observed_voters: target,
                })
                .await
                .unwrap();
            let driver: MovementDriver<FakeOperator, FakeCatalog> =
                MovementDriver::new(driver_config(1), operator, catalog, None, admission());
            assert_eq!(
                driver.request_cancel(5, "nothing".to_string()).await,
                CancelOutcome::NothingToCancel
            );
        });
    }

    async fn published_catalog() -> (FakeCatalog, RaftNodeId, [RaftNodeId; 3]) {
        let catalog = FakeCatalog::with_movement(MovementPhase::Intent);
        let target = catalog.target_voters;
        let removed = catalog
            .source_voters
            .iter()
            .copied()
            .find(|node| !target.contains(node))
            .unwrap();
        // Publish straight from Intent: the catalog gates on intent
        // existence and observed target, not on the phase.
        catalog
            .submit(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: [7; 16],
                shard_id: 5,
                observed_voters: target,
            })
            .await
            .unwrap();
        (catalog, removed, target)
    }

    #[test]
    fn replica_scan_removes_stale_replica_after_publish() {
        test_runtime().block_on(async {
            // Simulate crash-after-publish-before-cleanup: the movement is
            // gone, this node is no longer a voter, but its local replica
            // was never reclaimed.
            let (catalog, removed, _target) = published_catalog().await;
            let janitor = Arc::new(FakeJanitor::with_replicas(vec![5]));
            let driver: MovementDriver<FakeOperator, FakeCatalog> = MovementDriver::new(
                driver_config(removed),
                FakeOperator::default(),
                catalog,
                Some(janitor.clone()),
                admission(),
            );
            let outcome = driver.reconcile_local_replicas().await.unwrap();
            assert_eq!(outcome.scanned, 1);
            assert_eq!(outcome.removed, vec![5]);
            assert!(outcome.kept.is_empty());
            assert!(outcome.errors.is_empty());
            assert_eq!(*janitor.removed.lock().await, vec![5]);
            // A second scan is a no-op: the replica is gone.
            let outcome = driver.reconcile_local_replicas().await.unwrap();
            assert_eq!(outcome.scanned, 0);
            assert!(outcome.removed.is_empty());
        });
    }

    #[test]
    fn replica_scan_keeps_learner_replica_while_movement_targets_local() {
        test_runtime().block_on(async {
            // Mid-movement: the local node is the movement target, catching
            // up as a learner. Its replica must survive the scan.
            let catalog = FakeCatalog::with_movement(MovementPhase::CatchUp);
            let added = catalog
                .target_voters
                .iter()
                .copied()
                .find(|node| !catalog.source_voters.contains(node))
                .unwrap();
            let janitor = Arc::new(FakeJanitor::with_replicas(vec![5]));
            let driver: MovementDriver<FakeOperator, FakeCatalog> = MovementDriver::new(
                driver_config(added),
                FakeOperator::default(),
                catalog,
                Some(janitor.clone()),
                admission(),
            );
            let outcome = driver.reconcile_local_replicas().await.unwrap();
            assert_eq!(outcome.scanned, 1);
            assert!(outcome.removed.is_empty());
            assert_eq!(outcome.kept, vec![5]);
            assert!(janitor.removed.lock().await.is_empty());
        });
    }

    #[test]
    fn replica_scan_keeps_voter_and_unknown_shard_replicas() {
        test_runtime().block_on(async {
            let (catalog, _removed, target) = published_catalog().await;
            // Local node is a stable voter for shard 5; shard 5000 is beyond
            // LOGICAL_SHARD_COUNT so it has no committed placement
            // (fail closed: keep).
            let janitor = Arc::new(FakeJanitor::with_replicas(vec![5, 5000]));
            let driver: MovementDriver<FakeOperator, FakeCatalog> = MovementDriver::new(
                driver_config(target[0]),
                FakeOperator::default(),
                catalog,
                Some(janitor.clone()),
                admission(),
            );
            let outcome = driver.reconcile_local_replicas().await.unwrap();
            assert_eq!(outcome.scanned, 2);
            assert!(outcome.removed.is_empty());
            assert_eq!(outcome.kept, vec![5, 5000]);
            assert!(janitor.removed.lock().await.is_empty());
        });
    }

    #[test]
    fn replica_scan_without_janitor_is_a_no_op() {
        test_runtime().block_on(async {
            let (catalog, removed, _target) = published_catalog().await;
            let driver: MovementDriver<FakeOperator, FakeCatalog> = MovementDriver::new(
                driver_config(removed),
                FakeOperator::default(),
                catalog,
                None,
                admission(),
            );
            let outcome = driver.reconcile_local_replicas().await.unwrap();
            assert_eq!(outcome, ReplicaReconciliation::default());
        });
    }
}
