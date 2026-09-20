//! Deterministic rebalancing planner, bounded movement scheduler, topology
//! management surface, and catalog health gate.
//!
//! Traceability:
//!
//! - REQ-M4-BAL-001: the planner is equal-weight and failure-domain-aware;
//!   stable voter counts and desired-leader counts across eligible nodes
//!   differ by at most one when the topology permits it.
//! - REQ-M4-BAL-002: plans are deterministic for the same committed
//!   catalog/topology, avoid unnecessary moves, and never schedule two
//!   replicas of one shard onto one node.
//! - REQ-M4-BAL-003: the scheduler enforces configured cluster-wide and
//!   per-node movement concurrency limits and exposes
//!   queued/running/blocked/failed/completed counts.
//! - REQ-M4-BAL-004: the unavailable-node input is an advisory health view
//!   (gossip/failure detection). It only steers *new* replica placement; it
//!   never mutates committed placement, Raft membership, or leadership.
//! - REQ-M4-BAL-005: the catalog health gate blocks new placement mutations
//!   when catalog quorum is lost while reads of the last committed catalog
//!   view keep working, so data groups continue under their committed
//!   memberships.
//! - REQ-M4-FAIL-001..004: the scheduler recovers partial plans after
//!   restart, blocks (never force-drives) movements whose target is
//!   unavailable, fails closed on conflicting foreign intents, and never
//!   invents authority.
//! - REQ-M4-OPS-001/003: `TopologyView` is a stable serializable management
//!   surface with bounded cardinality (per-node entries, active movements
//!   only).
//!
//! The planner is a pure function over the committed catalog: it proposes,
//! never performs. The scheduler performs through the `RebalanceDrive`
//! trait, which has a blanket implementation over the M4-T4
//! `MovementDriver`, so intent submission and reconciliation stay behind one
//! controller. A planner never mutates authority (design section 4).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use xxhash_rust::xxh3::xxh3_64;

use crate::movement::{
    CancelOutcome, CatalogPort, DriverOutcome, MembershipOperator, MovementDriver,
};
use crate::movement_admission::{MovementWorkAdmission, MovementWorkConfig, MovementWorkMetrics};
#[cfg(test)]
use crate::placement::PlacementCatalog;
use crate::placement::{
    validate_voters, CatalogCommand, CatalogResponse, CatalogState, ClusterId, EligibleNode,
    MovementPhase, PlacementEpoch, PlacementError, ShardPlacement,
};
use crate::placement_raft::CatalogGroupError;
use crate::raft::RaftNodeId;
use crate::raft_observability::{ReplicaHealth, ReplicaRole, ReplicaStatus};

// ---------------------------------------------------------------------------
// Planner
// ---------------------------------------------------------------------------

/// Why the planner proposes moving one replica of a shard.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MoveReason {
    /// A stable voter is unavailable (or left the eligible set); evacuate it.
    UnavailableNodeEviction,
    /// A voter sits on a node above the balanced ceiling while another
    /// eligible node sits below the floor.
    SkewReduction,
    /// The shard's voters do not span three failure domains although the
    /// topology has three.
    DomainRepair,
}

/// One planned voter movement. Exactly one voter changes per movement, which
/// keeps the plan minimal and matches the one-active-movement-per-shard rule
/// (REQ-M4-MOVE-001).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlannedMovement {
    pub shard_id: u16,
    pub operation_id: [u8; 16],
    pub source_voters: [RaftNodeId; 3],
    pub target_voters: [RaftNodeId; 3],
    pub reason: MoveReason,
}

/// One planned advisory desired-leader reassignment (REQ-M4-BAL-001).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeaderReassignment {
    pub shard_id: u16,
    pub from: RaftNodeId,
    pub to: RaftNodeId,
}

/// Deterministic rebalance plan derived from one committed catalog view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RebalancePlan {
    pub cluster_id: ClusterId,
    pub source_epoch: PlacementEpoch,
    /// Ascending shard order.
    pub movements: Vec<PlannedMovement>,
    /// Ascending shard order.
    pub leader_reassignments: Vec<LeaderReassignment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebalanceError {
    NoPlacements,
    InsufficientAvailableNodes { available: usize },
    InvalidSchedulerConfig(String),
}

impl std::fmt::Display for RebalanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPlacements => write!(f, "catalog has no placements to rebalance"),
            Self::InsufficientAvailableNodes { available } => write!(
                f,
                "only {available} nodes available; RF=3 needs at least three"
            ),
            Self::InvalidSchedulerConfig(reason) => {
                write!(f, "invalid scheduler configuration: {reason}")
            }
        }
    }
}

impl std::error::Error for RebalanceError {}

/// Advisory input to the planner. `unavailable_nodes` is a health/gossip
/// view: it steers where new replicas go, and it never changes what is
/// currently authoritative (REQ-M4-BAL-004).
pub struct RebalanceInput<'a> {
    pub catalog: &'a CatalogState,
    pub unavailable_nodes: &'a BTreeSet<RaftNodeId>,
}

/// Deterministically derive a movement operation identity from the plan
/// coordinates. The same cluster/epoch/shard always yields the same id, so a
/// re-planned or failed-over controller re-issues the *same* intent and the
/// catalog's idempotence collapses duplicates (REQ-M4-MOVE-004).
pub fn movement_operation_id(
    cluster_id: &ClusterId,
    epoch: PlacementEpoch,
    shard_id: u16,
) -> [u8; 16] {
    let mut domain = [0u8; 26];
    domain[..16].copy_from_slice(cluster_id);
    domain[16..24].copy_from_slice(&epoch.to_le_bytes());
    domain[24..26].copy_from_slice(&shard_id.to_le_bytes());
    let lo = xxh3_64(&domain);
    domain[0] ^= 0xA5;
    let hi = xxh3_64(&domain);
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&lo.to_le_bytes());
    id[8..].copy_from_slice(&hi.to_le_bytes());
    if id == [0u8; 16] {
        id[0] = 1;
    }
    id
}

/// Compute a deterministic, minimal rebalance plan (REQ-M4-BAL-001/002).
///
/// The planner reads only committed stable placements. Shards with an active
/// movement are left alone. At most one voter changes per shard, and a change
/// is proposed only when it evacuates an unavailable voter, repairs domain
/// spread, or strictly reduces voter-count skew.
pub fn plan_rebalance(input: RebalanceInput<'_>) -> Result<RebalancePlan, RebalanceError> {
    let catalog = input.catalog;
    if catalog.placements.is_empty() {
        return Err(RebalanceError::NoPlacements);
    }
    let eligible: BTreeSet<RaftNodeId> = catalog.eligible_nodes.keys().copied().collect();
    let available: Vec<RaftNodeId> = eligible
        .iter()
        .copied()
        .filter(|node| !input.unavailable_nodes.contains(node))
        .collect();
    if available.len() < 3 {
        return Err(RebalanceError::InsufficientAvailableNodes {
            available: available.len(),
        });
    }
    let distinct_domains = catalog
        .eligible_nodes
        .values()
        .map(|node| node.failure_domain.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    // Mirrors `validate_voters`: the catalog requires three distinct domains
    // exactly when the eligible set has three.
    let require_distinct_domains = distinct_domains >= 3;

    let mut voter_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for node in &eligible {
        voter_counts.insert(*node, 0);
    }
    for placement in catalog.placements.values() {
        for voter in placement.voters {
            *voter_counts.entry(voter).or_insert(0) += 1;
        }
    }

    let total = catalog.placements.len() as u64 * 3;
    let nodes = available.len() as u64;
    let floor = total / nodes;
    let ceil = total.div_ceil(nodes);

    let mut movements = Vec::new();
    // BTreeMap iteration is ascending shard order: deterministic.
    for placement in catalog.placements.values() {
        if placement.pending_movement.is_some() {
            continue;
        }
        if let Some(movement) = plan_shard_movement(
            catalog,
            placement,
            input.unavailable_nodes,
            &eligible,
            &available,
            &mut voter_counts,
            floor,
            ceil,
            require_distinct_domains,
            distinct_domains,
        ) {
            movements.push(movement);
        }
    }

    let leader_reassignments = plan_leader_reassignments(catalog, &available, input);

    Ok(RebalancePlan {
        cluster_id: catalog.cluster_id,
        source_epoch: catalog.placement_epoch,
        movements,
        leader_reassignments,
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_shard_movement(
    catalog: &CatalogState,
    placement: &ShardPlacement,
    unavailable: &BTreeSet<RaftNodeId>,
    eligible: &BTreeSet<RaftNodeId>,
    available: &[RaftNodeId],
    voter_counts: &mut BTreeMap<RaftNodeId, u64>,
    floor: u64,
    ceil: u64,
    require_distinct_domains: bool,
    distinct_domains: usize,
) -> Option<PlannedMovement> {
    let voters = placement.voters;

    // Priority 1: evacuate a voter that is unavailable or no longer eligible.
    if let Some(&out) = voters
        .iter()
        .find(|voter| unavailable.contains(voter) || !eligible.contains(voter))
    {
        let replacement = best_replacement(
            &voters,
            out,
            available,
            voter_counts,
            &catalog.eligible_nodes,
            require_distinct_domains,
        )?;
        return Some(emit_move(
            catalog,
            placement,
            out,
            replacement,
            voter_counts,
            distinct_domains,
            MoveReason::UnavailableNodeEviction,
        ));
    }

    // Priority 2: repair failure-domain spread.
    if require_distinct_domains && !spans_three_domains(&voters, &catalog.eligible_nodes) {
        let out = voters
            .iter()
            .copied()
            .filter(|voter| {
                voters
                    .iter()
                    .filter(|other| {
                        domain_of(other, &catalog.eligible_nodes)
                            == domain_of(voter, &catalog.eligible_nodes)
                    })
                    .count()
                    > 1
            })
            .max_by_key(|voter| (voter_counts.get(voter).copied().unwrap_or(0), *voter))?;
        let present: BTreeSet<&str> = voters
            .iter()
            .filter(|voter| **voter != out)
            .map(|voter| domain_of(voter, &catalog.eligible_nodes))
            .collect();
        let replacement = available
            .iter()
            .copied()
            .filter(|node| !voters.contains(node))
            .filter(|node| !present.contains(domain_of(node, &catalog.eligible_nodes)))
            .min_by_key(|node| (voter_counts.get(node).copied().unwrap_or(0), *node))?;
        return Some(emit_move(
            catalog,
            placement,
            out,
            replacement,
            voter_counts,
            distinct_domains,
            MoveReason::DomainRepair,
        ));
    }

    // Priority 3: reduce voter-count skew. Move one replica from the most
    // loaded available voter to the least loaded available non-voter, only
    // when that strictly narrows the spread.
    let out = voters
        .iter()
        .copied()
        .filter(|voter| available.contains(voter))
        .filter(|voter| voter_counts.get(voter).copied().unwrap_or(0) > ceil)
        .max_by_key(|voter| (voter_counts.get(voter).copied().unwrap_or(0), *voter))?;
    let replacement = best_replacement(
        &voters,
        out,
        available,
        voter_counts,
        &catalog.eligible_nodes,
        require_distinct_domains,
    )
    .filter(|node| voter_counts.get(node).copied().unwrap_or(0) < floor)?;
    Some(emit_move(
        catalog,
        placement,
        out,
        replacement,
        voter_counts,
        distinct_domains,
        MoveReason::SkewReduction,
    ))
}

/// Pick the least-loaded available replacement for `out` that keeps the shard
/// valid: distinct voter, and domain spread preserved when required.
fn best_replacement(
    voters: &[RaftNodeId; 3],
    out: RaftNodeId,
    available: &[RaftNodeId],
    voter_counts: &BTreeMap<RaftNodeId, u64>,
    nodes: &BTreeMap<RaftNodeId, EligibleNode>,
    require_distinct_domains: bool,
) -> Option<RaftNodeId> {
    let present: BTreeSet<&str> = voters
        .iter()
        .filter(|voter| **voter != out)
        .map(|voter| domain_of(voter, nodes))
        .collect();
    available
        .iter()
        .copied()
        .filter(|node| !voters.contains(node))
        .filter(|node| !require_distinct_domains || !present.contains(domain_of(node, nodes)))
        .min_by_key(|node| (voter_counts.get(node).copied().unwrap_or(0), *node))
}

fn emit_move(
    catalog: &CatalogState,
    placement: &ShardPlacement,
    out: RaftNodeId,
    replacement: RaftNodeId,
    voter_counts: &mut BTreeMap<RaftNodeId, u64>,
    distinct_domains: usize,
    reason: MoveReason,
) -> PlannedMovement {
    let mut target: Vec<RaftNodeId> = placement
        .voters
        .iter()
        .copied()
        .filter(|voter| *voter != out)
        .chain(std::iter::once(replacement))
        .collect();
    target.sort_unstable();
    let target_voters: [RaftNodeId; 3] = target.try_into().expect("exactly three target voters");
    // Defensive: the planner must never emit a target the catalog would
    // reject. `begin_movement` re-validates anyway; skipping here keeps a
    // planner bug from scheduling an impossible move.
    debug_assert!(
        validate_voters(
            placement.shard_id,
            &target_voters,
            &catalog.eligible_nodes,
            distinct_domains
        )
        .is_ok(),
        "planner emitted an invalid target for shard {}",
        placement.shard_id
    );
    if let Some(count) = voter_counts.get_mut(&out) {
        *count = count.saturating_sub(1);
    }
    *voter_counts.entry(replacement).or_insert(0) += 1;
    PlannedMovement {
        shard_id: placement.shard_id,
        operation_id: movement_operation_id(
            &catalog.cluster_id,
            catalog.placement_epoch,
            placement.shard_id,
        ),
        source_voters: placement.voters,
        target_voters,
        reason,
    }
}

fn domain_of<'a>(node: &RaftNodeId, nodes: &'a BTreeMap<RaftNodeId, EligibleNode>) -> &'a str {
    nodes
        .get(node)
        .map(|entry| entry.failure_domain.as_str())
        .unwrap_or("")
}

fn spans_three_domains(
    voters: &[RaftNodeId; 3],
    nodes: &BTreeMap<RaftNodeId, EligibleNode>,
) -> bool {
    voters
        .iter()
        .map(|voter| domain_of(voter, nodes))
        .collect::<BTreeSet<_>>()
        .len()
        == 3
}

/// Plan advisory desired-leader reassignments so leader counts differ by at
/// most one across available nodes (REQ-M4-BAL-001). Leaders on unavailable
/// nodes are moved first; the rest is greedy balancing. Shards with an active
/// movement are never touched.
fn plan_leader_reassignments(
    catalog: &CatalogState,
    available: &[RaftNodeId],
    input: RebalanceInput<'_>,
) -> Vec<LeaderReassignment> {
    let mut leader_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for node in available {
        leader_counts.insert(*node, 0);
    }
    for placement in catalog.placements.values() {
        if available.contains(&placement.desired_leader) {
            *leader_counts.entry(placement.desired_leader).or_insert(0) += 1;
        }
    }

    let mut reassignments = Vec::new();
    let mut reassigned_shards = BTreeSet::new();

    // Priority 1: the desired leader is unavailable or ineligible.
    for (&shard_id, placement) in &catalog.placements {
        if placement.pending_movement.is_some() {
            continue;
        }
        let leader = placement.desired_leader;
        if input.unavailable_nodes.contains(&leader) || !available.contains(&leader) {
            let to = placement
                .voters
                .iter()
                .copied()
                .filter(|voter| available.contains(voter))
                .min_by_key(|voter| (leader_counts.get(voter).copied().unwrap_or(0), *voter));
            if let Some(to) = to {
                *leader_counts.entry(to).or_insert(0) += 1;
                reassigned_shards.insert(shard_id);
                reassignments.push(LeaderReassignment {
                    shard_id,
                    from: leader,
                    to,
                });
            }
        }
    }

    // Priority 2: greedy balance until the spread is at most one. Each
    // successful iteration strictly decreases the sum of squared counts, so
    // the loop always terminates; the placement count is a generous bound.
    for _ in 0..catalog.placements.len().max(1) {
        let (&max_node, &max_count) = leader_counts
            .iter()
            .max_by_key(|(node, count)| (**count, **node))
            .expect("available nodes are non-empty");
        let &min_count = leader_counts
            .values()
            .min()
            .expect("available nodes are non-empty");
        if max_count - min_count <= 1 {
            break;
        }
        // Smallest shard led by the overloaded node that has a strictly
        // less-loaded available voter to hand the hint to.
        let candidate = catalog
            .placements
            .iter()
            .filter(|(shard_id, placement)| {
                !reassigned_shards.contains(*shard_id)
                    && placement.pending_movement.is_none()
                    && placement.desired_leader == max_node
            })
            .filter_map(|(shard_id, placement)| {
                placement
                    .voters
                    .iter()
                    .copied()
                    .filter(|voter| available.contains(voter) && *voter != max_node)
                    .min_by_key(|voter| (leader_counts.get(voter).copied().unwrap_or(0), *voter))
                    .map(|to| (*shard_id, to))
            })
            .next();
        match candidate {
            Some((shard_id, to))
                if leader_counts.get(&to).copied().unwrap_or(0) < max_count - 1 =>
            {
                if let Some(count) = leader_counts.get_mut(&max_node) {
                    *count = count.saturating_sub(1);
                }
                *leader_counts.entry(to).or_insert(0) += 1;
                reassigned_shards.insert(shard_id);
                reassignments.push(LeaderReassignment {
                    shard_id,
                    from: max_node,
                    to,
                });
            }
            _ => break,
        }
    }

    reassignments
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

/// Outcome of committing one planned movement intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeginOutcome {
    Accepted,
    AlreadyActive,
    /// The catalog epoch moved under the scheduler; re-read and retry.
    StaleEpoch,
    /// A conflicting foreign intent is active; the scheduler fails the shard
    /// closed rather than clobbering it.
    Rejected(String),
}

/// Outcome of applying one desired-leader reassignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReassignOutcome {
    Updated,
    AlreadySet,
    StaleEpoch,
    Rejected(String),
}

/// Effect side of the scheduler, abstracted for testing.
///
/// The blanket implementation below wires this to the M4-T4
/// `MovementDriver`, so intent submission (`begin_movement`) and
/// reconciliation (`drive_movement`) stay behind one controller.
#[async_trait]
pub trait RebalanceDrive: Send + Sync {
    /// Commit the planned movement intent (idempotent by operation id).
    async fn begin_movement(
        &self,
        movement: &PlannedMovement,
        expected_epoch: PlacementEpoch,
    ) -> Result<BeginOutcome, String>;
    /// Drive one shard's pending movement forward with driver semantics.
    async fn drive_movement(&self, shard_id: u16) -> DriverOutcome;
    /// Apply one advisory desired-leader reassignment (idempotent).
    async fn reassign_leader(
        &self,
        shard_id: u16,
        new_leader: RaftNodeId,
        expected_epoch: PlacementEpoch,
    ) -> Result<ReassignOutcome, String>;
    /// Request cancellation of a shard's movement (pre-membership only).
    async fn cancel_movement(&self, shard_id: u16, reason: String) -> CancelOutcome;
    /// Read the last committed catalog state.
    async fn read_catalog(&self) -> Result<CatalogState, String>;
}

#[async_trait]
impl<D> RebalanceDrive for Arc<D>
where
    D: RebalanceDrive,
{
    async fn begin_movement(
        &self,
        movement: &PlannedMovement,
        expected_epoch: PlacementEpoch,
    ) -> Result<BeginOutcome, String> {
        (**self).begin_movement(movement, expected_epoch).await
    }

    async fn drive_movement(&self, shard_id: u16) -> DriverOutcome {
        (**self).drive_movement(shard_id).await
    }

    async fn reassign_leader(
        &self,
        shard_id: u16,
        new_leader: RaftNodeId,
        expected_epoch: PlacementEpoch,
    ) -> Result<ReassignOutcome, String> {
        (**self)
            .reassign_leader(shard_id, new_leader, expected_epoch)
            .await
    }

    async fn cancel_movement(&self, shard_id: u16, reason: String) -> CancelOutcome {
        (**self).cancel_movement(shard_id, reason).await
    }

    async fn read_catalog(&self) -> Result<CatalogState, String> {
        (**self).read_catalog().await
    }
}

fn map_begin(result: Result<CatalogResponse, CatalogGroupError>) -> Result<BeginOutcome, String> {
    match result {
        Ok(CatalogResponse::MovementAccepted { .. }) => Ok(BeginOutcome::Accepted),
        Ok(CatalogResponse::MovementAlreadyActive { .. }) => Ok(BeginOutcome::AlreadyActive),
        Ok(other) => Err(format!("unexpected catalog response: {other:?}")),
        Err(CatalogGroupError::InvalidPlacement(PlacementError::StaleCatalogView { .. })) => {
            Ok(BeginOutcome::StaleEpoch)
        }
        Err(CatalogGroupError::InvalidPlacement(PlacementError::ConflictingMovement {
            shard_id,
        })) => Ok(BeginOutcome::Rejected(format!(
            "conflicting movement on shard {shard_id}"
        ))),
        Err(error) => Err(error.to_string()),
    }
}

fn map_reassign(
    result: Result<CatalogResponse, CatalogGroupError>,
) -> Result<ReassignOutcome, String> {
    match result {
        Ok(CatalogResponse::DesiredLeaderUpdated { .. }) => Ok(ReassignOutcome::Updated),
        Ok(CatalogResponse::DesiredLeaderAlreadySet) => Ok(ReassignOutcome::AlreadySet),
        Ok(other) => Err(format!("unexpected catalog response: {other:?}")),
        Err(CatalogGroupError::InvalidPlacement(PlacementError::StaleCatalogView { .. })) => {
            Ok(ReassignOutcome::StaleEpoch)
        }
        Err(CatalogGroupError::InvalidPlacement(PlacementError::ConflictingMovement {
            shard_id,
        })) => Ok(ReassignOutcome::Rejected(format!(
            "conflicting movement on shard {shard_id}"
        ))),
        Err(error) => Err(error.to_string()),
    }
}

#[async_trait]
impl<O, C> RebalanceDrive for MovementDriver<O, C>
where
    O: MembershipOperator,
    C: CatalogPort,
{
    async fn begin_movement(
        &self,
        movement: &PlannedMovement,
        expected_epoch: PlacementEpoch,
    ) -> Result<BeginOutcome, String> {
        map_begin(
            self.catalog()
                .submit(CatalogCommand::BeginMovement {
                    expected_epoch,
                    operation_id: movement.operation_id,
                    shard_id: movement.shard_id,
                    target_voters: movement.target_voters,
                })
                .await,
        )
    }

    async fn drive_movement(&self, shard_id: u16) -> DriverOutcome {
        self.drive_shard(shard_id).await
    }

    async fn reassign_leader(
        &self,
        shard_id: u16,
        new_leader: RaftNodeId,
        expected_epoch: PlacementEpoch,
    ) -> Result<ReassignOutcome, String> {
        map_reassign(
            self.catalog()
                .submit(CatalogCommand::SetDesiredLeader {
                    expected_epoch,
                    shard_id,
                    new_leader,
                })
                .await,
        )
    }

    async fn cancel_movement(&self, shard_id: u16, reason: String) -> CancelOutcome {
        self.request_cancel(shard_id, reason).await
    }

    async fn read_catalog(&self) -> Result<CatalogState, String> {
        self.catalog()
            .read_committed()
            .await
            .map_err(|error| error.to_string())
    }
}

/// Per-shard lifecycle inside one plan (REQ-M4-BAL-003).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PlanShardState {
    Queued,
    Running,
    Blocked(BlockReason),
    Failed(String),
    Completed,
}

/// Why a shard is not currently being driven. Blocked is never terminal:
/// the scheduler retries blocked shards on later polls once the condition
/// clears.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BlockReason {
    /// A planned target voter is currently unavailable; driving the movement
    /// would strand the new replica.
    TargetUnavailable,
    /// The catalog would not accept the intent right now.
    CatalogWriteBlocked(String),
}

#[derive(Clone, Debug)]
pub struct RebalanceSchedulerConfig {
    /// Cluster-wide and per-node concurrent movement bounds (REQ-M4-BAL-003).
    pub work: MovementWorkConfig,
    /// Transient failures tolerated per shard before it is marked failed.
    pub max_shard_failures: u32,
}

impl Default for RebalanceSchedulerConfig {
    fn default() -> Self {
        Self {
            work: MovementWorkConfig {
                cluster_max_concurrent: 4,
                per_node_max_concurrent: 2,
            },
            max_shard_failures: 5,
        }
    }
}

/// Observable scheduler status (REQ-M4-BAL-003, REQ-M4-OPS-002).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SchedulerStatus {
    pub cluster_id: ClusterId,
    pub source_epoch: PlacementEpoch,
    pub queued: usize,
    pub running: usize,
    pub blocked: usize,
    pub failed: usize,
    pub completed: usize,
    pub leaders_pending: usize,
    pub leaders_done: usize,
    pub leaders_failed: usize,
    pub shards: BTreeMap<u16, PlanShardState>,
    pub admission: MovementWorkMetrics,
}

/// Bounded, restart-recoverable executor for one [`RebalancePlan`].
///
/// The scheduler is level-triggered: `poll` starts every queued shard whose
/// target is available inside the configured concurrency bounds, drives each
/// through the movement driver, and records the terminal outcome. `recover`
/// reconciles in-memory state with the committed catalog after a restart, so
/// a partial plan resumes instead of restarting or duplicating work.
pub struct RebalanceScheduler<D> {
    plan: RebalancePlan,
    config: RebalanceSchedulerConfig,
    drive: Arc<D>,
    admission: MovementWorkAdmission,
    unavailable: BTreeSet<RaftNodeId>,
    states: BTreeMap<u16, PlanShardState>,
    failures: BTreeMap<u16, u32>,
    begun: BTreeSet<u16>,
    leaders_done: BTreeSet<u16>,
    leaders_failed: BTreeMap<u16, String>,
}

impl<D> RebalanceScheduler<D>
where
    D: RebalanceDrive + 'static,
{
    pub fn new(
        plan: RebalancePlan,
        config: RebalanceSchedulerConfig,
        drive: D,
        unavailable: BTreeSet<RaftNodeId>,
    ) -> Result<Self, RebalanceError> {
        let admission = MovementWorkAdmission::new(config.work)
            .map_err(|error| RebalanceError::InvalidSchedulerConfig(error.to_string()))?;
        let mut states = BTreeMap::new();
        for movement in &plan.movements {
            states.insert(movement.shard_id, PlanShardState::Queued);
        }
        Ok(Self {
            plan,
            config,
            drive: Arc::new(drive),
            admission,
            unavailable,
            states,
            failures: BTreeMap::new(),
            begun: BTreeSet::new(),
            leaders_done: BTreeSet::new(),
            leaders_failed: BTreeMap::new(),
        })
    }

    /// Refresh the advisory unavailable set (health/gossip view).
    pub fn set_unavailable(&mut self, unavailable: BTreeSet<RaftNodeId>) {
        self.unavailable = unavailable;
        // A node that returned to service unblocks its shards.
        for movement in &self.plan.movements {
            let shard_id = movement.shard_id;
            if self.states.get(&shard_id)
                == Some(&PlanShardState::Blocked(BlockReason::TargetUnavailable))
                && !movement
                    .target_voters
                    .iter()
                    .any(|voter| self.unavailable.contains(voter))
            {
                self.states.insert(shard_id, PlanShardState::Queued);
            }
        }
    }

    pub fn status(&self) -> SchedulerStatus {
        let mut status = SchedulerStatus {
            cluster_id: self.plan.cluster_id,
            source_epoch: self.plan.source_epoch,
            queued: 0,
            running: 0,
            blocked: 0,
            failed: 0,
            completed: 0,
            leaders_pending: 0,
            leaders_done: self.leaders_done.len(),
            leaders_failed: self.leaders_failed.len(),
            shards: self.states.clone(),
            admission: self.admission.metrics(),
        };
        for state in self.states.values() {
            match state {
                PlanShardState::Queued => status.queued += 1,
                PlanShardState::Running => status.running += 1,
                PlanShardState::Blocked(_) => status.blocked += 1,
                PlanShardState::Failed(_) => status.failed += 1,
                PlanShardState::Completed => status.completed += 1,
            }
        }
        status.leaders_pending = self
            .plan
            .leader_reassignments
            .iter()
            .filter(|reassignment| {
                !self.leaders_done.contains(&reassignment.shard_id)
                    && !self.leaders_failed.contains_key(&reassignment.shard_id)
            })
            .count();
        status
    }

    /// Reconcile scheduler state with the committed catalog after a restart
    /// (REQ-M4-FAIL-003/004). Completed work stays completed; in-flight work
    /// with a matching operation id resumes; foreign intents fail closed.
    pub fn recover(&mut self, catalog: &CatalogState) {
        if catalog.cluster_id != self.plan.cluster_id {
            for movement in &self.plan.movements {
                let shard_id = movement.shard_id;
                if !is_terminal(self.states.get(&shard_id)) {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed("cluster identity changed".to_string()),
                    );
                }
            }
            return;
        }
        for movement in &self.plan.movements {
            let shard_id = movement.shard_id;
            if is_terminal(self.states.get(&shard_id)) {
                continue;
            }
            let placement = match catalog.placements.get(&shard_id) {
                Some(placement) => placement,
                None => {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed("placement vanished from catalog".to_string()),
                    );
                    continue;
                }
            };
            match &placement.pending_movement {
                None if placement.voters == movement.target_voters => {
                    self.states.insert(shard_id, PlanShardState::Completed);
                }
                None => {
                    // No intent committed (or it was cancelled pre-membership):
                    // the shard is safe to re-drive from the plan.
                    self.begun.remove(&shard_id);
                    self.states.insert(shard_id, PlanShardState::Queued);
                }
                Some(pending) if pending.operation_id == movement.operation_id => {
                    self.begun.insert(shard_id);
                    self.states.insert(shard_id, PlanShardState::Queued);
                }
                Some(pending) => {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed(format!(
                            "superseded by foreign operation {:02x?}",
                            pending.operation_id
                        )),
                    );
                }
            }
        }
        for reassignment in &self.plan.leader_reassignments {
            if catalog
                .placements
                .get(&reassignment.shard_id)
                .map(|placement| placement.desired_leader)
                == Some(reassignment.to)
            {
                self.leaders_done.insert(reassignment.shard_id);
            }
        }
    }

    /// Cancel every non-terminal shard of the plan. Intents that were never
    /// committed are dropped; committed pre-membership intents go through the
    /// driver's safe cancel; anything past membership commit must finish
    /// forward (REQ-M4-MOVE-005).
    pub async fn cancel_plan(&mut self, reason: String) {
        let shards: Vec<u16> = self
            .plan
            .movements
            .iter()
            .map(|movement| movement.shard_id)
            .filter(|shard_id| {
                matches!(
                    self.states.get(shard_id),
                    Some(PlanShardState::Queued)
                        | Some(PlanShardState::Running)
                        | Some(PlanShardState::Blocked(_))
                )
            })
            .collect();
        for shard_id in shards {
            if !self.begun.contains(&shard_id) {
                self.states.insert(
                    shard_id,
                    PlanShardState::Failed(format!("cancelled before intent: {reason}")),
                );
                continue;
            }
            match self.drive.cancel_movement(shard_id, reason.clone()).await {
                CancelOutcome::Cancelled | CancelOutcome::NothingToCancel => {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed(format!("cancelled: {reason}")),
                    );
                    self.begun.remove(&shard_id);
                }
                CancelOutcome::MustFinishForward { .. } => {
                    // Membership already changed: the movement must finish
                    // forward; leave it queued for the next poll.
                    self.states.insert(shard_id, PlanShardState::Queued);
                }
                CancelOutcome::AmbiguousMembership { .. } => {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed(
                            "cancel refused: ambiguous membership, must finish forward".to_string(),
                        ),
                    );
                }
                CancelOutcome::CatalogUnavailable(_) => {
                    // Leave queued; a later poll retries the cancel.
                }
            }
        }
    }

    /// One scheduler pass: apply pending leader reassignments, then start
    /// every queued shard whose target is available inside the concurrency
    /// bounds and drive it through the movement driver.
    pub async fn poll(&mut self) {
        self.apply_leader_reassignments().await;
        let catalog = match self.drive.read_catalog().await {
            Ok(state) => state,
            Err(_) => return,
        };
        // Fast path: another controller (or a previous scheduler instance)
        // already completed the movement; adopt the committed result instead
        // of re-driving (REQ-M4-MOVE-004).
        for movement in &self.plan.movements {
            let shard_id = movement.shard_id;
            if self.states.get(&shard_id) != Some(&PlanShardState::Queued) {
                continue;
            }
            if let Some(placement) = catalog.placements.get(&shard_id) {
                if placement.pending_movement.is_none()
                    && placement.voters == movement.target_voters
                {
                    self.states.insert(shard_id, PlanShardState::Completed);
                }
            }
        }
        let epoch = catalog.placement_epoch;

        let mut join_set = JoinSet::new();
        let mut in_flight = 0usize;
        let movements = self.plan.movements.clone();
        for movement in &movements {
            let shard_id = movement.shard_id;
            if self.states.get(&shard_id) != Some(&PlanShardState::Queued) {
                continue;
            }
            if *self.failures.get(&shard_id).unwrap_or(&0) >= self.config.max_shard_failures {
                self.states.insert(
                    shard_id,
                    PlanShardState::Failed("transient failure budget exhausted".to_string()),
                );
                continue;
            }
            if in_flight >= self.config.work.cluster_max_concurrent {
                break;
            }
            if movement
                .target_voters
                .iter()
                .any(|voter| self.unavailable.contains(voter))
            {
                self.states.insert(
                    shard_id,
                    PlanShardState::Blocked(BlockReason::TargetUnavailable),
                );
                continue;
            }
            let added: Vec<RaftNodeId> = movement
                .target_voters
                .iter()
                .copied()
                .filter(|voter| !movement.source_voters.contains(voter))
                .collect();
            let mut permits = Vec::with_capacity(added.len());
            let mut admitted = true;
            for node in &added {
                match self.admission.try_admit(*node) {
                    Ok(permit) => permits.push(permit),
                    Err(_) => {
                        admitted = false;
                        break;
                    }
                }
            }
            if !admitted {
                // Admission is transient: stay queued, retry next poll.
                drop(permits);
                continue;
            }
            self.states.insert(shard_id, PlanShardState::Running);
            in_flight += 1;
            let drive = Arc::clone(&self.drive);
            let movement = movement.clone();
            join_set.spawn(async move {
                let (begun, outcome) = match drive.begin_movement(&movement, epoch).await {
                    Ok(BeginOutcome::Accepted) | Ok(BeginOutcome::AlreadyActive) => (
                        true,
                        TaskOutcome::Driven(drive.drive_movement(movement.shard_id).await),
                    ),
                    Ok(BeginOutcome::StaleEpoch) => (false, TaskOutcome::StaleEpoch),
                    Ok(BeginOutcome::Rejected(reason)) => {
                        (false, TaskOutcome::BeginRejected(reason))
                    }
                    Err(reason) => (false, TaskOutcome::TransportFailed(reason)),
                };
                // Permits are held for the whole drive and released here.
                drop(permits);
                (movement.shard_id, begun, outcome)
            });
        }

        while let Some(joined) = join_set.join_next().await {
            let (shard_id, begun, outcome) = joined.expect("drive task must not panic");
            if begun {
                self.begun.insert(shard_id);
            }
            self.resolve_task_outcome(shard_id, outcome);
        }
    }

    /// Apply planned desired-leader reassignments with a fresh epoch read per
    /// command (each committed reassignment bumps the global epoch).
    async fn apply_leader_reassignments(&mut self) {
        let pending: Vec<LeaderReassignment> = self
            .plan
            .leader_reassignments
            .iter()
            .filter(|reassignment| {
                !self.leaders_done.contains(&reassignment.shard_id)
                    && !self.leaders_failed.contains_key(&reassignment.shard_id)
            })
            .cloned()
            .collect();
        for reassignment in pending {
            let epoch = match self.drive.read_catalog().await {
                Ok(state) => state.placement_epoch,
                Err(_) => return,
            };
            match self
                .drive
                .reassign_leader(reassignment.shard_id, reassignment.to, epoch)
                .await
            {
                Ok(ReassignOutcome::Updated) | Ok(ReassignOutcome::AlreadySet) => {
                    self.leaders_done.insert(reassignment.shard_id);
                }
                Ok(ReassignOutcome::StaleEpoch) => {
                    // Retry on the next poll with a fresh epoch.
                }
                Ok(ReassignOutcome::Rejected(reason)) | Err(reason) => {
                    self.leaders_failed.insert(reassignment.shard_id, reason);
                }
            }
        }
    }

    fn resolve_task_outcome(&mut self, shard_id: u16, outcome: TaskOutcome) {
        match outcome {
            TaskOutcome::Driven(DriverOutcome::Completed { .. }) => {
                self.states.insert(shard_id, PlanShardState::Completed);
                self.begun.remove(&shard_id);
            }
            TaskOutcome::Driven(DriverOutcome::Cancelled { .. }) => {
                self.states.insert(
                    shard_id,
                    PlanShardState::Failed("movement cancelled during drive".to_string()),
                );
                self.begun.remove(&shard_id);
            }
            TaskOutcome::Driven(DriverOutcome::FailClosed { reason, .. }) => {
                self.states.insert(shard_id, PlanShardState::Failed(reason));
                self.begun.remove(&shard_id);
            }
            TaskOutcome::BeginRejected(reason) => {
                // A conflicting foreign intent or an invalid target: fail
                // closed rather than retrying forever.
                self.states.insert(shard_id, PlanShardState::Failed(reason));
                self.begun.remove(&shard_id);
            }
            TaskOutcome::StaleEpoch
            | TaskOutcome::Driven(DriverOutcome::NoPendingMovement)
            | TaskOutcome::Driven(DriverOutcome::DeferredAdmission) => {
                // Transient: re-read and retry on the next poll.
                self.states.insert(shard_id, PlanShardState::Queued);
            }
            TaskOutcome::Driven(DriverOutcome::TransientFailure { reason })
            | TaskOutcome::TransportFailed(reason) => {
                let failures = self.failures.entry(shard_id).or_insert(0);
                *failures += 1;
                if *failures >= self.config.max_shard_failures {
                    self.states.insert(
                        shard_id,
                        PlanShardState::Failed(format!(
                            "transient failure budget exhausted: {reason}"
                        )),
                    );
                    self.begun.remove(&shard_id);
                } else {
                    self.states.insert(shard_id, PlanShardState::Queued);
                }
            }
        }
    }
}

fn is_terminal(state: Option<&PlanShardState>) -> bool {
    matches!(
        state,
        Some(PlanShardState::Completed) | Some(PlanShardState::Failed(_))
    )
}

#[derive(Debug)]
enum TaskOutcome {
    Driven(DriverOutcome),
    StaleEpoch,
    BeginRejected(String),
    TransportFailed(String),
}

// ---------------------------------------------------------------------------
// Topology management surface
// ---------------------------------------------------------------------------

/// Observed catalog health for the management surface and the health gate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CatalogHealth {
    Healthy,
    Degraded { unavailable_nodes: Vec<RaftNodeId> },
    QuorumLost,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeTopology {
    pub node_id: RaftNodeId,
    pub raft_endpoint: String,
    pub failure_domain: String,
    pub voter_shards: u64,
    pub leader_shards: u64,
    pub available: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementSummary {
    pub shard_id: u16,
    pub operation_id: [u8; 16],
    pub phase: MovementPhase,
    pub retries: u32,
    pub target_voters: [RaftNodeId; 3],
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlanSummary {
    pub source_epoch: PlacementEpoch,
    pub queued: usize,
    pub running: usize,
    pub blocked: usize,
    pub failed: usize,
    pub completed: usize,
    pub leaders_pending: usize,
    pub leaders_done: usize,
    pub leaders_failed: usize,
}

impl From<&SchedulerStatus> for PlanSummary {
    fn from(status: &SchedulerStatus) -> Self {
        Self {
            source_epoch: status.source_epoch,
            queued: status.queued,
            running: status.running,
            blocked: status.blocked,
            failed: status.failed,
            completed: status.completed,
            leaders_pending: status.leaders_pending,
            leaders_done: status.leaders_done,
            leaders_failed: status.leaders_failed,
        }
    }
}

/// Per-group Raft state in the stable topology view (REQ-M4-OPS-001).
///
/// Every field is plain data: role, term, leader, committed membership
/// config, commit/apply/log progress, apply lag and snapshot progress.
/// No OpenRaft types cross this boundary; [`GroupRaftView::from_status`]
/// is the single conversion point from [`ReplicaStatus`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupRaftView {
    pub shard_id: u16,
    pub role: ReplicaRole,
    pub health: ReplicaHealth,
    pub term: u64,
    pub leader_id: Option<RaftNodeId>,
    pub voters: Vec<RaftNodeId>,
    pub learners: Vec<RaftNodeId>,
    /// Last log index appended on this replica, if any.
    pub log_index: Option<u64>,
    /// Last committed log index known to this replica, if any.
    pub commit_index: Option<u64>,
    /// Last log index applied to this replica's state machine, if any.
    pub apply_index: Option<u64>,
    /// Committed-but-not-yet-applied entries (saturating subtraction).
    pub apply_lag: u64,
    /// Last log index included in a snapshot, if any.
    pub snapshot_index: Option<u64>,
}

impl GroupRaftView {
    /// Build the contract view from one replica's observed [`ReplicaStatus`].
    ///
    /// `shard_id` is the data-group shard this replica belongs to; the
    /// status itself carries the Raft node identity. Learners are the
    /// committed members that are not voters.
    pub fn from_status(shard_id: u16, status: &ReplicaStatus) -> Self {
        let mut voters = status.membership.voters.clone();
        voters.sort_unstable();
        let mut learners: Vec<RaftNodeId> = status
            .membership
            .members
            .iter()
            .filter(|member| !member.voter)
            .map(|member| member.node_id)
            .collect();
        learners.sort_unstable();
        let commit_index = status.committed.map(|position| position.index);
        let apply_index = status.applied.map(|position| position.index);
        let apply_lag = commit_index
            .unwrap_or(0)
            .saturating_sub(apply_index.unwrap_or(0));
        Self {
            shard_id,
            role: status.role,
            health: status.health,
            term: status.current_term,
            leader_id: status.leader_id,
            voters,
            learners,
            log_index: status.last_log_index,
            commit_index,
            apply_index,
            apply_lag,
            snapshot_index: status.snapshot.map(|position| position.index),
        }
    }
}

/// Stable serializable topology view (REQ-M4-OPS-001).
///
/// Cardinality is bounded: one entry per eligible node, one per *active*
/// movement, and one per data group with observed Raft state. No per-shard
/// metric labels are emitted; the per-group entries are the paginated
/// per-shard inspection payload (REQ-M4-OPS-003).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopologyView {
    pub cluster_id: ClusterId,
    pub placement_epoch: PlacementEpoch,
    pub catalog_health: CatalogHealth,
    pub nodes: Vec<NodeTopology>,
    pub groups: Vec<GroupRaftView>,
    pub voter_skew: u64,
    pub leader_skew: u64,
    pub active_movements: Vec<MovementSummary>,
    pub plan: Option<PlanSummary>,
}

pub fn build_topology_view(
    catalog: &CatalogState,
    unavailable: &BTreeSet<RaftNodeId>,
    catalog_health: CatalogHealth,
    groups: Vec<GroupRaftView>,
    plan: Option<PlanSummary>,
) -> TopologyView {
    let mut voter_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    let mut leader_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for node in catalog.eligible_nodes.keys() {
        voter_counts.insert(*node, 0);
        leader_counts.insert(*node, 0);
    }
    let mut active_movements = Vec::new();
    for placement in catalog.placements.values() {
        for voter in placement.voters {
            *voter_counts.entry(voter).or_insert(0) += 1;
        }
        *leader_counts.entry(placement.desired_leader).or_insert(0) += 1;
        if let Some(pending) = &placement.pending_movement {
            active_movements.push(MovementSummary {
                shard_id: placement.shard_id,
                operation_id: pending.operation_id,
                phase: pending.phase,
                retries: pending.retries,
                target_voters: pending.target_voters,
                last_error: pending.last_error.clone(),
            });
        }
    }
    active_movements.sort_by_key(|movement| movement.shard_id);

    let mut groups = groups;
    groups.sort_by_key(|group| group.shard_id);

    let nodes = catalog
        .eligible_nodes
        .values()
        .map(|node| NodeTopology {
            node_id: node.node_id,
            raft_endpoint: node.raft_endpoint.clone(),
            failure_domain: node.failure_domain.clone(),
            voter_shards: voter_counts.get(&node.node_id).copied().unwrap_or(0),
            leader_shards: leader_counts.get(&node.node_id).copied().unwrap_or(0),
            available: !unavailable.contains(&node.node_id),
        })
        .collect();

    TopologyView {
        cluster_id: catalog.cluster_id,
        placement_epoch: catalog.placement_epoch,
        catalog_health,
        nodes,
        groups,
        voter_skew: skew(voter_counts.values().copied()),
        leader_skew: skew(leader_counts.values().copied()),
        active_movements,
        plan,
    }
}

fn skew(counts: impl Iterator<Item = u64>) -> u64 {
    let mut min = u64::MAX;
    let mut max = 0u64;
    let mut any = false;
    for count in counts {
        any = true;
        min = min.min(count);
        max = max.max(count);
    }
    if any {
        max - min
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Catalog health gate
// ---------------------------------------------------------------------------

/// Observes catalog consensus health for the mutation gate.
pub trait CatalogHealthProbe: Send + Sync {
    fn health(&self) -> CatalogHealth;
}

/// Build a probe from a closure (tests and simple controller wiring).
pub struct FnProbe<F>(pub F);

impl<F> CatalogHealthProbe for FnProbe<F>
where
    F: Fn() -> CatalogHealth + Send + Sync,
{
    fn health(&self) -> CatalogHealth {
        (self.0)()
    }
}

/// Guards the catalog write path: new placement mutations are refused while
/// catalog quorum is lost or health is unknown, while reads of the last
/// committed catalog view keep working so data groups continue under their
/// committed memberships (REQ-M4-BAL-005).
///
/// Bootstrap is always allowed through: a fresh cluster has no quorum to
/// lose, and blocking it would make initialization impossible.
#[derive(Clone)]
pub struct CatalogHealthGate<C, P> {
    inner: C,
    probe: P,
}

impl<C, P> CatalogHealthGate<C, P> {
    pub fn new(inner: C, probe: P) -> Self {
        Self { inner, probe }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }
}

#[async_trait]
impl<C, P> CatalogPort for CatalogHealthGate<C, P>
where
    C: CatalogPort,
    P: CatalogHealthProbe,
{
    async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
        self.inner.read_committed().await
    }

    async fn submit(&self, command: CatalogCommand) -> Result<CatalogResponse, CatalogGroupError> {
        let writable = match self.probe.health() {
            CatalogHealth::Healthy | CatalogHealth::Degraded { .. } => true,
            CatalogHealth::QuorumLost | CatalogHealth::Unknown => {
                matches!(&command, CatalogCommand::Bootstrap { .. })
            }
        };
        if !writable {
            return Err(CatalogGroupError::ConsensusUnavailable);
        }
        self.inner.submit(command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::time::sleep;

    fn cluster_id() -> ClusterId {
        *b"homekv-m5-test01"
    }

    fn node(node_id: u64, domain: &str) -> EligibleNode {
        EligibleNode {
            node_id,
            raft_endpoint: format!("127.0.0.1:{}", 19_000 + node_id),
            failure_domain: domain.to_string(),
        }
    }

    fn bootstrap_catalog(nodes: Vec<EligibleNode>) -> PlacementCatalog {
        let mut catalog = PlacementCatalog::default();
        catalog
            .apply(CatalogCommand::Bootstrap {
                cluster_id: cluster_id(),
                eligible_nodes: nodes,
            })
            .unwrap();
        catalog
    }

    fn empty_unavailable() -> BTreeSet<RaftNodeId> {
        BTreeSet::new()
    }

    fn placement(
        shard_id: u16,
        voters: [RaftNodeId; 3],
        desired_leader: RaftNodeId,
    ) -> ShardPlacement {
        ShardPlacement {
            shard_id,
            group_id: crate::placement::data_group_id(shard_id).unwrap(),
            voters,
            desired_leader,
            epoch: 1,
            pending_movement: None,
        }
    }

    fn small_catalog(nodes: Vec<EligibleNode>, placements: Vec<ShardPlacement>) -> CatalogState {
        CatalogState {
            format_version: 1,
            cluster_id: cluster_id(),
            placement_epoch: 1,
            eligible_nodes: nodes.into_iter().map(|n| (n.node_id, n)).collect(),
            placements: placements.into_iter().map(|p| (p.shard_id, p)).collect(),
        }
    }

    fn voter_counts(catalog: &CatalogState) -> BTreeMap<RaftNodeId, u64> {
        let mut counts = BTreeMap::new();
        for placement in catalog.placements.values() {
            for voter in placement.voters {
                *counts.entry(voter).or_insert(0) += 1;
            }
        }
        counts
    }

    fn apply_plan_targets(
        catalog: &CatalogState,
        plan: &RebalancePlan,
    ) -> BTreeMap<RaftNodeId, u64> {
        let mut counts = voter_counts(catalog);
        for movement in &plan.movements {
            for voter in movement.source_voters {
                *counts.get_mut(&voter).unwrap() -= 1;
            }
            for voter in movement.target_voters {
                *counts.entry(voter).or_insert(0) += 1;
            }
        }
        counts
    }

    // -- planner -----------------------------------------------------------

    #[test]
    fn balanced_catalog_produces_empty_plan() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &empty_unavailable(),
        })
        .unwrap();
        assert!(plan.movements.is_empty());
        assert!(plan.leader_reassignments.is_empty());
        assert_eq!(plan.source_epoch, 1);
        assert_eq!(plan.cluster_id, cluster_id());
        // The planner is pure: the input catalog is untouched.
        assert_eq!(catalog.state().unwrap(), &state);
    }

    #[test]
    fn plan_is_deterministic_across_calls() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [4].into_iter().collect();
        let plan_a = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        let plan_b = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        assert_eq!(plan_a, plan_b);
        assert!(!plan_a.movements.is_empty());
    }

    #[test]
    fn unavailable_node_is_evicted_and_nothing_else_moves() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [4].into_iter().collect();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &unavailable,
        })
        .unwrap();

        // Every movement evacuates node 4 and nothing else moves.
        for movement in &plan.movements {
            assert!(
                movement.source_voters.contains(&4),
                "shard {} moves without an unavailable voter",
                movement.shard_id
            );
            assert!(!movement.target_voters.contains(&4));
            assert_eq!(movement.reason, MoveReason::UnavailableNodeEviction);
            // Exactly one voter changes.
            let changed = movement
                .source_voters
                .iter()
                .filter(|voter| !movement.target_voters.contains(voter))
                .count();
            assert_eq!(changed, 1);
            // Canonical ordering and no co-location.
            assert!(movement.target_voters.windows(2).all(|w| w[0] < w[1]));
            // The target passes the same validation the catalog enforces.
            validate_voters(
                movement.shard_id,
                &movement.target_voters,
                &state.eligible_nodes,
                4,
            )
            .unwrap();
            // The committed stable placement is the movement source: the
            // planner never invents membership (REQ-M4-BAL-004).
            assert_eq!(
                movement.source_voters,
                state.placements[&movement.shard_id].voters
            );
        }
        // Shards without node 4 are untouched.
        let moved: BTreeSet<u16> = plan.movements.iter().map(|m| m.shard_id).collect();
        for (shard_id, placement) in &state.placements {
            if !placement.voters.contains(&4) {
                assert!(!moved.contains(shard_id));
            }
        }
        // Operation ids are deterministic and non-empty.
        for movement in &plan.movements {
            assert_ne!(movement.operation_id, [0u8; 16]);
            assert_eq!(
                movement.operation_id,
                movement_operation_id(&cluster_id(), 1, movement.shard_id)
            );
        }
    }

    #[test]
    fn skew_is_reduced_with_minimal_moves() {
        // 4 nodes, 12 shards, hand-built 12/12/6/6 skew.
        let nodes = vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")];
        let mut placements = Vec::new();
        for shard_id in 0..6u16 {
            placements.push(placement(shard_id, [1, 2, 3], 1));
        }
        for shard_id in 6..12u16 {
            placements.push(placement(shard_id, [1, 2, 4], 2));
        }
        let catalog = small_catalog(nodes, placements);
        let plan = plan_rebalance(RebalanceInput {
            catalog: &catalog,
            unavailable_nodes: &empty_unavailable(),
        })
        .unwrap();

        assert_eq!(plan.movements.len(), 6);
        for movement in &plan.movements {
            assert_eq!(movement.reason, MoveReason::SkewReduction);
        }
        let counts = apply_plan_targets(&catalog, &plan);
        assert_eq!(
            counts,
            [(1, 9), (2, 9), (3, 9), (4, 9)].into_iter().collect()
        );
    }

    #[test]
    fn domain_spread_is_preserved_when_evacuating() {
        let catalog = bootstrap_catalog(vec![
            node(1, "a"),
            node(2, "b"),
            node(3, "c"),
            node(4, "a"),
            node(5, "b"),
            node(6, "c"),
        ]);
        let state = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [1].into_iter().collect();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        assert!(!plan.movements.is_empty());
        for movement in &plan.movements {
            assert!(!movement.target_voters.contains(&1));
            // Three distinct domains are still required (6 eligible nodes,
            // 3 domains) and the planner keeps them.
            validate_voters(
                movement.shard_id,
                &movement.target_voters,
                &state.eligible_nodes,
                3,
            )
            .unwrap();
        }
    }

    #[test]
    fn insufficient_available_nodes_fails_closed() {
        let catalog = bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let state = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [2, 3].into_iter().collect();
        assert!(matches!(
            plan_rebalance(RebalanceInput {
                catalog: &state,
                unavailable_nodes: &unavailable,
            }),
            Err(RebalanceError::InsufficientAvailableNodes { available: 1 })
        ));
    }

    #[test]
    fn shard_with_active_movement_is_left_alone() {
        let mut catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let stable = state.placements[&5].voters;
        let spare = state
            .eligible_nodes
            .keys()
            .copied()
            .find(|node| !stable.contains(node))
            .unwrap();
        let mut target: Vec<RaftNodeId> = stable
            .iter()
            .copied()
            .filter(|node| *node != stable[2])
            .chain(std::iter::once(spare))
            .collect();
        target.sort_unstable();
        let target: [RaftNodeId; 3] = target.try_into().unwrap();
        catalog
            .apply(CatalogCommand::BeginMovement {
                expected_epoch: 1,
                operation_id: [7u8; 16],
                shard_id: 5,
                target_voters: target,
            })
            .unwrap();
        let state = catalog.state().unwrap().clone();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &empty_unavailable(),
        })
        .unwrap();
        assert!(plan.movements.iter().all(|m| m.shard_id != 5));
    }

    #[test]
    fn unavailable_desired_leader_is_reassigned() {
        let nodes = vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")];
        let placements = (0..8u16)
            .map(|shard_id| placement(shard_id, [1, 2, 3], 4))
            .collect();
        let catalog = small_catalog(nodes, placements);
        let unavailable: BTreeSet<RaftNodeId> = [4].into_iter().collect();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &catalog,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        // Node 4 is not a voter here, so no movements; every shard's desired
        // leader moves off the unavailable node to a real voter.
        assert!(plan.movements.is_empty());
        assert_eq!(plan.leader_reassignments.len(), 8);
        for reassignment in &plan.leader_reassignments {
            assert_eq!(reassignment.from, 4);
            assert!([1, 2, 3].contains(&reassignment.to));
        }
    }

    #[test]
    fn leader_counts_are_balanced_within_one() {
        let nodes = vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")];
        // All 8 shards desire node 1; voters rotate so reassignment targets
        // always exist.
        let placements = (0..8u16)
            .map(|shard_id| {
                let voters = match shard_id % 3 {
                    0 => [1, 2, 3],
                    1 => [1, 2, 4],
                    _ => [1, 3, 4],
                };
                placement(shard_id, voters, 1)
            })
            .collect();
        let catalog = small_catalog(nodes, placements);
        let plan = plan_rebalance(RebalanceInput {
            catalog: &catalog,
            unavailable_nodes: &empty_unavailable(),
        })
        .unwrap();
        assert_eq!(plan.leader_reassignments.len(), 6);
        // Apply the reassignments and check the balance.
        let mut counts: BTreeMap<RaftNodeId, u64> =
            [(1, 8), (2, 0), (3, 0), (4, 0)].into_iter().collect();
        for reassignment in &plan.leader_reassignments {
            *counts.get_mut(&reassignment.from).unwrap() -= 1;
            *counts.entry(reassignment.to).or_insert(0) += 1;
            // The new leader is always a stable voter of the shard.
            assert!(catalog.placements[&reassignment.shard_id]
                .voters
                .contains(&reassignment.to));
        }
        let max = counts.values().max().unwrap();
        let min = counts.values().min().unwrap();
        assert!(max - min <= 1, "leader counts: {counts:?}");
    }

    #[test]
    fn gossip_unavailable_never_changes_committed_authority() {
        // The unavailable set is advisory: planning emits intents and hint
        // changes only. Stable placements in the input are byte-identical
        // after planning, and no plan element can cancel or publish.
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let before = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [4].into_iter().collect();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &before,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        assert_eq!(catalog.state().unwrap(), &before);
        for movement in &plan.movements {
            assert_eq!(
                before.placements[&movement.shard_id].voters,
                movement.source_voters
            );
        }
    }

    // -- scheduler ---------------------------------------------------------

    struct FakeDrive {
        catalog: Mutex<PlacementCatalog>,
        script: Mutex<BTreeMap<u16, VecDeque<DriverOutcome>>>,
        begin_log: Mutex<Vec<(u16, [u8; 16])>>,
        cancel_log: Mutex<Vec<u16>>,
        stale_begins: Mutex<BTreeMap<u16, u32>>,
        /// Every `begin_movement` call, including stale-epoch attempts that
        /// never reach the catalog.
        begin_attempts: Mutex<BTreeMap<u16, u32>>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl FakeDrive {
        fn new(catalog: PlacementCatalog) -> Self {
            Self {
                catalog: Mutex::new(catalog),
                script: Mutex::new(BTreeMap::new()),
                begin_log: Mutex::new(Vec::new()),
                cancel_log: Mutex::new(Vec::new()),
                stale_begins: Mutex::new(BTreeMap::new()),
                begin_attempts: Mutex::new(BTreeMap::new()),
                inflight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }
        }

        fn script_outcome(&self, shard_id: u16, outcome: DriverOutcome) {
            self.script
                .lock()
                .unwrap()
                .entry(shard_id)
                .or_default()
                .push_back(outcome);
        }

        /// The default drive: publish the committed intent immediately, like
        /// a healthy movement driver would.
        fn default_drive(&self, shard_id: u16) -> DriverOutcome {
            let mut catalog = self.catalog.lock().unwrap();
            let state = catalog.state().expect("catalog is initialized").clone();
            let pending = state.placements[&shard_id]
                .pending_movement
                .clone()
                .expect("intent was committed");
            match catalog.apply(CatalogCommand::PublishMovement {
                expected_epoch: state.placement_epoch,
                operation_id: pending.operation_id,
                shard_id,
                observed_voters: pending.target_voters,
            }) {
                Ok(_) => DriverOutcome::Completed {
                    operation_id: pending.operation_id,
                    phases_advanced: 8,
                },
                Err(error) => DriverOutcome::FailClosed {
                    operation_id: pending.operation_id,
                    reason: error.to_string(),
                },
            }
        }
    }

    #[async_trait]
    impl RebalanceDrive for FakeDrive {
        async fn begin_movement(
            &self,
            movement: &PlannedMovement,
            expected_epoch: PlacementEpoch,
        ) -> Result<BeginOutcome, String> {
            *self
                .begin_attempts
                .lock()
                .unwrap()
                .entry(movement.shard_id)
                .or_insert(0) += 1;
            if let Some(remaining) = self
                .stale_begins
                .lock()
                .unwrap()
                .get_mut(&movement.shard_id)
            {
                if *remaining > 0 {
                    *remaining -= 1;
                    return Ok(BeginOutcome::StaleEpoch);
                }
            }
            self.begin_log
                .lock()
                .unwrap()
                .push((movement.shard_id, movement.operation_id));
            let mut catalog = self.catalog.lock().unwrap();
            match catalog.apply(CatalogCommand::BeginMovement {
                expected_epoch,
                operation_id: movement.operation_id,
                shard_id: movement.shard_id,
                target_voters: movement.target_voters,
            }) {
                Ok(CatalogResponse::MovementAccepted { .. }) => Ok(BeginOutcome::Accepted),
                Ok(CatalogResponse::MovementAlreadyActive { .. }) => {
                    Ok(BeginOutcome::AlreadyActive)
                }
                Ok(other) => Err(format!("unexpected catalog response: {other:?}")),
                Err(PlacementError::StaleCatalogView { .. }) => Ok(BeginOutcome::StaleEpoch),
                Err(PlacementError::ConflictingMovement { shard_id }) => Ok(
                    BeginOutcome::Rejected(format!("conflicting movement on shard {shard_id}")),
                ),
                Err(error) => Err(error.to_string()),
            }
        }

        async fn drive_movement(&self, shard_id: u16) -> DriverOutcome {
            let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            sleep(Duration::from_millis(20)).await;
            let outcome = self
                .script
                .lock()
                .unwrap()
                .get_mut(&shard_id)
                .and_then(|queue| queue.pop_front())
                .unwrap_or_else(|| self.default_drive(shard_id));
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            outcome
        }

        async fn reassign_leader(
            &self,
            shard_id: u16,
            new_leader: RaftNodeId,
            expected_epoch: PlacementEpoch,
        ) -> Result<ReassignOutcome, String> {
            let mut catalog = self.catalog.lock().unwrap();
            match catalog.apply(CatalogCommand::SetDesiredLeader {
                expected_epoch,
                shard_id,
                new_leader,
            }) {
                Ok(CatalogResponse::DesiredLeaderUpdated { .. }) => Ok(ReassignOutcome::Updated),
                Ok(CatalogResponse::DesiredLeaderAlreadySet) => Ok(ReassignOutcome::AlreadySet),
                Ok(other) => Err(format!("unexpected catalog response: {other:?}")),
                Err(PlacementError::StaleCatalogView { .. }) => Ok(ReassignOutcome::StaleEpoch),
                Err(PlacementError::ConflictingMovement { shard_id }) => Ok(
                    ReassignOutcome::Rejected(format!("conflicting movement on shard {shard_id}")),
                ),
                Err(error) => Err(error.to_string()),
            }
        }

        async fn cancel_movement(&self, shard_id: u16, _reason: String) -> CancelOutcome {
            self.cancel_log.lock().unwrap().push(shard_id);
            let mut catalog = self.catalog.lock().unwrap();
            let state = catalog.state().expect("catalog is initialized").clone();
            let placement = &state.placements[&shard_id];
            let Some(pending) = placement.pending_movement.clone() else {
                return CancelOutcome::NothingToCancel;
            };
            match catalog.apply(CatalogCommand::CancelMovement {
                expected_epoch: state.placement_epoch,
                operation_id: pending.operation_id,
                shard_id,
                observed_voters: pending.source_voters,
                reason: "test cancel".to_string(),
            }) {
                Ok(_) => CancelOutcome::Cancelled,
                Err(error) => CancelOutcome::CatalogUnavailable(error.to_string()),
            }
        }

        async fn read_catalog(&self) -> Result<CatalogState, String> {
            self.catalog
                .lock()
                .unwrap()
                .state()
                .cloned()
                .ok_or_else(|| "catalog is not initialized".to_string())
        }
    }

    /// Build a real bootstrapped catalog, plan the evacuation of `evacuated`,
    /// and keep the first `shard_count` planned movements plus all planned
    /// leader reassignments.
    fn evacuation_fixture(
        shard_count: usize,
    ) -> (Arc<FakeDrive>, RebalancePlan, BTreeSet<RaftNodeId>) {
        evacuation_fixture_on(
            shard_count,
            vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")],
            4,
        )
    }

    fn evacuation_fixture_on(
        shard_count: usize,
        nodes: Vec<EligibleNode>,
        evacuated: RaftNodeId,
    ) -> (Arc<FakeDrive>, RebalancePlan, BTreeSet<RaftNodeId>) {
        let catalog = bootstrap_catalog(nodes);
        let state = catalog.state().unwrap().clone();
        let unavailable: BTreeSet<RaftNodeId> = [evacuated].into_iter().collect();
        let mut plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &unavailable,
        })
        .unwrap();
        assert!(plan.movements.len() >= shard_count);
        plan.movements.truncate(shard_count);
        let drive = Arc::new(FakeDrive::new(catalog));
        (drive, plan, unavailable)
    }

    fn scheduler_config(cluster: usize, per_node: usize) -> RebalanceSchedulerConfig {
        RebalanceSchedulerConfig {
            work: MovementWorkConfig {
                cluster_max_concurrent: cluster,
                per_node_max_concurrent: per_node,
            },
            max_shard_failures: 3,
        }
    }

    fn new_scheduler(
        drive: Arc<FakeDrive>,
        plan: RebalancePlan,
        unavailable: BTreeSet<RaftNodeId>,
        config: RebalanceSchedulerConfig,
    ) -> RebalanceScheduler<Arc<FakeDrive>> {
        RebalanceScheduler::new(plan, config, drive, unavailable).unwrap()
    }

    /// Poll until no shard is queued or running (bounded so a bug fails the
    /// test instead of hanging it).
    async fn drain(scheduler: &mut RebalanceScheduler<Arc<FakeDrive>>) -> SchedulerStatus {
        for _ in 0..25 {
            scheduler.poll().await;
            let status = scheduler.status();
            if status.queued + status.running == 0 {
                return status;
            }
        }
        panic!("scheduler did not drain: {:?}", scheduler.status());
    }

    /// The node a planned movement adds (the replica receiving the data).
    fn added_voter(movement: &PlannedMovement) -> RaftNodeId {
        movement
            .target_voters
            .iter()
            .copied()
            .find(|voter| !movement.source_voters.contains(voter))
            .expect("a movement always adds exactly one voter")
    }

    #[tokio::test]
    async fn scheduler_drives_plan_to_completion_within_bounds() {
        let (drive, plan, unavailable) = evacuation_fixture(6);
        let moved_shards: Vec<u16> = plan.movements.iter().map(|m| m.shard_id).collect();
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(2, 2),
        );
        let status = drain(&mut scheduler).await;
        assert_eq!(status.completed, 6);
        assert_eq!(status.failed, 0);
        assert_eq!(status.blocked, 0);
        // The concurrency bound was honored and actually used.
        assert_eq!(drive.peak.load(Ordering::SeqCst), 2);
        assert_eq!(status.admission.cluster_peak_inflight, 2);
        assert_eq!(status.admission.cluster_inflight, 0);
        // Every planned target is now the committed stable placement.
        let catalog = drive.read_catalog().await.unwrap();
        for shard_id in moved_shards {
            let placement = &catalog.placements[&shard_id];
            assert!(placement.pending_movement.is_none());
            assert!(!placement.voters.contains(&4));
        }
    }

    #[tokio::test]
    async fn target_unavailable_shards_block_and_resume() {
        let (drive, plan, _unavailable) = evacuation_fixture(4);
        let blocked_node = added_voter(&plan.movements[0]);
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            [blocked_node].into_iter().collect(),
            scheduler_config(4, 4),
        );
        scheduler.poll().await;
        let status = scheduler.status();
        assert!(status.blocked >= 1, "expected blocked shards: {status:?}");
        assert_eq!(status.failed, 0);
        // Blocked shards never committed an intent.
        for (shard_id, _) in drive.begin_log.lock().unwrap().iter() {
            assert!(
                !matches!(
                    scheduler.status().shards.get(shard_id),
                    Some(PlanShardState::Blocked(_))
                ),
                "blocked shard {shard_id} must not begin"
            );
        }
        // The node returns: blocked shards resume and complete.
        scheduler.set_unavailable(BTreeSet::new());
        let status = drain(&mut scheduler).await;
        assert_eq!(status.completed, 4);
        assert_eq!(status.blocked, 0);
    }

    #[tokio::test]
    async fn stale_epoch_begin_retries_on_next_poll() {
        let (drive, plan, unavailable) = evacuation_fixture(2);
        let stale_shard = plan.movements[0].shard_id;
        drive.stale_begins.lock().unwrap().insert(stale_shard, 1);
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(2, 2),
        );
        scheduler.poll().await;
        let status = scheduler.status();
        assert_eq!(status.completed, 1);
        assert_eq!(status.queued, 1);
        let status = drain(&mut scheduler).await;
        assert_eq!(status.completed, 2);
        // The stale attempt never reached the catalog, but the retry did.
        let attempts = drive.begin_attempts.lock().unwrap();
        assert_eq!(attempts.get(&stale_shard), Some(&2));
        let log = drive.begin_log.lock().unwrap();
        assert_eq!(
            log.iter()
                .filter(|(shard, _)| *shard == stale_shard)
                .count(),
            1,
            "only the retry commits an intent"
        );
    }

    #[tokio::test]
    async fn conflicting_foreign_intent_fails_the_shard_closed() {
        let (drive, plan, unavailable) = evacuation_fixture(3);
        let movement = &plan.movements[0];
        let shard = movement.shard_id;
        // A foreign controller commits a different intent first.
        let spare = [1u64, 2, 3, 4]
            .into_iter()
            .find(|node| !movement.source_voters.contains(node))
            .unwrap();
        let mut foreign_target = movement.source_voters;
        foreign_target[0] = spare;
        foreign_target.sort_unstable();
        {
            let mut catalog = drive.catalog.lock().unwrap();
            let epoch = catalog.state().unwrap().placement_epoch;
            catalog
                .apply(CatalogCommand::BeginMovement {
                    expected_epoch: epoch,
                    operation_id: [9u8; 16],
                    shard_id: shard,
                    target_voters: foreign_target,
                })
                .unwrap();
        }
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(3, 3),
        );
        let status = drain(&mut scheduler).await;
        assert!(
            matches!(status.shards.get(&shard), Some(PlanShardState::Failed(_))),
            "foreign intent must fail the shard closed: {status:?}"
        );
        assert_eq!(status.completed, 2);
        // The foreign intent is untouched: the scheduler never clobbers it.
        let catalog = drive.read_catalog().await.unwrap();
        let pending = catalog.placements[&shard].pending_movement.clone().unwrap();
        assert_eq!(pending.operation_id, [9u8; 16]);
    }

    #[tokio::test]
    async fn partial_plan_recovery_after_restart() {
        let (drive, plan, unavailable) = evacuation_fixture(4);
        let op_ids: BTreeMap<u16, [u8; 16]> = plan
            .movements
            .iter()
            .map(|m| (m.shard_id, m.operation_id))
            .collect();
        // First controller commits every intent, then "crashes" before any
        // drive completes.
        for movement in &plan.movements {
            drive.script_outcome(
                movement.shard_id,
                DriverOutcome::TransientFailure {
                    reason: "simulated crash".to_string(),
                },
            );
        }
        let mut first = new_scheduler(
            Arc::clone(&drive),
            plan.clone(),
            unavailable.clone(),
            scheduler_config(4, 4),
        );
        first.poll().await;
        assert_eq!(first.status().queued, 4);
        drop(first);

        // Failover controller recovers from the committed catalog.
        let mut second = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(4, 4),
        );
        let catalog = drive.read_catalog().await.unwrap();
        second.recover(&catalog);
        assert_eq!(second.status().queued, 4);
        let status = drain(&mut second).await;
        assert_eq!(status.completed, 4);
        assert_eq!(status.failed, 0);
        // Exactly one intent identity per shard across both controllers: the
        // re-begin was idempotent, never a duplicate.
        let log = drive.begin_log.lock().unwrap();
        for (shard_id, op_id) in &op_ids {
            let begins: Vec<_> = log.iter().filter(|(shard, _)| shard == shard_id).collect();
            assert_eq!(begins.len(), 2);
            assert!(begins.iter().all(|(_, id)| id == op_id));
        }
    }

    #[tokio::test]
    async fn failover_without_recover_adopts_completed_movements() {
        let (drive, plan, unavailable) = evacuation_fixture(2);
        let mut first = new_scheduler(
            Arc::clone(&drive),
            plan.clone(),
            unavailable.clone(),
            scheduler_config(2, 2),
        );
        let status = drain(&mut first).await;
        assert_eq!(status.completed, 2);
        drop(first);
        // A second scheduler driving the same plan without an explicit
        // recover adopts the committed results on its first poll.
        let mut second = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(2, 2),
        );
        second.poll().await;
        let status = second.status();
        assert_eq!(status.completed, 2);
        assert_eq!(status.queued, 0);
    }

    #[tokio::test]
    async fn leader_reassignments_are_applied_with_epoch_bumps() {
        let (drive, plan, unavailable) = evacuation_fixture(2);
        let reassign_count = plan.leader_reassignments.len();
        assert!(reassign_count > 0, "evacuation should reassign leaders");
        let epoch_before = drive.read_catalog().await.unwrap().placement_epoch;
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan.clone(),
            unavailable,
            scheduler_config(2, 2),
        );
        let status = drain(&mut scheduler).await;
        assert_eq!(status.completed, 2);
        assert_eq!(status.leaders_done, reassign_count);
        assert_eq!(status.leaders_pending, 0);
        assert_eq!(status.leaders_failed, 0);
        let catalog = drive.read_catalog().await.unwrap();
        // One epoch bump per reassignment plus one per published movement.
        assert_eq!(
            catalog.placement_epoch,
            epoch_before + reassign_count as u64 + 2
        );
        for reassignment in &plan.leader_reassignments {
            assert_eq!(
                catalog.placements[&reassignment.shard_id].desired_leader,
                reassignment.to
            );
        }
    }

    #[tokio::test]
    async fn transient_budget_exhaustion_fails_the_shard() {
        let (drive, plan, unavailable) = evacuation_fixture(1);
        let shard = plan.movements[0].shard_id;
        for _ in 0..5 {
            drive.script_outcome(
                shard,
                DriverOutcome::TransientFailure {
                    reason: "boom".to_string(),
                },
            );
        }
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(1, 1),
        );
        let status = drain(&mut scheduler).await;
        assert!(
            matches!(
                status.shards.get(&shard),
                Some(PlanShardState::Failed(reason)) if reason.contains("budget exhausted")
            ),
            "unexpected status: {status:?}"
        );
    }

    #[tokio::test]
    async fn cancel_plan_before_begin_drops_intents() {
        let (drive, plan, unavailable) = evacuation_fixture(3);
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            unavailable,
            scheduler_config(3, 3),
        );
        scheduler.cancel_plan("operator stop".to_string()).await;
        let status = scheduler.status();
        assert_eq!(status.failed, 3);
        assert!(drive.begin_log.lock().unwrap().is_empty());
        let catalog = drive.read_catalog().await.unwrap();
        assert!(catalog
            .placements
            .values()
            .all(|placement| placement.pending_movement.is_none()));
    }

    #[tokio::test]
    async fn cancel_plan_after_begin_uses_safe_cancel() {
        let (drive, plan, unavailable) = evacuation_fixture(2);
        for movement in &plan.movements {
            drive.script_outcome(
                movement.shard_id,
                DriverOutcome::TransientFailure {
                    reason: "slow".to_string(),
                },
            );
        }
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan.clone(),
            unavailable,
            scheduler_config(2, 2),
        );
        scheduler.poll().await;
        assert_eq!(scheduler.status().queued, 2);
        scheduler.cancel_plan("operator stop".to_string()).await;
        let status = scheduler.status();
        assert_eq!(status.failed, 2);
        assert_eq!(drive.cancel_log.lock().unwrap().len(), 2);
        // Pre-membership cancel leaves the old stable placement authoritative.
        let catalog = drive.read_catalog().await.unwrap();
        for movement in &plan.movements {
            let placement = &catalog.placements[&movement.shard_id];
            assert!(placement.pending_movement.is_none());
            assert_eq!(placement.voters, movement.source_voters);
        }
    }

    #[tokio::test]
    async fn status_counts_cover_all_lifecycle_states() {
        // Evacuation spreads replacements across nodes; pick four movements
        // that cover every lifecycle state: one conflicted by a foreign
        // intent, one blocked on an unavailable target, two that complete.
        // A five-node cluster gives varied targets so only some movements
        // touch the blocked node.
        let (drive, full_plan, _unavailable) = evacuation_fixture_on(
            120,
            vec![
                node(1, "a"),
                node(2, "b"),
                node(3, "c"),
                node(4, "d"),
                node(5, "e"),
            ],
            5,
        );
        let blocked = full_plan.movements[0].clone();
        let blocked_node = added_voter(&blocked);
        let mut live: Vec<PlannedMovement> = full_plan
            .movements
            .iter()
            .filter(|movement| !movement.target_voters.contains(&blocked_node))
            .take(3)
            .cloned()
            .collect();
        assert_eq!(
            live.len(),
            3,
            "need three movements avoiding {blocked_node}"
        );
        let conflict = live.remove(0);
        let ok1 = live.remove(0);
        let ok2 = live.remove(0);
        let conflict_shard = conflict.shard_id;
        // A foreign controller commits a different intent first: swap a
        // voter the plan keeps for the spare node.
        let spare = [1u64, 2, 3, 4]
            .into_iter()
            .find(|node| !conflict.source_voters.contains(node))
            .unwrap();
        let mut foreign_target = conflict.source_voters;
        foreign_target[0] = spare;
        foreign_target.sort_unstable();
        assert_ne!(foreign_target, conflict.source_voters);
        assert_ne!(foreign_target, conflict.target_voters);

        let mut plan = full_plan;
        plan.movements = vec![conflict, blocked, ok1, ok2];
        {
            let mut catalog = drive.catalog.lock().unwrap();
            let epoch = catalog.state().unwrap().placement_epoch;
            catalog
                .apply(CatalogCommand::BeginMovement {
                    expected_epoch: epoch,
                    operation_id: [9u8; 16],
                    shard_id: conflict_shard,
                    target_voters: foreign_target,
                })
                .unwrap();
        }
        let mut scheduler = new_scheduler(
            Arc::clone(&drive),
            plan,
            [blocked_node].into_iter().collect(),
            scheduler_config(4, 4),
        );
        let status = drain(&mut scheduler).await;
        assert_eq!(status.completed, 2, "{status:?}");
        assert_eq!(status.failed, 1, "{status:?}");
        assert_eq!(status.blocked, 1, "{status:?}");
        assert_eq!(status.queued, 0, "{status:?}");
        assert_eq!(status.running, 0, "{status:?}");
        assert!(
            matches!(
                status.shards.get(&conflict_shard),
                Some(PlanShardState::Failed(_))
            ),
            "{status:?}"
        );
    }

    #[test]
    fn invalid_scheduler_config_is_rejected() {
        let catalog = bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let state = catalog.state().unwrap().clone();
        let plan = plan_rebalance(RebalanceInput {
            catalog: &state,
            unavailable_nodes: &empty_unavailable(),
        })
        .unwrap();
        let bad = RebalanceSchedulerConfig {
            work: MovementWorkConfig {
                cluster_max_concurrent: 0,
                per_node_max_concurrent: 1,
            },
            max_shard_failures: 3,
        };
        assert!(matches!(
            RebalanceScheduler::new(
                plan,
                bad,
                Arc::new(FakeDrive::new(catalog)),
                BTreeSet::new()
            ),
            Err(RebalanceError::InvalidSchedulerConfig(_))
        ));
    }

    // -- topology view -----------------------------------------------------

    #[test]
    fn topology_view_is_serializable_and_reports_skew() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let view = build_topology_view(
            &state,
            &empty_unavailable(),
            CatalogHealth::Healthy,
            Vec::new(),
            None,
        );
        assert_eq!(view.nodes.len(), 4);
        assert!(view.voter_skew <= 1, "voter skew: {}", view.voter_skew);
        assert!(view.leader_skew <= 1, "leader skew: {}", view.leader_skew);
        assert_eq!(view.placement_epoch, 1);
        assert!(view.active_movements.is_empty());
        assert!(view.nodes.iter().all(|node| node.available));
        let json = serde_json::to_string(&view).unwrap();
        let decoded: TopologyView = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, view);
    }

    #[test]
    fn topology_view_reports_active_movements_and_plan() {
        let mut catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        // Find a shard whose stable voters differ from [1,2,3] so the begin
        // actually applies.
        let shard_id = catalog
            .state()
            .unwrap()
            .placements
            .iter()
            .find(|(_, placement)| placement.voters != [1, 2, 3])
            .map(|(shard_id, _)| *shard_id)
            .unwrap();
        catalog
            .apply(CatalogCommand::BeginMovement {
                expected_epoch: 1,
                operation_id: [5u8; 16],
                shard_id,
                target_voters: [1, 2, 3],
            })
            .unwrap();
        let state = catalog.state().unwrap().clone();
        let view = build_topology_view(
            &state,
            &[4].into_iter().collect(),
            CatalogHealth::Degraded {
                unavailable_nodes: vec![4],
            },
            Vec::new(),
            Some(PlanSummary {
                source_epoch: 1,
                queued: 3,
                running: 1,
                blocked: 0,
                failed: 0,
                completed: 2,
                leaders_pending: 0,
                leaders_done: 0,
                leaders_failed: 0,
            }),
        );
        assert_eq!(view.active_movements.len(), 1);
        assert_eq!(view.active_movements[0].shard_id, shard_id);
        assert!(
            !view
                .nodes
                .iter()
                .find(|node| node.node_id == 4)
                .unwrap()
                .available
        );
        assert_eq!(view.plan.as_ref().unwrap().queued, 3);
        assert_eq!(view.plan.as_ref().unwrap().completed, 2);
    }

    // -- catalog health gate -------------------------------------------------

    struct StaticProbe(CatalogHealth);

    impl CatalogHealthProbe for StaticProbe {
        fn health(&self) -> CatalogHealth {
            self.0.clone()
        }
    }

    struct DirectPort {
        catalog: Mutex<PlacementCatalog>,
    }

    #[async_trait]
    impl CatalogPort for DirectPort {
        async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
            self.catalog
                .lock()
                .unwrap()
                .state()
                .cloned()
                .ok_or(CatalogGroupError::MissingCommittedState)
        }

        async fn submit(
            &self,
            command: CatalogCommand,
        ) -> Result<CatalogResponse, CatalogGroupError> {
            self.catalog
                .lock()
                .unwrap()
                .apply(command)
                .map_err(CatalogGroupError::from)
        }
    }

    fn movement_command() -> CatalogCommand {
        CatalogCommand::BeginMovement {
            expected_epoch: 1,
            operation_id: [3u8; 16],
            shard_id: 5,
            target_voters: [1, 2, 3],
        }
    }

    #[tokio::test]
    async fn quorum_loss_blocks_mutations_but_not_reads() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let gate = CatalogHealthGate::new(
            DirectPort {
                catalog: Mutex::new(catalog),
            },
            StaticProbe(CatalogHealth::QuorumLost),
        );
        // Reads keep working: data groups continue under their last committed
        // memberships (REQ-M4-BAL-005).
        assert!(gate.read_committed().await.is_ok());
        // New placement mutations are blocked.
        assert!(matches!(
            gate.submit(movement_command()).await,
            Err(CatalogGroupError::ConsensusUnavailable)
        ));
        assert!(matches!(
            gate.submit(CatalogCommand::SetDesiredLeader {
                expected_epoch: 1,
                shard_id: 5,
                new_leader: 1,
            })
            .await,
            Err(CatalogGroupError::ConsensusUnavailable)
        ));
    }

    #[tokio::test]
    async fn bootstrap_is_allowed_without_quorum() {
        let gate = CatalogHealthGate::new(
            DirectPort {
                catalog: Mutex::new(PlacementCatalog::default()),
            },
            StaticProbe(CatalogHealth::Unknown),
        );
        let response = gate
            .submit(CatalogCommand::Bootstrap {
                cluster_id: cluster_id(),
                eligible_nodes: vec![node(1, "a"), node(2, "b"), node(3, "c")],
            })
            .await
            .unwrap();
        assert!(matches!(response, CatalogResponse::Initialized { .. }));
        // But a placement mutation is still blocked while health is unknown.
        assert!(matches!(
            gate.submit(movement_command()).await,
            Err(CatalogGroupError::ConsensusUnavailable)
        ));
    }

    #[tokio::test]
    async fn degraded_health_allows_mutations() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        // Find a shard where node 1 is a stable voter.
        let shard_id = catalog
            .state()
            .unwrap()
            .placements
            .iter()
            .find(|(_, placement)| placement.voters.contains(&1))
            .map(|(shard_id, _)| *shard_id)
            .unwrap();
        let gate = CatalogHealthGate::new(
            DirectPort {
                catalog: Mutex::new(catalog),
            },
            StaticProbe(CatalogHealth::Degraded {
                unavailable_nodes: vec![4],
            }),
        );
        // Quorum intact: the control plane keeps working.
        let response = gate
            .submit(CatalogCommand::SetDesiredLeader {
                expected_epoch: 1,
                shard_id,
                new_leader: 1,
            })
            .await
            .unwrap();
        assert!(matches!(
            response,
            CatalogResponse::DesiredLeaderUpdated { .. } | CatalogResponse::DesiredLeaderAlreadySet
        ));
    }

    // -- per-group Raft fields (REQ-M4-OPS-001) --------------------------------

    use crate::raft_observability::{
        ElectionIdentity, LeadershipMetricsSnapshot, LogPosition, MembershipStatus, ReplicaMember,
    };

    fn log_position(term: u64, index: u64) -> LogPosition {
        LogPosition {
            term,
            leader_id: 1,
            index,
        }
    }

    fn replica_status(
        role: ReplicaRole,
        term: u64,
        voters: Vec<u64>,
        learners: Vec<u64>,
        commit_index: Option<u64>,
        apply_index: Option<u64>,
    ) -> ReplicaStatus {
        let members = voters
            .iter()
            .map(|node_id| ReplicaMember {
                node_id: *node_id,
                endpoint: format!("127.0.0.1:{node_id}"),
                voter: true,
            })
            .chain(learners.iter().map(|node_id| ReplicaMember {
                node_id: *node_id,
                endpoint: format!("127.0.0.1:{node_id}"),
                voter: false,
            }))
            .collect();
        ReplicaStatus {
            node_id: 1,
            role,
            health: ReplicaHealth::Running,
            leader_id: Some(1),
            current_term: term,
            vote: ElectionIdentity {
                term,
                candidate_id: 1,
                committed: true,
            },
            last_log_index: commit_index,
            committed: commit_index.map(|index| log_position(term, index)),
            applied: apply_index.map(|index| log_position(term, index)),
            membership: MembershipStatus {
                log: None,
                voters,
                members,
            },
            snapshot: Some(log_position(term.saturating_sub(1), 40)),
            purged: None,
            leadership: LeadershipMetricsSnapshot::default(),
        }
    }

    #[test]
    fn group_raft_view_maps_role_term_config_and_progress() {
        let status = replica_status(
            ReplicaRole::Leader,
            7,
            vec![3, 1, 2],
            vec![9],
            Some(100),
            Some(96),
        );
        let view = GroupRaftView::from_status(42, &status);
        assert_eq!(view.shard_id, 42);
        assert_eq!(view.role, ReplicaRole::Leader);
        assert_eq!(view.health, ReplicaHealth::Running);
        assert_eq!(view.term, 7);
        assert_eq!(view.leader_id, Some(1));
        assert_eq!(view.voters, vec![1, 2, 3]);
        assert_eq!(view.learners, vec![9]);
        assert_eq!(view.log_index, Some(100));
        assert_eq!(view.commit_index, Some(100));
        assert_eq!(view.apply_index, Some(96));
        assert_eq!(view.apply_lag, 4);
        assert_eq!(view.snapshot_index, Some(40));
    }

    #[test]
    fn group_raft_view_lag_saturates_without_progress() {
        let status = replica_status(ReplicaRole::Follower, 3, vec![1, 2, 3], vec![], None, None);
        let view = GroupRaftView::from_status(7, &status);
        assert_eq!(view.commit_index, None);
        assert_eq!(view.apply_index, None);
        assert_eq!(view.apply_lag, 0);
        assert_eq!(view.snapshot_index, Some(40));
    }

    #[test]
    fn topology_view_carries_per_group_entries_sorted_by_shard() {
        let catalog =
            bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let state = catalog.state().unwrap().clone();
        let groups = vec![
            GroupRaftView::from_status(
                9,
                &replica_status(
                    ReplicaRole::Follower,
                    2,
                    vec![1, 2, 3],
                    vec![],
                    Some(10),
                    Some(10),
                ),
            ),
            GroupRaftView::from_status(
                3,
                &replica_status(
                    ReplicaRole::Leader,
                    2,
                    vec![1, 2, 3],
                    vec![],
                    Some(12),
                    Some(11),
                ),
            ),
        ];
        let view = build_topology_view(
            &state,
            &empty_unavailable(),
            CatalogHealth::Healthy,
            groups,
            None,
        );
        assert_eq!(view.groups.len(), 2);
        assert_eq!(view.groups[0].shard_id, 3);
        assert_eq!(view.groups[1].shard_id, 9);
        assert_eq!(view.groups[0].role, ReplicaRole::Leader);
        assert_eq!(view.groups[0].apply_lag, 1);
        assert_eq!(view.groups[1].apply_lag, 0);
    }

    #[test]
    fn topology_view_without_groups_stays_backward_compatible() {
        let catalog = bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let state = catalog.state().unwrap().clone();
        let view = build_topology_view(
            &state,
            &empty_unavailable(),
            CatalogHealth::Healthy,
            Vec::new(),
            None,
        );
        assert!(view.groups.is_empty());
        assert!(!view.nodes.is_empty());
    }

    #[test]
    fn topology_view_serialization_exposes_no_openraft_types() {
        let catalog = bootstrap_catalog(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let state = catalog.state().unwrap().clone();
        let groups = vec![GroupRaftView::from_status(
            5,
            &replica_status(
                ReplicaRole::Candidate,
                4,
                vec![1, 2, 3],
                vec![7],
                Some(20),
                Some(18),
            ),
        )];
        let view = build_topology_view(
            &state,
            &empty_unavailable(),
            CatalogHealth::Healthy,
            groups,
            None,
        );
        let json = serde_json::to_string(&view).unwrap();
        // The contract must not leak OpenRaft Rust type names.
        for leaked in [
            "openraft",
            "ServerState",
            "RaftMetrics",
            "StoredMembership",
            "MembershipConfig",
        ] {
            assert!(
                !json.contains(leaked),
                "topology JSON leaks OpenRaft type name: {leaked}"
            );
        }
        let round_tripped: TopologyView = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, view);
        assert_eq!(round_tripped.groups[0].learners, vec![7]);
        assert_eq!(round_tripped.groups[0].apply_lag, 2);
    }
}
