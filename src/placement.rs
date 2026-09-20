use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde_derive::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::raft::RaftNodeId;
use crate::storage::LOGICAL_SHARD_COUNT;

/// Reserved state-machine key holding the placement catalog snapshot.
///
/// The catalog group applies `CatalogCommand`s atomically against this key
/// inside the Raft state machine (`RaftCommand::CatalogMutation`), so
/// expected-epoch validation happens in replicated log order and concurrent
/// controllers cannot clobber each other's intent (REQ-M4-MOVE-004).
pub const CATALOG_STATE_KEY: &[u8] = b"\0homekv/system/placement-catalog/v1";

pub type GroupId = u64;
pub type PlacementEpoch = u64;
pub type ClusterId = [u8; 16];

pub const PLACEMENT_CATALOG_GROUP_ID: GroupId = 0;
pub const PLACEMENT_FORMAT_VERSION: u16 = 1;
pub const MAX_ELIGIBLE_NODES: usize = 1024;
pub const MAX_ENDPOINT_BYTES: usize = 1024;
pub const MAX_FAILURE_DOMAIN_BYTES: usize = 128;
/// Upper bound for operator-supplied movement notes and cancel reasons
/// persisted in the catalog (REQ-M4-MOVE-001 retry state stays bounded).
pub const MAX_MOVEMENT_NOTE_BYTES: usize = 256;

const SNAPSHOT_MAGIC: &[u8; 8] = b"HKVPLC01";
const SNAPSHOT_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;

pub fn data_group_id(shard_id: u16) -> Result<GroupId, PlacementError> {
    if shard_id >= LOGICAL_SHARD_COUNT {
        return Err(PlacementError::InvalidShard(shard_id));
    }
    Ok(u64::from(shard_id) + 1)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EligibleNode {
    pub node_id: RaftNodeId,
    pub raft_endpoint: String,
    pub failure_domain: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum MovementPhase {
    Intent,
    Learner,
    CatchUp,
    Promote,
    Lead,
    Remove,
    Publish,
    Cleanup,
}

/// The forward phase that follows `phase` in a normal movement.
/// `Publish` and `Cleanup` are terminal transitions driven by their own
/// atomic catalog commands, so they have no `AdvanceMovementPhase`
/// successor.
pub fn next_phase(phase: MovementPhase) -> Option<MovementPhase> {
    match phase {
        MovementPhase::Intent => Some(MovementPhase::Learner),
        MovementPhase::Learner => Some(MovementPhase::CatchUp),
        MovementPhase::CatchUp => Some(MovementPhase::Promote),
        MovementPhase::Promote => Some(MovementPhase::Lead),
        MovementPhase::Lead => Some(MovementPhase::Remove),
        MovementPhase::Remove | MovementPhase::Publish | MovementPhase::Cleanup => None,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingMovement {
    pub operation_id: [u8; 16],
    pub source_epoch: PlacementEpoch,
    pub source_voters: [RaftNodeId; 3],
    pub target_voters: [RaftNodeId; 3],
    pub phase: MovementPhase,
    pub retries: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShardPlacement {
    pub shard_id: u16,
    pub group_id: GroupId,
    pub voters: [RaftNodeId; 3],
    pub desired_leader: RaftNodeId,
    pub epoch: PlacementEpoch,
    pub pending_movement: Option<PendingMovement>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogState {
    pub format_version: u16,
    pub cluster_id: ClusterId,
    pub placement_epoch: PlacementEpoch,
    pub eligible_nodes: BTreeMap<RaftNodeId, EligibleNode>,
    pub placements: BTreeMap<u16, ShardPlacement>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CatalogCommand {
    Bootstrap {
        cluster_id: ClusterId,
        eligible_nodes: Vec<EligibleNode>,
    },
    /// Commit pending movement intent for one shard (phase `Intent`).
    ///
    /// `expected_epoch` is a compare-and-swap guard: the command applies
    /// only when the committed catalog epoch still matches the view the
    /// controller read, so two controllers cannot silently clobber each
    /// other's intent. Stable placement is unchanged by this command
    /// (REQ-M4-MOVE-002).
    BeginMovement {
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        target_voters: [RaftNodeId; 3],
    },
    /// Advance a movement's durable phase marker after the corresponding
    /// data-group step succeeded. Forward-only and idempotent for the
    /// current phase; `Publish` is reached only through `PublishMovement`.
    AdvanceMovementPhase {
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        phase: MovementPhase,
    },
    /// Record one reconciler attempt against a movement (REQ-M4-MOVE-001
    /// retry state). Does not change the phase.
    RecordMovementAttempt {
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        succeeded: bool,
        note: Option<String>,
    },
    /// Atomically replace the stable placement with the movement target,
    /// increment the global epoch, and clear the pending intent. Applies
    /// only when `observed_voters` equals the committed data-group
    /// membership the reconciler observed (REQ-M4-MOVE-002).
    PublishMovement {
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        observed_voters: [RaftNodeId; 3],
    },
    /// Cancel a movement whose membership has not changed yet. Applies only
    /// when `observed_voters` still equals the source membership, leaving
    /// the old stable placement authoritative (REQ-M4-MOVE-005).
    CancelMovement {
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        observed_voters: [RaftNodeId; 3],
        reason: String,
    },
    /// Reassign the advisory desired-leader hint for one shard
    /// (REQ-M4-BAL-001 leader distribution). Applies only when
    /// `new_leader` is a stable voter of the shard and no movement is
    /// active; the global epoch is bumped so route versions observe the
    /// change. Idempotent when the hint is already set.
    SetDesiredLeader {
        expected_epoch: PlacementEpoch,
        shard_id: u16,
        new_leader: RaftNodeId,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CatalogResponse {
    Initialized { placement_epoch: PlacementEpoch },
    AlreadyInitialized { placement_epoch: PlacementEpoch },
    MovementAccepted { phase: MovementPhase },
    MovementAlreadyActive { operation_id: [u8; 16] },
    PhaseAdvanced { phase: MovementPhase },
    AttemptRecorded { retries: u32 },
    MovementPublished { placement_epoch: PlacementEpoch },
    MovementCancelled,
    DesiredLeaderUpdated { placement_epoch: PlacementEpoch },
    DesiredLeaderAlreadySet,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementCatalog {
    state: Option<CatalogState>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CatalogSnapshotImage {
    format_version: u16,
    state: CatalogState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PlacementError {
    InvalidShard(u16),
    EmptyClusterId,
    TooFewEligibleNodes(usize),
    TooManyEligibleNodes(usize),
    DuplicateNode(RaftNodeId),
    InvalidEndpoint(RaftNodeId),
    InvalidFailureDomain(RaftNodeId),
    AlreadyInitializedWithDifferentConfiguration,
    NotInitialized,
    EpochExhausted,
    StaleCatalogView {
        expected: PlacementEpoch,
        actual: PlacementEpoch,
    },
    EmptyOperationId,
    UnknownMovement {
        shard_id: u16,
    },
    ConflictingMovement {
        shard_id: u16,
    },
    InvalidMovementTarget {
        shard_id: u16,
        reason: String,
    },
    InvalidPhaseTransition {
        shard_id: u16,
        current: MovementPhase,
        requested: MovementPhase,
    },
    PublishPreconditionFailed {
        shard_id: u16,
    },
    MembershipAlreadyCommitted {
        shard_id: u16,
    },
    AmbiguousMembership {
        shard_id: u16,
    },
    InvalidMovementNote {
        shard_id: u16,
    },
    InvalidSnapshotEnvelope,
    UnsupportedSnapshotVersion(u16),
    SnapshotLengthMismatch,
    SnapshotChecksumMismatch,
    SnapshotDecode(String),
    InvalidCatalog(String),
}

impl fmt::Display for PlacementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShard(shard) => {
                write!(
                    f,
                    "logical shard id {shard} is outside 0..{LOGICAL_SHARD_COUNT}"
                )
            }
            Self::EmptyClusterId => write!(f, "cluster identity must not be all zeroes"),
            Self::TooFewEligibleNodes(count) => {
                write!(
                    f,
                    "RF=3 placement requires at least three eligible nodes, got {count}"
                )
            }
            Self::TooManyEligibleNodes(count) => {
                write!(
                    f,
                    "eligible node count {count} exceeds {MAX_ELIGIBLE_NODES}"
                )
            }
            Self::DuplicateNode(node) => write!(f, "eligible node {node} is duplicated"),
            Self::InvalidEndpoint(node) => {
                write!(f, "eligible node {node} has an invalid endpoint")
            }
            Self::InvalidFailureDomain(node) => {
                write!(f, "eligible node {node} has an invalid failure domain")
            }
            Self::AlreadyInitializedWithDifferentConfiguration => {
                write!(
                    f,
                    "placement catalog is already initialized with different authority"
                )
            }
            Self::NotInitialized => write!(f, "placement catalog is not initialized"),
            Self::EpochExhausted => write!(f, "placement epoch is exhausted"),
            Self::StaleCatalogView { expected, actual } => write!(
                f,
                "catalog view is stale: command expected epoch {expected} but committed epoch is {actual}"
            ),
            Self::EmptyOperationId => {
                write!(f, "movement operation identity must not be empty")
            }
            Self::UnknownMovement { shard_id } => {
                write!(f, "shard {shard_id} has no active movement")
            }
            Self::ConflictingMovement { shard_id } => write!(
                f,
                "shard {shard_id} already has an active movement operation"
            ),
            Self::InvalidMovementTarget { shard_id, reason } => {
                write!(f, "shard {shard_id} movement target is invalid: {reason}")
            }
            Self::InvalidPhaseTransition {
                shard_id,
                current,
                requested,
            } => write!(
                f,
                "shard {shard_id} cannot advance movement from {current:?} to {requested:?}"
            ),
            Self::PublishPreconditionFailed { shard_id } => write!(
                f,
                "shard {shard_id} publish requires the observed data-group membership to equal the movement target"
            ),
            Self::MembershipAlreadyCommitted { shard_id } => write!(
                f,
                "shard {shard_id} movement already committed its membership change and must finish forward"
            ),
            Self::AmbiguousMembership { shard_id } => write!(
                f,
                "shard {shard_id} observed membership matches neither source nor target; refusing to guess"
            ),
            Self::InvalidMovementNote { shard_id } => write!(
                f,
                "shard {shard_id} movement note exceeds {MAX_MOVEMENT_NOTE_BYTES} bytes"
            ),
            Self::InvalidSnapshotEnvelope => write!(f, "invalid placement snapshot envelope"),
            Self::UnsupportedSnapshotVersion(version) => {
                write!(f, "unsupported placement snapshot version {version}")
            }
            Self::SnapshotLengthMismatch => write!(f, "placement snapshot length mismatch"),
            Self::SnapshotChecksumMismatch => write!(f, "placement snapshot checksum mismatch"),
            Self::SnapshotDecode(message) => {
                write!(f, "placement snapshot decode failed: {message}")
            }
            Self::InvalidCatalog(message) => write!(f, "invalid placement catalog: {message}"),
        }
    }
}

impl std::error::Error for PlacementError {}

impl PlacementCatalog {
    pub fn apply(&mut self, command: CatalogCommand) -> Result<CatalogResponse, PlacementError> {
        match command {
            CatalogCommand::Bootstrap {
                cluster_id,
                eligible_nodes,
            } => {
                let candidate = CatalogState::bootstrap(cluster_id, eligible_nodes)?;
                match &self.state {
                    None => {
                        let placement_epoch = candidate.placement_epoch;
                        self.state = Some(candidate);
                        Ok(CatalogResponse::Initialized { placement_epoch })
                    }
                    Some(current) if current == &candidate => {
                        Ok(CatalogResponse::AlreadyInitialized {
                            placement_epoch: current.placement_epoch,
                        })
                    }
                    Some(_) => Err(PlacementError::AlreadyInitializedWithDifferentConfiguration),
                }
            }
            CatalogCommand::BeginMovement {
                expected_epoch,
                operation_id,
                shard_id,
                target_voters,
            } => self.begin_movement(expected_epoch, operation_id, shard_id, target_voters),
            CatalogCommand::AdvanceMovementPhase {
                expected_epoch,
                operation_id,
                shard_id,
                phase,
            } => self.advance_movement_phase(expected_epoch, operation_id, shard_id, phase),
            CatalogCommand::RecordMovementAttempt {
                expected_epoch,
                operation_id,
                shard_id,
                succeeded,
                note,
            } => self.record_movement_attempt(
                expected_epoch,
                operation_id,
                shard_id,
                succeeded,
                note,
            ),
            CatalogCommand::PublishMovement {
                expected_epoch,
                operation_id,
                shard_id,
                observed_voters,
            } => self.publish_movement(expected_epoch, operation_id, shard_id, observed_voters),
            CatalogCommand::CancelMovement {
                expected_epoch,
                operation_id,
                shard_id,
                observed_voters,
                reason,
            } => self.cancel_movement(
                expected_epoch,
                operation_id,
                shard_id,
                observed_voters,
                reason,
            ),
            CatalogCommand::SetDesiredLeader {
                expected_epoch,
                shard_id,
                new_leader,
            } => self.set_desired_leader(expected_epoch, shard_id, new_leader),
        }
    }

    pub fn state(&self) -> Option<&CatalogState> {
        self.state.as_ref()
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>, PlacementError> {
        let state = self.state.as_ref().ok_or(PlacementError::NotInitialized)?;
        state.validate()?;
        Self::encode_image(&CatalogSnapshotImage {
            format_version: PLACEMENT_FORMAT_VERSION,
            state: state.clone(),
        })
    }

    pub fn restore_snapshot(bytes: &[u8]) -> Result<Self, PlacementError> {
        if bytes.len() < SNAPSHOT_HEADER_LEN || &bytes[..8] != SNAPSHOT_MAGIC {
            return Err(PlacementError::InvalidSnapshotEnvelope);
        }
        let version = u16::from_le_bytes(
            bytes[8..10]
                .try_into()
                .map_err(|_| PlacementError::InvalidSnapshotEnvelope)?,
        );
        if version != PLACEMENT_FORMAT_VERSION {
            return Err(PlacementError::UnsupportedSnapshotVersion(version));
        }
        let reserved = u16::from_le_bytes(
            bytes[10..12]
                .try_into()
                .map_err(|_| PlacementError::InvalidSnapshotEnvelope)?,
        );
        if reserved != 0 {
            return Err(PlacementError::InvalidSnapshotEnvelope);
        }
        let payload_len_u64 = u64::from_le_bytes(
            bytes[12..20]
                .try_into()
                .map_err(|_| PlacementError::InvalidSnapshotEnvelope)?,
        );
        let payload_len =
            usize::try_from(payload_len_u64).map_err(|_| PlacementError::SnapshotLengthMismatch)?;
        let checksum = u64::from_le_bytes(
            bytes[20..28]
                .try_into()
                .map_err(|_| PlacementError::InvalidSnapshotEnvelope)?,
        );
        if bytes.len() != SNAPSHOT_HEADER_LEN.saturating_add(payload_len) {
            return Err(PlacementError::SnapshotLengthMismatch);
        }
        let payload = &bytes[SNAPSHOT_HEADER_LEN..];
        if xxh3_64(payload) != checksum {
            return Err(PlacementError::SnapshotChecksumMismatch);
        }
        let image: CatalogSnapshotImage = bincode::deserialize(payload)
            .map_err(|error| PlacementError::SnapshotDecode(error.to_string()))?;
        if image.format_version != PLACEMENT_FORMAT_VERSION {
            return Err(PlacementError::UnsupportedSnapshotVersion(
                image.format_version,
            ));
        }
        image.state.validate()?;
        Ok(Self {
            state: Some(image.state),
        })
    }

    fn encode_image(image: &CatalogSnapshotImage) -> Result<Vec<u8>, PlacementError> {
        let payload = bincode::serialize(image)
            .map_err(|error| PlacementError::SnapshotDecode(error.to_string()))?;
        let payload_len =
            u64::try_from(payload.len()).map_err(|_| PlacementError::SnapshotLengthMismatch)?;
        let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_LEN + payload.len());
        bytes.extend_from_slice(SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&PLACEMENT_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    fn movement_state(
        &mut self,
        expected_epoch: PlacementEpoch,
        shard_id: u16,
    ) -> Result<&mut CatalogState, PlacementError> {
        let state = self.state.as_mut().ok_or(PlacementError::NotInitialized)?;
        if state.placement_epoch != expected_epoch {
            return Err(PlacementError::StaleCatalogView {
                expected: expected_epoch,
                actual: state.placement_epoch,
            });
        }
        if !state.placements.contains_key(&shard_id) {
            // `data_group_id` rejects out-of-range shards with InvalidShard.
            data_group_id(shard_id)?;
            return Err(PlacementError::InvalidCatalog(format!(
                "missing shard {shard_id}"
            )));
        }
        Ok(state)
    }

    fn active_movement<'a>(
        state: &'a CatalogState,
        shard_id: u16,
        operation_id: &[u8; 16],
    ) -> Result<&'a PendingMovement, PlacementError> {
        state
            .placements
            .get(&shard_id)
            .and_then(|placement| placement.pending_movement.as_ref())
            .filter(|pending| &pending.operation_id == operation_id)
            .ok_or(PlacementError::UnknownMovement { shard_id })
    }

    fn active_movement_mut<'a>(
        state: &'a mut CatalogState,
        shard_id: u16,
        operation_id: &[u8; 16],
    ) -> Result<&'a mut PendingMovement, PlacementError> {
        state
            .placements
            .get_mut(&shard_id)
            .and_then(|placement| placement.pending_movement.as_mut())
            .filter(|pending| &pending.operation_id == operation_id)
            .ok_or(PlacementError::UnknownMovement { shard_id })
    }

    fn begin_movement(
        &mut self,
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        target_voters: [RaftNodeId; 3],
    ) -> Result<CatalogResponse, PlacementError> {
        if operation_id.iter().all(|byte| *byte == 0) {
            return Err(PlacementError::EmptyOperationId);
        }
        let state = self.movement_state(expected_epoch, shard_id)?;
        // `validate_voters` enforces canonical ordering, eligibility and
        // failure-domain spread for the target (REQ-M4-MAP-004).
        let distinct_domains = state
            .eligible_nodes
            .values()
            .map(|node| node.failure_domain.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        validate_voters(
            shard_id,
            &target_voters,
            &state.eligible_nodes,
            distinct_domains,
        )
        .map_err(|error| PlacementError::InvalidMovementTarget {
            shard_id,
            reason: error.to_string(),
        })?;

        let placement = state
            .placements
            .get_mut(&shard_id)
            .expect("movement_state validated the shard");
        if target_voters == placement.voters {
            return Err(PlacementError::InvalidMovementTarget {
                shard_id,
                reason: "target voters equal the stable placement".to_string(),
            });
        }
        if let Some(active) = &placement.pending_movement {
            if active.operation_id == operation_id && active.target_voters == target_voters {
                // Idempotent retry of the same intent (REQ-M4-MOVE-004).
                return Ok(CatalogResponse::MovementAlreadyActive { operation_id });
            }
            return Err(PlacementError::ConflictingMovement { shard_id });
        }

        placement.pending_movement = Some(PendingMovement {
            operation_id,
            source_epoch: state.placement_epoch,
            source_voters: placement.voters,
            target_voters,
            phase: MovementPhase::Intent,
            retries: 0,
            last_error: None,
        });
        Ok(CatalogResponse::MovementAccepted {
            phase: MovementPhase::Intent,
        })
    }

    fn advance_movement_phase(
        &mut self,
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        phase: MovementPhase,
    ) -> Result<CatalogResponse, PlacementError> {
        let state = self.movement_state(expected_epoch, shard_id)?;
        let pending = Self::active_movement_mut(state, shard_id, &operation_id)?;
        if pending.phase == phase {
            return Ok(CatalogResponse::PhaseAdvanced { phase });
        }
        match next_phase(pending.phase) {
            Some(next) if next == phase => {
                pending.phase = phase;
                Ok(CatalogResponse::PhaseAdvanced { phase })
            }
            _ => Err(PlacementError::InvalidPhaseTransition {
                shard_id,
                current: pending.phase,
                requested: phase,
            }),
        }
    }

    fn record_movement_attempt(
        &mut self,
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        succeeded: bool,
        note: Option<String>,
    ) -> Result<CatalogResponse, PlacementError> {
        if let Some(note) = &note {
            if note.len() > MAX_MOVEMENT_NOTE_BYTES {
                return Err(PlacementError::InvalidMovementNote { shard_id });
            }
        }
        let state = self.movement_state(expected_epoch, shard_id)?;
        let pending = Self::active_movement_mut(state, shard_id, &operation_id)?;
        pending.retries = pending.retries.saturating_add(1);
        pending.last_error = if succeeded { None } else { note };
        Ok(CatalogResponse::AttemptRecorded {
            retries: pending.retries,
        })
    }

    fn publish_movement(
        &mut self,
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        observed_voters: [RaftNodeId; 3],
    ) -> Result<CatalogResponse, PlacementError> {
        let state = self.movement_state(expected_epoch, shard_id)?;
        // The borrow dance below keeps the epoch increment and the stable
        // record replacement atomic inside one committed command.
        let target_voters = {
            let pending = Self::active_movement(state, shard_id, &operation_id)?;
            if observed_voters != pending.target_voters {
                return Err(PlacementError::PublishPreconditionFailed { shard_id });
            }
            pending.target_voters
        };
        let new_epoch = state
            .placement_epoch
            .checked_add(1)
            .ok_or(PlacementError::EpochExhausted)?;
        let placement = state
            .placements
            .get_mut(&shard_id)
            .expect("movement_state validated the shard");
        let desired_leader = if target_voters.contains(&placement.desired_leader) {
            placement.desired_leader
        } else {
            // Deterministic fallback: the lowest target voter. The hint is
            // advisory; data-group authority elects the real leader.
            target_voters[0]
        };
        placement.voters = target_voters;
        placement.desired_leader = desired_leader;
        placement.epoch = new_epoch;
        placement.pending_movement = None;
        state.placement_epoch = new_epoch;
        Ok(CatalogResponse::MovementPublished {
            placement_epoch: new_epoch,
        })
    }

    fn cancel_movement(
        &mut self,
        expected_epoch: PlacementEpoch,
        operation_id: [u8; 16],
        shard_id: u16,
        observed_voters: [RaftNodeId; 3],
        reason: String,
    ) -> Result<CatalogResponse, PlacementError> {
        if reason.len() > MAX_MOVEMENT_NOTE_BYTES {
            return Err(PlacementError::InvalidMovementNote { shard_id });
        }
        let state = self.movement_state(expected_epoch, shard_id)?;
        let (source_voters, target_voters) = {
            let pending = Self::active_movement(state, shard_id, &operation_id)?;
            (pending.source_voters, pending.target_voters)
        };
        if observed_voters == source_voters {
            // Pre-membership cancellation: the old stable placement stays
            // authoritative (REQ-M4-MOVE-005).
            let placement = state
                .placements
                .get_mut(&shard_id)
                .expect("movement_state validated the shard");
            placement.pending_movement = None;
            return Ok(CatalogResponse::MovementCancelled);
        }
        if observed_voters == target_voters {
            // Membership already committed: cancellation must finish the
            // reconciliation forward instead (REQ-M4-MOVE-005).
            return Err(PlacementError::MembershipAlreadyCommitted { shard_id });
        }
        // Observed membership matches neither side: fail closed rather than
        // guessing which placement is authoritative (REQ-M4-MOVE-004).
        Err(PlacementError::AmbiguousMembership { shard_id })
    }

    /// Reassign the advisory desired-leader hint for one shard.
    ///
    /// The hint never confers authority (REQ-M4-ROUTE-003); it only steers
    /// clients toward a balanced leader. The command is rejected while a
    /// movement is active on the shard so it cannot race the driver's own
    /// desired-leader maintenance at publish time, and it requires the
    /// new leader to be a stable voter so the hint always names a real
    /// replica (REQ-M4-BAL-001).
    fn set_desired_leader(
        &mut self,
        expected_epoch: PlacementEpoch,
        shard_id: u16,
        new_leader: RaftNodeId,
    ) -> Result<CatalogResponse, PlacementError> {
        let state = self.movement_state(expected_epoch, shard_id)?;
        let placement = state
            .placements
            .get(&shard_id)
            .expect("movement_state validated the shard");
        if placement.pending_movement.is_some() {
            return Err(PlacementError::ConflictingMovement { shard_id });
        }
        if !placement.voters.contains(&new_leader) {
            return Err(PlacementError::InvalidMovementTarget {
                shard_id,
                reason: format!("desired leader {new_leader} is not a stable voter"),
            });
        }
        if placement.desired_leader == new_leader {
            return Ok(CatalogResponse::DesiredLeaderAlreadySet);
        }
        let new_epoch = state
            .placement_epoch
            .checked_add(1)
            .ok_or(PlacementError::EpochExhausted)?;
        let placement = state
            .placements
            .get_mut(&shard_id)
            .expect("movement_state validated the shard");
        placement.desired_leader = new_leader;
        placement.epoch = new_epoch;
        state.placement_epoch = new_epoch;
        Ok(CatalogResponse::DesiredLeaderUpdated {
            placement_epoch: new_epoch,
        })
    }
}

impl CatalogState {
    fn bootstrap(
        cluster_id: ClusterId,
        eligible_nodes: Vec<EligibleNode>,
    ) -> Result<Self, PlacementError> {
        if cluster_id.iter().all(|byte| *byte == 0) {
            return Err(PlacementError::EmptyClusterId);
        }
        if eligible_nodes.len() < 3 {
            return Err(PlacementError::TooFewEligibleNodes(eligible_nodes.len()));
        }
        if eligible_nodes.len() > MAX_ELIGIBLE_NODES {
            return Err(PlacementError::TooManyEligibleNodes(eligible_nodes.len()));
        }

        let mut nodes = BTreeMap::new();
        for node in eligible_nodes {
            validate_node(&node)?;
            let node_id = node.node_id;
            if nodes.insert(node_id, node).is_some() {
                return Err(PlacementError::DuplicateNode(node_id));
            }
        }

        let placements = build_initial_placements(&nodes)?;
        let state = Self {
            format_version: PLACEMENT_FORMAT_VERSION,
            cluster_id,
            placement_epoch: 1,
            eligible_nodes: nodes,
            placements,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), PlacementError> {
        if self.format_version != PLACEMENT_FORMAT_VERSION {
            return Err(PlacementError::InvalidCatalog(format!(
                "format version {} is unsupported",
                self.format_version
            )));
        }
        if self.cluster_id.iter().all(|byte| *byte == 0) {
            return Err(PlacementError::EmptyClusterId);
        }
        if self.placement_epoch == 0 {
            return Err(PlacementError::InvalidCatalog(
                "placement epoch must be non-zero".to_string(),
            ));
        }
        if self.eligible_nodes.len() < 3 {
            return Err(PlacementError::TooFewEligibleNodes(
                self.eligible_nodes.len(),
            ));
        }
        if self.eligible_nodes.len() > MAX_ELIGIBLE_NODES {
            return Err(PlacementError::TooManyEligibleNodes(
                self.eligible_nodes.len(),
            ));
        }
        for (node_id, node) in &self.eligible_nodes {
            if node_id != &node.node_id {
                return Err(PlacementError::InvalidCatalog(format!(
                    "node map key {node_id} does not match node {}",
                    node.node_id
                )));
            }
            validate_node(node)?;
        }
        if self.placements.len() != usize::from(LOGICAL_SHARD_COUNT) {
            return Err(PlacementError::InvalidCatalog(format!(
                "expected {LOGICAL_SHARD_COUNT} placements, got {}",
                self.placements.len()
            )));
        }

        let distinct_domains = self
            .eligible_nodes
            .values()
            .map(|node| node.failure_domain.as_str())
            .collect::<BTreeSet<_>>()
            .len();

        for shard_id in 0..LOGICAL_SHARD_COUNT {
            let placement = self.placements.get(&shard_id).ok_or_else(|| {
                PlacementError::InvalidCatalog(format!("missing shard {shard_id}"))
            })?;
            if placement.shard_id != shard_id {
                return Err(PlacementError::InvalidCatalog(format!(
                    "shard map key {shard_id} does not match record {}",
                    placement.shard_id
                )));
            }
            if placement.group_id != data_group_id(shard_id)? {
                return Err(PlacementError::InvalidCatalog(format!(
                    "shard {shard_id} has non-canonical group {}",
                    placement.group_id
                )));
            }
            if placement.epoch == 0 || placement.epoch > self.placement_epoch {
                return Err(PlacementError::InvalidCatalog(format!(
                    "shard {shard_id} epoch {} is outside catalog epoch {}",
                    placement.epoch, self.placement_epoch
                )));
            }
            validate_voters(
                shard_id,
                &placement.voters,
                &self.eligible_nodes,
                distinct_domains,
            )?;
            if !placement.voters.contains(&placement.desired_leader) {
                return Err(PlacementError::InvalidCatalog(format!(
                    "shard {shard_id} desired leader {} is not a voter",
                    placement.desired_leader
                )));
            }
            if let Some(pending) = &placement.pending_movement {
                validate_pending(
                    shard_id,
                    pending,
                    &self.eligible_nodes,
                    distinct_domains,
                    self.placement_epoch,
                )?;
            }
        }
        Ok(())
    }
}

fn validate_node(node: &EligibleNode) -> Result<(), PlacementError> {
    let endpoint_len = node.raft_endpoint.as_bytes().len();
    if endpoint_len == 0 || endpoint_len > MAX_ENDPOINT_BYTES {
        return Err(PlacementError::InvalidEndpoint(node.node_id));
    }
    let domain_len = node.failure_domain.as_bytes().len();
    if domain_len == 0 || domain_len > MAX_FAILURE_DOMAIN_BYTES {
        return Err(PlacementError::InvalidFailureDomain(node.node_id));
    }
    Ok(())
}

pub(crate) fn validate_voters(
    shard_id: u16,
    voters: &[RaftNodeId; 3],
    nodes: &BTreeMap<RaftNodeId, EligibleNode>,
    distinct_domains: usize,
) -> Result<(), PlacementError> {
    if voters[0] >= voters[1] || voters[1] >= voters[2] {
        return Err(PlacementError::InvalidCatalog(format!(
            "shard {shard_id} voters are not canonical and distinct"
        )));
    }
    let mut domains = BTreeSet::new();
    for voter in voters {
        let node = nodes.get(voter).ok_or_else(|| {
            PlacementError::InvalidCatalog(format!(
                "shard {shard_id} references unknown voter {voter}"
            ))
        })?;
        domains.insert(node.failure_domain.as_str());
    }
    if distinct_domains >= 3 && domains.len() != 3 {
        return Err(PlacementError::InvalidCatalog(format!(
            "shard {shard_id} does not span three failure domains"
        )));
    }
    Ok(())
}

fn validate_pending(
    shard_id: u16,
    pending: &PendingMovement,
    nodes: &BTreeMap<RaftNodeId, EligibleNode>,
    distinct_domains: usize,
    placement_epoch: PlacementEpoch,
) -> Result<(), PlacementError> {
    if pending.operation_id.iter().all(|byte| *byte == 0) {
        return Err(PlacementError::InvalidCatalog(format!(
            "shard {shard_id} movement has an empty operation identity"
        )));
    }
    if pending.source_epoch == 0 || pending.source_epoch > placement_epoch {
        return Err(PlacementError::InvalidCatalog(format!(
            "shard {shard_id} movement source epoch is invalid"
        )));
    }
    validate_voters(shard_id, &pending.source_voters, nodes, distinct_domains)?;
    validate_voters(shard_id, &pending.target_voters, nodes, distinct_domains)
}

fn build_initial_placements(
    nodes: &BTreeMap<RaftNodeId, EligibleNode>,
) -> Result<BTreeMap<u16, ShardPlacement>, PlacementError> {
    let node_ids = nodes.keys().copied().collect::<Vec<_>>();
    let distinct_domains = nodes
        .values()
        .map(|node| node.failure_domain.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let require_distinct_domains = distinct_domains >= 3;
    let mut voter_counts = BTreeMap::<RaftNodeId, u64>::new();
    let mut leader_counts = BTreeMap::<RaftNodeId, u64>::new();
    for node_id in &node_ids {
        voter_counts.insert(*node_id, 0);
        leader_counts.insert(*node_id, 0);
    }

    let mut placements = BTreeMap::new();
    for shard_id in 0..LOGICAL_SHARD_COUNT {
        let mut selected: Vec<RaftNodeId> = Vec::with_capacity(3);
        let mut selected_domains = BTreeSet::new();
        for replica in 0..3usize {
            let anchor = (usize::from(shard_id) * 3 + replica) % node_ids.len();
            let candidate = node_ids
                .iter()
                .enumerate()
                .filter(|(_, node_id)| !selected.contains(*node_id))
                .filter(|(_, node_id)| {
                    !require_distinct_domains
                        || !selected_domains.contains(
                            nodes
                                .get(node_id)
                                .expect("candidate comes from eligible node map")
                                .failure_domain
                                .as_str(),
                        )
                })
                .min_by_key(|(index, node_id)| {
                    (
                        voter_counts.get(node_id).copied().unwrap_or_default(),
                        rotated_distance(*index, anchor, node_ids.len()),
                        **node_id,
                    )
                })
                .map(|(_, node_id)| *node_id)
                .ok_or_else(|| {
                    PlacementError::InvalidCatalog(format!(
                        "cannot select RF=3 placement for shard {shard_id}"
                    ))
                })?;
            selected.push(candidate);
            selected_domains.insert(
                nodes
                    .get(&candidate)
                    .expect("selected node comes from eligible node map")
                    .failure_domain
                    .as_str(),
            );
            *voter_counts
                .get_mut(&candidate)
                .expect("selected node has a voter counter") += 1;
        }

        let leader_anchor = usize::from(shard_id) % node_ids.len();
        let desired_leader = selected
            .iter()
            .min_by_key(|node_id| {
                let index = node_ids
                    .binary_search(node_id)
                    .expect("selected node comes from sorted eligible nodes");
                (
                    leader_counts.get(node_id).copied().unwrap_or_default(),
                    rotated_distance(index, leader_anchor, node_ids.len()),
                    **node_id,
                )
            })
            .copied()
            .expect("RF=3 selection is non-empty");
        *leader_counts
            .get_mut(&desired_leader)
            .expect("leader has a counter") += 1;

        selected.sort_unstable();
        let voters: [RaftNodeId; 3] = selected
            .try_into()
            .expect("exactly three voters are selected");
        placements.insert(
            shard_id,
            ShardPlacement {
                shard_id,
                group_id: data_group_id(shard_id)?,
                voters,
                desired_leader,
                epoch: 1,
                pending_movement: None,
            },
        );
    }
    Ok(placements)
}

fn rotated_distance(index: usize, anchor: usize, len: usize) -> usize {
    (index + len - anchor) % len
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(node_id: u64, domain: &str) -> EligibleNode {
        EligibleNode {
            node_id,
            raft_endpoint: format!("127.0.0.1:{}", 19_000 + node_id),
            failure_domain: domain.to_string(),
        }
    }

    fn cluster_id() -> ClusterId {
        *b"homekv-m4-test01"
    }

    fn bootstrap(nodes: Vec<EligibleNode>) -> PlacementCatalog {
        let mut catalog = PlacementCatalog::default();
        assert_eq!(
            catalog
                .apply(CatalogCommand::Bootstrap {
                    cluster_id: cluster_id(),
                    eligible_nodes: nodes,
                })
                .unwrap(),
            CatalogResponse::Initialized { placement_epoch: 1 }
        );
        catalog
    }

    fn spread(counts: &BTreeMap<u64, u64>) -> u64 {
        counts.values().max().unwrap() - counts.values().min().unwrap()
    }

    #[test]
    fn three_node_bootstrap_has_canonical_golden_identity() {
        let catalog = bootstrap(vec![node(3, "c"), node(1, "a"), node(2, "b")]);
        let state = catalog.state().unwrap();
        assert_eq!(state.placements.len(), 1024);
        assert_eq!(
            state.eligible_nodes.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let zero = state.placements.get(&0).unwrap();
        assert_eq!(zero.group_id, 1);
        assert_eq!(zero.voters, [1, 2, 3]);
        assert_eq!(zero.desired_leader, 1);

        let one = state.placements.get(&1).unwrap();
        assert_eq!(one.group_id, 2);
        assert_eq!(one.voters, [1, 2, 3]);
        assert_eq!(one.desired_leader, 2);

        let last = state.placements.get(&1023).unwrap();
        assert_eq!(last.group_id, 1024);
        assert_eq!(last.voters, [1, 2, 3]);
        assert_eq!(last.desired_leader, 1);
        state.validate().unwrap();
    }

    #[test]
    fn six_node_bootstrap_is_domain_safe_and_balanced() {
        let catalog = bootstrap((1..=6).map(|id| node(id, &format!("zone-{id}"))).collect());
        let state = catalog.state().unwrap();
        let mut voters = BTreeMap::from_iter((1..=6).map(|id| (id, 0u64)));
        let mut leaders = voters.clone();

        for placement in state.placements.values() {
            let domains = placement
                .voters
                .iter()
                .map(|node_id| state.eligible_nodes[node_id].failure_domain.as_str())
                .collect::<BTreeSet<_>>();
            assert_eq!(domains.len(), 3);
            for voter in placement.voters {
                *voters.get_mut(&voter).unwrap() += 1;
            }
            *leaders.get_mut(&placement.desired_leader).unwrap() += 1;
        }

        assert!(spread(&voters) <= 1, "voter counts: {voters:?}");
        assert!(spread(&leaders) <= 1, "leader counts: {leaders:?}");
    }

    #[test]
    fn node_input_order_does_not_change_bootstrap() {
        let first = bootstrap(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let second = bootstrap(vec![node(4, "d"), node(2, "b"), node(1, "a"), node(3, "c")]);
        assert_eq!(first, second);
    }

    #[test]
    fn identical_bootstrap_is_idempotent_and_conflicts_fail_closed() {
        let nodes = vec![node(1, "a"), node(2, "b"), node(3, "c")];
        let mut catalog = bootstrap(nodes.clone());
        assert_eq!(
            catalog
                .apply(CatalogCommand::Bootstrap {
                    cluster_id: cluster_id(),
                    eligible_nodes: nodes,
                })
                .unwrap(),
            CatalogResponse::AlreadyInitialized { placement_epoch: 1 }
        );
        let before = catalog.clone();
        assert_eq!(
            catalog.apply(CatalogCommand::Bootstrap {
                cluster_id: *b"other-cluster-id",
                eligible_nodes: vec![node(1, "a"), node(2, "b"), node(3, "c")],
            }),
            Err(PlacementError::AlreadyInitializedWithDifferentConfiguration)
        );
        assert_eq!(catalog, before);
    }

    #[test]
    fn invalid_bootstrap_is_rejected_without_state() {
        let mut catalog = PlacementCatalog::default();
        assert_eq!(
            catalog.apply(CatalogCommand::Bootstrap {
                cluster_id: [0; 16],
                eligible_nodes: vec![node(1, "a"), node(2, "b"), node(3, "c")],
            }),
            Err(PlacementError::EmptyClusterId)
        );
        assert_eq!(catalog.state(), None);

        assert_eq!(
            catalog.apply(CatalogCommand::Bootstrap {
                cluster_id: cluster_id(),
                eligible_nodes: vec![node(1, "a"), node(1, "b"), node(3, "c")],
            }),
            Err(PlacementError::DuplicateNode(1))
        );
        assert_eq!(catalog.state(), None);
    }

    #[test]
    fn snapshot_round_trip_is_deterministic_and_complete() {
        let catalog = bootstrap(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")]);
        let first = catalog.encode_snapshot().unwrap();
        let second = catalog.encode_snapshot().unwrap();
        assert_eq!(first, second);

        let restored = PlacementCatalog::restore_snapshot(&first).unwrap();
        assert_eq!(restored, catalog);
        assert_eq!(
            restored.state().unwrap().placements.len(),
            usize::from(LOGICAL_SHARD_COUNT)
        );
    }

    #[test]
    fn corrupt_truncated_and_unknown_version_snapshots_fail_closed() {
        let catalog = bootstrap(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let bytes = catalog.encode_snapshot().unwrap();

        let mut corrupt = bytes.clone();
        *corrupt.last_mut().unwrap() ^= 0xff;
        assert_eq!(
            PlacementCatalog::restore_snapshot(&corrupt),
            Err(PlacementError::SnapshotChecksumMismatch)
        );

        let mut truncated = bytes.clone();
        truncated.truncate(truncated.len() - 7);
        assert_eq!(
            PlacementCatalog::restore_snapshot(&truncated),
            Err(PlacementError::SnapshotLengthMismatch)
        );

        let mut unknown = bytes;
        unknown[8..10].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            PlacementCatalog::restore_snapshot(&unknown),
            Err(PlacementError::UnsupportedSnapshotVersion(2))
        );
    }

    #[test]
    fn integrity_valid_but_incomplete_catalog_is_rejected() {
        let catalog = bootstrap(vec![node(1, "a"), node(2, "b"), node(3, "c")]);
        let mut state = catalog.state().unwrap().clone();
        state.placements.remove(&77);
        let bytes = PlacementCatalog::encode_image(&CatalogSnapshotImage {
            format_version: PLACEMENT_FORMAT_VERSION,
            state,
        })
        .unwrap();

        let error = PlacementCatalog::restore_snapshot(&bytes).unwrap_err();
        assert!(matches!(error, PlacementError::InvalidCatalog(_)));
    }

    #[test]
    fn group_identity_is_injective_and_catalog_group_is_reserved() {
        let ids = (0..LOGICAL_SHARD_COUNT)
            .map(|shard| data_group_id(shard).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), usize::from(LOGICAL_SHARD_COUNT));
        assert!(!ids.contains(&PLACEMENT_CATALOG_GROUP_ID));
        assert_eq!(
            data_group_id(LOGICAL_SHARD_COUNT),
            Err(PlacementError::InvalidShard(1024))
        );
    }

    fn four_node_catalog() -> PlacementCatalog {
        bootstrap(vec![node(1, "a"), node(2, "b"), node(3, "c"), node(4, "d")])
    }

    fn op_id(byte: u8) -> [u8; 16] {
        [byte; 16]
    }

    /// Valid movement intent for a shard: keep two stable voters, swap the
    /// third for the spare eligible node, canonically sorted.
    fn movement_target(
        catalog: &PlacementCatalog,
        shard_id: u16,
        removed: RaftNodeId,
    ) -> ([RaftNodeId; 3], [RaftNodeId; 3]) {
        let state = catalog.state().unwrap();
        let stable: [RaftNodeId; 3] = state.placements.get(&shard_id).unwrap().voters;
        assert!(
            stable.contains(&removed),
            "removed node must be a stable voter"
        );
        let spare = state
            .eligible_nodes
            .keys()
            .copied()
            .find(|node| !stable.contains(node))
            .expect("four eligible nodes and three voters leave one spare");
        let mut target: Vec<RaftNodeId> = stable
            .iter()
            .copied()
            .filter(|node| *node != removed)
            .chain(std::iter::once(spare))
            .collect();
        target.sort_unstable();
        (
            stable,
            target.try_into().unwrap_or_else(|voters: Vec<RaftNodeId>| {
                panic!("exactly three target voters, got {}", voters.len())
            }),
        )
    }

    fn begin(
        catalog: &mut PlacementCatalog,
        operation_id: [u8; 16],
        shard_id: u16,
        target: [RaftNodeId; 3],
    ) -> Result<CatalogResponse, PlacementError> {
        catalog.apply(CatalogCommand::BeginMovement {
            expected_epoch: 1,
            operation_id,
            shard_id,
            target_voters: target,
        })
    }

    fn pending(catalog: &PlacementCatalog, shard_id: u16) -> Option<&PendingMovement> {
        catalog
            .state()
            .unwrap()
            .placements
            .get(&shard_id)
            .unwrap()
            .pending_movement
            .as_ref()
    }

    #[test]
    fn begin_movement_commits_intent_without_touching_stable() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        assert_eq!(
            begin(&mut catalog, op_id(7), 5, target),
            Ok(CatalogResponse::MovementAccepted {
                phase: MovementPhase::Intent
            })
        );
        let state = catalog.state().unwrap();
        assert_eq!(state.placement_epoch, 1);
        let placement = state.placements.get(&5).unwrap();
        // Stable placement is unchanged by intent (REQ-M4-MOVE-002).
        assert_eq!(placement.voters, stable);
        assert_eq!(placement.epoch, 1);
        let movement = pending(&catalog, 5).unwrap();
        assert_eq!(movement.operation_id, op_id(7));
        assert_eq!(movement.source_epoch, 1);
        assert_eq!(movement.source_voters, stable);
        assert_eq!(movement.target_voters, target);
        assert_eq!(movement.phase, MovementPhase::Intent);
        assert_eq!(movement.retries, 0);
        assert_eq!(movement.last_error, None);
        state.validate().unwrap();
    }

    #[test]
    fn begin_movement_is_idempotent_for_the_same_intent() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        let (_, other_target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[1]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        assert_eq!(
            begin(&mut catalog, op_id(7), 5, target),
            Ok(CatalogResponse::MovementAlreadyActive {
                operation_id: op_id(7)
            })
        );
        // At most one active movement per shard (REQ-M4-MOVE-001).
        assert_eq!(
            begin(&mut catalog, op_id(8), 5, target),
            Err(PlacementError::ConflictingMovement { shard_id: 5 })
        );
        assert_eq!(
            begin(&mut catalog, op_id(7), 5, other_target),
            Err(PlacementError::ConflictingMovement { shard_id: 5 })
        );
        // A different shard is unaffected.
        let (_, shard6_target) = movement_target(&catalog, 6, {
            let state = catalog.state().unwrap();
            state.placements.get(&6).unwrap().voters[2]
        });
        assert!(begin(&mut catalog, op_id(8), 6, shard6_target).is_ok());
        let _ = stable;
    }

    #[test]
    fn begin_movement_validates_target_and_identity() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        assert_eq!(
            begin(&mut catalog, [0; 16], 5, target),
            Err(PlacementError::EmptyOperationId)
        );
        assert!(matches!(
            begin(&mut catalog, op_id(1), 5, stable),
            Err(PlacementError::InvalidMovementTarget { shard_id: 5, .. })
        ));
        // Non-canonical (unsorted) target.
        let mut unsorted = target;
        unsorted.swap(0, 2);
        assert!(matches!(
            begin(&mut catalog, op_id(1), 5, unsorted),
            Err(PlacementError::InvalidMovementTarget { shard_id: 5, .. })
        ));
        // Duplicate voter.
        assert!(matches!(
            begin(&mut catalog, op_id(1), 5, [target[0], target[1], target[1]]),
            Err(PlacementError::InvalidMovementTarget { shard_id: 5, .. })
        ));
        // Unknown voter.
        assert!(matches!(
            begin(&mut catalog, op_id(1), 5, [target[0], target[1], 9]),
            Err(PlacementError::InvalidMovementTarget { shard_id: 5, .. })
        ));
        // Stale compare-and-swap epoch.
        assert_eq!(
            catalog.apply(CatalogCommand::BeginMovement {
                expected_epoch: 99,
                operation_id: op_id(1),
                shard_id: 5,
                target_voters: target,
            }),
            Err(PlacementError::StaleCatalogView {
                expected: 99,
                actual: 1
            })
        );
        assert!(pending(&catalog, 5).is_none());
    }

    #[test]
    fn advance_phase_is_forward_only_and_idempotent() {
        let mut catalog = four_node_catalog();
        let (_, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        let advance = |catalog: &mut PlacementCatalog, phase: MovementPhase| {
            catalog.apply(CatalogCommand::AdvanceMovementPhase {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                phase,
            })
        };

        assert_eq!(
            advance(&mut catalog, MovementPhase::Learner),
            Ok(CatalogResponse::PhaseAdvanced {
                phase: MovementPhase::Learner
            })
        );
        // Same phase is idempotent.
        assert_eq!(
            advance(&mut catalog, MovementPhase::Learner),
            Ok(CatalogResponse::PhaseAdvanced {
                phase: MovementPhase::Learner
            })
        );
        // Skipping phases is rejected.
        assert_eq!(
            advance(&mut catalog, MovementPhase::Promote),
            Err(PlacementError::InvalidPhaseTransition {
                shard_id: 5,
                current: MovementPhase::Learner,
                requested: MovementPhase::Promote,
            })
        );
        // Moving backwards is rejected.
        assert_eq!(
            advance(&mut catalog, MovementPhase::Intent),
            Err(PlacementError::InvalidPhaseTransition {
                shard_id: 5,
                current: MovementPhase::Learner,
                requested: MovementPhase::Intent,
            })
        );
        // Publish has its own atomic command.
        assert!(matches!(
            advance(&mut catalog, MovementPhase::Publish),
            Err(PlacementError::InvalidPhaseTransition { .. })
        ));
        assert_eq!(pending(&catalog, 5).unwrap().phase, MovementPhase::Learner);

        // Unknown operation identity fails closed.
        assert_eq!(
            catalog.apply(CatalogCommand::AdvanceMovementPhase {
                expected_epoch: 1,
                operation_id: op_id(9),
                shard_id: 5,
                phase: MovementPhase::Learner,
            }),
            Err(PlacementError::UnknownMovement { shard_id: 5 })
        );
    }

    #[test]
    fn record_attempt_tracks_retry_state_without_moving_phase() {
        let mut catalog = four_node_catalog();
        let (_, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        let record = |catalog: &mut PlacementCatalog, succeeded: bool, note: Option<&str>| {
            catalog.apply(CatalogCommand::RecordMovementAttempt {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                succeeded,
                note: note.map(str::to_string),
            })
        };

        assert_eq!(
            record(&mut catalog, false, Some("learner add timed out")),
            Ok(CatalogResponse::AttemptRecorded { retries: 1 })
        );
        let movement = pending(&catalog, 5).unwrap();
        assert_eq!(movement.retries, 1);
        assert_eq!(
            movement.last_error.as_deref(),
            Some("learner add timed out")
        );
        assert_eq!(movement.phase, MovementPhase::Intent);

        assert_eq!(
            record(&mut catalog, true, None),
            Ok(CatalogResponse::AttemptRecorded { retries: 2 })
        );
        let movement = pending(&catalog, 5).unwrap();
        assert_eq!(movement.retries, 2);
        assert_eq!(movement.last_error, None);

        let oversize = "x".repeat(MAX_MOVEMENT_NOTE_BYTES + 1);
        assert_eq!(
            record(&mut catalog, false, Some(&oversize)),
            Err(PlacementError::InvalidMovementNote { shard_id: 5 })
        );
        // The rejected note did not consume a retry.
        assert_eq!(pending(&catalog, 5).unwrap().retries, 2);
    }

    fn advance_to(
        catalog: &mut PlacementCatalog,
        operation_id: [u8; 16],
        shard_id: u16,
        phases: &[MovementPhase],
    ) {
        for phase in phases {
            assert_eq!(
                catalog.apply(CatalogCommand::AdvanceMovementPhase {
                    expected_epoch: catalog.state().unwrap().placement_epoch,
                    operation_id,
                    shard_id,
                    phase: *phase,
                }),
                Ok(CatalogResponse::PhaseAdvanced { phase: *phase })
            );
        }
    }

    #[test]
    fn publish_replaces_stable_and_bumps_epoch_atomically() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();

        // Publish requires the observed data-group membership to equal the
        // target (REQ-M4-MOVE-002).
        assert_eq!(
            catalog.apply(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: stable,
            }),
            Err(PlacementError::PublishPreconditionFailed { shard_id: 5 })
        );
        assert!(pending(&catalog, 5).is_some());

        assert_eq!(
            catalog.apply(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: target,
            }),
            Ok(CatalogResponse::MovementPublished { placement_epoch: 2 })
        );
        let state = catalog.state().unwrap();
        assert_eq!(state.placement_epoch, 2);
        let placement = state.placements.get(&5).unwrap();
        assert_eq!(placement.voters, target);
        assert_eq!(placement.epoch, 2);
        assert!(placement.pending_movement.is_none());
        // Publishing twice is a no-op failure: the movement is gone.
        assert_eq!(
            catalog.apply(CatalogCommand::PublishMovement {
                expected_epoch: 2,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: target,
            }),
            Err(PlacementError::UnknownMovement { shard_id: 5 })
        );
        catalog.state().unwrap().validate().unwrap();
    }

    #[test]
    fn publish_recomputes_desired_leader_when_it_left() {
        let mut catalog = four_node_catalog();
        let leader = catalog
            .state()
            .unwrap()
            .placements
            .get(&1)
            .unwrap()
            .desired_leader;
        let (stable, target) = movement_target(&catalog, 1, leader);
        assert!(stable.contains(&leader));
        assert!(!target.contains(&leader));
        begin(&mut catalog, op_id(3), 1, target).unwrap();
        assert_eq!(
            catalog.apply(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: op_id(3),
                shard_id: 1,
                observed_voters: target,
            }),
            Ok(CatalogResponse::MovementPublished { placement_epoch: 2 })
        );
        let placement = catalog.state().unwrap().placements.get(&1).unwrap();
        // Deterministic fallback: lowest target voter; advisory only.
        assert_eq!(placement.desired_leader, target[0]);
        catalog.state().unwrap().validate().unwrap();
    }

    #[test]
    fn publish_keeps_desired_leader_when_it_stays() {
        let mut catalog = four_node_catalog();
        let leader = catalog
            .state()
            .unwrap()
            .placements
            .get(&5)
            .unwrap()
            .desired_leader;
        let stable = catalog.state().unwrap().placements.get(&5).unwrap().voters;
        // Remove a voter that is not the desired leader.
        let removed = stable.iter().copied().find(|node| *node != leader).unwrap();
        let (_, target) = movement_target(&catalog, 5, removed);
        assert!(target.contains(&leader));
        begin(&mut catalog, op_id(3), 5, target).unwrap();
        catalog
            .apply(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: op_id(3),
                shard_id: 5,
                observed_voters: target,
            })
            .unwrap();
        let placement = catalog.state().unwrap().placements.get(&5).unwrap();
        assert_eq!(placement.desired_leader, leader);
        catalog.state().unwrap().validate().unwrap();
    }

    #[test]
    fn cancel_before_membership_commit_leaves_stable_authoritative() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        advance_to(
            &mut catalog,
            op_id(7),
            5,
            &[MovementPhase::Learner, MovementPhase::CatchUp],
        );

        assert_eq!(
            catalog.apply(CatalogCommand::CancelMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: stable,
                reason: "target node drained".to_string(),
            }),
            Ok(CatalogResponse::MovementCancelled)
        );
        let state = catalog.state().unwrap();
        // Old stable placement remains authoritative; no epoch bump
        // (REQ-M4-MOVE-005).
        assert_eq!(state.placement_epoch, 1);
        let placement = state.placements.get(&5).unwrap();
        assert_eq!(placement.voters, stable);
        assert!(placement.pending_movement.is_none());
        state.validate().unwrap();
    }

    #[test]
    fn cancel_after_membership_commit_must_finish_forward() {
        let mut catalog = four_node_catalog();
        let (_, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        assert_eq!(
            catalog.apply(CatalogCommand::CancelMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: target,
                reason: "too late".to_string(),
            }),
            Err(PlacementError::MembershipAlreadyCommitted { shard_id: 5 })
        );
        assert!(pending(&catalog, 5).is_some());
    }

    #[test]
    fn cancel_with_ambiguous_membership_fails_closed() {
        let mut catalog = four_node_catalog();
        let (stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        // Neither source nor target: the reconciler must not guess.
        let mut ambiguous = stable;
        ambiguous[0] = 9;
        ambiguous.sort_unstable();
        assert_ne!(ambiguous, stable);
        assert_ne!(ambiguous, target);
        assert_eq!(
            catalog.apply(CatalogCommand::CancelMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: ambiguous,
                reason: "confused".to_string(),
            }),
            Err(PlacementError::AmbiguousMembership { shard_id: 5 })
        );
        assert!(pending(&catalog, 5).is_some());
    }

    #[test]
    fn movement_commands_fail_closed_on_uninitialized_catalog() {
        let mut catalog = PlacementCatalog::default();
        assert_eq!(
            begin(&mut catalog, op_id(7), 5, [1, 2, 4]),
            Err(PlacementError::NotInitialized)
        );
    }

    #[test]
    fn stale_epoch_is_rejected_for_every_movement_command() {
        let mut catalog = four_node_catalog();
        let (_, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        // Publish once so the epoch moves to 2.
        catalog
            .apply(CatalogCommand::PublishMovement {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                observed_voters: target,
            })
            .unwrap();
        let stale = Err(PlacementError::StaleCatalogView {
            expected: 1,
            actual: 2,
        });
        // A begin pinned to the old epoch fails the compare-and-swap.
        let (_, shard6_target) = movement_target(&catalog, 6, {
            let state = catalog.state().unwrap();
            state.placements.get(&6).unwrap().voters[2]
        });
        assert_eq!(begin(&mut catalog, op_id(8), 6, shard6_target), stale);
        assert!(pending(&catalog, 6).is_none());
        // A fresh begin at epoch 2 succeeds; stale advances still fail.
        assert!(catalog
            .apply(CatalogCommand::BeginMovement {
                expected_epoch: 2,
                operation_id: op_id(8),
                shard_id: 6,
                target_voters: shard6_target,
            })
            .is_ok());
        assert_eq!(
            catalog.apply(CatalogCommand::AdvanceMovementPhase {
                expected_epoch: 1,
                operation_id: op_id(8),
                shard_id: 6,
                phase: MovementPhase::Learner,
            }),
            stale
        );
        // Fresh epoch works.
        assert!(catalog
            .apply(CatalogCommand::AdvanceMovementPhase {
                expected_epoch: 2,
                operation_id: op_id(8),
                shard_id: 6,
                phase: MovementPhase::Learner,
            })
            .is_ok());
    }

    #[test]
    fn movement_snapshot_round_trip_preserves_pending_state() {
        let mut catalog = four_node_catalog();
        let (_, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(7), 5, target).unwrap();
        advance_to(&mut catalog, op_id(7), 5, &[MovementPhase::Learner]);
        catalog
            .apply(CatalogCommand::RecordMovementAttempt {
                expected_epoch: 1,
                operation_id: op_id(7),
                shard_id: 5,
                succeeded: false,
                note: Some("retry".to_string()),
            })
            .unwrap();
        let bytes = catalog.encode_snapshot().unwrap();
        let restored = PlacementCatalog::restore_snapshot(&bytes).unwrap();
        assert_eq!(restored, catalog);
        assert_eq!(pending(&restored, 5).unwrap().phase, MovementPhase::Learner);
        assert_eq!(pending(&restored, 5).unwrap().retries, 1);
    }

    fn set_leader(
        catalog: &mut PlacementCatalog,
        expected_epoch: PlacementEpoch,
        shard_id: u16,
        new_leader: RaftNodeId,
    ) -> Result<CatalogResponse, PlacementError> {
        catalog.apply(CatalogCommand::SetDesiredLeader {
            expected_epoch,
            shard_id,
            new_leader,
        })
    }

    #[test]
    fn set_desired_leader_updates_hint_and_bumps_epoch() {
        let mut catalog = four_node_catalog();
        let (current, voters) = {
            let state = catalog.state().unwrap();
            let placement = state.placements.get(&3).unwrap();
            (placement.desired_leader, placement.voters)
        };
        let replacement = voters
            .iter()
            .copied()
            .find(|node| *node != current)
            .unwrap();
        let response = set_leader(&mut catalog, 1, 3, replacement).unwrap();
        assert!(matches!(
            response,
            CatalogResponse::DesiredLeaderUpdated { placement_epoch: 2 }
        ));
        let state = catalog.state().unwrap();
        assert_eq!(state.placement_epoch, 2);
        let placement = state.placements.get(&3).unwrap();
        assert_eq!(placement.desired_leader, replacement);
        assert_eq!(placement.epoch, 2);
        // Stable voters are untouched; only the advisory hint changed.
        assert_eq!(placement.voters, voters);
        // Idempotent retry does not bump the epoch again.
        assert!(matches!(
            set_leader(&mut catalog, 2, 3, replacement).unwrap(),
            CatalogResponse::DesiredLeaderAlreadySet
        ));
        assert_eq!(catalog.state().unwrap().placement_epoch, 2);
    }

    #[test]
    fn set_desired_leader_rejects_non_voter_stale_epoch_and_active_movement() {
        let mut catalog = four_node_catalog();
        let (voters_of_3, non_voter) = {
            let state = catalog.state().unwrap();
            let placement = state.placements.get(&3).unwrap();
            let voters = placement.voters;
            let non_voter = state
                .eligible_nodes
                .keys()
                .copied()
                .find(|node| !voters.contains(node))
                .unwrap();
            (voters, non_voter)
        };
        assert!(matches!(
            set_leader(&mut catalog, 1, 3, non_voter),
            Err(PlacementError::InvalidMovementTarget { shard_id: 3, .. })
        ));
        assert!(matches!(
            set_leader(&mut catalog, 999, 3, voters_of_3[0]),
            Err(PlacementError::StaleCatalogView { .. })
        ));

        let (_stable, target) = movement_target(&catalog, 5, {
            let state = catalog.state().unwrap();
            state.placements.get(&5).unwrap().voters[2]
        });
        begin(&mut catalog, op_id(9), 5, target).unwrap();
        let other_voter = target
            .iter()
            .copied()
            .find(|node| *node != target[0])
            .unwrap();
        assert!(matches!(
            set_leader(&mut catalog, 1, 5, other_voter),
            Err(PlacementError::ConflictingMovement { shard_id: 5 })
        ));
    }
}
