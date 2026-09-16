use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde_derive::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::raft::RaftNodeId;
use crate::storage::LOGICAL_SHARD_COUNT;

pub type GroupId = u64;
pub type PlacementEpoch = u64;
pub type ClusterId = [u8; 16];

pub const PLACEMENT_CATALOG_GROUP_ID: GroupId = 0;
pub const PLACEMENT_FORMAT_VERSION: u16 = 1;
pub const MAX_ELIGIBLE_NODES: usize = 1024;
pub const MAX_ENDPOINT_BYTES: usize = 1024;
pub const MAX_FAILURE_DOMAIN_BYTES: usize = 128;

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CatalogResponse {
    Initialized { placement_epoch: PlacementEpoch },
    AlreadyInitialized { placement_epoch: PlacementEpoch },
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
                write!(f, "logical shard id {shard} is outside 0..{LOGICAL_SHARD_COUNT}")
            }
            Self::EmptyClusterId => write!(f, "cluster identity must not be all zeroes"),
            Self::TooFewEligibleNodes(count) => {
                write!(f, "RF=3 placement requires at least three eligible nodes, got {count}")
            }
            Self::TooManyEligibleNodes(count) => {
                write!(f, "eligible node count {count} exceeds {MAX_ELIGIBLE_NODES}")
            }
            Self::DuplicateNode(node) => write!(f, "eligible node {node} is duplicated"),
            Self::InvalidEndpoint(node) => write!(f, "eligible node {node} has an invalid endpoint"),
            Self::InvalidFailureDomain(node) => {
                write!(f, "eligible node {node} has an invalid failure domain")
            }
            Self::AlreadyInitializedWithDifferentConfiguration => {
                write!(f, "placement catalog is already initialized with different authority")
            }
            Self::NotInitialized => write!(f, "placement catalog is not initialized"),
            Self::EpochExhausted => write!(f, "placement epoch is exhausted"),
            Self::InvalidSnapshotEnvelope => write!(f, "invalid placement snapshot envelope"),
            Self::UnsupportedSnapshotVersion(version) => {
                write!(f, "unsupported placement snapshot version {version}")
            }
            Self::SnapshotLengthMismatch => write!(f, "placement snapshot length mismatch"),
            Self::SnapshotChecksumMismatch => write!(f, "placement snapshot checksum mismatch"),
            Self::SnapshotDecode(message) => write!(f, "placement snapshot decode failed: {message}"),
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
            return Err(PlacementError::TooFewEligibleNodes(
                eligible_nodes.len(),
            ));
        }
        if eligible_nodes.len() > MAX_ELIGIBLE_NODES {
            return Err(PlacementError::TooManyEligibleNodes(
                eligible_nodes.len(),
            ));
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

fn validate_voters(
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
    validate_voters(
        shard_id,
        &pending.source_voters,
        nodes,
        distinct_domains,
    )?;
    validate_voters(
        shard_id,
        &pending.target_voters,
        nodes,
        distinct_domains,
    )
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
        assert_eq!(state.eligible_nodes.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

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
        let first = bootstrap(vec![
            node(1, "a"),
            node(2, "b"),
            node(3, "c"),
            node(4, "d"),
        ]);
        let second = bootstrap(vec![
            node(4, "d"),
            node(2, "b"),
            node(1, "a"),
            node(3, "c"),
        ]);
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
        let catalog = bootstrap(vec![
            node(1, "a"),
            node(2, "b"),
            node(3, "c"),
            node(4, "d"),
        ]);
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
        assert_eq!(data_group_id(LOGICAL_SHARD_COUNT), Err(PlacementError::InvalidShard(1024)));
    }
}
