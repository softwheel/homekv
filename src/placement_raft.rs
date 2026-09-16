use std::fmt;

use openraft::raft::Raft;

use crate::placement::{
    CatalogCommand, CatalogResponse, CatalogState, EligibleNode, PlacementCatalog, PlacementError,
};
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftResponse};

const CATALOG_STATE_KEY: &[u8] = b"\0homekv/system/placement-catalog/v1";

#[derive(Clone, Debug)]
pub struct PlacementCatalogGroup {
    raft: Raft<HomeKvRaftConfig>,
    state_machine: HomeKvStateMachine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogGroupError {
    ConsensusUnavailable,
    MissingCommittedState,
    ConflictingBootstrap,
    InvalidPlacement(PlacementError),
}

impl fmt::Display for CatalogGroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsensusUnavailable => write!(f, "placement catalog consensus is unavailable"),
            Self::MissingCommittedState => write!(f, "placement catalog has no committed state"),
            Self::ConflictingBootstrap => {
                write!(f, "placement catalog is initialized with different authority")
            }
            Self::InvalidPlacement(error) => write!(f, "invalid placement catalog: {error}"),
        }
    }
}

impl std::error::Error for CatalogGroupError {}

impl From<PlacementError> for CatalogGroupError {
    fn from(error: PlacementError) -> Self {
        Self::InvalidPlacement(error)
    }
}

impl PlacementCatalogGroup {
    pub fn new(raft: Raft<HomeKvRaftConfig>, state_machine: HomeKvStateMachine) -> Self {
        Self { raft, state_machine }
    }

    pub async fn bootstrap(
        &self,
        cluster_id: [u8; 16],
        eligible_nodes: Vec<EligibleNode>,
    ) -> Result<CatalogResponse, CatalogGroupError> {
        let mut candidate = PlacementCatalog::default();
        candidate.apply(CatalogCommand::Bootstrap {
            cluster_id,
            eligible_nodes,
        })?;
        let bytes = candidate.encode_snapshot()?;
        let response = self
            .raft
            .client_write(RaftCommand::Initialize {
                key: CATALOG_STATE_KEY.to_vec(),
                value: bytes,
            })
            .await
            .map_err(|_| CatalogGroupError::ConsensusUnavailable)?;

        let epoch = candidate
            .state()
            .expect("validated bootstrap candidate must contain state")
            .placement_epoch;
        match response.data {
            RaftResponse::Applied { mutations: 1 } => {
                Ok(CatalogResponse::Initialized { placement_epoch: epoch })
            }
            RaftResponse::AlreadyPresent => {
                Ok(CatalogResponse::AlreadyInitialized { placement_epoch: epoch })
            }
            RaftResponse::Conflict => Err(CatalogGroupError::ConflictingBootstrap),
            _ => Err(CatalogGroupError::ConsensusUnavailable),
        }
    }

    pub async fn read(&self) -> Result<CatalogState, CatalogGroupError> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|_| CatalogGroupError::ConsensusUnavailable)?;
        self.committed_state().await
    }

    pub async fn committed_state(&self) -> Result<CatalogState, CatalogGroupError> {
        let bytes = self
            .state_machine
            .get(CATALOG_STATE_KEY)
            .await
            .ok_or(CatalogGroupError::MissingCommittedState)?;
        let catalog = PlacementCatalog::restore_snapshot(&bytes)?;
        catalog
            .state()
            .cloned()
            .ok_or(CatalogGroupError::MissingCommittedState)
    }
}
