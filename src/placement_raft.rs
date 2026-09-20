use std::fmt;

use openraft::raft::Raft;

use crate::placement::{
    CatalogCommand, CatalogResponse, CatalogState, EligibleNode, PlacementCatalog, PlacementError,
    CATALOG_STATE_KEY,
};
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftResponse};

#[derive(Clone)]
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
                write!(
                    f,
                    "placement catalog is initialized with different authority"
                )
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
        Self {
            raft,
            state_machine,
        }
    }

    pub async fn bootstrap(
        &self,
        cluster_id: [u8; 16],
        eligible_nodes: Vec<EligibleNode>,
    ) -> Result<CatalogResponse, CatalogGroupError> {
        // Bootstrap goes through the same atomic catalog-mutation path as
        // every other command: the state machine applies `Bootstrap` against
        // the committed image, so concurrent bootstraps are decided in log
        // order — identical configuration is idempotent, a different one is
        // a fail-closed conflict (REQ-M4-MOVE-004).
        let write = self
            .raft
            .client_write(RaftCommand::CatalogMutation {
                command: CatalogCommand::Bootstrap {
                    cluster_id,
                    eligible_nodes,
                },
            })
            .await
            .map_err(|_| CatalogGroupError::ConsensusUnavailable)?;
        match write.data {
            RaftResponse::CatalogApplied { response } => Ok(response),
            RaftResponse::CatalogRejected {
                error: PlacementError::AlreadyInitializedWithDifferentConfiguration,
            } => Err(CatalogGroupError::ConflictingBootstrap),
            RaftResponse::CatalogRejected { error } => {
                Err(CatalogGroupError::InvalidPlacement(error))
            }
            RaftResponse::CatalogNotInitialized | RaftResponse::CatalogCorrupt => {
                Err(CatalogGroupError::MissingCommittedState)
            }
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

    /// Submit a catalog command through Raft consensus.
    ///
    /// The command is replicated as `RaftCommand::CatalogMutation` and applied
    /// inside the state machine against the committed catalog image in log
    /// order. Expected-epoch validation therefore happens atomically: a
    /// concurrent writer that advanced the epoch makes this submit fail
    /// closed with `StaleCatalogView` instead of silently clobbering intent
    /// (REQ-M4-MOVE-004). Callers re-read and retry on that error. No
    /// separate linearizable read is needed — the write itself is ordered by
    /// the Raft log.
    pub async fn submit(
        &self,
        command: CatalogCommand,
    ) -> Result<CatalogResponse, CatalogGroupError> {
        let write = self
            .raft
            .client_write(RaftCommand::CatalogMutation { command })
            .await
            .map_err(|_| CatalogGroupError::ConsensusUnavailable)?;
        match write.data {
            RaftResponse::CatalogApplied { response } => Ok(response),
            RaftResponse::CatalogRejected { error } => {
                Err(CatalogGroupError::InvalidPlacement(error))
            }
            RaftResponse::CatalogNotInitialized | RaftResponse::CatalogCorrupt => {
                Err(CatalogGroupError::MissingCommittedState)
            }
            _ => Err(CatalogGroupError::ConsensusUnavailable),
        }
    }

    pub async fn committed_state(&self) -> Result<CatalogState, CatalogGroupError> {
        self.committed_catalog()
            .await?
            .state()
            .cloned()
            .ok_or(CatalogGroupError::MissingCommittedState)
    }

    async fn committed_catalog(&self) -> Result<PlacementCatalog, CatalogGroupError> {
        let bytes = self
            .state_machine
            .get(CATALOG_STATE_KEY)
            .await
            .ok_or(CatalogGroupError::MissingCommittedState)?;
        PlacementCatalog::restore_snapshot(&bytes).map_err(CatalogGroupError::from)
    }
}
