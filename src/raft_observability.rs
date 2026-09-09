use std::error::Error;
use std::fmt;

use openraft::raft::Raft;
use openraft::storage::RaftLogStorage;
use openraft::ServerState;
use serde_derive::{Deserialize, Serialize};

use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine};
use crate::raft_storage::HomeKvRaftLogStore;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplicaRole {
    Learner,
    Follower,
    Candidate,
    Leader,
    Stopped,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplicaHealth {
    Running,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogPosition {
    pub term: u64,
    pub leader_id: u64,
    pub index: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ElectionIdentity {
    pub term: u64,
    pub candidate_id: u64,
    pub committed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipStatus {
    pub log: Option<LogPosition>,
    pub voters: Vec<u64>,
    pub members: Vec<ReplicaMember>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaMember {
    pub node_id: u64,
    pub endpoint: String,
    pub voter: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaStatus {
    pub node_id: u64,
    pub role: ReplicaRole,
    pub health: ReplicaHealth,
    pub leader_id: Option<u64>,
    pub current_term: u64,
    pub vote: ElectionIdentity,
    pub last_log_index: Option<u64>,
    pub committed: Option<LogPosition>,
    pub applied: Option<LogPosition>,
    pub membership: MembershipStatus,
    pub snapshot: Option<LogPosition>,
    pub purged: Option<LogPosition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserveError {
    DurableStateUnavailable,
}

impl fmt::Display for ObserveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurableStateUnavailable => f.write_str("durable replica state unavailable"),
        }
    }
}

impl Error for ObserveError {}

#[derive(Clone)]
pub struct HomeKvReplicaObserver {
    raft: Raft<HomeKvRaftConfig>,
    log_store: HomeKvRaftLogStore,
    state_machine: HomeKvStateMachine,
}

impl HomeKvReplicaObserver {
    pub fn new(
        raft: Raft<HomeKvRaftConfig>,
        log_store: HomeKvRaftLogStore,
        state_machine: HomeKvStateMachine,
    ) -> Self {
        Self {
            raft,
            log_store,
            state_machine,
        }
    }

    pub async fn snapshot(&self) -> Result<ReplicaStatus, ObserveError> {
        let metrics = self.raft.metrics().borrow().clone();
        let state = self.state_machine.view().await;
        let mut log_store = self.log_store.clone();
        let committed = log_store
            .read_committed()
            .await
            .map_err(|_| ObserveError::DurableStateUnavailable)?;

        let role = match metrics.state {
            ServerState::Learner => ReplicaRole::Learner,
            ServerState::Follower => ReplicaRole::Follower,
            ServerState::Candidate => ReplicaRole::Candidate,
            ServerState::Leader => ReplicaRole::Leader,
            ServerState::Shutdown => ReplicaRole::Stopped,
        };
        let health = match (&metrics.running_state, metrics.state) {
            (Err(_), _) => ReplicaHealth::Failed,
            (_, ServerState::Shutdown) => ReplicaHealth::Stopped,
            _ => ReplicaHealth::Running,
        };

        let mut voters: Vec<_> = state.membership.voter_ids().collect();
        voters.sort_unstable();
        let mut members: Vec<_> = state
            .membership
            .nodes()
            .map(|(node_id, node)| ReplicaMember {
                node_id: *node_id,
                endpoint: node.addr.clone(),
                voter: voters.binary_search(node_id).is_ok(),
            })
            .collect();
        members.sort_by_key(|member| member.node_id);

        Ok(ReplicaStatus {
            node_id: metrics.id,
            role,
            health,
            leader_id: metrics.current_leader,
            current_term: metrics.current_term,
            vote: ElectionIdentity {
                term: metrics.vote.leader_id.term,
                candidate_id: metrics.vote.leader_id.node_id,
                committed: metrics.vote.committed,
            },
            last_log_index: metrics.last_log_index,
            committed: committed.map(LogPosition::from),
            applied: state.last_applied.map(LogPosition::from),
            membership: MembershipStatus {
                log: (*state.membership.log_id()).map(LogPosition::from),
                voters,
                members,
            },
            snapshot: metrics.snapshot.map(LogPosition::from),
            purged: metrics.purged.map(LogPosition::from),
        })
    }
}

impl From<openraft::LogId<u64>> for LogPosition {
    fn from(value: openraft::LogId<u64>) -> Self {
        Self {
            term: value.leader_id.term,
            leader_id: value.leader_id.node_id,
            index: value.index,
        }
    }
}
