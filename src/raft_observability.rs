use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LeadershipMetricsSnapshot {
    /// Monotonic sum of newly observed Raft terms.
    pub term_advances: u64,
    /// Times a non-empty leader identity different from the prior identity was observed.
    pub leader_selections: u64,
    /// Changes in the observed leader identity, including loss or discovery.
    pub leader_identity_changes: u64,
    pub local_leadership_acquisitions: u64,
    pub local_leadership_losses: u64,
}

#[derive(Debug, Default)]
struct LeadershipMetrics {
    term_advances: AtomicU64,
    leader_selections: AtomicU64,
    leader_identity_changes: AtomicU64,
    local_leadership_acquisitions: AtomicU64,
    local_leadership_losses: AtomicU64,
}

impl LeadershipMetrics {
    fn snapshot(&self) -> LeadershipMetricsSnapshot {
        LeadershipMetricsSnapshot {
            term_advances: self.term_advances.load(Ordering::Relaxed),
            leader_selections: self.leader_selections.load(Ordering::Relaxed),
            leader_identity_changes: self.leader_identity_changes.load(Ordering::Relaxed),
            local_leadership_acquisitions: self
                .local_leadership_acquisitions
                .load(Ordering::Relaxed),
            local_leadership_losses: self.local_leadership_losses.load(Ordering::Relaxed),
        }
    }
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
    pub leadership: LeadershipMetricsSnapshot,
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
    leadership: Arc<LeadershipMetrics>,
}

impl HomeKvReplicaObserver {
    pub fn new(
        raft: Raft<HomeKvRaftConfig>,
        log_store: HomeKvRaftLogStore,
        state_machine: HomeKvStateMachine,
    ) -> Self {
        let leadership = Arc::new(LeadershipMetrics::default());
        let mut receiver = raft.metrics();
        let initial = receiver.borrow().clone();
        let mut prior_term = initial.current_term;
        let mut prior_leader = initial.current_leader;
        let mut prior_local_leader =
            initial.state == ServerState::Leader && initial.current_leader == Some(initial.id);
        drop(initial);

        let tracked = leadership.clone();
        tokio::spawn(async move {
            while receiver.changed().await.is_ok() {
                let (current_term, current_leader, local_leader) = {
                    let metrics = receiver.borrow();
                    (
                        metrics.current_term,
                        metrics.current_leader,
                        metrics.state == ServerState::Leader
                            && metrics.current_leader == Some(metrics.id),
                    )
                };
                if current_term > prior_term {
                    tracked
                        .term_advances
                        .fetch_add(current_term - prior_term, Ordering::Relaxed);
                }
                if current_leader != prior_leader {
                    tracked
                        .leader_identity_changes
                        .fetch_add(1, Ordering::Relaxed);
                    if current_leader.is_some() {
                        tracked.leader_selections.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if local_leader != prior_local_leader {
                    if local_leader {
                        tracked
                            .local_leadership_acquisitions
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        tracked
                            .local_leadership_losses
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                prior_term = current_term;
                prior_leader = current_leader;
                prior_local_leader = local_leader;
            }
        });

        Self {
            raft,
            log_store,
            state_machine,
            leadership,
        }
    }

    pub fn leadership_metrics(&self) -> LeadershipMetricsSnapshot {
        self.leadership.snapshot()
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
            leadership: self.leadership.snapshot(),
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
