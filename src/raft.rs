use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, StorageError, StorageIOError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub type RaftNodeId = u64;
pub type RaftNode = BasicNode;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RaftMutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RaftCommand {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Batch { mutations: Vec<RaftMutation> },
}

impl fmt::Display for RaftCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Set { key, value } => write!(f, "set({},{})", key.len(), value.len()),
            Self::Delete { key } => write!(f, "delete({})", key.len()),
            Self::Batch { mutations } => write!(f, "batch({})", mutations.len()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RaftResponse {
    Applied { mutations: u32 },
    EmptyBatch,
    Noop,
}

impl fmt::Display for RaftResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Applied { mutations } => write!(f, "applied({mutations})"),
            Self::EmptyBatch => write!(f, "empty-batch"),
            Self::Noop => write!(f, "noop"),
        }
    }
}

openraft::declare_raft_types!(
    pub HomeKvRaftConfig:
        D = RaftCommand,
        R = RaftResponse,
        NodeId = RaftNodeId,
        Node = RaftNode,
);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateMachineView {
    pub last_applied: Option<LogId<RaftNodeId>>,
    pub membership: StoredMembership<RaftNodeId, RaftNode>,
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Default)]
struct StateMachineData {
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub struct HomeKvStateMachine {
    inner: Arc<RwLock<StateMachineData>>,
}

impl HomeKvStateMachine {
    pub async fn view(&self) -> StateMachineView {
        let state = self.inner.read().await;
        StateMachineView {
            last_applied: state.last_applied,
            membership: state.membership.clone(),
            data: state.data.clone(),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.inner.read().await.data.get(key).cloned()
    }

    fn storage_error(message: &'static str) -> StorageError<RaftNodeId> {
        let err = io::Error::new(io::ErrorKind::Unsupported, message);
        StorageIOError::read_state_machine(&err).into()
    }

    fn apply_command(state: &mut StateMachineData, command: RaftCommand) -> RaftResponse {
        match command {
            RaftCommand::Set { key, value } => {
                state.data.insert(key, value);
                RaftResponse::Applied { mutations: 1 }
            }
            RaftCommand::Delete { key } => {
                state.data.remove(&key);
                RaftResponse::Applied { mutations: 1 }
            }
            RaftCommand::Batch { mutations } => {
                if mutations.is_empty() {
                    return RaftResponse::EmptyBatch;
                }
                for mutation in &mutations {
                    match mutation {
                        RaftMutation::Set { key, value } => {
                            state.data.insert(key.clone(), value.clone());
                        }
                        RaftMutation::Delete { key } => {
                            state.data.remove(key);
                        }
                    }
                }
                RaftResponse::Applied {
                    mutations: mutations.len() as u32,
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeferredSnapshotBuilder;

impl RaftSnapshotBuilder<HomeKvRaftConfig> for DeferredSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<HomeKvRaftConfig>, StorageError<RaftNodeId>> {
        Err(HomeKvStateMachine::storage_error(
            "snapshot build is deferred to M3-T5",
        ))
    }
}

impl RaftStateMachine<HomeKvRaftConfig> for HomeKvStateMachine {
    type SnapshotBuilder = DeferredSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, RaftNode>,
        ),
        StorageError<RaftNodeId>,
    > {
        let state = self.inner.read().await;
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<RaftResponse>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<HomeKvRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let mut state = self.inner.write().await;

        for entry in entries {
            if let Some(last) = state.last_applied {
                if entry.log_id == last {
                    responses.push(RaftResponse::Noop);
                    continue;
                }
                if entry.log_id.index <= last.index {
                    return Err(Self::storage_error("state-machine apply order regressed"));
                }
            }

            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse::Noop,
                EntryPayload::Normal(command) => Self::apply_command(&mut state, command),
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(entry.log_id), membership);
                    RaftResponse::Noop
                }
            };
            state.last_applied = Some(entry.log_id);
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        DeferredSnapshotBuilder
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>,
        StorageError<RaftNodeId>,
    > {
        Err(Self::storage_error("snapshot receive is deferred to M3-T5"))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<RaftNodeId, RaftNode>,
        _snapshot: Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        Err(Self::storage_error("snapshot install is deferred to M3-T5"))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<HomeKvRaftConfig>>, StorageError<RaftNodeId>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use openraft::{CommittedLeaderId, Membership};

    use super::*;

    fn log_id(index: u64) -> LogId<RaftNodeId> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn normal(index: u64, command: RaftCommand) -> Entry<HomeKvRaftConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(command),
        }
    }

    #[tokio::test]
    async fn applies_commands_in_committed_order() {
        let mut sm = HomeKvStateMachine::default();
        let responses = sm
            .apply(vec![
                normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() }),
                normal(2, RaftCommand::Batch { mutations: vec![
                    RaftMutation::Set { key: b"b".to_vec(), value: b"two".to_vec() },
                    RaftMutation::Delete { key: b"a".to_vec() },
                ]}),
                normal(3, RaftCommand::Delete { key: b"missing".to_vec() }),
            ])
            .await
            .unwrap();

        assert_eq!(responses, vec![
            RaftResponse::Applied { mutations: 1 },
            RaftResponse::Applied { mutations: 2 },
            RaftResponse::Applied { mutations: 1 },
        ]);
        assert_eq!(sm.get(b"a").await, None);
        assert_eq!(sm.get(b"b").await, Some(b"two".to_vec()));
        assert_eq!(sm.view().await.last_applied, Some(log_id(3)));
    }

    #[tokio::test]
    async fn membership_entry_updates_metadata_only() {
        let mut sm = HomeKvStateMachine::default();
        sm.apply(vec![normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"v".to_vec() })])
            .await
            .unwrap();
        let before = sm.view().await.data;

        let voters = BTreeSet::from([1, 2, 3]);
        let membership = Membership::new(vec![voters], BTreeMap::<RaftNodeId, RaftNode>::new());
        sm.apply(vec![Entry {
            log_id: log_id(2),
            payload: EntryPayload::Membership(membership),
        }])
        .await
        .unwrap();

        let view = sm.view().await;
        assert_eq!(view.data, before);
        assert_eq!(view.last_applied, Some(log_id(2)));
        assert_eq!(view.membership.log_id(), Some(log_id(2)));
    }

    #[tokio::test]
    async fn replay_is_deterministic_and_duplicate_identity_is_not_reapplied() {
        let history = || vec![
            normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"v1".to_vec() }),
            normal(2, RaftCommand::Set { key: b"k".to_vec(), value: b"v2".to_vec() }),
        ];
        let mut first = HomeKvStateMachine::default();
        let mut recovered = HomeKvStateMachine::default();
        first.apply(history()).await.unwrap();
        recovered.apply(history()).await.unwrap();
        assert_eq!(first.view().await, recovered.view().await);

        let duplicate = normal(2, RaftCommand::Delete { key: b"k".to_vec() });
        assert_eq!(first.apply(vec![duplicate]).await.unwrap(), vec![RaftResponse::Noop]);
        assert_eq!(first.get(b"k").await, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn lower_log_index_fails_closed() {
        let mut sm = HomeKvStateMachine::default();
        sm.apply(vec![normal(2, RaftCommand::Set { key: b"k".to_vec(), value: b"v".to_vec() })])
            .await
            .unwrap();
        let before = sm.view().await;
        assert!(sm.apply(vec![normal(1, RaftCommand::Delete { key: b"k".to_vec() })]).await.is_err());
        assert_eq!(sm.view().await, before);
    }
}
