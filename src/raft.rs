use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Cursor};
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{BasicNode, Entry, EntryPayload, LogId, OptionalSend, StorageError, StorageIOError, StoredMembership};
use serde_derive::{Deserialize, Serialize};
use tokio::sync::RwLock;
use xxhash_rust::xxh3::xxh3_64;

pub type RaftNodeId = u64;
pub type RaftNode = BasicNode;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum RaftMutation { Set { key: Vec<u8>, value: Vec<u8> }, Delete { key: Vec<u8> } }

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
pub enum RaftResponse { Applied { mutations: u32 }, EmptyBatch, Noop }

impl fmt::Display for RaftResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Applied { mutations } => write!(f, "applied({mutations})"),
            Self::EmptyBatch => write!(f, "empty-batch"),
            Self::Noop => write!(f, "noop"),
        }
    }
}

openraft::declare_raft_types!(pub HomeKvRaftConfig: D = RaftCommand, R = RaftResponse, NodeId = RaftNodeId, Node = RaftNode,);

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
    current_snapshot: Option<(SnapshotMeta<RaftNodeId, RaftNode>, Vec<u8>)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotImage {
    format_version: u16,
    shard_id: u64,
    last_applied: Option<LogId<RaftNodeId>>,
    membership: StoredMembership<RaftNodeId, RaftNode>,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

const SNAPSHOT_MAGIC: &[u8; 8] = b"HKVSNAP1";
const SNAPSHOT_VERSION: u16 = 1;
const M3_SHARD_ID: u64 = 0;
const SNAPSHOT_HEADER_LEN: usize = 8 + 8 + 8;

#[derive(Clone, Debug, Default)]
pub struct HomeKvStateMachine { inner: Arc<RwLock<StateMachineData>> }

impl HomeKvStateMachine {
    pub async fn view(&self) -> StateMachineView {
        let state = self.inner.read().await;
        StateMachineView { last_applied: state.last_applied, membership: state.membership.clone(), data: state.data.clone() }
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> { self.inner.read().await.data.get(key).cloned() }

    fn storage_error(message: &'static str) -> StorageError<RaftNodeId> {
        let err = io::Error::new(io::ErrorKind::InvalidData, message);
        StorageIOError::read_state_machine(&err).into()
    }

    fn encode_snapshot(image: &SnapshotImage) -> Result<Vec<u8>, StorageError<RaftNodeId>> {
        let payload = bincode::serialize(image).map_err(|_| Self::storage_error("snapshot encode failed"))?;
        let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_LEN + payload.len());
        bytes.extend_from_slice(SNAPSHOT_MAGIC);
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotImage, StorageError<RaftNodeId>> {
        if bytes.len() < SNAPSHOT_HEADER_LEN || &bytes[..8] != SNAPSHOT_MAGIC { return Err(Self::storage_error("invalid snapshot envelope")); }
        let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let checksum = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        if bytes.len() != SNAPSHOT_HEADER_LEN + len { return Err(Self::storage_error("snapshot length mismatch")); }
        let payload = &bytes[SNAPSHOT_HEADER_LEN..];
        if xxh3_64(payload) != checksum { return Err(Self::storage_error("snapshot checksum mismatch")); }
        let image: SnapshotImage = bincode::deserialize(payload).map_err(|_| Self::storage_error("snapshot decode failed"))?;
        if image.format_version != SNAPSHOT_VERSION || image.shard_id != M3_SHARD_ID { return Err(Self::storage_error("snapshot version or shard mismatch")); }
        Ok(image)
    }

    fn apply_command(state: &mut StateMachineData, command: RaftCommand) -> RaftResponse {
        match command {
            RaftCommand::Set { key, value } => { state.data.insert(key, value); RaftResponse::Applied { mutations: 1 } }
            RaftCommand::Delete { key } => { state.data.remove(&key); RaftResponse::Applied { mutations: 1 } }
            RaftCommand::Batch { mutations } => {
                if mutations.is_empty() { return RaftResponse::EmptyBatch; }
                for mutation in &mutations {
                    match mutation {
                        RaftMutation::Set { key, value } => { state.data.insert(key.clone(), value.clone()); }
                        RaftMutation::Delete { key } => { state.data.remove(key); }
                    }
                }
                RaftResponse::Applied { mutations: mutations.len() as u32 }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct HomeKvSnapshotBuilder { state_machine: HomeKvStateMachine }

impl RaftSnapshotBuilder<HomeKvRaftConfig> for HomeKvSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<HomeKvRaftConfig>, StorageError<RaftNodeId>> {
        let (image, meta) = {
            let state = self.state_machine.inner.read().await;
            let image = SnapshotImage { format_version: SNAPSHOT_VERSION, shard_id: M3_SHARD_ID, last_applied: state.last_applied, membership: state.membership.clone(), data: state.data.clone() };
            let snapshot_id = match state.last_applied { Some(id) => format!("m3-{}-{}", id.leader_id.term, id.index), None => "m3-empty".to_string() };
            let meta = SnapshotMeta { last_log_id: state.last_applied, last_membership: state.membership.clone(), snapshot_id };
            (image, meta)
        };
        let bytes = HomeKvStateMachine::encode_snapshot(&image)?;
        self.state_machine.inner.write().await.current_snapshot = Some((meta.clone(), bytes.clone()));
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) })
    }
}

impl RaftStateMachine<HomeKvRaftConfig> for HomeKvStateMachine {
    type SnapshotBuilder = HomeKvSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId<RaftNodeId>>, StoredMembership<RaftNodeId, RaftNode>), StorageError<RaftNodeId>> {
        let state = self.inner.read().await;
        Ok((state.last_applied, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<RaftNodeId>>
    where I: IntoIterator<Item = Entry<HomeKvRaftConfig>> + OptionalSend, I::IntoIter: OptionalSend {
        let mut responses = Vec::new();
        let mut state = self.inner.write().await;
        for entry in entries {
            if let Some(last) = state.last_applied {
                if entry.log_id == last { responses.push(RaftResponse::Noop); continue; }
                if entry.log_id.index <= last.index { return Err(Self::storage_error("state-machine apply order regressed")); }
            }
            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse::Noop,
                EntryPayload::Normal(command) => Self::apply_command(&mut state, command),
                EntryPayload::Membership(membership) => { state.membership = StoredMembership::new(Some(entry.log_id), membership); RaftResponse::Noop }
            };
            state.last_applied = Some(entry.log_id);
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { HomeKvSnapshotBuilder { state_machine: self.clone() } }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<RaftNodeId, RaftNode>, snapshot: Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>) -> Result<(), StorageError<RaftNodeId>> {
        let bytes = snapshot.into_inner();
        let image = Self::decode_snapshot(&bytes)?;
        if image.last_applied != meta.last_log_id || image.membership != meta.last_membership { return Err(Self::storage_error("snapshot metadata mismatch")); }
        let mut state = self.inner.write().await;
        state.last_applied = image.last_applied;
        state.membership = image.membership;
        state.data = image.data;
        state.current_snapshot = Some((meta.clone(), bytes));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<HomeKvRaftConfig>>, StorageError<RaftNodeId>> {
        let state = self.inner.read().await;
        Ok(state.current_snapshot.as_ref().map(|(meta, bytes)| Snapshot { meta: meta.clone(), snapshot: Box::new(Cursor::new(bytes.clone())) }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Read;
    use openraft::{CommittedLeaderId, Membership};
    use super::*;

    fn log_id(index: u64) -> LogId<RaftNodeId> { LogId::new(CommittedLeaderId::new(1, 1), index) }
    fn normal(index: u64, command: RaftCommand) -> Entry<HomeKvRaftConfig> { Entry { log_id: log_id(index), payload: EntryPayload::Normal(command) } }

    #[tokio::test]
    async fn applies_commands_in_committed_order() {
        let mut sm = HomeKvStateMachine::default();
        let responses = sm.apply(vec![normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() }), normal(2, RaftCommand::Batch { mutations: vec![RaftMutation::Set { key: b"b".to_vec(), value: b"two".to_vec() }, RaftMutation::Delete { key: b"a".to_vec() }]}), normal(3, RaftCommand::Delete { key: b"missing".to_vec() })]).await.unwrap();
        assert_eq!(responses, vec![RaftResponse::Applied { mutations: 1 }, RaftResponse::Applied { mutations: 2 }, RaftResponse::Applied { mutations: 1 }]);
        assert_eq!(sm.get(b"a").await, None); assert_eq!(sm.get(b"b").await, Some(b"two".to_vec())); assert_eq!(sm.view().await.last_applied, Some(log_id(3)));
    }

    #[tokio::test]
    async fn membership_entry_updates_metadata_only() {
        let mut sm = HomeKvStateMachine::default(); sm.apply(vec![normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"v".to_vec() })]).await.unwrap(); let before = sm.view().await.data;
        let voters = BTreeSet::from([1, 2, 3]); let membership = Membership::new(vec![voters], BTreeMap::<RaftNodeId, RaftNode>::new());
        sm.apply(vec![Entry { log_id: log_id(2), payload: EntryPayload::Membership(membership) }]).await.unwrap();
        let view = sm.view().await; assert_eq!(view.data, before); assert_eq!(view.last_applied, Some(log_id(2))); assert_eq!(view.membership.log_id(), &Some(log_id(2)));
    }

    #[tokio::test]
    async fn replay_is_deterministic_and_duplicate_identity_is_not_reapplied() {
        let history = || vec![normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"v1".to_vec() }), normal(2, RaftCommand::Set { key: b"k".to_vec(), value: b"v2".to_vec() })];
        let mut first = HomeKvStateMachine::default(); let mut recovered = HomeKvStateMachine::default(); first.apply(history()).await.unwrap(); recovered.apply(history()).await.unwrap(); assert_eq!(first.view().await, recovered.view().await);
        let duplicate = normal(2, RaftCommand::Delete { key: b"k".to_vec() }); assert_eq!(first.apply(vec![duplicate]).await.unwrap(), vec![RaftResponse::Noop]); assert_eq!(first.get(b"k").await, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn lower_log_index_fails_closed() {
        let mut sm = HomeKvStateMachine::default(); sm.apply(vec![normal(2, RaftCommand::Set { key: b"k".to_vec(), value: b"v".to_vec() })]).await.unwrap(); let before = sm.view().await;
        assert!(sm.apply(vec![normal(1, RaftCommand::Delete { key: b"k".to_vec() })]).await.is_err()); assert_eq!(sm.view().await, before);
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_state_and_metadata() {
        let mut source = HomeKvStateMachine::default();
        source.apply(vec![normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() }), normal(2, RaftCommand::Set { key: b"b".to_vec(), value: b"two".to_vec() })]).await.unwrap();
        let mut builder = source.get_snapshot_builder().await; let snapshot = builder.build_snapshot().await.unwrap();
        let mut restored = HomeKvStateMachine::default(); restored.install_snapshot(&snapshot.meta, snapshot.snapshot).await.unwrap();
        assert_eq!(restored.view().await, source.view().await);
        assert!(restored.get_current_snapshot().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn corrupted_snapshot_is_rejected_without_mutating_state() {
        let mut source = HomeKvStateMachine::default(); source.apply(vec![normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() })]).await.unwrap();
        let mut builder = source.get_snapshot_builder().await; let snapshot = builder.build_snapshot().await.unwrap(); let meta = snapshot.meta;
        let mut cursor = snapshot.snapshot; let mut bytes = Vec::new(); cursor.read_to_end(&mut bytes).unwrap(); let last = bytes.len() - 1; bytes[last] ^= 0xff;
        let mut target = HomeKvStateMachine::default(); target.apply(vec![normal(1, RaftCommand::Set { key: b"existing".to_vec(), value: b"safe".to_vec() })]).await.unwrap(); let before = target.view().await;
        assert!(target.install_snapshot(&meta, Box::new(Cursor::new(bytes))).await.is_err()); assert_eq!(target.view().await, before);
    }

    #[tokio::test]
    async fn truncated_snapshot_is_rejected() {
        let mut source = HomeKvStateMachine::default(); source.apply(vec![normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() })]).await.unwrap();
        let mut builder = source.get_snapshot_builder().await; let snapshot = builder.build_snapshot().await.unwrap(); let meta = snapshot.meta;
        let mut cursor = snapshot.snapshot; let mut bytes = Vec::new(); cursor.read_to_end(&mut bytes).unwrap(); bytes.truncate(bytes.len() - 3);
        let mut target = HomeKvStateMachine::default(); assert!(target.install_snapshot(&meta, Box::new(Cursor::new(bytes))).await.is_err());
    }
}