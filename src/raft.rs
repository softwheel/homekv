use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
const APPLY_LATENCY_UPPER_BOUNDS_MICROS: [u64; 7] =
    [10, 50, 100, 500, 1_000, 5_000, 25_000];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyLatencyHistogramSnapshot {
    pub upper_bounds_micros: [u64; 7],
    /// Eight disjoint buckets: one for each upper bound and a final overflow bucket.
    pub bucket_counts: [u64; 8],
    pub observations: u64,
    pub total_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug)]
struct ApplyLatencyHistogram {
    buckets: [AtomicU64; 8],
    observations: AtomicU64,
    total_micros: AtomicU64,
    max_micros: AtomicU64,
}

pub type SnapshotLatencyHistogramSnapshot = ApplyLatencyHistogramSnapshot;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotMetricsSnapshot {
    pub build_attempts: u64,
    pub builds_succeeded: u64,
    pub build_failures: u64,
    pub build_bytes: u64,
    pub install_attempts: u64,
    pub installs_succeeded: u64,
    pub install_failures: u64,
    pub install_bytes: u64,
    pub build_latency: SnapshotLatencyHistogramSnapshot,
    pub install_latency: SnapshotLatencyHistogramSnapshot,
}

impl Default for ApplyLatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            observations: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
            max_micros: AtomicU64::new(0),
        }
    }
}

impl ApplyLatencyHistogram {
    fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let bucket = APPLY_LATENCY_UPPER_BOUNDS_MICROS
            .iter()
            .position(|upper| micros <= *upper)
            .unwrap_or(APPLY_LATENCY_UPPER_BOUNDS_MICROS.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.observations.fetch_add(1, Ordering::Relaxed);
        self.total_micros.fetch_add(micros, Ordering::Relaxed);
        self.max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ApplyLatencyHistogramSnapshot {
        ApplyLatencyHistogramSnapshot {
            upper_bounds_micros: APPLY_LATENCY_UPPER_BOUNDS_MICROS,
            bucket_counts: std::array::from_fn(|index| {
                self.buckets[index].load(Ordering::Relaxed)
            }),
            observations: self.observations.load(Ordering::Relaxed),
            total_micros: self.total_micros.load(Ordering::Relaxed),
            max_micros: self.max_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StateMachineApplyMetricsSnapshot {
    pub apply_calls: u64,
    pub entries_seen: u64,
    pub entries_applied: u64,
    pub normal_entries: u64,
    pub blank_entries: u64,
    pub membership_entries: u64,
    pub duplicate_entries: u64,
    pub mutations_applied: u64,
    pub apply_failures: u64,
    pub apply_latency: ApplyLatencyHistogramSnapshot,
}

#[derive(Debug, Default)]
struct StateMachineApplyMetricsInner {
    apply_calls: AtomicU64,
    entries_seen: AtomicU64,
    entries_applied: AtomicU64,
    normal_entries: AtomicU64,
    blank_entries: AtomicU64,
    membership_entries: AtomicU64,
    duplicate_entries: AtomicU64,
    mutations_applied: AtomicU64,
    apply_failures: AtomicU64,
    apply_latency: ApplyLatencyHistogram,
    snapshot_build_attempts: AtomicU64,
    snapshot_builds_succeeded: AtomicU64,
    snapshot_build_failures: AtomicU64,
    snapshot_build_bytes: AtomicU64,
    snapshot_install_attempts: AtomicU64,
    snapshot_installs_succeeded: AtomicU64,
    snapshot_install_failures: AtomicU64,
    snapshot_install_bytes: AtomicU64,
    snapshot_build_latency: ApplyLatencyHistogram,
    snapshot_install_latency: ApplyLatencyHistogram,
}

#[derive(Clone, Debug, Default)]
pub struct HomeKvStateMachineMetrics {
    inner: Arc<StateMachineApplyMetricsInner>,
}

#[derive(Debug, Default)]
struct ApplyObservation {
    entries_seen: u64,
    entries_applied: u64,
    normal_entries: u64,
    blank_entries: u64,
    membership_entries: u64,
    duplicate_entries: u64,
    mutations_applied: u64,
}

impl HomeKvStateMachineMetrics {
    fn record(&self, observation: &ApplyObservation, duration: Duration, failed: bool) {
        self.inner.apply_calls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .entries_seen
            .fetch_add(observation.entries_seen, Ordering::Relaxed);
        self.inner
            .entries_applied
            .fetch_add(observation.entries_applied, Ordering::Relaxed);
        self.inner
            .normal_entries
            .fetch_add(observation.normal_entries, Ordering::Relaxed);
        self.inner
            .blank_entries
            .fetch_add(observation.blank_entries, Ordering::Relaxed);
        self.inner
            .membership_entries
            .fetch_add(observation.membership_entries, Ordering::Relaxed);
        self.inner
            .duplicate_entries
            .fetch_add(observation.duplicate_entries, Ordering::Relaxed);
        self.inner
            .mutations_applied
            .fetch_add(observation.mutations_applied, Ordering::Relaxed);
        if failed {
            self.inner.apply_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.apply_latency.record(duration);
    }

    pub fn snapshot(&self) -> StateMachineApplyMetricsSnapshot {
        StateMachineApplyMetricsSnapshot {
            apply_calls: self.inner.apply_calls.load(Ordering::Relaxed),
            entries_seen: self.inner.entries_seen.load(Ordering::Relaxed),
            entries_applied: self.inner.entries_applied.load(Ordering::Relaxed),
            normal_entries: self.inner.normal_entries.load(Ordering::Relaxed),
            blank_entries: self.inner.blank_entries.load(Ordering::Relaxed),
            membership_entries: self.inner.membership_entries.load(Ordering::Relaxed),
            duplicate_entries: self.inner.duplicate_entries.load(Ordering::Relaxed),
            mutations_applied: self.inner.mutations_applied.load(Ordering::Relaxed),
            apply_failures: self.inner.apply_failures.load(Ordering::Relaxed),
            apply_latency: self.inner.apply_latency.snapshot(),
        }
    }

    fn record_snapshot_build(&self, duration: Duration, bytes: u64, failed: bool) {
        self.inner
            .snapshot_build_attempts
            .fetch_add(1, Ordering::Relaxed);
        if failed {
            self.inner
                .snapshot_build_failures
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .snapshot_builds_succeeded
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .snapshot_build_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        self.inner.snapshot_build_latency.record(duration);
    }

    fn record_snapshot_install(&self, duration: Duration, bytes: u64, failed: bool) {
        self.inner
            .snapshot_install_attempts
            .fetch_add(1, Ordering::Relaxed);
        if failed {
            self.inner
                .snapshot_install_failures
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .snapshot_installs_succeeded
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .snapshot_install_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        self.inner.snapshot_install_latency.record(duration);
    }

    pub fn snapshot_operations(&self) -> SnapshotMetricsSnapshot {
        SnapshotMetricsSnapshot {
            build_attempts: self.inner.snapshot_build_attempts.load(Ordering::Relaxed),
            builds_succeeded: self.inner.snapshot_builds_succeeded.load(Ordering::Relaxed),
            build_failures: self.inner.snapshot_build_failures.load(Ordering::Relaxed),
            build_bytes: self.inner.snapshot_build_bytes.load(Ordering::Relaxed),
            install_attempts: self.inner.snapshot_install_attempts.load(Ordering::Relaxed),
            installs_succeeded: self.inner.snapshot_installs_succeeded.load(Ordering::Relaxed),
            install_failures: self.inner.snapshot_install_failures.load(Ordering::Relaxed),
            install_bytes: self.inner.snapshot_install_bytes.load(Ordering::Relaxed),
            build_latency: self.inner.snapshot_build_latency.snapshot(),
            install_latency: self.inner.snapshot_install_latency.snapshot(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HomeKvStateMachine {
    inner: Arc<RwLock<StateMachineData>>,
    snapshot_path: Option<Arc<PathBuf>>,
    metrics: HomeKvStateMachineMetrics,
}

impl Default for HomeKvStateMachine {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(StateMachineData::default())),
            snapshot_path: None,
            metrics: HomeKvStateMachineMetrics::default(),
        }
    }
}

impl HomeKvStateMachine {
    pub fn open(snapshot_path: impl AsRef<Path>) -> Result<Self, StorageError<RaftNodeId>> {
        let path = snapshot_path.as_ref().to_path_buf();
        let state = match fs::read(&path) {
            Ok(bytes) => {
                let image = Self::decode_snapshot(&bytes)?;
                let meta = Self::snapshot_meta(&image);
                StateMachineData {
                    last_applied: image.last_applied,
                    membership: image.membership,
                    data: image.data,
                    current_snapshot: Some((meta, bytes)),
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => StateMachineData::default(),
            Err(_) => return Err(Self::storage_error("snapshot read failed")),
        };
        Ok(Self {
            inner: Arc::new(RwLock::new(state)),
            snapshot_path: Some(Arc::new(path)),
            metrics: HomeKvStateMachineMetrics::default(),
        })
    }

    pub async fn view(&self) -> StateMachineView {
        let state = self.inner.read().await;
        StateMachineView { last_applied: state.last_applied, membership: state.membership.clone(), data: state.data.clone() }
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> { self.inner.read().await.data.get(key).cloned() }

    pub fn metrics(&self) -> StateMachineApplyMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        self.metrics.snapshot_operations()
    }

    fn storage_error(message: &'static str) -> StorageError<RaftNodeId> {
        let err = io::Error::new(io::ErrorKind::InvalidData, message);
        StorageIOError::read_state_machine(&err).into()
    }

    fn snapshot_meta(image: &SnapshotImage) -> SnapshotMeta<RaftNodeId, RaftNode> {
        let snapshot_id = match image.last_applied {
            Some(id) => format!("m3-{}-{}", id.leader_id.term, id.index),
            None => "m3-empty".to_string(),
        };
        SnapshotMeta {
            last_log_id: image.last_applied,
            last_membership: image.membership.clone(),
            snapshot_id,
        }
    }

    fn persist_snapshot(&self, bytes: &[u8]) -> Result<(), StorageError<RaftNodeId>> {
        let Some(path) = self.snapshot_path.as_deref() else {
            return Ok(());
        };
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .map_err(|_| Self::storage_error("snapshot directory creation failed"))?;
        }
        let temporary = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| Self::storage_error("snapshot temporary file open failed"))?;
        file.write_all(bytes)
            .map_err(|_| Self::storage_error("snapshot write failed"))?;
        file.sync_all()
            .map_err(|_| Self::storage_error("snapshot file sync failed"))?;
        drop(file);
        fs::rename(&temporary, path)
            .map_err(|_| Self::storage_error("snapshot atomic replacement failed"))?;
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| Self::storage_error("snapshot directory sync failed"))?;
        }
        Ok(())
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
        let started = Instant::now();
        let result: Result<Snapshot<HomeKvRaftConfig>, StorageError<RaftNodeId>> = async {
            let mut state = self.state_machine.inner.write().await;
            let image = SnapshotImage {
                format_version: SNAPSHOT_VERSION,
                shard_id: M3_SHARD_ID,
                last_applied: state.last_applied,
                membership: state.membership.clone(),
                data: state.data.clone(),
            };
            let meta = HomeKvStateMachine::snapshot_meta(&image);
            let bytes = HomeKvStateMachine::encode_snapshot(&image)?;
            self.state_machine.persist_snapshot(&bytes)?;
            state.current_snapshot = Some((meta.clone(), bytes.clone()));
            Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(bytes)) })
        }
        .await;
        let bytes = result
            .as_ref()
            .map(|snapshot| snapshot.snapshot.get_ref().len() as u64)
            .unwrap_or(0);
        self.state_machine.metrics.record_snapshot_build(
            started.elapsed(),
            bytes,
            result.is_err(),
        );
        result
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
        let started = Instant::now();
        let mut observation = ApplyObservation::default();
        let result = async {
            let mut responses = Vec::new();
            let mut state = self.inner.write().await;
            for entry in entries {
                observation.entries_seen += 1;
                if let Some(last) = state.last_applied {
                    if entry.log_id == last {
                        observation.duplicate_entries += 1;
                        responses.push(RaftResponse::Noop);
                        continue;
                    }
                    if entry.log_id.index <= last.index {
                        return Err(Self::storage_error("state-machine apply order regressed"));
                    }
                }
                let response = match entry.payload {
                    EntryPayload::Blank => {
                        observation.blank_entries += 1;
                        RaftResponse::Noop
                    }
                    EntryPayload::Normal(command) => {
                        observation.normal_entries += 1;
                        Self::apply_command(&mut state, command)
                    }
                    EntryPayload::Membership(membership) => {
                        observation.membership_entries += 1;
                        state.membership =
                            StoredMembership::new(Some(entry.log_id), membership);
                        RaftResponse::Noop
                    }
                };
                if let RaftResponse::Applied { mutations } = &response {
                    observation.mutations_applied += u64::from(*mutations);
                }
                observation.entries_applied += 1;
                state.last_applied = Some(entry.log_id);
                responses.push(response);
            }
            Ok(responses)
        }
        .await;
        self.metrics
            .record(&observation, started.elapsed(), result.is_err());
        result
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { HomeKvSnapshotBuilder { state_machine: self.clone() } }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta<RaftNodeId, RaftNode>, snapshot: Box<<HomeKvRaftConfig as openraft::RaftTypeConfig>::SnapshotData>) -> Result<(), StorageError<RaftNodeId>> {
        let started = Instant::now();
        let bytes = snapshot.into_inner();
        let byte_count = bytes.len() as u64;
        let result = async {
            let image = Self::decode_snapshot(&bytes)?;
            if image.last_applied != meta.last_log_id || image.membership != meta.last_membership { return Err(Self::storage_error("snapshot metadata mismatch")); }
            let mut state = self.inner.write().await;
            self.persist_snapshot(&bytes)?;
            state.last_applied = image.last_applied;
            state.membership = image.membership;
            state.data = image.data;
            state.current_snapshot = Some((meta.clone(), bytes));
            Ok(())
        }
        .await;
        self.metrics.record_snapshot_install(
            started.elapsed(),
            if result.is_ok() { byte_count } else { 0 },
            result.is_err(),
        );
        result
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
    use std::time::{SystemTime, UNIX_EPOCH};
    use openraft::{CommittedLeaderId, Membership};
    use super::*;

    fn log_id(index: u64) -> LogId<RaftNodeId> { LogId::new(CommittedLeaderId::new(1, 1), index) }
    fn normal(index: u64, command: RaftCommand) -> Entry<HomeKvRaftConfig> { Entry { log_id: log_id(index), payload: EntryPayload::Normal(command) } }

    fn snapshot_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("homekv-{name}-{}-{nonce}.snapshot", std::process::id()))
    }

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
    async fn durable_snapshot_reopens_and_accepts_subsequent_log_replay() {
        let path = snapshot_path("reopen");
        let mut source = HomeKvStateMachine::open(&path).unwrap();
        source.apply(vec![
            normal(1, RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() }),
            normal(2, RaftCommand::Set { key: b"b".to_vec(), value: b"two".to_vec() }),
        ]).await.unwrap();
        source.get_snapshot_builder().await.build_snapshot().await.unwrap();
        drop(source);

        let mut recovered = HomeKvStateMachine::open(&path).unwrap();
        assert_eq!(recovered.get(b"a").await, Some(b"one".to_vec()));
        assert_eq!(recovered.view().await.last_applied, Some(log_id(2)));
        recovered.apply(vec![
            normal(3, RaftCommand::Delete { key: b"a".to_vec() }),
            normal(4, RaftCommand::Set { key: b"c".to_vec(), value: b"three".to_vec() }),
        ]).await.unwrap();
        let view = recovered.view().await;
        assert_eq!(view.last_applied, Some(log_id(4)));
        assert_eq!(view.data, BTreeMap::from([
            (b"b".to_vec(), b"two".to_vec()),
            (b"c".to_vec(), b"three".to_vec()),
        ]));
        assert!(recovered.get_current_snapshot().await.unwrap().is_some());
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn interrupted_temporary_snapshot_never_replaces_last_durable_image() {
        let path = snapshot_path("interrupted");
        let mut source = HomeKvStateMachine::open(&path).unwrap();
        source.apply(vec![normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"safe".to_vec() })]).await.unwrap();
        source.get_snapshot_builder().await.build_snapshot().await.unwrap();
        fs::write(path.with_extension("tmp"), b"incomplete").unwrap();
        drop(source);

        let recovered = HomeKvStateMachine::open(&path).unwrap();
        assert_eq!(recovered.get(b"k").await, Some(b"safe".to_vec()));
        assert_eq!(recovered.view().await.last_applied, Some(log_id(1)));
        fs::remove_file(path.with_extension("tmp")).unwrap();
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn corrupt_durable_snapshot_fails_closed_on_reopen() {
        let path = snapshot_path("corrupt");
        let mut source = HomeKvStateMachine::open(&path).unwrap();
        source.apply(vec![normal(1, RaftCommand::Set { key: b"k".to_vec(), value: b"safe".to_vec() })]).await.unwrap();
        source.get_snapshot_builder().await.build_snapshot().await.unwrap();
        drop(source);

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(HomeKvStateMachine::open(&path).is_err());
        fs::remove_file(path).unwrap();
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

    #[tokio::test]
    async fn apply_metrics_track_command_membership_duplicate_and_failure_transitions() {
        let mut sm = HomeKvStateMachine::default();
        let membership = Membership::new(
            vec![BTreeSet::from([1, 2, 3])],
            BTreeMap::<RaftNodeId, RaftNode>::new(),
        );
        sm.apply(vec![
            normal(
                1,
                RaftCommand::Set {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                },
            ),
            normal(
                2,
                RaftCommand::Batch {
                    mutations: vec![
                        RaftMutation::Set {
                            key: b"b".to_vec(),
                            value: b"two".to_vec(),
                        },
                        RaftMutation::Delete { key: b"a".to_vec() },
                    ],
                },
            ),
            Entry {
                log_id: log_id(3),
                payload: EntryPayload::Blank,
            },
            Entry {
                log_id: log_id(4),
                payload: EntryPayload::Membership(membership),
            },
        ])
        .await
        .unwrap();

        assert_eq!(
            sm.apply(vec![normal(
                4,
                RaftCommand::Delete { key: b"b".to_vec() },
            )])
            .await
            .unwrap(),
            vec![RaftResponse::Noop]
        );
        assert!(sm
            .apply(vec![normal(
                3,
                RaftCommand::Delete { key: b"b".to_vec() },
            )])
            .await
            .is_err());

        let view = sm.view().await;
        assert_eq!(view.last_applied, Some(log_id(4)));
        assert_eq!(view.membership.log_id(), &Some(log_id(4)));
        assert_eq!(view.data.get(b"b".as_slice()), Some(&b"two".to_vec()));

        let metrics = sm.metrics();
        assert_eq!(metrics.apply_calls, 3);
        assert_eq!(metrics.entries_seen, 6);
        assert_eq!(metrics.entries_applied, 4);
        assert_eq!(metrics.normal_entries, 2);
        assert_eq!(metrics.blank_entries, 1);
        assert_eq!(metrics.membership_entries, 1);
        assert_eq!(metrics.duplicate_entries, 1);
        assert_eq!(metrics.mutations_applied, 3);
        assert_eq!(metrics.apply_failures, 1);
        assert_eq!(metrics.apply_latency.observations, 3);
        assert_eq!(
            metrics.apply_latency.bucket_counts.iter().sum::<u64>(),
            metrics.apply_latency.observations
        );
        assert!(!serde_json::to_string(&metrics)
            .unwrap()
            .contains("openraft"));
    }

    #[tokio::test]
    async fn snapshot_metrics_track_success_corruption_and_persistence_failure() {
        let mut source = HomeKvStateMachine::default();
        source
            .apply(vec![normal(
                1,
                RaftCommand::Set {
                    key: b"k".to_vec(),
                    value: b"value".to_vec(),
                },
            )])
            .await
            .unwrap();
        let snapshot = source
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let source_metrics = source.snapshot_metrics();
        assert_eq!(source_metrics.build_attempts, 1);
        assert_eq!(source_metrics.builds_succeeded, 1);
        assert_eq!(source_metrics.build_failures, 0);
        assert!(source_metrics.build_bytes > SNAPSHOT_HEADER_LEN as u64);
        assert_eq!(source_metrics.build_latency.observations, 1);

        let meta = snapshot.meta;
        let bytes = snapshot.snapshot.into_inner();
        let mut target = HomeKvStateMachine::default();
        target
            .install_snapshot(&meta, Box::new(Cursor::new(bytes.clone())))
            .await
            .unwrap();
        let mut corrupted = bytes;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        assert!(target
            .install_snapshot(&meta, Box::new(Cursor::new(corrupted)))
            .await
            .is_err());

        let target_metrics = target.snapshot_metrics();
        assert_eq!(target_metrics.install_attempts, 2);
        assert_eq!(target_metrics.installs_succeeded, 1);
        assert_eq!(target_metrics.install_failures, 1);
        assert_eq!(target_metrics.install_bytes, source_metrics.build_bytes);
        assert_eq!(target_metrics.install_latency.observations, 2);
        assert_eq!(
            target_metrics
                .install_latency
                .bucket_counts
                .iter()
                .sum::<u64>(),
            target_metrics.install_latency.observations
        );
        assert!(!serde_json::to_string(&target_metrics)
            .unwrap()
            .contains("openraft"));

        let blocker = snapshot_path("metrics-blocker");
        fs::create_dir_all(&blocker).unwrap();
        let path = blocker.join("snapshot");
        let mut failing = HomeKvStateMachine::open(&path).unwrap();
        fs::remove_dir(&blocker).unwrap();
        fs::write(&blocker, b"not-a-directory").unwrap();
        assert!(failing
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .is_err());
        let failure_metrics = failing.snapshot_metrics();
        assert_eq!(failure_metrics.build_attempts, 1);
        assert_eq!(failure_metrics.builds_succeeded, 0);
        assert_eq!(failure_metrics.build_failures, 1);
        assert_eq!(failure_metrics.build_bytes, 0);
        assert_eq!(failure_metrics.build_latency.observations, 1);
        fs::remove_file(blocker).unwrap();
    }


}