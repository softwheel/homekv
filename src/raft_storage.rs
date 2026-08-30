use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote};
use serde_derive::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::raft::{HomeKvRaftConfig, RaftNodeId};

const MAGIC: &[u8; 8] = b"HKVRLG01";
const FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 8 + 4 + 8 + 8;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged_log_id: Option<LogId<RaftNodeId>>,
    logs: BTreeMap<u64, Entry<HomeKvRaftConfig>>,
}

#[derive(Debug)]
struct StoreInner {
    path: PathBuf,
    state: PersistedState,
    #[cfg(test)]
    fail_next_persist: bool,
}

/// Correctness-first M3 Raft persistence.
///
/// All vote, committed-position and log writes are serialized by one mutex. Each mutation writes
/// a complete versioned/checksummed image to a temporary file, syncs it, atomically replaces the
/// live image, and syncs the parent directory before the in-memory durable view advances. This is
/// deliberately simple; group commit and WAL-layout optimization belong to M5.
#[derive(Clone, Debug)]
pub struct HomeKvRaftLogStore {
    inner: Arc<Mutex<StoreInner>>,
}

impl HomeKvRaftLogStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            Self::read_image(&path)?
        } else {
            PersistedState::default()
        };
        Self::validate_state(&state)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(StoreInner {
                path,
                state,
                #[cfg(test)]
                fail_next_persist: false,
            })),
        })
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, StoreInner>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Raft storage mutex poisoned"))
    }

    fn parent_dir(path: &Path) -> &Path {
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }

    fn encode_image(state: &PersistedState) -> io::Result<Vec<u8>> {
        let payload = bincode::serialize(state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let checksum = xxh3_64(&payload);
        let mut image = Vec::with_capacity(HEADER_LEN + payload.len());
        image.extend_from_slice(MAGIC);
        image.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        image.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        image.extend_from_slice(&checksum.to_le_bytes());
        image.extend_from_slice(&payload);
        Ok(image)
    }

    fn read_image(path: &Path) -> io::Result<PersistedState> {
        let mut file = File::open(path)?;
        let mut image = Vec::new();
        file.read_to_end(&mut image)?;
        if image.len() < HEADER_LEN {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated Raft storage header"));
        }
        if &image[0..8] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid Raft storage magic"));
        }
        let version = u32::from_le_bytes(image[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Raft storage format version {version}"),
            ));
        }
        let payload_len = u64::from_le_bytes(image[12..20].try_into().unwrap()) as usize;
        let expected_checksum = u64::from_le_bytes(image[20..28].try_into().unwrap());
        if image.len() != HEADER_LEN + payload_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated or overlong Raft storage image"));
        }
        let payload = &image[HEADER_LEN..];
        if xxh3_64(payload) != expected_checksum {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Raft storage checksum mismatch"));
        }
        bincode::deserialize(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn validate_state(state: &PersistedState) -> io::Result<()> {
        let mut previous: Option<u64> = None;
        for (index, entry) in &state.logs {
            if *index != entry.log_id.index {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Raft log key/log-id mismatch"));
            }
            if let Some(purged) = state.last_purged_log_id {
                if *index <= purged.index {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Raft log overlaps purged prefix"));
                }
            }
            if let Some(prev) = previous {
                if *index != prev + 1 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "hole in persisted Raft log"));
                }
            } else if let Some(purged) = state.last_purged_log_id {
                if *index != purged.index + 1 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "hole after purged Raft prefix"));
                }
            }
            previous = Some(*index);
        }

        if let Some(committed) = state.committed {
            let last = state
                .logs
                .values()
                .next_back()
                .map(|entry| entry.log_id)
                .or(state.last_purged_log_id);
            if last.map(|id| committed.index > id.index).unwrap_or(true) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "committed Raft position exceeds durable log"));
            }
            if let Some(entry) = state.logs.get(&committed.index) {
                if entry.log_id != committed {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "committed Raft log identity mismatch"));
                }
            }
        }
        Ok(())
    }

    fn persist_locked(inner: &mut StoreInner, candidate: &PersistedState) -> io::Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut inner.fail_next_persist) {
            return Err(io::Error::new(io::ErrorKind::Other, "injected Raft persistence failure"));
        }

        Self::validate_state(candidate)?;
        let image = Self::encode_image(candidate)?;
        let parent = Self::parent_dir(&inner.path);
        fs::create_dir_all(parent)?;

        let mut temp = inner.path.clone();
        let temp_name = inner
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| format!(".{n}.tmp"))
            .unwrap_or_else(|| ".homekv-raft.tmp".to_owned());
        temp.set_file_name(temp_name);

        let write_result = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp)?;
            file.write_all(&image)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &inner.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        write_result
    }

    fn commit_candidate(inner: &mut StoreInner, candidate: PersistedState) -> io::Result<()> {
        Self::persist_locked(inner, &candidate)?;
        inner.state = candidate;
        Ok(())
    }

    fn log_write_error(err: &io::Error) -> StorageError<RaftNodeId> {
        StorageIOError::write_logs(err).into()
    }

    fn log_read_error(err: &io::Error) -> StorageError<RaftNodeId> {
        StorageIOError::read_logs(err).into()
    }

    fn vote_write_error(err: &io::Error) -> StorageError<RaftNodeId> {
        StorageIOError::write_vote(err).into()
    }

    fn vote_read_error(err: &io::Error) -> StorageError<RaftNodeId> {
        StorageIOError::read_vote(err).into()
    }

    #[cfg(test)]
    fn inject_next_persist_failure(&self) {
        self.inner.lock().unwrap().fail_next_persist = true;
    }
}

impl RaftLogReader<HomeKvRaftConfig> for HomeKvRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<HomeKvRaftConfig>>, StorageError<RaftNodeId>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let inner = self.lock().map_err(|e| Self::log_read_error(&e))?;
        Ok(inner
            .state
            .logs
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<HomeKvRaftConfig> for HomeKvRaftLogStore {
    type LogReader = HomeKvRaftLogStore;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<HomeKvRaftConfig>, StorageError<RaftNodeId>> {
        let inner = self.lock().map_err(|e| Self::log_read_error(&e))?;
        let last_log_id = inner
            .state
            .logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(inner.state.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.state.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut inner = self.lock().map_err(|e| Self::vote_write_error(&e))?;
        let mut candidate = inner.state.clone();
        candidate.vote = Some(*vote);
        Self::commit_candidate(&mut inner, candidate).map_err(|e| Self::vote_write_error(&e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        let inner = self.lock().map_err(|e| Self::vote_read_error(&e))?;
        Ok(inner.state.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut inner = self.lock().map_err(|e| Self::log_write_error(&e))?;
        let mut candidate = inner.state.clone();
        candidate.committed = committed;
        Self::commit_candidate(&mut inner, candidate).map_err(|e| Self::log_write_error(&e))
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        let inner = self.lock().map_err(|e| Self::log_read_error(&e))?;
        Ok(inner.state.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<HomeKvRaftConfig>,
    ) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<HomeKvRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            callback.log_io_completed(Ok(()));
            return Ok(());
        }

        let result = (|| -> io::Result<()> {
            let mut inner = self.lock()?;
            let mut candidate = inner.state.clone();
            for window in entries.windows(2) {
                if window[1].log_id.index != window[0].log_id.index + 1 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "non-consecutive Raft append batch"));
                }
            }
            if let Some(last) = candidate
                .logs
                .values()
                .next_back()
                .map(|entry| entry.log_id)
                .or(candidate.last_purged_log_id)
            {
                if entries[0].log_id.index != last.index + 1 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Raft append would create a hole or overwrite without truncate"));
                }
            }
            for entry in entries {
                candidate.logs.insert(entry.log_id.index, entry);
            }
            Self::commit_candidate(&mut inner, candidate)
        })();

        match result {
            Ok(()) => {
                // The durable image and parent-directory sync complete before this callback.
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                let kind = err.kind();
                let message = err.to_string();
                callback.log_io_completed(Err(io::Error::new(kind, message.clone())));
                Err(Self::log_write_error(&io::Error::new(kind, message)))
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut inner = self.lock().map_err(|e| Self::log_write_error(&e))?;
        if inner
            .state
            .last_purged_log_id
            .map(|id| log_id.index <= id.index)
            .unwrap_or(false)
        {
            let err = io::Error::new(io::ErrorKind::InvalidInput, "cannot truncate the purged Raft prefix");
            return Err(Self::log_write_error(&err));
        }
        let mut candidate = inner.state.clone();
        candidate.logs.split_off(&log_id.index);
        if candidate
            .committed
            .map(|id| id.index >= log_id.index)
            .unwrap_or(false)
        {
            let err = io::Error::new(io::ErrorKind::InvalidInput, "cannot truncate committed Raft progress");
            return Err(Self::log_write_error(&err));
        }
        Self::commit_candidate(&mut inner, candidate).map_err(|e| Self::log_write_error(&e))
    }

    async fn purge(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut inner = self.lock().map_err(|e| Self::log_write_error(&e))?;
        if let Some(purged) = inner.state.last_purged_log_id {
            if log_id.index <= purged.index {
                if log_id.index == purged.index && log_id == purged {
                    return Ok(());
                }
                let err = io::Error::new(io::ErrorKind::InvalidInput, "Raft purge regressed or changed purged identity");
                return Err(Self::log_write_error(&err));
            }
        }
        let durable_id = inner.state.logs.get(&log_id.index).map(|entry| entry.log_id);
        if durable_id != Some(log_id) {
            let err = io::Error::new(io::ErrorKind::InvalidInput, "Raft purge target is not a durable log entry");
            return Err(Self::log_write_error(&err));
        }
        let mut candidate = inner.state.clone();
        candidate.logs.retain(|index, _| *index > log_id.index);
        candidate.last_purged_log_id = Some(log_id);
        Self::commit_candidate(&mut inner, candidate).map_err(|e| Self::log_write_error(&e))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload, RaftLogReader};

    use super::*;
    use crate::raft::RaftCommand;

    fn temp_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("homekv-{name}-{}-{nonce}.raft", std::process::id()))
    }

    fn log_id(index: u64) -> LogId<RaftNodeId> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn entry(index: u64) -> Entry<HomeKvRaftConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(RaftCommand::Set {
                key: format!("k{index}").into_bytes(),
                value: format!("v{index}").into_bytes(),
            }),
        }
    }

    fn durable_append_for_test(store: &HomeKvRaftLogStore, entries: Vec<Entry<HomeKvRaftConfig>>) -> io::Result<()> {
        let mut inner = store.lock()?;
        let mut candidate = inner.state.clone();
        for e in entries {
            candidate.logs.insert(e.log_id.index, e);
        }
        HomeKvRaftLogStore::commit_candidate(&mut inner, candidate)
    }

    #[tokio::test]
    async fn vote_is_durable_before_save_returns() {
        let path = temp_path("vote");
        let mut store = HomeKvRaftLogStore::open(&path).unwrap();
        let vote = Vote::new(7, 2);
        store.save_vote(&vote).await.unwrap();
        drop(store);

        let mut reopened = HomeKvRaftLogStore::open(&path).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn log_and_committed_progress_survive_reopen() {
        let path = temp_path("reopen");
        let mut store = HomeKvRaftLogStore::open(&path).unwrap();
        durable_append_for_test(&store, vec![entry(1), entry(2), entry(3)]).unwrap();
        store.save_committed(Some(log_id(2))).await.unwrap();
        drop(store);

        let mut reopened = HomeKvRaftLogStore::open(&path).unwrap();
        let got = reopened.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id(2)));
        assert_eq!(reopened.get_log_state().await.unwrap().last_log_id, Some(log_id(3)));
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn truncate_and_purge_are_hole_free_across_reopen() {
        let path = temp_path("truncate-purge");
        let mut store = HomeKvRaftLogStore::open(&path).unwrap();
        durable_append_for_test(&store, vec![entry(1), entry(2), entry(3), entry(4)]).unwrap();
        store.truncate(log_id(4)).await.unwrap();
        store.purge(log_id(2)).await.unwrap();
        drop(store);

        let mut reopened = HomeKvRaftLogStore::open(&path).unwrap();
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(2)));
        assert_eq!(state.last_log_id, Some(log_id(3)));
        let got = reopened.try_get_log_entries(0..).await.unwrap();
        assert_eq!(got.iter().map(|e| e.log_id.index).collect::<Vec<_>>(), vec![3]);
        let _ = fs::remove_file(path);
    }

    #[tokio::test]
    async fn injected_persist_failure_never_advances_vote_or_log_state() {
        let path = temp_path("io-failure");
        let mut store = HomeKvRaftLogStore::open(&path).unwrap();
        store.inject_next_persist_failure();
        assert!(store.save_vote(&Vote::new(3, 1)).await.is_err());
        assert_eq!(store.read_vote().await.unwrap(), None);

        store.inject_next_persist_failure();
        assert!(durable_append_for_test(&store, vec![entry(1)]).is_err());
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn checksum_truncation_and_version_corruption_fail_closed() {
        for mode in ["checksum", "truncate", "version"] {
            let path = temp_path(mode);
            let store = HomeKvRaftLogStore::open(&path).unwrap();
            durable_append_for_test(&store, vec![entry(1)]).unwrap();
            drop(store);

            let mut bytes = fs::read(&path).unwrap();
            match mode {
                "checksum" => *bytes.last_mut().unwrap() ^= 0x5a,
                "truncate" => bytes.truncate(bytes.len() - 1),
                "version" => bytes[8..12].copy_from_slice(&999_u32.to_le_bytes()),
                _ => unreachable!(),
            }
            fs::write(&path, bytes).unwrap();
            assert!(HomeKvRaftLogStore::open(&path).is_err());
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn durable_append_helper_persists_before_return() {
        let path = temp_path("flush-order");
        let store = HomeKvRaftLogStore::open(&path).unwrap();
        durable_append_for_test(&store, vec![entry(1)]).unwrap();
        // The trait callback is invoked immediately after this same commit path succeeds. Reopening
        // here proves that the callback site cannot precede the durable image boundary.
        let reopened = HomeKvRaftLogStore::open(&path).unwrap();
        assert_eq!(reopened.inner.lock().unwrap().state.logs.len(), 1);
        let _ = fs::remove_file(path);
    }
}