use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote};
use serde_derive::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::raft::{HomeKvRaftConfig, RaftNodeId};

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------

/// Magic for one WAL record frame: `MAGIC | LEN u32 | CRC xxh3_64 u64 | payload`.
const WAL_MAGIC: &[u8; 8] = b"HKVWAL01";
const WAL_RECORD_HEADER_LEN: usize = 8 + 4 + 8;
/// Sanity cap for a single record payload; a larger declared length is corruption.
const WAL_MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;

/// Magic for the atomic metadata image: `MAGIC | VERSION u32 | LEN u64 | CRC u64 | payload`.
const META_MAGIC: &[u8; 8] = b"HKVMTA01";
const META_VERSION: u32 = 1;
const META_HEADER_LEN: usize = 8 + 4 + 8 + 8;

const META_FILE_NAME: &str = "meta";
const WAL_DIR_NAME: &str = "wal";
const SEGMENT_TMP_SUFFIX: &str = ".tmp";

/// Default bound for one WAL segment file (M5-T1; see Spec 0007 §10 open question 1).
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

const LATENCY_UPPER_BOUNDS_MICROS: [u64; 7] = [100, 500, 1_000, 5_000, 10_000, 50_000, 250_000];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LatencyHistogramSnapshot {
    pub upper_bounds_micros: [u64; 7],
    /// Eight disjoint buckets: one for each upper bound and a final overflow bucket.
    pub bucket_counts: [u64; 8],
    pub observations: u64,
    pub total_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug)]
struct LatencyHistogram {
    buckets: [AtomicU64; 8],
    observations: AtomicU64,
    total_micros: AtomicU64,
    max_micros: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            observations: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
            max_micros: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let bucket = LATENCY_UPPER_BOUNDS_MICROS
            .iter()
            .position(|upper| micros <= *upper)
            .unwrap_or(LATENCY_UPPER_BOUNDS_MICROS.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.observations.fetch_add(1, Ordering::Relaxed);
        self.total_micros.fetch_add(micros, Ordering::Relaxed);
        self.max_micros.fetch_max(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            upper_bounds_micros: LATENCY_UPPER_BOUNDS_MICROS,
            bucket_counts: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            observations: self.observations.load(Ordering::Relaxed),
            total_micros: self.total_micros.load(Ordering::Relaxed),
            max_micros: self.max_micros.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RaftStorageMetricsSnapshot {
    pub append_attempts: u64,
    pub appended_entries: u64,
    pub append_failures: u64,
    pub durable_persist_attempts: u64,
    pub durable_persist_failures: u64,
    pub recovery_attempts: u64,
    pub recovery_failures: u64,
    pub append_latency: LatencyHistogramSnapshot,
    pub durable_persist_latency: LatencyHistogramSnapshot,
    pub recovery_latency: LatencyHistogramSnapshot,
    // M5 segmented-WAL counters (REQ-M5-OPS-003).
    pub segment_rotations: u64,
    pub segment_deletions: u64,
    pub torn_tail_truncations: u64,
    pub orphan_segment_deletions: u64,
    pub segments_current: u64,
}

#[derive(Debug, Default)]
struct RaftStorageMetricsInner {
    append_attempts: AtomicU64,
    appended_entries: AtomicU64,
    append_failures: AtomicU64,
    durable_persist_attempts: AtomicU64,
    durable_persist_failures: AtomicU64,
    recovery_attempts: AtomicU64,
    recovery_failures: AtomicU64,
    append_latency: LatencyHistogram,
    durable_persist_latency: LatencyHistogram,
    recovery_latency: LatencyHistogram,
    segment_rotations: AtomicU64,
    segment_deletions: AtomicU64,
    torn_tail_truncations: AtomicU64,
    orphan_segment_deletions: AtomicU64,
    segments_current: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub struct HomeKvRaftStorageMetrics {
    inner: Arc<RaftStorageMetricsInner>,
}

impl HomeKvRaftStorageMetrics {
    fn record_append(&self, entries: usize, duration: Duration, failed: bool) {
        self.inner.append_attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .appended_entries
            .fetch_add(entries as u64, Ordering::Relaxed);
        if failed {
            self.inner.append_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.append_latency.record(duration);
    }

    fn record_persist(&self, duration: Duration, failed: bool) {
        self.inner
            .durable_persist_attempts
            .fetch_add(1, Ordering::Relaxed);
        if failed {
            self.inner
                .durable_persist_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.durable_persist_latency.record(duration);
    }

    fn record_recovery(&self, duration: Duration, failed: bool) {
        self.inner.recovery_attempts.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.inner.recovery_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.recovery_latency.record(duration);
    }

    fn record_rotation(&self) {
        self.inner.segment_rotations.fetch_add(1, Ordering::Relaxed);
    }

    fn record_segment_deletion(&self, count: u64) {
        self.inner
            .segment_deletions
            .fetch_add(count, Ordering::Relaxed);
    }

    fn record_torn_tail_truncation(&self) {
        self.inner
            .torn_tail_truncations
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_orphan_deletion(&self, count: u64) {
        self.inner
            .orphan_segment_deletions
            .fetch_add(count, Ordering::Relaxed);
    }

    fn set_segments_current(&self, count: u64) {
        self.inner.segments_current.store(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RaftStorageMetricsSnapshot {
        RaftStorageMetricsSnapshot {
            append_attempts: self.inner.append_attempts.load(Ordering::Relaxed),
            appended_entries: self.inner.appended_entries.load(Ordering::Relaxed),
            append_failures: self.inner.append_failures.load(Ordering::Relaxed),
            durable_persist_attempts: self.inner.durable_persist_attempts.load(Ordering::Relaxed),
            durable_persist_failures: self.inner.durable_persist_failures.load(Ordering::Relaxed),
            recovery_attempts: self.inner.recovery_attempts.load(Ordering::Relaxed),
            recovery_failures: self.inner.recovery_failures.load(Ordering::Relaxed),
            append_latency: self.inner.append_latency.snapshot(),
            durable_persist_latency: self.inner.durable_persist_latency.snapshot(),
            recovery_latency: self.inner.recovery_latency.snapshot(),
            segment_rotations: self.inner.segment_rotations.load(Ordering::Relaxed),
            segment_deletions: self.inner.segment_deletions.load(Ordering::Relaxed),
            torn_tail_truncations: self.inner.torn_tail_truncations.load(Ordering::Relaxed),
            orphan_segment_deletions: self.inner.orphan_segment_deletions.load(Ordering::Relaxed),
            segments_current: self.inner.segments_current.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration, framing, metadata
// ---------------------------------------------------------------------------

/// Segmented-WAL configuration (Spec 0007 §3).
#[derive(Clone, Copy, Debug)]
pub struct RaftWalConfig {
    /// A segment is rotated before an append would push it past this size.
    pub segment_max_bytes: u64,
}

impl Default for RaftWalConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
        }
    }
}

/// Durable reference to one WAL segment. The inventory (`Vec<SegmentRef>` in
/// [`WalMetadata`]) is the authority for which segments exist
/// (REQ-M5-WAL-003).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SegmentRef {
    seq: u64,
    first_index: u64,
}

/// Small atomic metadata image: vote, committed position, purged prefix, the
/// authoritative durable WAL tail, and the segment inventory. Rewritten
/// atomically on every durable advance (REQ-M5-WAL-002).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct WalMetadata {
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged_log_id: Option<LogId<RaftNodeId>>,
    /// Authoritative durable WAL tail: the last log entry acknowledged to
    /// Raft, i.e. covered by a successful metadata image write. Recovery
    /// replays at most through this index; bytes beyond it in the newest
    /// segment — complete records from a crashed append as well as a torn
    /// tail — are truncated, never resurrected (REQ-M5-WAL-005).
    last_log_id: Option<LogId<RaftNodeId>>,
    segments: Vec<SegmentRef>,
    /// Monotonic rotation counter; never reused so a segment file name is
    /// never recycled within one store directory.
    next_seq: u64,
}

/// In-memory segment bookkeeping: the durable [`SegmentRef`] plus the last
/// log index currently held by the segment (rebuilt on recovery).
#[derive(Clone, Debug)]
struct SegmentState {
    seq: u64,
    first_index: u64,
    last_index: u64,
}

impl SegmentState {
    fn file_name(&self) -> String {
        segment_file_name(self.first_index, self.seq)
    }
}

fn segment_file_name(first_index: u64, seq: u64) -> String {
    format!("seg_{first_index:020}_{seq:010}.wal")
}

fn parse_segment_file_name(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix("seg_")?.strip_suffix(".wal")?;
    let (first, seq) = rest.split_once('_')?;
    Some((first.parse().ok()?, seq.parse().ok()?))
}

fn encode_record(entry: &Entry<HomeKvRaftConfig>) -> io::Result<Vec<u8>> {
    let payload =
        bincode::serialize(entry).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if payload.len() > WAL_MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL record exceeds size cap",
        ));
    }
    let mut buf = Vec::with_capacity(WAL_RECORD_HEADER_LEN + payload.len());
    buf.extend_from_slice(WAL_MAGIC);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&xxh3_64(&payload).to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

enum RecordScan {
    /// (entry, bytes consumed)
    Ok((Entry<HomeKvRaftConfig>, usize)),
    /// Short read at the end of the buffer: a torn tail, not corruption.
    Torn,
    /// Any other framing violation: corruption, fail closed.
    Corrupt(&'static str),
}

fn scan_record(buf: &[u8], offset: usize) -> RecordScan {
    let remaining = &buf[offset..];
    if remaining.len() < WAL_RECORD_HEADER_LEN {
        return RecordScan::Torn;
    }
    if &remaining[0..8] != WAL_MAGIC {
        return RecordScan::Corrupt("invalid WAL record magic");
    }
    let len = u32::from_le_bytes(remaining[8..12].try_into().unwrap()) as usize;
    if len == 0 || len > WAL_MAX_RECORD_BYTES {
        return RecordScan::Corrupt("invalid WAL record length");
    }
    let expected_crc = u64::from_le_bytes(remaining[12..20].try_into().unwrap());
    if remaining.len() < WAL_RECORD_HEADER_LEN + len {
        return RecordScan::Torn;
    }
    let payload = &remaining[WAL_RECORD_HEADER_LEN..WAL_RECORD_HEADER_LEN + len];
    if xxh3_64(payload) != expected_crc {
        return RecordScan::Corrupt("WAL record checksum mismatch");
    }
    match bincode::deserialize::<Entry<HomeKvRaftConfig>>(payload) {
        Ok(entry) => RecordScan::Ok((entry, WAL_RECORD_HEADER_LEN + len)),
        Err(_) => RecordScan::Corrupt("WAL record decode failed"),
    }
}

#[derive(Debug)]
struct StoreInner {
    dir: PathBuf,
    wal_dir: PathBuf,
    meta_path: PathBuf,
    segment_max_bytes: u64,
    vote: Option<Vote<RaftNodeId>>,
    committed: Option<LogId<RaftNodeId>>,
    last_purged_log_id: Option<LogId<RaftNodeId>>,
    logs: BTreeMap<u64, Entry<HomeKvRaftConfig>>,
    segments: Vec<SegmentState>,
    next_seq: u64,
    active_file: Option<File>,
    active_len: u64,
    /// Set when a truncation's physical phase failed and best-effort repair
    /// could not restore the file to the durable tail. Writes fail fast
    /// until the store is reopened (recovery truncates the leftover via the
    /// durable tail); reads are unaffected.
    write_poisoned: bool,
    #[cfg(test)]
    fail_next_wal_write: bool,
    #[cfg(test)]
    fail_next_wal_sync: bool,
    #[cfg(test)]
    fail_next_meta_write: bool,
}

/// M5 segmented-WAL Raft log store (Spec 0007).
///
/// Raft log entries live in append-only, per-record-checksummed WAL
/// segments under `<dir>/wal/`; vote, committed position, purged prefix,
/// and the segment inventory live in the small atomic `<dir>/meta` image.
/// All durable mutations are serialized by one mutex. The per-append
/// completion callback fires only after the entry's bytes are flushed and
/// the metadata image is durably advanced, preserving the M3 acknowledgement
/// boundary (REQ-M5-BASE-001/002).
#[derive(Clone, Debug)]
pub struct HomeKvRaftLogStore {
    inner: Arc<Mutex<StoreInner>>,
    metrics: HomeKvRaftStorageMetrics,
}

impl HomeKvRaftLogStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_config_and_metrics(
            path,
            RaftWalConfig::default(),
            HomeKvRaftStorageMetrics::default(),
        )
    }

    pub fn open_with_metrics(
        path: impl AsRef<Path>,
        metrics: HomeKvRaftStorageMetrics,
    ) -> io::Result<Self> {
        Self::open_with_config_and_metrics(path, RaftWalConfig::default(), metrics)
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: RaftWalConfig) -> io::Result<Self> {
        Self::open_with_config_and_metrics(path, config, HomeKvRaftStorageMetrics::default())
    }

    pub fn open_with_config_and_metrics(
        path: impl AsRef<Path>,
        config: RaftWalConfig,
        metrics: HomeKvRaftStorageMetrics,
    ) -> io::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        let wal_dir = dir.join(WAL_DIR_NAME);
        let meta_path = dir.join(META_FILE_NAME);
        let existed = dir.exists();
        let started = Instant::now();
        let recovered = Self::recover(&dir, &wal_dir, &meta_path, config, &metrics);
        // A recovery attempt is recorded only when there was prior durable
        // state to recover (matching the pre-M5 metric semantics).
        if existed {
            metrics.record_recovery(started.elapsed(), recovered.is_err());
        }
        let inner = recovered?;
        metrics.set_segments_current(inner.segments.len() as u64);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            metrics,
        })
    }

    /// Test/config introspection: current segment inventory.
    #[cfg(test)]
    fn segment_inventory_for_test(&self) -> Vec<(u64, u64)> {
        self.inner
            .lock()
            .unwrap()
            .segments
            .iter()
            .map(|s| (s.first_index, s.seq))
            .collect()
    }

    pub fn metrics(&self) -> RaftStorageMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn metrics_handle(&self) -> HomeKvRaftStorageMetrics {
        self.metrics.clone()
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, StoreInner>> {
        self.inner
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Raft storage mutex poisoned"))
    }

    /// Lock for a mutating operation: fails fast if a previous truncation's
    /// physical phase left the WAL file inconsistent with the durable tail
    /// and repair failed. The store must be reopened (recovery truncates the
    /// leftover via the durable tail); reads are unaffected.
    fn lock_for_write(&self) -> io::Result<MutexGuard<'_, StoreInner>> {
        let inner = self.lock()?;
        if inner.write_poisoned {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Raft WAL store needs reopen after a failed truncation",
            ));
        }
        Ok(inner)
    }

    fn encode_meta_image(meta: &WalMetadata) -> io::Result<Vec<u8>> {
        let payload =
            bincode::serialize(meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let checksum = xxh3_64(&payload);
        let mut image = Vec::with_capacity(META_HEADER_LEN + payload.len());
        image.extend_from_slice(META_MAGIC);
        image.extend_from_slice(&META_VERSION.to_le_bytes());
        image.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        image.extend_from_slice(&checksum.to_le_bytes());
        image.extend_from_slice(&payload);
        Ok(image)
    }

    fn read_meta_image(path: &Path) -> io::Result<WalMetadata> {
        let mut file = File::open(path)?;
        let mut image = Vec::new();
        file.read_to_end(&mut image)?;
        if image.len() < META_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated Raft storage header",
            ));
        }
        if &image[0..8] != META_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Raft storage magic",
            ));
        }
        let version = u32::from_le_bytes(image[8..12].try_into().unwrap());
        if version != META_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Raft storage format version {version}"),
            ));
        }
        let payload_len = u64::from_le_bytes(image[12..20].try_into().unwrap()) as usize;
        let expected_checksum = u64::from_le_bytes(image[20..28].try_into().unwrap());
        if image.len() != META_HEADER_LEN + payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated or overlong Raft storage image",
            ));
        }
        let payload = &image[META_HEADER_LEN..];
        if xxh3_64(payload) != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Raft storage checksum mismatch",
            ));
        }
        let meta: WalMetadata = bincode::deserialize(payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Self::validate_metadata(&meta)?;
        Ok(meta)
    }

    /// Structural validation of a metadata image: inventory ordering and the
    /// monotonic rotation counter. Entry-level validation (holes, committed
    /// range) happens against the replayed log view.
    fn validate_metadata(meta: &WalMetadata) -> io::Result<()> {
        let mut previous_first: Option<u64> = None;
        let mut max_seq = 0u64;
        for segment in &meta.segments {
            if let Some(previous) = previous_first {
                if segment.first_index <= previous {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Raft WAL segment inventory out of order",
                    ));
                }
            }
            previous_first = Some(segment.first_index);
            max_seq = max_seq.max(segment.seq);
        }
        if meta.next_seq <= max_seq {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Raft WAL rotation counter regressed",
            ));
        }
        Ok(())
    }

    /// Committed-position validation against a known durable tail, mirroring
    /// the pre-M5 `validate_state` committed checks.
    fn validate_committed(
        committed: Option<LogId<RaftNodeId>>,
        logs: &BTreeMap<u64, Entry<HomeKvRaftConfig>>,
        last_purged_log_id: Option<LogId<RaftNodeId>>,
    ) -> io::Result<()> {
        if let Some(committed) = committed {
            let last = logs
                .values()
                .next_back()
                .map(|entry| entry.log_id)
                .or(last_purged_log_id);
            if last.map(|id| committed.index > id.index).unwrap_or(true) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "committed Raft position exceeds durable log",
                ));
            }
            if let Some(entry) = logs.get(&committed.index) {
                if entry.log_id != committed {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "committed Raft log identity mismatch",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Atomically replace the metadata image: validate, encode, write to a
    /// temp file, sync it, atomically rename over the live image, and sync
    /// the parent directory (REQ-M5-WAL-002).
    fn write_meta_image(
        inner: &mut StoreInner,
        meta: &WalMetadata,
        metrics: &HomeKvRaftStorageMetrics,
    ) -> io::Result<()> {
        let started = Instant::now();
        let result = (|| -> io::Result<()> {
            #[cfg(test)]
            if std::mem::take(&mut inner.fail_next_meta_write) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "injected Raft metadata write failure",
                ));
            }

            Self::validate_metadata(meta)?;
            Self::validate_committed(meta.committed, &inner.logs, meta.last_purged_log_id)?;
            let image = Self::encode_meta_image(meta)?;
            fs::create_dir_all(&inner.dir)?;

            let mut temp = inner.meta_path.clone();
            temp.set_extension("tmp");
            let write_result = (|| -> io::Result<()> {
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&temp)?;
                file.write_all(&image)?;
                file.sync_all()?;
                drop(file);
                fs::rename(&temp, &inner.meta_path)?;
                File::open(&inner.dir)?.sync_all()?;
                Ok(())
            })();
            if write_result.is_err() {
                let _ = fs::remove_file(&temp);
            }
            write_result
        })();
        metrics.record_persist(started.elapsed(), result.is_err());
        result
    }

    fn fresh_inner(
        dir: &Path,
        wal_dir: &Path,
        meta_path: &Path,
        segment_max_bytes: u64,
    ) -> StoreInner {
        StoreInner {
            dir: dir.to_path_buf(),
            wal_dir: wal_dir.to_path_buf(),
            meta_path: meta_path.to_path_buf(),
            segment_max_bytes,
            vote: None,
            committed: None,
            last_purged_log_id: None,
            logs: BTreeMap::new(),
            segments: Vec::new(),
            next_seq: 1,
            active_file: None,
            active_len: 0,
            write_poisoned: false,
            #[cfg(test)]
            fail_next_wal_write: false,
            #[cfg(test)]
            fail_next_wal_sync: false,
            #[cfg(test)]
            fail_next_meta_write: false,
        }
    }

    /// Recover the durable view: read and validate the metadata image,
    /// reconcile the segment inventory against the files on disk, and replay
    /// the WAL in index order (Spec 0007 §7).
    fn recover(
        dir: &Path,
        wal_dir: &Path,
        meta_path: &Path,
        config: RaftWalConfig,
        metrics: &HomeKvRaftStorageMetrics,
    ) -> io::Result<StoreInner> {
        if !dir.exists() {
            return Ok(Self::fresh_inner(
                dir,
                wal_dir,
                meta_path,
                config.segment_max_bytes,
            ));
        }
        if !dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "existing single-file Raft store layout is not migrated by M5; \
                 start the group on a fresh directory",
            ));
        }

        // Staging temp files are never authoritative; drop leftovers.
        Self::remove_staging_files(dir)?;
        if wal_dir.is_dir() {
            Self::remove_staging_files(wal_dir)?;
        }

        let meta: WalMetadata = if meta_path.exists() {
            Self::read_meta_image(meta_path)?
        } else {
            WalMetadata {
                next_seq: 1,
                ..WalMetadata::default()
            }
        };

        // Inventory reconciliation (REQ-M5-WAL-003): the inventory is the
        // authority. Every inventory entry must have its file (fail closed);
        // files absent from the inventory are orphans of an interrupted
        // rotation/append or of a purge whose deletion did not finish — their
        // records were never acknowledged, so they are not replayed and are
        // deleted best-effort.
        let mut on_disk: Vec<(u64, u64, PathBuf)> = Vec::new();
        if wal_dir.is_dir() {
            for entry in fs::read_dir(wal_dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if let Some((first_index, seq)) = parse_segment_file_name(name) {
                    on_disk.push((first_index, seq, entry.path()));
                }
            }
        }
        let inventory: HashSet<(u64, u64)> = meta
            .segments
            .iter()
            .map(|s| (s.first_index, s.seq))
            .collect();
        let mut orphans = 0u64;
        for (_, _, path) in &on_disk {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let parsed = parse_segment_file_name(name).unwrap();
            if !inventory.contains(&parsed) {
                let _ = fs::remove_file(path);
                orphans += 1;
            }
        }
        if orphans > 0 {
            metrics.record_orphan_deletion(orphans);
        }
        for segment in &meta.segments {
            let path = wal_dir.join(segment_file_name(segment.first_index, segment.seq));
            if !path.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL segment {} of the durable inventory is missing",
                        path.display()
                    ),
                ));
            }
        }

        // Ordered replay. The metadata's `last_log_id` is the authoritative
        // durable WAL tail: only records at/before it were acknowledged to
        // Raft. The newest segment may additionally hold complete but
        // unacknowledged records (crash between WAL fsync and the metadata
        // write) or a torn tail; both are truncated here, never resurrected
        // (REQ-M5-WAL-005). Sealed segments never hold unacknowledged
        // records: rotation is adopted by the metadata write, and a failed
        // append rolls its WAL bytes back.
        let mut logs: BTreeMap<u64, Entry<HomeKvRaftConfig>> = BTreeMap::new();
        let mut segment_states: Vec<SegmentState> = Vec::new();
        let mut expected: Option<u64> = None;
        let segment_count = meta.segments.len();
        let tail_index = meta.last_log_id.map(|id| id.index);
        for (position, segment) in meta.segments.iter().enumerate() {
            let is_last = position + 1 == segment_count;
            let path = wal_dir.join(segment_file_name(segment.first_index, segment.seq));
            let buf = fs::read(&path)?;
            let mut offset = 0usize;
            let mut last_index = segment.first_index.saturating_sub(1);
            // Newest segment only: offset just past the last replayed
            // (acknowledged) record, and the highest replayed index.
            let mut replayed_end = 0u64;
            let mut max_replayed: Option<u64> = None;
            while offset < buf.len() {
                match scan_record(&buf, offset) {
                    RecordScan::Ok((entry, consumed)) => {
                        let index = entry.log_id.index;
                        if is_last {
                            match tail_index {
                                // Records past the durable tail were never
                                // acknowledged: stop replaying. They are
                                // truncated below.
                                Some(tail) if index > tail => break,
                                // Nothing was ever acknowledged: no record
                                // in this segment may be replayed.
                                None => break,
                                _ => {}
                            }
                        }
                        if let Some(purged) = meta.last_purged_log_id {
                            if index <= purged.index {
                                offset += consumed;
                                last_index = last_index.max(index);
                                continue;
                            }
                        }
                        match expected {
                            Some(next) if index != next => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "hole in persisted Raft log",
                                ));
                            }
                            None => {
                                if let Some(purged) = meta.last_purged_log_id {
                                    if index != purged.index + 1 {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidData,
                                            "hole after purged Raft prefix",
                                        ));
                                    }
                                }
                            }
                            _ => {}
                        }
                        logs.insert(index, entry);
                        last_index = last_index.max(index);
                        expected = Some(index + 1);
                        offset += consumed;
                        if is_last {
                            replayed_end = offset as u64;
                            max_replayed = Some(index);
                        }
                    }
                    RecordScan::Torn => {
                        if !is_last {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "truncated WAL record in sealed segment {}",
                                    path.display()
                                ),
                            ));
                        }
                        // Torn tail of the newest segment: stop scanning;
                        // the truncation below drops it together with any
                        // complete-but-unacknowledged records past the tail.
                        break;
                    }
                    RecordScan::Corrupt(message) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "corrupt WAL record in segment {} at offset {offset}: {message}",
                                path.display()
                            ),
                        ));
                    }
                }
            }
            if is_last {
                // The durable tail must be recoverable: every inventoried
                // segment's first record was fsync'd before the metadata
                // write that inventoried it, so all records at/before the
                // tail are present and complete. Anything less means
                // acknowledged data was damaged: fail closed rather than
                // silently drop it.
                if let Some(tail) = tail_index {
                    if max_replayed.map(|max| max < tail).unwrap_or(true) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "durable WAL tail {tail} not recoverable from newest segment {}",
                                path.display()
                            ),
                        ));
                    }
                }
                if buf.len() as u64 > replayed_end {
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(replayed_end)?;
                    file.sync_all()?;
                    metrics.record_torn_tail_truncation();
                }
            }
            segment_states.push(SegmentState {
                seq: segment.seq,
                first_index: segment.first_index,
                last_index,
            });
        }

        Self::validate_committed(meta.committed, &logs, meta.last_purged_log_id)?;

        let mut inner = Self::fresh_inner(dir, wal_dir, meta_path, config.segment_max_bytes);
        inner.vote = meta.vote;
        inner.committed = meta.committed;
        inner.last_purged_log_id = meta.last_purged_log_id;
        inner.logs = logs;
        inner.segments = segment_states;
        inner.next_seq = meta.next_seq.max(1);
        Self::refresh_active(&mut inner)?;
        Ok(inner)
    }

    /// Remove `*.tmp` staging files; they are never authoritative.
    fn remove_staging_files(dir: &Path) -> io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(SEGMENT_TMP_SUFFIX) {
                let _ = fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// (Re)open the append handle on the last inventoried segment, or clear
    /// it when no segment remains.
    fn refresh_active(inner: &mut StoreInner) -> io::Result<()> {
        match inner.segments.last() {
            Some(segment) => {
                let path = inner.wal_dir.join(segment.file_name());
                let file = OpenOptions::new().create(true).append(true).open(&path)?;
                inner.active_len = file.metadata()?.len();
                inner.active_file = Some(file);
            }
            None => {
                inner.active_file = None;
                inner.active_len = 0;
            }
        }
        Ok(())
    }

    fn current_metadata(inner: &StoreInner) -> WalMetadata {
        Self::metadata_for(
            inner.vote,
            inner.committed,
            inner.last_purged_log_id,
            &inner.segments,
            &inner.logs,
            inner.next_seq,
        )
    }

    /// Build the atomic metadata image for an explicit log view. Callers
    /// performing truncate/purge pass the *candidate* view so the durable
    /// image — including the authoritative [`WalMetadata::last_log_id`]
    /// tail — advances before in-memory state is committed.
    fn metadata_for(
        vote: Option<Vote<RaftNodeId>>,
        committed: Option<LogId<RaftNodeId>>,
        last_purged_log_id: Option<LogId<RaftNodeId>>,
        segments: &[SegmentState],
        logs: &BTreeMap<u64, Entry<HomeKvRaftConfig>>,
        next_seq: u64,
    ) -> WalMetadata {
        WalMetadata {
            vote,
            committed,
            last_purged_log_id,
            last_log_id: Self::durable_tail(logs, last_purged_log_id),
            segments: segments
                .iter()
                .map(|s| SegmentRef {
                    seq: s.seq,
                    first_index: s.first_index,
                })
                .collect(),
            next_seq,
        }
    }

    /// Last acknowledged log entry of a view: the in-memory tail, or the
    /// purged boundary when the view holds no entries.
    fn durable_tail(
        logs: &BTreeMap<u64, Entry<HomeKvRaftConfig>>,
        last_purged_log_id: Option<LogId<RaftNodeId>>,
    ) -> Option<LogId<RaftNodeId>> {
        logs.values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(last_purged_log_id)
    }

    fn sync_dir(path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()?;
        Ok(())
    }
}

impl HomeKvRaftLogStore {
    /// Durable append: validate, write framed records to the active segment,
    /// fsync, advance the metadata image, then advance the in-memory view.
    /// (M5-T1 uses per-append fsync; group commit batching arrives in M5-T2.)
    fn append_entries(&self, entries: Vec<Entry<HomeKvRaftConfig>>) -> io::Result<()> {
        let started = Instant::now();
        let entry_count = entries.len();
        if entries.is_empty() {
            self.metrics.record_append(0, started.elapsed(), false);
            return Ok(());
        }
        let result = (|| -> io::Result<()> {
            let mut inner = self.lock_for_write()?;
            for window in entries.windows(2) {
                if window[1].log_id.index != window[0].log_id.index + 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "non-consecutive Raft append batch",
                    ));
                }
            }
            let tail_index = inner
                .logs
                .keys()
                .next_back()
                .copied()
                .or_else(|| inner.last_purged_log_id.map(|id| id.index));
            if let Some(tail) = tail_index {
                if entries[0].log_id.index != tail + 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Raft append would create a hole or overwrite without truncate",
                    ));
                }
            }

            // Split the batch into per-segment chunks so no segment exceeds
            // the size bound (REQ-M5-WAL-003). A single record larger than
            // the bound gets its own (oversized) segment — records are never
            // split across segments.
            let mut chunks: Vec<(u64, u64, Vec<u8>)> = Vec::new();
            for entry in &entries {
                let record = encode_record(entry)?;
                let index = entry.log_id.index;
                match chunks.last_mut() {
                    Some((_, last_index, bytes))
                        if !bytes.is_empty()
                            && bytes.len() as u64 + record.len() as u64
                                <= inner.segment_max_bytes =>
                    {
                        *last_index = index;
                        bytes.extend_from_slice(&record);
                    }
                    _ => chunks.push((index, index, record)),
                }
            }

            // Chunk 0 reuses the active segment when it fits; every later
            // chunk starts a new segment. New segment files are created
            // before the metadata image references them and adopted only
            // after the metadata write below succeeds.
            let first_reuses_active = inner.active_file.is_some()
                && inner.active_len + chunks[0].2.len() as u64 <= inner.segment_max_bytes;
            let mut new_segments: Vec<(SegmentRef, File)> = Vec::new();

            // Length of the previous active segment before this append: the
            // rollback point if anything fails before the metadata write.
            let pre_len = inner.active_len;
            // WAL write + fsync + atomic metadata image. Any failure before
            // the metadata write succeeds must roll the WAL files back: the
            // records were never acknowledged, and leaving them would let a
            // retry write duplicate log indices, poisoning the next recovery
            // (REQ-M5-WAL-005).
            let persist_outcome: io::Result<()> = (|| {
                if !first_reuses_active {
                    new_segments.push(Self::create_segment_file(&mut inner, chunks[0].0)?);
                }
                for (first_index, _, _) in chunks.iter().skip(1) {
                    new_segments.push(Self::create_segment_file(&mut inner, *first_index)?);
                }

                #[cfg(test)]
                if std::mem::take(&mut inner.fail_next_wal_write) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "injected Raft WAL write failure",
                    ));
                }
                // Write every chunk at the true end of its file.
                for (i, (_, _, bytes)) in chunks.iter().enumerate() {
                    let file: &mut File = if i == 0 && first_reuses_active {
                        inner.active_file.as_mut().ok_or_else(|| {
                            io::Error::new(io::ErrorKind::Other, "no active WAL segment")
                        })?
                    } else {
                        let seg_idx = if first_reuses_active { i - 1 } else { i };
                        &mut new_segments[seg_idx].1
                    };
                    // Defensive: always append at the true end of the file.
                    file.seek(SeekFrom::End(0))?;
                    file.write_all(bytes)?;
                }
                #[cfg(test)]
                if std::mem::take(&mut inner.fail_next_wal_sync) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "injected Raft WAL sync failure",
                    ));
                }
                if first_reuses_active {
                    inner
                        .active_file
                        .as_mut()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::Other, "no active WAL segment")
                        })?
                        .sync_all()?;
                }
                for (_, file) in new_segments.iter_mut() {
                    file.sync_all()?;
                }

                let mut meta = Self::current_metadata(&inner);
                for (segment, _) in &new_segments {
                    meta.segments.push(segment.clone());
                }
                // The durable tail advances with this append: the entries are
                // not yet in `inner.logs` (they are inserted after the
                // metadata write succeeds), so set it explicitly.
                meta.last_log_id = entries.last().map(|entry| entry.log_id);
                Self::write_meta_image(&mut inner, &meta, &self.metrics)?;
                Ok(())
            })();
            if persist_outcome.is_err() {
                // None of the new segments were inventoried: remove them
                // outright. `next_seq` stays bumped — sequence numbers are
                // never reused, so the names cannot collide later.
                for (segment, file) in new_segments.drain(..) {
                    drop(file);
                    let path = inner
                        .wal_dir
                        .join(segment_file_name(segment.first_index, segment.seq));
                    let _ = fs::remove_file(&path);
                }
                if first_reuses_active {
                    if let Some(file) = inner.active_file.as_mut() {
                        // Truncate the active segment back to its pre-append
                        // length; best effort. A crash before this rollback
                        // is durable is still safe: recovery truncates
                        // everything past the durable tail.
                        if file.set_len(pre_len).is_ok() {
                            let _ = file.seek(SeekFrom::End(0));
                            let _ = file.sync_all();
                        }
                    }
                }
            }
            persist_outcome?;

            for entry in entries {
                let index = entry.log_id.index;
                inner.logs.insert(index, entry);
            }
            // Commit the segment inventory: chunk 0 may have extended the
            // previous active segment; every new segment is appended in
            // order and the last one becomes active.
            if first_reuses_active {
                let chunk_last = chunks[0].1;
                inner.active_len += chunks[0].2.len() as u64;
                if let Some(segment) = inner.segments.last_mut() {
                    segment.last_index = segment.last_index.max(chunk_last);
                }
            }
            let mut new_segments = new_segments.into_iter();
            for (i, (first_index, last_index, bytes)) in chunks.into_iter().enumerate() {
                if i == 0 && first_reuses_active {
                    continue;
                }
                let (segment, file) = new_segments
                    .next()
                    .expect("one new segment file per new chunk");
                debug_assert_eq!(segment.first_index, first_index);
                inner.segments.push(SegmentState {
                    seq: segment.seq,
                    first_index: segment.first_index,
                    last_index,
                });
                inner.active_file = Some(file);
                inner.active_len = bytes.len() as u64;
                self.metrics.record_rotation();
            }
            self.metrics
                .set_segments_current(inner.segments.len() as u64);
            Ok(())
        })();
        self.metrics
            .record_append(entry_count, started.elapsed(), result.is_err());
        result
    }

    fn save_vote_inner(&self, vote: Vote<RaftNodeId>) -> io::Result<()> {
        let mut inner = self.lock_for_write()?;
        let mut meta = Self::current_metadata(&inner);
        meta.vote = Some(vote);
        Self::write_meta_image(&mut inner, &meta, &self.metrics)?;
        inner.vote = Some(vote);
        Ok(())
    }

    fn save_committed_inner(&self, committed: Option<LogId<RaftNodeId>>) -> io::Result<()> {
        let mut inner = self.lock_for_write()?;
        let mut meta = Self::current_metadata(&inner);
        meta.committed = committed;
        Self::write_meta_image(&mut inner, &meta, &self.metrics)?;
        inner.committed = committed;
        Ok(())
    }

    /// Create a new WAL segment file for `first_index`, fsync the directory
    /// entry, and bump the rotation counter. The file is not inventoried
    /// until the metadata write adopts it; on failure the caller removes it.
    fn create_segment_file(
        inner: &mut StoreInner,
        first_index: u64,
    ) -> io::Result<(SegmentRef, File)> {
        let segment = SegmentRef {
            seq: inner.next_seq,
            first_index,
        };
        fs::create_dir_all(&inner.wal_dir)?;
        let path = inner
            .wal_dir
            .join(segment_file_name(segment.first_index, segment.seq));
        if path.exists() {
            fs::remove_file(&path)?;
        }
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        Self::sync_dir(&inner.wal_dir)?;
        inner.next_seq += 1;
        Ok((segment, file))
    }

    /// Byte length of the retained records with
    /// `first_index <= index < keep_below`, by re-encoding them (the encoder
    /// is deterministic and wrote the segment file).
    fn retained_prefix_len(
        logs: &BTreeMap<u64, Entry<HomeKvRaftConfig>>,
        first_index: u64,
        keep_below: u64,
    ) -> io::Result<u64> {
        let mut len = 0u64;
        for (index, entry) in logs.range(first_index..keep_below) {
            debug_assert!(*index >= first_index && *index < keep_below);
            len += encode_record(entry)?.len() as u64;
        }
        Ok(len)
    }

    /// Shorten a segment file in place, keeping only records with
    /// `first_index <= index < keep_below`, then fsync. Used after the
    /// metadata image has already advanced the durable tail, so a crash
    /// before/during the shorten is reconciled by recovery via that tail.
    fn shorten_segment_file(
        inner: &StoreInner,
        segment: &SegmentState,
        keep_below: u64,
    ) -> io::Result<()> {
        let len = Self::retained_prefix_len(&inner.logs, segment.first_index, keep_below)?;
        let path = inner.wal_dir.join(segment.file_name());
        let file = OpenOptions::new().write(true).open(&path)?;
        file.set_len(len)?;
        file.sync_all()?;
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
        self.inner.lock().unwrap().fail_next_meta_write = true;
    }

    /// Test-only: simulate a crash between WAL fsync and the metadata write
    /// by flushing complete records to the active segment *without*
    /// advancing the metadata image (and thus the durable tail). Recovery
    /// must truncate these bytes, never resurrect them.
    #[cfg(test)]
    fn write_unacknowledged_records(&self, entries: &[Entry<HomeKvRaftConfig>]) -> io::Result<()> {
        let mut inner = self.lock()?;
        let mut record_bytes = Vec::new();
        for entry in entries {
            record_bytes.extend_from_slice(&encode_record(entry)?);
        }
        let file = inner
            .active_file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no active WAL segment"))?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(&record_bytes)?;
        file.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    fn inject_next_wal_write_failure(&self) {
        self.inner.lock().unwrap().fail_next_wal_write = true;
    }

    #[cfg(test)]
    fn inject_next_wal_sync_failure(&self) {
        self.inner.lock().unwrap().fail_next_wal_sync = true;
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
            .logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(inner.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.save_vote_inner(*vote)
            .map_err(|e| Self::vote_write_error(&e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        let inner = self.lock().map_err(|e| Self::vote_read_error(&e))?;
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.save_committed_inner(committed)
            .map_err(|e| Self::log_write_error(&e))
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        let inner = self.lock().map_err(|e| Self::log_read_error(&e))?;
        Ok(inner.committed)
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

        let result = self.append_entries(entries);

        match result {
            Ok(()) => {
                // The segment fsync and the metadata image (with parent
                // directory sync) complete before this callback: the M3
                // acknowledgement boundary is preserved (REQ-M5-BASE-002).
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
        let result = (|| -> io::Result<()> {
            let mut inner = self.lock_for_write()?;
            if inner
                .last_purged_log_id
                .map(|id| log_id.index <= id.index)
                .unwrap_or(false)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot truncate the purged Raft prefix",
                ));
            }
            if inner
                .committed
                .map(|id| id.index >= log_id.index)
                .unwrap_or(false)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot truncate committed Raft progress",
                ));
            }

            // Partition segments: drop those at/after the truncation point,
            // shorten the one containing it.
            let mut drop_seqs: Vec<u64> = Vec::new();
            let mut rewrite: Option<SegmentState> = None;
            for segment in &inner.segments {
                if segment.first_index >= log_id.index {
                    drop_seqs.push(segment.seq);
                } else if segment.last_index >= log_id.index {
                    rewrite = Some(segment.clone());
                }
            }

            // Candidate state: nothing durable or in-memory changes until
            // the metadata write below succeeds.
            let mut candidate_logs = inner.logs.clone();
            candidate_logs.split_off(&log_id.index);
            let mut candidate_segments: Vec<SegmentState> = inner
                .segments
                .iter()
                .filter(|s| !drop_seqs.contains(&s.seq))
                .cloned()
                .collect();
            let mut dropped_names: Vec<String> = inner
                .segments
                .iter()
                .filter(|s| drop_seqs.contains(&s.seq))
                .map(|s| s.file_name())
                .collect();
            // (segment to shorten, keep records with index < keep_below)
            let mut shorten: Option<(SegmentState, u64)> = None;
            if let Some(segment) = rewrite {
                // The shortened segment is the last one remaining: every
                // surviving record at/after its first_index belongs to it.
                let new_last = candidate_logs
                    .keys()
                    .next_back()
                    .copied()
                    .filter(|last| *last >= segment.first_index);
                match new_last {
                    Some(last) => {
                        if let Some(state) =
                            candidate_segments.iter_mut().find(|s| s.seq == segment.seq)
                        {
                            state.last_index = last;
                        }
                        shorten = Some((segment, log_id.index));
                    }
                    None => {
                        // The shorten keeps no records; drop the segment.
                        // Its file is deleted after the metadata write, so
                        // a crash in between only leaves an uninventoried
                        // orphan, which recovery deletes.
                        dropped_names.push(segment.file_name());
                        candidate_segments.retain(|s| s.seq != segment.seq);
                    }
                }
            }

            // The metadata write is the durable truncation point: the new
            // tail and inventory advance BEFORE any physical file shortening
            // or deletion. A crash in between leaves extra unacknowledged
            // bytes in the newest segment, which recovery truncates via the
            // durable tail (REQ-M5-WAL-005).
            let meta = Self::metadata_for(
                inner.vote,
                inner.committed,
                inner.last_purged_log_id,
                &candidate_segments,
                &candidate_logs,
                inner.next_seq,
            );
            Self::write_meta_image(&mut inner, &meta, &self.metrics)?;

            // Commit in-memory state only after the durable advance.
            inner.logs = candidate_logs;
            inner.segments = candidate_segments;

            // Physical phase: shorten the file, delete dropped segments,
            // reopen the active handle. Deletions are best-effort (leftovers
            // are reconciled as orphans on the next open). If shortening
            // fails, repair best-effort; if repair also fails the store is
            // poisoned for writes — continued appends could otherwise write
            // duplicate log indices past the tail. Recovery on the next open
            // truncates any leftover via the durable tail.
            let physical: io::Result<()> = (|| {
                if let Some((segment, keep_below)) = &shorten {
                    Self::shorten_segment_file(&inner, segment, *keep_below)?;
                }
                let mut deleted = 0u64;
                for name in &dropped_names {
                    if fs::remove_file(inner.wal_dir.join(name)).is_ok() {
                        deleted += 1;
                    }
                }
                if deleted > 0 {
                    self.metrics.record_segment_deletion(deleted);
                }
                Self::refresh_active(&mut inner)?;
                Ok(())
            })();
            if let Err(physical_err) = physical {
                if let Some((segment, keep_below)) = &shorten {
                    let _ = Self::shorten_segment_file(&inner, segment, *keep_below);
                }
                let _ = Self::refresh_active(&mut inner);
                let poisoned = match &shorten {
                    Some((segment, keep_below)) => {
                        let expected = Self::retained_prefix_len(
                            &inner.logs,
                            segment.first_index,
                            *keep_below,
                        )
                        .unwrap_or(u64::MAX);
                        fs::metadata(inner.wal_dir.join(segment.file_name()))
                            .map(|m| m.len())
                            .unwrap_or(u64::MAX)
                            > expected
                    }
                    None => false,
                };
                if poisoned {
                    inner.write_poisoned = true;
                }
                return Err(physical_err);
            }

            self.metrics
                .set_segments_current(inner.segments.len() as u64);
            Ok(())
        })();
        result.map_err(|e| Self::log_write_error(&e))
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        let result = (|| -> io::Result<()> {
            let mut inner = self.lock_for_write()?;
            if let Some(purged) = inner.last_purged_log_id {
                if log_id.index <= purged.index {
                    if log_id.index == purged.index && log_id == purged {
                        return Ok(());
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Raft purge regressed or changed purged identity",
                    ));
                }
            }
            let durable_id = inner.logs.get(&log_id.index).map(|entry| entry.log_id);
            match durable_id {
                Some(id) if id == log_id => {}
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Raft purge target identity does not match durable log entry",
                    ));
                }
                None => {
                    let last_local = inner.logs.values().next_back().map(|entry| entry.log_id);
                    if last_local
                        .map(|id| log_id.index <= id.index)
                        .unwrap_or(false)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Raft purge target is missing inside the durable log range",
                        ));
                    }
                    // Snapshot installation may advance a lagging follower's purged
                    // prefix beyond every log entry it ever held locally. OpenRaft
                    // supplies the validated snapshot's last log identity here.
                }
            }

            // Candidate state: the durable purge point first. Nothing
            // in-memory changes until the metadata write below succeeds, so
            // a failed purge can never leave memory ahead of durable state.
            let mut candidate_logs = inner.logs.clone();
            candidate_logs.retain(|index, _| *index > log_id.index);
            let candidate_purged = Some(log_id);
            let mut dropped: Vec<SegmentState> = Vec::new();
            let candidate_segments: Vec<SegmentState> = inner
                .segments
                .iter()
                .filter(|segment| {
                    if segment.last_index <= log_id.index {
                        dropped.push((*segment).clone());
                        false
                    } else {
                        true
                    }
                })
                .cloned()
                .collect();
            // The metadata write is the durable purge point (REQ-M5-WAL-006).
            let meta = Self::metadata_for(
                inner.vote,
                inner.committed,
                candidate_purged,
                &candidate_segments,
                &candidate_logs,
                inner.next_seq,
            );
            Self::write_meta_image(&mut inner, &meta, &self.metrics)?;

            // Commit in-memory state only after the durable advance.
            inner.logs = candidate_logs;
            inner.last_purged_log_id = candidate_purged;
            inner.segments = candidate_segments;

            // Best-effort deletion after the durable advance; leftovers are
            // reconciled as orphans on the next open.
            let mut deleted = 0u64;
            for segment in &dropped {
                if fs::remove_file(inner.wal_dir.join(segment.file_name())).is_ok() {
                    deleted += 1;
                }
            }
            if deleted > 0 {
                self.metrics.record_segment_deletion(deleted);
            }
            self.metrics
                .set_segments_current(inner.segments.len() as u64);
            Ok(())
        })();
        result.map_err(|e| Self::log_write_error(&e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftCommand;
    use openraft::storage::LogState;
    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload, RaftLogReader};
    use std::sync::atomic::AtomicU64;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_dir(name: &str) -> PathBuf {
        let unique = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "homekv-m5t1-{}-{}-{}",
            name,
            std::process::id(),
            unique
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn meta_path(dir: &Path) -> PathBuf {
        dir.join(META_FILE_NAME)
    }

    fn wal_dir_of(dir: &Path) -> PathBuf {
        dir.join(WAL_DIR_NAME)
    }

    fn wal_segment_files(dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = fs::read_dir(wal_dir_of(dir))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| parse_segment_file_name(name).is_some())
                    .unwrap_or(false)
            })
            .collect();
        files.sort();
        files
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

    fn small_config() -> RaftWalConfig {
        RaftWalConfig {
            segment_max_bytes: 1024,
        }
    }

    /// Append 1..=end in batches: segment rotation is evaluated between
    /// append calls, so a single giant batch would never rotate.
    fn append_batched(store: &HomeKvRaftLogStore, end: u64, batch: u64) {
        let mut start = 1;
        while start <= end {
            let stop = (start + batch - 1).min(end);
            store
                .append_entries((start..=stop).map(entry).collect())
                .unwrap();
            start = stop + 1;
        }
    }

    #[tokio::test]
    async fn snapshot_install_purge_may_advance_beyond_local_log() {
        let dir = test_dir("snapshot-install-purge");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();

        let snapshot_last = log_id(41);
        store.purge(snapshot_last).await.unwrap();

        let state: LogState<HomeKvRaftConfig> = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(snapshot_last));
        assert_eq!(state.last_log_id, Some(snapshot_last));

        let mut reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let state: LogState<HomeKvRaftConfig> = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(snapshot_last));
        assert!(reopened.try_get_log_entries(..).await.unwrap().is_empty());
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn vote_is_durable_before_save_returns() {
        let dir = test_dir("vote-durability");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        let vote = Vote::new(3, 7);

        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));

        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        assert_eq!(reopened.clone().read_vote().await.unwrap(), Some(vote));
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn log_and_committed_progress_survive_reopen() {
        let dir = test_dir("log-reopen");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.append_entries((1..=5).map(entry).collect()).unwrap();
        store.save_committed(Some(log_id(4))).await.unwrap();

        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 5);
        assert_eq!(logs[0].log_id, log_id(1));
        assert_eq!(
            reopened.clone().read_committed().await.unwrap(),
            Some(log_id(4))
        );
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn truncate_and_purge_are_hole_free_across_reopen() {
        let dir = test_dir("truncate-purge");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.append_entries((1..=10).map(entry).collect()).unwrap();
        store.truncate(log_id(8)).await.unwrap();
        store.purge(log_id(3)).await.unwrap();

        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let state: LogState<HomeKvRaftConfig> = reopened.clone().get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(3)));
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![4, 5, 6, 7]);
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn injected_persist_failure_never_advances_vote_or_log_state() {
        let dir = test_dir("injected-failure");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.inject_next_persist_failure();
        let vote = Vote::new(1, 7);
        assert!(store.save_vote(&vote).await.is_err());
        assert!(store.read_vote().await.unwrap().is_none());

        store.inject_next_persist_failure();
        assert!(store.append_entries(vec![entry(1)]).is_err());
        assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
        // The failed append left no durable metadata image behind.
        assert!(!meta_path(&dir).exists());

        // The uninventoried segment file is an orphan: recovery deletes it
        // instead of replaying the unacknowledged record.
        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        assert!(reopened
            .clone()
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty());
        assert!(wal_segment_files(&dir).is_empty());
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn injected_wal_failure_keeps_in_memory_view_and_inventory_clean() {
        let dir = test_dir("injected-wal-failure");
        let store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.inject_next_wal_write_failure();
        assert!(store.append_entries(vec![entry(1)]).is_err());
        assert!(store
            .clone()
            .try_get_log_entries(..)
            .await
            .unwrap()
            .is_empty());

        // The rotation happened before the write failure; the next append
        // must not write into the uninventoried file.
        store.append_entries(vec![entry(1)]).unwrap();
        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].log_id, log_id(1));
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn meta_checksum_truncation_and_version_corruption_fail_closed() {
        let dir = test_dir("meta-corruption");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries((1..=3).map(entry).collect()).unwrap();
        drop(store);

        // Flipping a byte in the versioned/checksummed metadata envelope must
        // fail closed on open (no partial or unverifiable state is accepted).
        let pristine = fs::read(meta_path(&dir)).unwrap();
        for offset in [0usize, 8, 16, 24] {
            let mut bytes = pristine.clone();
            bytes[offset] ^= 0xff;
            fs::write(meta_path(&dir), &bytes).unwrap();
            assert!(
                HomeKvRaftLogStore::open(&dir).is_err(),
                "corrupted metadata image at offset {offset} was accepted"
            );
            fs::write(meta_path(&dir), &pristine).unwrap();
        }

        // A truncated metadata image fails closed as well.
        let bytes = fs::read(meta_path(&dir)).unwrap();
        fs::write(meta_path(&dir), &bytes[..bytes.len() / 2]).unwrap();
        let err = HomeKvRaftLogStore::open(&dir).unwrap_err();
        assert!(
            err.to_string().contains("truncated"),
            "unexpected error: {err}"
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn durable_append_helper_persists_before_return() {
        let dir = test_dir("durable-append-helper");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.append_entries((1..=7).map(entry).collect()).unwrap();
        drop(store);

        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 7);
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn storage_metrics_cover_append_persist_and_failed_recovery() {
        let dir = test_dir("metrics");
        let metrics = HomeKvRaftStorageMetrics::default();
        let mut store = HomeKvRaftLogStore::open_with_config_and_metrics(
            &dir,
            RaftWalConfig::default(),
            metrics.clone(),
        )
        .unwrap();

        store.append_entries((1..=4).map(entry).collect()).unwrap();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.durable_persist_attempts, 1);
        assert_eq!(snapshot.durable_persist_failures, 0);
        assert_eq!(snapshot.append_attempts, 1);
        assert_eq!(snapshot.appended_entries, 4);
        assert!(snapshot.append_latency.observations > 0);

        // Corrupting the metadata image makes the next open attempt fail, and
        // the recovery metrics record the failure rather than silently
        // dropping the store.
        drop(store);
        let mut bytes = fs::read(meta_path(&dir)).unwrap();
        bytes[12] ^= 0xff;
        fs::write(meta_path(&dir), &bytes).unwrap();
        assert!(HomeKvRaftLogStore::open_with_config_and_metrics(
            &dir,
            RaftWalConfig::default(),
            metrics.clone()
        )
        .is_err());
        let snapshot = metrics.snapshot();
        // The first open saw the (empty) pre-created test dir and recorded a
        // successful attempt; the corrupted reopen records the failure.
        assert_eq!(snapshot.recovery_attempts, 2);
        assert_eq!(snapshot.recovery_failures, 1);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_record_framing_round_trip_and_corruption() {
        let record = encode_record(&entry(9)).unwrap();
        match scan_record(&record, 0) {
            RecordScan::Ok((decoded, consumed)) => {
                assert_eq!(consumed, record.len());
                assert_eq!(decoded.log_id, log_id(9));
            }
            _ => panic!("expected complete record"),
        }

        // Corrupt magic.
        let mut bad = record.clone();
        bad[0] ^= 0xff;
        assert!(matches!(scan_record(&bad, 0), RecordScan::Corrupt { .. }));

        // Corrupt length (high byte set: claims more than the maximum record
        // size, which can never be a torn tail).
        let mut bad = record.clone();
        bad[11] = 0xff;
        assert!(matches!(scan_record(&bad, 0), RecordScan::Corrupt { .. }));

        // Corrupt a payload byte (checksum mismatch).
        let mut bad = record.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0x01;
        assert!(matches!(scan_record(&bad, 0), RecordScan::Corrupt { .. }));

        // Truncated frame is a torn tail, not corruption.
        assert!(matches!(scan_record(&record[..10], 0), RecordScan::Torn));
        assert!(matches!(scan_record(&[], 0), RecordScan::Torn));
    }

    #[tokio::test]
    async fn wal_large_single_batch_splits_across_segments() {
        let dir = test_dir("wal-big-batch");
        let store = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();

        // One append batch far larger than the segment bound: it must be
        // split across segments, never produce an oversized segment.
        store
            .append_entries((1..=200).map(entry).collect())
            .unwrap();

        let segments = wal_segment_files(&dir);
        assert!(
            segments.len() > 1,
            "expected rotation, got {}",
            segments.len()
        );
        for path in &segments[..segments.len() - 1] {
            assert!(
                fs::metadata(path).unwrap().len() <= small_config().segment_max_bytes,
                "sealed segment over bound: {}",
                path.display()
            );
        }
        drop(store);

        let reopened = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 200);
        assert_eq!(logs[0].log_id, log_id(1));
        assert_eq!(logs[199].log_id, log_id(200));
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_rotation_is_lossless_and_bounded() {
        let dir = test_dir("wal-rotation");
        let metrics = HomeKvRaftStorageMetrics::default();
        let store =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();

        append_batched(&store, 200, 5);

        let segments = wal_segment_files(&dir);
        assert!(
            segments.len() > 1,
            "expected rotation, got {}",
            segments.len()
        );
        // Sealed segments stay within the configured bound.
        for path in &segments[..segments.len() - 1] {
            assert!(
                fs::metadata(path).unwrap().len() <= small_config().segment_max_bytes,
                "sealed segment over bound: {}",
                path.display()
            );
        }
        assert!(metrics.snapshot().segment_rotations >= 1);

        let reopened = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 200);
        assert_eq!(logs[0].log_id, log_id(1));
        assert_eq!(logs[199].log_id, log_id(200));
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_torn_tail_truncates_to_last_complete_record() {
        let dir = test_dir("wal-torn-tail");
        let metrics = HomeKvRaftStorageMetrics::default();
        let store =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();
        // Entries 1..=4 are acknowledged, so the durable tail is 4.
        store.append_entries((1..=4).map(entry).collect()).unwrap();
        drop(store);

        // Simulate a crash mid-append: entry 5's record reached the disk only
        // partially. (A torn write can only damage the in-flight tail — bytes
        // of already-acknowledged records are never rewritten.)
        let segments = wal_segment_files(&dir);
        let newest = segments.last().unwrap();
        let mut bytes = fs::read(newest).unwrap();
        let record5 = encode_record(&entry(5)).unwrap();
        bytes.extend_from_slice(&record5[..7]);
        fs::write(newest, &bytes).unwrap();

        let reopened =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        assert_eq!(metrics.snapshot().torn_tail_truncations, 1);

        // The store is usable again: the torn record can be re-appended.
        reopened.append_entries(vec![entry(5)]).unwrap();
        assert_eq!(
            reopened
                .clone()
                .try_get_log_entries(..)
                .await
                .unwrap()
                .len(),
            5
        );
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_mid_segment_corruption_fails_closed() {
        let dir = test_dir("wal-corruption");
        let store = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        append_batched(&store, 50, 5);
        drop(store);

        let segments = wal_segment_files(&dir);
        assert!(segments.len() > 1);
        // Corrupt a complete record in a sealed (non-newest) segment: the
        // store must fail closed instead of skipping it.
        let mut bytes = fs::read(&segments[0]).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0x40;
        fs::write(&segments[0], &bytes).unwrap();

        let err = HomeKvRaftLogStore::open(&dir).unwrap_err();
        assert!(
            err.to_string().contains("corrupt WAL record"),
            "unexpected error: {err}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_failed_meta_write_does_not_resurrect_unacknowledged() {
        let dir = test_dir("wal-no-resurrect");
        let store = HomeKvRaftLogStore::open(&dir).unwrap();

        // First append succeeds, so the segment is inventoried.
        store.append_entries(vec![entry(1)]).unwrap();
        // The next append flushes to the inventoried segment but its metadata
        // image write is injected to fail: the append errors, the in-memory
        // view does not advance, and the WAL bytes are rolled back.
        store.inject_next_persist_failure();
        assert!(store.append_entries(vec![entry(2)]).is_err());
        assert_eq!(
            store.clone().try_get_log_entries(..).await.unwrap().len(),
            1
        );

        // The failed append rolled its WAL bytes back, so retrying the same
        // entries succeeds: no duplicate log indices are left behind to
        // poison the next recovery.
        store.append_entries(vec![entry(2)]).unwrap();

        // Recovery replays exactly the acknowledged prefix — the
        // unacknowledged record was never resurrected.
        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2]);
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_recovery_truncates_beyond_durable_tail() {
        let dir = test_dir("wal-durable-tail");
        let store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries(vec![entry(1), entry(2)]).unwrap();

        // Simulate a crash after WAL fsync but before the metadata write:
        // complete records for entries 3..=4 sit in the newest segment while
        // the durable tail is still 2.
        store
            .write_unacknowledged_records(&[entry(3), entry(4)])
            .unwrap();
        drop(store);

        // Recovery truncates past the durable tail instead of resurrecting
        // the unacknowledged records.
        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2]);

        // The file was actually shortened: re-appending 3..=4 succeeds and a
        // second recovery replays the full acknowledged log with no
        // duplicates.
        reopened.append_entries(vec![entry(3), entry(4)]).unwrap();
        drop(reopened);
        let reread = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reread.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        drop(reread);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_uninventoried_orphan_is_not_replayed() {
        let dir = test_dir("wal-orphan");
        let metrics = HomeKvRaftStorageMetrics::default();
        let store =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();
        store.append_entries((1..=3).map(entry).collect()).unwrap();
        drop(store);

        // Forge a valid segment file that the inventory does not reference.
        let mut forged = encode_record(&entry(4)).unwrap();
        forged.extend_from_slice(&encode_record(&entry(5)).unwrap());
        fs::write(wal_dir_of(&dir).join(segment_file_name(4, 4242)), &forged).unwrap();

        let reopened =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2, 3]);
        // The orphan was deleted, not replayed.
        assert!(!wal_dir_of(&dir).join(segment_file_name(4, 4242)).exists());
        assert_eq!(metrics.snapshot().orphan_segment_deletions, 1);
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_inventory_without_file_fails_closed() {
        let dir = test_dir("wal-missing-segment");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries((1..=3).map(entry).collect()).unwrap();
        drop(store);

        let segments = wal_segment_files(&dir);
        assert_eq!(segments.len(), 1);
        fs::remove_file(&segments[0]).unwrap();

        let err = HomeKvRaftLogStore::open(&dir).unwrap_err();
        assert!(
            err.to_string().contains("durable inventory is missing"),
            "unexpected error: {err}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_purge_deletes_fully_covered_segments() {
        let dir = test_dir("wal-purge-segments");
        let metrics = HomeKvRaftStorageMetrics::default();
        let mut store =
            HomeKvRaftLogStore::open_with_config_and_metrics(&dir, small_config(), metrics.clone())
                .unwrap();

        append_batched(&store, 300, 10);
        let before = wal_segment_files(&dir).len();
        assert!(before > 2);

        store.purge(log_id(150)).await.unwrap();
        let after = wal_segment_files(&dir).len();
        assert!(after < before, "purge deleted no segments");
        assert!(metrics.snapshot().segment_deletions >= 1);

        let reopened = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes.len(), 150);
        assert_eq!(indexes[0], 151);
        assert_eq!(indexes[149], 300);
        let state: LogState<HomeKvRaftConfig> = reopened.clone().get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(150)));
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_truncate_mid_segment_rewrites_prefix() {
        let dir = test_dir("wal-truncate-rewrite");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries((1..=10).map(entry).collect()).unwrap();

        store.truncate(log_id(6)).await.unwrap();

        let reopened = HomeKvRaftLogStore::open(&dir).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, vec![1, 2, 3, 4, 5]);

        // Truncation survives reopen and new appends continue the log.
        reopened.append_entries(vec![entry(6)]).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 6);
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_truncate_drops_trailing_segments() {
        let dir = test_dir("wal-truncate-segments");
        let mut store = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        append_batched(&store, 100, 5);
        let before = wal_segment_files(&dir).len();
        assert!(before > 1);

        store.truncate(log_id(20)).await.unwrap();
        let after = wal_segment_files(&dir).len();
        assert!(after < before, "truncate deleted no segments");

        let reopened = HomeKvRaftLogStore::open_with_config(&dir, small_config()).unwrap();
        let logs = reopened.clone().try_get_log_entries(..).await.unwrap();
        let indexes: Vec<u64> = logs.iter().map(|entry| entry.log_id.index).collect();
        assert_eq!(indexes, (1..=19).collect::<Vec<u64>>());
        drop(reopened);
        fs::remove_dir_all(&dir).unwrap();
    }

    // NOTE: the `RaftLogStorage::append` trait method delegates to
    // `append_entries` and only fires the M3 acknowledgement callback after
    // that durable flush (WAL fsync + metadata image) returns. The callback
    // type's constructor is `pub(crate)` to openraft, so the ordering is
    // covered structurally here and end-to-end by the M3 integration tests,
    // which drive real replication through this store.

    #[test]
    fn wal_open_rejects_old_single_file_layout() {
        let dir = test_dir("wal-single-file");
        let file_path = dir.join("node-7.raft");
        fs::write(&file_path, b"legacy single-file image").unwrap();

        let err = HomeKvRaftLogStore::open(&file_path).unwrap_err();
        assert!(
            err.to_string().contains("not migrated"),
            "unexpected error: {err}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_save_committed_beyond_durable_tail_fails() {
        let dir = test_dir("wal-committed-guard");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries((1..=3).map(entry).collect()).unwrap();

        let err = store.save_committed(Some(log_id(5))).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("committed Raft position exceeds durable log"),
            "unexpected error: {err}"
        );
        assert!(store.read_committed().await.unwrap().is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_save_committed_at_purged_boundary_is_allowed() {
        let dir = test_dir("wal-committed-purged");
        let mut store = HomeKvRaftLogStore::open(&dir).unwrap();
        // Snapshot install: purge advances beyond the local log.
        store.purge(log_id(10)).await.unwrap();
        store.save_committed(Some(log_id(10))).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(10)));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wal_segment_name_round_trip() {
        assert_eq!(
            parse_segment_file_name(&segment_file_name(7, 42)),
            Some((7, 42))
        );
        assert!(parse_segment_file_name("meta").is_none());
        assert!(parse_segment_file_name("seg_nope.wal").is_none());
        assert!(parse_segment_file_name("seg_00000000000000000007_0000000042.wal.tmp").is_none());
    }

    #[test]
    fn wal_recover_rejects_index_regression() {
        let dir = test_dir("wal-index-regression");
        let store = HomeKvRaftLogStore::open(&dir).unwrap();
        store.append_entries((1..=3).map(entry).collect()).unwrap();
        drop(store);

        // Forge a second segment whose records regress the log index and
        // splice it into the metadata inventory: recovery must fail closed
        // instead of silently dropping or reordering entries.
        let forged = encode_record(&entry(2)).unwrap();
        let forged_name = segment_file_name(4, 1);
        fs::write(wal_dir_of(&dir).join(&forged_name), &forged).unwrap();
        let mut meta = HomeKvRaftLogStore::read_meta_image(&meta_path(&dir)).unwrap();
        meta.segments.push(SegmentRef {
            seq: 1,
            first_index: 4,
        });
        meta.next_seq = 2;
        let encoded = HomeKvRaftLogStore::encode_meta_image(&meta).unwrap();
        let tmp = dir.join(format!("{META_FILE_NAME}.tmp"));
        fs::write(&tmp, &encoded).unwrap();
        File::open(&tmp).unwrap().sync_all().unwrap();
        fs::rename(&tmp, meta_path(&dir)).unwrap();

        let err = HomeKvRaftLogStore::open(&dir).unwrap_err();
        assert!(
            err.to_string().contains("hole in persisted Raft log"),
            "unexpected error: {err}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn wal_non_consecutive_and_hole_appends_are_rejected() {
        let dir = test_dir("wal-append-validation");
        let store = HomeKvRaftLogStore::open(&dir).unwrap();

        store.append_entries((1..=3).map(entry).collect()).unwrap();
        // Non-consecutive batch.
        assert!(store.append_entries(vec![entry(5), entry(6)]).is_err());
        // Hole after the tail.
        assert!(store.append_entries(vec![entry(5)]).is_err());
        // Overwrite without truncate.
        assert!(store.append_entries(vec![entry(3)]).is_err());
        // State untouched by the rejected appends.
        let logs = store.clone().try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.len(), 3);
        fs::remove_dir_all(&dir).unwrap();
    }
}
