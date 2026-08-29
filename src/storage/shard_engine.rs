use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use xxhash_rust::xxh3::xxh3_64;

pub const LOGICAL_SHARD_COUNT: u16 = 1024;
const SHARD_MASK: u64 = (LOGICAL_SHARD_COUNT as u64) - 1;
const LIFE_OPEN: u8 = 0;
const LIFE_CLOSING: u8 = 1;
const LIFE_CLOSED: u8 = 2;

pub type ShardEngineResult<T> = std::result::Result<T, ShardEngineError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ShardId(u16);

impl ShardId {
    pub fn new(value: u16) -> ShardEngineResult<Self> {
        if value < LOGICAL_SHARD_COUNT {
            Ok(Self(value))
        } else {
            Err(ShardEngineError::InvalidShard(value))
        }
    }

    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

pub fn shard_for_key(key: &[u8]) -> ShardId {
    ShardId((xxh3_64(key) & SHARD_MASK) as u16)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardBatch {
    shard_id: ShardId,
    mutations: Vec<Mutation>,
}

impl ShardBatch {
    pub fn new(shard_id: ShardId, mutations: Vec<Mutation>) -> ShardEngineResult<Self> {
        if mutations.is_empty() {
            return Err(ShardEngineError::EmptyBatch);
        }
        for mutation in &mutations {
            let actual = shard_for_key(mutation.key());
            if actual != shard_id {
                return Err(ShardEngineError::CrossShardBatch {
                    expected: shard_id,
                    actual,
                });
            }
        }
        Ok(Self {
            shard_id,
            mutations,
        })
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardMetrics {
    pub shard_id: ShardId,
    pub key_count: usize,
    pub logical_bytes: usize,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub overload_rejections: u64,
    pub applied_mutations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardEngineError {
    InvalidShard(u16),
    WrongShard { expected: ShardId, actual: ShardId },
    CrossShardBatch { expected: ShardId, actual: ShardId },
    EmptyBatch,
    QueueFull,
    Closed,
    OwnerStopped,
}

impl fmt::Display for ShardEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardEngineError::InvalidShard(id) => {
                write!(f, "logical shard id {id} is outside 0..{LOGICAL_SHARD_COUNT}")
            }
            ShardEngineError::WrongShard { expected, actual } => write!(
                f,
                "operation belongs to shard {}, not engine shard {}",
                actual.as_u16(),
                expected.as_u16()
            ),
            ShardEngineError::CrossShardBatch { expected, actual } => write!(
                f,
                "batch contains shard {} while declared for shard {}",
                actual.as_u16(),
                expected.as_u16()
            ),
            ShardEngineError::EmptyBatch => write!(f, "atomic shard batch must not be empty"),
            ShardEngineError::QueueFull => write!(f, "shard admission queue is full"),
            ShardEngineError::Closed => write!(f, "shard engine is closed"),
            ShardEngineError::OwnerStopped => write!(f, "shard owner stopped before replying"),
        }
    }
}

impl std::error::Error for ShardEngineError {}

#[derive(Clone)]
pub struct ShardEngine {
    shard_id: ShardId,
    queue_capacity: usize,
    tx: mpsc::Sender<Request>,
    overload_rejections: Arc<AtomicU64>,
    lifecycle: Arc<AtomicU8>,
    admission: Arc<RwLock<()>>,
    owner_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

enum Request {
    Get {
        key: Vec<u8>,
        reply: oneshot::Sender<ShardEngineResult<Option<Vec<u8>>>>,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        reply: oneshot::Sender<ShardEngineResult<()>>,
    },
    Delete {
        key: Vec<u8>,
        reply: oneshot::Sender<ShardEngineResult<()>>,
    },
    Batch {
        batch: ShardBatch,
        reply: oneshot::Sender<ShardEngineResult<()>>,
    },
    Metrics {
        reply: oneshot::Sender<ShardEngineResult<OwnerMetrics>>,
    },
    Shutdown {
        reply: oneshot::Sender<ShardEngineResult<()>>,
    },
    #[cfg(test)]
    Snapshot {
        keys: Vec<Vec<u8>>,
        reply: oneshot::Sender<ShardEngineResult<Vec<Option<Vec<u8>>>>>,
    },
    #[cfg(test)]
    Pause {
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

#[derive(Clone, Debug)]
struct OwnerMetrics {
    key_count: usize,
    logical_bytes: usize,
    applied_mutations: u64,
}

#[derive(Default)]
struct OwnerState {
    data: HashMap<Vec<u8>, Vec<u8>>,
    logical_bytes: usize,
    applied_mutations: u64,
}

impl OwnerState {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let key_len = key.len();
        let value_len = value.len();
        match self.data.insert(key, value) {
            Some(previous) => self.logical_bytes = self.logical_bytes - previous.len() + value_len,
            None => self.logical_bytes += key_len + value_len,
        }
        self.applied_mutations += 1;
    }

    fn delete(&mut self, key: Vec<u8>) {
        if let Some(previous) = self.data.remove(&key) {
            self.logical_bytes -= key.len() + previous.len();
        }
        self.applied_mutations += 1;
    }

    fn apply_mutation(&mut self, mutation: Mutation) {
        match mutation {
            Mutation::Put { key, value } => self.put(key, value),
            Mutation::Delete { key } => self.delete(key),
        }
    }

    fn apply_batch(&mut self, batch: ShardBatch) {
        for mutation in batch.mutations {
            self.apply_mutation(mutation);
        }
    }

    fn metrics(&self) -> OwnerMetrics {
        OwnerMetrics {
            key_count: self.data.len(),
            logical_bytes: self.logical_bytes,
            applied_mutations: self.applied_mutations,
        }
    }
}

impl ShardEngine {
    pub fn spawn(shard_id: ShardId, queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0, "shard queue capacity must be positive");
        let (tx, rx) = mpsc::channel(queue_capacity);
        let owner_task = tokio::spawn(owner_loop(rx));
        Self {
            shard_id,
            queue_capacity,
            tx,
            overload_rejections: Arc::new(AtomicU64::new(0)),
            lifecycle: Arc::new(AtomicU8::new(LIFE_OPEN)),
            admission: Arc::new(RwLock::new(())),
            owner_task: Arc::new(Mutex::new(Some(owner_task))),
        }
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    fn validate_key(&self, key: &[u8]) -> ShardEngineResult<()> {
        let actual = shard_for_key(key);
        if actual == self.shard_id {
            Ok(())
        } else {
            Err(ShardEngineError::WrongShard {
                expected: self.shard_id,
                actual,
            })
        }
    }

    fn ensure_open(&self) -> ShardEngineResult<()> {
        if self.lifecycle.load(Ordering::Acquire) == LIFE_OPEN {
            Ok(())
        } else {
            Err(ShardEngineError::Closed)
        }
    }

    fn map_try_send_error(&self, error: mpsc::error::TrySendError<Request>) -> ShardEngineError {
        match error {
            mpsc::error::TrySendError::Full(_) => {
                self.overload_rejections.fetch_add(1, Ordering::Relaxed);
                ShardEngineError::QueueFull
            }
            mpsc::error::TrySendError::Closed(_) => ShardEngineError::Closed,
        }
    }

    async fn join_owner(&self) -> ShardEngineResult<()> {
        let mut owner_task = self.owner_task.lock().await;
        if let Some(handle) = owner_task.take() {
            handle.await.map_err(|_| ShardEngineError::OwnerStopped)?;
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> ShardEngineResult<Option<Vec<u8>>> {
        self.validate_key(key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Get {
                    key: key.to_vec(),
                    reply: reply_tx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn try_get(&self, key: &[u8]) -> ShardEngineResult<Option<Vec<u8>>> {
        self.validate_key(key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .try_send(Request::Get {
                    key: key.to_vec(),
                    reply: reply_tx,
                })
                .map_err(|error| self.map_try_send_error(error))?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Put {
                    key,
                    value,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn try_put(&self, key: Vec<u8>, value: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .try_send(Request::Put {
                    key,
                    value,
                    reply: reply_tx,
                })
                .map_err(|error| self.map_try_send_error(error))?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn delete(&self, key: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Delete {
                    key,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn try_delete(&self, key: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .try_send(Request::Delete {
                    key,
                    reply: reply_tx,
                })
                .map_err(|error| self.map_try_send_error(error))?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn apply_batch(&self, batch: ShardBatch) -> ShardEngineResult<()> {
        if batch.shard_id != self.shard_id {
            return Err(ShardEngineError::WrongShard {
                expected: self.shard_id,
                actual: batch.shard_id,
            });
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Batch {
                    batch,
                    reply: reply_tx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn try_apply_batch(&self, batch: ShardBatch) -> ShardEngineResult<()> {
        if batch.shard_id != self.shard_id {
            return Err(ShardEngineError::WrongShard {
                expected: self.shard_id,
                actual: batch.shard_id,
            });
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .try_send(Request::Batch {
                    batch,
                    reply: reply_tx,
                })
                .map_err(|error| self.map_try_send_error(error))?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn metrics(&self) -> ShardEngineResult<ShardMetrics> {
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Metrics { reply: reply_tx })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        let owner = reply_rx
            .await
            .map_err(|_| ShardEngineError::OwnerStopped)??;
        Ok(ShardMetrics {
            shard_id: self.shard_id,
            key_count: owner.key_count,
            logical_bytes: owner.logical_bytes,
            queue_capacity: self.queue_capacity,
            queue_depth: self.queue_capacity.saturating_sub(self.tx.capacity()),
            overload_rejections: self.overload_rejections.load(Ordering::Relaxed),
            applied_mutations: owner.applied_mutations,
        })
    }

    pub async fn shutdown(&self) -> ShardEngineResult<()> {
        let admission = self.admission.write().await;
        match self.lifecycle.load(Ordering::Acquire) {
            LIFE_CLOSED => return Ok(()),
            LIFE_CLOSING => {
                drop(admission);
                let result = self.join_owner().await;
                self.lifecycle.store(LIFE_CLOSED, Ordering::Release);
                return result;
            }
            LIFE_OPEN => {}
            _ => unreachable!("valid lifecycle state"),
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Shutdown { reply: reply_tx })
            .await
            .map_err(|_| ShardEngineError::Closed)?;
        self.lifecycle.store(LIFE_CLOSING, Ordering::Release);
        drop(admission);

        let reply_result = reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?;
        let join_result = self.join_owner().await;
        self.lifecycle.store(LIFE_CLOSED, Ordering::Release);
        reply_result?;
        join_result
    }

    #[cfg(test)]
    async fn snapshot(&self, keys: &[Vec<u8>]) -> ShardEngineResult<Vec<Option<Vec<u8>>>> {
        for key in keys {
            self.validate_key(key)?;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Snapshot {
                    keys: keys.to_vec(),
                    reply: reply_tx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    #[cfg(test)]
    async fn pause_owner(&self) -> ShardEngineResult<oneshot::Sender<()>> {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        {
            let _admission = self.admission.read().await;
            self.ensure_open()?;
            self.tx
                .send(Request::Pause {
                    entered: entered_tx,
                    release: release_rx,
                })
                .await
                .map_err(|_| ShardEngineError::Closed)?;
        }
        entered_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?;
        Ok(release_tx)
    }
}

async fn owner_loop(mut rx: mpsc::Receiver<Request>) {
    let mut state = OwnerState::default();
    while let Some(request) = rx.recv().await {
        match request {
            Request::Get { key, reply } => {
                let _ = reply.send(Ok(state.get(&key)));
            }
            Request::Put { key, value, reply } => {
                state.put(key, value);
                let _ = reply.send(Ok(()));
            }
            Request::Delete { key, reply } => {
                state.delete(key);
                let _ = reply.send(Ok(()));
            }
            Request::Batch { batch, reply } => {
                state.apply_batch(batch);
                let _ = reply.send(Ok(()));
            }
            Request::Metrics { reply } => {
                let _ = reply.send(Ok(state.metrics()));
            }
            Request::Shutdown { reply } => {
                rx.close();
                let _ = reply.send(Ok(()));
                break;
            }
            #[cfg(test)]
            Request::Snapshot { keys, reply } => {
                let values = keys.iter().map(|key| state.get(key)).collect();
                let _ = reply.send(Ok(values));
            }
            #[cfg(test)]
            Request::Pause { entered, release } => {
                let _ = entered.send(());
                let _ = release.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_for_shard(shard: ShardId, count: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(count);
        for i in 0_u64..1_000_000 {
            let key = format!("shard-key-{i}").into_bytes();
            if shard_for_key(&key) == shard {
                keys.push(key);
                if keys.len() == count {
                    return keys;
                }
            }
        }
        panic!("failed to find {count} keys for shard {}", shard.as_u16());
    }

    fn key_for_different_shard(shard: ShardId) -> Vec<u8> {
        for i in 0_u64..10_000 {
            let key = format!("other-shard-key-{i}").into_bytes();
            if shard_for_key(&key) != shard {
                return key;
            }
        }
        panic!("failed to find key outside shard {}", shard.as_u16());
    }

    async fn wait_until_queue_full(engine: &ShardEngine) {
        for _ in 0..1000 {
            if engine.tx.capacity() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("queue did not become full");
    }

    #[test]
    fn shard_id_bounds_are_fixed_to_1024() {
        assert_eq!(ShardId::new(0).unwrap().as_u16(), 0);
        assert_eq!(ShardId::new(1023).unwrap().as_u16(), 1023);
        assert_eq!(ShardId::new(1024), Err(ShardEngineError::InvalidShard(1024)));
    }

    #[test]
    fn xxh3_mapping_golden_vectors_are_stable() {
        assert_eq!(shard_for_key(b"").as_u16(), 194);
        assert_eq!(shard_for_key(b"abc").as_u16(), 336);
    }

    #[test]
    fn shard_mapping_is_deterministic_and_bounded() {
        for i in 0_u64..10_000 {
            let key = format!("homekv-key-{i}").into_bytes();
            assert_eq!(shard_for_key(&key), shard_for_key(&key));
            assert!(shard_for_key(&key).as_u16() < LOGICAL_SHARD_COUNT);
        }
    }

    #[tokio::test]
    async fn owner_engine_put_get_delete_round_trip() {
        let key = b"alpha".to_vec();
        let engine = ShardEngine::spawn(shard_for_key(&key), 16);
        assert_eq!(engine.get(&key).await.unwrap(), None);
        engine.put(key.clone(), b"one".to_vec()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), Some(b"one".to_vec()));
        engine.put(key.clone(), b"two".to_vec()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), Some(b"two".to_vec()));
        engine.delete(key.clone()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), None);
        engine.delete(key.clone()).await.unwrap();
    }

    #[tokio::test]
    async fn engine_rejects_keys_owned_by_another_shard() {
        let key = b"owned-key".to_vec();
        let shard = shard_for_key(&key);
        let engine = ShardEngine::spawn(shard, 8);
        let other = key_for_different_shard(shard);
        assert!(matches!(
            engine.put(other, b"v".to_vec()).await,
            Err(ShardEngineError::WrongShard { .. })
        ));
    }

    #[test]
    fn batch_constructor_rejects_empty_and_cross_shard_commands() {
        let key = b"batch-key".to_vec();
        let shard = shard_for_key(&key);
        assert_eq!(ShardBatch::new(shard, Vec::new()), Err(ShardEngineError::EmptyBatch));
        let other = key_for_different_shard(shard);
        assert!(matches!(
            ShardBatch::new(
                shard,
                vec![Mutation::Put {
                    key: other,
                    value: b"v".to_vec(),
                }],
            ),
            Err(ShardEngineError::CrossShardBatch { .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_snapshots_never_see_partial_batch() {
        let seed = b"visibility-seed".to_vec();
        let shard = shard_for_key(&seed);
        let keys = keys_for_shard(shard, 4);
        let engine = ShardEngine::spawn(shard, 64);
        let start = ShardBatch::new(
            shard,
            keys.iter()
                .cloned()
                .map(|key| Mutation::Put { key, value: b"a".to_vec() })
                .collect(),
        )
        .unwrap();
        engine.apply_batch(start).await.unwrap();

        let writer = {
            let engine = engine.clone();
            let keys = keys.clone();
            tokio::spawn(async move {
                for round in 0..200 {
                    let value = if round % 2 == 0 { b"b" } else { b"a" };
                    let batch = ShardBatch::new(
                        shard,
                        keys.iter()
                            .cloned()
                            .map(|key| Mutation::Put { key, value: value.to_vec() })
                            .collect(),
                    )
                    .unwrap();
                    engine.apply_batch(batch).await.unwrap();
                }
            })
        };
        let reader = {
            let engine = engine.clone();
            let keys = keys.clone();
            tokio::spawn(async move {
                for _ in 0..500 {
                    let snapshot = engine.snapshot(&keys).await.unwrap();
                    let first = snapshot[0].clone();
                    assert!(snapshot.iter().all(|value| *value == first));
                }
            })
        };
        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn logical_memory_accounting_tracks_replacements_deletes_and_batches() {
        let seed = b"metrics-seed".to_vec();
        let shard = shard_for_key(&seed);
        let keys = keys_for_shard(shard, 3);
        let engine = ShardEngine::spawn(shard, 16);
        let a = keys[0].clone();
        let b = keys[1].clone();
        let absent = keys[2].clone();
        engine.put(a.clone(), b"123".to_vec()).await.unwrap();
        assert_eq!(engine.metrics().await.unwrap().logical_bytes, a.len() + 3);
        engine.put(a.clone(), b"12345".to_vec()).await.unwrap();
        engine.delete(absent).await.unwrap();
        let batch = ShardBatch::new(
            shard,
            vec![
                Mutation::Put { key: b.clone(), value: b"x".to_vec() },
                Mutation::Put { key: b.clone(), value: b"longer".to_vec() },
                Mutation::Delete { key: a },
            ],
        )
        .unwrap();
        engine.apply_batch(batch).await.unwrap();
        let metrics = engine.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 1);
        assert_eq!(metrics.logical_bytes, b.len() + b"longer".len());
        assert_eq!(metrics.applied_mutations, 6);
        assert_eq!(metrics.overload_rejections, 0);
    }

    #[tokio::test]
    async fn try_admission_reports_queue_full_and_counts_rejection() {
        let seed = b"overload-seed".to_vec();
        let shard = shard_for_key(&seed);
        let keys = keys_for_shard(shard, 2);
        let engine = ShardEngine::spawn(shard, 1);
        let release = engine.pause_owner().await.unwrap();

        let queued = {
            let engine = engine.clone();
            let key = keys[0].clone();
            tokio::spawn(async move { engine.try_put(key, b"accepted".to_vec()).await })
        };
        wait_until_queue_full(&engine).await;
        assert_eq!(
            engine.try_put(keys[1].clone(), b"rejected".to_vec()).await,
            Err(ShardEngineError::QueueFull)
        );
        release.send(()).unwrap();
        queued.await.unwrap().unwrap();
        assert_eq!(engine.metrics().await.unwrap().overload_rejections, 1);
    }

    #[tokio::test]
    async fn cancellation_before_admission_does_not_apply_mutation() {
        let seed = b"cancel-before".to_vec();
        let shard = shard_for_key(&seed);
        let keys = keys_for_shard(shard, 2);
        let engine = ShardEngine::spawn(shard, 1);
        let release = engine.pause_owner().await.unwrap();

        let accepted = {
            let engine = engine.clone();
            let key = keys[0].clone();
            tokio::spawn(async move { engine.try_put(key, b"first".to_vec()).await })
        };
        wait_until_queue_full(&engine).await;
        let waiting = {
            let engine = engine.clone();
            let key = keys[1].clone();
            tokio::spawn(async move { engine.put(key, b"must-not-apply".to_vec()).await })
        };
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        release.send(()).unwrap();
        accepted.await.unwrap().unwrap();
        assert_eq!(engine.get(&keys[1]).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancellation_after_admission_still_applies_exactly_once() {
        let seed = b"cancel-after".to_vec();
        let shard = shard_for_key(&seed);
        let key = keys_for_shard(shard, 1).remove(0);
        let engine = ShardEngine::spawn(shard, 2);
        let release = engine.pause_owner().await.unwrap();

        let accepted = {
            let engine = engine.clone();
            let key = key.clone();
            tokio::spawn(async move { engine.try_put(key, b"committed".to_vec()).await })
        };
        for _ in 0..1000 {
            if engine.tx.capacity() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        accepted.abort();
        let _ = accepted.await;
        release.send(()).unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), Some(b"committed".to_vec()));
        assert_eq!(engine.metrics().await.unwrap().applied_mutations, 1);
    }

    #[tokio::test]
    async fn shutdown_drains_accepted_work_and_rejects_new_admission() {
        let seed = b"shutdown-seed".to_vec();
        let shard = shard_for_key(&seed);
        let key = keys_for_shard(shard, 1).remove(0);
        let engine = ShardEngine::spawn(shard, 1);
        let release = engine.pause_owner().await.unwrap();

        let accepted = {
            let engine = engine.clone();
            let key = key.clone();
            tokio::spawn(async move { engine.try_put(key, b"drained".to_vec()).await })
        };
        wait_until_queue_full(&engine).await;
        let shutdown = {
            let engine = engine.clone();
            tokio::spawn(async move { engine.shutdown().await })
        };
        tokio::task::yield_now().await;
        release.send(()).unwrap();
        accepted.await.unwrap().unwrap();
        shutdown.await.unwrap().unwrap();
        assert_eq!(engine.get(&key).await, Err(ShardEngineError::Closed));
        assert_eq!(engine.try_get(&key).await, Err(ShardEngineError::Closed));
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn independent_shards_do_not_share_state() {
        let left_key = b"left-key".to_vec();
        let left_shard = shard_for_key(&left_key);
        let right_key = key_for_different_shard(left_shard);
        let right_shard = shard_for_key(&right_key);
        let left = ShardEngine::spawn(left_shard, 8);
        let right = ShardEngine::spawn(right_shard, 8);
        left.put(left_key.clone(), b"left".to_vec()).await.unwrap();
        right.put(right_key.clone(), b"right".to_vec()).await.unwrap();
        assert_eq!(left.get(&left_key).await.unwrap(), Some(b"left".to_vec()));
        assert_eq!(right.get(&right_key).await.unwrap(), Some(b"right".to_vec()));
    }

    #[test]
    fn mutation_representation_is_deterministic_data() {
        let a = Mutation::Put { key: b"k".to_vec(), value: b"v".to_vec() };
        let b = Mutation::Put { key: b"k".to_vec(), value: b"v".to_vec() };
        assert_eq!(a, b);
    }
}
