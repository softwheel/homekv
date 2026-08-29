use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use xxhash_rust::xxh3::xxh3_64;

pub const LOGICAL_SHARD_COUNT: u16 = 1024;
const SHARD_MASK: u64 = (LOGICAL_SHARD_COUNT as u64) - 1;

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
    // Masking the low ten bits guarantees the value is always in 0..1024.
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
    WrongShard {
        expected: ShardId,
        actual: ShardId,
    },
    CrossShardBatch {
        expected: ShardId,
        actual: ShardId,
    },
    EmptyBatch,
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
            ShardEngineError::Closed => write!(f, "shard engine is closed"),
            ShardEngineError::OwnerStopped => write!(f, "shard owner stopped before replying"),
        }
    }
}

impl std::error::Error for ShardEngineError {}

#[derive(Clone, Debug)]
pub struct ShardEngine {
    shard_id: ShardId,
    queue_capacity: usize,
    tx: mpsc::Sender<Request>,
    overload_rejections: Arc<AtomicU64>,
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
    #[cfg(test)]
    Snapshot {
        keys: Vec<Vec<u8>>,
        reply: oneshot::Sender<ShardEngineResult<Vec<Option<Vec<u8>>>>>,
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
            Some(previous) => {
                self.logical_bytes = self.logical_bytes - previous.len() + value_len;
            }
            None => {
                self.logical_bytes += key_len + value_len;
            }
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
        // Candidate A's owner loop contains no await/yield inside this call, so no
        // other GET or mutation can observe a partially applied batch.
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
        tokio::spawn(owner_loop(rx));

        Self {
            shard_id,
            queue_capacity,
            tx,
            overload_rejections: Arc::new(AtomicU64::new(0)),
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

    pub async fn get(&self, key: &[u8]) -> ShardEngineResult<Option<Vec<u8>>> {
        self.validate_key(key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Get {
                key: key.to_vec(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Put {
                key,
                value,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn delete(&self, key: Vec<u8>) -> ShardEngineResult<()> {
        self.validate_key(&key)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Delete {
                key,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

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
        self.tx
            .send(Request::Batch {
                batch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }

    pub async fn metrics(&self) -> ShardEngineResult<ShardMetrics> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Metrics { reply: reply_tx })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

        let owner = reply_rx
            .await
            .map_err(|_| ShardEngineError::OwnerStopped)??;
        let queue_depth = self.queue_capacity.saturating_sub(self.tx.capacity());

        Ok(ShardMetrics {
            shard_id: self.shard_id,
            key_count: owner.key_count,
            logical_bytes: owner.logical_bytes,
            queue_capacity: self.queue_capacity,
            queue_depth,
            overload_rejections: self.overload_rejections.load(Ordering::Relaxed),
            applied_mutations: owner.applied_mutations,
        })
    }

    #[cfg(test)]
    async fn snapshot(&self, keys: &[Vec<u8>]) -> ShardEngineResult<Vec<Option<Vec<u8>>>> {
        for key in keys {
            self.validate_key(key)?;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Request::Snapshot {
                keys: keys.to_vec(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| ShardEngineError::Closed)?;

        reply_rx.await.map_err(|_| ShardEngineError::OwnerStopped)?
    }
}

async fn owner_loop(mut rx: mpsc::Receiver<Request>) {
    let mut state = OwnerState::default();

    while let Some(request) = rx.recv().await {
        match request {
            Request::Get { key, reply } => {
                let _ = reply.send(Ok(state.get(&key)));
            }
            Request::Put {
                key,
                value,
                reply,
            } => {
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
            #[cfg(test)]
            Request::Snapshot { keys, reply } => {
                let values = keys.iter().map(|key| state.get(key)).collect();
                let _ = reply.send(Ok(values));
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

    #[test]
    fn shard_id_bounds_are_fixed_to_1024() {
        assert_eq!(ShardId::new(0).unwrap().as_u16(), 0);
        assert_eq!(ShardId::new(1023).unwrap().as_u16(), 1023);
        assert_eq!(
            ShardId::new(1024),
            Err(ShardEngineError::InvalidShard(1024))
        );
        assert_eq!(
            ShardId::new(u16::MAX),
            Err(ShardEngineError::InvalidShard(u16::MAX))
        );
    }

    #[test]
    fn xxh3_mapping_golden_vectors_are_stable() {
        // Upstream XXH3-64(seed=0) vectors:
        // empty => 0x2d06800538d394c2, low 10 bits => 194
        // "abc" => 0x78af5f94892f3950, low 10 bits => 336
        assert_eq!(shard_for_key(b"").as_u16(), 194);
        assert_eq!(shard_for_key(b"abc").as_u16(), 336);
    }

    #[test]
    fn shard_mapping_is_deterministic_and_bounded() {
        for i in 0_u64..10_000 {
            let mut key = Vec::with_capacity(24);
            key.extend_from_slice(b"homekv-key-");
            key.extend_from_slice(&i.to_le_bytes());

            let first = shard_for_key(&key);
            let second = shard_for_key(&key);
            assert_eq!(first, second);
            assert!(first.as_u16() < LOGICAL_SHARD_COUNT);
        }
    }

    #[tokio::test]
    async fn owner_engine_put_get_delete_round_trip() {
        let key = b"alpha".to_vec();
        let engine = ShardEngine::spawn(shard_for_key(&key), 16);
        assert_eq!(engine.queue_capacity(), 16);

        assert_eq!(engine.get(&key).await.unwrap(), None);

        engine.put(key.clone(), b"one".to_vec()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), Some(b"one".to_vec()));

        engine.put(key.clone(), b"two".to_vec()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), Some(b"two".to_vec()));

        engine.delete(key.clone()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), None);

        // DELETE is deterministic/idempotent for an absent key.
        engine.delete(key.clone()).await.unwrap();
        assert_eq!(engine.get(&key).await.unwrap(), None);
    }

    #[tokio::test]
    async fn engine_rejects_keys_owned_by_another_shard() {
        let key = b"owned-key".to_vec();
        let shard = shard_for_key(&key);
        let engine = ShardEngine::spawn(shard, 8);
        let other = key_for_different_shard(shard);
        let actual = shard_for_key(&other);

        assert_eq!(
            engine.put(other.clone(), b"v".to_vec()).await,
            Err(ShardEngineError::WrongShard {
                expected: shard,
                actual,
            })
        );
        assert_eq!(
            engine.get(&other).await,
            Err(ShardEngineError::WrongShard {
                expected: shard,
                actual,
            })
        );
    }

    #[test]
    fn batch_constructor_rejects_empty_and_cross_shard_commands() {
        let key = b"batch-key".to_vec();
        let shard = shard_for_key(&key);
        assert_eq!(
            ShardBatch::new(shard, Vec::new()),
            Err(ShardEngineError::EmptyBatch)
        );

        let other = key_for_different_shard(shard);
        let actual = shard_for_key(&other);
        assert_eq!(
            ShardBatch::new(
                shard,
                vec![Mutation::Put {
                    key: other,
                    value: b"v".to_vec(),
                }],
            ),
            Err(ShardEngineError::CrossShardBatch {
                expected: shard,
                actual,
            })
        );
    }

    #[tokio::test]
    async fn atomic_batch_applies_complete_state() {
        let seed = b"batch-seed".to_vec();
        let shard = shard_for_key(&seed);
        let keys = keys_for_shard(shard, 3);
        let engine = ShardEngine::spawn(shard, 16);

        let initial = ShardBatch::new(
            shard,
            keys.iter()
                .cloned()
                .map(|key| Mutation::Put {
                    key,
                    value: b"old".to_vec(),
                })
                .collect(),
        )
        .unwrap();
        engine.apply_batch(initial).await.unwrap();
        assert!(engine
            .snapshot(&keys)
            .await
            .unwrap()
            .iter()
            .all(|value| value.as_deref() == Some(b"old")));

        let replacement = ShardBatch::new(
            shard,
            keys.iter()
                .cloned()
                .map(|key| Mutation::Put {
                    key,
                    value: b"new".to_vec(),
                })
                .collect(),
        )
        .unwrap();
        engine.apply_batch(replacement).await.unwrap();
        assert!(engine
            .snapshot(&keys)
            .await
            .unwrap()
            .iter()
            .all(|value| value.as_deref() == Some(b"new")));
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
                .map(|key| Mutation::Put {
                    key,
                    value: b"a".to_vec(),
                })
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
                            .map(|key| Mutation::Put {
                                key,
                                value: value.to_vec(),
                            })
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
        let metrics = engine.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 1);
        assert_eq!(metrics.logical_bytes, a.len() + 3);
        assert_eq!(metrics.applied_mutations, 1);

        engine.put(a.clone(), b"12345".to_vec()).await.unwrap();
        let metrics = engine.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 1);
        assert_eq!(metrics.logical_bytes, a.len() + 5);
        assert_eq!(metrics.applied_mutations, 2);

        engine.delete(absent).await.unwrap();
        let metrics = engine.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 1);
        assert_eq!(metrics.logical_bytes, a.len() + 5);
        assert_eq!(metrics.applied_mutations, 3);

        let batch = ShardBatch::new(
            shard,
            vec![
                Mutation::Put {
                    key: b.clone(),
                    value: b"x".to_vec(),
                },
                Mutation::Put {
                    key: b.clone(),
                    value: b"longer".to_vec(),
                },
                Mutation::Delete { key: a.clone() },
            ],
        )
        .unwrap();
        engine.apply_batch(batch).await.unwrap();

        let metrics = engine.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 1);
        assert_eq!(metrics.logical_bytes, b.len() + b"longer".len());
        assert_eq!(metrics.applied_mutations, 6);
        assert_eq!(metrics.queue_capacity, 16);
        assert!(metrics.queue_depth <= metrics.queue_capacity);
        assert_eq!(metrics.overload_rejections, 0);
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
        right
            .put(right_key.clone(), b"right".to_vec())
            .await
            .unwrap();

        assert_eq!(left.get(&left_key).await.unwrap(), Some(b"left".to_vec()));
        assert_eq!(right.get(&right_key).await.unwrap(), Some(b"right".to_vec()));
    }

    #[test]
    fn mutation_representation_is_deterministic_data() {
        let a = Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        let b = Mutation::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        assert_eq!(a, b);
    }
}
