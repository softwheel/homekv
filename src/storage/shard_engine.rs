use std::collections::HashMap;
use std::fmt;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardEngineError {
    InvalidShard(u16),
    Closed,
    OwnerStopped,
}

impl fmt::Display for ShardEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardEngineError::InvalidShard(id) => {
                write!(f, "logical shard id {id} is outside 0..{LOGICAL_SHARD_COUNT}")
            }
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
        }
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    pub async fn get(&self, key: &[u8]) -> ShardEngineResult<Option<Vec<u8>>> {
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
}

async fn owner_loop(mut rx: mpsc::Receiver<Request>) {
    let mut data = HashMap::<Vec<u8>, Vec<u8>>::new();

    while let Some(request) = rx.recv().await {
        match request {
            Request::Get { key, reply } => {
                let value = data.get(&key).cloned();
                let _ = reply.send(Ok(value));
            }
            Request::Put {
                key,
                value,
                reply,
            } => {
                data.insert(key, value);
                let _ = reply.send(Ok(()));
            }
            Request::Delete { key, reply } => {
                data.remove(&key);
                let _ = reply.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let engine = ShardEngine::spawn(ShardId::new(7).unwrap(), 16);
        assert_eq!(engine.shard_id().as_u16(), 7);
        assert_eq!(engine.queue_capacity(), 16);

        assert_eq!(engine.get(b"alpha").await.unwrap(), None);

        engine
            .put(b"alpha".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(engine.get(b"alpha").await.unwrap(), Some(b"one".to_vec()));

        engine
            .put(b"alpha".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        assert_eq!(engine.get(b"alpha").await.unwrap(), Some(b"two".to_vec()));

        engine.delete(b"alpha".to_vec()).await.unwrap();
        assert_eq!(engine.get(b"alpha").await.unwrap(), None);

        // DELETE is deterministic/idempotent for an absent key.
        engine.delete(b"alpha".to_vec()).await.unwrap();
        assert_eq!(engine.get(b"alpha").await.unwrap(), None);
    }

    #[tokio::test]
    async fn independent_shards_do_not_share_state() {
        let left = ShardEngine::spawn(ShardId::new(1).unwrap(), 8);
        let right = ShardEngine::spawn(ShardId::new(2).unwrap(), 8);

        left.put(b"k".to_vec(), b"left".to_vec()).await.unwrap();
        right
            .put(b"k".to_vec(), b"right".to_vec())
            .await
            .unwrap();

        assert_eq!(left.get(b"k").await.unwrap(), Some(b"left".to_vec()));
        assert_eq!(right.get(b"k").await.unwrap(), Some(b"right".to_vec()));
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
