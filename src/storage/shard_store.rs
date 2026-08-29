use std::collections::BTreeMap;

use super::{shard_for_key, Mutation, ShardBatch, ShardEngine, ShardEngineError, ShardEngineResult, ShardId, LOGICAL_SHARD_COUNT};

pub const DEFAULT_SHARD_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardStoreMetrics {
    pub key_count: usize,
    pub logical_bytes: usize,
    pub applied_mutations: u64,
    pub overload_rejections: u64,
}

#[derive(Clone)]
pub struct ShardStore {
    shards: Vec<ShardEngine>,
}

impl ShardStore {
    pub fn spawn(queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0, "shard queue capacity must be positive");
        let shards = (0..LOGICAL_SHARD_COUNT)
            .map(|id| ShardEngine::spawn(ShardId::new(id).expect("bounded logical shard id"), queue_capacity))
            .collect();
        Self { shards }
    }

    pub fn spawn_default() -> Self {
        Self::spawn(DEFAULT_SHARD_QUEUE_CAPACITY)
    }

    fn shard(&self, key: &[u8]) -> &ShardEngine {
        &self.shards[shard_for_key(key).as_u16() as usize]
    }

    pub async fn get(&self, key: &[u8]) -> ShardEngineResult<Option<Vec<u8>>> {
        self.shard(key).get(key).await
    }

    pub async fn get_many(&self, keys: &[Vec<u8>]) -> ShardEngineResult<Vec<Option<Vec<u8>>>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key).await?);
        }
        Ok(values)
    }

    pub async fn set_many(&self, records: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> ShardEngineResult<()> {
        let mut by_shard: BTreeMap<u16, Vec<Mutation>> = BTreeMap::new();
        for (key, value) in records {
            let shard_id = shard_for_key(&key).as_u16();
            let mutation = match value {
                Some(value) => Mutation::Put { key, value },
                None => Mutation::Delete { key },
            };
            by_shard.entry(shard_id).or_default().push(mutation);
        }
        for (shard_id, mutations) in by_shard {
            let shard_id = ShardId::new(shard_id)?;
            let batch = ShardBatch::new(shard_id, mutations)?;
            self.shards[shard_id.as_u16() as usize].apply_batch(batch).await?;
        }
        Ok(())
    }

    pub async fn delete_many(&self, keys: Vec<Vec<u8>>) -> ShardEngineResult<()> {
        self.set_many(keys.into_iter().map(|key| (key, None)).collect()).await
    }

    pub async fn metrics(&self) -> ShardEngineResult<ShardStoreMetrics> {
        let mut aggregate = ShardStoreMetrics {
            key_count: 0,
            logical_bytes: 0,
            applied_mutations: 0,
            overload_rejections: 0,
        };
        for shard in &self.shards {
            let metrics = shard.metrics().await?;
            aggregate.key_count += metrics.key_count;
            aggregate.logical_bytes += metrics.logical_bytes;
            aggregate.applied_mutations += metrics.applied_mutations;
            aggregate.overload_rejections += metrics.overload_rejections;
        }
        Ok(aggregate)
    }

    pub async fn shutdown(&self) -> ShardEngineResult<()> {
        for shard in &self.shards {
            shard.shutdown().await?;
        }
        Ok(())
    }
}

impl Default for ShardStore {
    fn default() -> Self {
        Self::spawn_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_keys_same_shard() -> (Vec<u8>, Vec<u8>) {
        let first = b"adapter-key-a".to_vec();
        let target = shard_for_key(&first);
        for i in 0..100_000u32 {
            let candidate = format!("adapter-key-{i}").into_bytes();
            if candidate != first && shard_for_key(&candidate) == target {
                return (first, candidate);
            }
        }
        panic!("failed to find same-shard test key");
    }

    #[tokio::test]
    async fn existing_api_semantics_route_through_shards() {
        let store = ShardStore::spawn(8);
        let key_a = b"alpha".to_vec();
        let key_b = b"beta".to_vec();

        store
            .set_many(vec![
                (key_a.clone(), Some(b"one".to_vec())),
                (key_b.clone(), Some(b"two".to_vec())),
            ])
            .await
            .unwrap();

        assert_eq!(
            store.get_many(&[key_a.clone(), key_b.clone()]).await.unwrap(),
            vec![Some(b"one".to_vec()), Some(b"two".to_vec())]
        );

        store.delete_many(vec![key_a.clone()]).await.unwrap();
        assert_eq!(store.get(&key_a).await.unwrap(), None);
        assert_eq!(store.get(&key_b).await.unwrap(), Some(b"two".to_vec()));
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn same_shard_set_request_uses_atomic_batch_accounting() {
        let store = ShardStore::spawn(8);
        let (key_a, key_b) = two_keys_same_shard();
        let shard = shard_for_key(&key_a);

        store
            .set_many(vec![
                (key_a.clone(), Some(b"one".to_vec())),
                (key_b.clone(), Some(b"two".to_vec())),
            ])
            .await
            .unwrap();

        let metrics = store.shards[shard.as_u16() as usize].metrics().await.unwrap();
        assert_eq!(metrics.key_count, 2);
        assert_eq!(metrics.applied_mutations, 2);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn aggregate_metrics_match_public_server_counters() {
        let store = ShardStore::spawn(8);
        store
            .set_many(vec![
                (b"a".to_vec(), Some(b"123".to_vec())),
                (b"bb".to_vec(), Some(b"45".to_vec())),
            ])
            .await
            .unwrap();
        let metrics = store.metrics().await.unwrap();
        assert_eq!(metrics.key_count, 2);
        assert_eq!(metrics.logical_bytes, 1 + 3 + 2 + 2);
        store.shutdown().await.unwrap();
    }
}
