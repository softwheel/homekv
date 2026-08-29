pub mod btree_store;
pub mod mvcc;
pub mod shard_engine;

pub use btree_store::BTreeStore;
pub use mvcc::Mvcc;
pub use shard_engine::{
    shard_for_key, Mutation, ShardBatch, ShardEngine, ShardEngineError, ShardId, ShardMetrics,
};

use crate::common::Result;

pub trait Store {
    // Delete a key, or does nothing if it does not exist
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    // Gets value for a key, if exist
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    // Sets value for a key, replacing existing value if exist
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}
