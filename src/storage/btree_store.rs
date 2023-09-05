use super::Store;
use crate::common::error::Result;

use std::collections::BTreeMap;

/// In-memory key-value store using the Rust standard library B-tree implementation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BTreeStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BTreeStore {
    /// Creates a new Memory key-value storage engine.
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Store for BTreeStore {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_vec(), value);
        Ok(())
    }
}
