mod config;
mod index;
mod segment;
mod storage;

use std::collections::HashMap;

use crate::common::error::Result;
use crate::common::Error;
use crate::storage::Store;

use segment::Segment;

/// Fragemented LSM key-value storage
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LSMStore {
    dir: String,
    mem_table: HashMap<Vec<u8>, u64>,
    active_segment: Segment,
    segments: Vec<Segment>,
}

impl LSMStore {
    pub fn new() -> Self {
        LSMStore {
            dir: String::from("/var/run/homekv/lsm_store/"),
            mem_table: HashMap::new(),
            active_segment: Segment::new(),
            segments: Vec::new(),
        }
    }

    fn get_offset(key: &[u8]) -> Result<Option<u64>> {
        todo!()
    }
}

impl Store for LSMStore {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(off) = self.get_offset(key) {
            self.active_segment.remove(off);
            Ok(())
        } else {
            Error::NotFound
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(off) = self.get_offset(key) {
            let s: Segment = Segment::new();
            let is_found: bool = false;
            for segment in self.segments {
                if segment.base_offset <= off && off < segment.next_offset {
                    s = segment;
                    is_found = true;
                    break;
                }
            }
            match is_found {
                true => Ok(s.read(off).cloned()),
                false => Error::Internal(format!(
                    "Internal Server Error: Key is found in mem_table, but offset is out of range: {}",
                    off
                )),
            }
        } else {
            Error::NotFound
        }
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.active_segment.append(key.to_vec(), value);
        Ok(())
    }
}
