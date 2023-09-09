use super::config::Config;
use super::index::Index;
use super::storage::Storage;

pub struct Segment {
    store: Storage,
    index: Index,
    base_offset: u64,
    next_offset: u64,
    config: Config,
}

impl Segment {
    fn new(dir: String, base_offset: u64, config: Config) -> Self {
        todo!()
    }

    fn append(record)
}
