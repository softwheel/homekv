use std::collections::BTreeMap;

use super::node::HoneyBee;
use super::serialize::*;
use super::Version;

/// A digest is a piece of information summarizing
/// the staleness of one peer's data.
///
/// It is equivalent to a map: peer -> max version.
pub struct Digest {
    pub(crate) node_max_version: BTreeMap<HoneyBee, Version>,
}

impl Digest {
    #[cfg(test)]
    pub fn add_node(&mut self, node: HoneyBee, max_version: Version) {
        self.node_max_version.insert(node, max_version);
    }
}

impl HBSerializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_max_version.len() as u16).serialize(buf);
        for (node, version) in &self.node_max_version {
            node.serialize(buf);
            version.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_nodes = u16::deserialize(buf)?;
        let mut node_max_version: BTreeMap<HoneyBee, Version> = Default::default();
        for _ in 0..num_nodes {
            let node = HoneyBee::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            node_max_version.insert(node, version);
        }
        Ok(Digest { node_max_version })
    }

    fn serialized_len(&self) -> usize {
        let mut len = (self.node_max_version.len() as u16).serialized_len();
        for (node, _) in &self.node_max_version {
            len += node.serialized_len();
            len += node.serialized_len();
        }
        len
    }
}
