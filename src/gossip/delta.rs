use std::collections::BTreeMap;
use std::mem;

use anyhow;

use super::node::Node;
use super::serialize::*;
use super::{Version, VersionedValue};

#[derive(Default, Eq, PartialEq, Debug)]
pub struct Delta {
    pub(crate) node_deltas: BTreeMap<Node, NodeDelta>,
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.node_deltas.len() as u16).serialize(buf);
        for (node, node_delta) in &self.node_deltas {
            node.serialize(buf);
            node_delta.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut node_deltas: BTreeMap<Node, NodeDelta> = Default::default();
        let num_nodes = u16::deserialize(buf)?;
        for _ in 0..num_nodes {
            let node = Node::deserialize(buf)?;
            let node_delta = NodeDelta::deserialize(buf)?;
            node_deltas.insert(node, node_delta);
        }
        Ok(Delta { node_deltas })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (node, node_delta) in &self.node_deltas {
            len += node.serialized_len();
            len += node_delta.serialized_len();
        }
        len
    }
}

#[derive(Serialize, Default, Eq, PartialEq, Debug)]
pub(crate) struct NodeDelta {
    pub key_values: BTreeMap<String, VersionedValue>,
}

impl NodeDelta {
    pub fn max_version(&self) -> Version {
        self.key_values.values()
            .map(|value| value.version)
            .max()
            .unwrap_or(0)
    }
}

pub struct DeltaWriter {
    delta: Delta,
    mtu: usize,
    num_bytes: usize,
    current_node: Option<Node>,
    current_node_delta: NodeDelta,
    reached_capacity: bool,
}


impl DeltaWriter {
    pub fn with_mtu(mtu: usize) -> Self {
        DeltaWriter {
            delta: Delta::default(),
            mtu,
            num_bytes: 2,
            current_node: None,
            current_node_delta: NodeDelta::default(),
            reached_capacity: false
        }
    }

    fn flush(&mut self) {
        let node_opt = mem::take(&mut self.current_node);
        let node_delta = mem::take(&mut self.current_node_delta);
        if let Some(node) = node_opt {
            self.delta.node_deltas.insert(node, node_delta);
        }
    }

    pub fn add_node(&mut self, node: Node) -> bool {
        assert!(Some(&node) != self.current_node.as_ref());
        assert!(!self.delta.node_deltas.contains_key(&node));
        self.flush();
        if !self.attempt_add_bytes(
            2 + node.id.len() + node.gossip_address.serialized_len() + 2
        ) {
            return false;
        }
        self.current_node = Some(node);
        true
    }

    fn attempt_add_bytes(&mut self, num_bytes: usize) -> bool {
        assert!(!self.reached_capacity);
        let new_num_bytes = self.num_bytes + num_bytes;
        if new_num_bytes > self.mtu {
            self.reached_capacity = true;
            return false;
        }
        self.num_bytes = new_num_bytes;
        true
    }

    pub fn add_kv(&mut self, key: &str, versioned_value: VersionedValue) -> bool {
        assert!(!self.current_node_delta.key_values.contains_key(key));
        if !self.attempt_add_bytes(2 + key.len() + 2 + versioned_value.value.len() + 8) {
            return false;
        }
        self.current_node_delta.key_values.insert(key.to_string(), versioned_value);
        true
    }
}

impl From<DeltaWriter> for Delta {
    fn from(mut delta_writer: DeltaWriter) -> Delta {
        delta_writer.flush();
        if cfg!(debug_assertions) {
            let mut buf = Vec::new();
            assert_eq!(delta_writer.num_bytes, delta_writer.delta.serialized_len());
            delta_writer.delta.serialize(&mut buf);
            assert_eq!(buf.len(), delta_writer.num_bytes);
        }
        delta_writer.delta
    }
}

impl Serializable for NodeDelta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.key_values.len() as u16).serialize(buf);
        for (key, VersionedValue { value, version }) in &self.key_values {
            key.serialize(buf);
            value.serialize(buf);
            version.serialize(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut key_values: BTreeMap<String, VersionedValue> = Default::default();
        let num_kvs: u16 = u16::deserialize(buf)?;
        for _ in 0..num_kvs {
            let key = String::deserialize(buf)?;
            let value = String::deserialize(buf)?;
            let version = u64::deserialize(buf)?;
            key_values.insert(key, VersionedValue { value, version });
        }
        Ok(NodeDelta { key_values })
    }

    fn serialized_len(&self) -> usize {
        let mut len = 2;
        for (key, VersionedValue { value, version }) in &self.key_values {
            len += key.serialized_len();
            len += value.serialized_len();
            len += version.serialized_len();
        }
        len
    }
}































































































