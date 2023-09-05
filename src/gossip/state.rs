use log::Level::Debug;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::net::SocketAddr;
use tokio::sync::watch;
use tokio::time::Instant;

use super::delta::{Delta, DeltaWriter};
use super::digest::Digest;
use super::node::Node;
use super::{Version, VersionedValue};

/// Maximum value size (in bytes) for a key-value item.
const MAX_KV_SIZE: usize = 500;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeState {
    pub(crate) key_values: BTreeMap<String, VersionedValue>,
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    last_heartbeat: Instant,
    pub(crate) max_version: u64,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            last_heartbeat: Instant::now(),
            max_version: Default::default(),
            key_values: Default::default(),
        }
    }
}

impl NodeState {
    /// Return an iterator over the version values that are older than `floor_version`.
    fn iter_stale_key_values(
        &self,
        floor_version: Version,
    ) -> impl Iterator<Item = (&str, &VersionedValue)> {
        // TODO optimize by checking the max version.
        self.key_values
            .iter()
            .filter(move |&(_key, versioned_value)| versioned_value.version > floor_version)
            .map(|(key, record)| (key.as_str(), record))
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.get_versioned(key)
            .map(|versioned_value| versioned_value.value.as_str())
    }

    pub fn get_versioned(&self, key: &str) -> Option<&VersionedValue> {
        self.key_values.get(key)
    }

    /// Sets a new value for a given key.
    ///
    /// Setting a new value automatically increases the version of the entire
    /// NodeState regardless of whether the value is really changed or not.
    pub fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) {
        let new_version = self.max_version + 1;
        self.set_with_version(key.to_string(), value.to_string(), new_version);
    }

    fn set_with_version(&mut self, key: String, value: String, version: Version) {
        assert!(version > self.max_version);
        let value_size = value.bytes().len();
        assert!(
            value_size <= MAX_KV_SIZE,
            "Value for key `{}` is too larget (actual: {}, maximum: {})",
            key,
            value_size,
            MAX_KV_SIZE
        );
        self.max_version = version;
        self.key_values
            .insert(key, VersionedValue { version, value });
    }
}

#[derive(Debug)]
pub(crate) struct ClusterState {
    pub(crate) node_states: BTreeMap<Node, NodeState>,
    seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
}

impl Default for ClusterState {
    fn default() -> Self {
        let (_seed_addrs_tx, seed_addrs_rx) = watch::channel(Default::default());
        Self {
            node_states: Default::default(),
            seed_addrs: seed_addrs_rx,
        }
    }
}

impl ClusterState {
    pub fn with_seed_addrs(seed_addrs: watch::Receiver<HashSet<SocketAddr>>) -> ClusterState {
        ClusterState {
            seed_addrs,
            node_states: BTreeMap::new(),
        }
    }

    pub(crate) fn node_state_mut(&mut self, node: &Node) -> &mut NodeState {
        self.node_states.entry(node.clone()).or_default()
    }

    pub fn node_state(&self, node: &Node) -> Option<&NodeState> {
        self.node_states.get(node)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.node_states.keys()
    }

    pub fn seed_addrs(&self) -> HashSet<SocketAddr> {
        self.seed_addrs.borrow().clone()
    }

    pub(crate) fn remove_node(&mut self, node: &Node) {
        self.node_states.remove(node);
    }

    pub(crate) fn apply_delta(&mut self, delta: Delta) {
        for (node, node_delta) in delta.node_deltas {
            let mut node_state_map = self
                .node_states
                .entry(node)
                .or_insert_with(NodeState::default);

            for (key, versioned_value) in node_delta.key_values {
                node_state_map.max_version =
                    node_state_map.max_version.max(versioned_value.version);
                let entry = node_state_map.key_values.entry(key);
                match entry {
                    Entry::Occupied(mut record) => {
                        if record.get().version >= versioned_value.version {
                            // Due to the message passing being totally asynchronous, it is not
                            // an error to receive updates that are already obsolete.
                            continue;
                        }
                        record.insert(versioned_value);
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(versioned_value);
                    }
                }
            }
            node_state_map.last_heartbeat = Instant::now();
        }
    }

    pub fn compute_digest(&self, dead_nodes: HashSet<&Node>) -> Digest {
        Digest {
            node_max_version: self
                .node_states
                .iter()
                .filter(|(node, _)| !dead_nodes.contains(node))
                .map(|(node, node_state)| (node.clone(), node_state.max_version))
                .collect(),
        }
    }

    /// Implements the scuttlebutt reconciliation with the scuttle-depth ordering.
    pub fn compute_delta(&self, digest: &Digest, mtu: usize, dead_nodes: HashSet<&Node>) -> Delta {
        let mut delta_writer = DeltaWriter::with_mtu(mtu);

        let mut node_sorted_by_stale_length = NodeSortedByStaleLength::default();
        for (node, node_state_map) in &self.node_states {
            if dead_nodes.contains(node) {
                continue;
            }
            let floor_version = digest.node_max_version.get(node).cloned().unwrap_or(0);
            let stale_kv_count = node_state_map.iter_stale_key_values(floor_version).count();
            if stale_kv_count > 0 {
                node_sorted_by_stale_length.insert(node, stale_kv_count);
            }
        }

        for node in node_sorted_by_stale_length.into_iter() {
            if !delta_writer.add_node(node.clone()) {
                break;
            }
            let node_state_map = self.node_states.get(node).unwrap();
            let floor_version = digest.node_max_version.get(node).cloned().unwrap_or(0);
            let mut stale_kvs: Vec<(&str, &VersionedValue)> = node_state_map
                .iter_stale_key_values(floor_version)
                .collect();
            assert!(!stale_kvs.is_empty());
            stale_kvs.sort_unstable_by_key(|(_, record)| record.version);
            for (key, versioned_value) in stale_kvs {
                if !delta_writer.add_kv(key, versioned_value.clone()) {
                    return delta_writer.into();
                }
            }
        }
        delta_writer.into()
    }
}

#[derive(Default)]
struct NodeSortedByStaleLength<'a> {
    node_per_stale_length: BTreeMap<usize, Vec<&'a Node>>,
    stale_lengths: BinaryHeap<usize>,
}

impl<'a> NodeSortedByStaleLength<'a> {
    fn insert(&mut self, node: &'a Node, stale_length: usize) {
        self.node_per_stale_length
            .entry(stale_length)
            .or_insert_with(|| {
                self.stale_lengths.push(stale_length);
                Vec::new()
            })
            .push(node)
    }

    fn into_iter(mut self) -> impl Iterator<Item = &'a Node> {
        let mut rng = random_generator();
        std::iter::from_fn(move || self.stale_lengths.pop()).flat_map(move |length| {
            let mut nodes = self.node_per_stale_length.remove(&length).unwrap();
            nodes.shuffle(&mut rng);
            nodes.into_iter()
        })
    }
}

#[cfg(not(test))]
fn random_generator() -> impl Rng {
    rand::thread_rng()
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterStateSnapshot {
    pub seed_addrs: HashSet<SocketAddr>,
    pub node_states: BTreeMap<String, NodeState>,
}

impl<'a> From<&'a ClusterState> for ClusterStateSnapshot {
    fn from(state: &'a ClusterState) -> Self {
        ClusterStateSnapshot {
            seed_addrs: state.seed_addrs(),
            node_states: state
                .node_states
                .iter()
                .map(|(node, node_state)| (node.id.clone(), node_state.clone()))
                .collect(),
        }
    }
}
