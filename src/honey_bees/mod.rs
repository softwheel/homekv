#![allow(clippy::type_complexity)]
#![allow(clippy::derive_partial_eq_without_eq)]

pub mod configuration;
pub mod delta;
pub mod digest;
pub mod failure_detector;
pub mod message;
pub mod node;
pub mod serialize;
pub mod server;
pub mod state;
pub mod transport;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::iter::Iterator;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tracing::{error, warn};

pub use configuration::GossipConfig;
pub use delta::Delta;
pub use digest::Digest;
pub use failure_detector::FailureDetector;
use message::{syn_ack_serialized_len, GossipMessage};
pub use node::HoneyBee;
use state::ClusterState;
pub use state::{ClusterStateSnapshot, NodeState};

/// Map key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

/// Maximum payload size (in bytes) for UDP.
///
/// Note 60KB typically won't fit in a UDP frame,
/// so long message will be sent over several frame.
///
/// We pick a large MTU because at the moment we send
/// the self digest "in full".
/// A frame of 1400B would limit us to 20 nodes or so.
const MTU: usize = 60_000;

pub type Version = u64;

/// A versioned value for a given Key-Value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
}

pub struct HoneyBees {
    config: GossipConfig,
    cluster_state: ClusterState,
    heartbeat: u64,
    failure_detector: FailureDetector,
    ready_nodes_watcher_tx: watch::Sender<HashSet<HoneyBee>>,
    ready_nodes_watcher_rx: watch::Receiver<HashSet<HoneyBee>>,
}

impl HoneyBees {
    pub fn with_node_id_and_seeds(
        config: GossipConfig,
        seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
        initial_key_values: Vec<(String, String)>,
    ) -> Self {
        let (ready_nodes_watcher_tx, ready_nodes_watcher_rx) = watch::channel(HashSet::new());
        let failure_detector = FailureDetector::new(config.failure_detector_config.clone());
        let mut honey_bees = HoneyBees {
            config,
            cluster_state: ClusterState::with_seed_addrs(seed_addrs),
            heartbeat: 0,
            failure_detector,
            ready_nodes_watcher_tx,
            ready_nodes_watcher_rx,
        };

        let self_node_state = honey_bees.self_node_state();

        // Instantly mark node as alive to ensure it responds to SYNs.
        self_node_state.set(HEARTBEAT_KEY, 0);

        for (key, value) in initial_key_values {
            self_node_state.set(key, value);
        }

        honey_bees
    }

    pub fn create_syn_message(&mut self) -> GossipMessage {
        let digest = self.compute_digset();
        GossipMessage::Syn {
            cluster_id: self.config.cluster_id.clone(),
            digest,
        }
    }

    /// Compute digest.
    ///
    /// This method also increments the heartbeat, to force the presence
    /// of at least one update, and have the node liveness propagated
    /// through the cluster.
    fn compute_digset(&mut self) -> Digest {
        // Ensure for every reply from this node, at least the heartbeat
        // is changed.
        let dead_nodes: HashSet<_> = self.dead_nodes().collect();
        self.cluster_state.compute_digest(dead_nodes)
    }

    pub fn process_message(&mut self, msg: GossipMessage) -> Option<GossipMessage> {
        match msg {
            GossipMessage::Syn { cluster_id, digest } => {
                if cluster_id != self.config.cluster_id {
                    warn!(
                        cluster_id = %cluster_id,
                        "rejecting syn message with mismatching cluster name"
                    );
                    return Some(GossipMessage::BadCluster);
                }
                let self_digest = self.compute_digest();
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let empty_delta = Delta::default();
                let delta_mtu = MTU - syn_ack_serialized_len(&self_digest, &empty_delta);
                let delta = self
                    .cluster_state
                    .compute_delta(&digest, delta_mtu, dead_nodes);
                self.report_to_failure_detector(&delta);
                Some(GossipMessage::SynAck {
                    digest: self_digest,
                    delta,
                })
            }
            GossipMessage::SynAck { digest, delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self
                    .cluster_state
                    .compute_delta(&digest, MTU - 1, dead_nodes);
                Some(GossipMessage::Ack { delta })
            }
            GossipMessage::Ack { delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
            GossipMessage::BadCluster => {
                warn!("message rejected by peer: cluster name mismatch");
                None
            }
        }
    }

    fn report_to_failure_detector(&mut self, delta: &Delta) {
        for (node, node_delta) in &delta.node_deltas {
            let local_max_version = self
                .cluster_state
                .node_states
                .get(node)
                .map(|node_state| node_state.max_version)
                .unwrap_or(0);
            let delta_max_version = node_delta.max_version();
            if local_max_version < delta_max_version {
                self.failure_detector.report_heartbeat(node);
            }
        }
    }

    /// Checks and marks nodes as dead / live / ready.
    pub fn update_nodes_liveness(&mut self) {
        let cluster_nodes = self
            .cluster_state
            .nodes()
            .filter(|&node| node != self.self_node())
            .collect::<Vec<_>>();
        for &node in &cluster_nodes {
            self.failure_detector.update_node_liveliness(node);
        }

        let ready_nodes_before = self.ready_nodes_watcher_rx.borrow().clone();
        let ready_nodes_after = self.ready_nodes().cloned().collect::<HashSet<_>>();
        if ready_nodes_before != ready_nodes_after {
            debub!(
                current_node = ?self.self_node(),
                live_nodes = ?ready_nodes_after,
                "nodes status changed"
            );
            if self.ready_nodes_watcher_tx.send(ready_nodes_after).is_err() {
                error!(
                    current_node = ?self.self_node_id(),
                    "error while reporting membership change event."
                )
            }
        }

        // Perform garbage collection.
        let garbage_collected_nodes = self.failure_detector.garbage_collect();
        for node in garbage_collected_nodes.iter() {
            self.cluster_state.remove_node(node);
        }
    }

    pub fn node_state(&self, node: &HoneyBee) -> Option<&NodeState> {
        self.cluster_state.node_state(node)
    }

    fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.config.node)
    }

    fn live_nodes(&self) -> impl Iterator<Item = &HoneyBee> {
        self.failure_detector.live_nodes()
    }

    fn ready_nodes(&self) -> impl Iterator<Item = &HoneyBee> {
        self.live_nodes().filter(|node| {
            let is_ready_pred = if let Some(pred) = self.config.is_ready_predicate.as_ref() {
                pred
            } else {
                // No predicate means that we consider all nodes as ready.
                return true;
            };
            self.node_state(node).map(is_ready_pred).unwrap_or(false)
        })
    }

    fn dead_nodes(&self) -> impl Iterator<Item = &HoneyBee> {
        self.failure_detector.dead_nodes()
    }

    fn seed_addrs(&self) -> HashSet<SocketAddr> {
        self.cluster_state.seed_addrs()
    }

    fn self_node(&self) -> &HoneyBee {
        &self.config.node
    }

    fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    fn update_heartbeat(&mut self) {
        self.heartbeat += 1;
        let heartbeat = self.heartbeat;
        self.self_node_state().set(HEARTBEAT_KEY, heartbeat);
    }

    fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }

    fn state_snapshot(&self) -> ClusterStateSnapshot {
        ClusterStateSnapshot::from(&self.cluster_state)
    }

    fn ready_nodes_watcher(&self) -> WatchStream<HashSet<HoneyBee>> {
        WatchStream::new(self.ready_nodes_watcher_rx.clone())
    }
}

#[derive(Debug)]
enum Command {
    Gossip(SocketAddr),
    Shutdown,
}

struct GossipHandle {
    node: HoneyBee,
    command_tx: UnboundedSender<Command>,
    honey_bees: Arc<Mutex<HoneyBees>>,
    join_handle: JoinHandle<Result<(), anyhow::Error>>,
}

impl GossipHandle {
    pub fn node(&self) -> &HoneyBee {
        &self.node
    }

    pub fn honey_bees(&self) -> Arc<Mutex<HoneyBees>> {
        self.honey_bees.clone()
    }

    pub async fn with_honey_bees<F, T>(&self, mut fun: F) -> T
    where
        F: FnMut(&mut HoneyBees) -> T,
    {
        let mut honey_bees = self.honey_bees.lock().await;
        fun(&mut honey_bees)
    }

    pub async fn shutdown(self) -> Result<(), anyhow::Error> {
        let _ = self.command_tx.send(Command::Shutdown);
        self.join_handle.await?
    }

    pub async fn gossip(&self, addr: SocketAddr) -> Result<(), anyhow::Error> {
        self.command_tx.send(Command::Gossip(addr))?;
        Ok(())
    }
}
