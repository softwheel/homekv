#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::SocketAddr;
use std::time::Duration;

use super::failure_detector::FailureDetectorConfig;
use super::node::HoneyBee;
use super::state::NodeState;

pub struct GossipConfig {
    pub node: HoneyBee,
    pub cluster_id: String,
    pub gossip_interval: Duration,
    pub listen_addr: SocketAddr,
    pub seed_nodes: Vec<String>,
    pub failure_detector_config: FailureDetectorConfig,
    // `is_ready_predicate` makes it possible for a node to advertise itself as
    // not "ready". For instance, if it is `starting` or if it lost connection
    // to a third-party service.
    pub is_ready_predicate: Option<Box<dyn Fn(&NodeState) -> bool + Send>>,
}

impl GossipConfig {
    pub fn set_is_ready_predication(&mut self, pred: impl Fn(&NodeState) -> bool + Send + 'static) {
        self.is_ready_predicate = Some(Box::new(pred));
    }
}

impl Default for GossipConfig {
    fn default() -> Self {
        let node = HoneyBee::with_localhost_port(10_000);
        let listen_addr = node.gossip_address;
        Self {
            node,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(1_000),
            listen_addr,
            seed_nodes: Vec::new(),
            failure_detector_config: Default::default(),
            is_ready_predicate: None,
        }
    }
}
