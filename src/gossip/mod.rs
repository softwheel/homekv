pub mod message;

use std::collections::HashSet;
use std::iter::Iterator;
use std::net::SocketAddr;
use tokio_stream::wrappers::WatchStream;

pub use message::GossipMessage;
pub use node::Node;
pub use state::{ClusterState, ClusterStateSnapshot, NodeState};
pub use delta::Delta;


pub trait Gossip {
    /// Create SYN message with cluster meta info and node state delta
    fn create_syn_message(&mut self) -> GossipMessage;

    /// Process different kinds of message
    fn process_message(&mut self, msg: GossipMessage) -> Option<GossipMessage>;

    /// Report node state delta to Failure Detector
    fn report_to_failure_detector(&mut self, delta: &Delta);

    /// Check and mark nodes as dead/live/ready.
    fn update_nodes_liveness(&mut self);

    /// Get state of a specific node
    fn node_state(&self, node_id: &NoteId) -> Option<&NodeState>;

    /// Get the mutable state of the node itself
    fn self_node_state(&mut self) -> &mut NodeState;

    /// Retrieve the list of all live nodes
    fn live_nodes(&self) -> impl Iterator<Item=&Node>;

    /// Retrieve the list of nodes that are ready.
    /// To be ready, a node has to be alive and pass the `is_ready_predicate`
    /// as defined in the Gossip configuration.
    fn ready_nodes(&self) -> impl Iterator<Item=&Node>;

    /// Retrieve the list of all dead nodes
    fn dead_nodes(&self) -> impl Iterator<Item=&Node>;

    /// Retrieve a list of seed node addresses
    fn seed_addrs(&self) -> HashSet<SocketAddr>;

    fn self_node_id(&self) -> &Node;

    fn cluster_id(&self) -> &str;

    fn update_heartbeat(&mut self);

    fn cluster_state(&self) -> &ClusterState;

    /// Return a serializable snapshot of the ClusterState
    fn state_snapshot(&self) -> &ClusterStateSnapshot;

    fn ready_nodes_watcher(&self) -> WatchStream<HashSet<Node>>;
}
