use crate::consistent_hash::ConsistentHashNode;

use super::serialize::Serializable;
use serde::Serialize;
/// [`HoneyBee`] represents a node of a HoneyBees gossip cluster.
///
/// For the lifetime of a cluster, nodes can go down and back up,
/// they may permanently die. These are couple of issues we want
/// to solve with [`HoneyBee`] struct:
/// - We want a fresh local HomeTalk state for every run of a node.
/// - We don't want other nodes to override a newly started node state
///   with an obsolete state.
/// - We want other running nodes to detect that a newly started node's
///   state prevails over all its previous state.
/// - We want a node to advertise its own gossip address.
/// - We want a node to have an id that is the same across subsequent
///   runs for keeping cache data around as long as possible.
///
/// Our solution to this is:
/// - The `id` attribute which represents the node's unique identifier
///   in the cluster should be dynamic on every run. This easily solves
///   our first three requirements. The tradeoff is that starting nodes
///   need to always propagate their fresh state and old states are
///   never reclaimed.
/// - Having `gossip_address` attribute fulfills our forth requirement,
///   whose value is expected to be from a config item or an ENV VAR.
/// - Making part of the `id` attribute static and related to the node
///   solves the last requirement.
///
/// Because HomeTalk instance is not concerned about caching strategy
/// and what needs to be cached, we let the client decide what makes
/// up the `id` attribute and how to extract its components.
///
/// One such client's id could be `{node_unique_id}/{node_generation}/`.
/// - node_unique_id: a static unique name for the node.
/// - node_generation: a monotonically increasing value (timestamp on
///   every run)
///
/// Note: using timestamp to make the `id` dynamic has the potential
/// of reusing a previously used `id` in cases where the clock is reset
/// in the past. We believe this very rare and things should just work
/// fine.
use std::net::SocketAddr;

pub struct HoneyBee {
    // The unique id of this node in the cluster.
    pub id: String,
    // The SocketAddr other peers should use to communicate.
    pub gossip_address: SocketAddr,
    // liveness state
    pub is_alive: u16,
}

impl HoneyBee {
    pub fn new(id: String, gossip_address: SocketAddr) -> Self {
        Self {
            id,
            gossip_address,
            is_alive: 1,
        }
    }

    pub fn with_localhost_port(port: u16) -> Self {
        HoneyBee::new(
            format!("node-{port}"),
            ([127u8, 0u8, 0u8, 1u8], port).into(),
        )
    }

    #[cfg(test)]
    pub fn public_port(&self) -> u16 {
        self.gossip_address.port()
    }
}

impl Serializable for HoneyBee {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.id.serialize(buf);
        self.gossip_address.serialize(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let id = String::deserialize(buf)?;
        let gossip_address = SocketAddr::deserialize(buf)?;
        let is_alive = u16::deserialize(buf)?;
        Ok(HoneyBee {
            id,
            gossip_address,
            is_alive,
        })
    }

    fn serialized_len(&self) -> usize {
        self.id.serialized_len() + self.gossip_address.serialized_len()
    }
}

impl ConsistentHashNode for HoneyBee {
    fn name(&self) -> &str {
        &self.id
    }

    fn is_valid(&self) -> bool {
        self.is_alive == 1
    }
}
