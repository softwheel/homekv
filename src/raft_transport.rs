use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::raft::RaftNodeId;

pub const M3_VOTER_IDS: [RaftNodeId; 3] = [1, 2, 3];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapNode {
    pub id: RaftNodeId,
    pub raft_endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreeNodeBootstrap {
    pub cluster_id: String,
    pub nodes: BTreeMap<RaftNodeId, BootstrapNode>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExistingBootstrap {
    pub cluster_id: String,
    pub voter_ids: BTreeSet<RaftNodeId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapReconcile {
    Initialize,
    AlreadyCompatible,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapError {
    EmptyClusterId,
    WrongVoterCount { actual: usize },
    UnexpectedVoters { actual: BTreeSet<RaftNodeId> },
    EmptyEndpoint { node_id: RaftNodeId },
    DuplicateEndpoint { endpoint: String },
    ClusterIdentityMismatch { expected: String, actual: String },
    MembershipMismatch {
        expected: BTreeSet<RaftNodeId>,
        actual: BTreeSet<RaftNodeId>,
    },
    UnknownPeer { node_id: RaftNodeId },
    InvalidOutstandingRpcLimit,
    RpcBackpressure { node_id: RaftNodeId },
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyClusterId => write!(f, "cluster identity must not be empty"),
            Self::WrongVoterCount { actual } => {
                write!(f, "M3 requires exactly three voters, got {actual}")
            }
            Self::UnexpectedVoters { actual } => {
                write!(f, "M3 voter ids must be exactly {{1,2,3}}, got {actual:?}")
            }
            Self::EmptyEndpoint { node_id } => {
                write!(f, "raft endpoint for node {node_id} must not be empty")
            }
            Self::DuplicateEndpoint { endpoint } => {
                write!(f, "raft endpoint must be unique, duplicate {endpoint}")
            }
            Self::ClusterIdentityMismatch { expected, actual } => write!(
                f,
                "bootstrap cluster identity mismatch: expected {expected}, got {actual}"
            ),
            Self::MembershipMismatch { expected, actual } => write!(
                f,
                "bootstrap membership mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::UnknownPeer { node_id } => write!(f, "unknown M3 peer {node_id}"),
            Self::InvalidOutstandingRpcLimit => {
                write!(f, "per-peer outstanding RPC limit must be greater than zero")
            }
            Self::RpcBackpressure { node_id } => {
                write!(f, "per-peer RPC bound reached for node {node_id}")
            }
        }
    }
}

impl Error for BootstrapError {}

impl ThreeNodeBootstrap {
    pub fn new(
        cluster_id: impl Into<String>,
        nodes: impl IntoIterator<Item = BootstrapNode>,
    ) -> Result<Self, BootstrapError> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            return Err(BootstrapError::EmptyClusterId);
        }

        let nodes = nodes
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<BTreeMap<_, _>>();
        let bootstrap = Self { cluster_id, nodes };
        bootstrap.validate()?;
        Ok(bootstrap)
    }

    pub fn voter_ids(&self) -> BTreeSet<RaftNodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn validate(&self) -> Result<(), BootstrapError> {
        if self.nodes.len() != M3_VOTER_IDS.len() {
            return Err(BootstrapError::WrongVoterCount {
                actual: self.nodes.len(),
            });
        }

        let expected = M3_VOTER_IDS.into_iter().collect::<BTreeSet<_>>();
        let actual = self.voter_ids();
        if actual != expected {
            return Err(BootstrapError::UnexpectedVoters { actual });
        }

        let mut endpoints = BTreeSet::new();
        for node in self.nodes.values() {
            if node.raft_endpoint.trim().is_empty() {
                return Err(BootstrapError::EmptyEndpoint { node_id: node.id });
            }
            if !endpoints.insert(node.raft_endpoint.clone()) {
                return Err(BootstrapError::DuplicateEndpoint {
                    endpoint: node.raft_endpoint.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn reconcile(
        &self,
        existing: Option<&ExistingBootstrap>,
    ) -> Result<BootstrapReconcile, BootstrapError> {
        let Some(existing) = existing else {
            return Ok(BootstrapReconcile::Initialize);
        };

        if existing.cluster_id != self.cluster_id {
            return Err(BootstrapError::ClusterIdentityMismatch {
                expected: self.cluster_id.clone(),
                actual: existing.cluster_id.clone(),
            });
        }

        let expected = self.voter_ids();
        if existing.voter_ids != expected {
            return Err(BootstrapError::MembershipMismatch {
                expected,
                actual: existing.voter_ids.clone(),
            });
        }

        Ok(BootstrapReconcile::AlreadyCompatible)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkRule {
    Pass,
    Drop,
    Delay(Duration),
}

#[derive(Clone, Debug, Default)]
pub struct TestLinkController {
    rules: Arc<RwLock<BTreeMap<(RaftNodeId, RaftNodeId), LinkRule>>>,
}

impl TestLinkController {
    pub fn set_rule(&self, from: RaftNodeId, to: RaftNodeId, rule: LinkRule) {
        self.rules
            .write()
            .expect("test link controller lock poisoned")
            .insert((from, to), rule);
    }

    pub fn partition_bidirectional(&self, a: RaftNodeId, b: RaftNodeId) {
        self.set_rule(a, b, LinkRule::Drop);
        self.set_rule(b, a, LinkRule::Drop);
    }

    pub fn heal(&self, from: RaftNodeId, to: RaftNodeId) {
        self.rules
            .write()
            .expect("test link controller lock poisoned")
            .remove(&(from, to));
    }

    pub fn heal_bidirectional(&self, a: RaftNodeId, b: RaftNodeId) {
        self.heal(a, b);
        self.heal(b, a);
    }

    pub fn rule(&self, from: RaftNodeId, to: RaftNodeId) -> LinkRule {
        self.rules
            .read()
            .expect("test link controller lock poisoned")
            .get(&(from, to))
            .copied()
            .unwrap_or(LinkRule::Pass)
    }
}

#[derive(Clone, Debug)]
pub struct PerPeerRpcLimiter {
    peers: Arc<BTreeMap<RaftNodeId, Arc<Semaphore>>>,
}

impl PerPeerRpcLimiter {
    pub fn new(
        bootstrap: &ThreeNodeBootstrap,
        max_outstanding_per_peer: usize,
    ) -> Result<Self, BootstrapError> {
        if max_outstanding_per_peer == 0 {
            return Err(BootstrapError::InvalidOutstandingRpcLimit);
        }

        let peers = bootstrap
            .nodes
            .keys()
            .copied()
            .map(|node_id| {
                (
                    node_id,
                    Arc::new(Semaphore::new(max_outstanding_per_peer)),
                )
            })
            .collect();
        Ok(Self {
            peers: Arc::new(peers),
        })
    }

    pub fn try_acquire(
        &self,
        node_id: RaftNodeId,
    ) -> Result<OwnedSemaphorePermit, BootstrapError> {
        let semaphore = self
            .peers
            .get(&node_id)
            .ok_or(BootstrapError::UnknownPeer { node_id })?
            .clone();
        semaphore
            .try_acquire_owned()
            .map_err(|_| BootstrapError::RpcBackpressure { node_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_bootstrap() -> ThreeNodeBootstrap {
        ThreeNodeBootstrap::new(
            "homekv-m3-test",
            [
                BootstrapNode {
                    id: 1,
                    raft_endpoint: "127.0.0.1:19001".to_string(),
                },
                BootstrapNode {
                    id: 2,
                    raft_endpoint: "127.0.0.1:19002".to_string(),
                },
                BootstrapNode {
                    id: 3,
                    raft_endpoint: "127.0.0.1:19003".to_string(),
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn bootstrap_requires_exact_stable_three_voter_topology() {
        let bootstrap = valid_bootstrap();
        assert_eq!(bootstrap.voter_ids(), BTreeSet::from([1, 2, 3]));

        let bad = ThreeNodeBootstrap::new(
            "homekv-m3-test",
            [
                BootstrapNode {
                    id: 1,
                    raft_endpoint: "a".to_string(),
                },
                BootstrapNode {
                    id: 2,
                    raft_endpoint: "b".to_string(),
                },
                BootstrapNode {
                    id: 4,
                    raft_endpoint: "c".to_string(),
                },
            ],
        );
        assert!(matches!(bad, Err(BootstrapError::UnexpectedVoters { .. })));
    }

    #[test]
    fn bootstrap_rejects_ambiguous_endpoints() {
        let bad = ThreeNodeBootstrap::new(
            "homekv-m3-test",
            [
                BootstrapNode {
                    id: 1,
                    raft_endpoint: "same".to_string(),
                },
                BootstrapNode {
                    id: 2,
                    raft_endpoint: "same".to_string(),
                },
                BootstrapNode {
                    id: 3,
                    raft_endpoint: "third".to_string(),
                },
            ],
        );
        assert!(matches!(bad, Err(BootstrapError::DuplicateEndpoint { .. })));
    }

    #[test]
    fn repeated_compatible_bootstrap_is_idempotent_and_mismatch_fails_closed() {
        let bootstrap = valid_bootstrap();
        assert_eq!(bootstrap.reconcile(None).unwrap(), BootstrapReconcile::Initialize);

        let existing = ExistingBootstrap {
            cluster_id: bootstrap.cluster_id.clone(),
            voter_ids: bootstrap.voter_ids(),
        };
        assert_eq!(
            bootstrap.reconcile(Some(&existing)).unwrap(),
            BootstrapReconcile::AlreadyCompatible
        );

        let incompatible = ExistingBootstrap {
            cluster_id: "different-cluster".to_string(),
            voter_ids: bootstrap.voter_ids(),
        };
        assert!(matches!(
            bootstrap.reconcile(Some(&incompatible)),
            Err(BootstrapError::ClusterIdentityMismatch { .. })
        ));
    }

    #[test]
    fn deterministic_link_controls_are_directional_and_healable() {
        let links = TestLinkController::default();
        links.set_rule(1, 2, LinkRule::Delay(Duration::from_millis(7)));
        assert_eq!(links.rule(1, 2), LinkRule::Delay(Duration::from_millis(7)));
        assert_eq!(links.rule(2, 1), LinkRule::Pass);

        links.partition_bidirectional(2, 3);
        assert_eq!(links.rule(2, 3), LinkRule::Drop);
        assert_eq!(links.rule(3, 2), LinkRule::Drop);
        links.heal_bidirectional(2, 3);
        assert_eq!(links.rule(2, 3), LinkRule::Pass);
        assert_eq!(links.rule(3, 2), LinkRule::Pass);
    }

    #[test]
    fn per_peer_rpc_limit_rejects_instead_of_queueing_unbounded_work() {
        let bootstrap = valid_bootstrap();
        let limiter = PerPeerRpcLimiter::new(&bootstrap, 1).unwrap();
        let first = limiter.try_acquire(2).unwrap();
        assert!(matches!(
            limiter.try_acquire(2),
            Err(BootstrapError::RpcBackpressure { node_id: 2 })
        ));
        drop(first);
        assert!(limiter.try_acquire(2).is_ok());
        assert!(matches!(
            limiter.try_acquire(99),
            Err(BootstrapError::UnknownPeer { node_id: 99 })
        ));
    }
}
