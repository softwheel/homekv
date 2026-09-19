//! Bounded concurrent movement-work admission.
//!
//! Traceability:
//!
//! - `REQ-M4-GROUP-004`: concurrent movement work gets an explicit
//!   configurable count bound with observable overload/rejection. This is
//!   the admission primitive the M4-T4 movement reconciler and the M4-T5
//!   rebalancing scheduler (`REQ-M4-BAL-003` cluster-wide/per-node movement
//!   concurrency limits) will consume; snapshot-byte movement traffic is
//!   already bounded by `SharedPeerTransport`.
//! - `REQ-M4-GROUP-006`: the bound is enforced per node and cluster-wide,
//!   so movement work on one node can never create an unbounded queue or
//!   prevent unrelated nodes/groups from making progress.
//!
//! Admission is permit-based, mirroring `SharedPeerTransport`: a successful
//! `try_admit` returns a `MovementWorkPermit` holding one unit of movement
//! concurrency; dropping the permit releases it.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_derive::{Deserialize, Serialize};

use crate::raft::RaftNodeId;

/// Which movement-work admission scope a rejection refers to.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MovementWorkScope {
    Cluster,
    Node,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementWorkConfig {
    pub cluster_max_concurrent: usize,
    pub per_node_max_concurrent: usize,
}

impl MovementWorkConfig {
    pub fn validate(self) -> Result<Self, MovementWorkError> {
        if self.cluster_max_concurrent == 0 {
            return Err(MovementWorkError::InvalidClusterCapacity);
        }
        if self.per_node_max_concurrent == 0 {
            return Err(MovementWorkError::InvalidPerNodeCapacity);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MovementWorkError {
    InvalidClusterCapacity,
    InvalidPerNodeCapacity,
    Saturated {
        node_id: RaftNodeId,
        scope: MovementWorkScope,
    },
}

impl fmt::Display for MovementWorkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClusterCapacity => {
                write!(f, "cluster movement-work capacity must be non-zero")
            }
            Self::InvalidPerNodeCapacity => {
                write!(f, "per-node movement-work capacity must be non-zero")
            }
            Self::Saturated { node_id, scope } => write!(
                f,
                "movement-work {scope:?} concurrency is exhausted for node {node_id}"
            ),
        }
    }
}

impl std::error::Error for MovementWorkError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementWorkMetrics {
    pub cluster_max_concurrent: usize,
    pub per_node_max_concurrent: usize,
    pub cluster_inflight: usize,
    pub cluster_peak_inflight: usize,
    pub attempts: u64,
    pub rejections: u64,
    pub cluster_rejections: u64,
    pub node_rejections: u64,
    pub nodes: Vec<NodeMovementWorkMetrics>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeMovementWorkMetrics {
    pub node_id: RaftNodeId,
    pub inflight: usize,
    pub peak_inflight: usize,
    pub attempts: u64,
    pub rejections: u64,
}

#[derive(Clone)]
pub struct MovementWorkAdmission {
    inner: Arc<AdmissionInner>,
}

struct AdmissionInner {
    config: MovementWorkConfig,
    state: Mutex<AdmissionState>,
}

#[derive(Default)]
struct AdmissionState {
    cluster_inflight: usize,
    cluster_peak_inflight: usize,
    attempts: u64,
    cluster_rejections: u64,
    node_rejections: u64,
    nodes: BTreeMap<RaftNodeId, MutableNodeWork>,
}

#[derive(Default)]
struct MutableNodeWork {
    inflight: usize,
    peak_inflight: usize,
    attempts: u64,
    rejections: u64,
}

impl MovementWorkAdmission {
    pub fn new(config: MovementWorkConfig) -> Result<Self, MovementWorkError> {
        let config = config.validate()?;
        Ok(Self {
            inner: Arc::new(AdmissionInner {
                config,
                state: Mutex::new(AdmissionState::default()),
            }),
        })
    }

    /// Admit one unit of concurrent movement work on `node_id`.
    ///
    /// The returned permit holds one per-node and one cluster-wide slot;
    /// dropping it releases both. A saturated scope fails with an explicit
    /// `Saturated` error and an observable rejection counter; other nodes
    /// are unaffected.
    pub fn try_admit(&self, node_id: RaftNodeId) -> Result<MovementWorkPermit, MovementWorkError> {
        let mut state = lock_state(&self.inner.state);
        try_admit_inner(&mut state, &self.inner.config, node_id)?;
        Ok(MovementWorkPermit {
            inner: Arc::clone(&self.inner),
            node_id,
        })
    }

    pub fn metrics(&self) -> MovementWorkMetrics {
        let state = lock_state(&self.inner.state);
        MovementWorkMetrics {
            cluster_max_concurrent: self.inner.config.cluster_max_concurrent,
            per_node_max_concurrent: self.inner.config.per_node_max_concurrent,
            cluster_inflight: state.cluster_inflight,
            cluster_peak_inflight: state.cluster_peak_inflight,
            attempts: state.attempts,
            rejections: state.cluster_rejections + state.node_rejections,
            cluster_rejections: state.cluster_rejections,
            node_rejections: state.node_rejections,
            nodes: state
                .nodes
                .iter()
                .map(|(&node_id, node)| NodeMovementWorkMetrics {
                    node_id,
                    inflight: node.inflight,
                    peak_inflight: node.peak_inflight,
                    attempts: node.attempts,
                    rejections: node.rejections,
                })
                .collect(),
        }
    }
}

pub struct MovementWorkPermit {
    inner: Arc<AdmissionInner>,
    node_id: RaftNodeId,
}

impl fmt::Debug for MovementWorkPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MovementWorkPermit")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl Drop for MovementWorkPermit {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner.state);
        state.cluster_inflight = state.cluster_inflight.saturating_sub(1);
        if let Some(node) = state.nodes.get_mut(&self.node_id) {
            node.inflight = node.inflight.saturating_sub(1);
        }
    }
}

fn try_admit_inner(
    state: &mut AdmissionState,
    config: &MovementWorkConfig,
    node_id: RaftNodeId,
) -> Result<(), MovementWorkError> {
    state.attempts = state.attempts.saturating_add(1);
    let node = state.nodes.entry(node_id).or_default();
    node.attempts = node.attempts.saturating_add(1);
    if node.inflight >= config.per_node_max_concurrent {
        node.rejections = node.rejections.saturating_add(1);
        state.node_rejections = state.node_rejections.saturating_add(1);
        return Err(MovementWorkError::Saturated {
            node_id,
            scope: MovementWorkScope::Node,
        });
    }
    if state.cluster_inflight >= config.cluster_max_concurrent {
        node.rejections = node.rejections.saturating_add(1);
        state.cluster_rejections = state.cluster_rejections.saturating_add(1);
        return Err(MovementWorkError::Saturated {
            node_id,
            scope: MovementWorkScope::Cluster,
        });
    }
    node.inflight += 1;
    node.peak_inflight = node.peak_inflight.max(node.inflight);
    state.cluster_inflight += 1;
    state.cluster_peak_inflight = state.cluster_peak_inflight.max(state.cluster_inflight);
    Ok(())
}

fn lock_state(mutex: &Mutex<AdmissionState>) -> MutexGuard<'_, AdmissionState> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(cluster: usize, per_node: usize) -> MovementWorkConfig {
        MovementWorkConfig {
            cluster_max_concurrent: cluster,
            per_node_max_concurrent: per_node,
        }
    }

    #[test]
    fn capacities_are_explicit_and_non_zero() {
        assert!(matches!(
            config(0, 1).validate(),
            Err(MovementWorkError::InvalidClusterCapacity)
        ));
        assert!(matches!(
            config(1, 0).validate(),
            Err(MovementWorkError::InvalidPerNodeCapacity)
        ));
    }

    #[test]
    fn node_bound_rejects_explicitly_and_releases_on_drop() {
        let admission = MovementWorkAdmission::new(config(4, 1)).unwrap();
        let first = admission.try_admit(1).unwrap();
        assert!(matches!(
            admission.try_admit(1),
            Err(MovementWorkError::Saturated {
                node_id: 1,
                scope: MovementWorkScope::Node,
            })
        ));
        let metrics = admission.metrics();
        assert_eq!(metrics.cluster_inflight, 1);
        assert_eq!(metrics.node_rejections, 1);
        assert_eq!(metrics.cluster_rejections, 0);
        assert_eq!(metrics.nodes.len(), 1);
        assert_eq!(metrics.nodes[0].inflight, 1);
        assert_eq!(metrics.nodes[0].rejections, 1);

        drop(first);
        let _second = admission.try_admit(1).unwrap();
        assert_eq!(admission.metrics().cluster_inflight, 1);
        serde_json::to_string(&admission.metrics()).unwrap();
    }

    #[test]
    fn cluster_bound_is_independent_of_the_per_node_bound() {
        let admission = MovementWorkAdmission::new(config(2, 2)).unwrap();
        let _first = admission.try_admit(1).unwrap();
        let _second = admission.try_admit(2).unwrap();
        // Neither node hit its own bound, but the cluster is full.
        assert!(matches!(
            admission.try_admit(3),
            Err(MovementWorkError::Saturated {
                node_id: 3,
                scope: MovementWorkScope::Cluster,
            })
        ));
        let metrics = admission.metrics();
        assert_eq!(metrics.cluster_inflight, 2);
        assert_eq!(metrics.cluster_peak_inflight, 2);
        assert_eq!(metrics.cluster_rejections, 1);
        assert_eq!(metrics.attempts, 3);
    }

    #[test]
    fn saturated_node_does_not_block_an_unrelated_healthy_node() {
        let admission = MovementWorkAdmission::new(config(8, 1)).unwrap();
        let _hot = admission.try_admit(1).unwrap();
        assert!(matches!(
            admission.try_admit(1),
            Err(MovementWorkError::Saturated { .. })
        ));
        let _healthy = admission.try_admit(2).unwrap();
        let metrics = admission.metrics();
        assert_eq!(metrics.cluster_inflight, 2);
        assert_eq!(metrics.node_rejections, 1);
        let healthy = metrics.nodes.iter().find(|node| node.node_id == 2).unwrap();
        assert_eq!(healthy.inflight, 1);
        assert_eq!(healthy.rejections, 0);
    }
}
