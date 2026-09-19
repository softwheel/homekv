//! M4-T3: shard-aware routing over the committed placement catalog.
//!
//! The M2 adapter resolves every request against the committed catalog view
//! before dispatch (spec 0006 §7):
//!
//! 1. ingress recomputes the shard identity from every raw key and rejects
//!    wrong-shard or cross-shard input (REQ-M4-ROUTE-001);
//! 2. the committed [`PlacementCatalog`] view is read;
//! 3. a node absent from the committed voters receives
//!    `STALE_ROUTE_OR_NOT_OWNER` with the best known committed placement
//!    epoch and an advisory endpoint/leader hint, and the operation is
//!    never applied or acknowledged locally (REQ-M4-ROUTE-002);
//! 4. route versions and hints are cache hints only: they never confer
//!    read or write authority (REQ-M4-ROUTE-003);
//! 5. pending movement intent never changes who may serve; only committed
//!    voters are authoritative (REQ-M4-ROUTE-004);
//! 6. PUT, DELETE and single-shard batches stay retry-safe and idempotent;
//!    no exactly-once or cross-shard atomicity is claimed (REQ-M4-ROUTE-005).
//!
//! Leadership and read-barrier authority stay with the data group's
//! OpenRaft state machine: the resolver gates on committed membership only
//! and never treats the catalog's advisory desired leader as authority.

use std::sync::Arc;

use serde_derive::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::placement::{CatalogState, PlacementCatalog, PlacementEpoch};
use crate::raft::RaftNodeId;

/// Format version of the [`RouteRedirect`] body encoding.
pub const ROUTE_REDIRECT_FORMAT_VERSION: u16 = 1;

/// Advisory routing hint carried in a `StaleRouteOrNotOwner` response body.
///
/// The encoding lives entirely inside the opaque response body, so no
/// wire-version change is required (spec 0006 §7). Hints are cache hints
/// only and MUST NOT grant read or write authority (REQ-M4-ROUTE-003).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteRedirect {
    pub format_version: u16,
    /// Best known committed placement epoch.
    pub route_epoch: PlacementEpoch,
    /// Advisory desired leader for the shard's group, if known.
    pub leader_hint: Option<RaftNodeId>,
    /// Advisory endpoint for the hinted leader, if known.
    pub endpoint_hint: Option<String>,
}

impl RouteRedirect {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("RouteRedirect must stay bincode-serializable")
    }

    /// Decode a redirect body. Returns `None` for foreign format versions or
    /// malformed input; callers treat that as "no usable hint".
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let decoded: Self = bincode::deserialize(bytes).ok()?;
        if decoded.format_version == ROUTE_REDIRECT_FORMAT_VERSION {
            Some(decoded)
        } else {
            None
        }
    }
}

/// Routing decision for one request, derived from committed catalog state only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteDecision {
    /// This node is a committed voter for the shard's group: serve locally.
    /// Group-level OpenRaft authority still decides leader/read-barrier
    /// success for the operation itself.
    Local { route_epoch: PlacementEpoch },
    /// This node is not in the committed placement: redirect and never serve.
    Redirect(RouteRedirect),
    /// No committed placement is known: the node cannot route yet.
    Unavailable,
}

/// Resolves shard placement against the committed catalog view.
///
/// The catalog handle is shared with the catalog runtime, which publishes
/// committed views; the resolver only ever reads them.
#[derive(Clone)]
pub struct CommittedRouteResolver {
    node_id: RaftNodeId,
    catalog: Arc<RwLock<PlacementCatalog>>,
}

impl CommittedRouteResolver {
    pub fn new(node_id: RaftNodeId, catalog: Arc<RwLock<PlacementCatalog>>) -> Self {
        Self { node_id, catalog }
    }

    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    pub async fn resolve(&self, shard_id: u16) -> RouteDecision {
        let catalog = self.catalog.read().await;
        Self::decide(self.node_id, catalog.state(), shard_id)
    }

    fn decide(node_id: RaftNodeId, state: Option<&CatalogState>, shard_id: u16) -> RouteDecision {
        let state = match state {
            Some(state) => state,
            None => return RouteDecision::Unavailable,
        };
        let placement = match state.placements.get(&shard_id) {
            Some(placement) => placement,
            None => return RouteDecision::Unavailable,
        };
        // Committed voters only: pending movement intent never confers
        // authority (REQ-M4-ROUTE-004). The target set is deliberately ignored.
        if placement.voters.contains(&node_id) {
            return RouteDecision::Local {
                route_epoch: state.placement_epoch,
            };
        }
        let endpoint_hint = state
            .eligible_nodes
            .get(&placement.desired_leader)
            .map(|node| node.raft_endpoint.clone());
        RouteDecision::Redirect(RouteRedirect {
            format_version: ROUTE_REDIRECT_FORMAT_VERSION,
            route_epoch: state.placement_epoch,
            leader_hint: Some(placement.desired_leader),
            endpoint_hint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::{
        CatalogCommand, CatalogResponse, EligibleNode, MovementPhase, PendingMovement,
    };

    const CLUSTER_ID: [u8; 16] = *b"homekv-m4-t3test";

    fn eligible(node_id: RaftNodeId, endpoint: &str) -> EligibleNode {
        EligibleNode {
            node_id,
            raft_endpoint: endpoint.to_string(),
            failure_domain: "test".to_string(),
        }
    }

    fn bootstrap(nodes: Vec<EligibleNode>) -> PlacementCatalog {
        let mut catalog = PlacementCatalog::default();
        assert_eq!(
            catalog
                .apply(CatalogCommand::Bootstrap {
                    cluster_id: CLUSTER_ID,
                    eligible_nodes: nodes,
                })
                .unwrap(),
            CatalogResponse::Initialized { placement_epoch: 1 }
        );
        catalog
    }

    fn resolver(node_id: RaftNodeId, catalog: PlacementCatalog) -> CommittedRouteResolver {
        CommittedRouteResolver::new(node_id, Arc::new(RwLock::new(catalog)))
    }

    #[test]
    fn redirect_body_round_trips() {
        let redirect = RouteRedirect {
            format_version: ROUTE_REDIRECT_FORMAT_VERSION,
            route_epoch: 7,
            leader_hint: Some(3),
            endpoint_hint: Some("10.0.0.3:20001".to_string()),
        };
        assert_eq!(RouteRedirect::decode(&redirect.encode()), Some(redirect));
    }

    #[test]
    fn redirect_body_without_hints_round_trips() {
        let redirect = RouteRedirect {
            format_version: ROUTE_REDIRECT_FORMAT_VERSION,
            route_epoch: 1,
            leader_hint: None,
            endpoint_hint: None,
        };
        assert_eq!(RouteRedirect::decode(&redirect.encode()), Some(redirect));
    }

    #[test]
    fn decode_rejects_foreign_format_and_garbage() {
        let foreign = RouteRedirect {
            format_version: ROUTE_REDIRECT_FORMAT_VERSION + 1,
            route_epoch: 9,
            leader_hint: Some(1),
            endpoint_hint: None,
        };
        assert_eq!(RouteRedirect::decode(&foreign.encode()), None);
        assert_eq!(RouteRedirect::decode(&[]), None);
        assert_eq!(RouteRedirect::decode(b"not-a-redirect"), None);
    }

    #[tokio::test]
    async fn uninitialized_catalog_is_unavailable() {
        let decision = resolver(1, PlacementCatalog::default()).resolve(0).await;
        assert_eq!(decision, RouteDecision::Unavailable);
    }

    #[tokio::test]
    async fn committed_voter_resolves_local() {
        let catalog = bootstrap(vec![
            eligible(1, "n1"),
            eligible(2, "n2"),
            eligible(3, "n3"),
        ]);
        // Shard 0's committed voters are [1, 2, 3] with epoch 1.
        let decision = resolver(2, catalog).resolve(0).await;
        assert_eq!(decision, RouteDecision::Local { route_epoch: 1 });
    }

    #[tokio::test]
    async fn non_member_gets_redirect_with_epoch_and_hint() {
        let catalog = bootstrap(vec![
            eligible(1, "n1"),
            eligible(2, "n2"),
            eligible(3, "n3"),
        ]);
        let decision = resolver(99, catalog).resolve(0).await;
        match decision {
            RouteDecision::Redirect(redirect) => {
                assert_eq!(redirect.route_epoch, 1);
                // Desired leader for shard 0 is node 1; the hint is advisory.
                assert_eq!(redirect.leader_hint, Some(1));
                assert_eq!(redirect.endpoint_hint.as_deref(), Some("n1"));
                // The hint must decode back out of the response body form.
                assert_eq!(RouteRedirect::decode(&redirect.encode()), Some(redirect));
            }
            other => panic!("expected redirect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pending_movement_target_is_not_authoritative() {
        let catalog = bootstrap(vec![
            eligible(1, "n1"),
            eligible(2, "n2"),
            eligible(3, "n3"),
        ]);
        let mut state = catalog.state().unwrap().clone();
        state.placements.get_mut(&0).unwrap().pending_movement = Some(PendingMovement {
            operation_id: [9u8; 16],
            source_epoch: 1,
            source_voters: [1, 2, 3],
            target_voters: [1, 2, 4],
            phase: MovementPhase::CatchUp,
            retries: 0,
            last_error: None,
        });

        // Node 4 is the pending target: intent alone grants nothing.
        assert!(matches!(
            CommittedRouteResolver::decide(4, Some(&state), 0),
            RouteDecision::Redirect(_)
        ));
        // Committed voters keep serving through the transition.
        assert_eq!(
            CommittedRouteResolver::decide(1, Some(&state), 0),
            RouteDecision::Local { route_epoch: 1 }
        );
    }

    #[tokio::test]
    async fn route_decision_tracks_committed_view_only() {
        // A forged "newer" hint has no input channel: the resolver decides
        // purely from the committed view, so receiving hints can never grant
        // authority (REQ-M4-ROUTE-003).
        let catalog = bootstrap(vec![
            eligible(1, "n1"),
            eligible(2, "n2"),
            eligible(3, "n3"),
        ]);
        let forged = RouteRedirect {
            format_version: ROUTE_REDIRECT_FORMAT_VERSION,
            route_epoch: 999,
            leader_hint: Some(99),
            endpoint_hint: Some("evil".to_string()),
        };
        assert_eq!(RouteRedirect::decode(&forged.encode()), Some(forged));
        let decision = resolver(99, catalog).resolve(0).await;
        assert!(matches!(decision, RouteDecision::Redirect(_)));
    }
}
