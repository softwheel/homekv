//! Spec 0006 §6 — routing and consistency histories (verification).
//!
//! Model-level histories over [`CommittedRouteResolver`] cover the mandatory
//! §6 scenarios: stale routes to a former replica, correct shard but wrong
//! member, correct member but stale/non-leader endpoint, route caches older
//! and newer than the locally committed catalog view, placement pending while
//! the old stable membership serves, data-group membership committed before
//! catalog publication, catalog publication with old-replica cleanup, and
//! redirect/retry after uncertain transport completion.
//!
//! A model ingress gate enforces the §7 dispatch rules
//! (REQ-M4-ROUTE-001..005): every raw key is re-hashed to its shard and
//! wrong-shard or cross-shard input is rejected; a node absent from the
//! committed voters receives `STALE_ROUTE_OR_NOT_OWNER` and never applies or
//! acknowledges a strong operation; route versions and hints are cache hints
//! only and never confer authority; pending movement intent never changes who
//! may serve; batches stay single-shard and idempotent with no cross-shard
//! atomicity advertised.
//!
//! Two end-to-end histories run the same rules against a real
//! [`PlacementNode`]: a stale resolver snapshotted from the committed catalog
//! is checked against the live committed view across a real movement, and
//! concurrent GET/SET/DELETE/batch histories on multiple shards pass a
//! per-shard linearizability check while routes refresh.

mod support;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::movement::LocalReplicaJanitor;
use homekv::placement::{
    next_phase, CatalogCommand, CatalogResponse, CatalogState, EligibleNode, MovementPhase,
    PlacementCatalog, PlacementEpoch,
};
use homekv::placement_node::{FsReplicaJanitor, PlacementNode, PlacementNodeConfig};
use homekv::raft::{RaftCommand, RaftMutation, RaftNodeId};
use homekv::routing::{CommittedRouteResolver, RouteDecision, RouteRedirect};
use homekv::storage::shard_engine::shard_for_key;
use tokio::sync::RwLock;

const CLUSTER_ID: [u8; 16] = *b"homekv-m4-route0";
const OP_ID: [u8; 16] = *b"m4-route-op-0001";

fn eligible(node_id: RaftNodeId) -> EligibleNode {
    EligibleNode {
        node_id,
        raft_endpoint: format!("127.0.0.1:{}", 31_000 + node_id),
        failure_domain: format!("fd-{node_id}"),
    }
}

fn bootstrapped_catalog() -> PlacementCatalog {
    let mut catalog = PlacementCatalog::default();
    let response = catalog
        .apply(CatalogCommand::Bootstrap {
            cluster_id: CLUSTER_ID,
            eligible_nodes: vec![eligible(1), eligible(2), eligible(3), eligible(4)],
        })
        .unwrap();
    assert_eq!(
        response,
        CatalogResponse::Initialized { placement_epoch: 1 }
    );
    catalog
}

/// A shard together with its committed voters and one eligible node that is
/// NOT among them (the warm standby for that shard).
fn shard_with_standby(catalog: &PlacementCatalog) -> (u16, [RaftNodeId; 3], RaftNodeId) {
    let state = catalog.state().expect("catalog bootstrapped");
    for (shard_id, placement) in &state.placements {
        if let Some(standby) = [1u64, 2, 3, 4]
            .into_iter()
            .find(|id| !placement.voters.contains(id))
        {
            return (*shard_id, placement.voters, standby);
        }
    }
    panic!("every RF=3 placement over 4 eligible nodes has a standby");
}

fn resolver_for(node_id: RaftNodeId, catalog: &PlacementCatalog) -> CommittedRouteResolver {
    CommittedRouteResolver::new(node_id, Arc::new(RwLock::new(catalog.clone())))
}

/// A deterministic key that hashes to `shard`: the model ingress gate
/// re-hashes every raw key and rejects wrong-shard input before routing, so
/// model tests must submit keys the target shard actually owns.
fn key_for_shard(shard: u16, prefix: &str) -> Vec<u8> {
    keys_for_shard(shard, 1, prefix).into_iter().next().unwrap()
}

/// Commit a full movement (intent through publication) at the catalog-model
/// level and return the new placement epoch.
fn publish_movement(
    catalog: &mut PlacementCatalog,
    shard_id: u16,
    target_voters: [RaftNodeId; 3],
) -> PlacementEpoch {
    let epoch = catalog
        .state()
        .expect("catalog bootstrapped")
        .placement_epoch;
    assert!(
        matches!(
            catalog
                .apply(CatalogCommand::BeginMovement {
                    expected_epoch: epoch,
                    operation_id: OP_ID,
                    shard_id,
                    target_voters,
                })
                .unwrap(),
            CatalogResponse::MovementAccepted {
                phase: MovementPhase::Intent
            }
        ),
        "begin movement must commit intent"
    );
    let mut phase = MovementPhase::Intent;
    while let Some(next) = next_phase(phase) {
        assert_eq!(
            catalog
                .apply(CatalogCommand::AdvanceMovementPhase {
                    expected_epoch: epoch,
                    operation_id: OP_ID,
                    shard_id,
                    phase: next,
                })
                .unwrap(),
            CatalogResponse::PhaseAdvanced { phase: next }
        );
        phase = next;
    }
    assert_eq!(phase, MovementPhase::Remove);
    match catalog
        .apply(CatalogCommand::PublishMovement {
            expected_epoch: epoch,
            operation_id: OP_ID,
            shard_id,
            observed_voters: target_voters,
        })
        .unwrap()
    {
        CatalogResponse::MovementPublished { placement_epoch } => placement_epoch,
        other => panic!("unexpected publish response: {other:?}"),
    }
}

/// Strong operations understood by the model ingress gate.
#[derive(Clone, Debug)]
enum ModelOp {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Batch { mutations: Vec<RaftMutation> },
}

impl ModelOp {
    fn keys(&self) -> Vec<&[u8]> {
        match self {
            ModelOp::Set { key, .. } | ModelOp::Delete { key } => vec![key.as_slice()],
            ModelOp::Batch { mutations } => mutations
                .iter()
                .map(|mutation| match mutation {
                    RaftMutation::Set { key, .. } => key.as_slice(),
                    RaftMutation::Delete { key } => key.as_slice(),
                })
                .collect(),
        }
    }
}

/// Outcome of submitting one operation to a node's model ingress gate.
#[derive(Debug)]
enum IngressOutcome {
    /// A committed voter served the operation locally: applied and acknowledged.
    Served { route_epoch: PlacementEpoch },
    /// A raw key hashed to a different shard: rejected before routing
    /// (REQ-M4-ROUTE-001). Nothing was applied anywhere.
    WrongShard { key: Vec<u8>, actual: u16 },
    /// STALE_ROUTE_OR_NOT_OWNER: advisory redirect; nothing applied or
    /// acknowledged locally (REQ-M4-ROUTE-002).
    StaleRouteOrNotOwner { redirect: RouteRedirect },
    /// No committed placement is known yet.
    Unavailable,
}

/// Model of one node's §7 ingress path: recompute the shard from every raw
/// key, resolve against the committed catalog view, and only then serve.
struct IngressGate {
    resolver: CommittedRouteResolver,
    /// Strong operations this node actually applied (and acknowledged).
    applied: Vec<ModelOp>,
    /// Redirects this node emitted, in order.
    redirects: Vec<RouteRedirect>,
}

impl IngressGate {
    fn new(node_id: RaftNodeId, catalog: &PlacementCatalog) -> Self {
        Self {
            resolver: resolver_for(node_id, catalog),
            applied: Vec::new(),
            redirects: Vec::new(),
        }
    }

    async fn submit(&mut self, shard_id: u16, op: ModelOp) -> IngressOutcome {
        for key in op.keys() {
            let actual = shard_for_key(key).as_u16();
            if actual != shard_id {
                return IngressOutcome::WrongShard {
                    key: key.to_vec(),
                    actual,
                };
            }
        }
        match self.resolver.resolve(shard_id).await {
            RouteDecision::Local { route_epoch } => {
                self.applied.push(op);
                IngressOutcome::Served { route_epoch }
            }
            RouteDecision::Redirect(redirect) => {
                // The redirect body is the wire form of
                // STALE_ROUTE_OR_NOT_OWNER (spec 0006 §7): it must round-trip.
                assert_eq!(
                    RouteRedirect::decode(&redirect.encode()),
                    Some(redirect.clone()),
                    "redirect body must round-trip"
                );
                self.redirects.push(redirect.clone());
                IngressOutcome::StaleRouteOrNotOwner { redirect }
            }
            RouteDecision::Unavailable => IngressOutcome::Unavailable,
        }
    }
}

fn set_op(key: &[u8], value: &[u8]) -> ModelOp {
    ModelOp::Set {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn delete_op(key: &[u8]) -> ModelOp {
    ModelOp::Delete { key: key.to_vec() }
}

fn batch_op(mutations: Vec<RaftMutation>) -> ModelOp {
    ModelOp::Batch { mutations }
}

/// Apply a model op to a sequential reference map (single-shard semantics).
fn apply_model(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: &ModelOp) {
    match op {
        ModelOp::Set { key, value } => {
            state.insert(key.clone(), value.clone());
        }
        ModelOp::Delete { key } => {
            state.remove(key);
        }
        ModelOp::Batch { mutations } => {
            for mutation in mutations {
                match mutation {
                    RaftMutation::Set { key, value } => {
                        state.insert(key.clone(), value.clone());
                    }
                    RaftMutation::Delete { key } => {
                        state.remove(key);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Model-level §6 histories
// ---------------------------------------------------------------------------

/// §6: stale client routes to a former replica. After publication the removed
/// replica's gate answers STALE_ROUTE_OR_NOT_OWNER with the new epoch and
/// applies nothing; the incoming replica serves.
#[tokio::test]
async fn stale_route_to_former_replica_redirects_and_never_applies() {
    let mut catalog = bootstrapped_catalog();
    let (shard, voters, standby) = shard_with_standby(&catalog);
    let removed = voters[2];
    let mut target = voters;
    target[2] = standby;
    target.sort_unstable();

    // The pre-publication catalog is the client's stale route cache.
    let stale_catalog = catalog.clone();
    let new_epoch = publish_movement(&mut catalog, shard, target);
    assert!(new_epoch > 1);

    // Under the stale cache the removed replica still looks authoritative:
    // this is exactly the hazard the redirect epoch lets clients detect.
    let mut stale_gate = IngressGate::new(removed, &stale_catalog);
    let key = key_for_shard(shard, "stale-route");
    assert!(
        matches!(
            stale_gate.submit(shard, set_op(&key, b"v")).await,
            IngressOutcome::Served { route_epoch: 1 }
        ),
        "stale cache predates the movement"
    );

    // Under the live committed view the former replica redirects and applies
    // nothing, for every strong operation kind.
    let mut live_gate = IngressGate::new(removed, &catalog);
    for op in [
        set_op(&key, b"v1"),
        delete_op(&key),
        batch_op(vec![
            RaftMutation::Set {
                key: key.clone(),
                value: b"v2".to_vec(),
            },
            RaftMutation::Delete { key: key.clone() },
        ]),
    ] {
        match live_gate.submit(shard, op).await {
            IngressOutcome::StaleRouteOrNotOwner { redirect } => {
                assert_eq!(redirect.route_epoch, new_epoch);
                assert_eq!(
                    redirect.leader_hint,
                    Some(
                        catalog
                            .state()
                            .unwrap()
                            .placements
                            .get(&shard)
                            .unwrap()
                            .desired_leader
                    ),
                    "redirect carries the advisory desired leader"
                );
            }
            other => panic!("former replica must redirect, got {other:?}"),
        }
    }
    assert!(
        live_gate.applied.is_empty(),
        "a former replica must never apply a strong operation"
    );
    assert_eq!(live_gate.redirects.len(), 3);

    // The incoming replica serves under the live view.
    let mut incoming_gate = IngressGate::new(standby, &catalog);
    assert!(matches!(
        incoming_gate.submit(shard, set_op(&key, b"v3")).await,
        IngressOutcome::Served { route_epoch } if route_epoch == new_epoch
    ));
    assert_eq!(incoming_gate.applied.len(), 1);
}

/// §6: correct shard but wrong member. An eligible node that is not a
/// committed voter redirects every strong operation and applies nothing.
#[tokio::test]
async fn wrong_member_for_correct_shard_never_applies() {
    let catalog = bootstrapped_catalog();
    let (shard, _voters, standby) = shard_with_standby(&catalog);
    let mut gate = IngressGate::new(standby, &catalog);
    let key = key_for_shard(shard, "wrong-member");

    for op in [
        set_op(&key, b"v1"),
        delete_op(&key),
        batch_op(vec![RaftMutation::Set {
            key: key.clone(),
            value: b"v2".to_vec(),
        }]),
    ] {
        assert!(
            matches!(
                gate.submit(shard, op).await,
                IngressOutcome::StaleRouteOrNotOwner { .. }
            ),
            "non-voter must redirect"
        );
    }
    assert!(gate.applied.is_empty());
    // Every redirect carries the committed epoch and an advisory leader hint.
    for redirect in &gate.redirects {
        assert_eq!(redirect.route_epoch, 1);
        assert_eq!(
            redirect.leader_hint,
            Some(catalog.state().unwrap().placements[&shard].desired_leader)
        );
    }
}

/// REQ-M4-ROUTE-003: hints and advisory endpoints never grant authority.
/// A forged redirect body claiming a newer epoch and naming the prober as
/// leader changes nothing: the resolver decides purely from the committed
/// view, which has no input channel for hints.
#[tokio::test]
async fn advisory_hints_never_grant_authority() {
    let catalog = bootstrapped_catalog();
    let (shard, _voters, standby) = shard_with_standby(&catalog);

    // A well-formed but forged hint: newer epoch, self as leader, self endpoint.
    let forged = homekv::routing::RouteRedirect {
        format_version: homekv::routing::ROUTE_REDIRECT_FORMAT_VERSION,
        route_epoch: 999,
        leader_hint: Some(standby),
        endpoint_hint: Some("127.0.0.1:31004".to_string()),
    };
    assert_eq!(
        RouteRedirect::decode(&forged.encode()),
        Some(forged.clone()),
        "the forged body is well-formed"
    );

    // The resolver has nowhere to receive the hint: the non-voter still
    // redirects, at the committed epoch, not the forged one.
    let decision = resolver_for(standby, &catalog).resolve(shard).await;
    match decision {
        RouteDecision::Redirect(redirect) => assert_eq!(redirect.route_epoch, 1),
        other => panic!("hint must not grant authority, got {other:?}"),
    }

    // Even the genuinely hinted desired leader redirects on a shard where it
    // is not a committed voter: hints describe, they do not authorize.
    let desired = catalog.state().unwrap().placements[&shard].desired_leader;
    let other = catalog
        .state()
        .unwrap()
        .placements
        .iter()
        .find(|(id, placement)| **id != shard && !placement.voters.contains(&desired))
        .map(|(id, placement)| (*id, placement.voters));
    if let Some((other_shard, _)) = other {
        let decision = resolver_for(desired, &catalog).resolve(other_shard).await;
        assert!(
            matches!(decision, RouteDecision::Redirect(_)),
            "desired-leader hint is not authority on shard {other_shard}"
        );
    }
}

/// §6: route cache older/newer than the locally committed catalog view.
/// The committed view is the only authority; older caches exhibit the stale
/// hazard and newer forged routes are ignored.
#[tokio::test]
async fn route_cache_older_and_newer_than_committed_view() {
    let mut catalog = bootstrapped_catalog();
    let (shard, voters, standby) = shard_with_standby(&catalog);
    let removed = voters[2];
    let mut target = voters;
    target[2] = standby;
    target.sort_unstable();

    // Older cache: snapshot bytes of the pre-movement committed view.
    let older_bytes = catalog.encode_snapshot().unwrap();
    let new_epoch = publish_movement(&mut catalog, shard, target);

    let older_catalog = PlacementCatalog::restore_snapshot(&older_bytes).unwrap();
    assert_eq!(
        resolver_for(removed, &older_catalog).resolve(shard).await,
        RouteDecision::Local { route_epoch: 1 },
        "older cache still routes to the removed replica"
    );

    // The committed view wins: the removed replica redirects at the new epoch.
    match resolver_for(removed, &catalog).resolve(shard).await {
        RouteDecision::Redirect(redirect) => assert_eq!(redirect.route_epoch, new_epoch),
        other => panic!("committed view must redirect the removed replica, got {other:?}"),
    }
    // The incoming replica is authoritative at the new epoch.
    assert_eq!(
        resolver_for(standby, &catalog).resolve(shard).await,
        RouteDecision::Local {
            route_epoch: new_epoch
        }
    );

    // Newer forged route: a body claiming epoch 999 confers nothing.
    let forged_newer = RouteRedirect {
        format_version: homekv::routing::ROUTE_REDIRECT_FORMAT_VERSION,
        route_epoch: 999,
        leader_hint: Some(removed),
        endpoint_hint: Some("127.0.0.1:31003".to_string()),
    };
    assert_eq!(
        RouteRedirect::decode(&forged_newer.encode()),
        Some(forged_newer)
    );
    assert!(
        matches!(
            resolver_for(removed, &catalog).resolve(shard).await,
            RouteDecision::Redirect(_)
        ),
        "a newer forged route must not override the committed view"
    );
}

/// REQ-M4-ROUTE-004 + §6: placement pending while the old stable membership
/// serves. The movement target is not authoritative in any phase before
/// publication; committed voters keep serving throughout.
#[tokio::test]
async fn pending_movement_confirms_no_target_authority() {
    let mut catalog = bootstrapped_catalog();
    let (shard, voters, standby) = shard_with_standby(&catalog);
    let mut target = voters;
    target[2] = standby;
    target.sort_unstable();

    let epoch = catalog.state().unwrap().placement_epoch;
    assert!(matches!(
        catalog
            .apply(CatalogCommand::BeginMovement {
                expected_epoch: epoch,
                operation_id: OP_ID,
                shard_id: shard,
                target_voters: target,
            })
            .unwrap(),
        CatalogResponse::MovementAccepted {
            phase: MovementPhase::Intent
        }
    ));

    let key = key_for_shard(shard, "pending-movement");
    let mut phase = MovementPhase::Intent;
    loop {
        // The pending target redirects in every phase before publication.
        let mut target_gate = IngressGate::new(standby, &catalog);
        assert!(
            matches!(
                target_gate.submit(shard, set_op(&key, b"v")).await,
                IngressOutcome::StaleRouteOrNotOwner { .. }
            ),
            "pending target must redirect in phase {phase:?}"
        );
        assert!(target_gate.applied.is_empty());

        // Committed voters keep serving through the transition.
        for voter in voters {
            let mut gate = IngressGate::new(voter, &catalog);
            assert!(
                matches!(
                    gate.submit(shard, set_op(&key, b"v")).await,
                    IngressOutcome::Served { route_epoch: 1 }
                ),
                "committed voter {voter} must serve in phase {phase:?}"
            );
        }

        match next_phase(phase) {
            Some(next) => {
                catalog
                    .apply(CatalogCommand::AdvanceMovementPhase {
                        expected_epoch: epoch,
                        operation_id: OP_ID,
                        shard_id: shard,
                        phase: next,
                    })
                    .unwrap();
                phase = next;
            }
            None => break,
        }
    }
    assert_eq!(phase, MovementPhase::Remove);
}

/// §6: membership committed before catalog publication. The data group may
/// have committed the new voter set, but routing authority still follows the
/// committed catalog view: the incoming node redirects and the old voters
/// serve until PublishMovement.
#[tokio::test]
async fn committed_membership_ahead_of_catalog_grants_no_authority() {
    let mut catalog = bootstrapped_catalog();
    let (shard, voters, standby) = shard_with_standby(&catalog);
    let mut target = voters;
    target[2] = standby;
    target.sort_unstable();

    // Drive the catalog to the last pre-publication phase: the data group has
    // committed the target membership (Remove phase runs against it), but the
    // catalog still records the old stable placement.
    let epoch = catalog.state().unwrap().placement_epoch;
    catalog
        .apply(CatalogCommand::BeginMovement {
            expected_epoch: epoch,
            operation_id: OP_ID,
            shard_id: shard,
            target_voters: target,
        })
        .unwrap();
    let mut phase = MovementPhase::Intent;
    while phase != MovementPhase::Remove {
        phase = next_phase(phase).unwrap();
        catalog
            .apply(CatalogCommand::AdvanceMovementPhase {
                expected_epoch: epoch,
                operation_id: OP_ID,
                shard_id: shard,
                phase,
            })
            .unwrap();
    }
    let state = catalog.state().unwrap();
    assert_eq!(
        state.placements[&shard]
            .pending_movement
            .as_ref()
            .unwrap()
            .phase,
        MovementPhase::Remove
    );
    // The stable placement is still the old voter set at the old epoch.
    assert_eq!(state.placements[&shard].voters, voters);
    assert_eq!(state.placement_epoch, epoch);

    // Routing authority is unchanged: the data-group-ahead node redirects.
    let key = key_for_shard(shard, "ahead");
    let mut incoming_gate = IngressGate::new(standby, &catalog);
    match incoming_gate.submit(shard, set_op(&key, b"v")).await {
        IngressOutcome::StaleRouteOrNotOwner { redirect } => {
            assert_eq!(redirect.route_epoch, epoch);
        }
        other => panic!("data-group membership alone grants nothing, got {other:?}"),
    }
    assert!(incoming_gate.applied.is_empty());

    let mut old_gate = IngressGate::new(voters[2], &catalog);
    assert!(matches!(
        old_gate.submit(shard, set_op(&key, b"v")).await,
        IngressOutcome::Served { .. }
    ));
}

/// §6: catalog publication and old-replica cleanup. After publication the new
/// voters serve, the removed replica redirects, and its local replica data is
/// cleaned up without touching other identities.
#[tokio::test]
async fn publish_reroutes_and_old_replica_cleanup() {
    let mut catalog = bootstrapped_catalog();
    let (shard, voters, standby) = shard_with_standby(&catalog);
    let removed = voters[2];
    let mut target = voters;
    target[2] = standby;
    target.sort_unstable();
    let new_epoch = publish_movement(&mut catalog, shard, target);

    let placement = &catalog.state().unwrap().placements[&shard];
    assert_eq!(placement.voters, target);
    assert!(placement.pending_movement.is_none());
    assert_eq!(placement.epoch, new_epoch);

    // New voters serve; the removed replica redirects on every op kind.
    let key = key_for_shard(shard, "publish");
    for voter in target {
        let mut gate = IngressGate::new(voter, &catalog);
        assert!(matches!(
            gate.submit(shard, set_op(&key, b"v")).await,
            IngressOutcome::Served { route_epoch } if route_epoch == new_epoch
        ));
    }
    let mut removed_gate = IngressGate::new(removed, &catalog);
    assert!(matches!(
        removed_gate.submit(shard, delete_op(&key)).await,
        IngressOutcome::StaleRouteOrNotOwner { .. }
    ));
    assert!(removed_gate.applied.is_empty());

    // Old-replica cleanup removes only the removed identity's replica dir.
    let root = std::env::temp_dir().join(format!(
        "homekv-m4-route-cleanup-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    for id in [1u64, 2, 3, 4] {
        std::fs::create_dir_all(
            root.join("groups")
                .join(format!("{shard:04}"))
                .join(format!("node-{id}.raft")),
        )
        .unwrap();
    }
    let janitor = FsReplicaJanitor::new(root.clone(), removed);
    assert_eq!(janitor.list_local_replicas().await.unwrap(), vec![shard]);
    janitor.remove_local_replica(shard).await.unwrap();
    assert!(!root
        .join("groups")
        .join(format!("{shard:04}"))
        .join(format!("node-{removed}.raft"))
        .exists());
    for id in [1u64, 2, 3, 4].into_iter().filter(|id| *id != removed) {
        assert!(
            root.join("groups")
                .join(format!("{shard:04}"))
                .join(format!("node-{id}.raft"))
                .exists(),
            "cleanup must not touch node {id}"
        );
    }
    assert!(janitor.list_local_replicas().await.unwrap().is_empty());
    std::fs::remove_dir_all(&root).unwrap();
}

/// REQ-M4-ROUTE-005: no cross-shard atomicity is advertised. A batch whose
/// keys span shards is rejected before routing and nothing is applied
/// anywhere; batches are single-shard and idempotent.
#[tokio::test]
async fn cross_shard_batch_rejected_no_atomicity_advertised() {
    let catalog = bootstrapped_catalog();
    let (shard_a, voters_a, _) = shard_with_standby(&catalog);
    let shard_b = catalog
        .state()
        .unwrap()
        .placements
        .keys()
        .find(|id| **id != shard_a)
        .copied()
        .expect("more than one shard");
    assert_ne!(shard_a, shard_b);

    // Find keys owned by each shard.
    let key_a = (0u64..)
        .map(|i| format!("xshard-a-{i}").into_bytes())
        .find(|k| shard_for_key(k).as_u16() == shard_a)
        .unwrap();
    let key_b = (0u64..)
        .map(|i| format!("xshard-b-{i}").into_bytes())
        .find(|k| shard_for_key(k).as_u16() == shard_b)
        .unwrap();

    let cross = batch_op(vec![
        RaftMutation::Set {
            key: key_a.clone(),
            value: b"1".to_vec(),
        },
        RaftMutation::Set {
            key: key_b.clone(),
            value: b"2".to_vec(),
        },
    ]);
    let mut gate = IngressGate::new(voters_a[0], &catalog);
    match gate.submit(shard_a, cross).await {
        IngressOutcome::WrongShard { key, actual } => {
            assert_eq!(key, key_b);
            assert_eq!(actual, shard_b);
        }
        other => panic!("cross-shard batch must be rejected, got {other:?}"),
    }
    assert!(
        gate.applied.is_empty(),
        "rejected cross-shard input applies nothing"
    );
}

/// §6: redirect/retry after uncertain transport completion, model level.
/// SET, DELETE and single-shard batches are retry-safe: applying the same
/// operation twice reaches the same state as applying it once.
#[tokio::test]
async fn retry_after_uncertain_completion_is_idempotent() {
    let catalog = bootstrapped_catalog();
    let (shard, voters, _) = shard_with_standby(&catalog);
    let mut gate = IngressGate::new(voters[0], &catalog);
    let key = key_for_shard(shard, "uncertain");

    let ops = [
        set_op(&key, b"v1"),
        set_op(&key, b"v1"), // duplicate delivery after uncertain completion
        delete_op(&key),
        delete_op(&key),
        batch_op(vec![
            RaftMutation::Set {
                key: key.clone(),
                value: b"v2".to_vec(),
            },
            RaftMutation::Delete { key: key.clone() },
        ]),
        batch_op(vec![
            RaftMutation::Set {
                key: key.clone(),
                value: b"v2".to_vec(),
            },
            RaftMutation::Delete { key: key.clone() },
        ]),
    ];
    let mut sequential = BTreeMap::new();
    for op in &ops {
        assert!(
            matches!(
                gate.submit(shard, op.clone()).await,
                IngressOutcome::Served { .. }
            ),
            "committed voter serves retries"
        );
        apply_model(&mut sequential, op);
    }
    // The retried sequence converges to the same state as the de-duplicated one.
    let mut deduped = BTreeMap::new();
    apply_model(&mut deduped, &ops[0]);
    apply_model(&mut deduped, &ops[2]);
    apply_model(&mut deduped, &ops[4]);
    assert_eq!(sequential, deduped);
    assert_eq!(sequential.get(&key), None);
}

// ---------------------------------------------------------------------------
// End-to-end §6 histories against a real PlacementNode
// ---------------------------------------------------------------------------

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("homekv-m4-{tag}-{}-{nonce}", std::process::id()))
}

async fn wait_for<F, Fut, T>(mut f: F, timeout: Duration, what: &str) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn node_config(tag: &str, cluster_id: [u8; 16], shards: Vec<u16>) -> PlacementNodeConfig {
    PlacementNodeConfig {
        node_ids: vec![1, 2, 3, 4],
        catalog_voters: [1, 2, 3],
        data_dir: unique_dir(tag),
        cluster_id,
        shards,
        drive_interval: Duration::from_millis(250),
        reconcile_interval: Duration::from_secs(1),
        election_min_ms: 150,
        election_max_ms: 300,
        ..PlacementNodeConfig::default()
    }
}

fn keys_for_shard(shard: u16, count: usize, prefix: &str) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(count);
    for i in 0..1_000_000u64 {
        let key = format!("{prefix}-{i}").into_bytes();
        if shard_for_key(&key).as_u16() == shard {
            keys.push(key);
            if keys.len() == count {
                break;
            }
        }
    }
    assert_eq!(
        keys.len(),
        count,
        "could not find {count} keys for shard {shard}"
    );
    keys
}

/// Routing-relevant projection of a committed catalog state: epoch, voters,
/// desired leaders and movement phase/targets. Retry counters and error notes
/// are excluded on purpose: they never affect routing authority.
type RoutingProjection = (
    PlacementEpoch,
    Vec<(
        u16,
        [RaftNodeId; 3],
        RaftNodeId,
        Option<(MovementPhase, [RaftNodeId; 3], [u8; 16])>,
    )>,
);

fn routing_projection(state: &CatalogState) -> RoutingProjection {
    let mut shards: Vec<_> = state
        .placements
        .iter()
        .map(|(id, placement)| {
            (
                *id,
                placement.voters,
                placement.desired_leader,
                placement.pending_movement.as_ref().map(|movement| {
                    (
                        movement.phase,
                        movement.target_voters,
                        movement.operation_id,
                    )
                }),
            )
        })
        .collect();
    shards.sort_by_key(|(id, ..)| *id);
    (state.placement_epoch, shards)
}

/// Test-side mirror of the node's committed catalog: replays the committed
/// transitions observed on the live node so resolvers can be built from a
/// real committed view at any point.
struct MirrorCatalog {
    catalog: PlacementCatalog,
}

impl MirrorCatalog {
    fn bootstrap(cluster_id: [u8; 16], eligible: Vec<EligibleNode>) -> Self {
        let mut catalog = PlacementCatalog::default();
        let response = catalog
            .apply(CatalogCommand::Bootstrap {
                cluster_id,
                eligible_nodes: eligible,
            })
            .unwrap();
        assert_eq!(
            response,
            CatalogResponse::Initialized { placement_epoch: 1 }
        );
        Self { catalog }
    }

    fn state(&self) -> &CatalogState {
        self.catalog.state().expect("mirror bootstrapped")
    }

    /// Replay committed transitions observed on the live node until the
    /// routing projection matches. Panics on unexpected divergence.
    fn sync_from(&mut self, live: &CatalogState) {
        for (shard_id, live_placement) in &live.placements {
            let mirror_placement = self
                .state()
                .placements
                .get(shard_id)
                .unwrap_or_else(|| panic!("mirror is missing shard {shard_id}"))
                .clone();
            match (
                mirror_placement.pending_movement,
                live_placement.pending_movement.clone(),
            ) {
                (None, None) => {
                    assert_eq!(
                        mirror_placement.voters, live_placement.voters,
                        "voter divergence on shard {shard_id}"
                    );
                    assert_eq!(
                        mirror_placement.desired_leader, live_placement.desired_leader,
                        "desired-leader divergence on shard {shard_id}"
                    );
                }
                (None, Some(pending)) => {
                    let epoch = self.state().placement_epoch;
                    self.catalog
                        .apply(CatalogCommand::BeginMovement {
                            expected_epoch: epoch,
                            operation_id: pending.operation_id,
                            shard_id: *shard_id,
                            target_voters: pending.target_voters,
                        })
                        .unwrap();
                    self.advance_to(*shard_id, pending.operation_id, pending.phase);
                }
                (Some(mirror_movement), Some(live_movement)) => {
                    assert_eq!(
                        mirror_movement.operation_id, live_movement.operation_id,
                        "operation id changed mid-movement on shard {shard_id}"
                    );
                    assert_eq!(mirror_movement.target_voters, live_movement.target_voters);
                    self.advance_to(*shard_id, live_movement.operation_id, live_movement.phase);
                }
                (Some(mirror_movement), None) => {
                    assert_eq!(
                        live_placement.voters, mirror_movement.target_voters,
                        "live voters must equal the movement target after publication on shard {shard_id}"
                    );
                    let epoch = self.state().placement_epoch;
                    let response = self
                        .catalog
                        .apply(CatalogCommand::PublishMovement {
                            expected_epoch: epoch,
                            operation_id: mirror_movement.operation_id,
                            shard_id: *shard_id,
                            observed_voters: mirror_movement.target_voters,
                        })
                        .unwrap();
                    assert!(
                        matches!(response, CatalogResponse::MovementPublished { .. }),
                        "unexpected publish response: {response:?}"
                    );
                }
            }
        }
        assert_eq!(
            routing_projection(self.state()),
            routing_projection(live),
            "mirror must track the live committed view"
        );
    }

    fn advance_to(&mut self, shard_id: u16, operation_id: [u8; 16], target: MovementPhase) {
        loop {
            let (epoch, phase) = {
                let state = self.state();
                let pending = state
                    .placements
                    .get(&shard_id)
                    .unwrap()
                    .pending_movement
                    .as_ref()
                    .unwrap();
                (state.placement_epoch, pending.phase)
            };
            if phase == target {
                return;
            }
            let next = next_phase(phase).unwrap_or_else(|| panic!("cannot advance past {phase:?}"));
            self.catalog
                .apply(CatalogCommand::AdvanceMovementPhase {
                    expected_epoch: epoch,
                    operation_id,
                    shard_id,
                    phase: next,
                })
                .unwrap();
        }
    }
}

/// §6: a stale resolver snapshotted from the committed catalog is checked
/// against the live committed view across a real movement. The stale cache
/// still routes to the removed replica (the hazard), while the live view
/// answers STALE_ROUTE_OR_NOT_OWNER with the new epoch and the removed
/// replica applies nothing.
#[tokio::test]
async fn stale_resolver_snapshot_checked_against_live_committed_view() {
    const SHARDS: [u16; 3] = [3, 11, 29];
    const MOVING: u16 = 11;
    const CLUSTER: [u8; 16] = *b"homekv-m4-route1";
    let node = PlacementNode::start(node_config("route-stale", CLUSTER, SHARDS.to_vec()))
        .await
        .expect("node starts");

    // The mirror deterministically reproduces the live committed catalog.
    // Wrapped in a mutex: the convergence poll below closes over it across
    // an await boundary, so the borrow cannot escape the closure body.
    let mirror = std::sync::Mutex::new(MirrorCatalog::bootstrap(
        CLUSTER,
        [1u64, 2, 3, 4].into_iter().map(eligible).collect(),
    ));
    let live = node.committed_state().await.expect("catalog readable");
    {
        let mirror = mirror.lock().unwrap();
        assert_eq!(
            mirror.state(),
            &live,
            "deterministic bootstrap must reproduce the live catalog"
        );
    }

    // Snapshot the pre-movement committed view: the client's stale route cache.
    let stale_bytes = mirror
        .lock()
        .unwrap()
        .catalog
        .encode_snapshot()
        .expect("snapshot encodes");

    // Baseline write through the elected leader, replicated to every voter.
    let source_voters = live.placements.get(&MOVING).unwrap().voters;
    let incoming = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !source_voters.contains(id))
        .expect("an eligible standby exists");
    let removed = source_voters[2];
    let mut target_voters = source_voters;
    target_voters[2] = incoming;
    target_voters.sort_unstable();

    let leader = wait_for(
        || async { node.group_leader(MOVING) },
        Duration::from_secs(15),
        "group leader",
    )
    .await;
    let key = keys_for_shard(MOVING, 1, "stale-view")
        .into_iter()
        .next()
        .unwrap();
    node.group_raft(MOVING, leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: key.clone(),
            value: b"before".to_vec(),
        })
        .await
        .expect("baseline write commits");
    wait_for(
        || async {
            let mut ok = true;
            for id in source_voters {
                let sm = node.group_state_machine(MOVING, id).expect("replica sm");
                if sm.get(&key).await != Some(b"before".to_vec()) {
                    ok = false;
                }
            }
            ok.then_some(())
        },
        Duration::from_secs(15),
        "baseline replication",
    )
    .await;

    // Commit a movement intent and let the drive loop converge it.
    let epoch = node
        .committed_state()
        .await
        .expect("catalog readable")
        .placement_epoch;
    let response = node
        .catalog()
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: epoch,
            operation_id: OP_ID,
            shard_id: MOVING,
            target_voters,
        })
        .await
        .expect("begin movement commits");
    assert!(
        matches!(response, CatalogResponse::MovementAccepted { .. }),
        "unexpected catalog response: {response:?}"
    );
    wait_for(
        || async {
            let live = node.committed_state().await.ok()?;
            mirror.lock().unwrap().sync_from(&live);
            let placement = live.placements.get(&MOVING)?;
            (placement.voters == target_voters && placement.pending_movement.is_none())
                .then_some(())
        },
        Duration::from_secs(60),
        "movement convergence",
    )
    .await;

    let live = node.committed_state().await.expect("catalog readable");
    mirror.lock().unwrap().sync_from(&live);
    let live_epoch = live.placement_epoch;
    assert!(live_epoch > epoch, "publication bumps the epoch");

    // The stale snapshot restores the pre-movement view.
    let stale_catalog =
        PlacementCatalog::restore_snapshot(&stale_bytes).expect("stale snapshot restores");
    assert_eq!(
        routing_projection(stale_catalog.state().unwrap()).0,
        epoch,
        "stale cache predates the movement"
    );

    // Stale resolver: still routes to the removed replica — the stale hazard
    // is real, which is why the redirect epoch matters.
    let stale_resolver = CommittedRouteResolver::new(removed, Arc::new(RwLock::new(stale_catalog)));
    assert_eq!(
        stale_resolver.resolve(MOVING).await,
        RouteDecision::Local { route_epoch: epoch }
    );

    // Live committed view: the removed replica redirects at the new epoch and
    // the incoming replica serves.
    let live_catalog = mirror.lock().unwrap().catalog.clone();
    match resolver_for(removed, &live_catalog).resolve(MOVING).await {
        RouteDecision::Redirect(redirect) => {
            assert_eq!(redirect.route_epoch, live_epoch);
            assert_eq!(
                redirect.leader_hint,
                Some(live.placements.get(&MOVING).unwrap().desired_leader),
                "redirect carries the advisory desired leader"
            );
            assert_eq!(
                RouteRedirect::decode(&redirect.encode()),
                Some(redirect),
                "redirect body is the STALE_ROUTE_OR_NOT_OWNER wire form"
            );
        }
        other => panic!("removed replica must redirect under the live view, got {other:?}"),
    }
    assert_eq!(
        resolver_for(incoming, &live_catalog).resolve(MOVING).await,
        RouteDecision::Local {
            route_epoch: live_epoch
        }
    );

    // End-to-end: a request sent to the removed replica under the live view is
    // rejected and applies nothing.
    let mut removed_gate = IngressGate::new(removed, &live_catalog);
    assert!(
        matches!(
            removed_gate.submit(MOVING, set_op(&key, b"after")).await,
            IngressOutcome::StaleRouteOrNotOwner { .. }
        ),
        "removed replica must answer STALE_ROUTE_OR_NOT_OWNER"
    );
    assert!(
        removed_gate.applied.is_empty(),
        "removed replica must never apply"
    );

    // The pre-movement write survived on the new voter set.
    for id in target_voters {
        let sm = node.group_state_machine(MOVING, id).expect("replica sm");
        assert_eq!(
            sm.get(&key).await,
            Some(b"before".to_vec()),
            "data preserved on node {id} after movement"
        );
    }

    node.shutdown();
}

// ---------------------------------------------------------------------------
// Concurrent multi-shard linearizable histories during route refresh
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum E2eOp {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Batch { mutations: Vec<RaftMutation> },
    Get { key: Vec<u8> },
}

#[derive(Clone, Debug)]
enum Transition {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    Batch {
        mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
    Read {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
}

#[derive(Debug)]
struct HistoryOp {
    invoke: u64,
    complete: u64,
    transition: Transition,
}

fn next_state(
    state: &BTreeMap<Vec<u8>, Vec<u8>>,
    transition: &Transition,
) -> Option<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut next = state.clone();
    match transition {
        Transition::Set { key, value } => {
            next.insert(key.clone(), value.clone());
        }
        Transition::Delete { key } => {
            next.remove(key);
        }
        Transition::Batch { mutations } => {
            for (key, value) in mutations {
                match value {
                    Some(value) => {
                        next.insert(key.clone(), value.clone());
                    }
                    None => {
                        next.remove(key);
                    }
                }
            }
        }
        Transition::Read { key, value } => {
            if next.get(key) != value.as_ref() {
                return None;
            }
        }
    }
    Some(next)
}

/// Sequential-model linearizability check (the M3 history pattern,
/// generalized to multi-key state with batches).
fn admits_linearization(history: &[HistoryOp]) -> bool {
    fn search(
        history: &[HistoryOp],
        used: &mut [bool],
        placed: usize,
        state: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> bool {
        if placed == history.len() {
            return true;
        }
        for candidate in 0..history.len() {
            if used[candidate] {
                continue;
            }
            let violates_real_time = (0..history.len()).any(|prior| {
                prior != candidate
                    && !used[prior]
                    && history[prior].complete < history[candidate].invoke
            });
            if violates_real_time {
                continue;
            }
            let Some(next) = next_state(state, &history[candidate].transition) else {
                continue;
            };
            used[candidate] = true;
            if search(history, used, placed + 1, &next) {
                return true;
            }
            used[candidate] = false;
        }
        false
    }
    let mut used = vec![false; history.len()];
    search(history, &mut used, 0, &BTreeMap::new())
}

/// Redirects observed while refreshing routes, by cause of the refresh.
#[derive(Debug)]
struct RedirectEvent {
    shard: u16,
    node: RaftNodeId,
    route_epoch: PlacementEpoch,
}

struct ClientCtx {
    node: Arc<PlacementNode>,
    mirror: tokio::sync::Mutex<MirrorCatalog>,
    clock: AtomicU64,
    ledger: tokio::sync::Mutex<Vec<RedirectEvent>>,
}

async fn write_with_retry(node: &PlacementNode, shard: u16, cmd: RaftCommand) {
    for _ in 0..120 {
        if let Some(leader) = node.group_leader(shard) {
            if let Some(raft) = node.group_raft(shard, leader) {
                if raft.client_write(cmd.clone()).await.is_ok() {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("write did not commit on shard {shard} after retries");
}

async fn read_with_retry(node: &PlacementNode, shard: u16, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..120 {
        if let Some(leader) = node.group_leader(shard) {
            if let Some(raft) = node.group_raft(shard, leader) {
                if raft.ensure_linearizable().await.is_ok() {
                    if let Some(sm) = node.group_state_machine(shard, leader) {
                        return sm.get(key).await;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("strong read did not complete on shard {shard} after retries");
}

/// Execute one op: route it against the committed view (refreshing the route
/// cache on redirect), then execute only once the route is Local. Execution
/// itself goes through the elected Raft leader, so only real leadership can
/// acknowledge a strong operation.
async fn execute_routed_op(
    ctx: &ClientCtx,
    cache: &mut BTreeMap<u16, RaftNodeId>,
    shard: u16,
    op: E2eOp,
) -> HistoryOp {
    let invoke = ctx.clock.fetch_add(1, Ordering::SeqCst);
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        assert!(attempts <= 30, "route did not converge for shard {shard}");
        let live = ctx.node.committed_state().await.expect("catalog readable");
        let (node_id, resolver) = {
            let mut mirror = ctx.mirror.lock().await;
            mirror.sync_from(&live);
            let node_id = *cache
                .entry(shard)
                .or_insert_with(|| live.placements.get(&shard).expect("shard placed").voters[0]);
            (
                node_id,
                CommittedRouteResolver::new(node_id, Arc::new(RwLock::new(mirror.catalog.clone()))),
            )
        };
        match resolver.resolve(shard).await {
            RouteDecision::Local { .. } => break,
            RouteDecision::Redirect(redirect) => {
                ctx.ledger.lock().await.push(RedirectEvent {
                    shard,
                    node: node_id,
                    route_epoch: redirect.route_epoch,
                });
                let live = ctx.node.committed_state().await.expect("catalog readable");
                let voters = live.placements.get(&shard).expect("shard placed").voters;
                // Refresh from the advisory hint; fall back to a voter.
                cache.insert(shard, redirect.leader_hint.unwrap_or(voters[0]));
            }
            RouteDecision::Unavailable => panic!("no committed placement for shard {shard}"),
        }
    }

    let transition = match op {
        E2eOp::Set { key, value } => {
            write_with_retry(
                &ctx.node,
                shard,
                RaftCommand::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
            )
            .await;
            Transition::Set { key, value }
        }
        E2eOp::Delete { key } => {
            write_with_retry(&ctx.node, shard, RaftCommand::Delete { key: key.clone() }).await;
            Transition::Delete { key }
        }
        E2eOp::Batch { mutations } => {
            let flat: Vec<(Vec<u8>, Option<Vec<u8>>)> = mutations
                .iter()
                .map(|mutation| match mutation {
                    RaftMutation::Set { key, value } => (key.clone(), Some(value.clone())),
                    RaftMutation::Delete { key } => (key.clone(), None),
                })
                .collect();
            write_with_retry(&ctx.node, shard, RaftCommand::Batch { mutations }).await;
            Transition::Batch { mutations: flat }
        }
        E2eOp::Get { key } => {
            let value = read_with_retry(&ctx.node, shard, &key).await;
            Transition::Read { key, value }
        }
    };
    let complete = ctx.clock.fetch_add(1, Ordering::SeqCst);
    HistoryOp {
        invoke,
        complete,
        transition,
    }
}

/// §6: concurrent GET/SET/DELETE/batch on multiple shards during route
/// refresh. Every completed per-shard history passes the linearizability
/// check against the sequential model; redirects observed mid-history carry
/// only advisory data and never acknowledge an operation.
#[tokio::test]
async fn concurrent_multi_shard_histories_linearize_during_route_refresh() {
    const SHARDS: [u16; 3] = [3, 11, 29];
    const MOVING: u16 = 11;
    const CLUSTER: [u8; 16] = *b"homekv-m4-route2";
    let node = Arc::new(
        PlacementNode::start(node_config("route-hist", CLUSTER, SHARDS.to_vec()))
            .await
            .expect("node starts"),
    );

    let mirror =
        MirrorCatalog::bootstrap(CLUSTER, [1u64, 2, 3, 4].into_iter().map(eligible).collect());
    let live = node.committed_state().await.expect("catalog readable");
    assert_eq!(
        mirror.state(),
        &live,
        "deterministic bootstrap must reproduce the live catalog"
    );
    let ctx = Arc::new(ClientCtx {
        node: Arc::clone(&node),
        mirror: tokio::sync::Mutex::new(mirror),
        clock: AtomicU64::new(0),
        ledger: tokio::sync::Mutex::new(Vec::new()),
    });

    for shard in SHARDS {
        wait_for(
            || async { node.group_leader(shard) },
            // 60s matches the other leader waits in this file: under
            // full-suite parallel load, elections on this VM can take
            // far longer than the 150-300ms election timeouts suggest.
            Duration::from_secs(60),
            "group leader",
        )
        .await;
    }

    let mut keys: BTreeMap<u16, Vec<Vec<u8>>> = BTreeMap::new();
    for shard in SHARDS {
        keys.insert(shard, keys_for_shard(shard, 2, "route-hist"));
    }

    // The movement plan: the route cache for the moving shard points at the
    // voter that will be removed, so post-publication traffic must redirect
    // and refresh.
    let source_voters = live.placements.get(&MOVING).unwrap().voters;
    let removed = source_voters[2];
    let incoming = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !source_voters.contains(id))
        .expect("an eligible standby exists");
    let mut target_voters = source_voters;
    target_voters[2] = incoming;
    target_voters.sort_unstable();
    node.catalog()
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: live.placement_epoch,
            operation_id: OP_ID,
            shard_id: MOVING,
            target_voters,
        })
        .await
        .expect("begin movement commits");

    // Wave 1: concurrent GET/SET/DELETE/batch on every shard while the drive
    // loop converges the movement.
    let mut tasks = Vec::new();
    for shard in SHARDS {
        let ks = keys[&shard].clone();
        for task_idx in 0..3usize {
            let ctx = Arc::clone(&ctx);
            let (k0, k1) = (ks[0].clone(), ks[1].clone());
            tasks.push(tokio::spawn(async move {
                let mut cache = BTreeMap::new();
                if shard == MOVING {
                    cache.insert(shard, removed);
                }
                let value = |i: usize| format!("s{shard}-t{task_idx}-o{i}").into_bytes();
                let script: Vec<E2eOp> = match task_idx {
                    0 => vec![
                        E2eOp::Set {
                            key: k0.clone(),
                            value: value(0),
                        },
                        E2eOp::Get { key: k0.clone() },
                        E2eOp::Delete { key: k1.clone() },
                        E2eOp::Get { key: k1.clone() },
                    ],
                    1 => vec![
                        E2eOp::Batch {
                            mutations: vec![
                                RaftMutation::Set {
                                    key: k0.clone(),
                                    value: value(0),
                                },
                                RaftMutation::Set {
                                    key: k1.clone(),
                                    value: value(1),
                                },
                            ],
                        },
                        E2eOp::Get { key: k0.clone() },
                        E2eOp::Set {
                            key: k1.clone(),
                            value: value(2),
                        },
                        E2eOp::Get { key: k1.clone() },
                    ],
                    _ => vec![
                        E2eOp::Delete { key: k0.clone() },
                        E2eOp::Set {
                            key: k0.clone(),
                            value: value(1),
                        },
                        E2eOp::Batch {
                            mutations: vec![RaftMutation::Delete { key: k1.clone() }],
                        },
                        E2eOp::Get { key: k0.clone() },
                    ],
                };
                let mut history = Vec::new();
                for op in script {
                    history.push(execute_routed_op(&ctx, &mut cache, shard, op).await);
                }
                (shard, history)
            }));
        }
    }
    let mut histories: BTreeMap<u16, Vec<HistoryOp>> = BTreeMap::new();
    for task in tasks {
        let (shard, history) = task.await.expect("client task");
        histories.entry(shard).or_default().extend(history);
    }

    // Let the movement converge; routes refresh to the new voters.
    wait_for(
        || async {
            let live = ctx.node.committed_state().await.ok()?;
            ctx.mirror.lock().await.sync_from(&live);
            let placement = live.placements.get(&MOVING)?;
            (placement.voters == target_voters && placement.pending_movement.is_none())
                .then_some(())
        },
        Duration::from_secs(60),
        "movement convergence",
    )
    .await;
    let post_epoch = ctx
        .node
        .committed_state()
        .await
        .expect("catalog readable")
        .placement_epoch;

    // Wave 2: the stale cache still points at the removed replica, so the
    // first routed op on the moved shard must redirect — at the new epoch —
    // and the cache refresh succeeds without further redirects. Stable
    // shards redirect never.
    for shard in SHARDS {
        let ks = keys[&shard].clone();
        let mut cache = BTreeMap::from([(MOVING, removed)]);
        let ledger_before = ctx.ledger.lock().await.len();
        for op in [
            E2eOp::Set {
                key: ks[0].clone(),
                value: b"wave2".to_vec(),
            },
            E2eOp::Get { key: ks[0].clone() },
        ] {
            let completed = execute_routed_op(&ctx, &mut cache, shard, op).await;
            histories.entry(shard).or_default().push(completed);
        }
        let ledger = ctx.ledger.lock().await;
        let shard_events: Vec<&RedirectEvent> = ledger[ledger_before..]
            .iter()
            .filter(|event| event.shard == shard)
            .collect();
        if shard == MOVING {
            assert!(
                !shard_events.is_empty(),
                "stale cache must redirect on the moved shard"
            );
            for event in shard_events {
                assert_eq!(event.node, removed);
                assert_eq!(event.route_epoch, post_epoch);
            }
        } else {
            assert!(
                shard_events.is_empty(),
                "stable shards must not redirect: {shard_events:?}"
            );
        }
    }

    // Every completed per-shard history linearizes against the sequential
    // model: reads see the last acknowledged write.
    for shard in SHARDS {
        let history = histories.get(&shard).expect("shard history");
        assert_eq!(history.len(), 14, "12 wave-1 + 2 wave-2 ops");
        assert!(
            admits_linearization(history),
            "shard {shard} history is not linearizable: {history:?}"
        );
    }

    // Redirects observed mid-history carried only advisory data: every event
    // names the epoch it was issued at, and none acknowledged an operation.
    for event in ctx.ledger.lock().await.iter() {
        assert!(
            event.route_epoch == 1 || event.route_epoch == post_epoch,
            "redirect epoch must be a committed catalog epoch: {event:?}"
        );
    }

    drop(ctx);
    match Arc::try_unwrap(node) {
        Ok(node) => node.shutdown(),
        Err(_) => panic!("no outstanding node references"),
    }
}

/// §6 + REQ-M4-ROUTE-002 (end-to-end): a wrong-owner or stale leader never
/// acknowledges or applies a strong operation. A follower's write is refused
/// with ForwardToLeader, a non-voter's write is refused outright, and only
/// the elected leader's write commits — to the committed voters only.
#[tokio::test]
async fn wrong_owner_or_stale_leader_never_acknowledges_strong_op() {
    const SHARD: u16 = 5;
    const CLUSTER: [u8; 16] = *b"homekv-m4-route3";
    let node = PlacementNode::start(node_config("route-owner", CLUSTER, vec![SHARD]))
        .await
        .expect("node starts");

    let live = node.committed_state().await.expect("catalog readable");
    let voters = live.placements.get(&SHARD).unwrap().voters;
    let leader = wait_for(
        || async { node.group_leader(SHARD) },
        Duration::from_secs(15),
        "group leader",
    )
    .await;
    let follower = voters
        .iter()
        .copied()
        .find(|id| *id != leader)
        .expect("a follower exists");
    let standby = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !voters.contains(id))
        .expect("an eligible standby exists");

    let key = keys_for_shard(SHARD, 1, "route-owner")
        .into_iter()
        .next()
        .unwrap();
    let write = || RaftCommand::Set {
        key: key.clone(),
        value: b"owner-proof".to_vec(),
    };

    // A follower is the wrong owner for this term: no acknowledgement.
    let follower_raft = node.group_raft(SHARD, follower).expect("follower raft");
    let err = follower_raft
        .client_write(write())
        .await
        .expect_err("a follower must not acknowledge a strong write");
    assert!(
        format!("{err:?}").contains("ForwardToLeader"),
        "follower must defer to the leader, got {err:?}"
    );
    assert_eq!(
        node.group_state_machine(SHARD, follower)
            .expect("follower sm")
            .get(&key)
            .await,
        None,
        "follower applied nothing"
    );

    // The warm standby is not a committed voter: no authority at all.
    let standby_raft = node.group_raft(SHARD, standby).expect("standby raft");
    assert!(
        standby_raft.client_write(write()).await.is_err(),
        "a non-voter must not acknowledge a strong write"
    );
    assert_eq!(
        node.group_state_machine(SHARD, standby)
            .expect("standby sm")
            .get(&key)
            .await,
        None,
        "non-voter applied nothing"
    );

    // The elected leader acknowledges; the write lands on every committed
    // voter and nowhere else.
    node.group_raft(SHARD, leader)
        .expect("leader raft")
        .client_write(write())
        .await
        .expect("leader write commits");
    wait_for(
        || async {
            let mut ok = true;
            for id in voters {
                let sm = node.group_state_machine(SHARD, id).expect("replica sm");
                if sm.get(&key).await != Some(b"owner-proof".to_vec()) {
                    ok = false;
                }
            }
            ok.then_some(())
        },
        Duration::from_secs(15),
        "write replication to committed voters",
    )
    .await;
    assert_eq!(
        node.group_state_machine(SHARD, standby)
            .expect("standby sm")
            .get(&key)
            .await,
        None,
        "non-voter never applies the committed write"
    );

    node.shutdown();
}
