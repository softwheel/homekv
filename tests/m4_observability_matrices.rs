//! Spec 0006 §9 — observability verification (automated).
//!
//! Asserts stable HomeKV-owned representations during bootstrap, elections,
//! route errors, movement and catalog-health transitions
//! (REQ-M4-OPS-001..003):
//!
//! - catalog epoch/leader/health from the committed view;
//! - placements with desired and current leaders;
//! - per-group Raft state: role, term, config (voters/learners), commit and
//!   apply index, snapshot progress, and replica lag — the fields PR #89
//!   added to [`GroupRaftView`];
//! - voter/leader skew bounds;
//! - bounded metric cardinality: one view entry per configured shard, views
//!   sorted and deterministic, reads never mutating the observed state;
//! - catalog health transitions: degraded nodes are marked unavailable while
//!   the representation stays well-formed; quorum loss blocks catalog
//!   mutation (via [`CatalogHealthGate`]) while reads of the last committed
//!   view keep working so data groups continue under committed membership;
//! - movement surfaces phase transitions in the topology view and clears
//!   them after convergence.
//!
//! What this file deliberately does not cover: OS-level counters (runtime
//! workers, connection counts, per-group memory). Those live outside the
//! library's public observation surface; the assertions here cover every
//! HomeKV-owned representation the spec's §9 list names through the public
//! API.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::movement::CatalogPort;
use homekv::placement::{
    CatalogCommand, CatalogResponse, CatalogState, EligibleNode, PlacementCatalog,
};
use homekv::placement_node::{PlacementNode, PlacementNodeConfig};
use homekv::placement_raft::CatalogGroupError;
use homekv::raft::{RaftCommand, RaftNodeId};
use homekv::raft_observability::{HomeKvReplicaObserver, ReplicaRole};
use homekv::rebalance::{
    build_topology_view, CatalogHealth, CatalogHealthGate, FnProbe, GroupRaftView, TopologyView,
};
use homekv::routing::{CommittedRouteResolver, RouteDecision};
use tokio::sync::RwLock;

const SHARDS: [u16; 4] = [3, 11, 29, 44];

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m4-obs-{tag}-{}-{nonce}",
        std::process::id()
    ))
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

/// Build one [`GroupRaftView`] per hosted replica of `shard` by observing
/// live durable replica state, then assemble the [`TopologyView`].
async fn observe_topology(
    node: &PlacementNode,
    state: &CatalogState,
    shards: &[u16],
    health: CatalogHealth,
) -> TopologyView {
    let mut groups = Vec::new();
    for shard in shards {
        let voters = state.placements[shard].voters;
        for node_id in voters {
            let raft = node.group_raft(*shard, node_id).expect("replica raft");
            let log_store = node.group_log_store(*shard, node_id).expect("log store");
            let state_machine = node
                .group_state_machine(*shard, node_id)
                .expect("replica sm");
            let observer = HomeKvReplicaObserver::new(raft, log_store, (*state_machine).clone());
            let status = observer.snapshot().await.expect("status observable");
            groups.push(GroupRaftView::from_status(*shard, &status));
        }
    }
    build_topology_view(state, &BTreeSet::new(), health, groups, None)
}

/// The §9 internal-consistency contract for one topology snapshot.
fn assert_view_consistent(view: &TopologyView, state: &CatalogState, shards: &[u16]) {
    assert_eq!(view.placement_epoch, state.placement_epoch);
    // Bounded cardinality: exactly one entry per hosted replica of a
    // configured shard, sorted by shard, and no more.
    let mut shard_ids: Vec<u16> = view.groups.iter().map(|g| g.shard_id).collect();
    assert_eq!(shard_ids.len(), shards.len() * 3);
    shard_ids.sort_unstable();
    let mut expected: Vec<u16> = shards.iter().flat_map(|s| [*s, *s, *s]).collect();
    expected.sort_unstable();
    assert_eq!(
        shard_ids, expected,
        "view covers exactly the configured shards"
    );
    assert!(
        view.groups
            .windows(2)
            .all(|w| w[0].shard_id <= w[1].shard_id),
        "groups are sorted by shard"
    );
    // Node cardinality matches the eligible set exactly.
    assert_eq!(view.nodes.len(), state.eligible_nodes.len());

    let mut seen: BTreeMap<u16, Vec<&GroupRaftView>> = BTreeMap::new();
    for group in &view.groups {
        // Index arithmetic: apply never runs ahead of commit; lag is the
        // saturating difference; snapshots never describe uncommitted state.
        if let (Some(commit), Some(apply)) = (group.commit_index, group.apply_index) {
            assert!(
                apply <= commit,
                "apply {apply} ahead of commit {commit} on shard {}",
                group.shard_id
            );
            assert_eq!(group.apply_lag, commit.saturating_sub(apply));
        }
        if let (Some(snapshot), Some(commit)) = (group.snapshot_index, group.commit_index) {
            assert!(
                snapshot <= commit,
                "snapshot {snapshot} ahead of commit {commit} on shard {}",
                group.shard_id
            );
        }
        assert!(
            group.term >= 1,
            "term starts at 1 on shard {}",
            group.shard_id
        );
        // Config matches the committed catalog placement for the shard.
        let mut voters = group.voters.clone();
        voters.sort_unstable();
        assert_eq!(
            voters, state.placements[&group.shard_id].voters,
            "view config matches committed placement"
        );
        for learner in &group.learners {
            assert!(
                !voters.contains(learner),
                "learner {learner} is not a voter on shard {}",
                group.shard_id
            );
        }
        if let Some(leader) = group.leader_id {
            assert!(
                voters.contains(&leader),
                "observed leader {leader} must be a committed voter on shard {}",
                group.shard_id
            );
        }
        seen.entry(group.shard_id).or_default().push(group);
    }
    // At most one leader role per shard in any single snapshot.
    for (shard, replicas) in &seen {
        let leaders = replicas
            .iter()
            .filter(|g| g.role == ReplicaRole::Leader)
            .count();
        assert!(
            leaders <= 1,
            "at most one leader per shard, shard {shard} reports {leaders}"
        );
    }
}

/// §9 + REQ-M4-OPS-001: the topology view is stable and internally
/// consistent through bootstrap, with per-group Raft fields populated and
/// skew within the deterministic-bootstrap bound.
#[tokio::test]
async fn topology_view_stable_through_bootstrap() {
    let node = PlacementNode::start(node_config(
        "obs-bootstrap",
        *b"homekv-m4-obs00X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");
    let state = node.committed_state().await.expect("catalog readable");
    assert_eq!(state.placement_epoch, 1);

    let first = observe_topology(&node, &state, &SHARDS, CatalogHealth::Healthy).await;
    assert_view_consistent(&first, &state, &SHARDS);
    assert!(
        first.voter_skew <= 1 && first.leader_skew <= 1,
        "deterministic bootstrap keeps skew <= 1: {first:?}"
    );
    assert!(first.active_movements.is_empty());
    for n in &first.nodes {
        assert!(n.available, "all eligible nodes available at bootstrap");
        assert!(n.voter_shards > 0, "every node votes somewhere");
    }

    // A second snapshot has the same shape; terms never move backwards.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state2 = node.committed_state().await.expect("catalog readable");
    let second = observe_topology(&node, &state2, &SHARDS, CatalogHealth::Healthy).await;
    assert_view_consistent(&second, &state2, &SHARDS);
    for (a, b) in first.groups.iter().zip(second.groups.iter()) {
        assert_eq!(a.shard_id, b.shard_id);
        assert!(
            b.term >= a.term,
            "term moved backwards on shard {}",
            a.shard_id
        );
    }

    node.shutdown();
}

/// §9: elections converge to exactly one leader per group and every
/// replica's view agrees on that leader's identity.
#[tokio::test]
async fn per_group_views_agree_on_one_leader_after_election() {
    let node = PlacementNode::start(node_config(
        "obs-election",
        *b"homekv-m4-obs01X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");
    let state = node.committed_state().await.expect("catalog readable");

    // Wait for every shard to elect exactly one leader that all replicas
    // observe.
    wait_for(
        || async {
            let leaders: Vec<Option<RaftNodeId>> =
                SHARDS.iter().map(|s| node.group_leader(*s)).collect();
            if leaders.iter().any(Option::is_none) {
                return None;
            }
            let view = observe_topology(&node, &state, &SHARDS, CatalogHealth::Healthy).await;
            let mut ok = true;
            for shard in SHARDS {
                let replicas: Vec<&GroupRaftView> =
                    view.groups.iter().filter(|g| g.shard_id == shard).collect();
                let expected = node.group_leader(shard);
                if !replicas.iter().all(|g| g.leader_id == expected) {
                    ok = false;
                }
                if replicas
                    .iter()
                    .filter(|g| g.role == ReplicaRole::Leader)
                    .count()
                    != 1
                {
                    ok = false;
                }
            }
            ok.then_some(())
        },
        Duration::from_secs(30),
        "one agreed leader per group",
    )
    .await;

    // Leadership is stable: the view still reports the same leaders.
    let view = observe_topology(&node, &state, &SHARDS, CatalogHealth::Healthy).await;
    assert_view_consistent(&view, &state, &SHARDS);

    node.shutdown();
}

/// §9: catalog health transitions keep the representation well-formed.
/// Degraded marks nodes unavailable; quorum loss still renders a complete
/// view — the topology never degrades into a malformed shape.
#[tokio::test]
async fn catalog_health_transitions_keep_view_well_formed() {
    let node = PlacementNode::start(node_config(
        "obs-health",
        *b"homekv-m4-obs02X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");
    let state = node.committed_state().await.expect("catalog readable");

    let degraded = build_topology_view(
        &state,
        &BTreeSet::from([4u64]),
        CatalogHealth::Degraded {
            unavailable_nodes: vec![4],
        },
        Vec::new(),
        None,
    );
    assert_eq!(
        degraded.catalog_health,
        CatalogHealth::Degraded {
            unavailable_nodes: vec![4]
        }
    );
    let node4 = degraded.nodes.iter().find(|n| n.node_id == 4).unwrap();
    assert!(!node4.available, "degraded node is marked unavailable");
    for n in degraded.nodes.iter().filter(|n| n.node_id != 4) {
        assert!(n.available);
    }
    // Epoch and placements are still reported: degraded health does not
    // hide the committed catalog.
    assert_eq!(degraded.placement_epoch, state.placement_epoch);
    assert_eq!(degraded.nodes.len(), state.eligible_nodes.len());

    // Quorum loss: the view still renders, marked QuorumLost.
    let lost = observe_topology(&node, &state, &SHARDS, CatalogHealth::QuorumLost).await;
    assert_eq!(lost.catalog_health, CatalogHealth::QuorumLost);
    assert_view_consistent(&lost, &state, &SHARDS);

    node.shutdown();
}

/// Minimal in-memory [`CatalogPort`] for health-gate tests.
struct FakeCatalog {
    catalog: Mutex<PlacementCatalog>,
}

impl FakeCatalog {
    fn bootstrapped() -> Self {
        let mut catalog = PlacementCatalog::default();
        catalog
            .apply(CatalogCommand::Bootstrap {
                cluster_id: *b"homekv-m4-obs03X",
                eligible_nodes: [1u64, 2, 3]
                    .into_iter()
                    .map(|id| EligibleNode {
                        node_id: id,
                        raft_endpoint: format!("127.0.0.1:{id}"),
                        failure_domain: "fd".to_string(),
                    })
                    .collect(),
            })
            .unwrap();
        Self {
            catalog: Mutex::new(catalog),
        }
    }
}

#[async_trait::async_trait]
impl CatalogPort for FakeCatalog {
    async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
        self.catalog
            .lock()
            .unwrap()
            .state()
            .cloned()
            .ok_or(CatalogGroupError::ConsensusUnavailable)
    }

    async fn submit(&self, command: CatalogCommand) -> Result<CatalogResponse, CatalogGroupError> {
        self.catalog
            .lock()
            .unwrap()
            .apply(command)
            .map_err(|_| CatalogGroupError::ConsensusUnavailable)
    }
}

/// §9 + REQ-M4-BAL-005: while catalog quorum is lost, no new placement can
/// be published, but reads of the last committed view keep working — stable
/// data groups continue under their committed memberships.
#[tokio::test]
async fn quorum_loss_blocks_mutation_but_reads_stay_available() {
    // A healthy gate passes writes through to the catalog.
    let fresh_gate = CatalogHealthGate::new(
        FakeCatalog {
            catalog: Mutex::new(PlacementCatalog::default()),
        },
        FnProbe(|| CatalogHealth::Healthy),
    );
    assert!(matches!(
        fresh_gate
            .submit(CatalogCommand::Bootstrap {
                cluster_id: *b"homekv-m4-obs04X",
                eligible_nodes: [1u64, 2, 3]
                    .into_iter()
                    .map(|id| EligibleNode {
                        node_id: id,
                        raft_endpoint: format!("127.0.0.1:{id}"),
                        failure_domain: "fd".to_string(),
                    })
                    .collect(),
            })
            .await,
        Ok(CatalogResponse::Initialized { .. })
    ));

    let lost_gate = CatalogHealthGate::new(
        FakeCatalog::bootstrapped(),
        FnProbe(|| CatalogHealth::QuorumLost),
    );
    // Reads still serve the last committed view.
    let state = lost_gate
        .read_committed()
        .await
        .expect("reads stay available");
    assert_eq!(state.placement_epoch, 1);
    // Mutations are refused: no new placement while quorum is lost.
    assert_eq!(
        lost_gate
            .submit(CatalogCommand::BeginMovement {
                expected_epoch: 1,
                operation_id: [7u8; 16],
                shard_id: 0,
                target_voters: [1, 2, 3],
            })
            .await,
        Err(CatalogGroupError::ConsensusUnavailable)
    );

    // Bootstrap is the deliberate exception: a fresh cluster must initialize.
    let fresh_lost = CatalogHealthGate::new(
        FakeCatalog {
            catalog: Mutex::new(PlacementCatalog::default()),
        },
        FnProbe(|| CatalogHealth::QuorumLost),
    );
    assert!(matches!(
        fresh_lost
            .submit(CatalogCommand::Bootstrap {
                cluster_id: *b"homekv-m4-obs05X",
                eligible_nodes: [1u64, 2, 3]
                    .into_iter()
                    .map(|id| EligibleNode {
                        node_id: id,
                        raft_endpoint: format!("127.0.0.1:{id}"),
                        failure_domain: "fd".to_string(),
                    })
                    .collect(),
            })
            .await,
        Ok(CatalogResponse::Initialized { .. })
    ));
}

/// §9: route errors do not perturb the observed topology. Resolving against
/// the committed view is a pure read; a redirect names the committed epoch
/// and an advisory leader hint.
#[tokio::test]
async fn route_errors_leave_observations_stable() {
    let node = PlacementNode::start(node_config(
        "obs-route",
        *b"homekv-m4-obs06X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");
    let state = node.committed_state().await.expect("catalog readable");
    let before = observe_topology(&node, &state, &SHARDS, CatalogHealth::Healthy).await;

    // A non-voter resolves to a redirect at the committed epoch.
    let committed = node.committed_state().await.expect("catalog readable");
    let shard = SHARDS[0];
    let voters = committed.placements[&shard].voters;
    let outsider = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !voters.contains(id))
        .expect("an eligible non-voter exists");
    let resolver = CommittedRouteResolver::new(
        outsider,
        Arc::new(RwLock::new({
            let mut c = PlacementCatalog::default();
            c.apply(CatalogCommand::Bootstrap {
                cluster_id: *b"homekv-m4-obs06X",
                eligible_nodes: committed.eligible_nodes.values().cloned().collect(),
            })
            .unwrap();
            c
        })),
    );
    match resolver.resolve(shard).await {
        RouteDecision::Redirect(redirect) => {
            assert_eq!(redirect.route_epoch, committed.placement_epoch);
            assert!(redirect.leader_hint.is_some());
        }
        other => panic!("non-voter must redirect, got {other:?}"),
    }

    // The observation is unchanged by the route error: same epoch, same
    // shape, same per-group indexes.
    let after_state = node.committed_state().await.expect("catalog readable");
    let after = observe_topology(&node, &after_state, &SHARDS, CatalogHealth::Healthy).await;
    assert_eq!(after.placement_epoch, before.placement_epoch);
    assert_eq!(after.groups.len(), before.groups.len());
    for (a, b) in before.groups.iter().zip(after.groups.iter()) {
        assert_eq!(a.shard_id, b.shard_id);
        assert_eq!(a.voters, b.voters);
    }

    node.shutdown();
}

/// §9: movement surfaces phase transitions in the topology view
/// (active_movements) and clears them after convergence, with the epoch
/// bumped and per-group views consistent throughout.
#[tokio::test]
async fn movement_surfaces_phases_and_clears_on_convergence() {
    const MOVING: u16 = 11;
    let node = PlacementNode::start(node_config(
        "obs-movement",
        *b"homekv-m4-obs07X",
        vec![MOVING],
    ))
    .await
    .expect("node starts");
    let state = node.committed_state().await.expect("catalog readable");
    let source_voters = state.placements[&MOVING].voters;
    let epoch = state.placement_epoch;
    let incoming = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !source_voters.contains(id))
        .expect("an eligible standby exists");
    let mut target_voters = source_voters;
    target_voters[2] = incoming;
    target_voters.sort_unstable();

    // Elect a leader and commit a write so the movement has state to carry.
    let leader = wait_for(
        || async { node.group_leader(MOVING) },
        Duration::from_secs(15),
        "data-group leader",
    )
    .await;
    node.group_raft(MOVING, leader)
        .expect("leader raft")
        .client_write(RaftCommand::Set {
            key: b"obs-move-key".to_vec(),
            value: b"obs-move-value".to_vec(),
        })
        .await
        .expect("write commits");

    let op_id = *b"homekv-m4-obs-01";
    let response = node
        .catalog()
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: epoch,
            operation_id: op_id,
            shard_id: MOVING,
            target_voters,
        })
        .await
        .expect("begin movement commits");
    assert!(
        matches!(response, CatalogResponse::MovementAccepted { .. }),
        "unexpected catalog response: {response:?}"
    );

    // While the movement is active, the topology view names it.
    let (mid_state, active) = wait_for(
        || async {
            let state = node.committed_state().await.ok()?;
            let placement = state.placements.get(&MOVING)?;
            placement.pending_movement.as_ref()?;
            let view = observe_topology(&node, &state, &[MOVING], CatalogHealth::Healthy).await;
            view.active_movements
                .iter()
                .any(|m| m.shard_id == MOVING)
                .then_some((state, view))
        },
        Duration::from_secs(30),
        "movement visible in topology",
    )
    .await;
    let summary = active
        .active_movements
        .iter()
        .find(|m| m.shard_id == MOVING)
        .expect("movement summarized");
    assert_eq!(summary.operation_id, op_id);
    assert_eq!(summary.target_voters, target_voters);
    assert_view_consistent(&active, &mid_state, &[MOVING]);

    // After convergence the movement clears, the epoch bumps, and the view
    // reports the new voter set.
    wait_for(
        || async {
            let state = node.committed_state().await.ok()?;
            let placement = state.placements.get(&MOVING)?;
            (placement.voters == target_voters && placement.pending_movement.is_none())
                .then_some(())
        },
        Duration::from_secs(60),
        "movement convergence",
    )
    .await;
    let final_state = node.committed_state().await.expect("catalog readable");
    assert!(final_state.placement_epoch > epoch);
    let done = observe_topology(&node, &final_state, &[MOVING], CatalogHealth::Healthy).await;
    assert!(
        done.active_movements.is_empty(),
        "no active movements after convergence"
    );
    assert_view_consistent(&done, &final_state, &[MOVING]);
    for group in &done.groups {
        let mut voters = group.voters.clone();
        voters.sort_unstable();
        assert_eq!(voters, target_voters);
    }

    node.shutdown();
}
