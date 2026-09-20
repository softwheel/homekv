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
//! What this file deliberately does not cover: timer counts (no stable Tokio
//! API without `tokio_unstable`), socket-level connection bytes (the
//! in-process transport has no sockets), catalog *health* (operator-computed
//! from probes, covered by the topology-view tests above), and
//! movement-attributed byte transfer (replication does not tag bytes as
//! movement vs. ordinary traffic; per-replica snapshot transfer bytes are
//! reported instead). Those gaps are documented, not invented, in
//! [`homekv::placement_metrics`].

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::movement::CatalogPort;
use homekv::placement::{
    CatalogCommand, CatalogResponse, CatalogState, EligibleNode, PlacementCatalog,
};
use homekv::placement_metrics::PlacementMetrics;
use homekv::placement_node::{PlacementNode, PlacementNodeConfig};
use homekv::placement_raft::CatalogGroupError;
use homekv::raft::{RaftCommand, RaftNodeId};
use homekv::raft_observability::{HomeKvReplicaObserver, ReplicaRole};
use homekv::rebalance::{
    build_topology_view, CatalogHealth, CatalogHealthGate, FnProbe, GroupRaftView, TopologyView,
};
use homekv::routing::{CommittedRouteResolver, RedirectCause, RouteDecision};
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

    // Stabilize: every hosted replica must have voted at least once
    // (term >= 1) before the well-formedness assertions below, which
    // require it. Under parallel-suite load the first election can lag
    // behind node startup; a term-0 replica is legitimate transient
    // state, not a malformed view.
    wait_for(
        || async {
            let view = observe_topology(&node, &state, &SHARDS, CatalogHealth::Healthy).await;
            view.groups.iter().all(|g| g.term >= 1).then_some(())
        },
        Duration::from_secs(30),
        "replicas to reach term >= 1",
    )
    .await;

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

// ---- Slice C: §9 production metrics surface -------------------------------
// The tests below cover the [`PlacementNode::metrics_snapshot`] surface:
// runtime workers, per-group Raft fields, RPC counts/bytes/rejections,
// memory by group and aggregate, redirects by cause, movement
// phase/duration/result, voter/leader skew, and bounded cardinality.

/// §9: the production snapshot covers every §9 field through one
/// HomeKV-owned, serializable representation after real bootstrap,
/// elections and writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_snapshot_covers_section_9_fields() {
    let node = PlacementNode::start(node_config(
        "obs-prod-snap",
        *b"homekv-m4-obs10X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");

    // Elect a leader per group and commit one write per shard so the
    // snapshot observes real replication traffic (attempts + payload
    // bytes), not idle heartbeats.
    for shard in SHARDS {
        let leader = wait_for(
            || async { node.group_leader(shard) },
            Duration::from_secs(15),
            "data-group leader",
        )
        .await;
        node.group_raft(shard, leader)
            .expect("leader raft")
            .client_write(RaftCommand::Set {
                key: format!("obs-snap-key-{shard}").into_bytes(),
                value: b"obs-snap-value".to_vec(),
            })
            .await
            .expect("write commits");
    }
    wait_for(
        || async { node.catalog().raft().metrics().borrow().current_leader },
        Duration::from_secs(15),
        "catalog leader",
    )
    .await;

    let snap = node.metrics_snapshot().await.expect("snapshot works");

    // Runtime: the shared worker pool is fixed and sampled from the live
    // runtime.
    assert!(snap.runtime.sampled);
    assert_eq!(snap.runtime.num_workers, 4);
    // Timers: the placement node owns exactly the drive + reconcile
    // intervals (HomeKV-owned §9 "timer" signal; tokio's internal timer
    // wheel is not observable via the stable API).
    assert_eq!(snap.runtime.node_timers, 2);

    // Catalog identity matches the committed view.
    let state = node.committed_state().await.expect("catalog readable");
    assert_eq!(snap.catalog.placement_epoch, state.placement_epoch);
    assert!(snap.catalog.leader_id.is_some(), "catalog elected a leader");

    // Group counts: exclusive states covering every loaded shard.
    assert_eq!(snap.group_counts.loaded, SHARDS.len());
    assert_eq!(
        snap.group_counts.active + snap.group_counts.idle + snap.group_counts.error,
        snap.group_counts.loaded,
        "active/idle/error partition the loaded shards"
    );
    assert_eq!(
        snap.group_counts.active,
        SHARDS.len(),
        "every group has an observed leader"
    );

    // Skew matches the deterministic bootstrap bound.
    assert!(
        snap.skew.voter_skew <= 1 && snap.skew.leader_skew <= 1,
        "skew: {:?}",
        snap.skew
    );

    // RPC: catalog factories (one per catalog voter) plus one factory set
    // per shard (one per hosted identity); every factory carries a fixed
    // peer set (3 catalog voters / 4 group nodes); real traffic moved real
    // bytes.
    assert_eq!(snap.rpc.factories.len(), 3 + SHARDS.len() * 4);
    for factory in &snap.rpc.factories {
        match factory.shard_id {
            None => assert_eq!(factory.peers.len(), 3, "catalog peer set is the 3 voters"),
            Some(_) => assert_eq!(factory.peers.len(), 4, "group peer set is all 4 nodes"),
        }
    }
    assert!(snap.rpc.totals.attempts > 0);
    assert!(
        snap.rpc.totals.bytes > 0,
        "writes replicate application payload bytes"
    );

    // Memory: RSS parsed on Linux, one entry per hosted replica,
    // aggregate equals the sum of the parts.
    assert!(
        snap.memory.process_rss_bytes.is_some(),
        "RSS readable on Linux"
    );
    assert_eq!(snap.memory.per_replica.len(), SHARDS.len() * 4);
    let sum: u64 = snap
        .memory
        .per_replica
        .iter()
        .map(|entry| entry.state_machine_data_bytes)
        .sum();
    assert_eq!(snap.memory.aggregate_state_machine_data_bytes, sum);

    // Redirects: bounded cause set, quiet when nothing redirects.
    assert_eq!(snap.redirects.total, 0);
    assert_eq!(snap.redirects.by_cause.len(), 2);

    // Movement: one driver view per (shard, identity); bounded history.
    assert_eq!(snap.movement.len(), SHARDS.len() * 4);
    for driver in &snap.movement {
        assert!(
            driver.metrics.recent.len() <= 64,
            "recent-operation ring stays bounded"
        );
    }

    // Per-shard detail: shape, sort order, cross-links.
    assert_eq!(snap.shards.len(), SHARDS.len());
    let shard_ids: Vec<u16> = snap.shards.iter().map(|s| s.shard_id).collect();
    let mut sorted = shard_ids.clone();
    sorted.sort_unstable();
    assert_eq!(shard_ids, sorted, "shard entries sorted");
    for shard in &snap.shards {
        assert_eq!(shard.replicas.len(), 4);
        assert_eq!(shard.movement.len(), 4);
        assert_eq!(shard.rpc.len(), 4);
        assert!(shard.active, "shard {} has a leader", shard.shard_id);
        assert!(!shard.error);
        for replica in &shard.replicas {
            assert_eq!(replica.memory.shard_id, shard.shard_id);
            // Role/term/config/commit/apply/snapshot/lag are the PR #89
            // per-group Raft fields for the same replica.
            assert_eq!(replica.raft.shard_id, shard.shard_id);
        }
    }

    // The whole snapshot is a serializable HomeKV-owned representation:
    // JSON round-trips with no openraft leakage.
    let json = serde_json::to_string(&snap).expect("snapshot serializes");
    assert!(!json.contains("openraft"), "no openraft types leak");
    let back: PlacementMetrics = serde_json::from_str(&json).expect("snapshot deserializes");
    assert_eq!(back, snap);

    node.shutdown();
}

/// §9 / REQ-M4-GROUP-002 + GROUP-003: the shared runtime worker pool and
/// the per-factory peer sets stay fixed while the group count grows; only
/// per-replica series (shards x hosted identities) and factory handles
/// scale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_workers_and_peer_sets_fixed_as_groups_grow() {
    let small = PlacementNode::start(node_config(
        "obs-scale-small",
        *b"homekv-m4-obs11X",
        vec![7],
    ))
    .await
    .expect("small node starts");
    let big = PlacementNode::start(node_config(
        "obs-scale-big",
        *b"homekv-m4-obs12X",
        (0..8).collect(),
    ))
    .await
    .expect("big node starts");

    let small_snap = small.metrics_snapshot().await.expect("small snapshot");
    let big_snap = big.metrics_snapshot().await.expect("big snapshot");

    // The shared worker pool does not grow with the group count.
    assert_eq!(small_snap.runtime.num_workers, 4);
    assert_eq!(big_snap.runtime.num_workers, 4);

    // Per-replica series grow exactly with shards x hosted identities.
    assert_eq!(small_snap.memory.per_replica.len(), 4); // 1 shard x 4 identities
    assert_eq!(big_snap.memory.per_replica.len(), 32); // 8 shards x 4 identities
    assert_eq!(big_snap.movement.len(), 8 * small_snap.movement.len());

    // The per-factory peer set is fixed (3 catalog voters / 4 group nodes)
    // on both nodes: the connection-like unit is the peer slot, not a
    // socket, and it does not multiply with shards. Factory *handles* are
    // per (group, identity).
    for factory in small_snap
        .rpc
        .factories
        .iter()
        .chain(&big_snap.rpc.factories)
    {
        let expected = if factory.shard_id.is_none() { 3 } else { 4 };
        assert_eq!(factory.peers.len(), expected, "fixed peer set per factory");
    }
    assert_eq!(small_snap.rpc.factories.len(), 7); // 3 catalog voters + 1 shard x 4
    assert_eq!(big_snap.rpc.factories.len(), 35); // 3 catalog voters + 8 shards x 4

    small.shutdown();
    big.shutdown();
}

/// §9 / REQ-M4-OPS-003: per-shard inspection is paginated and the
/// per-replica series count is independent of the key count.
#[tokio::test]
async fn shard_metrics_pagination_bounds_cardinality() {
    let node = PlacementNode::start(node_config(
        "obs-pages",
        *b"homekv-m4-obs13X",
        (0..8).collect(),
    ))
    .await
    .expect("node starts");

    // Commit many keys: per-replica series must not grow with the key
    // count (no per-key labels anywhere).
    let leader = wait_for(
        || async { node.group_leader(0) },
        Duration::from_secs(15),
        "data-group leader",
    )
    .await;
    for i in 0..50u32 {
        node.group_raft(0, leader)
            .expect("leader raft")
            .client_write(RaftCommand::Set {
                key: format!("page-key-{i}").into_bytes(),
                value: vec![0u8; 16],
            })
            .await
            .expect("write commits");
    }

    let snap = node.metrics_snapshot().await.expect("snapshot");
    assert_eq!(
        snap.memory.per_replica.len(),
        8 * 4,
        "no key-derived series after 50 writes"
    );

    let page0 = node.metrics_for_shards(0, 3).await;
    assert_eq!(page0.len(), 3);
    let page1 = node.metrics_for_shards(3, 3).await;
    assert_eq!(page1.len(), 3);
    let page2 = node.metrics_for_shards(6, 3).await;
    assert_eq!(page2.len(), 2);
    assert!(node.metrics_for_shards(8, 3).await.is_empty());
    assert!(node.metrics_for_shards(0, 0).await.is_empty());

    let ids: Vec<u16> = page0
        .iter()
        .chain(&page1)
        .chain(&page2)
        .map(|entry| entry.shard_id)
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "pages arrive in shard-id order");
    assert_eq!(ids.len(), 8, "pages cover every shard exactly once");

    node.shutdown();
}

/// §9: route redirects are counted by cause through the node's shared
/// metrics handle; local routes are not counted.
#[tokio::test]
async fn route_redirects_counted_by_cause() {
    let node = PlacementNode::start(node_config(
        "obs-redirect-cause",
        *b"homekv-m4-obs14X",
        SHARDS.to_vec(),
    ))
    .await
    .expect("node starts");
    let committed = node.committed_state().await.expect("catalog readable");

    let metrics = node.route_redirect_metrics();
    let catalog = Arc::new(RwLock::new({
        let mut catalog = PlacementCatalog::default();
        catalog
            .apply(CatalogCommand::Bootstrap {
                cluster_id: *b"homekv-m4-obs14X",
                eligible_nodes: committed.eligible_nodes.values().cloned().collect(),
            })
            .unwrap();
        catalog
    }));

    let shard = SHARDS[0];
    let voters = committed.placements[&shard].voters;
    let outsider = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !voters.contains(id))
        .expect("an eligible non-voter exists");
    let voter = voters[0];
    let outsider_resolver =
        CommittedRouteResolver::with_metrics(outsider, Arc::clone(&catalog), metrics.clone());
    let voter_resolver =
        CommittedRouteResolver::with_metrics(voter, Arc::clone(&catalog), metrics.clone());

    assert!(matches!(
        outsider_resolver.resolve(shard).await,
        RouteDecision::Redirect(_)
    ));
    assert!(matches!(
        outsider_resolver.resolve(9999).await,
        RouteDecision::Unavailable
    ));
    assert!(matches!(
        voter_resolver.resolve(shard).await,
        RouteDecision::Local { .. }
    ));
    assert!(matches!(
        outsider_resolver.resolve(shard).await,
        RouteDecision::Redirect(_)
    ));

    // The resolver handle shares the node's counters.
    assert_eq!(metrics.total(), 3);

    let snap = node.metrics_snapshot().await.expect("snapshot");
    let by_cause: BTreeMap<_, _> = snap.redirects.by_cause.into_iter().collect();
    assert_eq!(by_cause.len(), 2, "closed cause set");
    assert_eq!(by_cause[&RedirectCause::NotCommittedVoter], 2);
    assert_eq!(by_cause[&RedirectCause::NoCommittedPlacement], 1);
    assert_eq!(snap.redirects.total, 3);

    node.shutdown();
}

/// §9: every movement driver surfaces phase attempts/successes/failures,
/// operation outcomes and bounded per-operation durations after a real
/// movement converges.
#[tokio::test]
async fn movement_driver_metrics_surfaced_per_driver() {
    const MOVING: u16 = 11;
    let node = PlacementNode::start(node_config(
        "obs-movement-metrics",
        *b"homekv-m4-obs15X",
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

    let leader = wait_for(
        || async { node.group_leader(MOVING) },
        Duration::from_secs(15),
        "data-group leader",
    )
    .await;
    node.group_raft(MOVING, leader)
        .expect("leader raft")
        .client_write(RaftCommand::Set {
            key: b"obs-mvmetrics-key".to_vec(),
            value: b"obs-mvmetrics-value".to_vec(),
        })
        .await
        .expect("write commits");

    let op_id = *b"homekv-m4-obs-02";
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

    let snap = node.metrics_snapshot().await.expect("snapshot");
    assert_eq!(snap.movement.len(), 4, "one view per hosted driver");
    let mut saw_phase_activity = false;
    let mut saw_completed_operation = false;
    for driver in &snap.movement {
        assert_eq!(driver.shard_id, MOVING);
        let metrics = &driver.metrics;
        assert!(
            metrics.recent.len() <= 64,
            "recent-operation ring stays bounded"
        );
        let phase_attempts: u64 = metrics.phase_attempts.values().sum();
        if phase_attempts > 0 {
            saw_phase_activity = true;
        }
        if metrics.operations_completed > 0 {
            saw_completed_operation = true;
            assert!(
                metrics.recent.iter().any(|op| op.duration_millis > 0),
                "completed operations record durations"
            );
        }
    }
    assert!(
        saw_phase_activity,
        "drivers recorded movement phase attempts"
    );
    assert!(
        saw_completed_operation,
        "at least one driver completed the movement"
    );

    // The shard entry carries the same driver views.
    let entry = snap
        .shards
        .iter()
        .find(|s| s.shard_id == MOVING)
        .expect("shard entry");
    assert_eq!(entry.movement.len(), 4);

    node.shutdown();
}
