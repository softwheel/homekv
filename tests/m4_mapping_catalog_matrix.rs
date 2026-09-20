//! Spec 0006 §4 (mapping/catalog) automated verification matrix, plus the
//! REQ-M4-BAL-001 planner-skew assertions.
//!
//! Traceability to `specs/0006-multi-raft-placement/verification.md` §4:
//!
//! | §4 item | Automated here |
//! |---|---|
//! | 1. parent/M1 XXH3 golden vectors identical | `xxh3_golden_vectors_match_parent_m1` |
//! | 2. arbitrary byte keys map within `0..1024` | `arbitrary_byte_keys_always_map_within_1024` |
//! | 3. 1,024 group IDs unique and stable across restart | `group_ids_unique_and_stable_across_catalog_restart` |
//! | 4. deterministic bootstrap: RF=3, no duplicate voter, domain spread, voter/leader skew ≤ 1 | `real_cluster_bootstrap_is_rf3_domain_spread_and_skew_bounded` (model-level twins live in `src/placement.rs` tests: `three_node_bootstrap_has_canonical_golden_identity`, `six_node_bootstrap_is_domain_safe_and_balanced`) |
//! | 5. repeated identical bootstrap is a no-op; conflicting identity/topology fails closed | `repeated_bootstrap_is_noop_and_conflicts_fail_closed` (identical bootstrap → `AlreadyInitialized`, conflicting identity/topology → `ConflictingBootstrap`, committed image unchanged; model-level twin in `src/placement.rs::identical_bootstrap_is_idempotent_and_conflicts_fail_closed`) |
//! | 6. catalog writes/reads require normal OpenRaft authority | covered by `tests/m4_catalog_group.rs::catalog_group_is_quorum_authoritative_and_recovers_durable_bootstrap` (strong read and bootstrap write both fail on the quorum-less replica) |
//! | 7. catalog quorum loss cannot publish a placement | `catalog_quorum_loss_cannot_publish_movement_intent` |
//! | 8. catalog leader failover preserves committed epoch and pending movement | `catalog_leader_failover_preserves_epoch_and_pending_movement` (epoch preservation alone is also covered by `tests/m4_catalog_group.rs`) |
//! | 9. snapshot + log replay reproduces the reference catalog | covered by `tests/m4_catalog_group.rs::catalog_group_is_quorum_authoritative_and_recovers_durable_bootstrap` (persist snapshots on all voters → full stop → restart → `read()` equals the pre-restart committed `CatalogState`) |
//! | 10. corrupt/version-incompatible catalog state fails closed | `corrupt_on_disk_catalog_state_fails_closed` (on-disk snapshot/log corruption plus the on-disk catalog-envelope round trip; in-memory envelope variants live in `src/placement.rs::corrupt_truncated_and_unknown_version_snapshots_fail_closed` and `tests/m4_failure_matrix.rs::corrupt_and_version_incompatible_snapshots_fail_closed`) |
//!
//! REQ-M4-BAL-001 ("stable voter counts and desired-leader counts differ by
//! at most one when topology permits") is asserted by
//! `planner_targets_keep_voter_skew_bounded_on_symmetric_topology`,
//! `planner_leader_reassignments_keep_leader_skew_bounded`, and
//! `asymmetric_topology_documents_the_when_permitted_caveat`, which compute
//! post-plan voter/leader counts from the plan's target placements and
//! reassignments.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::placement::{
    data_group_id, CatalogCommand, CatalogResponse, CatalogState, ClusterId, EligibleNode,
    PlacementCatalog, PlacementError, ShardPlacement, PLACEMENT_CATALOG_GROUP_ID,
    PLACEMENT_FORMAT_VERSION,
};
use homekv::placement_raft::{CatalogGroupError, PlacementCatalogGroup};
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode, RaftNodeId};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use homekv::rebalance::{plan_rebalance, RebalanceInput};
use homekv::storage::shard_engine::shard_for_key;
use homekv::storage::LOGICAL_SHARD_COUNT;
use openraft::raft::Raft;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Config, SnapshotPolicy};
use xxhash_rust::xxh3::xxh3_64;

const CLUSTER_ID: [u8; 16] = *b"homekv-m4s4s5-01";

// ---------------------------------------------------------------------------
// §4 item 1: parent/M1 XXH3 golden vectors
// ---------------------------------------------------------------------------

#[test]
fn xxh3_golden_vectors_match_parent_m1() {
    // The M1 golden vectors from `src/storage/shard_engine.rs`
    // (`xxh3_mapping_golden_vectors_are_stable`) must stay identical: the
    // shard mapping is a cross-version compatibility surface
    // (REQ-M4-MAP-001, REQ-M4-BASE-004).
    assert_eq!(shard_for_key(b"").as_u16(), 194);
    assert_eq!(shard_for_key(b"abc").as_u16(), 336);
    // Cross-check against the requirement formula itself,
    // `shard_id = XXH3_64(raw_key_bytes) & 1023`, on representative keys.
    for key in [
        b"".as_slice(),
        b"abc".as_slice(),
        b"homekv".as_slice(),
        &[0xffu8; 16][..],
        &[0x00u8; 64][..],
    ] {
        assert_eq!(
            shard_for_key(key).as_u16(),
            (xxh3_64(key) & 1023) as u16,
            "key {key:?} does not follow the REQ-M4-MAP-001 formula"
        );
    }
}

// ---------------------------------------------------------------------------
// §4 item 2: arbitrary byte keys stay within 0..1024
// ---------------------------------------------------------------------------

#[test]
fn arbitrary_byte_keys_always_map_within_1024() {
    let mut keys: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x00],
        vec![0x00; 64],
        vec![0xff],
        vec![0xff; 32],
        vec![0xff; 1024],
        b"a".to_vec(),
    ];
    // Deterministic xorshift64* stream: 50k pseudo-random keys with random
    // lengths in 0..=128, including adversarial all-0x00 / all-0xFF runs.
    let mut rng = 0x1234_5678_9abc_def1u64;
    let mut next_byte = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        (rng & 0xff) as u8
    };
    for _ in 0..50_000 {
        let len = usize::from(next_byte()) % 129;
        keys.push((0..len).map(|_| next_byte()).collect());
    }
    assert_eq!(keys.len(), 50_007);
    for key in &keys {
        let shard = shard_for_key(key).as_u16();
        assert!(
            shard < LOGICAL_SHARD_COUNT,
            "key of len {} mapped outside 0..1024: {shard}",
            key.len()
        );
    }
}

// ---------------------------------------------------------------------------
// Real catalog-cluster harness (items 3, 4, 7, 8, 10)
// ---------------------------------------------------------------------------

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m4-mapping-catalog-matrix",
        [
            BootstrapNode {
                id: 1,
                raft_endpoint: "127.0.0.1:19701".into(),
            },
            BootstrapNode {
                id: 2,
                raft_endpoint: "127.0.0.1:19702".into(),
            },
            BootstrapNode {
                id: 3,
                raft_endpoint: "127.0.0.1:19703".into(),
            },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19701")),
        (2, RaftNode::new("127.0.0.1:19702")),
        (3, RaftNode::new("127.0.0.1:19703")),
    ])
}

fn eligible_nodes() -> Vec<EligibleNode> {
    (1..=6)
        .map(|node_id| EligibleNode {
            node_id,
            raft_endpoint: format!("127.0.0.1:{}", 21_000 + node_id),
            failure_domain: format!("zone-{}", (node_id - 1) % 3),
        })
        .collect()
}

/// Read the committed catalog state, tolerating transient leader changes:
/// under parallel test load a freshly elected leader can flap, so a single
/// `read()` may hit `ConsensusUnavailable`. This retries for a bounded time
/// and still inspects the committed state (never a timing-only assertion).
async fn read_committed(group: &PlacementCatalogGroup) -> CatalogState {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match group.read().await {
            Ok(state) => return state,
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "catalog read never stabilized: {error:?}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn unique_test_dir(tag: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m4-mapping-matrix-{tag}-{nonce}-{}",
        std::process::id()
    ))
}

fn config() -> std::sync::Arc<Config> {
    std::sync::Arc::new(
        Config {
            cluster_name: "homekv-m4-mapping-catalog-matrix".into(),
            heartbeat_interval: 25,
            election_timeout_min: 100,
            election_timeout_max: 200,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

struct CatalogCluster {
    root: std::path::PathBuf,
    nodes: BTreeMap<u64, Raft<HomeKvRaftConfig>>,
    state_machines: BTreeMap<u64, HomeKvStateMachine>,
    links: TestLinkController,
}

impl CatalogCluster {
    async fn start(root: std::path::PathBuf, initialize: bool) -> Self {
        fs::create_dir_all(&root).unwrap();
        let links = TestLinkController::default();
        let mut factories = BTreeMap::new();
        let mut nodes = BTreeMap::new();
        let mut state_machines = BTreeMap::new();
        for id in 1..=3 {
            let factory =
                HomeKvRaftNetworkFactory::new(id, bootstrap(), 16, links.clone()).unwrap();
            let store =
                HomeKvRaftLogStore::open(root.join(format!("catalog-node-{id}.raft"))).unwrap();
            let state_machine =
                HomeKvStateMachine::open(root.join(format!("catalog-node-{id}.snapshot"))).unwrap();
            let raft = Raft::new(id, config(), factory.clone(), store, state_machine.clone())
                .await
                .unwrap();
            factories.insert(id, factory);
            nodes.insert(id, raft);
            state_machines.insert(id, state_machine);
        }
        for factory in factories.values() {
            for (id, raft) in &nodes {
                factory
                    .register_handler(*id, std::sync::Arc::new(raft.clone()))
                    .unwrap();
            }
        }
        if initialize {
            nodes[&1].initialize(membership()).await.unwrap();
        }
        Self {
            root,
            nodes,
            state_machines,
            links,
        }
    }

    fn group(&self, node_id: u64) -> PlacementCatalogGroup {
        PlacementCatalogGroup::new(
            self.nodes[&node_id].clone(),
            self.state_machines[&node_id].clone(),
        )
    }

    fn isolate(&self, node_id: u64) {
        for peer in 1..=3 {
            if peer != node_id {
                self.links.partition_bidirectional(node_id, peer);
            }
        }
    }

    fn heal_all(&self) {
        for (a, b) in [(1, 2), (1, 3), (2, 3)] {
            self.links.heal_bidirectional(a, b);
        }
    }

    async fn persist_snapshots(&self) {
        for state_machine in self.state_machines.values() {
            let mut state_machine = state_machine.clone();
            state_machine
                .get_snapshot_builder()
                .await
                .build_snapshot()
                .await
                .unwrap();
        }
    }

    async fn stop(self, remove_root: bool) {
        for raft in self.nodes.values() {
            raft.shutdown().await.unwrap();
        }
        if remove_root {
            fs::remove_dir_all(self.root).unwrap();
        }
    }
}

/// Build a valid movement target for `shard_id`: swap the highest stable
/// voter for the lowest spare eligible node in the same failure domain, so
/// the RF=3 / domain-spread invariants the catalog enforces are preserved.
fn movement_target(state: &CatalogState, shard_id: u16) -> [RaftNodeId; 3] {
    let stable = state.placements.get(&shard_id).unwrap().voters;
    let removed = stable[2];
    let removed_domain = state.eligible_nodes[&removed].failure_domain.clone();
    let spare = state
        .eligible_nodes
        .values()
        .filter(|node| !stable.contains(&node.node_id) && node.failure_domain == removed_domain)
        .map(|node| node.node_id)
        .min()
        .expect("the 6-node/3-zone topology always has a same-domain spare");
    let mut target: Vec<RaftNodeId> = stable
        .iter()
        .copied()
        .filter(|voter| *voter != removed)
        .chain(std::iter::once(spare))
        .collect();
    target.sort_unstable();
    target
        .try_into()
        .unwrap_or_else(|voters: Vec<RaftNodeId>| panic!("exactly three target voters: {voters:?}"))
}

fn spread(counts: &BTreeMap<u64, u64>) -> u64 {
    counts.values().max().unwrap() - counts.values().min().unwrap()
}

// ---------------------------------------------------------------------------
// §4 item 3: all 1,024 group IDs unique and stable across restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_ids_unique_and_stable_across_catalog_restart() {
    // Pure-function half: `data_group_id` is injective over 0..1024, never
    // collides with the reserved catalog group, and rejects 1024.
    let ids: BTreeSet<u64> = (0..LOGICAL_SHARD_COUNT)
        .map(|shard| data_group_id(shard).unwrap())
        .collect();
    assert_eq!(ids.len(), usize::from(LOGICAL_SHARD_COUNT));
    assert!(!ids.contains(&PLACEMENT_CATALOG_GROUP_ID));
    assert!(data_group_id(LOGICAL_SHARD_COUNT).is_err());

    // Replicated half: every committed placement carries the canonical
    // group identity, and a full stop/restart reproduces the mapping.
    let root = unique_test_dir("group-ids");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let group = cluster.group(leader);
    assert_eq!(
        group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap(),
        CatalogResponse::Initialized { placement_epoch: 1 }
    );
    let committed = read_committed(&group).await;
    let before: BTreeMap<u16, u64> = committed
        .placements
        .iter()
        .map(|(shard_id, placement)| {
            assert_eq!(
                placement.group_id,
                data_group_id(*shard_id).unwrap(),
                "shard {shard_id} has a non-canonical group identity"
            );
            (*shard_id, placement.group_id)
        })
        .collect();
    assert_eq!(before.len(), usize::from(LOGICAL_SHARD_COUNT));

    cluster.persist_snapshots().await;
    cluster.stop(false).await;

    let recovered = CatalogCluster::start(root, false).await;
    let recovered_leader = support::authoritative_leader(&recovered.nodes, None).await;
    let after: BTreeMap<u16, u64> = recovered
        .group(recovered_leader)
        .read()
        .await
        .unwrap()
        .placements
        .iter()
        .map(|(shard_id, placement)| (*shard_id, placement.group_id))
        .collect();
    assert_eq!(
        before, after,
        "group identities must be stable across restart"
    );
    recovered.stop(true).await;
}

// ---------------------------------------------------------------------------
// §4 item 4: deterministic bootstrap — RF=3, no duplicate voter,
// permitted-domain spread, voter/leader skew ≤ 1 (on the real cluster)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_cluster_bootstrap_is_rf3_domain_spread_and_skew_bounded() {
    let root = unique_test_dir("bootstrap");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let group = cluster.group(leader);
    group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap();
    let committed = read_committed(&group).await;
    assert_eq!(committed.placements.len(), usize::from(LOGICAL_SHARD_COUNT));

    let mut voter_counts: BTreeMap<u64, u64> = BTreeMap::new();
    let mut leader_counts: BTreeMap<u64, u64> = BTreeMap::new();
    for placement in committed.placements.values() {
        // Exactly three distinct voters in canonical sorted order.
        assert!(
            placement.voters[0] < placement.voters[1] && placement.voters[1] < placement.voters[2],
            "shard {} voters are not canonical and distinct: {:?}",
            placement.shard_id,
            placement.voters
        );
        // Permitted-domain spread: with three eligible domains every
        // placement spans all three.
        let domains: BTreeSet<&str> = placement
            .voters
            .iter()
            .map(|voter| committed.eligible_nodes[voter].failure_domain.as_str())
            .collect();
        assert_eq!(
            domains.len(),
            3,
            "shard {} does not span three failure domains",
            placement.shard_id
        );
        assert!(
            placement.voters.contains(&placement.desired_leader),
            "shard {} desired leader is not a voter",
            placement.shard_id
        );
        for voter in placement.voters {
            *voter_counts.entry(voter).or_insert(0) += 1;
        }
        *leader_counts.entry(placement.desired_leader).or_insert(0) += 1;
    }
    assert_eq!(voter_counts.len(), 6);
    assert_eq!(leader_counts.len(), 6);
    assert!(
        spread(&voter_counts) <= 1,
        "voter skew exceeds one: {voter_counts:?}"
    );
    assert!(
        spread(&leader_counts) <= 1,
        "desired-leader skew exceeds one: {leader_counts:?}"
    );
    committed.validate().unwrap();
    cluster.stop(true).await;
}

// ---------------------------------------------------------------------------
// §4 item 5: repeated identical bootstrap is a no-op; conflicting
// identity/topology fails closed
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_bootstrap_is_noop_and_conflicts_fail_closed() {
    let root = unique_test_dir("bootstrap-idempotence");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let group = cluster.group(leader);
    assert_eq!(
        group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap(),
        CatalogResponse::Initialized { placement_epoch: 1 }
    );
    let before = read_committed(&group).await;

    // An identical repeat is a no-op: the epoch does not move and the
    // committed image is unchanged.
    assert_eq!(
        group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap(),
        CatalogResponse::AlreadyInitialized { placement_epoch: 1 }
    );
    assert_eq!(read_committed(&group).await, before);

    // A conflicting cluster identity fails closed…
    assert_eq!(
        group
            .bootstrap([9; 16], eligible_nodes())
            .await
            .unwrap_err(),
        CatalogGroupError::ConflictingBootstrap
    );
    // …as does a conflicting topology.
    let mut different_topology = eligible_nodes();
    different_topology.pop();
    assert_eq!(
        group
            .bootstrap(CLUSTER_ID, different_topology)
            .await
            .unwrap_err(),
        CatalogGroupError::ConflictingBootstrap
    );

    // Neither conflict moved the committed image.
    assert_eq!(read_committed(&group).await, before);
    cluster.stop(true).await;
}

// ---------------------------------------------------------------------------
// §4 item 7: catalog quorum loss cannot publish a placement
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_quorum_loss_cannot_publish_movement_intent() {
    let root = unique_test_dir("quorum-loss");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let group = cluster.group(leader);
    group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap();
    let committed = read_committed(&group).await;
    let shard_id = 5u16;
    let target = movement_target(&committed, shard_id);

    // Kill 2 of 3 voters: partition the leader from both followers *and*
    // the followers from each other, so no Raft quorum remains anywhere.
    cluster.isolate(leader);
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|id| *id != leader).collect();
    cluster.links.partition_bidirectional(others[0], others[1]);

    // The movement-intent publish is never acknowledged: the partitioned
    // leader cannot reach quorum (it hangs or errors; either way it must
    // not return success).
    let attempt = tokio::time::timeout(
        Duration::from_secs(2),
        cluster.group(leader).submit(CatalogCommand::BeginMovement {
            expected_epoch: committed.placement_epoch,
            operation_id: [7; 16],
            shard_id,
            target_voters: target,
        }),
    )
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "catalog quorum loss must not acknowledge a movement intent: {attempt:?}"
    );

    // And it is never applied: the partitioned replica's committed image
    // has no pending movement and the epoch did not move.
    let local = cluster.group(leader).committed_state().await.unwrap();
    assert_eq!(local.placement_epoch, committed.placement_epoch);
    assert!(
        local.placements[&shard_id].pending_movement.is_none(),
        "a quorum-less intent must never be applied"
    );

    // Heal, elect a leader, and confirm the intent never landed anywhere.
    cluster.heal_all();
    let healed_leader = support::authoritative_leader(&cluster.nodes, None).await;
    let healed = read_committed(&cluster.group(healed_leader)).await;
    assert_eq!(healed.placement_epoch, committed.placement_epoch);
    assert!(
        healed
            .placements
            .values()
            .all(|placement| placement.pending_movement.is_none()),
        "no movement intent may be committed without catalog quorum"
    );
    cluster.stop(true).await;
}

// ---------------------------------------------------------------------------
// §4 item 8: catalog leader failover preserves committed epoch and pending
// movement
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_leader_failover_preserves_epoch_and_pending_movement() {
    let root = unique_test_dir("failover");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let group = cluster.group(leader);
    group.bootstrap(CLUSTER_ID, eligible_nodes()).await.unwrap();
    let committed = read_committed(&group).await;
    let shard_id = 5u16;
    let stable_voters = committed.placements[&shard_id].voters;
    let target = movement_target(&committed, shard_id);

    // Commit a movement intent (phase Intent) through the old leader.
    assert_eq!(
        group
            .submit(CatalogCommand::BeginMovement {
                expected_epoch: committed.placement_epoch,
                operation_id: [7; 16],
                shard_id,
                target_voters: target,
            })
            .await
            .unwrap(),
        CatalogResponse::MovementAccepted {
            phase: homekv::placement::MovementPhase::Intent
        }
    );

    // Kill the leader; the remaining two voters elect a replacement.
    cluster.isolate(leader);
    let replacement = support::authoritative_leader(&cluster.nodes, Some(leader)).await;
    assert_ne!(replacement, leader);

    // The committed epoch and the full pending movement survive failover.
    let after = read_committed(&cluster.group(replacement)).await;
    assert_eq!(after.placement_epoch, committed.placement_epoch);
    let pending = after.placements[&shard_id]
        .pending_movement
        .as_ref()
        .expect("pending movement must survive catalog leader failover");
    assert_eq!(pending.operation_id, [7; 16]);
    assert_eq!(pending.phase, homekv::placement::MovementPhase::Intent);
    assert_eq!(pending.source_voters, stable_voters);
    assert_eq!(pending.target_voters, target);
    assert_eq!(pending.retries, 0);
    // Stable placement is untouched by the intent.
    assert_eq!(after.placements[&shard_id].voters, stable_voters);
    cluster.stop(true).await;
}

// ---------------------------------------------------------------------------
// §4 item 10: corrupt/version-incompatible catalog state fails closed
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_on_disk_catalog_state_fails_closed() {
    let root = unique_test_dir("corrupt");
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    cluster
        .group(leader)
        .bootstrap(CLUSTER_ID, eligible_nodes())
        .await
        .unwrap();
    // Persist snapshots so every voter has an on-disk snapshot file, then
    // shut the cluster down; the corruption below targets those files.
    cluster.persist_snapshots().await;
    cluster.stop(false).await;

    // Bit-flipped state-machine snapshot: open fails cleanly, no panic.
    let snapshot_path = root.join("catalog-node-1.snapshot");
    let snapshot_bytes = fs::read(&snapshot_path).unwrap();
    assert!(!snapshot_bytes.is_empty());
    let mut flipped = snapshot_bytes.clone();
    *flipped.last_mut().unwrap() ^= 0xff;
    fs::write(&snapshot_path, &flipped).unwrap();
    assert!(
        HomeKvStateMachine::open(&snapshot_path).is_err(),
        "a bit-flipped on-disk snapshot must fail closed"
    );

    // Truncated state-machine snapshot: open fails cleanly, no panic.
    let mut truncated = snapshot_bytes.clone();
    truncated.truncate(truncated.len() / 2);
    fs::write(&snapshot_path, &truncated).unwrap();
    assert!(
        HomeKvStateMachine::open(&snapshot_path).is_err(),
        "a truncated on-disk snapshot must fail closed"
    );

    // Truncated Raft log store: open fails cleanly, no panic.
    let log_path = root.join("catalog-node-2.raft");
    let log_bytes = fs::read(&log_path).unwrap();
    assert!(!log_bytes.is_empty());
    let mut truncated_log = log_bytes.clone();
    truncated_log.truncate(truncated_log.len() / 2);
    fs::write(&log_path, &truncated_log).unwrap();
    assert!(
        HomeKvRaftLogStore::open(&log_path).is_err(),
        "a truncated on-disk Raft log must fail closed"
    );

    // Version-incompatible catalog envelope on disk: the write→read→restore
    // round trip fails closed with `UnsupportedSnapshotVersion`.
    let mut catalog = PlacementCatalog::default();
    catalog
        .apply(CatalogCommand::Bootstrap {
            cluster_id: CLUSTER_ID,
            eligible_nodes: eligible_nodes(),
        })
        .unwrap();
    let image = catalog.encode_snapshot().unwrap();
    let envelope_path = root.join("catalog-envelope.bin");
    fs::write(&envelope_path, &image).unwrap();
    let mut wrong_version = fs::read(&envelope_path).unwrap();
    wrong_version[8..10].copy_from_slice(&2u16.to_le_bytes());
    fs::write(&envelope_path, &wrong_version).unwrap();
    assert_eq!(
        PlacementCatalog::restore_snapshot(&fs::read(&envelope_path).unwrap()),
        Err(PlacementError::UnsupportedSnapshotVersion(2)),
        "a version-incompatible on-disk catalog envelope must fail closed"
    );
    // The untouched envelope still restores exactly.
    let restored = PlacementCatalog::restore_snapshot(&image).unwrap();
    assert_eq!(restored, catalog);

    fs::remove_dir_all(&root).unwrap();
}

// ---------------------------------------------------------------------------
// REQ-M4-BAL-001: planner skew ≤ 1 on resulting plans
// ---------------------------------------------------------------------------

fn bal_cluster_id() -> ClusterId {
    *b"homekv-bal001-01"
}

fn bal_node(node_id: u64, domain: &str) -> EligibleNode {
    EligibleNode {
        node_id,
        raft_endpoint: format!("127.0.0.1:{}", 22_000 + node_id),
        failure_domain: domain.to_string(),
    }
}

fn bal_bootstrap(nodes: Vec<EligibleNode>) -> CatalogState {
    let mut catalog = PlacementCatalog::default();
    catalog
        .apply(CatalogCommand::Bootstrap {
            cluster_id: bal_cluster_id(),
            eligible_nodes: nodes,
        })
        .unwrap();
    catalog.state().unwrap().clone()
}

fn bal_placement(
    shard_id: u16,
    voters: [RaftNodeId; 3],
    desired_leader: RaftNodeId,
) -> ShardPlacement {
    ShardPlacement {
        shard_id,
        group_id: data_group_id(shard_id).unwrap(),
        voters,
        desired_leader,
        epoch: 1,
        pending_movement: None,
    }
}

fn small_bal_catalog(nodes: Vec<EligibleNode>, placements: Vec<ShardPlacement>) -> CatalogState {
    CatalogState {
        format_version: PLACEMENT_FORMAT_VERSION,
        cluster_id: bal_cluster_id(),
        placement_epoch: 1,
        eligible_nodes: nodes.into_iter().map(|node| (node.node_id, node)).collect(),
        placements: placements
            .into_iter()
            .map(|placement| (placement.shard_id, placement))
            .collect(),
    }
}

fn no_unavailable() -> BTreeSet<RaftNodeId> {
    BTreeSet::new()
}

/// Post-plan voter counts: start from the committed counts, then apply each
/// planned movement's target voter set (exactly one voter changes per
/// movement, mirroring `RebalancePlan` semantics).
fn post_plan_voter_counts(
    catalog: &CatalogState,
    plan: &homekv::rebalance::RebalancePlan,
) -> BTreeMap<RaftNodeId, u64> {
    let mut counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for placement in catalog.placements.values() {
        for voter in placement.voters {
            *counts.entry(voter).or_insert(0) += 1;
        }
    }
    for movement in &plan.movements {
        for voter in movement.source_voters {
            *counts.get_mut(&voter).expect("source voter is counted") -= 1;
        }
        for voter in movement.target_voters {
            *counts.entry(voter).or_insert(0) += 1;
        }
    }
    counts
}

/// Post-plan desired-leader counts over `scope` nodes after applying the
/// plan's leader reassignments.
fn post_plan_leader_counts(
    catalog: &CatalogState,
    plan: &homekv::rebalance::RebalancePlan,
    scope: &BTreeSet<RaftNodeId>,
) -> BTreeMap<RaftNodeId, u64> {
    let mut counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for node in scope {
        counts.insert(*node, 0);
    }
    for placement in catalog.placements.values() {
        if scope.contains(&placement.desired_leader) {
            *counts.entry(placement.desired_leader).or_insert(0) += 1;
        }
    }
    for reassignment in &plan.leader_reassignments {
        if let Some(count) = counts.get_mut(&reassignment.from) {
            *count = count.saturating_sub(1);
        }
        *counts.entry(reassignment.to).or_insert(0) += 1;
    }
    counts
}

#[test]
fn planner_targets_keep_voter_skew_bounded_on_symmetric_topology() {
    // Healthy symmetric case first: a real 1,024-shard bootstrap over six
    // nodes is already balanced, so the plan is empty and the committed
    // skew itself satisfies BAL-001 (mirrors the bootstrap skew asserts).
    let state = bal_bootstrap(
        (1..=6)
            .map(|id| bal_node(id, &format!("zone-{id}")))
            .collect(),
    );
    let plan = plan_rebalance(RebalanceInput {
        catalog: &state,
        unavailable_nodes: &no_unavailable(),
    })
    .unwrap();
    assert!(plan.movements.is_empty());
    let committed_voters = post_plan_voter_counts(&state, &plan);
    assert!(
        spread(&committed_voters) <= 1,
        "bootstrap voter skew exceeds one: {committed_voters:?}"
    );

    // Skewed case: 4 nodes, 12 shards, hand-built 12/12/6/6 voter skew on a
    // healthy symmetric topology. The planner must repair it with minimal
    // moves and land at skew ≤ 1.
    let nodes = vec![
        bal_node(1, "a"),
        bal_node(2, "b"),
        bal_node(3, "c"),
        bal_node(4, "d"),
    ];
    let mut placements = Vec::new();
    for shard_id in 0..6u16 {
        placements.push(bal_placement(shard_id, [1, 2, 3], 1));
    }
    for shard_id in 6..12u16 {
        placements.push(bal_placement(shard_id, [1, 2, 4], 2));
    }
    let catalog = small_bal_catalog(nodes, placements);
    let before = post_plan_voter_counts(
        &catalog,
        &homekv::rebalance::RebalancePlan {
            cluster_id: bal_cluster_id(),
            source_epoch: 1,
            movements: Vec::new(),
            leader_reassignments: Vec::new(),
        },
    );
    assert_eq!(spread(&before), 6, "test setup must start skewed");
    let plan = plan_rebalance(RebalanceInput {
        catalog: &catalog,
        unavailable_nodes: &no_unavailable(),
    })
    .unwrap();
    assert!(!plan.movements.is_empty());
    for movement in &plan.movements {
        // Exactly one voter changes; the target stays canonical RF=3 with
        // no co-located replicas.
        assert!(movement.target_voters.windows(2).all(|w| w[0] < w[1]));
        let changed = movement
            .source_voters
            .iter()
            .filter(|voter| !movement.target_voters.contains(voter))
            .count();
        assert_eq!(changed, 1, "planner must move exactly one voter per shard");
    }
    let after = post_plan_voter_counts(&catalog, &plan);
    assert!(
        spread(&after) <= 1,
        "post-plan voter skew exceeds one on a symmetric topology: {after:?}"
    );
}

#[test]
fn planner_leader_reassignments_keep_leader_skew_bounded() {
    // All eight desired leaders sit on node 4 of a healthy 4-node topology.
    let nodes = vec![
        bal_node(1, "a"),
        bal_node(2, "b"),
        bal_node(3, "c"),
        bal_node(4, "d"),
    ];
    let placements = (0..8u16)
        .map(|shard_id| bal_placement(shard_id, [1, 2, 3], 4))
        .collect();
    let catalog = small_bal_catalog(nodes, placements);
    let scope: BTreeSet<RaftNodeId> = [1, 2, 3, 4].into_iter().collect();
    let plan = plan_rebalance(RebalanceInput {
        catalog: &catalog,
        unavailable_nodes: &no_unavailable(),
    })
    .unwrap();
    assert!(!plan.leader_reassignments.is_empty());
    for reassignment in &plan.leader_reassignments {
        let voters = catalog.placements[&reassignment.shard_id].voters;
        assert!(
            voters.contains(&reassignment.to),
            "reassigned leader must be a stable voter"
        );
    }
    let after = post_plan_leader_counts(&catalog, &plan, &scope);
    assert!(
        spread(&after) <= 1,
        "post-plan desired-leader skew exceeds one: {after:?}"
    );
}

#[test]
fn asymmetric_topology_documents_the_when_permitted_caveat() {
    // REQ-M4-BAL-001 binds skew "when topology permits". With node 4
    // unavailable, no plan can keep node 4 at the balanced count: its
    // replicas must evacuate, so the ≤ 1 bound applies to the nodes that
    // can actually host replicas (the available set).
    let nodes = vec![
        bal_node(1, "a"),
        bal_node(2, "b"),
        bal_node(3, "c"),
        bal_node(4, "d"),
    ];
    // 6 shards on [1,2,4], 6 shards on [1,3,4]: node counts 12/6/6/12.
    let mut placements = Vec::new();
    for shard_id in 0..6u16 {
        placements.push(bal_placement(shard_id, [1, 2, 4], 1));
    }
    for shard_id in 6..12u16 {
        placements.push(bal_placement(shard_id, [1, 3, 4], 1));
    }
    let catalog = small_bal_catalog(nodes, placements);
    let unavailable: BTreeSet<RaftNodeId> = [4].into_iter().collect();
    let plan = plan_rebalance(RebalanceInput {
        catalog: &catalog,
        unavailable_nodes: &unavailable,
    })
    .unwrap();
    assert!(!plan.movements.is_empty());
    for movement in &plan.movements {
        assert!(
            !movement.target_voters.contains(&4),
            "an unavailable node must not receive replicas"
        );
    }
    let after = post_plan_voter_counts(&catalog, &plan);
    // The bound holds over the available nodes…
    let available_counts: BTreeMap<u64, u64> = after
        .iter()
        .filter(|(node, _)| **node != 4)
        .map(|(node, count)| (*node, *count))
        .collect();
    assert_eq!(
        available_counts,
        [(1, 12), (2, 12), (3, 12)].into_iter().collect(),
        "evacuation must balance the available nodes exactly here"
    );
    assert!(spread(&available_counts) <= 1);
    // …but necessarily not over all eligible nodes: the evacuated node
    // sits at zero while the others hold twelve each. This is the
    // documented "when topology permits" caveat, not a violation.
    assert_eq!(after[&4], 0);
    assert!(spread(&after) > 1, "caveat: skew over all eligible nodes");
}
