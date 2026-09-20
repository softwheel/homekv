//! Spec 0006 verification §8 — rebalancing and failure matrix.
//!
//! | Fault/scenario | Required invariant | Test |
//! |---|---|---|
//! | data-group single voter loss | quorum writes commit; other shards progress | `data_group_single_voter_loss_keeps_quorum` (real [`PlacementNode`]) |
//! | data-group quorum loss | no acknowledgment; other shards progress | `data_group_quorum_loss_blocks_writes_other_shard_progresses` (real [`PlacementNode`]) |
//! | catalog leader killed | quorum elects replacement; one movement identity persists | `catalog_leader_change_keeps_single_movement_identity` |
//! | catalog loses quorum | no new placement; stable data groups continue normally | `catalog_quorum_loss_rejects_mutations_but_reads_and_data_continue` |
//! | node with many groups restarts | groups recover independently before service | `placement_node_restart_recovers_groups_independently` (real [`PlacementNode`]) |
//! | planner restart | deterministic plan/idempotent phase continuation | `planner_restart_is_deterministic` (+ §7 reconciler-restart tests in `m4_movement_matrix.rs`) |
//! | scheduler restart | completed work stays completed; in-flight resumes without duplication | `scheduler_restart_recovers_without_duplication` |
//! | gossip divergence | planner input stays advisory; committed placement untouched | `gossip_divergence_does_not_mutate_committed_placement` |
//! | slow movement | attempt bound and metrics stay bounded while foreground traffic progresses | `slow_movement_stays_bounded_while_traffic_progresses` |
//! | corrupt catalog snapshot | fails closed, no partial restored state | `corrupt_and_version_incompatible_snapshots_fail_closed` |
//!
//! The real-node tests run full in-process clusters (catalog Raft group +
//! data Raft groups). They are bounded: every wait has an explicit timeout.
//!
//! Honest limitation (recorded, not hidden): with quorum lost, OpenRaft's
//! public `client_write` does not return an explicit error within the bound
//! — the write stays pending. The test asserts the write is *not
//! acknowledged* (timeout, never success) and *not applied*, rather than
//! claiming an explicit-error API that does not exist.
//!
//! Traceability: REQ-M4-BAL-001..005, REQ-M4-FAIL-001..004.

#[path = "support/movement_fakes.rs"]
mod movement_fakes;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use homekv::movement::{CancelOutcome, DriverOutcome, OperatorError};
use homekv::placement::{
    CatalogCommand, CatalogResponse, CatalogState, MovementPhase, PlacementCatalog, PlacementError,
};
use homekv::placement_node::{PlacementNode, PlacementNodeConfig};
use homekv::placement_raft::CatalogGroupError;
use homekv::raft::{RaftCommand, RaftNodeId};
use homekv::rebalance::{
    movement_operation_id, plan_rebalance, BeginOutcome, CatalogHealth, CatalogHealthGate, FnProbe,
    MoveReason, PlanShardState, PlannedMovement, ReassignOutcome, RebalanceDrive, RebalanceInput,
    RebalancePlan, RebalanceScheduler, RebalanceSchedulerConfig,
};
use movement_fakes::{CommandKind, FaultPoint, Harness, OP_ID, SHARD};

const SHARD_B: u16 = 8;

// ---------------------------------------------------------------------------
// Real-node helpers
// ---------------------------------------------------------------------------

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m4-failure-matrix-{tag}-{}-{nonce}",
        std::process::id()
    ))
}

/// Serializes the real-cluster tests below. Each spins up an 11-instance
/// Raft cluster; running three concurrently oversubscribes small CI runners
/// and starves timing-sensitive tests in other binaries sharing the machine.
static REAL_CLUSTER_GUARD: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

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

struct Cluster {
    node: PlacementNode,
}

impl Cluster {
    /// Start a node. `PlacementNode::start` waits (bounded) for the catalog
    /// primary to win the leadership race before the bootstrap write, so
    /// startup is deterministic even under parallel-test load; each test
    /// still gets an isolated directory.
    async fn start(tag: &str, base: u64, shards: Vec<u16>) -> Self {
        let node_ids = vec![base, base + 1, base + 2, base + 3];
        let config = PlacementNodeConfig {
            node_ids: node_ids.clone(),
            catalog_voters: [base, base + 1, base + 2],
            data_dir: unique_dir(tag),
            cluster_id: *b"homekv-m4-fail00",
            shards: shards.clone(),
            drive_interval: Duration::from_secs(60),
            reconcile_interval: Duration::from_secs(120),
            election_min_ms: 150,
            election_max_ms: 300,
            ..PlacementNodeConfig::default()
        };
        let node = PlacementNode::start(config)
            .await
            .expect("placement node starts");
        Self { node }
    }

    /// Elect leaders for every shard and acknowledge one write per shard.
    async fn bootstrap_traffic(&self, shards: &[u16]) {
        for shard_id in shards {
            let leader_id = wait_for(
                || async { self.node.group_leader(*shard_id) },
                Duration::from_secs(20),
                "data-group leader",
            )
            .await;
            self.node
                .group_raft(*shard_id, leader_id)
                .expect("leader raft")
                .client_write(RaftCommand::Set {
                    key: format!("fail-key-{shard_id}").into_bytes(),
                    value: format!("fail-value-{shard_id}").into_bytes(),
                })
                .await
                .expect("seed write commits");
        }
    }

    async fn write_via_leader(&self, shard_id: u16, key: &[u8], value: &[u8]) {
        let leader_id = wait_for(
            || async { self.node.group_leader(shard_id) },
            Duration::from_secs(20),
            "data-group leader",
        )
        .await;
        self.node
            .group_raft(shard_id, leader_id)
            .expect("leader raft")
            .client_write(RaftCommand::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .await
            .expect("write commits");
    }

    async fn shutdown_replica(&self, shard_id: u16, node_id: RaftNodeId) {
        self.node
            .group_raft(shard_id, node_id)
            .expect("replica raft")
            .shutdown()
            .await
            .expect("replica shuts down");
    }

    async fn voters(&self, shard_id: u16) -> [RaftNodeId; 3] {
        let state = self.node.committed_state().await.expect("catalog readable");
        state.placements[&shard_id].voters
    }
}

// ---------------------------------------------------------------------------
// §8 rows: data-group faults on real nodes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_single_voter_loss_keeps_quorum() {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let cluster = Cluster::start("single-loss", 1, vec![SHARD, SHARD_B]).await;
    cluster.bootstrap_traffic(&[SHARD, SHARD_B]).await;

    // Kill one voter of shard 5 that is not the leader.
    let leader = cluster.node.group_leader(SHARD).expect("leader elected");
    let voters = cluster.voters(SHARD).await;
    let victim = voters
        .into_iter()
        .find(|id| *id != leader)
        .expect("follower");
    cluster.shutdown_replica(SHARD, victim).await;

    // Quorum (2/3) remains: writes still commit on the degraded shard...
    cluster
        .write_via_leader(SHARD, b"degraded-key", b"degraded-value")
        .await;
    // ...and the healthy shard is unaffected.
    cluster
        .write_via_leader(SHARD_B, b"healthy-key", b"healthy-value")
        .await;

    let sm = cluster.node.group_state_machine(SHARD, leader).expect("sm");
    assert_eq!(
        sm.get(b"degraded-key").await,
        Some(b"degraded-value".to_vec())
    );
    cluster.node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_quorum_loss_blocks_writes_other_shard_progresses() {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let cluster = Cluster::start("quorum-loss", 1, vec![SHARD, SHARD_B]).await;
    cluster.bootstrap_traffic(&[SHARD, SHARD_B]).await;

    // Kill two of the three voters, keeping the leader alive so the write
    // path is exercised against a live-but-quorumless leader.
    let leader = cluster.node.group_leader(SHARD).expect("leader elected");
    let voters = cluster.voters(SHARD).await;
    let mut victims = voters.into_iter().filter(|id| *id != leader);
    cluster
        .shutdown_replica(SHARD, victims.next().expect("follower"))
        .await;
    cluster
        .shutdown_replica(SHARD, victims.next().expect("follower"))
        .await;
    let leader_raft = cluster.node.group_raft(SHARD, leader).expect("leader raft");

    // No acknowledgment within the bound. OpenRaft's public API does not
    // surface an explicit quorum error here — the write stays pending — so
    // the honest assertion is "never acknowledged", not "returns an error".
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        leader_raft.client_write(RaftCommand::Set {
            key: b"quorum-lost-key".to_vec(),
            value: b"quorum-lost-value".to_vec(),
        }),
    )
    .await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "quorum-lost write must not be acknowledged: {outcome:?}"
    );
    // And it was not applied either.
    let sm = cluster.node.group_state_machine(SHARD, leader).expect("sm");
    assert_eq!(sm.get(b"quorum-lost-key").await, None);

    // The independent shard keeps committing: failure is contained per group.
    cluster
        .write_via_leader(SHARD_B, b"isolated-key", b"isolated-value")
        .await;
    let leader_b = cluster.node.group_leader(SHARD_B).expect("leader");
    let sm_b = cluster
        .node
        .group_state_machine(SHARD_B, leader_b)
        .expect("sm");
    assert_eq!(
        sm_b.get(b"isolated-key").await,
        Some(b"isolated-value".to_vec())
    );

    cluster.node.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn placement_node_restart_recovers_groups_independently() {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let data_dir = unique_dir("restart");
    let config = || PlacementNodeConfig {
        node_ids: vec![1, 2, 3, 4],
        catalog_voters: [1, 2, 3],
        data_dir: data_dir.clone(),
        cluster_id: *b"homekv-m4-fail01",
        shards: vec![SHARD, SHARD_B],
        drive_interval: Duration::from_secs(60),
        reconcile_interval: Duration::from_secs(120),
        election_min_ms: 150,
        election_max_ms: 300,
        ..PlacementNodeConfig::default()
    };
    // `PlacementNode::start` waits (bounded) for catalog leadership before
    // the bootstrap write, so no startup race remains to paper over; the
    // data directory stays fixed so the restart finds its state.
    let node = PlacementNode::start(config())
        .await
        .expect("placement node starts");
    let cluster = Cluster { node };
    cluster.bootstrap_traffic(&[SHARD, SHARD_B]).await;
    cluster.node.shutdown();

    // Restart from the same directories: bootstrap is idempotent and
    // initialize tolerates already-initialized groups.
    let node = PlacementNode::start(config())
        .await
        .expect("placement node restarts");
    let cluster = Cluster { node };
    for shard_id in [SHARD, SHARD_B] {
        let leader_id = wait_for(
            || async { cluster.node.group_leader(shard_id) },
            Duration::from_secs(30),
            "data-group leader after restart",
        )
        .await;
        // Pre-restart writes survived on disk...
        let sm = cluster
            .node
            .group_state_machine(shard_id, leader_id)
            .expect("sm");
        assert_eq!(
            sm.get(format!("fail-key-{shard_id}").as_bytes()).await,
            Some(format!("fail-value-{shard_id}").as_bytes().to_vec()),
            "shard {shard_id} recovered its data"
        );
        // ...and each group serves new writes independently.
        cluster
            .write_via_leader(shard_id, b"post-restart-key", b"post-restart-value")
            .await;
    }
    cluster.node.shutdown();
}

// ---------------------------------------------------------------------------
// §8 rows: catalog faults (model level)
// ---------------------------------------------------------------------------

/// Wrap the harness catalog in the production health gate with a fixed probe.
fn gated_catalog(
    harness: &Harness,
    health: CatalogHealth,
) -> CatalogHealthGate<
    movement_fakes::FakeCatalogPort,
    FnProbe<impl Fn() -> CatalogHealth + Send + Sync>,
> {
    CatalogHealthGate::new(
        movement_fakes::FakeCatalogPort(harness.catalog.clone()),
        FnProbe(move || health.clone()),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_quorum_loss_rejects_mutations_but_reads_and_data_continue() {
    use homekv::movement::CatalogPort as _;
    let harness = Harness::new(MovementPhase::Intent);
    harness.ack_write(b"gate-key", b"gate-value").await;

    // Catalog quorum lost: every placement mutation is refused...
    let gated = gated_catalog(&harness, CatalogHealth::QuorumLost);
    let rejected = gated
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: 1,
            operation_id: [8; 16],
            shard_id: SHARD,
            target_voters: harness.catalog.target_voters,
        })
        .await;
    assert!(
        matches!(rejected, Err(CatalogGroupError::ConsensusUnavailable)),
        "unexpected: {rejected:?}"
    );
    let rejected = gated
        .submit(CatalogCommand::CancelMovement {
            expected_epoch: 1,
            operation_id: OP_ID,
            shard_id: SHARD,
            observed_voters: harness.catalog.source_voters,
            reason: "gate check".to_string(),
        })
        .await;
    assert!(matches!(
        rejected,
        Err(CatalogGroupError::ConsensusUnavailable)
    ));

    // ...while the last committed catalog view stays readable...
    let state: CatalogState = gated.read_committed().await.expect("reads keep working");
    assert_eq!(
        state.placements[&SHARD].voters,
        harness.catalog.source_voters
    );

    // ...and the stable data groups continue serving under their committed
    // memberships (the gate only guards the catalog write path).
    harness.ack_write(b"gate-key-2", b"gate-value-2").await;
    assert_eq!(
        harness.strong_read(b"gate-key-2").await,
        Some(b"gate-value-2".to_vec())
    );

    // Unknown health fails closed the same way; Degraded still allows writes.
    let gated = gated_catalog(&harness, CatalogHealth::Unknown);
    assert!(matches!(
        gated
            .submit(CatalogCommand::BeginMovement {
                expected_epoch: 1,
                operation_id: [8; 16],
                shard_id: SHARD,
                target_voters: harness.catalog.target_voters,
            })
            .await,
        Err(CatalogGroupError::ConsensusUnavailable)
    ));
    let gated = gated_catalog(
        &harness,
        CatalogHealth::Degraded {
            unavailable_nodes: Vec::new(),
        },
    );
    let degraded = gated
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: 1,
            operation_id: [9; 16],
            shard_id: SHARD,
            target_voters: harness.catalog.target_voters,
        })
        .await;
    assert!(
        matches!(
            degraded,
            Err(CatalogGroupError::InvalidPlacement(
                PlacementError::ConflictingMovement { .. }
            ))
        ),
        "degraded catalog still takes writes (rejected here only by the active movement): {degraded:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_leader_change_keeps_single_movement_identity() {
    // Two consecutive catalog leader changes (each submit fails as if the
    // leader were unknown) while the publish is in flight.
    let harness = Harness::new(MovementPhase::Remove);
    let target: BTreeSet<u64> = harness.catalog.target_voters.iter().copied().collect();
    harness.set_observed(target, BTreeSet::new()).await;
    let removed = harness.catalog.removed();
    harness.ack_write(b"leader-key", b"leader-value").await;
    for _ in 0..2 {
        harness
            .catalog
            .fail_next_submit(
                Some(CommandKind::Publish),
                CatalogGroupError::ConsensusUnavailable,
            )
            .await;
    }
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    // One movement identity across both leader changes: exactly one publish.
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "a second leader must not fork a second movement"
    );
    assert!(driver.tombstone(SHARD, &OP_ID).await.is_some());
}

// ---------------------------------------------------------------------------
// §8 rows: planner/scheduler restart and gossip
// ---------------------------------------------------------------------------

/// Build a committed catalog with several shards and skewed voters so the
/// planner has something to propose.
async fn skewed_catalog() -> (Harness, CatalogState) {
    let harness = Harness::new(MovementPhase::Intent);
    // Cancel the default movement: the planner only reads stable placements.
    let removed = harness.catalog.removed();
    let driver = harness.driver(removed, 64);
    assert_eq!(
        driver
            .request_cancel(SHARD, "planner fixture".to_string())
            .await,
        homekv::movement::CancelOutcome::Cancelled
    );
    let state = harness.catalog.read_committed().await.expect("readable");
    (harness, state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planner_restart_is_deterministic() {
    let (_harness, state) = skewed_catalog().await;
    let unavailable: BTreeSet<RaftNodeId> = BTreeSet::from([4]);
    let input = RebalanceInput {
        catalog: &state,
        unavailable_nodes: &unavailable,
    };
    // A restarted planner sees the same committed view and derives the same
    // plan, down to the movement operation identities.
    let first = plan_rebalance(input).expect("planner succeeds");
    let second = plan_rebalance(RebalanceInput {
        catalog: &state,
        unavailable_nodes: &unavailable,
    })
    .expect("planner succeeds after restart");
    assert_eq!(first, second, "planner must be deterministic");
    for movement in &first.movements {
        assert_eq!(
            movement.operation_id,
            movement_operation_id(&state.cluster_id, state.placement_epoch, movement.shard_id),
            "operation id derived deterministically from plan coordinates"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gossip_divergence_does_not_mutate_committed_placement() {
    let (_harness, state) = skewed_catalog().await;
    let before = state.clone();
    // Divergent gossip: a view that disagrees with reality (unknown nodes,
    // wrong unavailability) is advisory input only.
    let divergent: BTreeSet<RaftNodeId> = BTreeSet::from([4, 99, 100]);
    let plan = plan_rebalance(RebalanceInput {
        catalog: &state,
        unavailable_nodes: &divergent,
    })
    .expect("planner tolerates divergent gossip");
    assert_eq!(
        state, before,
        "planning must not mutate the committed catalog"
    );
    // And the same divergent input replays to the same advisory plan.
    let replay = plan_rebalance(RebalanceInput {
        catalog: &state,
        unavailable_nodes: &divergent,
    })
    .expect("planner replays");
    assert_eq!(plan, replay);
    // Unknown gossip-only nodes never become planned voters.
    for movement in &plan.movements {
        for voter in movement.target_voters {
            assert!(
                state.eligible_nodes.contains_key(&voter),
                "gossip must not invent voters: {voter}"
            );
        }
    }
}

/// Minimal [`RebalanceDrive`] over the deterministic harness: begins submit
/// the intent through the fake catalog and drives run the real
/// [`MovementDriver`] to completion.
struct SchedulerDrive {
    harness: Harness,
    begins: std::sync::Arc<Mutex<Vec<u16>>>,
}

#[async_trait]
impl RebalanceDrive for SchedulerDrive {
    async fn begin_movement(
        &self,
        movement: &PlannedMovement,
        expected_epoch: u64,
    ) -> Result<BeginOutcome, String> {
        self.begins.lock().unwrap().push(movement.shard_id);
        match self
            .harness
            .catalog
            .submit(CatalogCommand::BeginMovement {
                expected_epoch,
                operation_id: movement.operation_id,
                shard_id: movement.shard_id,
                target_voters: movement.target_voters,
            })
            .await
        {
            Ok(CatalogResponse::MovementAccepted { .. }) => Ok(BeginOutcome::Accepted),
            Ok(CatalogResponse::MovementAlreadyActive { .. }) => Ok(BeginOutcome::AlreadyActive),
            Ok(other) => Ok(BeginOutcome::Rejected(format!("{other:?}"))),
            Err(CatalogGroupError::InvalidPlacement(PlacementError::StaleCatalogView {
                ..
            })) => Ok(BeginOutcome::StaleEpoch),
            Err(CatalogGroupError::InvalidPlacement(PlacementError::ConflictingMovement {
                ..
            })) => Ok(BeginOutcome::Rejected("conflicting movement".to_string())),
            Err(error) => Err(error.to_string()),
        }
    }

    async fn drive_movement(&self, shard_id: u16) -> DriverOutcome {
        let driver = self.harness.driver(self.harness.catalog.removed(), 64);
        driver.drive_shard(shard_id).await
    }

    async fn reassign_leader(
        &self,
        _shard_id: u16,
        _new_leader: RaftNodeId,
        _expected_epoch: u64,
    ) -> Result<ReassignOutcome, String> {
        Ok(ReassignOutcome::AlreadySet)
    }

    async fn cancel_movement(&self, _shard_id: u16, _reason: String) -> CancelOutcome {
        CancelOutcome::NothingToCancel
    }

    async fn read_catalog(&self) -> Result<CatalogState, String> {
        self.harness
            .catalog
            .read_committed()
            .await
            .map_err(|error| error.to_string())
    }
}

async fn single_shard_plan(harness: &Harness) -> RebalancePlan {
    let state = harness.catalog.read_committed().await.expect("readable");
    RebalancePlan {
        cluster_id: state.cluster_id,
        source_epoch: state.placement_epoch,
        movements: vec![PlannedMovement {
            shard_id: SHARD,
            operation_id: OP_ID,
            source_voters: harness.catalog.source_voters,
            target_voters: harness.catalog.target_voters,
            reason: MoveReason::SkewReduction,
        }],
        leader_reassignments: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_restart_recovers_without_duplication() {
    // A fresh catalog with no active movement: the scheduler begins the
    // planned intent exactly once.
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    let driver = harness.driver(removed, 64);
    assert_eq!(
        driver
            .request_cancel(SHARD, "scheduler fixture".to_string())
            .await,
        homekv::movement::CancelOutcome::Cancelled
    );
    let plan = single_shard_plan(&harness).await;
    let config = RebalanceSchedulerConfig::default();

    // Simulate the crash mid-plan: drop the scheduler without polling.
    let begins: std::sync::Arc<Mutex<Vec<u16>>> = Default::default();
    let drive = SchedulerDrive {
        harness: harness.clone(),
        begins: begins.clone(),
    };
    let scheduler = RebalanceScheduler::new(plan.clone(), config.clone(), drive, BTreeSet::new())
        .expect("scheduler builds");
    // Simulate the crash mid-plan: drop the scheduler without polling.
    drop(scheduler);

    // The restarted scheduler reconciles with the committed catalog and
    // resumes: nothing completed, nothing begun, so the shard is queued.
    let drive = SchedulerDrive {
        harness: harness.clone(),
        begins: begins.clone(),
    };
    let mut scheduler =
        RebalanceScheduler::new(plan.clone(), config.clone(), drive, BTreeSet::new())
            .expect("scheduler rebuilds");
    let catalog = harness.catalog.read_committed().await.expect("readable");
    scheduler.recover(&catalog);
    assert_eq!(
        scheduler.status().shards.get(&SHARD),
        Some(&PlanShardState::Queued),
        "in-flight work resumes as queued, not failed"
    );

    // One poll begins and drives the movement to completion...
    scheduler.poll().await;
    assert_eq!(
        begins.lock().unwrap().as_slice(),
        &[SHARD],
        "the intent is begun exactly once"
    );
    let catalog = harness.catalog.read_committed().await.expect("readable");
    let placement = &catalog.placements[&SHARD];
    assert!(placement.pending_movement.is_none());
    assert_eq!(placement.voters, harness.catalog.target_voters);

    // ...and a second restart adopts the committed result instead of
    // re-driving: no duplicate intent.
    let drive = SchedulerDrive {
        harness: harness.clone(),
        begins: begins.clone(),
    };
    let mut scheduler = RebalanceScheduler::new(plan.clone(), config, drive, BTreeSet::new())
        .expect("scheduler rebuilds again");
    let catalog = harness.catalog.read_committed().await.expect("readable");
    scheduler.recover(&catalog);
    assert_eq!(
        scheduler.status().shards.get(&SHARD),
        Some(&PlanShardState::Completed),
        "completed work stays completed across the restart"
    );
    scheduler.poll().await;
    assert_eq!(
        begins.lock().unwrap().as_slice(),
        &[SHARD],
        "the restarted scheduler must not re-begin a second movement"
    );
    assert_eq!(
        harness.catalog.publish_count.load(Ordering::SeqCst),
        1,
        "exactly one publish across both scheduler lifetimes"
    );
}

// ---------------------------------------------------------------------------
// §8 rows: slow movement, snapshots
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_movement_stays_bounded_while_traffic_progresses() {
    let harness = Harness::new(MovementPhase::Intent);
    let removed = harness.catalog.removed();
    // The target stays unavailable for a long time: every learner admission
    // fails, so the movement crawls.
    harness
        .operator
        .fail_times(
            FaultPoint::AddLearner,
            40,
            OperatorError::Transient("target unavailable".to_string()),
        )
        .await;
    harness.ack_write(b"slow-0", b"value-0").await;

    // Small per-drive attempt bound, like a controller with a tight tick
    // budget. Foreground traffic keeps getting acknowledged between drives.
    let driver = harness.driver(removed, 5);
    let mut rounds = 0;
    for i in 1..=6u32 {
        let outcome = driver.drive_shard(SHARD).await;
        assert!(
            matches!(outcome, DriverOutcome::TransientFailure { .. }),
            "slow movement must stay retryable, never fail closed: {outcome:?}"
        );
        harness
            .ack_write(format!("slow-{i}").as_bytes(), b"value".as_ref())
            .await;
        rounds += 1;
    }
    assert_eq!(rounds, 6);
    let metrics = driver.metrics().snapshot();
    let total_attempts: u64 = metrics.phase_attempts.values().sum();
    assert!(
        total_attempts <= 6 * 5,
        "phase attempts bounded by rounds x per-drive bound: {total_attempts}"
    );
    assert_eq!(
        metrics.operations_fail_closed, 0,
        "a slow movement must not fail closed"
    );
    assert!(
        harness
            .catalog
            .read_committed()
            .await
            .expect("readable")
            .placements[&SHARD]
            .pending_movement
            .is_some(),
        "the movement is still active, not dropped"
    );

    // The target recovers: the same intent converges, and every write from
    // the slow window survived.
    let driver = harness.driver(removed, 64);
    harness.drive_until_completed(&driver).await;
    harness.assert_converged().await;
    assert_eq!(harness.data.acked().await.len(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_and_version_incompatible_snapshots_fail_closed() {
    let harness = Harness::new(MovementPhase::CatchUp);
    let catalog = harness.catalog.read_committed().await.expect("readable");
    let image = harness
        .catalog
        .encode_snapshot()
        .await
        .expect("encode succeeds");

    // Corrupted payload fails closed (last byte is inside the payload, past
    // the 28-byte header, so the checksum catches it)...
    let mut corrupt = image.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xFF;
    assert!(matches!(
        PlacementCatalog::restore_snapshot(&corrupt),
        Err(PlacementError::SnapshotChecksumMismatch)
    ));

    // ...a version-incompatible image fails closed (version lives at
    // bytes[8..10], little-endian)...
    let mut wrong_version = image.clone();
    wrong_version[8] = 0xFF;
    wrong_version[9] = 0xFF;
    assert!(matches!(
        PlacementCatalog::restore_snapshot(&wrong_version),
        Err(PlacementError::UnsupportedSnapshotVersion(_))
    ));

    // ...a truncated envelope fails closed...
    assert!(matches!(
        PlacementCatalog::restore_snapshot(&image[..8]),
        Err(PlacementError::InvalidSnapshotEnvelope)
    ));

    // ...and no partial state escapes: the valid image still restores
    // exactly the committed catalog afterwards.
    let restored = PlacementCatalog::restore_snapshot(&image).expect("valid image restores");
    let restored_state = restored.state().expect("restored state").clone();
    assert_eq!(restored_state.placement_epoch, catalog.placement_epoch);
    assert_eq!(
        restored_state.placements[&SHARD].voters,
        catalog.placements[&SHARD].voters
    );
    assert_eq!(
        restored_state.placements[&SHARD]
            .pending_movement
            .as_ref()
            .expect("pending movement restored")
            .phase,
        MovementPhase::CatchUp
    );
}
