//! Spec 0006 verification §8 / REQ-M4-FAIL-004 — data-group log and snapshot
//! corruption fails closed.
//!
//! | Fault | Required invariant | Test |
//! |---|---|---|
//! | data-group log store checksum corruption | affected replica fails closed; 2/3 quorum serves; heal via janitor | `data_group_log_checksum_corruption_fails_closed` |
//! | data-group log store truncation (torn write) | affected replica fails closed; 2/3 quorum serves; heal via janitor | `data_group_log_truncation_fails_closed` |
//! | data-group log store version corruption | affected replica fails closed; 2/3 quorum serves; heal via janitor | `data_group_log_version_corruption_fails_closed` |
//! | data-group snapshot artifact corruption | affected replica fails closed; 2/3 quorum serves; heal replays intact log | `data_group_snapshot_corruption_fails_closed` |
//! | corrupt data replica store under the production composition root | `PlacementNode::start` fails closed (clean error, no panic); janitor removal + restart heals | `placement_node_start_fails_closed_on_corrupt_data_replica_store` |
//!
//! Every test runs a real 3-voter data Raft group: real [`Raft`] instances,
//! real disk-backed [`HomeKvRaftLogStore`]s (one file per replica, the exact
//! layout the production [`PlacementNode`] uses), and the real in-process
//! Raft transport. Corruption is real on-disk mutation followed by a real
//! restart — never timing-only: assertions read committed/applied key/value
//! state and acknowledged writes.
//!
//! Honest scope notes (recorded, not hidden):
//!
//! - The data-group state machine snapshot artifact is only written to disk
//!   when a snapshot path is configured. The production [`PlacementNode`]
//!   wires data-group state machines with `snapshot_path: None` (in-memory
//!   snapshots), so the on-disk snapshot corruption test covers the
//!   library's on-disk snapshot format (fail-closed decode, already
//!   unit-tested in `raft.rs`) at group level, using a configured snapshot
//!   path. The production on-disk fault surface for data groups is exactly
//!   the M5 segmented-WAL store directory per replica (`meta` atomic image +
//!   `wal/` segments).
//! - `PlacementNode::start` fails the whole node (clean
//!   [`PlacementNodeError::Io`]) when any replica's store is corrupt, rather
//!   than isolating the bad group. That is honest fail-closed — the corrupt
//!   replica can never serve — and is asserted as the designed behavior.
//!
//! Traceability: REQ-M4-FAIL-004 ("Corrupt placement, data-group log, or
//! snapshot state MUST fail closed for the affected authority; it MUST NOT be
//! repaired by accepting cached client or gossip claims") and the §8 row
//! "corrupt data/catalog artifact → affected authority fails closed".

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::movement::LocalReplicaJanitor;
use homekv::placement::CatalogState;
use homekv::placement_node::{
    FsReplicaJanitor, PlacementNode, PlacementNodeConfig, PlacementNodeError,
};
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftNode, RaftNodeId};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{Config, Raft, ServerState};

const SHARD: u16 = 5;
const SHARD_B: u16 = 8;

/// Serializes the real-cluster tests: each spins up a 3-instance Raft group
/// with elections, and concurrent groups oversubscribe small CI runners.
static REAL_CLUSTER_GUARD: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

fn unique_dir(tag: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-fail004-{tag}-{}-{nonce}",
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

#[derive(Clone, Copy)]
enum CorruptMode {
    Checksum,
    Truncate,
    Version,
}

struct DataGroupNode {
    raft: Raft<HomeKvRaftConfig>,
    state_machine: Arc<HomeKvStateMachine>,
}

/// A real 3-voter data Raft group with per-node data directories, real
/// disk-backed [`HomeKvRaftLogStore`]s and the real in-process Raft
/// transport — the lightest faithful equivalent of one production data
/// group spread across three processes.
///
/// Replica `id`'s store lives at `<root>/node-<id>/groups/<shard:04>/`
/// `node-<id>.raft`: the same store directory layout the production
/// [`PlacementNode`] composition root uses, so [`FsReplicaJanitor`] (the
/// production janitor) operates on it directly.
struct DataGroupCluster {
    root: PathBuf,
    shard: u16,
    ids: Vec<RaftNodeId>,
    links: TestLinkController,
    bootstrap: ThreeNodeBootstrap,
    raft_config: Arc<Config>,
    factories: BTreeMap<RaftNodeId, HomeKvRaftNetworkFactory>,
    nodes: BTreeMap<RaftNodeId, DataGroupNode>,
    snapshot_files: bool,
}

impl DataGroupCluster {
    async fn start(tag: &str, shard: u16, snapshot_files: bool, port_base: u16) -> Self {
        let ids = vec![1u64, 2, 3];
        let links = TestLinkController::default();
        let bootstrap = ThreeNodeBootstrap::new_extended(
            format!("homekv-fail004-{tag}"),
            ids.iter().map(|id| BootstrapNode {
                id: *id,
                raft_endpoint: format!("127.0.0.1:{}", port_base + *id as u16),
            }),
        )
        .expect("bootstrap builds");
        let raft_config = Arc::new(
            Config {
                cluster_name: format!("homekv-fail004-{tag}"),
                heartbeat_interval: 100,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .expect("raft config validates"),
        );
        let mut cluster = Self {
            root: unique_dir(tag),
            shard,
            ids: ids.clone(),
            links,
            bootstrap,
            raft_config,
            factories: BTreeMap::new(),
            nodes: BTreeMap::new(),
            snapshot_files,
        };
        // Two phases, mirroring `start_replicas`: every node's network
        // factory first, then every replica, then cross-registration so the
        // in-process transport routes between the "processes".
        for id in &ids {
            let factory = HomeKvRaftNetworkFactory::new_extended(
                *id,
                cluster.bootstrap.clone(),
                64,
                cluster.links.clone(),
            )
            .expect("network factory builds");
            cluster.factories.insert(*id, factory);
        }
        for id in &ids {
            cluster.start_node(*id).await.expect("node starts");
        }
        // Initialize once via node 1; restarts never re-initialize (a
        // pristine rejoining replica must not seed a divergent incarnation).
        let members: BTreeMap<RaftNodeId, RaftNode> = ids
            .iter()
            .map(|id| {
                let endpoint = cluster.bootstrap.nodes[id].raft_endpoint.clone();
                (*id, RaftNode::new(endpoint))
            })
            .collect();
        match cluster.nodes[&1].raft.initialize(members).await {
            Ok(_) => {}
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {}
            Err(e) => panic!("group initialize failed: {e:?}"),
        }
        cluster
    }

    /// Start (or restart) one replica. A corrupt on-disk artifact fails here
    /// with a clean error — no Raft instance is created, so the replica can
    /// never serve or acknowledge operations.
    async fn start_node(&mut self, id: RaftNodeId) -> Result<(), String> {
        let store = HomeKvRaftLogStore::open(self.store_path(id))
            .map_err(|e| format!("node {id} log store open failed: {e}"))?;
        let state_machine = Arc::new(self.open_state_machine(id)?);
        let factory = self.factories.get(&id).expect("factory exists").clone();
        let raft = Raft::new(
            id,
            self.raft_config.clone(),
            factory,
            store,
            (*state_machine).clone(),
        )
        .await
        .map_err(|e| format!("node {id} raft::new failed: {e:?}"))?;
        // (Re-)register this replica's handler on every factory so peers
        // route to the live instance; insert overwrites the pre-restart
        // entry left behind by a shut-down replica.
        for factory in self.factories.values() {
            factory
                .register_handler(id, Arc::new(raft.clone()))
                .map_err(|e| format!("node {id} handler registration failed: {e:?}"))?;
        }
        self.nodes.insert(
            id,
            DataGroupNode {
                raft,
                state_machine,
            },
        );
        Ok(())
    }

    fn open_state_machine(&self, id: RaftNodeId) -> Result<HomeKvStateMachine, String> {
        if self.snapshot_files {
            HomeKvStateMachine::open(self.snapshot_path(id))
                .map_err(|e| format!("node {id} state machine open failed: {e:?}"))
        } else {
            Ok(HomeKvStateMachine::default())
        }
    }

    fn node_dir(&self, id: RaftNodeId) -> PathBuf {
        self.root.join(format!("node-{id}"))
    }

    fn store_path(&self, id: RaftNodeId) -> PathBuf {
        self.node_dir(id)
            .join("groups")
            .join(format!("{:04}", self.shard))
            .join(format!("node-{id}.raft"))
    }

    fn snapshot_path(&self, id: RaftNodeId) -> PathBuf {
        self.node_dir(id)
            .join("groups")
            .join(format!("{:04}", self.shard))
            .join(format!("node-{id}.snapshot"))
    }

    async fn shutdown_node(&mut self, id: RaftNodeId) {
        if let Some(node) = self.nodes.remove(&id) {
            node.raft.shutdown().await.expect("replica shuts down");
        }
    }

    async fn shutdown_all(&mut self) {
        for id in self.ids.clone() {
            self.shutdown_node(id).await;
        }
    }

    fn leader(&self) -> Option<RaftNodeId> {
        self.nodes.iter().find_map(|(id, node)| {
            (node.raft.metrics().borrow().state == ServerState::Leader).then_some(*id)
        })
    }

    /// Write through the current leader; the write is acknowledged only on
    /// quorum commit. Retries across leader changes: right after a restart
    /// the locally observed leader can be stale, in which case the write is
    /// answered with `ForwardToLeader` and must be re-resolved.
    async fn write(&self, key: &[u8], value: &[u8]) {
        let request = RaftCommand::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        wait_for(
            || async {
                let leader = self.leader()?;
                match self.nodes[&leader].raft.client_write(request.clone()).await {
                    Ok(_) => Some(()),
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => None,
                    Err(other) => panic!("write failed: {other:?}"),
                }
            },
            Duration::from_secs(20),
            "write commits through quorum",
        )
        .await;
    }

    /// Assert committed key/value state is applied on the given replicas'
    /// state machines — never timing-only.
    async fn wait_applied(&self, ids: &[RaftNodeId], key: &[u8], value: &[u8], what: &str) {
        wait_for(
            || async {
                let mut ok = true;
                for id in ids {
                    let Some(node) = self.nodes.get(id) else {
                        ok = false;
                        break;
                    };
                    if node.state_machine.get(key).await != Some(value.to_vec()) {
                        ok = false;
                    }
                }
                ok.then_some(())
            },
            Duration::from_secs(20),
            what,
        )
        .await;
    }

    /// Corrupt one replica's on-disk log store the way a torn write, bit
    /// rot, or format-version skew would. Returns the message fragment the
    /// fail-closed error must contain.
    ///
    /// M5 segmented WAL: the corruption targets the atomic `meta` image
    /// (vote, committed position, segment inventory) inside the store
    /// directory.
    fn corrupt_store(&self, id: RaftNodeId, mode: CorruptMode) -> &'static str {
        let path = self.store_path(id).join("meta");
        let mut bytes = std::fs::read(&path).expect("store meta image exists");
        let expect = match mode {
            CorruptMode::Checksum => {
                let mid = bytes.len() / 2;
                assert!(mid > 28, "store image must extend past the 28-byte header");
                bytes[mid] ^= 0x5a;
                "checksum"
            }
            CorruptMode::Truncate => {
                bytes.truncate(bytes.len() - 1);
                "truncated or overlong"
            }
            CorruptMode::Version => {
                bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
                "unsupported Raft storage format version"
            }
        };
        std::fs::write(&path, bytes).expect("corrupt store written");
        expect
    }

    async fn trigger_snapshot(&self, id: RaftNodeId) {
        self.nodes[&id]
            .raft
            .trigger()
            .snapshot()
            .await
            .expect("snapshot trigger accepted");
        // The trigger future resolves at initiation; wait for the durable
        // artifact itself — the test corrupts the file, not the future.
        wait_for(
            || async { self.snapshot_path(id).exists().then_some(()) },
            Duration::from_secs(20),
            "snapshot artifact",
        )
        .await;
    }
}

/// Shared body for the three log-store corruption modes: checksum bit-flip,
/// truncation (torn write), and version-byte corruption.
async fn log_corruption_fails_closed(mode: CorruptMode, tag: &str, port_base: u16) {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let mut cluster = DataGroupCluster::start(tag, SHARD, false, port_base).await;

    // Baseline: a write is acknowledged through quorum and applied on every
    // replica before the fault.
    cluster.write(b"fail004-baseline", b"v1").await;
    cluster
        .wait_applied(
            &[1, 2, 3],
            b"fail004-baseline",
            b"v1",
            "pre-fault replication",
        )
        .await;

    // The fault: node 3's process stops, then its on-disk store is corrupted.
    cluster.shutdown_node(3).await;
    let expect = cluster.corrupt_store(3, mode);

    // Fail closed: the store refuses to open with a clean error (no panic),
    // so node 3 never gets a Raft instance and never serves or acknowledges
    // operations.
    let err = cluster
        .start_node(3)
        .await
        .expect_err("corrupt store must fail closed");
    assert!(
        err.contains(expect),
        "fail-closed error must name the corruption, got: {err}"
    );
    assert!(
        !cluster.nodes.contains_key(&3),
        "corrupt replica must never serve"
    );

    // The surviving 2/3 quorum keeps serving: a new write is acknowledged and
    // applied while the corrupt replica is down.
    cluster.write(b"fail004-degraded", b"v2").await;
    cluster
        .wait_applied(
            &[1, 2],
            b"fail004-degraded",
            b"v2",
            "quorum progress with corrupt replica down",
        )
        .await;

    // Recovery via the janitor path: the unrecoverable artifact is removed
    // (never "repaired" from cached client or gossip claims). Recovery is a
    // full node restart: the production deployment hosts every replica
    // identity in one process, so a restart also clears all in-memory
    // replication progress and the pristine replica catches up from the
    // healthy quorum's durable state. (A wiped voter rejoining under the same
    // live leader that still holds its pre-wipe replication progress trips
    // openraft's debug-only "follower log reversion" assertion — a Raft voter
    // must not lose its log under a live leader — so rolling rejoin is not
    // a supported recovery; the snapshot test below covers the rolling case
    // where the log itself survives.)
    FsReplicaJanitor::new(cluster.node_dir(3), 3)
        .remove_local_replica(SHARD)
        .await
        .expect("janitor removes the corrupt replica artifact");
    assert!(
        !cluster.store_path(3).exists(),
        "corrupt artifact must be gone"
    );
    cluster.shutdown_node(1).await;
    cluster.shutdown_node(2).await;
    for id in [1u64, 2, 3] {
        cluster
            .start_node(id)
            .await
            .expect("replica restarts after janitor removal");
    }
    cluster
        .wait_applied(
            &[3],
            b"fail004-baseline",
            b"v1",
            "recovered replica replays baseline",
        )
        .await;
    cluster
        .wait_applied(
            &[3],
            b"fail004-degraded",
            b"v2",
            "recovered replica replays degraded-window write",
        )
        .await;

    // The healed group commits as a full 3-voter group again.
    cluster.write(b"fail004-healed", b"v3").await;
    cluster
        .wait_applied(
            &[1, 2, 3],
            b"fail004-healed",
            b"v3",
            "post-heal replication",
        )
        .await;

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_log_checksum_corruption_fails_closed() {
    log_corruption_fails_closed(CorruptMode::Checksum, "checksum", 32210).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_log_truncation_fails_closed() {
    log_corruption_fails_closed(CorruptMode::Truncate, "truncate", 32220).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_log_version_corruption_fails_closed() {
    log_corruption_fails_closed(CorruptMode::Version, "version", 32230).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_group_snapshot_corruption_fails_closed() {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let mut cluster = DataGroupCluster::start("snapshot", SHARD, true, 32240).await;

    cluster.write(b"fail004-snap", b"v1").await;
    cluster
        .wait_applied(&[1, 2, 3], b"fail004-snap", b"v1", "pre-fault replication")
        .await;

    // Build a real on-disk snapshot artifact on node 3, then stop it and
    // corrupt the artifact (bit flip inside the checksummed payload).
    cluster.trigger_snapshot(3).await;
    cluster.shutdown_node(3).await;
    let path = cluster.snapshot_path(3);
    let mut bytes = std::fs::read(&path).expect("snapshot artifact exists");
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, bytes).expect("corrupt snapshot written");

    // Fail closed: the state machine refuses the corrupt artifact with a
    // clean error — the replica never starts, never serves.
    let err = cluster
        .start_node(3)
        .await
        .expect_err("corrupt snapshot must fail closed");
    assert!(
        err.contains("snapshot"),
        "fail-closed error must name the snapshot, got: {err}"
    );
    assert!(
        !cluster.nodes.contains_key(&3),
        "corrupt replica must never serve"
    );

    // The surviving 2/3 quorum keeps serving while the corrupt replica is
    // down.
    cluster.write(b"fail004-snap-2", b"v2").await;
    cluster
        .wait_applied(&[1, 2], b"fail004-snap-2", b"v2", "quorum progress")
        .await;

    // Recovery: remove the corrupt artifact; the replica restarts with an
    // empty state machine and replays its INTACT log — recovered state comes
    // from the group's durable log, never from cached client or gossip
    // claims.
    std::fs::remove_file(&path).expect("remove corrupt snapshot");
    cluster
        .start_node(3)
        .await
        .expect("recovered replica restarts");
    cluster
        .wait_applied(
            &[3],
            b"fail004-snap",
            b"v1",
            "recovered replica replays its log",
        )
        .await;
    cluster
        .wait_applied(
            &[3],
            b"fail004-snap-2",
            b"v2",
            "recovered replica applies quorum writes",
        )
        .await;
    cluster.write(b"fail004-snap-3", b"v3").await;
    cluster
        .wait_applied(
            &[1, 2, 3],
            b"fail004-snap-3",
            b"v3",
            "post-heal replication",
        )
        .await;

    cluster.shutdown_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn placement_node_start_fails_closed_on_corrupt_data_replica_store() {
    let _guard = REAL_CLUSTER_GUARD.lock().await;
    let data_dir = unique_dir("placement-corrupt");
    let config = || PlacementNodeConfig {
        node_ids: vec![1, 2, 3, 4],
        catalog_voters: [1, 2, 3],
        data_dir: data_dir.clone(),
        cluster_id: *b"homekv-fail004pn",
        shards: vec![SHARD, SHARD_B],
        drive_interval: Duration::from_secs(60),
        reconcile_interval: Duration::from_secs(120),
        election_min_ms: 150,
        election_max_ms: 300,
        ..PlacementNodeConfig::default()
    };

    let node = PlacementNode::start(config()).await.expect("node starts");
    for shard in [SHARD, SHARD_B] {
        let leader = wait_for(
            || async { node.group_leader(shard) },
            Duration::from_secs(20),
            "data-group leader",
        )
        .await;
        node.group_raft(shard, leader)
            .expect("leader raft")
            .client_write(RaftCommand::Set {
                key: format!("pn-key-{shard}").into_bytes(),
                value: b"pn-value".to_vec(),
            })
            .await
            .expect("seed write commits");
    }
    // Baseline: every committed voter applied the seed write on both shards.
    let voters_of = |shard: u16, state: &CatalogState| state.placements[&shard].voters;
    let state = node.committed_state().await.expect("catalog readable");
    for shard in [SHARD, SHARD_B] {
        for id in voters_of(shard, &state) {
            let key = format!("pn-key-{shard}");
            wait_for(
                || async {
                    let sm = node.group_state_machine(shard, id)?;
                    (sm.get(key.as_bytes()).await == Some(b"pn-value".to_vec())).then_some(())
                },
                Duration::from_secs(20),
                "seed replication",
            )
            .await;
        }
    }
    // Deliberately corrupt voters[0]'s store: this exercises the
    // initialize-target fix — a pristine rejoining replica must not seed a
    // divergent incarnation on restart.
    let victim = voters_of(SHARD, &state)[0];
    node.shutdown();

    let store_path = data_dir
        .join("groups")
        .join(format!("{SHARD:04}"))
        .join(format!("node-{victim}.raft"));
    // M5 segmented WAL: corrupt the atomic metadata image (the last byte is
    // inside the checksummed payload, so this deterministically trips the
    // checksum gate and fails closed on open).
    let meta_path = store_path.join("meta");
    let mut bytes = std::fs::read(&meta_path).expect("replica meta image exists");
    let last = bytes.len() - 1;
    bytes[last] ^= 0x5a;
    std::fs::write(&meta_path, bytes).expect("corrupt store written");

    // The production composition root fails closed: a clean Io error naming
    // the corruption — no panic, and the node never finishes starting, so
    // the corrupt replica can never serve or acknowledge operations.
    // (`PlacementNode` is not `Debug`, so `expect_err` cannot be used.)
    let err = match PlacementNode::start(config()).await {
        Err(err) => err,
        Ok(_) => panic!("node must fail closed on corrupt data replica store"),
    };
    assert!(
        matches!(err, PlacementNodeError::Io(_)),
        "fail-closed must surface as Io, got: {err}"
    );
    assert!(
        err.to_string().contains("checksum"),
        "error must name the corruption, got: {err}"
    );

    // Janitor path removes the unrecoverable artifact (the M5 store is a
    // directory, so `remove_dir_all` removes it directly).
    FsReplicaJanitor::new(data_dir.clone(), victim)
        .remove_local_replica(SHARD)
        .await
        .expect("janitor removes the corrupt replica directory");
    assert!(!store_path.exists(), "corrupt artifact must be gone");

    // Restart: the whole node comes back — catalog, both shards, every
    // pre-corruption write intact — and the healed replica rejoins through
    // the healthy quorum instead of seeding a divergent incarnation.
    let node = PlacementNode::start(config())
        .await
        .expect("node restarts after janitor removal");
    let state = node.committed_state().await.expect("catalog readable");
    assert!(state.placements.contains_key(&SHARD));
    assert!(state.placements.contains_key(&SHARD_B));
    for shard in [SHARD, SHARD_B] {
        let leader = wait_for(
            || async { node.group_leader(shard) },
            Duration::from_secs(30),
            "leader after heal",
        )
        .await;
        for id in voters_of(shard, &state) {
            let key = format!("pn-key-{shard}");
            wait_for(
                || async {
                    let sm = node.group_state_machine(shard, id)?;
                    (sm.get(key.as_bytes()).await == Some(b"pn-value".to_vec())).then_some(())
                },
                Duration::from_secs(30),
                "healed replication",
            )
            .await;
        }
        node.group_raft(shard, leader)
            .expect("leader raft")
            .client_write(RaftCommand::Set {
                key: format!("pn-key-{shard}-post").into_bytes(),
                value: b"pn-post".to_vec(),
            })
            .await
            .expect("post-heal write commits");
    }
    node.shutdown();
}
