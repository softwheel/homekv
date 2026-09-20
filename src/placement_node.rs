//! Production placement-node composition root.
//!
//! Blocker 3 of the spec-0006 verification reconciliation: a real binary must
//! instantiate the movement machinery instead of leaving `MovementDriver` as
//! a library-only type exercised solely by tests.
//!
//! [`PlacementNode`] composes, in one deployable unit:
//!
//! - the catalog Raft group ([`PlacementCatalogGroup`], 3 voters),
//! - one data Raft group per hosted shard (all local node identities hold a
//!   replica; the committed voters come from the catalog),
//! - one [`MovementDriver`] per hosted (shard, node identity) pair, built on
//!   the real [`LiveMembershipOperator`],
//! - a filesystem-backed [`FsReplicaJanitor`] per node identity,
//! - a background drive loop that polls the committed catalog and drives
//!   pending movements through the current leader's driver, plus periodic
//!   local-replica reconciliation.
//!
//! The `homekv` binary runs this composition in `--placement` mode.
//!
//! Deployment model: this process hosts several Raft node identities
//! (default 1, 2, 3 plus warm-standby 4) with the in-process Raft transport,
//! real disk-backed log stores, and real consensus. A warm-standby identity
//! holds idle replicas so a movement can add it as a learner without a
//! separate provisioning step. One driver per (shard, identity) mirrors a
//! multi-process deployment where each process hosts one identity and runs
//! one driver per shard; the drive loop routes each pending movement to the
//! current leader's driver.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use openraft::{Config, ServerState};
use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::movement::{
    LiveMembershipOperator, LocalReplicaJanitor, MovementDriver, MovementDriverConfig,
    MovementMetrics,
};
use crate::movement_admission::{MovementWorkAdmission, MovementWorkConfig};
use crate::placement::{CatalogState, EligibleNode};
use crate::placement_raft::PlacementCatalogGroup;
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode, RaftNodeId};
use crate::raft_network::HomeKvRaftNetworkFactory;
use crate::raft_observability::HomeKvReplicaObserver;
use crate::raft_storage::HomeKvRaftLogStore;
use crate::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use crate::routing::RouteRedirectMetrics;
use openraft::Raft;

/// Configuration for [`PlacementNode`].
#[derive(Clone, Debug)]
pub struct PlacementNodeConfig {
    /// All node identities this process hosts (catalog voters + data replicas
    /// + warm standbys). Default `[1, 2, 3, 4]`.
    pub node_ids: Vec<RaftNodeId>,
    /// The three catalog Raft voters; must be a subset of `node_ids`.
    pub catalog_voters: [RaftNodeId; 3],
    /// Directory holding catalog and data-group state.
    pub data_dir: PathBuf,
    /// Cluster identity committed to the catalog at bootstrap.
    pub cluster_id: [u8; 16],
    /// Shards whose data groups this node hosts and drives.
    pub shards: Vec<u16>,
    /// How often the drive loop polls the committed catalog.
    pub drive_interval: Duration,
    /// How often the drive loop reconciles local replicas.
    pub reconcile_interval: Duration,
    /// Movement admission limits.
    pub max_concurrent_movements: usize,
    pub per_node_max_concurrent: usize,
    /// Raft timing.
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Default for PlacementNodeConfig {
    fn default() -> Self {
        Self {
            node_ids: vec![1, 2, 3, 4],
            catalog_voters: [1, 2, 3],
            data_dir: PathBuf::from("./homekv-placement"),
            cluster_id: *b"homekv-place0001",
            shards: vec![0],
            drive_interval: Duration::from_secs(1),
            reconcile_interval: Duration::from_secs(5),
            max_concurrent_movements: 4,
            per_node_max_concurrent: 2,
            heartbeat_ms: 100,
            election_min_ms: 500,
            election_max_ms: 1000,
        }
    }
}

/// Configuration errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlacementNodeError {
    EmptyNodeIds,
    CatalogVoterNotHosted { voter: RaftNodeId },
    DuplicateNodeId { node_id: RaftNodeId },
    EmptyShards,
    NonPositiveInterval { name: &'static str },
    ZeroAdmissionLimit { name: &'static str },
    InvertedElectionTimeout,
    Io(String),
    Raft(String),
    Catalog(String),
    Admission(String),
}

impl std::fmt::Display for PlacementNodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyNodeIds => write!(f, "placement node: no node identities configured"),
            Self::CatalogVoterNotHosted { voter } => {
                write!(
                    f,
                    "placement node: catalog voter {voter} is not a hosted identity"
                )
            }
            Self::DuplicateNodeId { node_id } => {
                write!(f, "placement node: duplicate node identity {node_id}")
            }
            Self::EmptyShards => write!(f, "placement node: no shards configured"),
            Self::NonPositiveInterval { name } => {
                write!(f, "placement node: {name} must be positive")
            }
            Self::ZeroAdmissionLimit { name } => {
                write!(f, "placement node: {name} must be positive")
            }
            Self::InvertedElectionTimeout => {
                write!(
                    f,
                    "placement node: election_min_ms must not exceed election_max_ms"
                )
            }
            Self::Io(reason) => write!(f, "placement node I/O: {reason}"),
            Self::Raft(reason) => write!(f, "placement node raft: {reason}"),
            Self::Catalog(reason) => write!(f, "placement node catalog: {reason}"),
            Self::Admission(reason) => write!(f, "placement node admission: {reason}"),
        }
    }
}

impl std::error::Error for PlacementNodeError {}

impl PlacementNodeConfig {
    pub fn validate(&self) -> Result<(), PlacementNodeError> {
        if self.node_ids.is_empty() {
            return Err(PlacementNodeError::EmptyNodeIds);
        }
        let mut seen = std::collections::BTreeSet::new();
        for id in &self.node_ids {
            if !seen.insert(id) {
                return Err(PlacementNodeError::DuplicateNodeId { node_id: *id });
            }
        }
        for voter in self.catalog_voters {
            if !seen.contains(&voter) {
                return Err(PlacementNodeError::CatalogVoterNotHosted { voter });
            }
        }
        if self.shards.is_empty() {
            return Err(PlacementNodeError::EmptyShards);
        }
        if self.drive_interval.is_zero() {
            return Err(PlacementNodeError::NonPositiveInterval {
                name: "drive_interval",
            });
        }
        if self.reconcile_interval.is_zero() {
            return Err(PlacementNodeError::NonPositiveInterval {
                name: "reconcile_interval",
            });
        }
        if self.max_concurrent_movements == 0 {
            return Err(PlacementNodeError::ZeroAdmissionLimit {
                name: "max_concurrent_movements",
            });
        }
        if self.per_node_max_concurrent == 0 {
            return Err(PlacementNodeError::ZeroAdmissionLimit {
                name: "per_node_max_concurrent",
            });
        }
        if self.election_min_ms > self.election_max_ms {
            return Err(PlacementNodeError::InvertedElectionTimeout);
        }
        Ok(())
    }
}

/// One hosted replica: the Raft handle plus its state machine.
///
/// The log store handle is retained so observability tooling (and tests)
/// can observe live durable replica state; it shares the same underlying
/// store as the Raft instance.
#[derive(Clone)]
pub(crate) struct Replica {
    pub(crate) raft: Raft<HomeKvRaftConfig>,
    pub(crate) state_machine: Arc<HomeKvStateMachine>,
    pub(crate) log_store: HomeKvRaftLogStore,
}

/// Start all replicas for one Raft group (catalog or data group).
///
/// Two phases, mirroring `hkvm4bench`: first create every node's network
/// factory, log store, and Raft instance; then register every replica's RPC
/// handler on every factory so the in-process transport can route between
/// them.
///
/// `extended` selects the warm-standby factory path (data groups); the
/// catalog uses the pinned M3 path.
///
/// The network factories are returned (not dropped) so the production
/// metrics surface can observe per-peer RPC attempts, failures,
/// backpressure rejections and payload bytes for the node's whole
/// lifetime (spec 0006 §9).
struct StartedReplicas {
    replicas: BTreeMap<RaftNodeId, Replica>,
    factories: BTreeMap<RaftNodeId, HomeKvRaftNetworkFactory>,
}

async fn start_replicas(
    raft_config: &Arc<Config>,
    links: &TestLinkController,
    bootstrap: &ThreeNodeBootstrap,
    cluster_name: &str,
    dir: &Path,
    node_ids: &[RaftNodeId],
    extended: bool,
) -> Result<StartedReplicas, PlacementNodeError> {
    std::fs::create_dir_all(dir).map_err(|e| PlacementNodeError::Io(e.to_string()))?;
    let mut factories = BTreeMap::new();
    for id in node_ids {
        let factory = if extended {
            HomeKvRaftNetworkFactory::new_extended(*id, bootstrap.clone(), 64, links.clone())
        } else {
            HomeKvRaftNetworkFactory::new(*id, bootstrap.clone(), 64, links.clone())
        }
        .map_err(|e| PlacementNodeError::Raft(format!("{cluster_name} factory: {e:?}")))?;
        factories.insert(*id, factory);
    }
    let mut replicas = BTreeMap::new();
    for id in node_ids {
        let store_path = dir.join(format!("node-{id}.raft"));
        let store = HomeKvRaftLogStore::open(&store_path)
            .map_err(|e| PlacementNodeError::Io(e.to_string()))?;
        let state_machine = Arc::new(HomeKvStateMachine::default());
        let raft = Raft::new(
            *id,
            raft_config.clone(),
            factories[id].clone(),
            store.clone(),
            (*state_machine).clone(),
        )
        .await
        .map_err(|e| PlacementNodeError::Raft(format!("{cluster_name} raft::new: {e:?}")))?;
        replicas.insert(
            *id,
            Replica {
                raft,
                state_machine,
                log_store: store,
            },
        );
    }
    for factory in factories.values() {
        for (id, replica) in &replicas {
            factory
                .register_handler(*id, Arc::new(replica.raft.clone()))
                .map_err(|e| PlacementNodeError::Raft(format!("{cluster_name} handler: {e:?}")))?;
        }
    }
    Ok(StartedReplicas {
        replicas,
        factories,
    })
}

/// Wait, bounded by `timeout`, for the freshly initialized catalog primary
/// to become leader. A genuinely leaderless catalog still fails closed with
/// the same `ConsensusUnavailable` the bootstrap write would have produced.
async fn wait_for_catalog_leadership(
    raft: &Raft<HomeKvRaftConfig>,
    timeout: Duration,
) -> Result<(), PlacementNodeError> {
    let deadline = Instant::now() + timeout;
    loop {
        // `ensure_linearizable` succeeds only on the leader; it is the
        // non-deprecated leadership check in openraft 0.9.
        if raft.ensure_linearizable().await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(PlacementNodeError::Catalog(
                "placement catalog consensus is unavailable".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Initialize a Raft cluster once; a restart that finds existing state is not
/// an error.
async fn initialize_once(
    raft: &Raft<HomeKvRaftConfig>,
    members: BTreeMap<RaftNodeId, RaftNode>,
    what: &str,
) -> Result<(), PlacementNodeError> {
    use openraft::error::{InitializeError, RaftError};
    match raft.initialize(members).await {
        Ok(_) => Ok(()),
        Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
        Err(e) => Err(PlacementNodeError::Raft(format!("{what} init: {e:?}"))),
    }
}

/// Production [`LocalReplicaJanitor`] backed by the placement data directory.
///
/// One janitor per node identity: a removed replica's on-disk state lives at
/// `<data_dir>/groups/<shard:04>/node-<id>.raft`, and cleanup deletes only
/// that identity's directory. Listing scans the group directories for this
/// identity's replica dirs.
#[derive(Clone, Debug)]
pub struct FsReplicaJanitor {
    data_dir: PathBuf,
    node_id: RaftNodeId,
}

impl FsReplicaJanitor {
    pub fn new(data_dir: PathBuf, node_id: RaftNodeId) -> Self {
        Self { data_dir, node_id }
    }

    fn replica_dir(&self, shard_id: u16) -> PathBuf {
        self.data_dir
            .join("groups")
            .join(format!("{shard_id:04}"))
            .join(format!("node-{}.raft", self.node_id))
    }
}

#[async_trait]
impl LocalReplicaJanitor for FsReplicaJanitor {
    async fn remove_local_replica(&self, shard_id: u16) -> Result<(), String> {
        let path = self.replica_dir(shard_id);
        if path.is_dir() {
            std::fs::remove_dir_all(&path)
                .map_err(|e| format!("remove replica dir {}: {e}", path.display()))?;
        } else if path.exists() {
            // The placement composition root persists each replica's Raft
            // log store as a single `node-<id>.raft` file (see
            // `start_replicas`); `remove_dir_all` fails on a file, so remove
            // it directly. Without this the janitor cannot clear an
            // unrecoverable replica after a corruption fail-closed.
            std::fs::remove_file(&path)
                .map_err(|e| format!("remove replica file {}: {e}", path.display()))?;
        }
        Ok(())
    }

    async fn list_local_replicas(&self) -> Result<Vec<u16>, String> {
        let groups = self.data_dir.join("groups");
        if !groups.exists() {
            return Ok(Vec::new());
        }
        let want = format!("node-{}.raft", self.node_id);
        let mut shards = Vec::new();
        let entries =
            std::fs::read_dir(&groups).map_err(|e| format!("list {}: {e}", groups.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("read dir entry: {e}"))?;
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let shard_dir = entry.path();
            if shard_dir.join(&want).exists() {
                if let Some(name) = shard_dir.file_name().and_then(|n| n.to_str()) {
                    if let Ok(shard) = name.parse::<u16>() {
                        shards.push(shard);
                    }
                }
            }
        }
        shards.sort_unstable();
        Ok(shards)
    }
}

/// Driver key: (shard, node identity).
type DriverKey = (u16, RaftNodeId);

/// The production placement-node composition root.
///
/// Besides the Raft groups and movement drivers, the node retains the
/// network factories, movement metric handles, replica observers and the
/// redirect metric handle for its whole lifetime so the §9 production
/// metrics surface ([`PlacementNode::metrics_snapshot`]) can observe them.
/// Nothing here is per-key or per-operation: every retained handle is
/// bounded by (shards × identities) plus the catalog.
pub struct PlacementNode {
    pub(crate) catalog: PlacementCatalogGroup,
    pub(crate) catalog_factories: BTreeMap<RaftNodeId, HomeKvRaftNetworkFactory>,
    pub(crate) groups: BTreeMap<u16, BTreeMap<RaftNodeId, Replica>>,
    pub(crate) group_factories: BTreeMap<u16, BTreeMap<RaftNodeId, HomeKvRaftNetworkFactory>>,
    pub(crate) movement_metrics: BTreeMap<DriverKey, MovementMetrics>,
    pub(crate) observers: BTreeMap<DriverKey, HomeKvReplicaObserver>,
    pub(crate) redirect_metrics: RouteRedirectMetrics,
    pub(crate) shards: Vec<u16>,
    drive_task: JoinHandle<()>,
}

impl PlacementNode {
    /// Start the placement node: catalog group, data groups, per-shard
    /// movement drivers, and the background drive loop.
    pub async fn start(config: PlacementNodeConfig) -> Result<Self, PlacementNodeError> {
        config.validate()?;
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| PlacementNodeError::Io(e.to_string()))?;

        let raft_config = Arc::new(
            Config {
                cluster_name: "homekv-placement".to_string(),
                heartbeat_interval: config.heartbeat_ms,
                election_timeout_min: config.election_min_ms,
                election_timeout_max: config.election_max_ms,
                ..Default::default()
            }
            .validate()
            .map_err(|e| PlacementNodeError::Raft(e.to_string()))?,
        );
        let links = TestLinkController::default();

        // --- catalog group -------------------------------------------------
        let catalog_bootstrap = ThreeNodeBootstrap::new(
            "homekv-placement-catalog",
            config.catalog_voters.map(|id| BootstrapNode {
                id,
                raft_endpoint: format!("127.0.0.1:{}", node_port(id)),
            }),
        )
        .map_err(|e| PlacementNodeError::Raft(format!("catalog bootstrap: {e:?}")))?;

        let catalog_started = start_replicas(
            &raft_config,
            &links,
            &catalog_bootstrap,
            "catalog",
            &config.data_dir.join("catalog"),
            &config.catalog_voters,
            false,
        )
        .await?;
        let catalog_replicas = catalog_started.replicas;
        let catalog_factories = catalog_started.factories;
        let catalog_membership: BTreeMap<RaftNodeId, RaftNode> = config
            .catalog_voters
            .iter()
            .map(|id| (*id, RaftNode::new(format!("127.0.0.1:{}", node_port(*id)))))
            .collect();
        // Initialize the catalog exactly once. A restart that finds existing
        // state is not an error; a pristine replica rejoining after its store
        // was removed (e.g. janitor cleanup of a corrupt artifact) must NOT
        // initialize — openraft would seed a divergent single-node incarnation
        // that can never rejoin the group. Any durable store file among the
        // voters proves the group was already initialized.
        let catalog_dir = config.data_dir.join("catalog");
        let catalog_initialized = config
            .catalog_voters
            .iter()
            .any(|id| catalog_dir.join(format!("node-{id}.raft")).exists());
        if !catalog_initialized {
            initialize_once(
                &catalog_replicas[&config.catalog_voters[0]].raft,
                catalog_membership,
                "catalog",
            )
            .await?;
        }
        // `initialize()` returns once the membership change is accepted, not
        // once this node has won the election. The bootstrap write below must
        // land on the leader (any error, including `ForwardToLeader`, maps to
        // `ConsensusUnavailable`), so wait — bounded — for leadership first.
        // Without this, node startup fails spuriously on a loaded machine when
        // the write races the election.
        wait_for_catalog_leadership(
            &catalog_replicas[&config.catalog_voters[0]].raft,
            Duration::from_secs(15),
        )
        .await?;
        let catalog_primary = &catalog_replicas[&config.catalog_voters[0]];

        let catalog = PlacementCatalogGroup::new(
            catalog_primary.raft.clone(),
            (*catalog_primary.state_machine).clone(),
        );
        let eligible: Vec<EligibleNode> = config
            .node_ids
            .iter()
            .map(|id| EligibleNode {
                node_id: *id,
                raft_endpoint: format!("127.0.0.1:{}", node_port(*id)),
                failure_domain: format!("fd-{id}"),
            })
            .collect();
        catalog
            .bootstrap(config.cluster_id, eligible)
            .await
            .map_err(|e| PlacementNodeError::Catalog(e.to_string()))?;

        // --- data groups ---------------------------------------------------
        // Initial voters come from the freshly committed catalog so the data
        // groups and the catalog agree from the first tick.
        let committed = catalog
            .committed_state()
            .await
            .map_err(|e| PlacementNodeError::Catalog(e.to_string()))?;

        let admission = MovementWorkAdmission::new(MovementWorkConfig {
            cluster_max_concurrent: config.max_concurrent_movements,
            per_node_max_concurrent: config.per_node_max_concurrent,
        })
        .map_err(|e| PlacementNodeError::Admission(e.to_string()))?;

        let mut groups = BTreeMap::new();
        let mut group_factories: BTreeMap<u16, BTreeMap<RaftNodeId, HomeKvRaftNetworkFactory>> =
            BTreeMap::new();
        let mut drivers = BTreeMap::new();
        for shard_id in &config.shards {
            let placement = committed.placements.get(shard_id).ok_or_else(|| {
                PlacementNodeError::Catalog(format!("no committed placement for shard {shard_id}"))
            })?;
            let group_dir = config
                .data_dir
                .join("groups")
                .join(format!("{shard_id:04}"));
            let group_bootstrap = ThreeNodeBootstrap::new_extended(
                format!("homekv-placement-group-{shard_id:04}"),
                config.node_ids.iter().map(|id| BootstrapNode {
                    id: *id,
                    raft_endpoint: format!("127.0.0.1:{}", node_port(*id)),
                }),
            )
            .map_err(|e| PlacementNodeError::Raft(format!("group bootstrap: {e:?}")))?;

            let group_started = start_replicas(
                &raft_config,
                &links,
                &group_bootstrap,
                &format!("group-{shard_id:04}"),
                &group_dir,
                &config.node_ids,
                true,
            )
            .await?;
            let replicas = group_started.replicas;
            group_factories.insert(*shard_id, group_started.factories);
            // Only the committed voters form the initial membership; warm
            // standbys hold idle replicas until a movement adds them.
            let initial: BTreeMap<RaftNodeId, RaftNode> = placement
                .voters
                .iter()
                .map(|id| (*id, RaftNode::new(format!("127.0.0.1:{}", node_port(*id)))))
                .collect();
            // Initialize the group exactly once. A pristine replica rejoining
            // after janitor removal of a corrupt store must NOT initialize:
            // openraft would seed a divergent single-node incarnation that
            // can never rejoin the healthy quorum. Any durable store file
            // among the voters proves the group was already initialized.
            let group_initialized = placement
                .voters
                .iter()
                .any(|id| group_dir.join(format!("node-{id}.raft")).exists());
            if !group_initialized {
                initialize_once(
                    &replicas[&placement.voters[0]].raft,
                    initial,
                    &format!("group {shard_id}"),
                )
                .await?;
            }

            for (id, replica) in &replicas {
                let operator = LiveMembershipOperator::new(
                    replica.raft.clone(),
                    replica.state_machine.clone(),
                );
                let janitor: Arc<dyn LocalReplicaJanitor> =
                    Arc::new(FsReplicaJanitor::new(config.data_dir.clone(), *id));
                let driver_config = MovementDriverConfig {
                    local_node_id: *id,
                    work_node_id: *id,
                    ..MovementDriverConfig::default()
                };
                let driver = MovementDriver::new(
                    driver_config,
                    operator,
                    catalog.clone(),
                    Some(janitor),
                    admission.clone(),
                );
                drivers.insert((*shard_id, *id), driver);
            }
            groups.insert(*shard_id, replicas);
        }

        // --- observability handles -----------------------------------------
        // One long-lived replica observer per hosted replica: the observer
        // tracks leadership transitions in the background, and its snapshot
        // is the §9 per-group Raft field source. Observers are created once
        // (not per scrape) so metrics collection spawns no per-call tasks.
        let mut observers = BTreeMap::new();
        for (shard_id, replicas) in &groups {
            for (id, replica) in replicas {
                observers.insert(
                    (*shard_id, *id),
                    HomeKvReplicaObserver::new(
                        replica.raft.clone(),
                        replica.log_store.clone(),
                        (*replica.state_machine).clone(),
                    ),
                );
            }
        }
        // Movement metric handles are cloned out of the drivers before the
        // drivers move into the drive loop.
        let movement_metrics: BTreeMap<DriverKey, MovementMetrics> = drivers
            .iter()
            .map(|(key, driver)| (*key, driver.metrics()))
            .collect();

        // --- drive loop ----------------------------------------------------
        let drivers = Arc::new(drivers);
        let drive_task = {
            let catalog = catalog.clone();
            let drivers = Arc::clone(&drivers);
            let raft_handles: Arc<BTreeMap<DriverKey, Raft<HomeKvRaftConfig>>> = Arc::new(
                groups
                    .iter()
                    .flat_map(|(shard_id, replicas)| {
                        replicas
                            .iter()
                            .map(|(id, replica)| ((*shard_id, *id), replica.raft.clone()))
                    })
                    .collect(),
            );
            let shards = config.shards.clone();
            let drive_interval = config.drive_interval;
            let reconcile_interval = config.reconcile_interval;
            tokio::spawn(async move {
                let mut drive_tick = interval(drive_interval);
                let mut reconcile_tick = interval(reconcile_interval);
                loop {
                    tokio::select! {
                        _ = drive_tick.tick() => {
                            let state = match catalog.committed_state().await {
                                Ok(state) => state,
                                Err(_) => continue,
                            };
                            for shard_id in &shards {
                                let has_pending = state
                                    .placements
                                    .get(shard_id)
                                    .is_some_and(|p| p.pending_movement.is_some());
                                if !has_pending {
                                    continue;
                                }
                                // Route the movement to the current leader's
                                // driver; while leaderless, poll every
                                // driver's operator so the election winner
                                // picks it up.
                                let leader = raft_handles.iter().find_map(|((s, id), raft)| {
                                    (*s == *shard_id
                                        && raft.metrics().borrow().state == ServerState::Leader)
                                        .then_some(*id)
                                });
                                let targets: Vec<RaftNodeId> = match leader {
                                    Some(id) => vec![id],
                                    None => raft_handles
                                        .keys()
                                        .filter(|(s, _)| s == shard_id)
                                        .map(|(_, id)| *id)
                                        .collect(),
                                };
                                for id in targets {
                                    if let Some(driver) = drivers.get(&(*shard_id, id)) {
                                        let _ = driver.drive_shard(*shard_id).await;
                                    }
                                }
                            }
                        }
                        _ = reconcile_tick.tick() => {
                            for driver in drivers.values() {
                                let _ = driver.reconcile_local_replicas().await;
                            }
                        }
                    }
                }
            })
        };

        Ok(Self {
            catalog,
            catalog_factories,
            groups,
            group_factories,
            movement_metrics,
            observers,
            redirect_metrics: RouteRedirectMetrics::default(),
            shards: config.shards.clone(),
            drive_task,
        })
    }

    /// Shared redirect counters for this node's serving layer.
    ///
    /// The placement composition does not serve data-plane requests itself;
    /// a deployment wires its [`CommittedRouteResolver`](crate::routing::CommittedRouteResolver)s
    /// to this handle (via
    /// [`with_metrics`](crate::routing::CommittedRouteResolver::with_metrics))
    /// so redirects by cause are observable on the node.
    pub fn route_redirect_metrics(&self) -> RouteRedirectMetrics {
        self.redirect_metrics.clone()
    }

    /// The catalog group handle (for operators/tests).
    pub fn catalog(&self) -> &PlacementCatalogGroup {
        &self.catalog
    }

    /// The committed catalog state (convenience for operators/tests).
    pub async fn committed_state(&self) -> Result<CatalogState, PlacementNodeError> {
        self.catalog
            .committed_state()
            .await
            .map_err(|e| PlacementNodeError::Catalog(e.to_string()))
    }

    /// The node id currently leading `shard_id`'s data group, if any.
    pub fn group_leader(&self, shard_id: u16) -> Option<RaftNodeId> {
        self.groups.get(&shard_id).and_then(|replicas| {
            replicas.iter().find_map(|(id, replica)| {
                (replica.raft.metrics().borrow().state == ServerState::Leader).then_some(*id)
            })
        })
    }

    /// Read-only access to a hosted replica's state machine (observability and
    /// tests).
    pub fn group_state_machine(
        &self,
        shard_id: u16,
        node_id: RaftNodeId,
    ) -> Option<Arc<HomeKvStateMachine>> {
        self.groups
            .get(&shard_id)
            .and_then(|replicas| replicas.get(&node_id))
            .map(|replica| replica.state_machine.clone())
    }

    /// The Raft handle of a hosted replica (observability and tests).
    pub fn group_raft(&self, shard_id: u16, node_id: RaftNodeId) -> Option<Raft<HomeKvRaftConfig>> {
        self.groups
            .get(&shard_id)
            .and_then(|replicas| replicas.get(&node_id))
            .map(|replica| replica.raft.clone())
    }

    /// The log store handle of a hosted replica (observability and tests).
    ///
    /// Shares the same underlying store as the replica's Raft instance, so
    /// a replica observer built from it sees live durable state.
    pub fn group_log_store(
        &self,
        shard_id: u16,
        node_id: RaftNodeId,
    ) -> Option<HomeKvRaftLogStore> {
        self.groups
            .get(&shard_id)
            .and_then(|replicas| replicas.get(&node_id))
            .map(|replica| replica.log_store.clone())
    }

    /// Stop the drive loop. Raft instances shut down on drop.
    pub fn shutdown(self) {
        self.drive_task.abort();
    }
}

fn node_port(node_id: RaftNodeId) -> u16 {
    31_000 + (node_id % 1000) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> PlacementNodeConfig {
        PlacementNodeConfig {
            data_dir: std::env::temp_dir().join("homekv-placement-node-test"),
            ..PlacementNodeConfig::default()
        }
    }

    #[test]
    fn default_config_validates() {
        valid_config().validate().unwrap();
    }

    #[test]
    fn config_rejects_empty_node_ids() {
        let mut config = valid_config();
        config.node_ids.clear();
        assert_eq!(config.validate(), Err(PlacementNodeError::EmptyNodeIds));
    }

    #[test]
    fn config_rejects_catalog_voter_not_hosted() {
        let mut config = valid_config();
        config.catalog_voters = [1, 2, 99];
        assert_eq!(
            config.validate(),
            Err(PlacementNodeError::CatalogVoterNotHosted { voter: 99 })
        );
    }

    #[test]
    fn config_rejects_duplicate_node_id() {
        let mut config = valid_config();
        config.node_ids = vec![1, 2, 2, 3];
        assert_eq!(
            config.validate(),
            Err(PlacementNodeError::DuplicateNodeId { node_id: 2 })
        );
    }

    #[test]
    fn config_rejects_empty_shards_and_zero_intervals() {
        let mut config = valid_config();
        config.shards.clear();
        assert_eq!(config.validate(), Err(PlacementNodeError::EmptyShards));

        let mut config = valid_config();
        config.drive_interval = Duration::from_millis(0);
        assert_eq!(
            config.validate(),
            Err(PlacementNodeError::NonPositiveInterval {
                name: "drive_interval"
            })
        );
    }

    #[tokio::test]
    async fn fs_janitor_lists_and_removes_only_its_identity() {
        let root = std::env::temp_dir().join(format!(
            "homekv-janitor-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let janitor = FsReplicaJanitor::new(root.clone(), 3);
        // Layout: shard 7 has replicas for nodes 1..4; shard 8 only node 3.
        for (shard, ids) in [(7u16, vec![1, 2, 3, 4]), (8u16, vec![3])] {
            for id in ids {
                std::fs::create_dir_all(
                    root.join("groups")
                        .join(format!("{shard:04}"))
                        .join(format!("node-{id}.raft")),
                )
                .unwrap();
            }
        }

        let mut listed = janitor.list_local_replicas().await.unwrap();
        listed.sort_unstable();
        assert_eq!(listed, vec![7, 8]);

        janitor.remove_local_replica(7).await.unwrap();
        // Only node 3's dir is gone; the other identities' replicas remain.
        assert!(!root
            .join("groups")
            .join("0007")
            .join("node-3.raft")
            .exists());
        assert!(root
            .join("groups")
            .join("0007")
            .join("node-1.raft")
            .exists());
        assert!(root
            .join("groups")
            .join("0008")
            .join("node-3.raft")
            .exists());

        let listed = janitor.list_local_replicas().await.unwrap();
        assert_eq!(listed, vec![8]);

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn fs_janitor_removes_single_file_replica_store() {
        // `start_replicas` persists each replica's Raft log store as a single
        // `node-<id>.raft` FILE (not a directory). The janitor must remove
        // that file — `remove_dir_all` alone fails on it, which previously
        // made janitor cleanup of a corrupt replica impossible.
        let root = std::env::temp_dir().join(format!(
            "homekv-janitor-file-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let janitor = FsReplicaJanitor::new(root.clone(), 2);
        let shard_dir = root.join("groups").join("0005");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let own = shard_dir.join("node-2.raft");
        std::fs::write(&own, b"fake-raft-image").unwrap();
        // A sibling identity's replica file must be untouched.
        let sibling = shard_dir.join("node-3.raft");
        std::fs::write(&sibling, b"sibling").unwrap();
        assert_eq!(
            janitor.list_local_replicas().await.unwrap(),
            vec![5],
            "shard with this node's replica file is listed before removal"
        );

        janitor.remove_local_replica(5).await.unwrap();
        assert!(
            !own.exists(),
            "janitor must remove the single-file replica store"
        );
        assert!(sibling.exists(), "sibling replica must be untouched");
        // `list_local_replicas` only reports shards holding THIS node's
        // (`node-2`) replica file, so shard 5 is gone now that it was removed.
        assert_eq!(
            janitor.list_local_replicas().await.unwrap(),
            Vec::<u16>::new()
        );

        // Removing a nonexistent replica is a no-op, not an error.
        janitor.remove_local_replica(6).await.unwrap();

        std::fs::remove_dir_all(&root).unwrap();
    }
}
