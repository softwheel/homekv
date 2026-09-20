//! Production metrics surface for [`PlacementNode`](crate::placement_node::PlacementNode).
//!
//! Spec 0006 §9 / REQ-M4-OPS-002/003: a HomeKV-owned, serializable snapshot
//! covering the §9 metric list as far as the in-process deployment honestly
//! provides. No OpenRaft types cross this boundary.
//!
//! What each §9 item maps to:
//!
//! - runtime workers/tasks/timers/queue depth: [`RuntimeMetricsSnapshot`],
//!   sampled from `tokio::runtime::Handle::current().metrics()` for workers
//!   and tasks. Tokio's internal timer wheel has no stable `Handle` API, so
//!   the timer signal is the HomeKV-owned count of timers the placement
//!   node itself owns (`node_timers`: the drive + reconcile intervals) —
//!   documented, not invented.
//! - per-group role/term/config/commit/apply/snapshot/lag: one
//!   [`ReplicaMetricsEntry`] per hosted replica, reusing the PR #89
//!   [`GroupRaftView`](crate::rebalance::GroupRaftView) extraction via the
//!   node's long-lived replica observers.
//! - connection and RPC counts/bytes/rejections: [`PlacementRpcMetrics`]
//!   aggregates the retained per-factory
//!   [`PeerRpcMetricsSnapshot`](crate::raft_network::PeerRpcMetricsSnapshot)s
//!   (attempts, failures, backpressure rejections, payload bytes). The
//!   in-process transport has no sockets: the connection-like unit is the
//!   per-peer entry inside each factory — a fixed peer set per factory with
//!   a configured per-peer width. Factory handles themselves are created
//!   per (group, node identity) by the openraft integration, so the handle
//!   count is `(shards + catalog) × identities`; what is independent of
//!   shard count is the per-factory peer set and the per-peer width.
//! - memory by group and aggregate: [`MemorySnapshot`] with process RSS
//!   (parsed from `/proc/self/status`, no new dependencies) plus
//!   per-replica state-machine data bytes and snapshot transfer bytes.
//! - route redirects by cause: [`RedirectMetricsSnapshot`], sourced from the
//!   node's shared [`RouteRedirectMetrics`](crate::routing::RouteRedirectMetrics)
//!   handle (see [`CommittedRouteResolver::with_metrics`](crate::routing::CommittedRouteResolver::with_metrics)).
//! - movement phase/duration/result: [`MovementDriverView`] exposes each
//!   driver's [`MovementMetricsSnapshot`](crate::movement::MovementMetricsSnapshot)
//!   (phase attempts/successes/failures, operation outcomes, per-operation
//!   durations). Movement byte transfer is not tracked by the driver; the
//!   byte flow that exists (snapshot build/install/receive) is reported per
//!   replica in [`ReplicaMemoryEntry`].
//! - voter/leader skew: [`SkewSnapshot`], computed from the committed
//!   catalog with the same math as
//!   [`build_topology_view`](crate::rebalance::build_topology_view).
//! - catalog epoch/leader: [`CatalogMetricsView`]. Catalog *health*
//!   (Healthy/Degraded/QuorumLost) is operator-computed from probes and is
//!   covered by the topology-view tests, not by this node-local snapshot.
//!
//! Cardinality (REQ-M4-OPS-003): every series is bounded — one entry per
//! hosted replica, one per (shard, identity) driver, one per factory, one
//! per peer, two redirect causes, `num_workers` queue depths. There are no
//! per-key, per-client or per-operation series; the movement driver's
//! recent-operation ring is bounded at 64 entries per driver. Per-shard
//! inspection is paginated via
//! [`PlacementNode::metrics_for_shards`].

use std::collections::BTreeMap;

use openraft::ServerState;
use serde_derive::{Deserialize, Serialize};

use crate::movement::MovementMetricsSnapshot;
use crate::placement::{CatalogState, PlacementEpoch};
use crate::placement_node::{PlacementNode, PlacementNodeError};
use crate::raft::RaftNodeId;
use crate::raft_network::{HomeKvRaftNetworkFactory, PeerRpcMetricsSnapshot};
use crate::rebalance::GroupRaftView;
use crate::routing::RedirectCause;

/// The shared async runtime (REQ-M4-GROUP-002 observation).
///
/// Groups share one bounded worker pool; this snapshot proves the OS-worker
/// count stays at the configured bound as groups grow.
///
/// Only `num_workers` and `num_alive_tasks` are reported from tokio:
/// tokio's queue depth / blocking-thread counters require the
/// `tokio_unstable` cfg, which this build does not enable, so they are
/// documented gaps rather than invented numbers. Queue depth *is*
/// observable where HomeKV owns the queue: per-peer in-flight RPC counts
/// (`current`/`peak`) and backpressure rejections in
/// [`PlacementRpcMetrics`].
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeMetricsSnapshot {
    /// False when sampled outside a Tokio runtime (never in production).
    pub sampled: bool,
    /// Configured OS worker thread count.
    pub num_workers: usize,
    pub num_alive_tasks: usize,
    /// Timers owned by the placement node itself (the drive loop's drive +
    /// reconcile intervals). Tokio's internal timer wheel is not observable
    /// through the stable API, so this HomeKV-owned count is the honest
    /// "timer" signal for §9; it is set by [`PlacementNode`] at startup.
    pub node_timers: u64,
}

/// Catalog identity for the snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogMetricsView {
    pub placement_epoch: PlacementEpoch,
    pub leader_id: Option<RaftNodeId>,
    pub term: u64,
}

/// Loaded/active/idle/error group counts (§9).
///
/// Definitions, from live node state only:
/// - `loaded`: shards hosted by this node;
/// - `active`: a leader is currently observed among the hosted replicas;
/// - `error`: no leader observed and at least one fail-closed movement is
///   recorded by the shard's drivers;
/// - `idle`: hosted but neither active nor error (e.g. electing).
///
/// The three states are exclusive, so `loaded == active + idle + error`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupCountsSnapshot {
    pub loaded: usize,
    pub active: usize,
    pub idle: usize,
    pub error: usize,
}

/// Placement skew from the committed catalog (§9).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkewSnapshot {
    pub voter_skew: u64,
    pub leader_skew: u64,
}

/// Aggregate RPC counters across all retained factories.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RpcTotalsSnapshot {
    pub attempts: u64,
    pub failures: u64,
    pub backpressure_rejections: u64,
    pub bytes: u64,
}

/// Per-peer RPC view of one retained network factory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FactoryRpcView {
    /// None = catalog group factory; Some(shard) = data-group factory.
    pub shard_id: Option<u16>,
    pub local_node_id: RaftNodeId,
    /// One entry per bootstrap peer (fixed set; sorted by peer id).
    pub peers: Vec<PeerRpcMetricsSnapshot>,
}

/// RPC/connection surface (§9 "connection and RPC counts/bytes/rejections").
///
/// See the module docs for what "connection-like" means in the in-process
/// transport.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlacementRpcMetrics {
    /// Catalog factories first, then per-shard factories; sorted.
    pub factories: Vec<FactoryRpcView>,
    pub totals: RpcTotalsSnapshot,
}

/// Per-replica memory accounting (§9 "memory by group").
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaMemoryEntry {
    pub shard_id: u16,
    pub node_id: RaftNodeId,
    /// Application key+value bytes currently held in the state machine.
    pub state_machine_data_bytes: u64,
    pub snapshot_build_bytes: u64,
    pub snapshot_install_bytes: u64,
    pub snapshot_receive_peak_bytes: u64,
}

/// Memory surface (§9 "memory by group and aggregate").
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemorySnapshot {
    /// Process RSS in bytes from `/proc/self/status` (`VmRSS`); `None`
    /// off-Linux or when unparsable — never invented.
    pub process_rss_bytes: Option<u64>,
    /// Sorted by (shard_id, node_id); one entry per hosted replica.
    pub per_replica: Vec<ReplicaMemoryEntry>,
    pub aggregate_state_machine_data_bytes: u64,
}

/// Redirects by cause (§9).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RedirectMetricsSnapshot {
    /// Sorted by cause; exactly one entry per [`RedirectCause`].
    pub by_cause: Vec<(RedirectCause, u64)>,
    pub total: u64,
}

/// One movement driver's metrics (§9 "movement phase/bytes/duration/result").
///
/// `metrics` carries phase attempts/successes/failures, operation outcomes
/// and per-operation durations; see the module docs for the byte caveat.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MovementDriverView {
    pub shard_id: u16,
    pub node_id: RaftNodeId,
    pub metrics: MovementMetricsSnapshot,
}

/// One hosted replica's Raft state plus memory (§9 per-group fields).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaMetricsEntry {
    pub node_id: RaftNodeId,
    /// Role/term/config(voters/learners)/commit/apply/snapshot/lag.
    pub raft: GroupRaftView,
    pub memory: ReplicaMemoryEntry,
}

/// Full per-shard inspection payload (the paginated unit).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShardMetricsEntry {
    pub shard_id: u16,
    pub active: bool,
    pub error: bool,
    /// One per hosted replica, sorted by node id.
    pub replicas: Vec<ReplicaMetricsEntry>,
    /// One per (shard, identity) driver, sorted by node id.
    pub movement: Vec<MovementDriverView>,
    /// One per retained factory for this shard, sorted by local node id.
    pub rpc: Vec<FactoryRpcView>,
}

/// The full §9 production metrics snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlacementMetrics {
    pub runtime: RuntimeMetricsSnapshot,
    pub catalog: CatalogMetricsView,
    pub group_counts: GroupCountsSnapshot,
    pub skew: SkewSnapshot,
    pub rpc: PlacementRpcMetrics,
    pub memory: MemorySnapshot,
    pub redirects: RedirectMetricsSnapshot,
    /// One per (shard, identity) driver, sorted by (shard_id, node_id).
    pub movement: Vec<MovementDriverView>,
    /// Full per-shard detail, sorted by shard id. Use
    /// [`PlacementNode::metrics_for_shards`] for paginated inspection.
    pub shards: Vec<ShardMetricsEntry>,
}

fn sample_runtime() -> RuntimeMetricsSnapshot {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return RuntimeMetricsSnapshot::default();
    };
    let metrics = handle.metrics();
    RuntimeMetricsSnapshot {
        sampled: true,
        num_workers: metrics.num_workers(),
        num_alive_tasks: metrics.num_alive_tasks(),
        // The HomeKV-owned timer count is filled in by `metrics_snapshot`;
        // `sample_runtime` alone cannot know the node's timers.
        node_timers: 0,
    }
}

/// Process RSS in bytes, parsed from `/proc/self/status` (`VmRSS`).
/// HomeKV-owned parsing, no new dependencies; `None` off-Linux.
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kb| kb.parse::<u64>().ok())?;
    Some(kb.saturating_mul(1024))
}

fn skew_of(counts: impl Iterator<Item = u64>) -> u64 {
    let mut min = u64::MAX;
    let mut max = 0u64;
    let mut any = false;
    for count in counts {
        any = true;
        min = min.min(count);
        max = max.max(count);
    }
    if any {
        max.saturating_sub(min)
    } else {
        0
    }
}

/// Same voter/leader skew math as
/// [`build_topology_view`](crate::rebalance::build_topology_view).
fn skew_snapshot(state: &CatalogState) -> SkewSnapshot {
    let mut voter_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    let mut leader_counts: BTreeMap<RaftNodeId, u64> = BTreeMap::new();
    for node in state.eligible_nodes.keys() {
        voter_counts.insert(*node, 0);
        leader_counts.insert(*node, 0);
    }
    for placement in state.placements.values() {
        for voter in placement.voters {
            *voter_counts.entry(voter).or_insert(0) += 1;
        }
        *leader_counts.entry(placement.desired_leader).or_insert(0) += 1;
    }
    SkewSnapshot {
        voter_skew: skew_of(voter_counts.values().copied()),
        leader_skew: skew_of(leader_counts.values().copied()),
    }
}

fn factory_rpc_view(
    shard_id: Option<u16>,
    local_node_id: RaftNodeId,
    factory: &HomeKvRaftNetworkFactory,
) -> FactoryRpcView {
    FactoryRpcView {
        shard_id,
        local_node_id,
        peers: factory.metrics().peers,
    }
}

fn accumulate_totals(totals: &mut RpcTotalsSnapshot, view: &FactoryRpcView) {
    for peer in &view.peers {
        totals.attempts += peer.attempts;
        totals.failures += peer.failures;
        totals.backpressure_rejections += peer.backpressure_rejections;
        totals.bytes += peer.bytes;
    }
}

impl PlacementNode {
    /// Full §9 production metrics snapshot.
    ///
    /// Fails only when the committed catalog is unreadable; every section
    /// is a HomeKV-owned serializable representation with bounded
    /// cardinality (REQ-M4-OPS-003).
    pub async fn metrics_snapshot(&self) -> Result<PlacementMetrics, PlacementNodeError> {
        let state = self
            .catalog
            .committed_state()
            .await
            .map_err(|e| PlacementNodeError::Catalog(format!("metrics snapshot: {e}")))?;

        let mut shard_ids: Vec<u16> = self.shards.clone();
        shard_ids.sort_unstable();
        let mut shards = Vec::with_capacity(shard_ids.len());
        for shard_id in &shard_ids {
            shards.push(self.shard_entry(*shard_id).await);
        }

        let catalog_metrics = self.catalog.raft().metrics();
        let catalog_view = catalog_metrics.borrow();
        let catalog = CatalogMetricsView {
            placement_epoch: state.placement_epoch,
            leader_id: catalog_view.current_leader,
            term: catalog_view.current_term,
        };

        let mut loaded = 0usize;
        let mut active = 0usize;
        let mut idle = 0usize;
        let mut error = 0usize;
        for entry in &shards {
            loaded += 1;
            if entry.active {
                active += 1;
            } else if entry.error {
                error += 1;
            } else {
                idle += 1;
            }
        }

        let mut rpc_factories = Vec::new();
        for (node_id, factory) in &self.catalog_factories {
            rpc_factories.push(factory_rpc_view(None, *node_id, factory));
        }
        let mut totals = RpcTotalsSnapshot::default();
        for view in &rpc_factories {
            accumulate_totals(&mut totals, view);
        }
        for entry in &shards {
            for view in &entry.rpc {
                rpc_factories.push(view.clone());
                accumulate_totals(&mut totals, view);
            }
        }

        let mut per_replica = Vec::new();
        for entry in &shards {
            for replica in &entry.replicas {
                per_replica.push(replica.memory.clone());
            }
        }
        per_replica.sort_by_key(|entry| (entry.shard_id, entry.node_id));
        let aggregate_state_machine_data_bytes = per_replica
            .iter()
            .map(|entry| entry.state_machine_data_bytes)
            .sum();

        let movement: Vec<MovementDriverView> = self
            .movement_metrics
            .iter()
            .map(|((shard_id, node_id), metrics)| MovementDriverView {
                shard_id: *shard_id,
                node_id: *node_id,
                metrics: metrics.snapshot(),
            })
            .collect();

        let redirect_snapshot = self.redirect_metrics.snapshot();
        let redirects = RedirectMetricsSnapshot {
            total: redirect_snapshot.values().sum(),
            by_cause: redirect_snapshot.into_iter().collect(),
        };

        let mut runtime = sample_runtime();
        runtime.node_timers = self.owned_timers;

        Ok(PlacementMetrics {
            runtime,
            catalog,
            group_counts: GroupCountsSnapshot {
                loaded,
                active,
                idle,
                error,
            },
            skew: skew_snapshot(&state),
            rpc: PlacementRpcMetrics {
                factories: rpc_factories,
                totals,
            },
            memory: MemorySnapshot {
                process_rss_bytes: process_rss_bytes(),
                per_replica,
                aggregate_state_machine_data_bytes,
            },
            redirects,
            movement,
            shards,
        })
    }

    /// Paginated per-shard inspection (§9, REQ-M4-OPS-003).
    ///
    /// Returns at most `limit` entries starting at `offset` over the
    /// shards sorted by id. Aggregate health stays available on the full
    /// snapshot even when per-shard inspection is limited.
    pub async fn metrics_for_shards(&self, offset: usize, limit: usize) -> Vec<ShardMetricsEntry> {
        let mut shards: Vec<u16> = self.shards.clone();
        shards.sort_unstable();
        let mut entries = Vec::new();
        for shard_id in shards.into_iter().skip(offset).take(limit) {
            entries.push(self.shard_entry(shard_id).await);
        }
        entries
    }

    /// Build one shard's inspection entry.
    async fn shard_entry(&self, shard_id: u16) -> ShardMetricsEntry {
        let mut replicas = Vec::new();
        let mut rpc = Vec::new();
        let mut movement = Vec::new();
        let mut active = false;

        if let Some(replica_map) = self.groups.get(&shard_id) {
            for (node_id, replica) in replica_map {
                if replica.raft.metrics().borrow().state == ServerState::Leader {
                    active = true;
                }
                let raft = match self.observers.get(&(shard_id, *node_id)) {
                    Some(observer) => match observer.snapshot().await {
                        Ok(status) => Some(GroupRaftView::from_status(shard_id, &status)),
                        Err(_) => None,
                    },
                    None => None,
                };
                let view = replica.state_machine.view().await;
                let state_machine_data_bytes: u64 = view
                    .data
                    .iter()
                    .map(|(key, value)| (key.len() + value.len()) as u64)
                    .sum();
                let snapshot_metrics = replica.state_machine.snapshot_metrics();
                let memory = ReplicaMemoryEntry {
                    shard_id,
                    node_id: *node_id,
                    state_machine_data_bytes,
                    snapshot_build_bytes: snapshot_metrics.build_bytes,
                    snapshot_install_bytes: snapshot_metrics.install_bytes,
                    snapshot_receive_peak_bytes: snapshot_metrics.receive_peak_bytes,
                };
                if let Some(raft) = raft {
                    replicas.push(ReplicaMetricsEntry {
                        node_id: *node_id,
                        raft,
                        memory,
                    });
                }
            }
        }
        if let Some(factories) = self.group_factories.get(&shard_id) {
            for (node_id, factory) in factories {
                rpc.push(factory_rpc_view(Some(shard_id), *node_id, factory));
            }
        }
        let mut fail_closed = false;
        for ((entry_shard, node_id), metrics) in &self.movement_metrics {
            if *entry_shard == shard_id {
                movement.push(MovementDriverView {
                    shard_id,
                    node_id: *node_id,
                    metrics: metrics.snapshot(),
                });
                if metrics.snapshot().operations_fail_closed > 0 {
                    fail_closed = true;
                }
            }
        }

        ShardMetricsEntry {
            shard_id,
            active,
            error: !active && fail_closed,
            replicas,
            movement,
            rpc,
        }
    }
}
