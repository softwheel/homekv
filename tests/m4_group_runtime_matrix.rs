//! Spec 0006 §5 (group/runtime/transport) library-level verification matrix.
//!
//! Traceability to `specs/0006-multi-raft-placement/verification.md` §5.
//! Each row is exercised through the public library API with small
//! deterministic capacities plus the full 1,024-group case where the row
//! demands it. Unit tests in `src/group_registry.rs`, `src/group_runtime.rs`,
//! `src/group_transport.rs` and `src/movement_admission.rs` cover the same
//! rows at component level; the tests here compose the components the way
//! the node lifecycle does, and cite those unit tests per row.
//!
//! | §5 row | Test here | Component-level twin |
//! |---|---|---|
//! | registry refuses capacity + 1 without partial allocation | `registry_refuses_capacity_plus_one_without_partial_allocation` | `group_registry::tests::catalog_has_a_separate_slot_and_capacity_rejection_allocates_nothing` |
//! | no group serves before coherent recovery | `no_group_serves_before_coherent_recovery` | `group_registry::tests::group_is_not_served_until_recovery_completes`, `multi_group_restart_recovery_is_isolated_and_recover_before_serve` |
//! | runtime OS-worker count stays at configured bound as groups grow | `runtime_worker_count_stays_at_configured_bound_as_groups_grow` (all 1,024 groups) | `group_runtime::tests::worker_count_is_constant_across_all_data_groups` |
//! | fixed topology uses shared peer connections independent of group count | `shared_peer_connections_are_independent_of_group_count` (all 1,024 groups) | `group_transport::tests::connection_count_depends_on_peers_and_width_not_groups` |
//! | per-group/per-peer/global RPC and byte saturation → explicit bounded failure/backpressure | `rpc_and_byte_saturation_returns_bounded_backpressure` | `group_transport::tests::layered_rpc_and_byte_limits_report_the_exhausted_scope`, `hot_group_is_bounded_without_starving_an_independent_group` |
//! | snapshot and movement bounds remain enforced | `snapshot_and_movement_bounds_are_enforced` | `group_transport::tests::snapshot_bytes_have_separate_peer_and_global_bounds`, `movement_admission::tests::*` |
//! | cancellation releases permits | `cancellation_releases_permits` | `group_registry::tests::cancelled_recovery_releases_reserved_slot_and_memory`, `group_transport::tests::hot_group_is_bounded_without_starving_an_independent_group` (cancellation counting) |
//! | one hot/delayed/unavailable group does not starve healthy groups | `hot_group_does_not_starve_healthy_groups` | `group_runtime::tests::hot_group_is_bounded_and_cannot_starve_a_healthy_group`, `group_registry::tests::saturated_group_does_not_block_an_unrelated_healthy_group`, `movement_admission::tests::saturated_node_does_not_block_an_unrelated_healthy_node` |
//! | group stop/restart/remove is idempotent | `group_stop_restart_remove_is_idempotent` | `group_registry::tests::memory_rejection_and_remove_restart_return_to_baseline` |
//! | aggregate accounting returns to baseline after cleanup | `aggregate_accounting_returns_to_baseline_after_cleanup` | `group_registry::tests::memory_rejection_and_remove_restart_return_to_baseline`, `multi_group_restart_recovery_is_isolated_and_recover_before_serve` |

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use homekv::group_registry::{
    GroupKind, GroupRegistry, GroupRegistryConfig, GroupRegistryError, RemovalOutcome,
};
use homekv::group_runtime::{
    QueueSaturationScope, SharedGroupRuntime, SharedGroupRuntimeConfig, SharedGroupRuntimeError,
};
use homekv::group_transport::{
    AdmissionResource, AdmissionScope, GroupRpcEnvelope, GroupRpcKind, SharedPeerTransport,
    SharedPeerTransportConfig, SharedPeerTransportError,
};
use homekv::movement_admission::{
    MovementWorkAdmission, MovementWorkConfig, MovementWorkError, MovementWorkScope,
};
use homekv::storage::LOGICAL_SHARD_COUNT;
use tokio::sync::oneshot;

fn registry_config(
    data_group_capacity: usize,
    memory_capacity_bytes: usize,
) -> GroupRegistryConfig {
    GroupRegistryConfig {
        data_group_capacity,
        memory_capacity_bytes,
        per_group_event_capacity: 8,
        per_group_foreground_capacity: 4,
    }
}

async fn recover(
    registry: &GroupRegistry<String>,
    kind: GroupKind,
    bytes: usize,
    value: &str,
) -> homekv::group_registry::RegistryAdmission<String> {
    let value = value.to_owned();
    registry
        .recover_or_get(kind, bytes, move || async move { Ok(value) })
        .await
        .unwrap()
}

fn transport_config() -> SharedPeerTransportConfig {
    SharedPeerTransportConfig {
        connection_pool_width: 2,
        global_inflight_rpcs: 4,
        global_inflight_bytes: 400,
        per_peer_inflight_rpcs: 3,
        per_peer_inflight_bytes: 300,
        per_group_inflight_rpcs: 1,
        per_group_inflight_bytes: 200,
        global_snapshot_inflight_bytes: 250,
        per_peer_snapshot_inflight_bytes: 150,
    }
}

fn envelope(shard_id: u16, request_id: u64, bytes: usize) -> GroupRpcEnvelope {
    GroupRpcEnvelope::new(
        GroupKind::Data { shard_id },
        GroupRpcKind::AppendEntries,
        request_id,
        bytes,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Row 1: registry refuses capacity + 1 without partial allocation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn registry_refuses_capacity_plus_one_without_partial_allocation() {
    let registry = GroupRegistry::new(registry_config(2, 64)).unwrap();
    recover(&registry, GroupKind::PlacementCatalog, 8, "catalog").await;
    recover(&registry, GroupKind::Data { shard_id: 0 }, 8, "zero").await;
    recover(&registry, GroupKind::Data { shard_id: 1 }, 8, "one").await;

    // The catalog group has its own slot: only data-group capacity is
    // exhausted. The rejected recovery callback must never run, so no
    // handle, byte budget, or reservation leaks into the registry.
    let invoked = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&invoked);
    let rejected = registry
        .recover_or_get(GroupKind::Data { shard_id: 2 }, 8, move || {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok("two".to_owned()) }
        })
        .await;
    assert!(matches!(
        rejected,
        Err(GroupRegistryError::DataCapacityExceeded { capacity: 2 })
    ));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    let metrics = registry.metrics();
    assert_eq!(metrics.ready_catalog_groups, 1);
    assert_eq!(metrics.ready_data_groups, 2);
    assert_eq!(metrics.recovering_groups, 0);
    assert_eq!(metrics.accounted_bytes, 24);
    assert_eq!(metrics.data_capacity_rejections, 1);
    assert!(registry
        .get(GroupKind::Data { shard_id: 2 })
        .unwrap()
        .is_none());
}

// ---------------------------------------------------------------------------
// Row 2: no group serves before coherent recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_group_serves_before_coherent_recovery() {
    let registry = GroupRegistry::<String>::new(registry_config(4, 64)).unwrap();
    let kind = GroupKind::Data { shard_id: 7 };
    let worker = registry.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        worker
            .recover_or_get(kind, 8, || async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok("recovered".to_owned())
            })
            .await
    });

    started_rx.await.unwrap();
    // While recovery is in flight the group is invisible to service:
    // `get` returns nothing and foreground admission fails closed.
    assert!(registry.get(kind).unwrap().is_none());
    assert!(matches!(
        registry.admit_foreground(kind),
        Err(GroupRegistryError::GroupNotAdmitted { .. })
    ));
    assert_eq!(registry.metrics().recovering_groups, 1);
    assert_eq!(registry.metrics().ready_data_groups, 0);

    release_tx.send(()).unwrap();
    let admission = task.await.unwrap().unwrap();
    assert!(admission.was_recovered());
    assert_eq!(registry.get(kind).unwrap().unwrap().as_str(), "recovered");
    // Now the recovered group admits foreground work.
    let _permit = registry.admit_foreground(kind).unwrap();
}

// ---------------------------------------------------------------------------
// Row 3: runtime OS-worker count stays at the configured bound as groups grow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runtime_worker_count_stays_at_configured_bound_as_groups_grow() {
    let runtime = SharedGroupRuntime::new(SharedGroupRuntimeConfig {
        worker_count: 2,
        global_queue_capacity: usize::from(LOGICAL_SHARD_COUNT),
        per_group_queue_capacity: 1,
    })
    .unwrap();
    // One unit of work per group across the full 1,024-group space.
    let mut receipts = Vec::new();
    for shard_id in 0..LOGICAL_SHARD_COUNT {
        receipts.push(
            runtime
                .try_submit(GroupKind::Data { shard_id }, async {})
                .unwrap(),
        );
    }
    for receipt in receipts {
        receipt.wait().await.unwrap();
    }
    let metrics = runtime.metrics();
    assert_eq!(metrics.worker_count, 2);
    assert_eq!(metrics.worker_tasks_spawned, 2);
    assert_eq!(metrics.known_groups, usize::from(LOGICAL_SHARD_COUNT));
    assert_eq!(metrics.submitted, u64::from(LOGICAL_SHARD_COUNT));
    assert_eq!(metrics.completed, u64::from(LOGICAL_SHARD_COUNT));
    assert_eq!(metrics.queued, 0);
    assert_eq!(metrics.running, 0);
    runtime.close();
}

// ---------------------------------------------------------------------------
// Row 4: shared peer connections are independent of group count
// ---------------------------------------------------------------------------

#[test]
fn shared_peer_connections_are_independent_of_group_count() {
    let transport = SharedPeerTransport::new([2, 3, 4], transport_config()).unwrap();
    // Admit one RPC per group across all 1,024 groups on a fixed 3-peer
    // topology: every group multiplexes over the same shared connections.
    for shard_id in 0..LOGICAL_SHARD_COUNT {
        transport
            .try_admit(2, envelope(shard_id, u64::from(shard_id) + 1, 1))
            .unwrap()
            .succeed();
    }
    let metrics = transport.metrics();
    assert_eq!(metrics.groups.len(), usize::from(LOGICAL_SHARD_COUNT));
    assert_eq!(metrics.configured_peers, 3);
    assert_eq!(metrics.connection_pool_width, 2);
    assert_eq!(metrics.connection_count, 6);
    assert!(
        metrics.peers.iter().all(|peer| peer.connection_count == 2),
        "connection count is a function of peers × pool width, not shards"
    );
    assert_eq!(metrics.admission.inflight_rpcs, 0);
    assert_eq!(metrics.admission.successes, u64::from(LOGICAL_SHARD_COUNT));
}

// ---------------------------------------------------------------------------
// Row 5: per-group/per-peer/global RPC and byte saturation → bounded failure
// ---------------------------------------------------------------------------

#[test]
fn rpc_and_byte_saturation_returns_bounded_backpressure() {
    let transport = SharedPeerTransport::new([2, 3], transport_config()).unwrap();

    // Per-group RPC bound first.
    let hot = transport.try_admit(2, envelope(0, 1, 100)).unwrap();
    assert_eq!(
        transport.try_admit(2, envelope(0, 2, 1)).unwrap_err(),
        SharedPeerTransportError::Saturated {
            peer_id: 2,
            group_id: 1,
            scope: AdmissionScope::Group,
            resource: AdmissionResource::Rpcs,
        }
    );
    // Per-peer byte bound next: peer 2 already carries 100 bytes, so one
    // more 200-byte RPC fills it exactly…
    let _peer_full = transport.try_admit(2, envelope(1, 3, 200)).unwrap();
    assert_eq!(
        transport.try_admit(2, envelope(2, 4, 1)).unwrap_err(),
        SharedPeerTransportError::Saturated {
            peer_id: 2,
            group_id: 3,
            scope: AdmissionScope::Peer,
            resource: AdmissionResource::Bytes,
        }
    );
    // …and the global byte bound trips on a fresh peer: 300 bytes are
    // already inflight globally, so 150 more exceed the 400 budget while
    // every per-peer/per-group scope still has headroom.
    assert_eq!(
        transport.try_admit(3, envelope(3, 5, 150)).unwrap_err(),
        SharedPeerTransportError::Saturated {
            peer_id: 3,
            group_id: 4,
            scope: AdmissionScope::Global,
            resource: AdmissionResource::Bytes,
        }
    );
    drop(hot);

    let metrics = transport.metrics();
    assert_eq!(metrics.admission.rejections, 3);
    assert_eq!(metrics.admission.inflight_rpcs, 1);
    assert_eq!(metrics.admission.inflight_bytes, 200);
    // Rejections are observable per scope without taking the transport down.
    let group = metrics.groups.iter().find(|g| g.group_id == 1).unwrap();
    assert_eq!(group.admission.rejections, 1);
}

// ---------------------------------------------------------------------------
// Row 6: snapshot and movement bounds remain enforced
// ---------------------------------------------------------------------------

#[test]
fn snapshot_and_movement_bounds_are_enforced() {
    let transport = SharedPeerTransport::new([2, 3], transport_config()).unwrap();
    let snapshot = |shard_id, request_id, payload_bytes| {
        GroupRpcEnvelope::new(
            GroupKind::Data { shard_id },
            GroupRpcKind::InstallSnapshot,
            request_id,
            payload_bytes,
        )
        .unwrap()
    };
    // Per-peer snapshot-byte bound.
    let first = transport.try_admit(2, snapshot(0, 1, 150)).unwrap();
    assert_eq!(
        transport.try_admit(2, snapshot(1, 2, 1)).unwrap_err(),
        SharedPeerTransportError::Saturated {
            peer_id: 2,
            group_id: 2,
            scope: AdmissionScope::Peer,
            resource: AdmissionResource::SnapshotBytes,
        }
    );
    // Global snapshot-byte bound: 150 + 101 > 250.
    assert_eq!(
        transport.try_admit(3, snapshot(1, 3, 101)).unwrap_err(),
        SharedPeerTransportError::Saturated {
            peer_id: 3,
            group_id: 2,
            scope: AdmissionScope::Global,
            resource: AdmissionResource::SnapshotBytes,
        }
    );
    drop(first);
    assert_eq!(transport.metrics().admission.snapshot_inflight_bytes, 0);

    // Movement-work bounds: per-node and cluster-wide, permit-based.
    let admission = MovementWorkAdmission::new(MovementWorkConfig {
        cluster_max_concurrent: 2,
        per_node_max_concurrent: 1,
    })
    .unwrap();
    let _node1 = admission.try_admit(1).unwrap();
    assert_eq!(
        admission.try_admit(1).unwrap_err(),
        MovementWorkError::Saturated {
            node_id: 1,
            scope: MovementWorkScope::Node,
        }
    );
    let _node2 = admission.try_admit(2).unwrap();
    assert_eq!(
        admission.try_admit(3).unwrap_err(),
        MovementWorkError::Saturated {
            node_id: 3,
            scope: MovementWorkScope::Cluster,
        }
    );
    let metrics = admission.metrics();
    assert_eq!(metrics.cluster_inflight, 2);
    assert_eq!(metrics.node_rejections, 1);
    assert_eq!(metrics.cluster_rejections, 1);
}

// ---------------------------------------------------------------------------
// Row 7: cancellation releases permits
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancellation_releases_permits() {
    // Registry: aborting a recovery releases its slot and byte budget.
    let registry = GroupRegistry::<String>::new(registry_config(1, 8)).unwrap();
    let worker = registry.clone();
    let (started_tx, started_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        worker
            .recover_or_get(GroupKind::Data { shard_id: 20 }, 8, || async move {
                started_tx.send(()).unwrap();
                release_rx.await.map_err(|error| error.to_string())?;
                Ok("never".to_owned())
            })
            .await
    });
    started_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let metrics = registry.metrics();
    assert_eq!(metrics.recovering_groups, 0);
    assert_eq!(metrics.accounted_bytes, 0);
    assert_eq!(metrics.recovery_cancellations, 1);
    // The slot is immediately reusable.
    assert!(
        recover(&registry, GroupKind::Data { shard_id: 20 }, 8, "retried")
            .await
            .was_recovered()
    );

    // Transport: dropping a permit without succeed()/fail() counts a
    // cancellation and releases rpc/byte accounting at every scope.
    let transport = SharedPeerTransport::new([2], transport_config()).unwrap();
    let permit = transport.try_admit(2, envelope(0, 1, 100)).unwrap();
    drop(permit);
    let metrics = transport.metrics();
    assert_eq!(metrics.admission.cancellations, 1);
    assert_eq!(metrics.admission.inflight_rpcs, 0);
    assert_eq!(metrics.admission.inflight_bytes, 0);
    let peer = metrics.peers.iter().find(|p| p.peer_id == 2).unwrap();
    assert_eq!(peer.admission.cancellations, 1);
    assert_eq!(peer.admission.inflight_rpcs, 0);
}

// ---------------------------------------------------------------------------
// Row 8: a hot group does not starve independent healthy groups
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hot_group_does_not_starve_healthy_groups() {
    let runtime = SharedGroupRuntime::new(SharedGroupRuntimeConfig {
        worker_count: 1,
        global_queue_capacity: 8,
        per_group_queue_capacity: 1,
    })
    .unwrap();
    let hot = GroupKind::Data { shard_id: 0 };
    let healthy = GroupKind::Data { shard_id: 1 };

    // Occupy the single worker with a blocked hot-group job, then fill
    // the hot group's queue so further hot submissions are rejected.
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let blocker = runtime
        .try_submit(hot, async move {
            started_tx.send(()).unwrap();
            release_rx.await.unwrap();
        })
        .unwrap();
    started_rx.await.unwrap();
    let _hot_queued = runtime.try_submit(hot, async {}).unwrap();
    assert!(matches!(
        runtime.try_submit(hot, async {}),
        Err(SharedGroupRuntimeError::QueueSaturated {
            scope: QueueSaturationScope::Group,
            ..
        })
    ));

    // The healthy group still gets its work through: the runtime drains
    // per-group queues round-robin instead of head-of-line blocking.
    let (done_tx, done_rx) = oneshot::channel();
    let healthy_receipt = runtime
        .try_submit(healthy, async move {
            done_tx.send(()).unwrap();
        })
        .unwrap();
    release_tx.send(()).unwrap();
    blocker.wait().await.unwrap();
    done_rx.await.unwrap();
    healthy_receipt.wait().await.unwrap();

    let metrics = runtime.metrics();
    assert_eq!(metrics.group_rejections, 1);
    assert_eq!(metrics.global_rejections, 0);
    assert_eq!(metrics.completed, 3);
    runtime.close();
}

// ---------------------------------------------------------------------------
// Row 9: group stop/restart/remove is idempotent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn group_stop_restart_remove_is_idempotent() {
    let registry = GroupRegistry::<String>::new(registry_config(2, 32)).unwrap();
    let kind = GroupKind::Data { shard_id: 30 };

    // Removing an absent group is a no-op, not an error.
    assert_eq!(
        registry.remove(kind).unwrap(),
        RemovalOutcome::AlreadyAbsent
    );
    recover(&registry, kind, 8, "v1").await;
    assert_eq!(registry.remove(kind).unwrap(), RemovalOutcome::Removed);
    // Double remove stays idempotent.
    assert_eq!(
        registry.remove(kind).unwrap(),
        RemovalOutcome::AlreadyAbsent
    );
    // A removed group fails closed for admission…
    assert!(matches!(
        registry.admit_foreground(kind),
        Err(GroupRegistryError::GroupNotAdmitted { .. })
    ));
    // …and restarts cleanly through the same recovery path.
    let restarted = recover(&registry, kind, 8, "v2").await;
    assert!(restarted.was_recovered());
    assert_eq!(restarted.handle().as_str(), "v2");
    assert_eq!(registry.remove(kind).unwrap(), RemovalOutcome::Removed);

    let metrics = registry.metrics();
    assert_eq!(metrics.ready_data_groups, 0);
    assert_eq!(metrics.removals, 2);
}

// ---------------------------------------------------------------------------
// Row 10: aggregate accounting returns to baseline after cleanup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_accounting_returns_to_baseline_after_cleanup() {
    let registry = GroupRegistry::<String>::new(registry_config(4, 64)).unwrap();
    recover(&registry, GroupKind::PlacementCatalog, 8, "catalog").await;
    for shard_id in 0..4u16 {
        recover(
            &registry,
            GroupKind::Data { shard_id },
            8,
            &format!("group-{shard_id}"),
        )
        .await;
    }
    let before = registry.metrics();
    assert_eq!(before.ready_catalog_groups, 1);
    assert_eq!(before.ready_data_groups, 4);
    assert_eq!(before.accounted_bytes, 40);

    // Saturate one group's foreground admission, then remove everything:
    // permits and admission state must not leak into the baseline.
    let victim = GroupKind::Data { shard_id: 0 };
    let _permit = registry.admit_foreground(victim).unwrap();
    assert_eq!(
        registry.remove(GroupKind::PlacementCatalog).unwrap(),
        RemovalOutcome::Removed
    );
    for shard_id in 0..4u16 {
        assert_eq!(
            registry.remove(GroupKind::Data { shard_id }).unwrap(),
            RemovalOutcome::Removed
        );
    }

    let after = registry.metrics();
    assert_eq!(after.ready_catalog_groups, 0);
    assert_eq!(after.ready_data_groups, 0);
    assert_eq!(after.recovering_groups, 0);
    assert_eq!(after.accounted_bytes, 0);
    assert_eq!(after.removals, 5);
    assert!(registry.admission_metrics().is_empty());
}
