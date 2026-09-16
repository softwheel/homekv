use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_derive::{Deserialize, Serialize};

use crate::group_registry::{GroupKind, GroupRegistryError};
use crate::placement::GroupId;
use crate::raft::RaftNodeId;
use crate::storage::LOGICAL_SHARD_COUNT;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedPeerTransportConfig {
    pub connection_pool_width: usize,
    pub global_inflight_rpcs: usize,
    pub global_inflight_bytes: usize,
    pub per_peer_inflight_rpcs: usize,
    pub per_peer_inflight_bytes: usize,
    pub per_group_inflight_rpcs: usize,
    pub per_group_inflight_bytes: usize,
    pub global_snapshot_inflight_bytes: usize,
    pub per_peer_snapshot_inflight_bytes: usize,
}

impl SharedPeerTransportConfig {
    pub fn validate(self) -> Result<Self, SharedPeerTransportError> {
        if self.connection_pool_width == 0 {
            return Err(SharedPeerTransportError::InvalidConnectionPoolWidth);
        }
        if self.global_inflight_rpcs == 0
            || self.per_peer_inflight_rpcs == 0
            || self.per_group_inflight_rpcs == 0
        {
            return Err(SharedPeerTransportError::InvalidRpcCapacity);
        }
        if self.global_inflight_bytes == 0
            || self.per_peer_inflight_bytes == 0
            || self.per_group_inflight_bytes == 0
            || self.global_snapshot_inflight_bytes == 0
            || self.per_peer_snapshot_inflight_bytes == 0
        {
            return Err(SharedPeerTransportError::InvalidByteCapacity);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GroupRpcKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupRpcEnvelope {
    group_id: GroupId,
    kind: GroupRpcKind,
    request_id: u64,
    payload_bytes: usize,
}

impl GroupRpcEnvelope {
    pub fn new(
        group: GroupKind,
        kind: GroupRpcKind,
        request_id: u64,
        payload_bytes: usize,
    ) -> Result<Self, SharedPeerTransportError> {
        if request_id == 0 {
            return Err(SharedPeerTransportError::InvalidRequestId);
        }
        if payload_bytes == 0 {
            return Err(SharedPeerTransportError::InvalidPayloadBytes);
        }
        Ok(Self {
            group_id: group.group_id().map_err(map_group_error)?,
            kind,
            request_id,
            payload_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdmissionScope {
    Global,
    Peer,
    Group,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AdmissionResource {
    Rpcs,
    Bytes,
    SnapshotBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SharedPeerTransportError {
    InvalidConnectionPoolWidth,
    InvalidRpcCapacity,
    InvalidByteCapacity,
    InvalidRequestId,
    InvalidPayloadBytes,
    UnknownPeer { peer_id: RaftNodeId },
    UnknownGroup { shard_id: u16 },
    InvalidGroupId { group_id: GroupId },
    Saturated {
        peer_id: RaftNodeId,
        group_id: GroupId,
        scope: AdmissionScope,
        resource: AdmissionResource,
    },
}

impl fmt::Display for SharedPeerTransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConnectionPoolWidth => write!(f, "connection pool width must be non-zero"),
            Self::InvalidRpcCapacity => write!(f, "RPC capacities must be non-zero"),
            Self::InvalidByteCapacity => write!(f, "byte capacities must be non-zero"),
            Self::InvalidRequestId => write!(f, "request id must be non-zero"),
            Self::InvalidPayloadBytes => write!(f, "payload size must be non-zero"),
            Self::UnknownPeer { peer_id } => write!(f, "peer {peer_id} is not configured"),
            Self::UnknownGroup { shard_id } => {
                write!(f, "data group for logical shard {shard_id} does not exist")
            }
            Self::InvalidGroupId { group_id } => write!(f, "group id {group_id} does not exist"),
            Self::Saturated {
                peer_id,
                group_id,
                scope,
                resource,
            } => write!(
                f,
                "shared transport {scope:?} {resource:?} capacity is exhausted for peer {peer_id} group {group_id}"
            ),
        }
    }
}

impl std::error::Error for SharedPeerTransportError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionMetrics {
    pub inflight_rpcs: usize,
    pub peak_inflight_rpcs: usize,
    pub inflight_bytes: usize,
    pub peak_inflight_bytes: usize,
    pub snapshot_inflight_bytes: usize,
    pub peak_snapshot_inflight_bytes: usize,
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub cancellations: u64,
    pub rejections: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PeerTransportMetrics {
    pub peer_id: RaftNodeId,
    pub connection_count: usize,
    pub admission: AdmissionMetrics,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupTransportMetrics {
    pub group_id: GroupId,
    pub admission: AdmissionMetrics,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedPeerTransportMetrics {
    pub connection_pool_width: usize,
    pub configured_peers: usize,
    pub connection_count: usize,
    pub admission: AdmissionMetrics,
    pub peers: Vec<PeerTransportMetrics>,
    pub groups: Vec<GroupTransportMetrics>,
}

#[derive(Clone)]
pub struct SharedPeerTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    config: SharedPeerTransportConfig,
    peers: BTreeSet<RaftNodeId>,
    state: Mutex<TransportState>,
}

#[derive(Default)]
struct TransportState {
    admission: MutableAdmissionMetrics,
    peers: BTreeMap<RaftNodeId, MutableAdmissionMetrics>,
    groups: BTreeMap<GroupId, MutableAdmissionMetrics>,
}

#[derive(Default)]
struct MutableAdmissionMetrics {
    inflight_rpcs: usize,
    peak_inflight_rpcs: usize,
    inflight_bytes: usize,
    peak_inflight_bytes: usize,
    snapshot_inflight_bytes: usize,
    peak_snapshot_inflight_bytes: usize,
    attempts: u64,
    successes: u64,
    failures: u64,
    cancellations: u64,
    rejections: u64,
}

impl SharedPeerTransport {
    pub fn new<I>(
        peers: I,
        config: SharedPeerTransportConfig,
    ) -> Result<Self, SharedPeerTransportError>
    where
        I: IntoIterator<Item = RaftNodeId>,
    {
        let config = config.validate()?;
        let peers = peers.into_iter().collect::<BTreeSet<_>>();
        let peer_metrics = peers
            .iter()
            .copied()
            .map(|peer_id| (peer_id, MutableAdmissionMetrics::default()))
            .collect();
        Ok(Self {
            inner: Arc::new(TransportInner {
                config,
                peers,
                state: Mutex::new(TransportState {
                    admission: MutableAdmissionMetrics::default(),
                    peers: peer_metrics,
                    groups: BTreeMap::new(),
                }),
            }),
        })
    }

    pub fn try_admit(
        &self,
        peer_id: RaftNodeId,
        envelope: GroupRpcEnvelope,
    ) -> Result<TransportPermit, SharedPeerTransportError> {
        if !self.inner.peers.contains(&peer_id) {
            return Err(SharedPeerTransportError::UnknownPeer { peer_id });
        }
        if envelope.group_id > u64::from(LOGICAL_SHARD_COUNT) {
            return Err(SharedPeerTransportError::InvalidGroupId {
                group_id: envelope.group_id,
            });
        }
        if envelope.request_id == 0 {
            return Err(SharedPeerTransportError::InvalidRequestId);
        }
        if envelope.payload_bytes == 0 {
            return Err(SharedPeerTransportError::InvalidPayloadBytes);
        }

        let mut state = lock_state(&self.inner.state);
        state.admission.attempts = state.admission.attempts.saturating_add(1);
        let peer = state.peers.get_mut(&peer_id).expect("configured peer");
        peer.attempts = peer.attempts.saturating_add(1);
        let group = state.groups.entry(envelope.group_id).or_default();
        group.attempts = group.attempts.saturating_add(1);

        let snapshot_bytes = if envelope.kind == GroupRpcKind::InstallSnapshot {
            envelope.payload_bytes
        } else {
            0
        };
        let checks = [
            (
                AdmissionScope::Group,
                AdmissionResource::Rpcs,
                state.groups[&envelope.group_id].inflight_rpcs,
                1,
                self.inner.config.per_group_inflight_rpcs,
            ),
            (
                AdmissionScope::Group,
                AdmissionResource::Bytes,
                state.groups[&envelope.group_id].inflight_bytes,
                envelope.payload_bytes,
                self.inner.config.per_group_inflight_bytes,
            ),
            (
                AdmissionScope::Peer,
                AdmissionResource::Rpcs,
                state.peers[&peer_id].inflight_rpcs,
                1,
                self.inner.config.per_peer_inflight_rpcs,
            ),
            (
                AdmissionScope::Peer,
                AdmissionResource::Bytes,
                state.peers[&peer_id].inflight_bytes,
                envelope.payload_bytes,
                self.inner.config.per_peer_inflight_bytes,
            ),
            (
                AdmissionScope::Global,
                AdmissionResource::Rpcs,
                state.admission.inflight_rpcs,
                1,
                self.inner.config.global_inflight_rpcs,
            ),
            (
                AdmissionScope::Global,
                AdmissionResource::Bytes,
                state.admission.inflight_bytes,
                envelope.payload_bytes,
                self.inner.config.global_inflight_bytes,
            ),
            (
                AdmissionScope::Peer,
                AdmissionResource::SnapshotBytes,
                state.peers[&peer_id].snapshot_inflight_bytes,
                snapshot_bytes,
                self.inner.config.per_peer_snapshot_inflight_bytes,
            ),
            (
                AdmissionScope::Global,
                AdmissionResource::SnapshotBytes,
                state.admission.snapshot_inflight_bytes,
                snapshot_bytes,
                self.inner.config.global_snapshot_inflight_bytes,
            ),
        ];
        for (scope, resource, used, requested, capacity) in checks {
            if requested > capacity.saturating_sub(used) {
                reject(&mut state, peer_id, envelope.group_id);
                return Err(SharedPeerTransportError::Saturated {
                    peer_id,
                    group_id: envelope.group_id,
                    scope,
                    resource,
                });
            }
        }

        admit(
            &mut state,
            peer_id,
            envelope.group_id,
            envelope.payload_bytes,
            snapshot_bytes,
        );
        let connection_slot = (envelope.request_id as usize) % self.inner.config.connection_pool_width;
        Ok(TransportPermit {
            inner: Arc::clone(&self.inner),
            peer_id,
            group_id: envelope.group_id,
            payload_bytes: envelope.payload_bytes,
            snapshot_bytes,
            connection_slot,
            outcome: PermitOutcome::Cancelled,
        })
    }

    pub fn metrics(&self) -> SharedPeerTransportMetrics {
        let state = lock_state(&self.inner.state);
        SharedPeerTransportMetrics {
            connection_pool_width: self.inner.config.connection_pool_width,
            configured_peers: self.inner.peers.len(),
            connection_count: self
                .inner
                .peers
                .len()
                .saturating_mul(self.inner.config.connection_pool_width),
            admission: snapshot_metrics(&state.admission),
            peers: state
                .peers
                .iter()
                .map(|(&peer_id, metrics)| PeerTransportMetrics {
                    peer_id,
                    connection_count: self.inner.config.connection_pool_width,
                    admission: snapshot_metrics(metrics),
                })
                .collect(),
            groups: state
                .groups
                .iter()
                .map(|(&group_id, metrics)| GroupTransportMetrics {
                    group_id,
                    admission: snapshot_metrics(metrics),
                })
                .collect(),
        }
    }
}

pub struct TransportPermit {
    inner: Arc<TransportInner>,
    peer_id: RaftNodeId,
    group_id: GroupId,
    payload_bytes: usize,
    snapshot_bytes: usize,
    connection_slot: usize,
    outcome: PermitOutcome,
}

impl fmt::Debug for TransportPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportPermit")
            .field("peer_id", &self.peer_id)
            .field("group_id", &self.group_id)
            .field("payload_bytes", &self.payload_bytes)
            .field("snapshot_bytes", &self.snapshot_bytes)
            .field("connection_slot", &self.connection_slot)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum PermitOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

impl TransportPermit {
    pub fn connection_slot(&self) -> usize {
        self.connection_slot
    }

    pub fn succeed(mut self) {
        self.outcome = PermitOutcome::Succeeded;
    }

    pub fn fail(mut self) {
        self.outcome = PermitOutcome::Failed;
    }
}

impl Drop for TransportPermit {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner.state);
        release(
            &mut state,
            self.peer_id,
            self.group_id,
            self.payload_bytes,
            self.snapshot_bytes,
            self.outcome,
        );
    }
}

fn reject(state: &mut TransportState, peer_id: RaftNodeId, group_id: GroupId) {
    state.admission.rejections = state.admission.rejections.saturating_add(1);
    state.peers.get_mut(&peer_id).expect("configured peer").rejections += 1;
    state.groups.get_mut(&group_id).expect("known group").rejections += 1;
}

fn admit(
    state: &mut TransportState,
    peer_id: RaftNodeId,
    group_id: GroupId,
    payload_bytes: usize,
    snapshot_bytes: usize,
) {
    update_admitted(&mut state.admission, payload_bytes, snapshot_bytes);
    update_admitted(
        state.peers.get_mut(&peer_id).expect("configured peer"),
        payload_bytes,
        snapshot_bytes,
    );
    update_admitted(
        state.groups.get_mut(&group_id).expect("known group"),
        payload_bytes,
        snapshot_bytes,
    );
}

fn update_admitted(metrics: &mut MutableAdmissionMetrics, bytes: usize, snapshot_bytes: usize) {
    metrics.inflight_rpcs += 1;
    metrics.peak_inflight_rpcs = metrics.peak_inflight_rpcs.max(metrics.inflight_rpcs);
    metrics.inflight_bytes += bytes;
    metrics.peak_inflight_bytes = metrics.peak_inflight_bytes.max(metrics.inflight_bytes);
    metrics.snapshot_inflight_bytes += snapshot_bytes;
    metrics.peak_snapshot_inflight_bytes = metrics
        .peak_snapshot_inflight_bytes
        .max(metrics.snapshot_inflight_bytes);
}

fn release(
    state: &mut TransportState,
    peer_id: RaftNodeId,
    group_id: GroupId,
    bytes: usize,
    snapshot_bytes: usize,
    outcome: PermitOutcome,
) {
    update_released(&mut state.admission, bytes, snapshot_bytes, outcome);
    update_released(
        state.peers.get_mut(&peer_id).expect("configured peer"),
        bytes,
        snapshot_bytes,
        outcome,
    );
    update_released(
        state.groups.get_mut(&group_id).expect("known group"),
        bytes,
        snapshot_bytes,
        outcome,
    );
}

fn update_released(
    metrics: &mut MutableAdmissionMetrics,
    bytes: usize,
    snapshot_bytes: usize,
    outcome: PermitOutcome,
) {
    metrics.inflight_rpcs -= 1;
    metrics.inflight_bytes -= bytes;
    metrics.snapshot_inflight_bytes -= snapshot_bytes;
    match outcome {
        PermitOutcome::Succeeded => metrics.successes = metrics.successes.saturating_add(1),
        PermitOutcome::Failed => metrics.failures = metrics.failures.saturating_add(1),
        PermitOutcome::Cancelled => {
            metrics.cancellations = metrics.cancellations.saturating_add(1)
        }
    }
}

fn snapshot_metrics(metrics: &MutableAdmissionMetrics) -> AdmissionMetrics {
    AdmissionMetrics {
        inflight_rpcs: metrics.inflight_rpcs,
        peak_inflight_rpcs: metrics.peak_inflight_rpcs,
        inflight_bytes: metrics.inflight_bytes,
        peak_inflight_bytes: metrics.peak_inflight_bytes,
        snapshot_inflight_bytes: metrics.snapshot_inflight_bytes,
        peak_snapshot_inflight_bytes: metrics.peak_snapshot_inflight_bytes,
        attempts: metrics.attempts,
        successes: metrics.successes,
        failures: metrics.failures,
        cancellations: metrics.cancellations,
        rejections: metrics.rejections,
    }
}

fn lock_state<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn map_group_error(error: GroupRegistryError) -> SharedPeerTransportError {
    match error {
        GroupRegistryError::UnknownDataGroup { shard_id } => {
            SharedPeerTransportError::UnknownGroup { shard_id }
        }
        _ => unreachable!("group id conversion has only one failure mode"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> SharedPeerTransportConfig {
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

    #[test]
    fn connection_count_depends_on_peers_and_width_not_groups() {
        let transport = SharedPeerTransport::new([2, 3, 4], config()).unwrap();
        for shard_id in 0..LOGICAL_SHARD_COUNT {
            transport
                .try_admit(2, envelope(shard_id, u64::from(shard_id) + 1, 1))
                .unwrap()
                .succeed();
        }
        let metrics = transport.metrics();
        assert_eq!(metrics.groups.len(), usize::from(LOGICAL_SHARD_COUNT));
        assert_eq!(metrics.configured_peers, 3);
        assert_eq!(metrics.connection_count, 6);
        assert!(metrics.peers.iter().all(|peer| peer.connection_count == 2));
    }

    #[test]
    fn hot_group_is_bounded_without_starving_an_independent_group() {
        let transport = SharedPeerTransport::new([2], config()).unwrap();
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
        let healthy = transport.try_admit(2, envelope(1, 3, 100)).unwrap();
        healthy.succeed();
        drop(hot);
        transport.try_admit(2, envelope(0, 4, 1)).unwrap().succeed();

        let metrics = transport.metrics();
        assert_eq!(metrics.admission.cancellations, 1);
        assert_eq!(metrics.admission.successes, 2);
        assert_eq!(metrics.admission.rejections, 1);
        assert_eq!(metrics.admission.inflight_rpcs, 0);
        assert_eq!(metrics.admission.inflight_bytes, 0);
    }

    #[test]
    fn layered_rpc_and_byte_limits_report_the_exhausted_scope() {
        let transport = SharedPeerTransport::new([2, 3], config()).unwrap();
        let first = transport.try_admit(2, envelope(0, 1, 160)).unwrap();
        assert_eq!(
            transport.try_admit(2, envelope(1, 2, 160)).unwrap_err(),
            SharedPeerTransportError::Saturated {
                peer_id: 2,
                group_id: 2,
                scope: AdmissionScope::Peer,
                resource: AdmissionResource::Bytes,
            }
        );
        let second = transport.try_admit(3, envelope(1, 3, 160)).unwrap();
        assert_eq!(
            transport.try_admit(3, envelope(2, 4, 100)).unwrap_err(),
            SharedPeerTransportError::Saturated {
                peer_id: 3,
                group_id: 3,
                scope: AdmissionScope::Global,
                resource: AdmissionResource::Bytes,
            }
        );
        first.fail();
        second.succeed();
        let metrics = transport.metrics();
        assert_eq!(metrics.admission.failures, 1);
        assert_eq!(metrics.admission.successes, 1);
        assert_eq!(metrics.admission.rejections, 2);
    }

    #[test]
    fn snapshot_bytes_have_separate_peer_and_global_bounds() {
        let transport = SharedPeerTransport::new([2, 3], config()).unwrap();
        let snapshot = |shard_id, request_id, payload_bytes| {
            GroupRpcEnvelope::new(
                GroupKind::Data { shard_id },
                GroupRpcKind::InstallSnapshot,
                request_id,
                payload_bytes,
            )
            .unwrap()
        };
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
        let second = transport.try_admit(3, snapshot(1, 3, 100)).unwrap();
        assert_eq!(
            transport.try_admit(3, snapshot(2, 4, 1)).unwrap_err(),
            SharedPeerTransportError::Saturated {
                peer_id: 3,
                group_id: 3,
                scope: AdmissionScope::Global,
                resource: AdmissionResource::SnapshotBytes,
            }
        );
        drop(first);
        drop(second);
        assert_eq!(transport.metrics().admission.snapshot_inflight_bytes, 0);
    }

    #[test]
    fn invalid_requests_do_not_allocate_group_or_peer_state() {
        let transport = SharedPeerTransport::new([2], config()).unwrap();
        assert!(matches!(
            transport.try_admit(9, envelope(0, 1, 1)),
            Err(SharedPeerTransportError::UnknownPeer { peer_id: 9 })
        ));
        assert!(matches!(
            GroupRpcEnvelope::new(
                GroupKind::Data {
                    shard_id: LOGICAL_SHARD_COUNT,
                },
                GroupRpcKind::Vote,
                1,
                1,
            ),
            Err(SharedPeerTransportError::UnknownGroup { .. })
        ));
        let metrics = transport.metrics();
        assert!(metrics.groups.is_empty());
        assert_eq!(metrics.admission.attempts, 0);
    }
}
