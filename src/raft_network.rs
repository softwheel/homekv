use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Raft, VoteRequest, VoteResponse,
};
use serde_derive::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;

use crate::raft::{HomeKvRaftConfig, RaftNode, RaftNodeId};
use crate::raft_transport::{
    BootstrapError, LinkRule, PerPeerRpcLimiter, TestLinkController, ThreeNodeBootstrap,
};

type RpcResult<T, E = RaftError<RaftNodeId>> = Result<T, RPCError<RaftNodeId, RaftNode, E>>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PeerRpcMetricsSnapshot {
    pub target_node_id: RaftNodeId,
    pub capacity: usize,
    pub current: usize,
    pub peak: usize,
    pub attempts: u64,
    pub failures: u64,
    pub backpressure_rejections: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RaftNetworkMetricsSnapshot {
    pub peers: Vec<PeerRpcMetricsSnapshot>,
}

#[derive(Debug)]
struct PeerRpcMetrics {
    target_node_id: RaftNodeId,
    capacity: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
    attempts: AtomicU64,
    failures: AtomicU64,
    backpressure_rejections: AtomicU64,
}

impl PeerRpcMetrics {
    fn started(&self) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn admitted(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> RpcAttempt {
        let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(current, Ordering::Relaxed);
        RpcAttempt {
            _permit: permit,
            metrics: self.clone(),
            completed: false,
        }
    }

    fn failed(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn rejected(&self) {
        self.backpressure_rejections
            .fetch_add(1, Ordering::Relaxed);
        self.failed();
    }

    fn snapshot(&self) -> PeerRpcMetricsSnapshot {
        PeerRpcMetricsSnapshot {
            target_node_id: self.target_node_id,
            capacity: self.capacity,
            current: self.current.load(Ordering::Relaxed),
            peak: self.peak.load(Ordering::Relaxed),
            attempts: self.attempts.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            backpressure_rejections: self.backpressure_rejections.load(Ordering::Relaxed),
        }
    }
}

struct RpcAttempt {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<PeerRpcMetrics>,
    completed: bool,
}

impl RpcAttempt {
    fn succeeded(&mut self) {
        self.completed = true;
    }

    fn failed(&mut self) {
        self.metrics.failed();
        self.completed = true;
    }
}

impl Drop for RpcAttempt {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.failed();
        }
        self.metrics.current.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug)]
struct RaftNetworkMetrics {
    peers: Arc<BTreeMap<RaftNodeId, Arc<PeerRpcMetrics>>>,
}

impl RaftNetworkMetrics {
    fn new(bootstrap: &ThreeNodeBootstrap, capacity: usize) -> Self {
        let peers = bootstrap
            .nodes
            .keys()
            .copied()
            .map(|target_node_id| {
                (
                    target_node_id,
                    Arc::new(PeerRpcMetrics {
                        target_node_id,
                        capacity,
                        current: AtomicUsize::new(0),
                        peak: AtomicUsize::new(0),
                        attempts: AtomicU64::new(0),
                        failures: AtomicU64::new(0),
                        backpressure_rejections: AtomicU64::new(0),
                    }),
                )
            })
            .collect();
        Self {
            peers: Arc::new(peers),
        }
    }

    fn peer(&self, node_id: RaftNodeId) -> Option<Arc<PeerRpcMetrics>> {
        self.peers.get(&node_id).cloned()
    }

    fn snapshot(&self) -> RaftNetworkMetricsSnapshot {
        RaftNetworkMetricsSnapshot {
            peers: self.peers.values().map(|peer| peer.snapshot()).collect(),
        }
    }
}

#[async_trait]
pub trait RaftRpcHandler: Send + Sync + 'static {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<HomeKvRaftConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RaftError<RaftNodeId>>;

    async fn vote(
        &self,
        req: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, RaftError<RaftNodeId>>;

    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<HomeKvRaftConfig>,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RaftError<RaftNodeId, InstallSnapshotError>,
    >;
}

#[async_trait]
impl RaftRpcHandler for Raft<HomeKvRaftConfig> {
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<HomeKvRaftConfig>,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RaftError<RaftNodeId>> {
        Raft::append_entries(self, req).await
    }

    async fn vote(
        &self,
        req: VoteRequest<RaftNodeId>,
    ) -> Result<VoteResponse<RaftNodeId>, RaftError<RaftNodeId>> {
        Raft::vote(self, req).await
    }

    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<HomeKvRaftConfig>,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RaftError<RaftNodeId, InstallSnapshotError>,
    > {
        #[allow(deprecated)]
        Raft::install_snapshot(self, req).await
    }
}

#[derive(Clone)]
pub struct HomeKvRaftNetworkFactory {
    local_id: RaftNodeId,
    bootstrap: Arc<ThreeNodeBootstrap>,
    limiter: PerPeerRpcLimiter,
    metrics: RaftNetworkMetrics,
    links: TestLinkController,
    handlers: Arc<RwLock<BTreeMap<RaftNodeId, Arc<dyn RaftRpcHandler>>>>,
}

impl HomeKvRaftNetworkFactory {
    pub fn new(
        local_id: RaftNodeId,
        bootstrap: ThreeNodeBootstrap,
        max_outstanding_per_peer: usize,
        links: TestLinkController,
    ) -> Result<Self, BootstrapError> {
        bootstrap.validate()?;
        if !bootstrap.nodes.contains_key(&local_id) {
            return Err(BootstrapError::UnknownPeer { node_id: local_id });
        }
        let limiter = PerPeerRpcLimiter::new(&bootstrap, max_outstanding_per_peer)?;
        let metrics = RaftNetworkMetrics::new(&bootstrap, max_outstanding_per_peer);
        Ok(Self {
            local_id,
            bootstrap: Arc::new(bootstrap),
            limiter,
            metrics,
            links,
            handlers: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub fn local_id(&self) -> RaftNodeId {
        self.local_id
    }

    pub fn metrics(&self) -> RaftNetworkMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn register_handler(
        &self,
        node_id: RaftNodeId,
        handler: Arc<dyn RaftRpcHandler>,
    ) -> Result<(), BootstrapError> {
        if !self.bootstrap.nodes.contains_key(&node_id) {
            return Err(BootstrapError::UnknownPeer { node_id });
        }
        self.handlers
            .write()
            .expect("raft handler registry lock poisoned")
            .insert(node_id, handler);
        Ok(())
    }
}

pub struct HomeKvRaftNetworkConnection {
    source: RaftNodeId,
    target: RaftNodeId,
    target_node: RaftNode,
    bootstrap: Arc<ThreeNodeBootstrap>,
    limiter: PerPeerRpcLimiter,
    metrics: RaftNetworkMetrics,
    links: TestLinkController,
    handlers: Arc<RwLock<BTreeMap<RaftNodeId, Arc<dyn RaftRpcHandler>>>>,
}

impl HomeKvRaftNetworkConnection {
    fn unreachable<E: Error + 'static>(message: impl Into<String>) -> RPCError<RaftNodeId, RaftNode, E> {
        let err = io::Error::new(io::ErrorKind::ConnectionRefused, message.into());
        RPCError::Unreachable(Unreachable::new(&err))
    }

    fn network<E: Error + 'static>(message: impl Into<String>) -> RPCError<RaftNodeId, RaftNode, E> {
        let err = io::Error::new(io::ErrorKind::WouldBlock, message.into());
        RPCError::Network(NetworkError::new(&err))
    }

    async fn before_rpc<E: Error + 'static>(
        &self,
    ) -> Result<
        (RpcAttempt, Arc<dyn RaftRpcHandler>),
        RPCError<RaftNodeId, RaftNode, E>,
    > {
        let peer = self
            .metrics
            .peer(self.target)
            .ok_or_else(|| Self::unreachable(format!("unknown M3 target {}", self.target)))?;
        peer.started();

        let expected = match self.bootstrap.nodes.get(&self.target) {
            Some(expected) => expected,
            None => {
                peer.failed();
                return Err(Self::unreachable(format!(
                    "unknown M3 target {}",
                    self.target
                )));
            }
        };
        if self.target_node.addr != expected.raft_endpoint {
            peer.failed();
            return Err(Self::unreachable(format!(
                "Raft endpoint mismatch for node {}: membership={}, bootstrap={}",
                self.target, self.target_node.addr, expected.raft_endpoint
            )));
        }

        // Capacity is acquired before deterministic delay/drop handling. A slow transport
        // therefore consumes its peer budget instead of accumulating outside the bound.
        let permit = match self.limiter.try_acquire(self.target) {
            Ok(permit) => permit,
            Err(err) => {
                peer.rejected();
                return Err(Self::network(err.to_string()));
            }
        };
        let attempt = peer.admitted(permit);

        match self.links.rule(self.source, self.target) {
            LinkRule::Pass => {}
            LinkRule::Drop => {
                return Err(Self::unreachable(format!(
                    "directed test link {} -> {} is dropped",
                    self.source, self.target
                )))
            }
            LinkRule::Delay(delay) => tokio::time::sleep(delay).await,
        }

        let handler = self
            .handlers
            .read()
            .expect("raft handler registry lock poisoned")
            .get(&self.target)
            .cloned()
            .ok_or_else(|| Self::unreachable(format!("Raft target {} is not registered", self.target)))?;
        Ok((attempt, handler))
    }
}

impl RaftNetworkFactory<HomeKvRaftConfig> for HomeKvRaftNetworkFactory {
    type Network = HomeKvRaftNetworkConnection;

    async fn new_client(&mut self, target: RaftNodeId, node: &RaftNode) -> Self::Network {
        HomeKvRaftNetworkConnection {
            source: self.local_id,
            target,
            target_node: node.clone(),
            bootstrap: self.bootstrap.clone(),
            limiter: self.limiter.clone(),
            metrics: self.metrics.clone(),
            links: self.links.clone(),
            handlers: self.handlers.clone(),
        }
    }
}

impl RaftNetwork<HomeKvRaftConfig> for HomeKvRaftNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<HomeKvRaftConfig>,
        _option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<RaftNodeId>> {
        let (mut attempt, handler) = self.before_rpc().await?;
        match handler.append_entries(req).await {
            Ok(response) => {
                attempt.succeeded();
                Ok(response)
            }
            Err(err) => {
                attempt.failed();
                Err(RPCError::RemoteError(RemoteError::new(self.target, err)))
            }
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> RpcResult<VoteResponse<RaftNodeId>> {
        let (mut attempt, handler) = self.before_rpc().await?;
        match handler.vote(req).await {
            Ok(response) => {
                attempt.succeeded();
                Ok(response)
            }
            Err(err) => {
                attempt.failed();
                Err(RPCError::RemoteError(RemoteError::new(self.target, err)))
            }
        }
    }

    #[allow(deprecated)]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<HomeKvRaftConfig>,
        _option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<RaftNodeId>, RaftError<RaftNodeId, InstallSnapshotError>> {
        let (mut attempt, handler) = self.before_rpc().await?;
        match handler.install_snapshot(req).await {
            Ok(response) => {
                attempt.succeeded();
                Ok(response)
            }
            Err(err) => {
                attempt.failed();
                Err(RPCError::RemoteError(RemoteError::new(self.target, err)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::raft_transport::BootstrapNode;

    fn bootstrap() -> ThreeNodeBootstrap {
        ThreeNodeBootstrap::new(
            "homekv-m3-network-test",
            [
                BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19101".into() },
                BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19102".into() },
                BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19103".into() },
            ],
        )
        .unwrap()
    }

    fn peer(snapshot: &RaftNetworkMetricsSnapshot, node_id: RaftNodeId) -> &PeerRpcMetricsSnapshot {
        snapshot
            .peers
            .iter()
            .find(|peer| peer.target_node_id == node_id)
            .unwrap()
    }

    #[test]
    fn factory_rejects_unknown_local_identity_and_zero_bound() {
        assert!(matches!(
            HomeKvRaftNetworkFactory::new(4, bootstrap(), 1, TestLinkController::default()),
            Err(BootstrapError::UnknownPeer { node_id: 4 })
        ));
        assert!(matches!(
            HomeKvRaftNetworkFactory::new(1, bootstrap(), 0, TestLinkController::default()),
            Err(BootstrapError::InvalidOutstandingRpcLimit)
        ));
    }

    #[test]
    fn factory_is_the_exact_openraft_0925_network_factory() {
        fn assert_factory<T: RaftNetworkFactory<HomeKvRaftConfig>>() {}
        fn assert_network<T: RaftNetwork<HomeKvRaftConfig>>() {}
        assert_factory::<HomeKvRaftNetworkFactory>();
        assert_network::<HomeKvRaftNetworkConnection>();
    }

    #[tokio::test]
    async fn slow_and_unreachable_peer_saturation_is_bounded_and_observable() {
        let links = TestLinkController::default();
        links.set_rule(1, 2, LinkRule::Delay(Duration::from_secs(30)));
        let mut factory =
            HomeKvRaftNetworkFactory::new(1, bootstrap(), 1, links.clone()).unwrap();
        let target = RaftNode {
            addr: "127.0.0.1:19102".into(),
        };
        let delayed = factory.new_client(2, &target).await;
        let rejected = factory.new_client(2, &target).await;

        let first = tokio::spawn(async move {
            let _ = delayed.before_rpc::<RaftError<RaftNodeId>>().await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if peer(&factory.metrics(), 2).current == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delayed RPC must consume the one-peer budget");

        let second = tokio::time::timeout(
            Duration::from_secs(1),
            rejected.before_rpc::<RaftError<RaftNodeId>>(),
        )
        .await
        .expect("saturation must reject without waiting");
        assert!(matches!(second, Err(RPCError::Network(_))));

        let snapshot = factory.metrics();
        let saturated = peer(&snapshot, 2);
        assert_eq!(saturated.capacity, 1);
        assert_eq!(saturated.current, 1);
        assert_eq!(saturated.peak, 1);
        assert_eq!(saturated.attempts, 2);
        assert_eq!(saturated.failures, 1);
        assert_eq!(saturated.backpressure_rejections, 1);
        assert!(!serde_json::to_string(&snapshot)
            .unwrap()
            .contains("openraft"));

        first.abort();
        let _ = first.await;
        assert_eq!(peer(&factory.metrics(), 2).current, 0);
        assert_eq!(peer(&factory.metrics(), 2).failures, 2);

        links.set_rule(1, 2, LinkRule::Drop);
        let dropped = factory.new_client(2, &target).await;
        let result = dropped.before_rpc::<RaftError<RaftNodeId>>().await;
        assert!(matches!(result, Err(RPCError::Unreachable(_))));

        let snapshot = factory.metrics();
        let unreachable = peer(&snapshot, 2);
        assert_eq!(unreachable.current, 0);
        assert_eq!(unreachable.peak, 1);
        assert_eq!(unreachable.attempts, 3);
        assert_eq!(unreachable.failures, 3);
        assert_eq!(unreachable.backpressure_rejections, 1);
    }
}
