use std::collections::BTreeMap;
use std::error::Error;
use std::io;
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

use crate::raft::{HomeKvRaftConfig, RaftNode, RaftNodeId};
use crate::raft_transport::{
    BootstrapError, LinkRule, PerPeerRpcLimiter, TestLinkController, ThreeNodeBootstrap,
};

type RpcResult<T, E = RaftError<RaftNodeId>> = Result<T, RPCError<RaftNodeId, RaftNode, E>>;

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
        Ok(Self {
            local_id,
            bootstrap: Arc::new(bootstrap),
            limiter,
            links,
            handlers: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    pub fn local_id(&self) -> RaftNodeId {
        self.local_id
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
        (
            tokio::sync::OwnedSemaphorePermit,
            Arc<dyn RaftRpcHandler>,
        ),
        RPCError<RaftNodeId, RaftNode, E>,
    > {
        let expected = self
            .bootstrap
            .nodes
            .get(&self.target)
            .ok_or_else(|| Self::unreachable(format!("unknown M3 target {}", self.target)))?;
        if self.target_node.addr != expected.raft_endpoint {
            return Err(Self::unreachable(format!(
                "Raft endpoint mismatch for node {}: membership={}, bootstrap={}",
                self.target, self.target_node.addr, expected.raft_endpoint
            )));
        }

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

        let permit = self
            .limiter
            .try_acquire(self.target)
            .map_err(|e| Self::network(e.to_string()))?;
        let handler = self
            .handlers
            .read()
            .expect("raft handler registry lock poisoned")
            .get(&self.target)
            .cloned()
            .ok_or_else(|| Self::unreachable(format!("Raft target {} is not registered", self.target)))?;
        Ok((permit, handler))
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
        let (_permit, handler) = self.before_rpc().await?;
        handler
            .append_entries(req)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> RpcResult<VoteResponse<RaftNodeId>> {
        let (_permit, handler) = self.before_rpc().await?;
        handler
            .vote(req)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }

    #[allow(deprecated)]
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<HomeKvRaftConfig>,
        _option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<RaftNodeId>, RaftError<RaftNodeId, InstallSnapshotError>> {
        let (_permit, handler) = self.before_rpc().await?;
        handler
            .install_snapshot(req)
            .await
            .map_err(|err| RPCError::RemoteError(RemoteError::new(self.target, err)))
    }
}

#[cfg(test)]
mod tests {
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
}
