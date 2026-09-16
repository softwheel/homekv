mod support;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::placement::{CatalogResponse, EligibleNode};
use homekv::placement_raft::{CatalogGroupError, PlacementCatalogGroup};
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Config, SnapshotPolicy};

const CLUSTER_ID: [u8; 16] = *b"homekv-m4-test01";

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m4-placement-catalog",
        [
            BootstrapNode {
                id: 1,
                raft_endpoint: "127.0.0.1:19901".into(),
            },
            BootstrapNode {
                id: 2,
                raft_endpoint: "127.0.0.1:19902".into(),
            },
            BootstrapNode {
                id: 3,
                raft_endpoint: "127.0.0.1:19903".into(),
            },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19901")),
        (2, RaftNode::new("127.0.0.1:19902")),
        (3, RaftNode::new("127.0.0.1:19903")),
    ])
}

fn eligible_nodes() -> Vec<EligibleNode> {
    (1..=6)
        .map(|node_id| EligibleNode {
            node_id,
            raft_endpoint: format!("127.0.0.1:{}", 20_000 + node_id),
            failure_domain: format!("zone-{}", (node_id - 1) % 3),
        })
        .collect()
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m4-placement-catalog-{}-{nonce}",
        std::process::id()
    ))
}

fn config() -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: "homekv-m4-placement-catalog".into(),
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
            let state_machine = HomeKvStateMachine::open(
                root.join(format!("catalog-node-{id}.snapshot")),
            )
            .unwrap();
            let raft = Raft::new(
                id,
                config(),
                factory.clone(),
                store,
                state_machine.clone(),
            )
            .await
            .unwrap();
            factories.insert(id, factory);
            nodes.insert(id, raft);
            state_machines.insert(id, state_machine);
        }
        for factory in factories.values() {
            for (id, raft) in &nodes {
                factory
                    .register_handler(*id, Arc::new(raft.clone()))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_group_is_quorum_authoritative_and_recovers_durable_bootstrap() {
    let root = unique_test_dir();
    let cluster = CatalogCluster::start(root.clone(), true).await;
    let leader = support::authoritative_leader(&cluster.nodes, None).await;
    let leader_group = cluster.group(leader);

    assert_eq!(
        leader_group
            .bootstrap(CLUSTER_ID, eligible_nodes())
            .await
            .unwrap(),
        CatalogResponse::Initialized { placement_epoch: 1 }
    );
    assert_eq!(
        leader_group
            .bootstrap(CLUSTER_ID, eligible_nodes())
            .await
            .unwrap(),
        CatalogResponse::AlreadyInitialized { placement_epoch: 1 }
    );

    let mut conflicting_nodes = eligible_nodes();
    conflicting_nodes[0].failure_domain = "conflicting-zone".into();
    assert_eq!(
        leader_group
            .bootstrap(CLUSTER_ID, conflicting_nodes)
            .await
            .unwrap_err(),
        CatalogGroupError::ConflictingBootstrap
    );

    let committed = leader_group.read().await.unwrap();
    assert_eq!(committed.cluster_id, CLUSTER_ID);
    assert_eq!(committed.placement_epoch, 1);
    assert_eq!(committed.placements.len(), 1_024);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut converged = true;
        for id in 1..=3 {
            match cluster.group(id).committed_state().await {
                Ok(state) if state == committed => {}
                _ => converged = false,
            }
        }
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "catalog replicas did not converge on the committed bootstrap"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cluster.persist_snapshots().await;
    cluster.stop(false).await;

    let recovered = CatalogCluster::start(root, false).await;
    let recovered_leader = support::authoritative_leader(&recovered.nodes, None).await;
    assert_eq!(recovered.group(recovered_leader).read().await.unwrap(), committed);

    recovered.isolate(recovered_leader);
    let replacement = support::authoritative_leader(&recovered.nodes, Some(recovered_leader)).await;
    assert_eq!(recovered.group(replacement).read().await.unwrap(), committed);

    recovered.isolate(replacement);
    let strong_read = tokio::time::timeout(
        Duration::from_millis(750),
        recovered.group(replacement).read(),
    )
    .await;
    assert!(
        !matches!(strong_read, Ok(Ok(_))),
        "a catalog replica without quorum must not serve committed state as a strong read"
    );
    let write = tokio::time::timeout(
        Duration::from_millis(750),
        recovered
            .group(replacement)
            .bootstrap(CLUSTER_ID, eligible_nodes()),
    )
    .await;
    assert!(
        !matches!(write, Ok(Ok(_))),
        "catalog quorum loss must not acknowledge a placement command"
    );

    recovered.stop(true).await;
}
