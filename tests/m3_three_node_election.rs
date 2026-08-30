use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::{Config, ServerState};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-three-node-election",
        [
            BootstrapNode {
                id: 1,
                raft_endpoint: "127.0.0.1:19201".into(),
            },
            BootstrapNode {
                id: 2,
                raft_endpoint: "127.0.0.1:19202".into(),
            },
            BootstrapNode {
                id: 3,
                raft_endpoint: "127.0.0.1:19203".into(),
            },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19201")),
        (2, RaftNode::new("127.0.0.1:19202")),
        (3, RaftNode::new("127.0.0.1:19203")),
    ])
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("homekv-m3-election-{}-{nonce}", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_three_node_cluster_converges_to_exactly_one_leader() {
    let root = unique_test_dir();
    fs::create_dir_all(&root).unwrap();

    let config = Arc::new(
        Config {
            cluster_name: "homekv-m3-three-node-election".into(),
            heartbeat_interval: 25,
            election_timeout_min: 100,
            election_timeout_max: 200,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let bootstrap = bootstrap();
    let links = TestLinkController::default();

    let mut factories = BTreeMap::new();
    for id in 1..=3 {
        factories.insert(
            id,
            HomeKvRaftNetworkFactory::new(id, bootstrap.clone(), 16, links.clone()).unwrap(),
        );
    }

    let mut nodes: BTreeMap<u64, Raft<HomeKvRaftConfig>> = BTreeMap::new();
    for id in 1..=3 {
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let raft = Raft::new(
            id,
            config.clone(),
            factories.get(&id).unwrap().clone(),
            store,
            HomeKvStateMachine::default(),
        )
        .await
        .unwrap();
        nodes.insert(id, raft);
    }

    for factory in factories.values() {
        for (id, raft) in &nodes {
            factory
                .register_handler(*id, Arc::new(raft.clone()))
                .unwrap();
        }
    }

    nodes.get(&1).unwrap().initialize(membership()).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let elected = loop {
        let snapshots: Vec<_> = nodes
            .iter()
            .map(|(id, raft)| (*id, raft.metrics().borrow().clone()))
            .collect();
        let leaders: Vec<_> = snapshots
            .iter()
            .filter_map(|(id, metrics)| (metrics.state == ServerState::Leader).then_some(*id))
            .collect();
        let known_leaders: BTreeSet<_> = snapshots
            .iter()
            .filter_map(|(_, metrics)| metrics.current_leader)
            .collect();

        if leaders.len() == 1 && known_leaders == BTreeSet::from([leaders[0]]) {
            break leaders[0];
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("three-node cluster did not converge to exactly one leader: {snapshots:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert!(matches!(elected, 1 | 2 | 3));
    assert_eq!(nodes.len(), 3);

    for raft in nodes.values() {
        raft.shutdown().await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
