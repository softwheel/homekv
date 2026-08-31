use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::{Config, ServerState};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-restart-recovery",
        [
            BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19501".into() },
            BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19502".into() },
            BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19503".into() },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19501")),
        (2, RaftNode::new("127.0.0.1:19502")),
        (3, RaftNode::new("127.0.0.1:19503")),
    ])
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("homekv-m3-restart-recovery-{}-{nonce}", std::process::id()))
}

async fn wait_for_leader(nodes: &BTreeMap<u64, Raft<HomeKvRaftConfig>>) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let leaders: Vec<_> = nodes
            .iter()
            .filter_map(|(id, raft)| (raft.metrics().borrow().state == ServerState::Leader).then_some(*id))
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(tokio::time::Instant::now() < deadline, "leader election timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_value(sm: &HomeKvStateMachine, key: &[u8], expected: &[u8]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if sm.get(key).await.as_deref() == Some(expected) {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "replica did not recover expected value");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_replica_replays_committed_history_and_catches_up() {
    let root = unique_test_dir();
    fs::create_dir_all(&root).unwrap();
    let config = Arc::new(
        Config {
            cluster_name: "homekv-m3-restart-recovery".into(),
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

    let mut nodes = BTreeMap::new();
    let mut state_machines = BTreeMap::new();
    for id in 1..=3 {
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let sm = HomeKvStateMachine::default();
        let raft = Raft::new(id, config.clone(), factories.get(&id).unwrap().clone(), store, sm.clone())
            .await
            .unwrap();
        state_machines.insert(id, sm);
        nodes.insert(id, raft);
    }
    for factory in factories.values() {
        for (id, raft) in &nodes {
            factory.register_handler(*id, Arc::new(raft.clone())).unwrap();
        }
    }
    nodes.get(&1).unwrap().initialize(membership()).await.unwrap();

    let leader = wait_for_leader(&nodes).await;
    let restart_id = [1_u64, 2, 3].into_iter().find(|id| *id != leader).unwrap();

    nodes
        .get(&leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: b"before-restart".to_vec(),
            value: b"committed-before".to_vec(),
        })
        .await
        .unwrap();
    wait_for_value(state_machines.get(&restart_id).unwrap(), b"before-restart", b"committed-before").await;

    nodes.get(&restart_id).unwrap().shutdown().await.unwrap();

    nodes
        .get(&leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: b"while-replica-down".to_vec(),
            value: b"committed-with-quorum".to_vec(),
        })
        .await
        .unwrap();

    let restarted_sm = HomeKvStateMachine::default();
    let restarted_store = HomeKvRaftLogStore::open(root.join(format!("node-{restart_id}.raft"))).unwrap();
    let restarted = Raft::new(
        restart_id,
        config.clone(),
        factories.get(&restart_id).unwrap().clone(),
        restarted_store,
        restarted_sm.clone(),
    )
    .await
    .unwrap();

    for factory in factories.values() {
        factory.register_handler(restart_id, Arc::new(restarted.clone())).unwrap();
    }

    wait_for_value(&restarted_sm, b"before-restart", b"committed-before").await;
    wait_for_value(&restarted_sm, b"while-replica-down", b"committed-with-quorum").await;

    restarted.shutdown().await.unwrap();
    for (id, raft) in &nodes {
        if *id != restart_id {
            raft.shutdown().await.unwrap();
        }
    }
    fs::remove_dir_all(root).unwrap();
}
