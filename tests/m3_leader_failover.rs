use std::collections::{BTreeMap, BTreeSet};
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
        "homekv-m3-leader-failover",
        [
            BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19401".into() },
            BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19402".into() },
            BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19403".into() },
        ],
    ).unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19401")),
        (2, RaftNode::new("127.0.0.1:19402")),
        (3, RaftNode::new("127.0.0.1:19403")),
    ])
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("homekv-m3-leader-failover-{}-{nonce}", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_quorum_elects_new_leader_and_preserves_acknowledged_state() {
    let root = unique_test_dir();
    fs::create_dir_all(&root).unwrap();
    let config = Arc::new(Config {
        cluster_name: "homekv-m3-leader-failover".into(),
        heartbeat_interval: 25,
        election_timeout_min: 100,
        election_timeout_max: 200,
        ..Default::default()
    }.validate().unwrap());
    let bootstrap = bootstrap();
    let links = TestLinkController::default();
    let mut factories = BTreeMap::new();
    for id in 1..=3 {
        factories.insert(id, HomeKvRaftNetworkFactory::new(id, bootstrap.clone(), 16, links.clone()).unwrap());
    }
    let mut nodes = BTreeMap::new();
    let mut state_machines = BTreeMap::new();
    for id in 1..=3 {
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let sm = HomeKvStateMachine::default();
        let raft = Raft::new(id, config.clone(), factories.get(&id).unwrap().clone(), store, sm.clone()).await.unwrap();
        state_machines.insert(id, sm);
        nodes.insert(id, raft);
    }
    for factory in factories.values() {
        for (id, raft) in &nodes { factory.register_handler(*id, Arc::new(raft.clone())).unwrap(); }
    }
    nodes.get(&1).unwrap().initialize(membership()).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let old_leader = loop {
        let leaders: Vec<_> = nodes.iter().filter_map(|(id, raft)| (raft.metrics().borrow().state == ServerState::Leader).then_some(*id)).collect();
        if leaders.len() == 1 { break leaders[0]; }
        assert!(tokio::time::Instant::now() < deadline, "initial leader election timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    nodes.get(&old_leader).unwrap().client_write(RaftCommand::Set {
        key: b"before-failover".to_vec(), value: b"committed".to_vec(),
    }).await.unwrap();

    for peer in [1_u64, 2, 3] {
        if peer != old_leader { links.partition_bidirectional(old_leader, peer); }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let new_leader = loop {
        let leaders: Vec<_> = nodes.iter().filter_map(|(id, raft)| {
            (*id != old_leader && raft.metrics().borrow().state == ServerState::Leader).then_some(*id)
        }).collect();
        if leaders.len() == 1 { break leaders[0]; }
        assert!(tokio::time::Instant::now() < deadline, "healthy quorum did not elect replacement leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_ne!(new_leader, old_leader);

    nodes.get(&new_leader).unwrap().ensure_linearizable().await.unwrap();
    assert_eq!(state_machines.get(&new_leader).unwrap().get(b"before-failover").await, Some(b"committed".to_vec()));
    nodes.get(&new_leader).unwrap().client_write(RaftCommand::Set {
        key: b"after-failover".to_vec(), value: b"resumed".to_vec(),
    }).await.unwrap();
    nodes.get(&new_leader).unwrap().ensure_linearizable().await.unwrap();
    assert_eq!(state_machines.get(&new_leader).unwrap().get(b"after-failover").await, Some(b"resumed".to_vec()));

    let old_write = nodes.get(&old_leader).unwrap().client_write(RaftCommand::Set {
        key: b"isolated-old-leader".to_vec(), value: b"forbidden".to_vec(),
    });
    let outcome = tokio::time::timeout(Duration::from_millis(750), old_write).await;
    assert!(!matches!(outcome, Ok(Ok(_))), "isolated old leader must not acknowledge a write");
    assert_eq!(state_machines.get(&old_leader).unwrap().get(b"isolated-old-leader").await, None);

    for raft in nodes.values() { raft.shutdown().await.unwrap(); }
    fs::remove_dir_all(root).unwrap();
}
