mod support;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_observability::{
    HomeKvReplicaObserver, ReplicaHealth, ReplicaRole,
};
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::Config;

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m3-observability-{}-{nonce}",
        std::process::id()
    ))
}

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-observability",
        [
            BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19601".into() },
            BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19602".into() },
            BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19603".into() },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19601")),
        (2, RaftNode::new("127.0.0.1:19602")),
        (3, RaftNode::new("127.0.0.1:19603")),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stable_replica_status_tracks_committed_three_node_state() {
    let root = unique_test_dir();
    fs::create_dir_all(&root).unwrap();
    let config = Arc::new(
        Config {
            cluster_name: "homekv-m3-observability".into(),
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
    let mut observers = BTreeMap::new();
    for id in 1..=3 {
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let state_machine = HomeKvStateMachine::default();
        let raft = Raft::new(
            id,
            config.clone(),
            factories[&id].clone(),
            store.clone(),
            state_machine.clone(),
        )
        .await
        .unwrap();
        observers.insert(
            id,
            HomeKvReplicaObserver::new(raft.clone(), store, state_machine.clone()),
        );
        state_machines.insert(id, state_machine);
        nodes.insert(id, raft);
    }
    for factory in factories.values() {
        for (id, raft) in &nodes {
            factory.register_handler(*id, Arc::new(raft.clone())).unwrap();
        }
    }
    nodes[&1].initialize(membership()).await.unwrap();

    let leader = support::authoritative_leader(&nodes, None).await;
    nodes[&leader]
        .client_write(RaftCommand::Set {
            key: b"observed".to_vec(),
            value: b"committed".to_vec(),
        })
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut ready = true;
        for state_machine in state_machines.values() {
            ready &= state_machine.get(b"observed").await == Some(b"committed".to_vec());
        }
        if ready {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replicas did not apply the observed write"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    for (id, observer) in &observers {
        let status = observer.snapshot().await.unwrap();
        assert_eq!(status.node_id, *id);
        assert_eq!(status.health, ReplicaHealth::Running);
        assert_eq!(status.leader_id, Some(leader));
        assert!(status.current_term > 0);
        assert_eq!(status.vote.term, status.current_term);
        assert_eq!(status.vote.candidate_id, leader);
        assert!(status.vote.committed);
        assert_eq!(status.membership.voters, vec![1, 2, 3]);
        assert_eq!(status.membership.members.len(), 3);
        assert!(status.membership.members.iter().all(|member| member.voter));
        assert!(status.membership.log.is_some());
        assert!(status.committed.is_some());
        assert!(status.applied.is_some());
        assert!(status.last_log_index >= status.committed.map(|position| position.index));
        assert!(
            status.committed.unwrap().index >= status.applied.unwrap().index,
            "durable committed progress must not trail applied progress"
        );
        assert_eq!(
            status.role,
            if *id == leader {
                ReplicaRole::Leader
            } else {
                ReplicaRole::Follower
            }
        );

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"node_id\""));
        assert!(!json.contains("OpenRaft"));
        assert!(!json.contains("ServerState"));
    }

    for raft in nodes.values() {
        raft.shutdown().await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
