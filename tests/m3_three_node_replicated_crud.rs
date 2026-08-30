use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{
    HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftMutation, RaftNode, RaftResponse,
};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::{Config, ServerState};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-replicated-crud",
        [
            BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19301".into() },
            BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19302".into() },
            BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19303".into() },
        ],
    ).unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19301")),
        (2, RaftNode::new("127.0.0.1:19302")),
        (3, RaftNode::new("127.0.0.1:19303")),
    ])
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("homekv-m3-replicated-crud-{}-{nonce}", std::process::id()))
}

struct Cluster {
    root: std::path::PathBuf,
    nodes: BTreeMap<u64, Raft<HomeKvRaftConfig>>,
    state_machines: BTreeMap<u64, HomeKvStateMachine>,
    links: TestLinkController,
}

impl Cluster {
    async fn start() -> Self {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let config = Arc::new(Config {
            cluster_name: "homekv-m3-replicated-crud".into(),
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
            for (id, raft) in &nodes {
                factory.register_handler(*id, Arc::new(raft.clone())).unwrap();
            }
        }
        nodes.get(&1).unwrap().initialize(membership()).await.unwrap();
        Self { root, nodes, state_machines, links }
    }

    async fn leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshots: Vec<_> = self.nodes.iter().map(|(id, raft)| (*id, raft.metrics().borrow().clone())).collect();
            let leaders: Vec<_> = snapshots.iter().filter_map(|(id, metrics)| (metrics.state == ServerState::Leader).then_some(*id)).collect();
            let known: BTreeSet<_> = snapshots.iter().filter_map(|(_, metrics)| metrics.current_leader).collect();
            if leaders.len() == 1 && known == BTreeSet::from([leaders[0]]) { return leaders[0]; }
            if tokio::time::Instant::now() >= deadline { panic!("cluster did not converge to one leader: {snapshots:?}"); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn isolate(&self, node: u64) {
        for peer in [1_u64, 2, 3] {
            if peer != node { self.links.partition_bidirectional(node, peer); }
        }
    }

    async fn stop(self) {
        for raft in self.nodes.values() { raft.shutdown().await.unwrap(); }
        fs::remove_dir_all(self.root).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_client_write_replicates_set_delete_and_batch_to_all_voters() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let raft = cluster.nodes.get(&leader).unwrap();
    let set = raft.client_write(RaftCommand::Set { key: b"a".to_vec(), value: b"one".to_vec() }).await.unwrap();
    assert_eq!(set.data, RaftResponse::Applied { mutations: 1 });
    let batch = raft.client_write(RaftCommand::Batch { mutations: vec![
        RaftMutation::Set { key: b"b".to_vec(), value: b"two".to_vec() },
        RaftMutation::Delete { key: b"a".to_vec() },
    ]}).await.unwrap();
    assert_eq!(batch.data, RaftResponse::Applied { mutations: 2 });
    let delete = raft.client_write(RaftCommand::Delete { key: b"missing".to_vec() }).await.unwrap();
    assert_eq!(delete.data, RaftResponse::Applied { mutations: 1 });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut all_match = true;
        for sm in cluster.state_machines.values() {
            if sm.get(b"a").await.is_some() || sm.get(b"b").await != Some(b"two".to_vec()) { all_match = false; break; }
        }
        if all_match { break; }
        if tokio::time::Instant::now() >= deadline {
            let mut views = Vec::new();
            for (id, sm) in &cluster.state_machines { views.push((*id, sm.view().await)); }
            panic!("replicated state did not converge: {views:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strong_get_barrier_is_leader_authoritative_and_observes_applied_write() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let leader_raft = cluster.nodes.get(&leader).unwrap();
    leader_raft.client_write(RaftCommand::Set { key: b"k".to_vec(), value: b"v".to_vec() }).await.unwrap();
    leader_raft.ensure_linearizable().await.unwrap();
    assert_eq!(cluster.state_machines.get(&leader).unwrap().get(b"k").await, Some(b"v".to_vec()));

    let follower = [1_u64, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    assert!(cluster.nodes.get(&follower).unwrap().ensure_linearizable().await.is_err());
    assert!(cluster.nodes.get(&follower).unwrap().client_write(RaftCommand::Set {
        key: b"forbidden".to_vec(), value: b"local".to_vec(),
    }).await.is_err());
    assert_eq!(cluster.state_machines.get(&follower).unwrap().get(b"forbidden").await, None);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_old_leader_cannot_acknowledge_or_apply_a_minority_write() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    cluster.isolate(leader);

    let write = cluster.nodes.get(&leader).unwrap().client_write(RaftCommand::Set {
        key: b"minority".to_vec(),
        value: b"must-not-commit".to_vec(),
    });
    let outcome = tokio::time::timeout(Duration::from_millis(750), write).await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "an isolated minority leader must never acknowledge a write"
    );
    assert_eq!(
        cluster.state_machines.get(&leader).unwrap().get(b"minority").await,
        None,
        "an uncommitted minority write must not be applied locally"
    );

    cluster.stop().await;
}
