use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::data_plane::{Request, RequestBody, Status};
use homekv::data_plane_runtime::RequestHandler;
use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftNode};
use homekv::raft_data_plane::ReplicatedShardRequestHandler;
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, LinkRule, TestLinkController, ThreeNodeBootstrap};
use homekv::storage::shard_for_key;
use openraft::raft::Raft;
use openraft::{Config, ServerState};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-replicated-serving",
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
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m3-replicated-serving-{}-{nonce}",
        std::process::id()
    ))
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
        let config = Arc::new(
            Config {
                cluster_name: "homekv-m3-replicated-serving".into(),
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
            let raft = Raft::new(
                id,
                config.clone(),
                factories.get(&id).unwrap().clone(),
                store,
                sm.clone(),
            )
            .await
            .unwrap();
            state_machines.insert(id, sm);
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
        Self {
            root,
            nodes,
            state_machines,
            links,
        }
    }

    async fn leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshots: Vec<_> = self
                .nodes
                .iter()
                .map(|(id, raft)| (*id, raft.metrics().borrow().clone()))
                .collect();
            let leaders: Vec<_> = snapshots
                .iter()
                .filter_map(|(id, metrics)| (metrics.state == ServerState::Leader).then_some(*id))
                .collect();
            let known: BTreeSet<_> = snapshots
                .iter()
                .filter_map(|(_, metrics)| metrics.current_leader)
                .collect();
            if leaders.len() == 1 && known == BTreeSet::from([leaders[0]]) {
                return leaders[0];
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("cluster did not converge to one leader: {snapshots:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(self) {
        for raft in self.nodes.values() {
            raft.shutdown().await.unwrap();
        }
        fs::remove_dir_all(self.root).unwrap();
    }
}

fn request(id: u64, shard_id: u16, body: RequestBody) -> Request {
    Request {
        request_id: id,
        shard_id,
        body,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_contract_routes_strong_operations_through_raft_authority() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let follower = [1_u64, 2, 3]
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();
    let key = b"served-key".to_vec();
    let shard = shard_for_key(&key).as_u16();

    let leader_handler = ReplicatedShardRequestHandler::new(
        cluster.nodes.get(&leader).unwrap().clone(),
        cluster.state_machines.get(&leader).unwrap().clone(),
        shard,
        8,
    );
    let follower_handler = ReplicatedShardRequestHandler::new(
        cluster.nodes.get(&follower).unwrap().clone(),
        cluster.state_machines.get(&follower).unwrap().clone(),
        shard,
        8,
    );

    let set = leader_handler
        .handle(request(
            1,
            shard,
            RequestBody::Set {
                key: key.clone(),
                value: b"value".to_vec(),
            },
        ))
        .await;
    assert_eq!(set.status, Status::Ok);

    let get = leader_handler
        .handle(request(2, shard, RequestBody::Get { key: key.clone() }))
        .await;
    assert_eq!(get.status, Status::Ok);
    assert_eq!(get.body, b"value");

    let follower_get = follower_handler
        .handle(request(3, shard, RequestBody::Get { key: key.clone() }))
        .await;
    assert_eq!(follower_get.status, Status::StaleRouteOrNotOwner);

    let follower_write = follower_handler
        .handle(request(
            4,
            shard,
            RequestBody::Set {
                key: key.clone(),
                value: b"forbidden".to_vec(),
            },
        ))
        .await;
    assert_eq!(follower_write.status, Status::StaleRouteOrNotOwner);

    let wrong_shard = (shard + 1) % 1024;
    let wrong = leader_handler
        .handle(request(
            5,
            wrong_shard,
            RequestBody::Set {
                key: key.clone(),
                value: b"wrong".to_vec(),
            },
        ))
        .await;
    assert_eq!(wrong.status, Status::WrongShard);

    let final_get = leader_handler
        .handle(request(6, shard, RequestBody::Get { key }))
        .await;
    assert_eq!(final_get.status, Status::Ok);
    assert_eq!(final_get.body, b"value");

    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_write_survives_transport_future_cancellation() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let key = b"cancelled-client-key".to_vec();
    let shard = shard_for_key(&key).as_u16();
    let handler = ReplicatedShardRequestHandler::new(
        cluster.nodes.get(&leader).unwrap().clone(),
        cluster.state_machines.get(&leader).unwrap().clone(),
        shard,
        8,
    );

    // Hold replication long enough to cancel the caller after admission but before quorum
    // completion. The handler's detached consensus task must retain both the command and its
    // bounded admission permit after the transport-facing future disappears.
    for peer in [1_u64, 2, 3].into_iter().filter(|id| *id != leader) {
        cluster
            .links
            .set_rule(leader, peer, LinkRule::Delay(Duration::from_millis(75)));
    }

    let write_handler = handler.clone();
    let write_key = key.clone();
    let transport = tokio::spawn(async move {
        write_handler
            .handle(request(
                10,
                shard,
                RequestBody::Set {
                    key: write_key,
                    value: b"committed-after-cancel".to_vec(),
                },
            ))
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    transport.abort();
    assert!(transport.await.unwrap_err().is_cancelled());

    for peer in [1_u64, 2, 3].into_iter().filter(|id| *id != leader) {
        cluster.links.heal(leader, peer);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cluster.state_machines.get(&leader).unwrap().get(&key).await
            == Some(b"committed-after-cancel".to_vec())
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "admitted write was revoked when the transport future was cancelled"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let read = handler
        .handle(request(11, shard, RequestBody::Get { key }))
        .await;
    assert_eq!(read.status, Status::Ok);
    assert_eq!(read.body, b"committed-after-cancel");

    cluster.stop().await;
}
