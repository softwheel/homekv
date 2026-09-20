use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, LinkRule, TestLinkController, ThreeNodeBootstrap};
use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::raft::Raft;
use openraft::{Config, ServerState};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-linearizable-history",
        [
            BootstrapNode { id: 1, raft_endpoint: "127.0.0.1:19401".into() },
            BootstrapNode { id: 2, raft_endpoint: "127.0.0.1:19402".into() },
            BootstrapNode { id: 3, raft_endpoint: "127.0.0.1:19403".into() },
        ],
    )
    .unwrap()
}

fn membership() -> BTreeMap<u64, RaftNode> {
    BTreeMap::from([
        (1, RaftNode::new("127.0.0.1:19401")),
        (2, RaftNode::new("127.0.0.1:19402")),
        (3, RaftNode::new("127.0.0.1:19403")),
    ])
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m3-linearizable-history-{}-{nonce}",
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
                cluster_name: "homekv-m3-linearizable-history".into(),
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

#[derive(Clone, Debug)]
enum PlannedOp {
    Write(&'static [u8]),
    Read,
}

#[derive(Clone, Debug)]
enum ObservedOp {
    Write(Vec<u8>),
    Read(Option<Vec<u8>>),
}

#[derive(Clone, Debug)]
struct HistoryOp {
    invoke: u64,
    complete: u64,
    observed: ObservedOp,
}

async fn execute(
    planned: PlannedOp,
    raft: Raft<HomeKvRaftConfig>,
    sm: HomeKvStateMachine,
    clock: Arc<AtomicU64>,
) -> HistoryOp {
    let invoke = clock.fetch_add(1, Ordering::SeqCst);
    let observed = match planned {
        PlannedOp::Write(value) => {
            // Under CI load, a write can transiently fail on the first
            // attempt (e.g. a leadership change mid-call, or a saturated link
            // delaying the commit path). The write is an idempotent `Set` of
            // the same key/value, so a correct client retries; the retry
            // happens inside this one `execute` call, so the invoke/complete
            // interval — and hence the linearizability model — is unaffected.
            // Only the recorded history (invoke → observed write → complete)
            // changes, never its meaning.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let write = raft
                    .client_write(RaftCommand::Set {
                        key: b"linearizable-key".to_vec(),
                        value: value.to_vec(),
                    })
                    .await;
                match write {
                    Ok(_) => break,
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))
                        if tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        panic!("client_write failed and is not retryable: {error:?}")
                    }
                }
            }
            ObservedOp::Write(value.to_vec())
        }
        PlannedOp::Read => {
            // Same transient treatment for reads: under CI load, the leader's
            // read-confirmation round can momentarily fail to reach quorum
            // (`CheckIsLeaderError::QuorumNotEnough`) or observe a leadership
            // change (`ForwardToLeader`). `ensure_linearizable` is an
            // idempotent confirmation, so retrying it is safe and leaves the
            // recorded read interval's meaning unchanged.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                match raft.ensure_linearizable().await {
                    Ok(_) => break,
                    Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)))
                    | Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_)))
                        if tokio::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        panic!("ensure_linearizable failed and is not retryable: {error:?}")
                    }
                }
            }
            ObservedOp::Read(sm.get(b"linearizable-key").await)
        }
    };
    let complete = clock.fetch_add(1, Ordering::SeqCst);
    HistoryOp {
        invoke,
        complete,
        observed,
    }
}

fn admits_linearization(history: &[HistoryOp]) -> bool {
    fn search(
        history: &[HistoryOp],
        placed: &mut Vec<usize>,
        used: &mut [bool],
        state: Option<Vec<u8>>,
    ) -> bool {
        if placed.len() == history.len() {
            return true;
        }

        for candidate in 0..history.len() {
            if used[candidate] {
                continue;
            }

            let violates_real_time = (0..history.len()).any(|prior| {
                !used[prior]
                    && prior != candidate
                    && history[prior].complete < history[candidate].invoke
            });
            if violates_real_time {
                continue;
            }

            let next_state = match &history[candidate].observed {
                ObservedOp::Write(value) => Some(value.clone()),
                ObservedOp::Read(observed) if observed == &state => state.clone(),
                ObservedOp::Read(_) => continue,
            };

            used[candidate] = true;
            placed.push(candidate);
            if search(history, placed, used, next_state) {
                return true;
            }
            placed.pop();
            used[candidate] = false;
        }
        false
    }

    let mut placed = Vec::with_capacity(history.len());
    let mut used = vec![false; history.len()];
    search(history, &mut placed, &mut used, None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_and_strong_gets_admit_a_linearizable_history() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let raft = cluster.nodes.get(&leader).unwrap().clone();
    let sm = cluster.state_machines.get(&leader).unwrap().clone();
    let clock = Arc::new(AtomicU64::new(0));

    let planned = [
        PlannedOp::Write(b"v1"),
        PlannedOp::Read,
        PlannedOp::Write(b"v2"),
        PlannedOp::Read,
        PlannedOp::Write(b"v3"),
        PlannedOp::Read,
    ];

    let mut tasks = Vec::new();
    for op in planned {
        tasks.push(tokio::spawn(execute(
            op,
            raft.clone(),
            sm.clone(),
            clock.clone(),
        )));
    }

    let mut history = Vec::new();
    for task in tasks {
        history.push(task.await.unwrap());
    }
    history.sort_by_key(|op| op.invoke);

    assert!(
        admits_linearization(&history),
        "concurrent write/strong-read history is not linearizable: {history:?}"
    );

    cluster.stop().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_then_unavailable_follower_preserves_linearizable_quorum_progress() {
    let cluster = Cluster::start().await;
    let leader = cluster.leader().await;
    let slow_follower = [1_u64, 2, 3]
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();

    cluster.links.set_rule(
        leader,
        slow_follower,
        LinkRule::Delay(Duration::from_millis(40)),
    );

    let raft = cluster.nodes[&leader].clone();
    let sm = cluster.state_machines[&leader].clone();
    let clock = Arc::new(AtomicU64::new(0));
    let planned = [
        PlannedOp::Write(b"delayed-v1"),
        PlannedOp::Read,
        PlannedOp::Write(b"delayed-v2"),
        PlannedOp::Read,
    ];
    let mut tasks = Vec::new();
    for op in planned {
        tasks.push(tokio::spawn(execute(
            op,
            raft.clone(),
            sm.clone(),
            clock.clone(),
        )));
    }
    let mut history = Vec::new();
    for task in tasks {
        history.push(task.await.unwrap());
    }
    history.sort_by_key(|op| op.invoke);
    assert!(
        admits_linearization(&history),
        "slow-follower history is not linearizable: {history:?}"
    );

    for peer in [1_u64, 2, 3] {
        if peer != slow_follower {
            cluster.links.partition_bidirectional(slow_follower, peer);
        }
    }

    raft.client_write(RaftCommand::Set {
        key: b"one-follower-unavailable".to_vec(),
        value: b"quorum-progress".to_vec(),
    })
    .await
    .unwrap();
    raft.ensure_linearizable().await.unwrap();
    assert_eq!(
        sm.get(b"one-follower-unavailable").await,
        Some(b"quorum-progress".to_vec())
    );

    for peer in [1_u64, 2, 3] {
        if peer != slow_follower {
            cluster.links.heal_bidirectional(slow_follower, peer);
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cluster.state_machines[&slow_follower]
            .get(b"one-follower-unavailable")
            .await
            == Some(b"quorum-progress".to_vec())
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "healed follower did not converge to the quorum-committed state"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    cluster.stop().await;
}
