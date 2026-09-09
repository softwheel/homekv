mod support;

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvStateMachine, RaftCommand, RaftNode, StateMachineView};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Config, SnapshotPolicy};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-snapshot-catchup",
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

fn config() -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: "homekv-m3-snapshot-catchup".into(),
            heartbeat_interval: 25,
            election_timeout_min: 100,
            election_timeout_max: 200,
            snapshot_policy: SnapshotPolicy::Never,
            snapshot_max_chunk_size: 64,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "homekv-m3-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

async fn bounded<T>(phase: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(8), future)
        .await
        .unwrap_or_else(|_| panic!("{phase}: operation timed out"))
}

async fn wait_for_state(sm: &HomeKvStateMachine, expected: &StateMachineView, phase: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let actual = sm.view().await;
        if actual.data == expected.data
            && actual.membership == expected.membership
            && actual.last_applied.map(|id| id.index) >= expected.last_applied.map(|id| id.index)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{phase}: state mismatch; expected={expected:?}; actual={actual:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_requires_snapshot_then_replays_subsequent_log() {
    let root = unique_test_dir("forced-snapshot-catchup");
    let config = config();
    let links = TestLinkController::default();
    let mut factories = BTreeMap::new();
    let mut nodes = BTreeMap::new();
    let mut stores = BTreeMap::new();
    let mut state_machines = BTreeMap::new();

    for id in 1..=3 {
        let factory =
            HomeKvRaftNetworkFactory::new(id, bootstrap(), 16, links.clone()).unwrap();
        let store =
            HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let sm =
            HomeKvStateMachine::open(root.join(format!("node-{id}.snapshot"))).unwrap();
        let raft = bounded(
            "node startup",
            Raft::new(
                id,
                config.clone(),
                factory.clone(),
                store.clone(),
                sm.clone(),
            ),
        )
        .await
        .unwrap();
        factories.insert(id, factory);
        stores.insert(id, store);
        state_machines.insert(id, sm);
        nodes.insert(id, raft);
    }
    for factory in factories.values() {
        for (id, raft) in &nodes {
            factory.register_handler(*id, Arc::new(raft.clone())).unwrap();
        }
    }

    bounded("bootstrap", nodes[&1].initialize(membership()))
        .await
        .unwrap();
    let leader = support::authoritative_leader(&nodes, None).await;
    bounded(
        "seed write",
        nodes[&leader].client_write(RaftCommand::Set {
            key: b"seed".to_vec(),
            value: b"present".to_vec(),
        }),
    )
    .await
    .unwrap();
    let seeded = state_machines[&leader].view().await;
    for sm in state_machines.values() {
        wait_for_state(sm, &seeded, "seed replication").await;
    }

    let lagging = (1..=3).find(|id| *id != leader).unwrap();
    for peer in 1..=3 {
        if peer != lagging {
            links.partition_bidirectional(lagging, peer);
        }
    }

    for index in 0..8u8 {
        bounded(
            "write while follower is isolated",
            nodes[&leader].client_write(RaftCommand::Set {
                key: vec![b'k', index],
                value: vec![b'v', index],
            }),
        )
        .await
        .unwrap();
    }
    let at_snapshot = state_machines[&leader].view().await;
    let snapshot_index = at_snapshot.last_applied.unwrap().index;
    bounded("trigger snapshot", nodes[&leader].trigger().snapshot())
        .await
        .unwrap();

    let snapshot_meta = {
        let mut leader_sm = state_machines[&leader].clone();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            if let Some(snapshot) = leader_sm.get_current_snapshot().await.unwrap() {
                if snapshot.meta.last_log_id.map(|id| id.index) == Some(snapshot_index) {
                    break snapshot.meta;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "leader did not retain the requested snapshot"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    bounded(
        "purge logs covered by snapshot",
        nodes[&leader].trigger().purge_log(snapshot_index),
    )
    .await
    .unwrap();
    let mut leader_store = stores[&leader].clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let state = leader_store.get_log_state().await.unwrap();
        if state.last_purged_log_id.map(|id| id.index) == Some(snapshot_index) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader did not purge through snapshot index {snapshot_index}: {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    bounded(
        "post-snapshot write",
        nodes[&leader].client_write(RaftCommand::Set {
            key: b"after-snapshot".to_vec(),
            value: b"replayed".to_vec(),
        }),
    )
    .await
    .unwrap();
    let expected = state_machines[&leader].view().await;
    assert!(
        expected.last_applied.unwrap().index > snapshot_index,
        "the final state must include a log entry after the snapshot"
    );

    for peer in 1..=3 {
        if peer != lagging {
            links.heal_bidirectional(lagging, peer);
        }
    }
    let mut lagging_sm = state_machines[&lagging].clone();
    let install_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let installed = loop {
        if let Some(snapshot) = lagging_sm.get_current_snapshot().await.unwrap() {
            if snapshot.meta == snapshot_meta {
                break snapshot;
            }
        }
        assert!(
            tokio::time::Instant::now() < install_deadline,
            "lagging follower did not install the exact leader snapshot"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(installed.meta, snapshot_meta);
    wait_for_state(
        &state_machines[&lagging],
        &expected,
        "subsequent log replay after snapshot install",
    )
    .await;
    assert!(
        fs::metadata(root.join(format!("node-{lagging}.snapshot")))
            .unwrap()
            .len()
            > 0
    );

    for raft in nodes.values() {
        bounded("cluster shutdown", raft.shutdown()).await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn abandoned_or_corrupt_snapshot_receive_preserves_durable_state() {
    let root = unique_test_dir("snapshot-receive-safety");
    let target_path = root.join("target.snapshot");
    let mut target = HomeKvStateMachine::open(&target_path).unwrap();
    target
        .apply(vec![openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
            payload: openraft::EntryPayload::Normal(RaftCommand::Set {
                key: b"authoritative".to_vec(),
                value: b"old-state".to_vec(),
            }),
        }])
        .await
        .unwrap();
    target
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let before = target.view().await;

    let mut partial = target.begin_receiving_snapshot().await.unwrap();
    partial.get_mut().extend_from_slice(b"partial-snapshot");
    drop(partial);
    assert_eq!(target.view().await, before);
    drop(target);
    assert_eq!(HomeKvStateMachine::open(&target_path).unwrap().view().await, before);

    let mut source = HomeKvStateMachine::default();
    source
        .apply(vec![openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(2, 2), 2),
            payload: openraft::EntryPayload::Normal(RaftCommand::Set {
                key: b"replacement".to_vec(),
                value: b"must-not-install".to_vec(),
            }),
        }])
        .await
        .unwrap();
    let mut snapshot = source
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let meta = snapshot.meta;
    let mut bytes = Vec::new();
    snapshot.snapshot.read_to_end(&mut bytes).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;

    let mut reopened = HomeKvStateMachine::open(&target_path).unwrap();
    assert!(
        reopened
            .install_snapshot(&meta, Box::new(std::io::Cursor::new(bytes)))
            .await
            .is_err()
    );
    assert_eq!(reopened.view().await, before);
    drop(reopened);
    assert_eq!(HomeKvStateMachine::open(&target_path).unwrap().view().await, before);

    fs::remove_dir_all(root).unwrap();
}
