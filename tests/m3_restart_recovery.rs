mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvStateMachine, RaftCommand, RaftMutation, RaftNode, StateMachineView};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, Raft};
use openraft::storage::RaftLogStorage;
use openraft::{
    CommittedLeaderId, Config, Entry, EntryPayload, LogId, Membership, RaftLogReader,
    SnapshotPolicy, Vote,
};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-restart-recovery",
        [
            BootstrapNode {
                id: 1,
                raft_endpoint: "127.0.0.1:19501".into(),
            },
            BootstrapNode {
                id: 2,
                raft_endpoint: "127.0.0.1:19502".into(),
            },
            BootstrapNode {
                id: 3,
                raft_endpoint: "127.0.0.1:19503".into(),
            },
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

fn config(elections: bool) -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: "homekv-m3-restart-recovery".into(),
            heartbeat_interval: 25,
            election_timeout_min: 100,
            election_timeout_max: 200,
            enable_elect: elections,
            // This slice proves retained-log recovery, not snapshot recovery.
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

fn unique_test_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "homekv-m3-restart-recovery-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

async fn bounded<T>(phase: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap_or_else(|_| panic!("{phase}: operation timed out"))
}

async fn wait_for_state(sm: &HomeKvStateMachine, expected: &StateMachineView, phase: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let actual = bounded(phase, sm.view()).await;
        // A new leader may append a blank entry during rejoin. It may advance
        // applied progress, but must not change the expected data or membership.
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

fn set(key: &[u8], value: &[u8]) -> RaftCommand {
    RaftCommand::Set {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_replica_replays_committed_history_and_catches_up() {
    let root = unique_test_dir();
    let config = config(true);
    let links = TestLinkController::default();
    let mut factories = BTreeMap::new();
    let mut nodes = BTreeMap::new();
    let mut state_machines = BTreeMap::new();
    for id in 1..=3 {
        let factory = HomeKvRaftNetworkFactory::new(id, bootstrap(), 16, links.clone()).unwrap();
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let sm = HomeKvStateMachine::default();
        let raft = bounded(
            "initial startup",
            Raft::new(id, config.clone(), factory.clone(), store, sm.clone()),
        )
        .await
        .unwrap();
        factories.insert(id, factory);
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
    bounded("bootstrap", nodes[&1].initialize(membership()))
        .await
        .unwrap();
    let leader = support::authoritative_leader(&nodes, None).await;
    let restart_id = (1..=3).find(|id| *id != leader).unwrap();

    // Independent expected map catches wrong replay ordering and resurrected deletes.
    for command in [
        set(b"before-restart", b"old"),
        set(b"deleted", b"must-disappear"),
        RaftCommand::Batch {
            mutations: vec![
                RaftMutation::Set {
                    key: b"before-restart".to_vec(),
                    value: b"committed-before".to_vec(),
                },
                RaftMutation::Delete {
                    key: b"deleted".to_vec(),
                },
                RaftMutation::Set {
                    key: b"batch-key".to_vec(),
                    value: b"batch-value".to_vec(),
                },
            ],
        },
    ] {
        bounded("pre-restart write", nodes[&leader].client_write(command))
            .await
            .unwrap();
    }
    let before = state_machines[&leader].view().await;
    assert_eq!(
        before.data,
        BTreeMap::from([
            (b"before-restart".to_vec(), b"committed-before".to_vec()),
            (b"batch-key".to_vec(), b"batch-value".to_vec()),
        ])
    );
    wait_for_state(
        &state_machines[&restart_id],
        &before,
        "replication before shutdown",
    )
    .await;
    for peer in 1..=3 {
        if peer != restart_id {
            links.partition_bidirectional(restart_id, peer);
        }
    }
    let stopped = nodes.remove(&restart_id).unwrap();
    bounded("replica shutdown", stopped.shutdown())
        .await
        .unwrap();
    drop(stopped);
    state_machines.remove(&restart_id);

    bounded(
        "surviving quorum write",
        nodes[&leader].client_write(set(b"while-replica-down", b"committed-with-quorum")),
    )
    .await
    .unwrap();
    let after = state_machines[&leader].view().await;
    let mut expected_after = before.data.clone();
    expected_after.insert(
        b"while-replica-down".to_vec(),
        b"committed-with-quorum".to_vec(),
    );
    assert_eq!(after.data, expected_after);

    let mut store = HomeKvRaftLogStore::open(root.join(format!("node-{restart_id}.raft"))).unwrap();
    assert_eq!(store.read_committed().await.unwrap(), before.last_applied);
    let recovered = HomeKvStateMachine::default();
    let restarted = bounded(
        "restart from disk",
        Raft::new(
            restart_id,
            config.clone(),
            factories[&restart_id].clone(),
            store,
            recovered.clone(),
        ),
    )
    .await
    .unwrap();
    // Both directions remain partitioned: peers cannot conceal a broken local replay.
    wait_for_state(&recovered, &before, "isolated durable replay").await;
    assert_eq!(
        recovered.view().await,
        before,
        "local replay must stop at the durable committed boundary"
    );
    assert_eq!(recovered.get(b"while-replica-down").await, None);
    for factory in factories.values() {
        factory
            .register_handler(restart_id, Arc::new(restarted.clone()))
            .unwrap();
    }
    for peer in 1..=3 {
        if peer != restart_id {
            links.heal_bidirectional(restart_id, peer);
        }
    }
    wait_for_state(&recovered, &after, "catch-up after reconnect").await;
    bounded("restarted replica shutdown", restarted.shutdown())
        .await
        .unwrap();
    for raft in nodes.values() {
        bounded("cluster shutdown", raft.shutdown()).await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_does_not_apply_durable_uncommitted_suffix() {
    let root = unique_test_dir();
    let path = root.join("node-2.raft");
    let factory =
        HomeKvRaftNetworkFactory::new(2, bootstrap(), 16, TestLinkController::default()).unwrap();
    // A deterministic follower storage fixture: no peer handlers or elections can
    // commit the suffix. This is not a claim of a client-acknowledged quorum write.
    let config = config(false);
    let sm = HomeKvStateMachine::default();
    let raft = bounded(
        "fixture startup",
        Raft::new(
            2,
            config.clone(),
            factory.clone(),
            HomeKvRaftLogStore::open(&path).unwrap(),
            sm.clone(),
        ),
    )
    .await
    .unwrap();
    let log_id = |index| LogId::new(CommittedLeaderId::new(1, 1), index);
    let entries = vec![
        Entry {
            log_id: log_id(0),
            payload: EntryPayload::Membership(Membership::new(
                vec![BTreeSet::from([1, 2, 3])],
                membership(),
            )),
        },
        Entry {
            log_id: log_id(1),
            payload: EntryPayload::Normal(set(b"committed", b"safe")),
        },
        Entry {
            log_id: log_id(2),
            payload: EntryPayload::Normal(set(b"speculative", b"must-not-appear")),
        },
    ];
    let reply = bounded(
        "persist prefix and speculative suffix",
        raft.append_entries(AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries,
            leader_commit: Some(log_id(1)),
        }),
    )
    .await
    .unwrap();
    assert_eq!(reply, AppendEntriesResponse::Success);
    bounded(
        "committed prefix apply",
        raft.wait(Some(Duration::from_secs(5)))
            .applied_index(Some(1), "fixture prefix"),
    )
    .await
    .unwrap();
    let before = sm.view().await;
    assert_eq!(
        before.data,
        BTreeMap::from([(b"committed".to_vec(), b"safe".to_vec())])
    );
    bounded("fixture shutdown", raft.shutdown()).await.unwrap();
    drop(raft);
    drop(sm);

    let mut store = HomeKvRaftLogStore::open(&path).unwrap();
    assert_eq!(store.read_committed().await.unwrap(), Some(log_id(1)));
    assert_eq!(
        store.get_log_state().await.unwrap().last_log_id,
        Some(log_id(2))
    );
    let suffix = store.try_get_log_entries(2..=2).await.unwrap();
    assert_eq!(
        suffix.len(),
        1,
        "negative case must retain an actual durable suffix"
    );
    assert!(
        matches!(&suffix[0].payload, EntryPayload::Normal(command) if command == &set(b"speculative", b"must-not-appear"))
    );
    let recovered = HomeKvStateMachine::default();
    let restarted = bounded(
        "restart with speculative suffix",
        Raft::new(2, config, factory, store, recovered.clone()),
    )
    .await
    .unwrap();
    wait_for_state(&recovered, &before, "committed-only restart").await;
    assert_eq!(recovered.get(b"speculative").await, None);
    bounded("recovered fixture shutdown", restarted.shutdown())
        .await
        .unwrap();
    assert_eq!(
        recovered.view().await,
        before,
        "speculative state must remain excluded through shutdown"
    );
    fs::remove_dir_all(root).unwrap();
}
