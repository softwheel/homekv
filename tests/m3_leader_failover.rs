mod support;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::raft::{HomeKvStateMachine, RaftCommand, RaftNode};
use homekv::raft_network::HomeKvRaftNetworkFactory;
use homekv::raft_observability::HomeKvReplicaObserver;
use homekv::raft_storage::HomeKvRaftLogStore;
use homekv::raft_transport::{BootstrapNode, TestLinkController, ThreeNodeBootstrap};
use openraft::raft::Raft;
use openraft::storage::RaftLogStorage;
use openraft::{Config, EntryPayload, RaftLogReader};

fn bootstrap() -> ThreeNodeBootstrap {
    ThreeNodeBootstrap::new(
        "homekv-m3-leader-failover",
        [
            BootstrapNode {
                id: 1,
                raft_endpoint: "127.0.0.1:19401".into(),
            },
            BootstrapNode {
                id: 2,
                raft_endpoint: "127.0.0.1:19402".into(),
            },
            BootstrapNode {
                id: 3,
                raft_endpoint: "127.0.0.1:19403".into(),
            },
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
        "homekv-m3-leader-failover-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthy_quorum_elects_new_leader_and_preserves_acknowledged_state() {
    let root = unique_test_dir();
    fs::create_dir_all(&root).unwrap();
    let config = Arc::new(
        Config {
            cluster_name: "homekv-m3-leader-failover".into(),
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
    let mut stores = BTreeMap::new();
    let mut state_machines = BTreeMap::new();
    let mut observers = BTreeMap::new();
    for id in 1..=3 {
        let store = HomeKvRaftLogStore::open(root.join(format!("node-{id}.raft"))).unwrap();
        let sm = HomeKvStateMachine::default();
        let raft = Raft::new(
            id,
            config.clone(),
            factories.get(&id).unwrap().clone(),
            store.clone(),
            sm.clone(),
        )
        .await
        .unwrap();
        observers.insert(
            id,
            HomeKvReplicaObserver::new(raft.clone(), store.clone(), sm.clone()),
        );
        stores.insert(id, store.clone());
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
    nodes
        .get(&1)
        .unwrap()
        .initialize(membership())
        .await
        .unwrap();

    let old_leader = support::authoritative_leader(&nodes, None).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let observed = observers.values().all(|observer| {
            let metrics = observer.leadership_metrics();
            metrics.term_advances > 0
                && metrics.leader_selections > 0
                && metrics.leader_identity_changes > 0
        });
        if observed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "observers did not record the initial authoritative leader"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let initial_leadership: BTreeMap<_, _> = observers
        .iter()
        .map(|(id, observer)| (*id, observer.leadership_metrics()))
        .collect();

    nodes
        .get(&old_leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: b"before-failover".to_vec(),
            value: b"committed".to_vec(),
        })
        .await
        .unwrap();

    for peer in [1_u64, 2, 3] {
        if peer != old_leader {
            links.partition_bidirectional(old_leader, peer);
        }
    }

    let new_leader = support::authoritative_leader(&nodes, Some(old_leader)).await;
    assert_ne!(new_leader, old_leader);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let healthy_observed = observers
            .iter()
            .filter(|(id, _)| **id != old_leader)
            .all(|(id, observer)| {
                let current = observer.leadership_metrics();
                let initial = initial_leadership[id];
                current.term_advances > initial.term_advances
                    && current.leader_selections > initial.leader_selections
                    && current.leader_identity_changes > initial.leader_identity_changes
            });
        let new_leader_acquired = observers[&new_leader]
            .leadership_metrics()
            .local_leadership_acquisitions
            > initial_leadership[&new_leader].local_leadership_acquisitions;
        if healthy_observed && new_leader_acquired {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "healthy quorum did not expose the new election and leadership transition"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let leadership_json =
        serde_json::to_string(&observers[&new_leader].leadership_metrics()).unwrap();
    assert!(!leadership_json.contains("OpenRaft"));
    assert!(!leadership_json.contains("ServerState"));

    assert_eq!(
        state_machines
            .get(&new_leader)
            .unwrap()
            .get(b"before-failover")
            .await,
        Some(b"committed".to_vec())
    );
    nodes
        .get(&new_leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: b"after-failover".to_vec(),
            value: b"resumed".to_vec(),
        })
        .await
        .unwrap();
    nodes
        .get(&new_leader)
        .unwrap()
        .ensure_linearizable()
        .await
        .unwrap();
    assert_eq!(
        state_machines
            .get(&new_leader)
            .unwrap()
            .get(b"after-failover")
            .await,
        Some(b"resumed".to_vec())
    );

    let old_write = nodes
        .get(&old_leader)
        .unwrap()
        .client_write(RaftCommand::Set {
            key: b"isolated-old-leader".to_vec(),
            value: b"forbidden".to_vec(),
        });
    let outcome = tokio::time::timeout(Duration::from_millis(750), old_write).await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "isolated old leader must not acknowledge a write"
    );
    assert_eq!(
        state_machines
            .get(&old_leader)
            .unwrap()
            .get(b"isolated-old-leader")
            .await,
        None
    );

    // Retain evidence that the negative acknowledgement is backed by a real
    // durable uncommitted suffix, not merely rejection before admission.
    let mut old_store = stores[&old_leader].clone();
    let committed_before_heal = old_store
        .read_committed()
        .await
        .unwrap()
        .expect("the acknowledged prefix must remain committed");
    let speculative_log = old_store
        .get_log_state()
        .await
        .unwrap()
        .last_log_id
        .expect("the isolated leader must retain its admitted entry");
    assert!(
        speculative_log.index > committed_before_heal.index,
        "negative case must retain an actual uncommitted suffix: committed={committed_before_heal:?}, last={speculative_log:?}"
    );
    let speculative_entries = old_store
        .try_get_log_entries(speculative_log.index..=speculative_log.index)
        .await
        .unwrap();
    assert!(
        matches!(
            speculative_entries.as_slice(),
            [entry]
                if matches!(
                    &entry.payload,
                    EntryPayload::Normal(RaftCommand::Set { key, value })
                        if key == b"isolated-old-leader" && value == b"forbidden"
                )
        ),
        "the retained suffix must contain the isolated leader's command: {speculative_entries:?}"
    );

    // Exercise transient quorum failure deterministically. A cached Leader role
    // must not make the readiness helper succeed while every link is blocked.
    for a in 1..=3 {
        for b in (a + 1)..=3 {
            links.partition_bidirectional(a, b);
        }
    }
    let mut readiness = Box::pin(support::authoritative_leader(&nodes, None));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut readiness)
            .await
            .is_err(),
        "readiness must not bypass the quorum barrier"
    );
    for a in 1..=3 {
        for b in (a + 1)..=3 {
            links.heal_bidirectional(a, b);
        }
    }
    let healed_leader = readiness.await;
    let expected = state_machines[&healed_leader].view().await;
    assert_eq!(
        state_machines[&healed_leader]
            .get(b"isolated-old-leader")
            .await,
        None,
        "the conflicting suffix must not become committed after healing"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let converged = {
            let mut converged = true;
            for sm in state_machines.values() {
                converged &= sm.view().await == expected;
                converged &= sm.get(b"isolated-old-leader").await.is_none();
            }
            converged
        };
        if converged {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "former leader did not converge after suffix reconciliation"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if observers[&old_leader]
            .leadership_metrics()
            .local_leadership_losses
            > initial_leadership[&old_leader].local_leadership_losses
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "former leader did not expose loss of local leadership after healing"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let committed_after_heal = old_store
        .read_committed()
        .await
        .unwrap()
        .expect("healed former leader must recover committed progress");
    assert!(
        committed_after_heal.index >= speculative_log.index,
        "healed former leader did not advance through the conflicting suffix index"
    );
    let reconciled = old_store
        .try_get_log_entries(speculative_log.index..=speculative_log.index)
        .await
        .unwrap();
    assert!(
        reconciled.iter().all(|entry| !matches!(
            &entry.payload,
            EntryPayload::Normal(RaftCommand::Set { key, value })
                if key == b"isolated-old-leader" && value == b"forbidden"
        )),
        "the old leader's conflicting suffix survived reconciliation: {reconciled:?}"
    );

    for raft in nodes.values() {
        raft.shutdown().await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}
