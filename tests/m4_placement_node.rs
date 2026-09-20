//! End-to-end proof for the production placement-node composition (spec-0006
//! verification blocker 3).
//!
//! Starts a real [`PlacementNode`](homekv::placement_node::PlacementNode)
//! (catalog Raft group + data Raft groups + per-shard `MovementDriver`s +
//! drive loop), writes data, commits a movement intent through the catalog,
//! and asserts the drive loop converges the movement while preserving the
//! data — the exact path the `homekv --placement` binary runs.

use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use homekv::placement::{CatalogCommand, CatalogResponse};
use homekv::placement_node::{PlacementNode, PlacementNodeConfig};
use homekv::raft::RaftCommand;

const SHARD: u16 = 7;

fn unique_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "homekv-m4-placement-node-{}-{nonce}",
        std::process::id()
    ))
}

async fn wait_for<F, Fut, T>(mut f: F, timeout: Duration, what: &str) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = f().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn placement_node_drives_movement_end_to_end() {
    let config = PlacementNodeConfig {
        node_ids: vec![1, 2, 3, 4],
        catalog_voters: [1, 2, 3],
        data_dir: unique_dir(),
        cluster_id: *b"homekv-m4-e2e000",
        shards: vec![SHARD],
        drive_interval: Duration::from_millis(250),
        reconcile_interval: Duration::from_secs(1),
        election_min_ms: 150,
        election_max_ms: 300,
        ..PlacementNodeConfig::default()
    };
    let node = PlacementNode::start(config).await.expect("node starts");

    // 1. The catalog bootstraps with canonical placements. Read shard 7's
    //    actual initial voters (with 4 eligible nodes the bootstrap spreads
    //    placements; we adapt the movement target to reality).
    let state = node.committed_state().await.expect("catalog readable");
    let placement = state.placements.get(&SHARD).expect("shard placement");
    let source_voters = placement.voters;
    assert!(placement.pending_movement.is_none());
    let epoch = state.placement_epoch;
    // Target: swap the last voter for the eligible node not currently voting.
    let incoming = [1u64, 2, 3, 4]
        .into_iter()
        .find(|id| !source_voters.contains(id))
        .expect("an eligible standby exists");
    let mut target_voters = source_voters;
    target_voters[2] = incoming;
    target_voters.sort_unstable();
    assert_ne!(target_voters, source_voters);

    // 2. Elect a data-group leader and write data through consensus.
    let leader_id = wait_for(
        || async { node.group_leader(SHARD) },
        Duration::from_secs(15),
        "data-group leader",
    )
    .await;
    let leader_raft = node.group_raft(SHARD, leader_id).expect("leader raft");
    leader_raft
        .client_write(RaftCommand::Set {
            key: b"movement-e2e-key".to_vec(),
            value: b"movement-e2e-value".to_vec(),
        })
        .await
        .expect("write commits");

    // 3. The write is replicated to every voter.
    wait_for(
        || async {
            let mut ok = true;
            for id in source_voters {
                let sm = node.group_state_machine(SHARD, id).expect("replica sm");
                if sm.get(b"movement-e2e-key").await != Some(b"movement-e2e-value".to_vec()) {
                    ok = false;
                }
            }
            ok.then_some(())
        },
        Duration::from_secs(15),
        "write replication",
    )
    .await;

    // 4. Commit a movement intent through the catalog: swap one voter for
    //    the eligible standby hosted by the same process.
    let response = node
        .catalog()
        .submit(CatalogCommand::BeginMovement {
            expected_epoch: epoch,
            operation_id: *b"m4-e2e-op-000001",
            shard_id: SHARD,
            target_voters,
        })
        .await
        .expect("begin movement commits");
    assert!(
        matches!(response, CatalogResponse::MovementAccepted { .. }),
        "unexpected catalog response: {response:?}"
    );

    // 5. The drive loop converges the movement: learner admission, catch-up,
    //    promotion, voter removal, and catalog publication.
    wait_for(
        || async {
            let state = node.committed_state().await.ok()?;
            let placement = state.placements.get(&SHARD)?;
            (placement.voters == target_voters && placement.pending_movement.is_none())
                .then_some(())
        },
        Duration::from_secs(60),
        "movement convergence",
    )
    .await;

    // 6. The pre-movement write survived on the new voter set, including the
    //    promoted learner.
    for id in target_voters {
        let sm = node.group_state_machine(SHARD, id).expect("replica exists");
        assert_eq!(
            sm.get(b"movement-e2e-key").await,
            Some(b"movement-e2e-value".to_vec()),
            "data preserved on node {id} after movement"
        );
    }

    node.shutdown();
}
