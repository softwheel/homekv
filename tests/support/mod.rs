use std::collections::BTreeMap;
use std::time::Duration;

use homekv::raft::HomeKvRaftConfig;
use openraft::error::{CheckIsLeaderError, RaftError};
use openraft::raft::Raft;
use openraft::ServerState;

/// A role transition alone is not evidence that a read quorum is ready.
/// Retry only transient authority errors, and retain the real read barrier.
pub async fn authoritative_leader(
    nodes: &BTreeMap<u64, Raft<HomeKvRaftConfig>>,
    excluded: Option<u64>,
) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last_error = String::from("no leader candidate");
    loop {
        let snapshots: Vec<_> = nodes
            .iter()
            .map(|(id, raft)| (*id, raft.metrics().borrow().clone()))
            .collect();
        let leaders: Vec<_> = snapshots
            .iter()
            .filter_map(|(id, metrics)| {
                (Some(*id) != excluded && metrics.state == ServerState::Leader).then_some(*id)
            })
            .collect();
        assert!(
            tokio::time::Instant::now() < deadline,
            "read quorum did not become ready: {last_error}; metrics={snapshots:?}"
        );
        if let [id] = leaders.as_slice() {
            match tokio::time::timeout_at(deadline, nodes[id].ensure_linearizable()).await {
                Ok(Ok(_)) => return *id,
                Ok(Err(RaftError::APIError(
                    error @ (CheckIsLeaderError::QuorumNotEnough(_)
                    | CheckIsLeaderError::ForwardToLeader(_)),
                ))) => {
                    last_error = format!("node {id}: {error:?}");
                }
                Ok(Err(error)) => panic!("fatal read-barrier failure on node {id}: {error:?}"),
                Err(_) => panic!("read barrier timed out on node {id}; metrics={snapshots:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
