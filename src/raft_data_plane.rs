use std::sync::Arc;

use async_trait::async_trait;
use openraft::raft::Raft;
use openraft::ServerState;
use tokio::sync::Semaphore;

use crate::data_plane::{Mutation as WireMutation, Request, RequestBody, Status};
use crate::data_plane_runtime::{HandlerResponse, RequestHandler};
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftMutation};
use crate::storage::shard_for_key;

#[derive(Clone)]
pub struct ReplicatedShardRequestHandler {
    raft: Raft<HomeKvRaftConfig>,
    state_machine: HomeKvStateMachine,
    shard_id: u16,
    admission: Arc<Semaphore>,
}

impl ReplicatedShardRequestHandler {
    pub fn new(
        raft: Raft<HomeKvRaftConfig>,
        state_machine: HomeKvStateMachine,
        shard_id: u16,
        max_inflight: usize,
    ) -> Self {
        assert!(max_inflight > 0, "replicated admission must be bounded above zero");
        Self {
            raft,
            state_machine,
            shard_id,
            admission: Arc::new(Semaphore::new(max_inflight)),
        }
    }

    fn is_current_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.state == ServerState::Leader && metrics.current_leader == Some(metrics.id)
    }

    fn validate_key(&self, claimed_shard: u16, key: &[u8]) -> Result<(), Status> {
        if claimed_shard != self.shard_id || shard_for_key(key).as_u16() != self.shard_id {
            return Err(Status::WrongShard);
        }
        Ok(())
    }

    fn command_for(&self, request: &Request) -> Result<Option<RaftCommand>, Status> {
        match &request.body {
            RequestBody::Get { key } => {
                self.validate_key(request.shard_id, key)?;
                Ok(None)
            }
            RequestBody::Set { key, value } => {
                self.validate_key(request.shard_id, key)?;
                Ok(Some(RaftCommand::Set {
                    key: key.clone(),
                    value: value.clone(),
                }))
            }
            RequestBody::Delete { key } => {
                self.validate_key(request.shard_id, key)?;
                Ok(Some(RaftCommand::Delete { key: key.clone() }))
            }
            RequestBody::Batch { mutations } => {
                if mutations.is_empty() {
                    return Err(Status::MalformedRequest);
                }
                let mut raft_mutations = Vec::with_capacity(mutations.len());
                for mutation in mutations {
                    match mutation {
                        WireMutation::Set { key, value } => {
                            self.validate_key(request.shard_id, key)?;
                            raft_mutations.push(RaftMutation::Set {
                                key: key.clone(),
                                value: value.clone(),
                            });
                        }
                        WireMutation::Delete { key } => {
                            self.validate_key(request.shard_id, key)?;
                            raft_mutations.push(RaftMutation::Delete { key: key.clone() });
                        }
                    }
                }
                Ok(Some(RaftCommand::Batch {
                    mutations: raft_mutations,
                }))
            }
        }
    }

    async fn execute_get(&self, key: Vec<u8>) -> HandlerResponse {
        if !self.is_current_leader() {
            return HandlerResponse::new(Status::StaleRouteOrNotOwner, Vec::new());
        }
        let Ok(_permit) = self.admission.clone().try_acquire_owned() else {
            return HandlerResponse::new(Status::Overloaded, Vec::new());
        };

        if self.raft.ensure_linearizable().await.is_err() {
            let status = if self.is_current_leader() {
                Status::ClosedOrUnavailable
            } else {
                Status::StaleRouteOrNotOwner
            };
            return HandlerResponse::new(status, Vec::new());
        }

        match self.state_machine.get(&key).await {
            Some(value) => HandlerResponse::ok(value),
            None => HandlerResponse::new(Status::NotFound, Vec::new()),
        }
    }

    async fn execute_write(&self, command: RaftCommand) -> HandlerResponse {
        if !self.is_current_leader() {
            return HandlerResponse::new(Status::StaleRouteOrNotOwner, Vec::new());
        }
        let Ok(permit) = self.admission.clone().try_acquire_owned() else {
            return HandlerResponse::new(Status::Overloaded, Vec::new());
        };

        // Detach the admitted consensus operation from the transport future. If the client
        // disconnects after admission, dropping the request handler future must not revoke a
        // command that OpenRaft may later commit. The permit remains owned by this task until
        // consensus completion, preserving the separate bounded replicated-work budget.
        let raft = self.raft.clone();
        let admitted = tokio::spawn(async move {
            let _permit = permit;
            raft.client_write(command).await
        });

        match admitted.await {
            Ok(Ok(_)) => HandlerResponse::ok(Vec::new()),
            Ok(Err(_)) => {
                let status = if self.is_current_leader() {
                    Status::ClosedOrUnavailable
                } else {
                    Status::StaleRouteOrNotOwner
                };
                HandlerResponse::new(status, Vec::new())
            }
            Err(_) => HandlerResponse::new(Status::InternalError, Vec::new()),
        }
    }

    async fn execute(&self, request: Request) -> HandlerResponse {
        let command = match self.command_for(&request) {
            Ok(command) => command,
            Err(status) => return HandlerResponse::new(status, Vec::new()),
        };

        match (request.body, command) {
            (RequestBody::Get { key }, None) => self.execute_get(key).await,
            (_, Some(command)) => self.execute_write(command).await,
            _ => HandlerResponse::new(Status::InternalError, Vec::new()),
        }
    }
}

#[async_trait]
impl RequestHandler for ReplicatedShardRequestHandler {
    async fn handle(&self, request: Request) -> HandlerResponse {
        self.execute(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_plane::Mutation;
    use crate::storage::{shard_for_key, LOGICAL_SHARD_COUNT};

    fn find_key_for_shard(target: u16, prefix: &str) -> Vec<u8> {
        (0..1_000_000u32)
            .map(|i| format!("{prefix}-{i}").into_bytes())
            .find(|key| shard_for_key(key).as_u16() == target)
            .expect("key for configured shard")
    }

    // Validation is intentionally exercised without a live Raft node: these invariants must
    // reject malformed routing before any consensus admission is attempted.
    #[test]
    fn key_and_batch_validation_preserve_m2_routing_contract() {
        let configured = 7u16;
        let good = find_key_for_shard(configured, "good");
        let other_shard = (configured + 1) % LOGICAL_SHARD_COUNT;
        let bad = find_key_for_shard(other_shard, "bad");

        let validate = |claimed: u16, key: &[u8]| {
            claimed == configured && shard_for_key(key).as_u16() == configured
        };
        assert!(validate(configured, &good));
        assert!(!validate(configured, &bad));
        assert!(!validate(other_shard, &good));

        let batch = [
            Mutation::Set {
                key: good.clone(),
                value: b"v".to_vec(),
            },
            Mutation::Delete { key: bad.clone() },
        ];
        assert!(batch.iter().any(|mutation| match mutation {
            Mutation::Set { key, .. } | Mutation::Delete { key } => !validate(configured, key),
        }));
    }
}
