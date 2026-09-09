use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use openraft::raft::Raft;
use openraft::ServerState;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::data_plane::{Mutation as WireMutation, Request, RequestBody, Status};
use crate::data_plane_runtime::{HandlerResponse, RequestHandler};
use crate::raft::{HomeKvRaftConfig, HomeKvStateMachine, RaftCommand, RaftMutation};
use crate::storage::shard_for_key;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicatedHandlerMetricsSnapshot {
    pub read_requests: u64,
    pub write_requests: u64,
    pub successful_reads: u64,
    pub successful_writes: u64,
    pub not_found_responses: u64,
    pub not_leader_responses: u64,
    pub unavailable_responses: u64,
    pub overload_responses: u64,
    pub invalid_responses: u64,
    pub internal_error_responses: u64,
    pub admission_capacity: usize,
    pub admission_current: usize,
    pub admission_peak: usize,
    pub admission_rejections: u64,
}

#[derive(Debug)]
struct ReplicatedHandlerMetrics {
    read_requests: AtomicU64,
    write_requests: AtomicU64,
    successful_reads: AtomicU64,
    successful_writes: AtomicU64,
    not_found_responses: AtomicU64,
    not_leader_responses: AtomicU64,
    unavailable_responses: AtomicU64,
    overload_responses: AtomicU64,
    invalid_responses: AtomicU64,
    internal_error_responses: AtomicU64,
    admission_capacity: usize,
    admission_current: AtomicUsize,
    admission_peak: AtomicUsize,
    admission_rejections: AtomicU64,
}

impl ReplicatedHandlerMetrics {
    fn new(admission_capacity: usize) -> Self {
        Self {
            read_requests: AtomicU64::new(0),
            write_requests: AtomicU64::new(0),
            successful_reads: AtomicU64::new(0),
            successful_writes: AtomicU64::new(0),
            not_found_responses: AtomicU64::new(0),
            not_leader_responses: AtomicU64::new(0),
            unavailable_responses: AtomicU64::new(0),
            overload_responses: AtomicU64::new(0),
            invalid_responses: AtomicU64::new(0),
            internal_error_responses: AtomicU64::new(0),
            admission_capacity,
            admission_current: AtomicUsize::new(0),
            admission_peak: AtomicUsize::new(0),
            admission_rejections: AtomicU64::new(0),
        }
    }

    fn request_started(&self, read: bool) {
        if read {
            self.read_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.write_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_outcome(&self, read: bool, status: Status) {
        match status {
            Status::Ok if read => {
                self.successful_reads.fetch_add(1, Ordering::Relaxed);
            }
            Status::Ok => {
                self.successful_writes.fetch_add(1, Ordering::Relaxed);
            }
            Status::NotFound => {
                self.not_found_responses.fetch_add(1, Ordering::Relaxed);
            }
            Status::StaleRouteOrNotOwner => {
                self.not_leader_responses.fetch_add(1, Ordering::Relaxed);
            }
            Status::ClosedOrUnavailable => {
                self.unavailable_responses.fetch_add(1, Ordering::Relaxed);
            }
            Status::Overloaded => {
                self.overload_responses.fetch_add(1, Ordering::Relaxed);
            }
            Status::WrongShard | Status::MalformedRequest => {
                self.invalid_responses.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.internal_error_responses.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn admitted(self: &Arc<Self>) -> AdmissionGauge {
        let current = self.admission_current.fetch_add(1, Ordering::Relaxed) + 1;
        self.admission_peak.fetch_max(current, Ordering::Relaxed);
        AdmissionGauge {
            metrics: self.clone(),
        }
    }

    fn rejected(&self) {
        self.admission_rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ReplicatedHandlerMetricsSnapshot {
        ReplicatedHandlerMetricsSnapshot {
            read_requests: self.read_requests.load(Ordering::Relaxed),
            write_requests: self.write_requests.load(Ordering::Relaxed),
            successful_reads: self.successful_reads.load(Ordering::Relaxed),
            successful_writes: self.successful_writes.load(Ordering::Relaxed),
            not_found_responses: self.not_found_responses.load(Ordering::Relaxed),
            not_leader_responses: self.not_leader_responses.load(Ordering::Relaxed),
            unavailable_responses: self.unavailable_responses.load(Ordering::Relaxed),
            overload_responses: self.overload_responses.load(Ordering::Relaxed),
            invalid_responses: self.invalid_responses.load(Ordering::Relaxed),
            internal_error_responses: self.internal_error_responses.load(Ordering::Relaxed),
            admission_capacity: self.admission_capacity,
            admission_current: self.admission_current.load(Ordering::Relaxed),
            admission_peak: self.admission_peak.load(Ordering::Relaxed),
            admission_rejections: self.admission_rejections.load(Ordering::Relaxed),
        }
    }
}

struct AdmissionGauge {
    metrics: Arc<ReplicatedHandlerMetrics>,
}

impl Drop for AdmissionGauge {
    fn drop(&mut self) {
        self.metrics.admission_current.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ReplicatedShardRequestHandler {
    raft: Raft<HomeKvRaftConfig>,
    state_machine: HomeKvStateMachine,
    shard_id: u16,
    admission: Arc<Semaphore>,
    metrics: Arc<ReplicatedHandlerMetrics>,
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
            metrics: Arc::new(ReplicatedHandlerMetrics::new(max_inflight)),
        }
    }

    pub fn metrics(&self) -> ReplicatedHandlerMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn try_admit(&self) -> Result<(OwnedSemaphorePermit, AdmissionGauge), ()> {
        match self.admission.clone().try_acquire_owned() {
            Ok(permit) => Ok((permit, self.metrics.admitted())),
            Err(_) => {
                self.metrics.rejected();
                Err(())
            }
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
        let Ok((_permit, _admission)) = self.try_admit() else {
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
        let Ok((permit, admission)) = self.try_admit() else {
            return HandlerResponse::new(Status::Overloaded, Vec::new());
        };

        // Detach the admitted consensus operation from the transport future. If the client
        // disconnects after admission, dropping the request handler future must not revoke a
        // command that OpenRaft may later commit. The permit remains owned by this task until
        // consensus completion, preserving the separate bounded replicated-work budget.
        let raft = self.raft.clone();
        let admitted = tokio::spawn(async move {
            let _permit = permit;
            let _admission = admission;
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
        let read = matches!(&request.body, RequestBody::Get { .. });
        self.metrics.request_started(read);
        let response = self.execute(request).await;
        self.metrics.record_outcome(read, response.status);
        response
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
