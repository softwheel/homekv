//! Deterministic driver-level fakes for the M4 movement (§7) and failure
//! (§8) matrix integration tests.
//!
//! The fakes mirror the unit-test doubles in `homekv::movement` (lines
//! 1512-1900) but add three capabilities the matrix needs:
//!
//! - [`FaultOperator`]: per-[`FaultPoint`] one-shot fault queues, so a test
//!   can fail one operation N times and then let it succeed (bounded retry);
//! - [`FakeDataPlane`]: a per-replica keyspace plus the acknowledged-write
//!   log, so tests can assert "no lost acknowledged write" and "no stale
//!   strong read" against committed state rather than timing;
//! - [`FakeCatalog`]: persists the [`PlacementCatalog`] across driver
//!   reconstructions (controller crash = drop the driver, build a new one
//!   over the same catalog), records the stable-voter history for the
//!   "exactly one active transition" invariant, and supports submit faults
//!   plus pre-submit hooks (concurrent-controller races).
//!
//! [`Harness::assert_converged`] encodes the shared §7/§8 invariants:
//! at most one active transition per shard, no premature target authority,
//! no destroyed still-authoritative replica, no lost acknowledged write,
//! no stale strong read, final catalog placement equals the converged voter
//! set, and all retained replicas converge.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use homekv::movement::{
    CatalogPort, LearnerEndpoint, LocalReplicaJanitor, MembershipOperator, MovementDriver,
    MovementDriverConfig, ObservedMembership, OperatorError,
};
use homekv::movement_admission::{MovementWorkAdmission, MovementWorkConfig};
use homekv::placement::{
    CatalogCommand, CatalogResponse, CatalogState, EligibleNode, MovementPhase, PlacementCatalog,
    PlacementError, ShardPlacement,
};
use homekv::placement_raft::CatalogGroupError;
use homekv::raft::RaftNodeId;

pub const SHARD: u16 = 5;
pub const OP_ID: [u8; 16] = [7; 16];

/// Injection point for one [`OperatorError`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FaultPoint {
    Observe,
    AddLearner,
    Promote,
    Remove,
    IsLeader,
}

/// Fake data plane: per-replica keyspaces plus the acknowledged-write log.
///
/// A write is "acknowledged" once it is replicated to every current voter;
/// the log is what a catching-up learner copies. All assertions read this
/// committed model state, never timing.
/// Per-replica keyspaces: node -> (key -> value).
type ReplicaKeyspaces = BTreeMap<RaftNodeId, BTreeMap<Vec<u8>, Vec<u8>>>;

#[derive(Default)]
pub struct FakeDataPlane {
    nodes: tokio::sync::Mutex<ReplicaKeyspaces>,
    acked: tokio::sync::Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl FakeDataPlane {
    /// Model an acknowledged Raft write: replicated to every voter.
    pub async fn write(&self, voters: &BTreeSet<RaftNodeId>, key: &[u8], value: &[u8]) {
        let mut nodes = self.nodes.lock().await;
        for voter in voters {
            nodes
                .entry(*voter)
                .or_default()
                .insert(key.to_vec(), value.to_vec());
        }
        drop(nodes);
        self.acked.lock().await.push((key.to_vec(), value.to_vec()));
    }

    /// A caught-up learner copies the full acknowledged log.
    pub async fn catch_up(&self, node: RaftNodeId) {
        let acked = self.acked.lock().await.clone();
        let mut nodes = self.nodes.lock().await;
        let map = nodes.entry(node).or_default();
        for (key, value) in acked {
            map.insert(key, value);
        }
    }

    pub async fn get(&self, node: RaftNodeId, key: &[u8]) -> Option<Vec<u8>> {
        self.nodes
            .lock()
            .await
            .get(&node)
            .and_then(|map| map.get(key))
            .cloned()
    }

    pub async fn acked(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.acked.lock().await.clone()
    }
}

/// Fake membership operator with per-operation fault queues.
///
/// Each queued fault is consumed by one call; once the queue for a
/// [`FaultPoint`] is empty the operation follows its success path. Faults
/// never mutate the observed membership: a failed call leaves the committed
/// view untouched, exactly like a real rejected RPC.
pub struct FaultOperator {
    pub observed: tokio::sync::Mutex<ObservedMembership>,
    faults: tokio::sync::Mutex<BTreeMap<FaultPoint, VecDeque<OperatorError>>>,
    pub added_learners: tokio::sync::Mutex<Vec<(RaftNodeId, bool)>>,
    /// Successfully applied promotions (failed attempts are not recorded).
    pub promoted: tokio::sync::Mutex<Vec<RaftNodeId>>,
    /// Successfully applied removals (failed attempts are not recorded).
    pub removed: tokio::sync::Mutex<Vec<RaftNodeId>>,
    pub is_leader_calls: tokio::sync::Mutex<u32>,
    pub leader: tokio::sync::Mutex<bool>,
    pub data: Arc<FakeDataPlane>,
    pub catchup_delay: tokio::sync::Mutex<Duration>,
}

impl FaultOperator {
    pub fn new(
        voters: BTreeSet<RaftNodeId>,
        learners: BTreeSet<RaftNodeId>,
        data: Arc<FakeDataPlane>,
    ) -> Self {
        Self {
            observed: tokio::sync::Mutex::new(ObservedMembership {
                voters,
                learners,
                membership_changing: false,
                term: 3,
            }),
            faults: tokio::sync::Mutex::new(BTreeMap::new()),
            added_learners: tokio::sync::Mutex::new(Vec::new()),
            promoted: tokio::sync::Mutex::new(Vec::new()),
            removed: tokio::sync::Mutex::new(Vec::new()),
            is_leader_calls: tokio::sync::Mutex::new(0),
            leader: tokio::sync::Mutex::new(true),
            data,
            catchup_delay: tokio::sync::Mutex::new(Duration::ZERO),
        }
    }

    /// Queue `times` failures at `point`; the (`times`+1)-st call succeeds.
    pub async fn fail_times(&self, point: FaultPoint, times: usize, error: OperatorError) {
        let mut faults = self.faults.lock().await;
        for _ in 0..times {
            faults.entry(point).or_default().push_back(error.clone());
        }
    }

    async fn take_fault(&self, point: FaultPoint) -> Option<OperatorError> {
        self.faults
            .lock()
            .await
            .get_mut(&point)
            .and_then(|queue| queue.pop_front())
    }
}

/// Newtype so [`MovementDriver`] can hold the fake operator by Arc while
/// respecting the orphan rule.
#[derive(Clone)]
pub struct Operator(pub Arc<FaultOperator>);

#[async_trait]
impl MembershipOperator for Operator {
    async fn observe(&self) -> Result<ObservedMembership, OperatorError> {
        let op = &self.0;
        if let Some(error) = op.take_fault(FaultPoint::Observe).await {
            return Err(error);
        }
        Ok(op.observed.lock().await.clone())
    }

    async fn add_learner(
        &self,
        learner: LearnerEndpoint,
        blocking: bool,
    ) -> Result<(), OperatorError> {
        let op = &self.0;
        op.added_learners.lock().await.push((learner.id, blocking));
        if let Some(error) = op.take_fault(FaultPoint::AddLearner).await {
            return Err(error);
        }
        op.observed.lock().await.learners.insert(learner.id);
        if blocking {
            let delay = *op.catchup_delay.lock().await;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            op.data.catch_up(learner.id).await;
        }
        Ok(())
    }

    async fn promote_learner(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
        let op = &self.0;
        if let Some(error) = op.take_fault(FaultPoint::Promote).await {
            return Err(error);
        }
        let mut observed = op.observed.lock().await;
        observed.learners.remove(&voter);
        observed.voters.insert(voter);
        drop(observed);
        op.promoted.lock().await.push(voter);
        Ok(())
    }

    async fn remove_voter(&self, voter: RaftNodeId) -> Result<(), OperatorError> {
        let op = &self.0;
        if let Some(error) = op.take_fault(FaultPoint::Remove).await {
            return Err(error);
        }
        op.observed.lock().await.voters.remove(&voter);
        op.removed.lock().await.push(voter);
        Ok(())
    }

    async fn is_leader(&self) -> Result<bool, OperatorError> {
        let op = &self.0;
        *op.is_leader_calls.lock().await += 1;
        if let Some(error) = op.take_fault(FaultPoint::IsLeader).await {
            return Err(error);
        }
        Ok(*op.leader.lock().await)
    }
}

/// Discriminant of a [`CatalogCommand`] for targeted submit faults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandKind {
    Begin,
    Advance,
    Record,
    Publish,
    Cancel,
    Other,
}

fn command_kind(command: &CatalogCommand) -> CommandKind {
    match command {
        CatalogCommand::BeginMovement { .. } => CommandKind::Begin,
        CatalogCommand::AdvanceMovementPhase { .. } => CommandKind::Advance,
        CatalogCommand::RecordMovementAttempt { .. } => CommandKind::Record,
        CatalogCommand::PublishMovement { .. } => CommandKind::Publish,
        CatalogCommand::CancelMovement { .. } => CommandKind::Cancel,
        _ => CommandKind::Other,
    }
}

struct SubmitFault {
    only: Option<CommandKind>,
    error: CatalogGroupError,
}

type PreHook = Box<dyn FnOnce(&mut PlacementCatalog) + Send>;

/// Fake catalog port: the durable record shared across driver instances.
///
/// A controller crash is modeled by dropping the [`MovementDriver`] and
/// constructing a new one over the same `Arc<FakeCatalog>`: the
/// [`PlacementCatalog`] (including any [`PendingMovement`]) survives,
/// exactly like the real disk-backed catalog.
pub struct FakeCatalog {
    inner: tokio::sync::Mutex<PlacementCatalog>,
    pub source_voters: [RaftNodeId; 3],
    pub target_voters: [RaftNodeId; 3],
    /// Committed stable voters of [`SHARD`] after every successful submit.
    /// The convergence invariant requires exactly one transition here:
    /// `source, .., source, target`.
    pub stable_history: tokio::sync::Mutex<Vec<[RaftNodeId; 3]>>,
    /// Number of committed `PublishMovement` commands.
    pub publish_count: Arc<AtomicU32>,
    submit_faults: tokio::sync::Mutex<VecDeque<SubmitFault>>,
    pre_hooks: tokio::sync::Mutex<VecDeque<PreHook>>,
}

impl FakeCatalog {
    /// Bootstrap a catalog and begin a movement left at `phase`.
    pub fn with_movement(phase: MovementPhase) -> Self {
        let mut catalog = PlacementCatalog::default();
        catalog
            .apply(CatalogCommand::Bootstrap {
                cluster_id: *b"movement-test01!",
                eligible_nodes: vec![
                    EligibleNode {
                        node_id: 1,
                        raft_endpoint: "node1:9000".to_string(),
                        failure_domain: "a".to_string(),
                    },
                    EligibleNode {
                        node_id: 2,
                        raft_endpoint: "node2:9000".to_string(),
                        failure_domain: "b".to_string(),
                    },
                    EligibleNode {
                        node_id: 3,
                        raft_endpoint: "node3:9000".to_string(),
                        failure_domain: "c".to_string(),
                    },
                    EligibleNode {
                        node_id: 4,
                        raft_endpoint: "node4:9000".to_string(),
                        failure_domain: "d".to_string(),
                    },
                ],
            })
            .expect("bootstrap applies");
        // Derive a valid intent from the actual bootstrap placement: swap
        // the highest stable voter for the spare eligible node.
        let state = catalog.state().expect("bootstrapped").clone();
        let source_voters: [RaftNodeId; 3] = state
            .placements
            .get(&SHARD)
            .expect("shard placement")
            .voters;
        let removed = source_voters[2];
        let spare = state
            .eligible_nodes
            .keys()
            .copied()
            .find(|node| !source_voters.contains(node))
            .expect("spare eligible node");
        let mut target: Vec<RaftNodeId> = source_voters
            .iter()
            .copied()
            .filter(|node| *node != removed)
            .chain(std::iter::once(spare))
            .collect();
        target.sort_unstable();
        let target_voters: [RaftNodeId; 3] = target.try_into().expect("3 voters");
        catalog
            .apply(CatalogCommand::BeginMovement {
                expected_epoch: 1,
                operation_id: OP_ID,
                shard_id: SHARD,
                target_voters,
            })
            .expect("begin applies");
        for next in phases_up_to(phase) {
            let epoch = catalog.state().expect("state").placement_epoch;
            catalog
                .apply(CatalogCommand::AdvanceMovementPhase {
                    expected_epoch: epoch,
                    operation_id: OP_ID,
                    shard_id: SHARD,
                    phase: next,
                })
                .expect("advance applies");
        }
        Self {
            inner: tokio::sync::Mutex::new(catalog),
            source_voters,
            target_voters,
            stable_history: tokio::sync::Mutex::new(vec![source_voters]),
            publish_count: Arc::new(AtomicU32::new(0)),
            submit_faults: tokio::sync::Mutex::new(VecDeque::new()),
            pre_hooks: tokio::sync::Mutex::new(VecDeque::new()),
        }
    }

    pub fn added(&self) -> RaftNodeId {
        self.target_voters
            .iter()
            .copied()
            .find(|node| !self.source_voters.contains(node))
            .expect("one added voter")
    }

    pub fn removed(&self) -> RaftNodeId {
        self.source_voters
            .iter()
            .copied()
            .find(|node| !self.target_voters.contains(node))
            .expect("one removed voter")
    }

    /// Encode the committed catalog as a snapshot image (for §8 snapshot
    /// tests). (Used by the §8 matrix; kept for the §7 binary via allow.)
    #[allow(dead_code)]
    pub async fn encode_snapshot(&self) -> Result<Vec<u8>, PlacementError> {
        self.inner.lock().await.encode_snapshot()
    }

    /// Committed placement of any shard (for concurrent-controller tests).
    /// (Used by the §7 matrix; kept for the §8 binary via allow.)
    #[allow(dead_code)]
    pub async fn placement(&self, shard_id: u16) -> Option<ShardPlacement> {
        self.inner
            .lock()
            .await
            .state()
            .and_then(|state| state.placements.get(&shard_id))
            .cloned()
    }

    /// Read the committed state (inherent mirror of the [`CatalogPort`] path).
    pub async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
        self.inner
            .lock()
            .await
            .state()
            .cloned()
            .ok_or(CatalogGroupError::MissingCommittedState)
    }

    /// Submit a catalog command (inherent mirror of the [`CatalogPort`] path).
    pub async fn submit(
        &self,
        command: CatalogCommand,
    ) -> Result<CatalogResponse, CatalogGroupError> {
        self.submit_inner(command).await
    }

    /// Fail the next submit (optionally only for one command kind) with
    /// `error`, once. A `None` kind matches any command.
    pub async fn fail_next_submit(&self, only: Option<CommandKind>, error: CatalogGroupError) {
        self.submit_faults
            .lock()
            .await
            .push_back(SubmitFault { only, error });
    }

    /// Run `hook` against the inner catalog immediately before the next
    /// submit is applied. Models a concurrent controller racing this one.
    /// (Used by the §7 matrix; kept for the §8 binary via allow.)
    #[allow(dead_code)]
    pub async fn hook_next_submit(
        &self,
        hook: impl FnOnce(&mut PlacementCatalog) + Send + 'static,
    ) {
        self.pre_hooks.lock().await.push_back(Box::new(hook));
    }

    async fn record_locked(&self, inner: &PlacementCatalog) {
        if let Some(placement) = inner.state().and_then(|state| state.placements.get(&SHARD)) {
            self.stable_history.lock().await.push(placement.voters);
        }
    }

    async fn take_fault(&self, command: &CatalogCommand) -> Option<CatalogGroupError> {
        let mut faults = self.submit_faults.lock().await;
        let hits = faults
            .front()
            .is_some_and(|fault| fault.only.is_none_or(|kind| kind == command_kind(command)));
        if hits {
            faults.pop_front().map(|fault| fault.error)
        } else {
            None
        }
    }

    async fn submit_inner(
        &self,
        command: CatalogCommand,
    ) -> Result<CatalogResponse, CatalogGroupError> {
        let hook = self.pre_hooks.lock().await.pop_front();
        let fault = self.take_fault(&command).await;
        let mut inner = self.inner.lock().await;
        if let Some(hook) = hook {
            hook(&mut inner);
        }
        if let Some(error) = fault {
            return Err(error);
        }
        let result = inner.apply(command).map_err(CatalogGroupError::from);
        if result.is_ok() {
            self.record_locked(&inner).await;
            if matches!(result, Ok(CatalogResponse::MovementPublished { .. })) {
                self.publish_count.fetch_add(1, Ordering::SeqCst);
            }
        }
        result
    }
}

fn phases_up_to(phase: MovementPhase) -> Vec<MovementPhase> {
    let order = [
        MovementPhase::Learner,
        MovementPhase::CatchUp,
        MovementPhase::Promote,
        MovementPhase::Lead,
        MovementPhase::Remove,
    ];
    match order.iter().position(|candidate| *candidate == phase) {
        Some(pos) => order[..=pos].to_vec(),
        // Intent/Publish/Cleanup are not reachable via AdvanceMovementPhase.
        None => Vec::new(),
    }
}

/// Newtype so [`MovementDriver`] can hold the fake catalog by Arc while
/// respecting the orphan rule.
#[derive(Clone)]
pub struct FakeCatalogPort(pub Arc<FakeCatalog>);

#[async_trait]
impl CatalogPort for FakeCatalogPort {
    async fn read_committed(&self) -> Result<CatalogState, CatalogGroupError> {
        self.0
            .inner
            .lock()
            .await
            .state()
            .cloned()
            .ok_or(CatalogGroupError::MissingCommittedState)
    }

    async fn submit(&self, command: CatalogCommand) -> Result<CatalogResponse, CatalogGroupError> {
        self.0.submit_inner(command).await
    }
}

/// Fake janitor recording removals with the publish count at removal time,
/// so tests can prove no replica was destroyed while still authoritative.
pub struct FakeJanitor {
    pub removals: tokio::sync::Mutex<Vec<(u16, u32)>>,
    local_replicas: tokio::sync::Mutex<Vec<u16>>,
    fail_remaining: tokio::sync::Mutex<u32>,
    publish_count: Arc<AtomicU32>,
}

impl FakeJanitor {
    pub fn with_replicas(replicas: Vec<u16>, publish_count: Arc<AtomicU32>) -> Self {
        Self {
            removals: tokio::sync::Mutex::new(Vec::new()),
            local_replicas: tokio::sync::Mutex::new(replicas),
            fail_remaining: tokio::sync::Mutex::new(0),
            publish_count,
        }
    }

    /// Fail the next `times` removals with an injected error.
    /// (Used by the §7 matrix; kept for the §8 binary via allow.)
    #[allow(dead_code)]
    pub async fn fail_next(&self, times: u32) {
        *self.fail_remaining.lock().await = times;
    }
}

#[async_trait]
impl LocalReplicaJanitor for FakeJanitor {
    async fn remove_local_replica(&self, shard_id: u16) -> Result<(), String> {
        let mut fail = self.fail_remaining.lock().await;
        if *fail > 0 {
            *fail -= 1;
            return Err("injected janitor fault".to_string());
        }
        drop(fail);
        let published = self.publish_count.load(Ordering::SeqCst);
        self.removals.lock().await.push((shard_id, published));
        self.local_replicas
            .lock()
            .await
            .retain(|shard| *shard != shard_id);
        Ok(())
    }

    async fn list_local_replicas(&self) -> Result<Vec<u16>, String> {
        Ok(self.local_replicas.lock().await.clone())
    }
}

/// The driver type under test.
pub type TestDriver = MovementDriver<Operator, FakeCatalogPort>;

/// One self-contained movement scenario: catalog + operator + janitor +
/// data plane, wired so a controller crash is just "drop the driver and
/// build a new one".
#[derive(Clone)]
pub struct Harness {
    pub catalog: Arc<FakeCatalog>,
    pub operator: Arc<FaultOperator>,
    pub janitor: Arc<FakeJanitor>,
    pub data: Arc<FakeDataPlane>,
}

impl Harness {
    /// Build a harness with the movement parked at `phase` and the
    /// data-group observation set to the state the driver expects there:
    /// source voters (and no learners) at Intent/Learner, source voters
    /// plus the admitted learner at CatchUp/Promote, and the promoted
    /// membership at Lead/Remove.
    ///
    /// The publish-ready simulation (membership committed, publish not yet
    /// submitted) is built with `phase = Remove` followed by
    /// [`Harness::set_observed`] to the target voters.
    pub fn new(phase: MovementPhase) -> Self {
        let catalog = Arc::new(FakeCatalog::with_movement(phase));
        let source: BTreeSet<RaftNodeId> = catalog.source_voters.iter().copied().collect();
        let added = catalog.added();
        let (voters, learners) = match phase {
            MovementPhase::Intent | MovementPhase::Learner => (source.clone(), BTreeSet::new()),
            MovementPhase::CatchUp | MovementPhase::Promote => {
                (source.clone(), BTreeSet::from([added]))
            }
            MovementPhase::Lead | MovementPhase::Remove => {
                let mut voters = source.clone();
                voters.insert(added);
                (voters, BTreeSet::new())
            }
            // Publish/Cleanup are terminal markers unreachable via
            // AdvanceMovementPhase; park at the source and let the test set
            // the observation explicitly.
            MovementPhase::Publish | MovementPhase::Cleanup => (source, BTreeSet::new()),
        };
        let data = Arc::new(FakeDataPlane::default());
        let operator = Arc::new(FaultOperator::new(voters, learners, data.clone()));
        let janitor = Arc::new(FakeJanitor::with_replicas(
            vec![SHARD],
            catalog.publish_count.clone(),
        ));
        Self {
            catalog,
            operator,
            janitor,
            data,
        }
    }

    /// Replace the observed data-group membership (replica restart, or the
    /// publish-ready simulation where the committed membership already
    /// equals the target).
    pub async fn set_observed(&self, voters: BTreeSet<RaftNodeId>, learners: BTreeSet<RaftNodeId>) {
        *self.operator.observed.lock().await = ObservedMembership {
            voters,
            learners,
            membership_changing: false,
            term: 3,
        };
    }

    /// The removed voter is the natural driver identity: its local replica
    /// must be reclaimed only after publication.
    pub fn driver(&self, local_node: RaftNodeId, max_attempts: u32) -> TestDriver {
        let config = MovementDriverConfig {
            local_node_id: local_node,
            work_node_id: local_node,
            max_attempts_per_drive: max_attempts,
            retry_delay: Duration::from_millis(1),
            settle_delay: Duration::from_millis(1),
            tombstone_capacity: 16,
        };
        let admission = MovementWorkAdmission::new(MovementWorkConfig {
            cluster_max_concurrent: 4,
            per_node_max_concurrent: 2,
        })
        .expect("admission config valid");
        MovementDriver::new(
            config,
            Operator(self.operator.clone()),
            FakeCatalogPort(self.catalog.clone()),
            Some(self.janitor.clone() as Arc<dyn LocalReplicaJanitor>),
            admission,
        )
    }

    /// Model an acknowledged foreground write through the current voters.
    /// Learners replicate the log too, like real Raft learners.
    pub async fn ack_write(&self, key: &[u8], value: &[u8]) {
        let observed = self.operator.observed.lock().await;
        let mut targets = observed.voters.clone();
        targets.extend(observed.learners.iter().copied());
        drop(observed);
        self.data.write(&targets, key, value).await;
    }

    /// Strong read through the committed stable placement (voters[0]).
    pub async fn strong_read(&self, key: &[u8]) -> Option<Vec<u8>> {
        let state = self.catalog.read_committed().await.ok()?;
        let placement = state.placements.get(&SHARD)?;
        self.data.get(placement.voters[0], key).await
    }

    /// Drive until the movement completes; any other terminal outcome is a
    /// test failure.
    pub async fn drive_until_completed(&self, driver: &TestDriver) {
        use homekv::movement::DriverOutcome;
        for _ in 0..10 {
            match driver.drive_shard(SHARD).await {
                DriverOutcome::Completed { .. } => return,
                DriverOutcome::TransientFailure { .. } => continue,
                other => panic!("drive stalled with unexpected outcome: {other:?}"),
            }
        }
        panic!("movement did not converge within 10 drive rounds");
    }

    /// The shared §7/§8 invariant battery, read from committed state:
    ///
    /// - final catalog placement equals the converged voter set, and the
    ///   movement is published (no pending intent left behind);
    /// - the operator's committed membership reconciled forward to it;
    /// - exactly one stable transition happened (`source,..,source,target`):
    ///   at most one active transition per shard and no premature target
    ///   authority (stable placement untouched until the atomic publish);
    /// - every acknowledged write is present on every final voter (no lost
    ///   acknowledged write) and visible to strong reads (no stale read);
    /// - every reclaimed replica was removed only after the publish
    ///   committed (no destroyed still-authoritative replica).
    pub async fn assert_converged(&self) {
        let target: BTreeSet<RaftNodeId> = self.catalog.target_voters.iter().copied().collect();
        let state = self
            .catalog
            .read_committed()
            .await
            .expect("catalog readable");
        let placement = state.placements.get(&SHARD).expect("shard placement");
        assert_eq!(
            placement.voters, self.catalog.target_voters,
            "final catalog placement must equal the converged voter set"
        );
        assert!(
            placement.pending_movement.is_none(),
            "movement must be published, not left pending"
        );

        let observed = self.operator.observed.lock().await.clone();
        assert_eq!(
            observed.voters, target,
            "committed data-group membership must reconcile forward to the target"
        );

        let history = self.catalog.stable_history.lock().await;
        assert!(
            history.len() >= 2,
            "stable history must show the publish transition"
        );
        assert!(
            history[..history.len() - 1]
                .iter()
                .all(|voters| *voters == self.catalog.source_voters),
            "stable placement must stay at the source until the atomic publish: {history:?}"
        );
        assert_eq!(
            history[history.len() - 1],
            self.catalog.target_voters,
            "exactly one active transition per shard"
        );
        drop(history);

        let acked = self.data.acked().await;
        assert!(!acked.is_empty(), "test must acknowledge a write first");
        for (key, value) in &acked {
            for node in &target {
                assert_eq!(
                    self.data.get(*node, key).await,
                    Some(value.clone()),
                    "no lost acknowledged write on node {node}"
                );
            }
            assert_eq!(
                self.strong_read(key).await,
                Some(value.clone()),
                "no stale strong read"
            );
        }

        for (shard, published_at_removal) in self.janitor.removals.lock().await.iter() {
            assert!(
                *published_at_removal >= 1,
                "replica for shard {shard} destroyed before its publish committed"
            );
        }
    }
}
