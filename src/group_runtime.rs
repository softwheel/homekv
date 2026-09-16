use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_derive::{Deserialize, Serialize};
use tokio::sync::{oneshot, Notify};

use crate::group_registry::{GroupKind, GroupRegistryError};
use crate::placement::GroupId;

struct GroupJob {
    work: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    completion: oneshot::Sender<()>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedGroupRuntimeConfig {
    pub worker_count: usize,
    pub global_queue_capacity: usize,
    pub per_group_queue_capacity: usize,
}

impl SharedGroupRuntimeConfig {
    pub fn validate(self) -> Result<Self, SharedGroupRuntimeError> {
        if self.worker_count == 0 {
            return Err(SharedGroupRuntimeError::InvalidWorkerCount);
        }
        if self.global_queue_capacity == 0 {
            return Err(SharedGroupRuntimeError::InvalidGlobalQueueCapacity);
        }
        if self.per_group_queue_capacity == 0 {
            return Err(SharedGroupRuntimeError::InvalidPerGroupQueueCapacity);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum QueueSaturationScope {
    Group,
    Global,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SharedGroupRuntimeError {
    InvalidWorkerCount,
    InvalidGlobalQueueCapacity,
    InvalidPerGroupQueueCapacity,
    UnknownGroup { shard_id: u16 },
    Closed,
    QueueSaturated {
        group_id: GroupId,
        scope: QueueSaturationScope,
    },
    ReceiptClosed,
}

impl fmt::Display for SharedGroupRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWorkerCount => write!(f, "shared runtime worker count must be non-zero"),
            Self::InvalidGlobalQueueCapacity => {
                write!(f, "shared runtime global queue capacity must be non-zero")
            }
            Self::InvalidPerGroupQueueCapacity => {
                write!(f, "shared runtime per-group queue capacity must be non-zero")
            }
            Self::UnknownGroup { shard_id } => {
                write!(f, "data group for logical shard {shard_id} does not exist")
            }
            Self::Closed => write!(f, "shared group runtime is closed"),
            Self::QueueSaturated { group_id, scope } => {
                write!(f, "shared runtime {scope:?} queue is full for group {group_id}")
            }
            Self::ReceiptClosed => write!(f, "shared runtime task receipt closed unexpectedly"),
        }
    }
}

impl std::error::Error for SharedGroupRuntimeError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupQueueMetrics {
    pub group_id: GroupId,
    pub queued: usize,
    pub peak_queued: usize,
    pub submitted: u64,
    pub completed: u64,
    pub rejections: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SharedGroupRuntimeMetrics {
    pub worker_count: usize,
    pub worker_tasks_spawned: usize,
    pub global_queue_capacity: usize,
    pub per_group_queue_capacity: usize,
    pub known_groups: usize,
    pub queued: usize,
    pub peak_queued: usize,
    pub running: usize,
    pub peak_running: usize,
    pub submitted: u64,
    pub completed: u64,
    pub group_rejections: u64,
    pub global_rejections: u64,
    pub closed: bool,
    pub groups: Vec<GroupQueueMetrics>,
}

pub struct GroupTaskReceipt {
    receiver: oneshot::Receiver<()>,
}

impl GroupTaskReceipt {
    pub async fn wait(self) -> Result<(), SharedGroupRuntimeError> {
        self.receiver
            .await
            .map_err(|_| SharedGroupRuntimeError::ReceiptClosed)
    }
}

#[derive(Clone)]
pub struct SharedGroupRuntime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    config: SharedGroupRuntimeConfig,
    state: Mutex<RuntimeState>,
    available: Notify,
}

struct RuntimeState {
    queues: BTreeMap<GroupId, VecDeque<GroupJob>>,
    active_groups: VecDeque<GroupId>,
    groups: BTreeMap<GroupId, MutableGroupMetrics>,
    queued: usize,
    peak_queued: usize,
    running: usize,
    peak_running: usize,
    submitted: u64,
    completed: u64,
    group_rejections: u64,
    global_rejections: u64,
    closed: bool,
}

#[derive(Default)]
struct MutableGroupMetrics {
    queued: usize,
    peak_queued: usize,
    submitted: u64,
    completed: u64,
    rejections: u64,
}

impl SharedGroupRuntime {
    pub fn new(config: SharedGroupRuntimeConfig) -> Result<Self, SharedGroupRuntimeError> {
        let config = config.validate()?;
        let runtime = Self {
            inner: Arc::new(RuntimeInner {
                config,
                state: Mutex::new(RuntimeState {
                    queues: BTreeMap::new(),
                    active_groups: VecDeque::new(),
                    groups: BTreeMap::new(),
                    queued: 0,
                    peak_queued: 0,
                    running: 0,
                    peak_running: 0,
                    submitted: 0,
                    completed: 0,
                    group_rejections: 0,
                    global_rejections: 0,
                    closed: false,
                }),
                available: Notify::new(),
            }),
        };
        for _ in 0..config.worker_count {
            let inner = Arc::clone(&runtime.inner);
            tokio::spawn(async move { worker_loop(inner).await });
        }
        Ok(runtime)
    }

    pub fn try_submit<F>(
        &self,
        kind: GroupKind,
        work: F,
    ) -> Result<GroupTaskReceipt, SharedGroupRuntimeError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let group_id = kind.group_id().map_err(map_group_error)?;
        let (sender, receiver) = oneshot::channel();
        let job = GroupJob {
            work: Box::pin(work),
            completion: sender,
        };
        {
            let mut state = lock_state(&self.inner.state);
            if state.closed {
                return Err(SharedGroupRuntimeError::Closed);
            }

            if state.queued >= self.inner.config.global_queue_capacity {
                state.global_rejections = state.global_rejections.saturating_add(1);
                let group = state.groups.entry(group_id).or_default();
                group.rejections = group.rejections.saturating_add(1);
                return Err(SharedGroupRuntimeError::QueueSaturated {
                    group_id,
                    scope: QueueSaturationScope::Global,
                });
            }

            let group_queued = state.queues.get(&group_id).map_or(0, VecDeque::len);
            if group_queued >= self.inner.config.per_group_queue_capacity {
                state.group_rejections = state.group_rejections.saturating_add(1);
                let group = state.groups.entry(group_id).or_default();
                group.rejections = group.rejections.saturating_add(1);
                return Err(SharedGroupRuntimeError::QueueSaturated {
                    group_id,
                    scope: QueueSaturationScope::Group,
                });
            }

            let queue = state.queues.entry(group_id).or_default();
            let was_empty = queue.is_empty();
            queue.push_back(job);
            if was_empty {
                state.active_groups.push_back(group_id);
            }
            state.queued += 1;
            state.peak_queued = state.peak_queued.max(state.queued);
            state.submitted = state.submitted.saturating_add(1);
            let queued = state.queues.get(&group_id).map_or(0, VecDeque::len);
            let group = state.groups.entry(group_id).or_default();
            group.queued = queued;
            group.peak_queued = group.peak_queued.max(queued);
            group.submitted = group.submitted.saturating_add(1);
        }
        self.inner.available.notify_one();
        Ok(GroupTaskReceipt { receiver })
    }

    pub fn close(&self) {
        let mut state = lock_state(&self.inner.state);
        state.closed = true;
        drop(state);
        self.inner.available.notify_waiters();
    }

    pub fn metrics(&self) -> SharedGroupRuntimeMetrics {
        let state = lock_state(&self.inner.state);
        SharedGroupRuntimeMetrics {
            worker_count: self.inner.config.worker_count,
            worker_tasks_spawned: self.inner.config.worker_count,
            global_queue_capacity: self.inner.config.global_queue_capacity,
            per_group_queue_capacity: self.inner.config.per_group_queue_capacity,
            known_groups: state.groups.len(),
            queued: state.queued,
            peak_queued: state.peak_queued,
            running: state.running,
            peak_running: state.peak_running,
            submitted: state.submitted,
            completed: state.completed,
            group_rejections: state.group_rejections,
            global_rejections: state.global_rejections,
            closed: state.closed,
            groups: state
                .groups
                .iter()
                .map(|(group_id, metrics)| GroupQueueMetrics {
                    group_id: *group_id,
                    queued: metrics.queued,
                    peak_queued: metrics.peak_queued,
                    submitted: metrics.submitted,
                    completed: metrics.completed,
                    rejections: metrics.rejections,
                })
                .collect(),
        }
    }
}

async fn worker_loop(inner: Arc<RuntimeInner>) {
    loop {
        let notified = inner.available.notified();
        let next = {
            let mut state = lock_state(&inner.state);
            let next = pop_next_job(&mut state);
            if next.is_none() && state.closed {
                return;
            }
            next
        };
        let Some((group_id, job)) = next else {
            notified.await;
            continue;
        };

        job.work.await;

        let mut state = lock_state(&inner.state);
        state.running = state.running.saturating_sub(1);
        state.completed = state.completed.saturating_add(1);
        if let Some(group) = state.groups.get_mut(&group_id) {
            group.completed = group.completed.saturating_add(1);
        }
        if state.closed && state.queued == 0 && state.running == 0 {
            drop(state);
            inner.available.notify_waiters();
        }
        let _ = job.completion.send(());
    }
}

fn pop_next_job(state: &mut RuntimeState) -> Option<(GroupId, GroupJob)> {
    let group_id = state.active_groups.pop_front()?;
    let queue = state
        .queues
        .get_mut(&group_id)
        .expect("active group must have a queue");
    let job = queue.pop_front().expect("active group queue must be non-empty");
    let remaining = queue.len();
    if remaining > 0 {
        state.active_groups.push_back(group_id);
    }
    state.queued = state.queued.saturating_sub(1);
    state.running += 1;
    state.peak_running = state.peak_running.max(state.running);
    if let Some(group) = state.groups.get_mut(&group_id) {
        group.queued = remaining;
    }
    Some((group_id, job))
}

fn lock_state(mutex: &Mutex<RuntimeState>) -> MutexGuard<'_, RuntimeState> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn map_group_error(error: GroupRegistryError) -> SharedGroupRuntimeError {
    match error {
        GroupRegistryError::UnknownDataGroup { shard_id } => {
            SharedGroupRuntimeError::UnknownGroup { shard_id }
        }
        _ => unreachable!("group identity validation returned an unrelated registry error"),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::storage::LOGICAL_SHARD_COUNT;

    fn config(
        worker_count: usize,
        global_queue_capacity: usize,
        per_group_queue_capacity: usize,
    ) -> SharedGroupRuntimeConfig {
        SharedGroupRuntimeConfig {
            worker_count,
            global_queue_capacity,
            per_group_queue_capacity,
        }
    }

    #[test]
    fn configuration_is_explicit_and_non_zero() {
        assert!(matches!(
            SharedGroupRuntimeConfig {
                worker_count: 0,
                global_queue_capacity: 1,
                per_group_queue_capacity: 1,
            }
            .validate(),
            Err(SharedGroupRuntimeError::InvalidWorkerCount)
        ));
        assert!(matches!(
            config(1, 0, 1).validate(),
            Err(SharedGroupRuntimeError::InvalidGlobalQueueCapacity)
        ));
        assert!(matches!(
            config(1, 1, 0).validate(),
            Err(SharedGroupRuntimeError::InvalidPerGroupQueueCapacity)
        ));
    }

    #[tokio::test]
    async fn worker_count_is_constant_across_all_data_groups() {
        let runtime = SharedGroupRuntime::new(config(
            2,
            usize::from(LOGICAL_SHARD_COUNT),
            1,
        ))
        .unwrap();
        let mut receipts = Vec::new();
        for shard_id in 0..LOGICAL_SHARD_COUNT {
            receipts.push(
                runtime
                    .try_submit(GroupKind::Data { shard_id }, async {})
                    .unwrap(),
            );
        }
        for receipt in receipts {
            receipt.wait().await.unwrap();
        }
        let metrics = runtime.metrics();
        assert_eq!(metrics.worker_count, 2);
        assert_eq!(metrics.worker_tasks_spawned, 2);
        assert_eq!(metrics.known_groups, usize::from(LOGICAL_SHARD_COUNT));
        assert_eq!(metrics.submitted, u64::from(LOGICAL_SHARD_COUNT));
        assert_eq!(metrics.completed, u64::from(LOGICAL_SHARD_COUNT));
        assert_eq!(metrics.queued, 0);
        assert_eq!(metrics.running, 0);
        runtime.close();
    }

    #[tokio::test]
    async fn hot_group_is_bounded_and_cannot_starve_a_healthy_group() {
        let runtime = SharedGroupRuntime::new(config(1, 4, 2)).unwrap();
        let hot = GroupKind::Data { shard_id: 0 };
        let healthy = GroupKind::Data { shard_id: 1 };
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let blocker = runtime
            .try_submit(hot, async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
            })
            .unwrap();
        started_rx.await.unwrap();

        let (order_tx, mut order_rx) = mpsc::unbounded_channel();
        let first_sender = order_tx.clone();
        let first_hot = runtime
            .try_submit(hot, async move {
                first_sender.send("hot-1").unwrap();
            })
            .unwrap();
        let second_sender = order_tx.clone();
        let second_hot = runtime
            .try_submit(hot, async move {
                second_sender.send("hot-2").unwrap();
            })
            .unwrap();
        assert!(matches!(
            runtime.try_submit(hot, async {}),
            Err(SharedGroupRuntimeError::QueueSaturated {
                scope: QueueSaturationScope::Group,
                ..
            })
        ));
        let healthy_sender = order_tx.clone();
        let healthy_receipt = runtime
            .try_submit(healthy, async move {
                healthy_sender.send("healthy").unwrap();
            })
            .unwrap();

        release_tx.send(()).unwrap();
        blocker.wait().await.unwrap();
        first_hot.wait().await.unwrap();
        healthy_receipt.wait().await.unwrap();
        second_hot.wait().await.unwrap();
        assert_eq!(order_rx.recv().await, Some("hot-1"));
        assert_eq!(order_rx.recv().await, Some("healthy"));
        assert_eq!(order_rx.recv().await, Some("hot-2"));
        let metrics = runtime.metrics();
        assert_eq!(metrics.group_rejections, 1);
        assert_eq!(metrics.completed, 4);
        runtime.close();
    }

    #[tokio::test]
    async fn global_queue_rejects_before_allocation_and_close_drains_admitted_work() {
        let runtime = SharedGroupRuntime::new(config(1, 1, 1)).unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let running = runtime
            .try_submit(GroupKind::PlacementCatalog, async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
            })
            .unwrap();
        started_rx.await.unwrap();
        let queued = runtime
            .try_submit(GroupKind::Data { shard_id: 3 }, async {})
            .unwrap();
        assert!(matches!(
            runtime.try_submit(GroupKind::Data { shard_id: 4 }, async {}),
            Err(SharedGroupRuntimeError::QueueSaturated {
                scope: QueueSaturationScope::Global,
                ..
            })
        ));
        runtime.close();
        assert!(matches!(
            runtime.try_submit(GroupKind::Data { shard_id: 5 }, async {}),
            Err(SharedGroupRuntimeError::Closed)
        ));
        release_tx.send(()).unwrap();
        running.wait().await.unwrap();
        queued.wait().await.unwrap();
        let metrics = runtime.metrics();
        assert!(metrics.closed);
        assert_eq!(metrics.global_rejections, 1);
        assert_eq!(metrics.queued, 0);
        assert_eq!(metrics.running, 0);
        assert_eq!(metrics.completed, 2);
        serde_json::to_string(&metrics).unwrap();
    }
}
