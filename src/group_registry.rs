use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_derive::{Deserialize, Serialize};

use crate::placement::{data_group_id, GroupId, PLACEMENT_CATALOG_GROUP_ID};
use crate::storage::LOGICAL_SHARD_COUNT;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GroupKind {
    PlacementCatalog,
    Data { shard_id: u16 },
}

impl GroupKind {
    pub fn group_id(self) -> Result<GroupId, GroupRegistryError> {
        match self {
            Self::PlacementCatalog => Ok(PLACEMENT_CATALOG_GROUP_ID),
            Self::Data { shard_id } => data_group_id(shard_id)
                .map_err(|_| GroupRegistryError::UnknownDataGroup { shard_id }),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupRegistryConfig {
    pub data_group_capacity: usize,
    pub memory_capacity_bytes: usize,
    pub per_group_event_capacity: usize,
    pub per_group_foreground_capacity: usize,
}

impl GroupRegistryConfig {
    pub fn validate(self) -> Result<Self, GroupRegistryError> {
        if self.data_group_capacity == 0
            || self.data_group_capacity > usize::from(LOGICAL_SHARD_COUNT)
        {
            return Err(GroupRegistryError::InvalidDataGroupCapacity {
                configured: self.data_group_capacity,
                maximum: usize::from(LOGICAL_SHARD_COUNT),
            });
        }
        if self.memory_capacity_bytes == 0 {
            return Err(GroupRegistryError::InvalidMemoryCapacity);
        }
        if self.per_group_event_capacity == 0 || self.per_group_foreground_capacity == 0 {
            return Err(GroupRegistryError::InvalidQueueCapacity);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupRegistryMetrics {
    pub data_group_capacity: usize,
    pub memory_capacity_bytes: usize,
    pub per_group_event_capacity: usize,
    pub per_group_foreground_capacity: usize,
    pub recovering_groups: usize,
    pub ready_catalog_groups: usize,
    pub ready_data_groups: usize,
    pub accounted_bytes: usize,
    pub recovery_attempts: u64,
    pub recovery_successes: u64,
    pub recovery_failures: u64,
    pub recovery_cancellations: u64,
    pub data_capacity_rejections: u64,
    pub memory_capacity_rejections: u64,
    pub removals: u64,
}

#[derive(Debug)]
pub enum RegistryAdmission<T> {
    Recovered(Arc<T>),
    AlreadyReady(Arc<T>),
}

impl<T> RegistryAdmission<T> {
    pub fn handle(&self) -> &Arc<T> {
        match self {
            Self::Recovered(handle) | Self::AlreadyReady(handle) => handle,
        }
    }

    pub fn was_recovered(&self) -> bool {
        matches!(self, Self::Recovered(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalOutcome {
    Removed,
    AlreadyAbsent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupRegistryError {
    InvalidDataGroupCapacity { configured: usize, maximum: usize },
    InvalidMemoryCapacity,
    InvalidQueueCapacity,
    InvalidAccountedBytes,
    UnknownDataGroup { shard_id: u16 },
    DataCapacityExceeded { capacity: usize },
    MemoryCapacityExceeded {
        capacity_bytes: usize,
        accounted_bytes: usize,
        requested_bytes: usize,
    },
    RecoveryInProgress { group_id: GroupId },
    RecoveryFailed { group_id: GroupId, message: String },
    InternalInvariant { group_id: GroupId },
}

impl fmt::Display for GroupRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDataGroupCapacity {
                configured,
                maximum,
            } => write!(
                f,
                "data-group capacity {configured} is outside 1..={maximum}"
            ),
            Self::InvalidMemoryCapacity => write!(f, "registry memory capacity must be non-zero"),
            Self::InvalidQueueCapacity => {
                write!(f, "per-group event and foreground capacities must be non-zero")
            }
            Self::InvalidAccountedBytes => {
                write!(f, "a group must reserve a non-zero accounted byte budget")
            }
            Self::UnknownDataGroup { shard_id } => {
                write!(f, "data group for logical shard {shard_id} does not exist")
            }
            Self::DataCapacityExceeded { capacity } => {
                write!(f, "data-group capacity {capacity} is exhausted")
            }
            Self::MemoryCapacityExceeded {
                capacity_bytes,
                accounted_bytes,
                requested_bytes,
            } => write!(
                f,
                "registry memory capacity {capacity_bytes} cannot admit {requested_bytes} bytes with {accounted_bytes} already reserved"
            ),
            Self::RecoveryInProgress { group_id } => {
                write!(f, "group {group_id} recovery is already in progress")
            }
            Self::RecoveryFailed { group_id, message } => {
                write!(f, "group {group_id} recovery failed: {message}")
            }
            Self::InternalInvariant { group_id } => {
                write!(f, "group {group_id} registry state changed during recovery")
            }
        }
    }
}

impl std::error::Error for GroupRegistryError {}

pub struct GroupRegistry<T> {
    inner: Arc<RegistryInner<T>>,
}

impl<T> Clone for GroupRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct RegistryInner<T> {
    config: GroupRegistryConfig,
    state: Mutex<RegistryState<T>>,
}

struct RegistryState<T> {
    entries: BTreeMap<GroupId, RegistryEntry<T>>,
    accounted_bytes: usize,
    next_reservation_id: u64,
    counters: RegistryCounters,
}

enum RegistryEntry<T> {
    Recovering {
        kind: GroupKind,
        accounted_bytes: usize,
        reservation_id: u64,
    },
    Ready {
        kind: GroupKind,
        accounted_bytes: usize,
        handle: Arc<T>,
    },
}

#[derive(Default)]
struct RegistryCounters {
    recovery_attempts: u64,
    recovery_successes: u64,
    recovery_failures: u64,
    recovery_cancellations: u64,
    data_capacity_rejections: u64,
    memory_capacity_rejections: u64,
    removals: u64,
}

struct RecoveryReservation<T> {
    inner: Arc<RegistryInner<T>>,
    group_id: GroupId,
    reservation_id: u64,
    active: bool,
}

impl<T> Drop for RecoveryReservation<T> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = lock_state(&self.inner.state);
        if state.remove_reservation(self.group_id, self.reservation_id) {
            state.counters.recovery_cancellations =
                state.counters.recovery_cancellations.saturating_add(1);
        }
    }
}

impl<T> RegistryState<T> {
    fn remove_reservation(&mut self, group_id: GroupId, reservation_id: u64) -> bool {
        let matches = matches!(
            self.entries.get(&group_id),
            Some(RegistryEntry::Recovering {
                reservation_id: current,
                ..
            }) if *current == reservation_id
        );
        if !matches {
            return false;
        }
        if let Some(RegistryEntry::Recovering {
            accounted_bytes, ..
        }) = self.entries.remove(&group_id)
        {
            self.accounted_bytes = self.accounted_bytes.saturating_sub(accounted_bytes);
        }
        true
    }
}

impl<T> GroupRegistry<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(config: GroupRegistryConfig) -> Result<Self, GroupRegistryError> {
        let config = config.validate()?;
        Ok(Self {
            inner: Arc::new(RegistryInner {
                config,
                state: Mutex::new(RegistryState {
                    entries: BTreeMap::new(),
                    accounted_bytes: 0,
                    next_reservation_id: 1,
                    counters: RegistryCounters::default(),
                }),
            }),
        })
    }

    pub async fn recover_or_get<F, Fut>(
        &self,
        kind: GroupKind,
        accounted_bytes: usize,
        recover: F,
    ) -> Result<RegistryAdmission<T>, GroupRegistryError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        if accounted_bytes == 0 {
            return Err(GroupRegistryError::InvalidAccountedBytes);
        }
        let group_id = kind.group_id()?;
        let reservation_id = {
            let mut state = lock_state(&self.inner.state);
            if let Some(entry) = state.entries.get(&group_id) {
                return match entry {
                    RegistryEntry::Ready {
                        kind: current_kind,
                        handle,
                        ..
                    } if *current_kind == kind => {
                        Ok(RegistryAdmission::AlreadyReady(Arc::clone(handle)))
                    }
                    RegistryEntry::Recovering { .. } => {
                        Err(GroupRegistryError::RecoveryInProgress { group_id })
                    }
                    RegistryEntry::Ready { .. } => {
                        Err(GroupRegistryError::InternalInvariant { group_id })
                    }
                };
            }

            if matches!(kind, GroupKind::Data { .. }) {
                let admitted_data_groups = state
                    .entries
                    .values()
                    .filter(|entry| match entry {
                        RegistryEntry::Recovering { kind, .. }
                        | RegistryEntry::Ready { kind, .. } => {
                            matches!(kind, GroupKind::Data { .. })
                        }
                    })
                    .count();
                if admitted_data_groups >= self.inner.config.data_group_capacity {
                    state.counters.data_capacity_rejections =
                        state.counters.data_capacity_rejections.saturating_add(1);
                    return Err(GroupRegistryError::DataCapacityExceeded {
                        capacity: self.inner.config.data_group_capacity,
                    });
                }
            }

            let next_accounted = state.accounted_bytes.checked_add(accounted_bytes);
            if next_accounted
                .map(|bytes| bytes > self.inner.config.memory_capacity_bytes)
                .unwrap_or(true)
            {
                state.counters.memory_capacity_rejections =
                    state.counters.memory_capacity_rejections.saturating_add(1);
                return Err(GroupRegistryError::MemoryCapacityExceeded {
                    capacity_bytes: self.inner.config.memory_capacity_bytes,
                    accounted_bytes: state.accounted_bytes,
                    requested_bytes: accounted_bytes,
                });
            }

            let reservation_id = state.next_reservation_id;
            state.next_reservation_id = state.next_reservation_id.wrapping_add(1).max(1);
            state.accounted_bytes = next_accounted.expect("checked above");
            state.counters.recovery_attempts =
                state.counters.recovery_attempts.saturating_add(1);
            state.entries.insert(
                group_id,
                RegistryEntry::Recovering {
                    kind,
                    accounted_bytes,
                    reservation_id,
                },
            );
            reservation_id
        };

        let mut reservation = RecoveryReservation {
            inner: Arc::clone(&self.inner),
            group_id,
            reservation_id,
            active: true,
        };

        let recovered = match recover().await {
            Ok(handle) => Arc::new(handle),
            Err(message) => {
                let mut state = lock_state(&self.inner.state);
                if state.remove_reservation(group_id, reservation_id) {
                    state.counters.recovery_failures =
                        state.counters.recovery_failures.saturating_add(1);
                }
                reservation.active = false;
                return Err(GroupRegistryError::RecoveryFailed { group_id, message });
            }
        };

        let mut state = lock_state(&self.inner.state);
        let valid_reservation = matches!(
            state.entries.get(&group_id),
            Some(RegistryEntry::Recovering {
                reservation_id: current,
                ..
            }) if *current == reservation_id
        );
        if !valid_reservation {
            reservation.active = false;
            return Err(GroupRegistryError::InternalInvariant { group_id });
        }
        state.entries.insert(
            group_id,
            RegistryEntry::Ready {
                kind,
                accounted_bytes,
                handle: Arc::clone(&recovered),
            },
        );
        state.counters.recovery_successes =
            state.counters.recovery_successes.saturating_add(1);
        reservation.active = false;
        Ok(RegistryAdmission::Recovered(recovered))
    }

    pub fn get(&self, kind: GroupKind) -> Result<Option<Arc<T>>, GroupRegistryError> {
        let group_id = kind.group_id()?;
        let state = lock_state(&self.inner.state);
        Ok(match state.entries.get(&group_id) {
            Some(RegistryEntry::Ready {
                kind: current_kind,
                handle,
                ..
            }) if *current_kind == kind => Some(Arc::clone(handle)),
            _ => None,
        })
    }

    pub fn remove(&self, kind: GroupKind) -> Result<RemovalOutcome, GroupRegistryError> {
        let group_id = kind.group_id()?;
        let mut state = lock_state(&self.inner.state);
        match state.entries.get(&group_id) {
            None => Ok(RemovalOutcome::AlreadyAbsent),
            Some(RegistryEntry::Recovering { .. }) => {
                Err(GroupRegistryError::RecoveryInProgress { group_id })
            }
            Some(RegistryEntry::Ready {
                kind: current_kind, ..
            }) if *current_kind != kind => Err(GroupRegistryError::InternalInvariant { group_id }),
            Some(RegistryEntry::Ready { .. }) => {
                if let Some(RegistryEntry::Ready {
                    accounted_bytes, ..
                }) = state.entries.remove(&group_id)
                {
                    state.accounted_bytes =
                        state.accounted_bytes.saturating_sub(accounted_bytes);
                }
                state.counters.removals = state.counters.removals.saturating_add(1);
                Ok(RemovalOutcome::Removed)
            }
        }
    }

    pub fn metrics(&self) -> GroupRegistryMetrics {
        let state = lock_state(&self.inner.state);
        let mut recovering_groups = 0;
        let mut ready_catalog_groups = 0;
        let mut ready_data_groups = 0;
        for entry in state.entries.values() {
            match entry {
                RegistryEntry::Recovering { .. } => recovering_groups += 1,
                RegistryEntry::Ready {
                    kind: GroupKind::PlacementCatalog,
                    ..
                } => ready_catalog_groups += 1,
                RegistryEntry::Ready {
                    kind: GroupKind::Data { .. },
                    ..
                } => ready_data_groups += 1,
            }
        }
        GroupRegistryMetrics {
            data_group_capacity: self.inner.config.data_group_capacity,
            memory_capacity_bytes: self.inner.config.memory_capacity_bytes,
            per_group_event_capacity: self.inner.config.per_group_event_capacity,
            per_group_foreground_capacity: self.inner.config.per_group_foreground_capacity,
            recovering_groups,
            ready_catalog_groups,
            ready_data_groups,
            accounted_bytes: state.accounted_bytes,
            recovery_attempts: state.counters.recovery_attempts,
            recovery_successes: state.counters.recovery_successes,
            recovery_failures: state.counters.recovery_failures,
            recovery_cancellations: state.counters.recovery_cancellations,
            data_capacity_rejections: state.counters.data_capacity_rejections,
            memory_capacity_rejections: state.counters.memory_capacity_rejections,
            removals: state.counters.removals,
        }
    }
}

fn lock_state<T>(mutex: &Mutex<RegistryState<T>>) -> MutexGuard<'_, RegistryState<T>> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    use super::*;

    fn config(data_group_capacity: usize, memory_capacity_bytes: usize) -> GroupRegistryConfig {
        GroupRegistryConfig {
            data_group_capacity,
            memory_capacity_bytes,
            per_group_event_capacity: 8,
            per_group_foreground_capacity: 4,
        }
    }

    async fn recover_value(
        registry: &GroupRegistry<String>,
        kind: GroupKind,
        bytes: usize,
        value: &str,
    ) -> RegistryAdmission<String> {
        let value = value.to_owned();
        registry
            .recover_or_get(kind, bytes, move || async move { Ok(value) })
            .await
            .unwrap()
    }

    #[test]
    fn configuration_and_unknown_groups_fail_closed() {
        assert!(matches!(
            GroupRegistry::<String>::new(config(0, 1)),
            Err(GroupRegistryError::InvalidDataGroupCapacity { .. })
        ));
        assert!(matches!(
            GroupRegistry::<String>::new(config(usize::from(LOGICAL_SHARD_COUNT) + 1, 1)),
            Err(GroupRegistryError::InvalidDataGroupCapacity { .. })
        ));
        assert!(matches!(
            GroupRegistry::<String>::new(config(1, 0)),
            Err(GroupRegistryError::InvalidMemoryCapacity)
        ));
        let registry = GroupRegistry::<String>::new(config(1, 16)).unwrap();
        assert_eq!(
            registry.get(GroupKind::Data {
                shard_id: LOGICAL_SHARD_COUNT
            }),
            Err(GroupRegistryError::UnknownDataGroup {
                shard_id: LOGICAL_SHARD_COUNT
            })
        );
        assert_eq!(registry.metrics().accounted_bytes, 0);
    }

    #[tokio::test]
    async fn catalog_has_a_separate_slot_and_capacity_rejection_allocates_nothing() {
        let registry = GroupRegistry::new(config(2, 40)).unwrap();
        recover_value(&registry, GroupKind::PlacementCatalog, 8, "catalog").await;
        recover_value(&registry, GroupKind::Data { shard_id: 0 }, 8, "zero").await;
        recover_value(&registry, GroupKind::Data { shard_id: 1 }, 8, "one").await;

        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&invoked);
        let result = registry
            .recover_or_get(GroupKind::Data { shard_id: 2 }, 8, move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok("two".to_owned()) }
            })
            .await;
        assert!(matches!(
            result,
            Err(GroupRegistryError::DataCapacityExceeded { capacity: 2 })
        ));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        let metrics = registry.metrics();
        assert_eq!(metrics.ready_catalog_groups, 1);
        assert_eq!(metrics.ready_data_groups, 2);
        assert_eq!(metrics.accounted_bytes, 24);
        assert_eq!(metrics.data_capacity_rejections, 1);
    }

    #[tokio::test]
    async fn group_is_not_served_until_recovery_completes() {
        let registry = GroupRegistry::new(config(1, 16)).unwrap();
        let worker = registry.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            worker
                .recover_or_get(GroupKind::Data { shard_id: 4 }, 8, || async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok("recovered".to_owned())
                })
                .await
        });

        started_rx.await.unwrap();
        assert!(registry
            .get(GroupKind::Data { shard_id: 4 })
            .unwrap()
            .is_none());
        let recovering = registry.metrics();
        assert_eq!(recovering.recovering_groups, 1);
        assert_eq!(recovering.ready_data_groups, 0);
        assert_eq!(
            registry
                .recover_or_get(GroupKind::Data { shard_id: 4 }, 8, || async {
                    Ok("duplicate".to_owned())
                })
                .await
                .unwrap_err(),
            GroupRegistryError::RecoveryInProgress { group_id: 5 }
        );

        release_tx.send(()).unwrap();
        let admission = task.await.unwrap().unwrap();
        assert!(admission.was_recovered());
        assert_eq!(
            registry
                .get(GroupKind::Data { shard_id: 4 })
                .unwrap()
                .as_ref()
                .map(|handle| handle.as_str()),
            Some("recovered")
        );
    }

    #[tokio::test]
    async fn failed_recovery_releases_capacity_and_retry_is_idempotent() {
        let registry = GroupRegistry::new(config(1, 8)).unwrap();
        let kind = GroupKind::Data { shard_id: 9 };
        let failed = registry
            .recover_or_get(kind, 8, || async { Err("corrupt durable state".to_owned()) })
            .await;
        assert_eq!(
            failed.unwrap_err(),
            GroupRegistryError::RecoveryFailed {
                group_id: 10,
                message: "corrupt durable state".to_owned()
            }
        );
        assert!(registry.get(kind).unwrap().is_none());
        assert_eq!(registry.metrics().accounted_bytes, 0);

        let recovered = recover_value(&registry, kind, 8, "healthy").await;
        let already = recover_value(&registry, kind, 8, "must-not-replace").await;
        assert!(matches!(recovered, RegistryAdmission::Recovered(_)));
        assert!(matches!(already, RegistryAdmission::AlreadyReady(_)));
        assert!(Arc::ptr_eq(recovered.handle(), already.handle()));
        assert_eq!(already.handle().as_str(), "healthy");
        let metrics = registry.metrics();
        assert_eq!(metrics.recovery_attempts, 2);
        assert_eq!(metrics.recovery_failures, 1);
        assert_eq!(metrics.recovery_successes, 1);
    }

    #[tokio::test]
    async fn memory_rejection_and_remove_restart_return_to_baseline() {
        let registry = GroupRegistry::new(config(2, 8)).unwrap();
        let first = GroupKind::Data { shard_id: 12 };
        let second = GroupKind::Data { shard_id: 13 };
        recover_value(&registry, first, 8, "first").await;

        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&invoked);
        let rejected = registry
            .recover_or_get(second, 1, move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok("second".to_owned()) }
            })
            .await;
        assert!(matches!(
            rejected,
            Err(GroupRegistryError::MemoryCapacityExceeded { .. })
        ));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert_eq!(registry.remove(first).unwrap(), RemovalOutcome::Removed);
        assert_eq!(
            registry.remove(first).unwrap(),
            RemovalOutcome::AlreadyAbsent
        );
        assert_eq!(registry.metrics().accounted_bytes, 0);

        let restarted = recover_value(&registry, first, 8, "restarted").await;
        assert!(restarted.was_recovered());
        assert_eq!(restarted.handle().as_str(), "restarted");
        assert_eq!(registry.remove(first).unwrap(), RemovalOutcome::Removed);
        let final_metrics = registry.metrics();
        assert_eq!(final_metrics.accounted_bytes, 0);
        assert_eq!(final_metrics.ready_data_groups, 0);
        assert_eq!(final_metrics.memory_capacity_rejections, 1);
        assert_eq!(final_metrics.removals, 2);
    }

    #[tokio::test]
    async fn cancelled_recovery_releases_reserved_slot_and_memory() {
        let registry = GroupRegistry::<String>::new(config(1, 8)).unwrap();
        let worker = registry.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            worker
                .recover_or_get(GroupKind::Data { shard_id: 20 }, 8, || async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.map_err(|error| error.to_string())?;
                    Ok("never".to_owned())
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let metrics = registry.metrics();
        assert_eq!(metrics.recovering_groups, 0);
        assert_eq!(metrics.accounted_bytes, 0);
        assert_eq!(metrics.recovery_cancellations, 1);
        let retried = recover_value(
            &registry,
            GroupKind::Data { shard_id: 20 },
            8,
            "retried",
        )
        .await;
        assert!(retried.was_recovered());
    }

    #[tokio::test]
    async fn multi_group_restart_recovery_is_isolated_and_recover_before_serve() {
        let registry = GroupRegistry::new(config(3, 32)).unwrap();
        let first = GroupKind::Data { shard_id: 30 };
        let second = GroupKind::Data { shard_id: 31 };
        let corrupt = GroupKind::Data { shard_id: 32 };

        recover_value(&registry, first, 8, "first-v1").await;
        recover_value(&registry, second, 8, "second-v1").await;
        assert_eq!(registry.remove(first).unwrap(), RemovalOutcome::Removed);
        assert_eq!(registry.remove(second).unwrap(), RemovalOutcome::Removed);
        assert_eq!(registry.metrics().accounted_bytes, 0);

        let worker = registry.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let first_restart = tokio::spawn(async move {
            worker
                .recover_or_get(first, 8, || async move {
                    started_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok("first-v2".to_owned())
                })
                .await
        });

        started_rx.await.unwrap();
        assert!(registry.get(first).unwrap().is_none());
        let during_first_restart = registry.metrics();
        assert_eq!(during_first_restart.recovering_groups, 1);
        assert_eq!(during_first_restart.ready_data_groups, 0);
        assert_eq!(during_first_restart.accounted_bytes, 8);

        let second_restart = recover_value(&registry, second, 8, "second-v2").await;
        assert!(second_restart.was_recovered());
        assert_eq!(second_restart.handle().as_str(), "second-v2");
        assert_eq!(
            registry
                .get(second)
                .unwrap()
                .as_ref()
                .map(|handle| handle.as_str()),
            Some("second-v2")
        );
        assert!(registry.get(first).unwrap().is_none());

        let failed = registry
            .recover_or_get(corrupt, 8, || async {
                Err("corrupt group snapshot".to_owned())
            })
            .await;
        assert_eq!(
            failed.unwrap_err(),
            GroupRegistryError::RecoveryFailed {
                group_id: 33,
                message: "corrupt group snapshot".to_owned(),
            }
        );
        assert!(registry.get(corrupt).unwrap().is_none());
        assert_eq!(
            registry
                .get(second)
                .unwrap()
                .as_ref()
                .map(|handle| handle.as_str()),
            Some("second-v2")
        );

        release_tx.send(()).unwrap();
        let first_restart = first_restart.await.unwrap().unwrap();
        assert!(first_restart.was_recovered());
        assert_eq!(first_restart.handle().as_str(), "first-v2");

        let ready = registry.metrics();
        assert_eq!(ready.recovering_groups, 0);
        assert_eq!(ready.ready_data_groups, 2);
        assert_eq!(ready.accounted_bytes, 16);
        assert_eq!(ready.recovery_failures, 1);

        assert_eq!(registry.remove(first).unwrap(), RemovalOutcome::Removed);
        assert_eq!(registry.remove(second).unwrap(), RemovalOutcome::Removed);
        let final_metrics = registry.metrics();
        assert_eq!(final_metrics.ready_data_groups, 0);
        assert_eq!(final_metrics.recovering_groups, 0);
        assert_eq!(final_metrics.accounted_bytes, 0);
    }

}
