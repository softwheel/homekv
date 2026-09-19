//! Per-group bounded admission for foreground operations and group events.
//!
//! Traceability:
//!
//! - `REQ-M4-GROUP-004`: foreground operations and group events each get an
//!   explicit configurable per-group count bound with observable
//!   overload/rejection. This makes the
//!   `GroupRegistryConfig::{per_group_foreground_capacity,
//!   per_group_event_capacity}` bounds declared by the registry real: they
//!   were validated and reported before, but nothing enforced them.
//! - `REQ-M4-GROUP-006`: admission is scoped per group, so a hot group that
//!   exhausts its own bound cannot create an unbounded queue and cannot
//!   prevent an unrelated healthy group from admitting work.
//!
//! Admission is permit-based, mirroring `SharedPeerTransport`: a successful
//! `try_admit` returns a `GroupAdmissionPermit` that holds one in-flight
//! slot; dropping the permit releases it. The `GroupRegistry` owns one
//! `GroupAdmission` and only admits foreground/event work for groups it has
//! already admitted, so unknown or removed groups fail closed.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_derive::{Deserialize, Serialize};

use crate::placement::GroupId;

/// Which per-group admission scope a permit belongs to.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GroupAdmissionScope {
    Foreground,
    Event,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PerGroupAdmissionConfig {
    pub per_group_foreground_capacity: usize,
    pub per_group_event_capacity: usize,
}

impl PerGroupAdmissionConfig {
    pub fn validate(self) -> Result<Self, GroupAdmissionError> {
        if self.per_group_foreground_capacity == 0 {
            return Err(GroupAdmissionError::InvalidForegroundCapacity);
        }
        if self.per_group_event_capacity == 0 {
            return Err(GroupAdmissionError::InvalidEventCapacity);
        }
        Ok(self)
    }

    fn capacity_for(&self, scope: GroupAdmissionScope) -> usize {
        match scope {
            GroupAdmissionScope::Foreground => self.per_group_foreground_capacity,
            GroupAdmissionScope::Event => self.per_group_event_capacity,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupAdmissionError {
    InvalidForegroundCapacity,
    InvalidEventCapacity,
    Saturated {
        group_id: GroupId,
        scope: GroupAdmissionScope,
    },
}

impl fmt::Display for GroupAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidForegroundCapacity => {
                write!(f, "per-group foreground capacity must be non-zero")
            }
            Self::InvalidEventCapacity => {
                write!(f, "per-group event capacity must be non-zero")
            }
            Self::Saturated { group_id, scope } => write!(
                f,
                "per-group {scope:?} admission is saturated for group {group_id}"
            ),
        }
    }
}

impl std::error::Error for GroupAdmissionError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AdmissionScopeMetrics {
    pub capacity: usize,
    pub inflight: usize,
    pub peak_inflight: usize,
    pub attempts: u64,
    pub rejections: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GroupAdmissionMetrics {
    pub group_id: GroupId,
    pub foreground: AdmissionScopeMetrics,
    pub event: AdmissionScopeMetrics,
}

#[derive(Clone)]
pub struct GroupAdmission {
    inner: Arc<AdmissionInner>,
}

struct AdmissionInner {
    config: PerGroupAdmissionConfig,
    state: Mutex<BTreeMap<GroupId, MutableGroupAdmission>>,
}

#[derive(Default)]
struct MutableGroupAdmission {
    foreground_inflight: usize,
    foreground_peak_inflight: usize,
    foreground_attempts: u64,
    foreground_rejections: u64,
    event_inflight: usize,
    event_peak_inflight: usize,
    event_attempts: u64,
    event_rejections: u64,
}

impl MutableGroupAdmission {
    fn inflight(&self, scope: GroupAdmissionScope) -> usize {
        match scope {
            GroupAdmissionScope::Foreground => self.foreground_inflight,
            GroupAdmissionScope::Event => self.event_inflight,
        }
    }

    fn record_attempt(&mut self, scope: GroupAdmissionScope) {
        match scope {
            GroupAdmissionScope::Foreground => {
                self.foreground_attempts = self.foreground_attempts.saturating_add(1);
            }
            GroupAdmissionScope::Event => {
                self.event_attempts = self.event_attempts.saturating_add(1);
            }
        }
    }

    fn record_rejection(&mut self, scope: GroupAdmissionScope) {
        match scope {
            GroupAdmissionScope::Foreground => {
                self.foreground_rejections = self.foreground_rejections.saturating_add(1);
            }
            GroupAdmissionScope::Event => {
                self.event_rejections = self.event_rejections.saturating_add(1);
            }
        }
    }

    fn admit(&mut self, scope: GroupAdmissionScope) {
        match scope {
            GroupAdmissionScope::Foreground => {
                self.foreground_inflight += 1;
                self.foreground_peak_inflight =
                    self.foreground_peak_inflight.max(self.foreground_inflight);
            }
            GroupAdmissionScope::Event => {
                self.event_inflight += 1;
                self.event_peak_inflight = self.event_peak_inflight.max(self.event_inflight);
            }
        }
    }

    fn release(&mut self, scope: GroupAdmissionScope) {
        match scope {
            GroupAdmissionScope::Foreground => {
                self.foreground_inflight = self.foreground_inflight.saturating_sub(1);
            }
            GroupAdmissionScope::Event => {
                self.event_inflight = self.event_inflight.saturating_sub(1);
            }
        }
    }
}

impl GroupAdmission {
    pub fn new(config: PerGroupAdmissionConfig) -> Result<Self, GroupAdmissionError> {
        let config = config.validate()?;
        Ok(Self {
            inner: Arc::new(AdmissionInner {
                config,
                state: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Admit one in-flight unit of `scope` work for `group_id`.
    ///
    /// Returns a permit holding the slot; dropping the permit releases it.
    /// A saturated group fails with an explicit `Saturated` error and an
    /// observable rejection counter; other groups are unaffected.
    pub fn try_admit(
        &self,
        scope: GroupAdmissionScope,
        group_id: GroupId,
    ) -> Result<GroupAdmissionPermit, GroupAdmissionError> {
        let mut state = lock_state(&self.inner.state);
        let group = state.entry(group_id).or_default();
        group.record_attempt(scope);
        let capacity = self.inner.config.capacity_for(scope);
        if group.inflight(scope) >= capacity {
            group.record_rejection(scope);
            return Err(GroupAdmissionError::Saturated { group_id, scope });
        }
        group.admit(scope);
        Ok(GroupAdmissionPermit {
            inner: Arc::clone(&self.inner),
            group_id,
            scope,
        })
    }

    /// Forget all admission state for `group_id`.
    ///
    /// Called when the owning registry removes the group so a later
    /// re-admission starts from clean counters.
    pub fn reset_group(&self, group_id: GroupId) {
        let mut state = lock_state(&self.inner.state);
        state.remove(&group_id);
    }

    pub fn group_metrics(&self, group_id: GroupId) -> Option<GroupAdmissionMetrics> {
        let state = lock_state(&self.inner.state);
        state
            .get(&group_id)
            .map(|group| snapshot_group_metrics(&self.inner.config, group_id, group))
    }

    pub fn metrics(&self) -> Vec<GroupAdmissionMetrics> {
        let state = lock_state(&self.inner.state);
        state
            .iter()
            .map(|(&group_id, group)| snapshot_group_metrics(&self.inner.config, group_id, group))
            .collect()
    }

    /// Aggregate rejection counts across all groups, for registry rollups.
    pub fn total_rejections(&self, scope: GroupAdmissionScope) -> u64 {
        let state = lock_state(&self.inner.state);
        state
            .values()
            .map(|group| match scope {
                GroupAdmissionScope::Foreground => group.foreground_rejections,
                GroupAdmissionScope::Event => group.event_rejections,
            })
            .sum()
    }
}

pub struct GroupAdmissionPermit {
    inner: Arc<AdmissionInner>,
    group_id: GroupId,
    scope: GroupAdmissionScope,
}

impl fmt::Debug for GroupAdmissionPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupAdmissionPermit")
            .field("group_id", &self.group_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl Drop for GroupAdmissionPermit {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner.state);
        if let Some(group) = state.get_mut(&self.group_id) {
            group.release(self.scope);
        }
    }
}

fn snapshot_group_metrics(
    config: &PerGroupAdmissionConfig,
    group_id: GroupId,
    group: &MutableGroupAdmission,
) -> GroupAdmissionMetrics {
    let scope_metrics = |scope: GroupAdmissionScope| AdmissionScopeMetrics {
        capacity: config.capacity_for(scope),
        inflight: group.inflight(scope),
        peak_inflight: match scope {
            GroupAdmissionScope::Foreground => group.foreground_peak_inflight,
            GroupAdmissionScope::Event => group.event_peak_inflight,
        },
        attempts: match scope {
            GroupAdmissionScope::Foreground => group.foreground_attempts,
            GroupAdmissionScope::Event => group.event_attempts,
        },
        rejections: match scope {
            GroupAdmissionScope::Foreground => group.foreground_rejections,
            GroupAdmissionScope::Event => group.event_rejections,
        },
    };
    GroupAdmissionMetrics {
        group_id,
        foreground: scope_metrics(GroupAdmissionScope::Foreground),
        event: scope_metrics(GroupAdmissionScope::Event),
    }
}

fn lock_state(
    mutex: &Mutex<BTreeMap<GroupId, MutableGroupAdmission>>,
) -> MutexGuard<'_, BTreeMap<GroupId, MutableGroupAdmission>> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(foreground: usize, event: usize) -> PerGroupAdmissionConfig {
        PerGroupAdmissionConfig {
            per_group_foreground_capacity: foreground,
            per_group_event_capacity: event,
        }
    }

    #[test]
    fn capacities_are_explicit_and_non_zero() {
        assert!(matches!(
            config(0, 1).validate(),
            Err(GroupAdmissionError::InvalidForegroundCapacity)
        ));
        assert!(matches!(
            config(1, 0).validate(),
            Err(GroupAdmissionError::InvalidEventCapacity)
        ));
    }

    #[test]
    fn saturation_rejects_explicitly_and_releases_on_drop() {
        let admission = GroupAdmission::new(config(1, 2)).unwrap();
        let first = admission
            .try_admit(GroupAdmissionScope::Foreground, 7)
            .unwrap();
        assert!(matches!(
            admission.try_admit(GroupAdmissionScope::Foreground, 7),
            Err(GroupAdmissionError::Saturated {
                group_id: 7,
                scope: GroupAdmissionScope::Foreground,
            })
        ));
        // The event scope has its own independent bound.
        let _event = admission.try_admit(GroupAdmissionScope::Event, 7).unwrap();
        let metrics = admission.group_metrics(7).unwrap();
        assert_eq!(metrics.foreground.inflight, 1);
        assert_eq!(metrics.foreground.attempts, 2);
        assert_eq!(metrics.foreground.rejections, 1);
        assert_eq!(metrics.event.inflight, 1);
        assert_eq!(metrics.event.rejections, 0);

        drop(first);
        let _second = admission
            .try_admit(GroupAdmissionScope::Foreground, 7)
            .unwrap();
        let metrics = admission.group_metrics(7).unwrap();
        assert_eq!(metrics.foreground.inflight, 1);
        assert_eq!(metrics.foreground.peak_inflight, 1);
        serde_json::to_string(&metrics).unwrap();
    }

    #[test]
    fn hot_group_cannot_block_an_unrelated_healthy_group() {
        let admission = GroupAdmission::new(config(1, 1)).unwrap();
        let _hot_foreground = admission
            .try_admit(GroupAdmissionScope::Foreground, 1)
            .unwrap();
        let _hot_event = admission.try_admit(GroupAdmissionScope::Event, 1).unwrap();
        assert!(matches!(
            admission.try_admit(GroupAdmissionScope::Foreground, 1),
            Err(GroupAdmissionError::Saturated { .. })
        ));
        assert!(matches!(
            admission.try_admit(GroupAdmissionScope::Event, 1),
            Err(GroupAdmissionError::Saturated { .. })
        ));

        // The healthy group admits both scopes independently.
        let _healthy_foreground = admission
            .try_admit(GroupAdmissionScope::Foreground, 2)
            .unwrap();
        let _healthy_event = admission.try_admit(GroupAdmissionScope::Event, 2).unwrap();
        let metrics = admission.group_metrics(2).unwrap();
        assert_eq!(metrics.foreground.inflight, 1);
        assert_eq!(metrics.event.inflight, 1);
        assert_eq!(metrics.foreground.rejections, 0);
        assert_eq!(
            admission.total_rejections(GroupAdmissionScope::Foreground),
            1
        );
        assert_eq!(admission.total_rejections(GroupAdmissionScope::Event), 1);
    }

    #[test]
    fn reset_group_starts_counters_clean() {
        let admission = GroupAdmission::new(config(1, 1)).unwrap();
        let _permit = admission
            .try_admit(GroupAdmissionScope::Foreground, 9)
            .unwrap();
        admission.reset_group(9);
        assert!(admission.group_metrics(9).is_none());
        assert_eq!(
            admission.total_rejections(GroupAdmissionScope::Foreground),
            0
        );
        admission
            .try_admit(GroupAdmissionScope::Foreground, 9)
            .unwrap();
    }
}
