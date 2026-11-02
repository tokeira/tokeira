//! Complete activity intent derived from committed run state.
//!
//! Rows contain scheduling/routing metadata only. Discovery reads them without
//! claiming work; the execution owner alone can start an activity. Checked
//! derivation never uses old rows to fill missing authoritative information.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_kernel::{Priority, WorkflowState, merge_priority};
use tokeira_types::{
    ExecutionStatus, NamespaceId, QueueKey, RunKey, ShardId, TaskKind, TaskQueueName,
    TransitionSeq, dsql_spread_uuid,
};
use uuid::Uuid;

use crate::{
    DispatchableActivityTask,
    dispatch_key::{dispatch_timestamp, lookup_digest},
};

/// Maximum physical keys examined by one activity read or repair page.
pub const ACTIVITY_DISPATCH_PAGE_LIMIT: u32 = 64;

pub(crate) fn activity_start_observation_matches(
    observed: &WorkflowState,
    current: &WorkflowState,
    activity_id: &str,
) -> bool {
    let Some(activity) = observed.activities.get(activity_id) else {
        return false;
    };
    let Some(receipt) = observed.activity_start_receipts.get(activity_id) else {
        return false;
    };
    // Include lineage and the exact receipt, not just a sequence: even a bad
    // delete/recreate fixture must not authorize a different run incarnation.
    observed.is_open()
        && activity.started_at.is_some()
        && activity.started_event_id.is_some()
        && observed.run_key == current.run_key
        && observed.run_id == current.run_id
        && observed.namespace_id == current.namespace_id
        && observed.workflow_id == current.workflow_id
        && observed.transition_seq == current.transition_seq
        && observed.status == current.status
        && current.activities.get(activity_id) == Some(activity)
        && current.activity_start_receipts.get(activity_id) == Some(receipt)
}

/// A repair transaction lost OCC; retry with fresh authority and the same fence.
#[derive(Debug, thiserror::Error)]
#[error("activity dispatch repair serialization conflict")]
pub struct ActivityDispatchRepairConflict;

/// Invalid authority or derived data; callers must fail rather than guess a row.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid activity dispatch for run {run_key:?}, activity {activity_id:?}: {reason}")]
pub struct ActivityDispatchDataError {
    /// Run whose intent cannot be interpreted safely.
    pub run_key: RunKey,
    /// Raw activity identifier, retained for operator diagnosis.
    pub activity_id: String,
    /// The invalid invariant, without payload or credential data.
    pub reason: String,
}

/// Complete incarnation, including activity-ID reuse within the same run.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActivityIncarnation {
    /// Owning execution.
    pub run_key: RunKey,
    /// Application identifier within the run.
    pub activity_id: String,
    /// Original schedule distinguishes reuse from a retry of the same activity.
    pub schedule_event_id: i64,
    /// Positive current attempt.
    pub attempt: u32,
    /// Control generation invalidating superseded offers.
    pub stamp: u64,
}

/// Authoritative metadata projected into the disposable dispatch index.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityDispatchRow {
    /// Stable physical slot; raw coordinates must also agree before replacement.
    pub key: Uuid,
    /// Current startable incarnation.
    pub incarnation: ActivityIncarnation,
    /// Home owning authoritative transitions, independent of queue placement.
    pub execution_home: ShardId,
    /// Committed source routing; runtime can select a newer registry target.
    pub queue: QueueKey,
    /// Bounded lookup digest. Never substitutes for raw queue comparison.
    pub queue_key: String,
    /// Fieldwise workflow/activity priority inheritance.
    pub priority: Option<Priority>,
    /// Committed workflow routing revision.
    pub dispatch_revision: i64,
    /// Original attempt eligibility, normalized to database timestamp precision.
    pub dispatch_at: OffsetDateTime,
}

impl ActivityDispatchRow {
    /// Conservative write accounting, including scalar/index overhead. This
    /// measures a row without imposing the service's physical limits on memory.
    pub(crate) fn write_cost(&self) -> anyhow::Result<crate::write_budget::WriteCost> {
        let priority = self
            .priority
            .as_ref()
            .map(postcard::to_allocvec)
            .transpose()?;
        Ok(crate::write_budget::WriteCost::write(
            4096 + self.incarnation.activity_id.len()
                + self.queue.task_queue.0.len()
                + self.queue.deployment.as_ref().map_or(0, |v| v.0.len())
                + self.queue.build_id.as_ref().map_or(0, |v| v.0.len())
                + priority.as_ref().map_or(0, Vec::len),
        ))
    }

    /// Total due-range position, independent of diagnostic insertion time.
    #[must_use]
    pub fn position(&self) -> ActivityDispatchPosition {
        ActivityDispatchPosition {
            dispatch_at: self.dispatch_at,
            key: self.key,
        }
    }

    /// Produce a volatile source offer without inputs, headers or credentials.
    #[must_use]
    pub fn task(&self) -> DispatchableActivityTask {
        DispatchableActivityTask {
            run_key: self.incarnation.run_key,
            queue: self.queue.clone(),
            activity_id: self.incarnation.activity_id.clone(),
            schedule_event_id: self.incarnation.schedule_event_id,
            attempt: self.incarnation.attempt,
            stamp: self.incarnation.stamp,
            dispatch_revision: self.dispatch_revision,
            priority: self.priority.clone(),
            order: None,
        }
    }

    /// Reject malformed scalars and hash/coordinate mismatches before use.
    pub fn validate(&self) -> Result<(), ActivityDispatchDataError> {
        let invalid = |reason: &str| ActivityDispatchDataError {
            run_key: self.incarnation.run_key,
            activity_id: self.incarnation.activity_id.clone(),
            reason: reason.into(),
        };
        if self.incarnation.activity_id.is_empty() || self.incarnation.schedule_event_id <= 0 {
            return Err(invalid(
                "activity identity requires a nonempty ID and positive schedule",
            ));
        }
        if self.incarnation.attempt == 0 || i32::try_from(self.incarnation.attempt).is_err() {
            return Err(invalid("attempt must be positive and fit INTEGER"));
        }
        if i64::try_from(self.incarnation.stamp).is_err() {
            return Err(invalid("stamp does not fit BIGINT"));
        }
        if self.key
            != activity_dispatch_key(self.incarnation.run_key, &self.incarnation.activity_id)
        {
            return Err(invalid(
                "slot key disagrees with raw run/activity coordinates",
            ));
        }
        if self.queue.task_kind != TaskKind::Activity
            || self.queue_key != lookup_digest(b"queue", Some(&self.queue.task_queue.0))
        {
            return Err(invalid(
                "queue lookup key disagrees with raw activity queue",
            ));
        }
        Ok(())
    }
}

/// One queue family shared by every worker deployment/build demand.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActivityDiscoveryRange {
    /// Namespace authority, not a display name.
    pub namespace_id: NamespaceId,
    /// Full raw name retained to reject digest collisions.
    pub queue_name: TaskQueueName,
}

impl ActivityDiscoveryRange {
    /// Bounded equality key used by both backend implementations.
    #[must_use]
    pub fn lookup_key(&self) -> String {
        lookup_digest(b"queue", Some(&self.queue_name.0))
    }

    /// Check raw coordinates after a digest lookup, independent of worker version.
    #[must_use]
    pub fn matches(&self, row: &ActivityDispatchRow) -> bool {
        row.queue.namespace_id == self.namespace_id && row.queue.task_queue == self.queue_name
    }
}

/// Exclusive continuation in a fixed-cutoff due pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActivityDispatchPosition {
    /// Database-normalized eligibility time.
    pub dispatch_at: OffsetDateTime,
    /// UUID tie-breaker; remains usable after deletion of the observed row.
    pub key: Uuid,
}

/// Complete home traversal includes NULL-digest legacy rows and orphan slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActivityHomePosition {
    /// Run prefix within the home.
    pub run_key: RunKey,
    /// Physical row tie-breaker.
    pub key: Uuid,
}

/// A payload decode failure retains its valid ordering position for progress.
#[derive(Debug)]
pub struct ActivityDispatchCandidate {
    /// Decoded before any fallible non-key metadata.
    pub position: ActivityDispatchPosition,
    /// Checked intent, or an explicit per-row error to retry on a later pass.
    pub row: anyhow::Result<ActivityDispatchRow>,
}

/// Read-only due slice; exhaustion counts physical keys, including collisions.
#[derive(Debug, Default)]
pub struct ActivityDispatchPage {
    /// At most the requested bounded physical candidates.
    pub candidates: Vec<ActivityDispatchCandidate>,
    /// Last examined key even if its payload was malformed or incompatible.
    pub last_examined: Option<ActivityDispatchPosition>,
    /// True when fewer than the effective limit were observed.
    pub exhausted: bool,
}

/// Read-only home slice with no requirement that row payloads decode.
#[derive(Clone, Debug, Default)]
pub struct ActivityHomePage {
    /// Complete physical positions for repair's second walk.
    pub candidates: Vec<ActivityHomePosition>,
    /// Last examined position.
    pub last_examined: Option<ActivityHomePosition>,
    /// True at the end of the home prefix.
    pub exhausted: bool,
}

/// Repair first fills wanted slots, then removes/replaces stale existing slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivityRepairPhase {
    /// Enumerate the fresh authoritative desired set.
    Wanted,
    /// Enumerate every existing physical slot for the run.
    Existing,
}

/// A run-local continuation; a changed sequence restarts the wanted phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivityRepairCursor {
    /// Current phase within the run.
    pub phase: ActivityRepairPhase,
    /// Last actually examined key, not the end of an unprocessed fetched page.
    pub after: Option<Uuid>,
    /// None denotes an absent run, not an assumed initial sequence.
    pub observed_seq: Option<TransitionSeq>,
}

impl ActivityRepairCursor {
    pub(crate) fn resume(after: Option<Self>, observed_seq: Option<TransitionSeq>) -> Self {
        after
            .filter(|cursor| cursor.observed_seq == observed_seq)
            .unwrap_or(Self {
                phase: if observed_seq.is_some() {
                    ActivityRepairPhase::Wanted
                } else {
                    ActivityRepairPhase::Existing
                },
                after: None,
                observed_seq,
            })
    }
}

/// A row larger than the repair work budget cannot be retried forever at the
/// same cursor. Refuse it explicitly; the caller leaves the home non-serving.
pub(crate) fn activity_repair_fits(
    bytes: usize,
    cost: crate::write_budget::WriteCost,
) -> anyhow::Result<bool> {
    let limit = crate::write_budget::MAX_BYTES_PER_TRANSACTION;
    anyhow::ensure!(
        cost.bytes <= limit,
        "activity dispatch row exceeds the bounded repair byte budget"
    );
    Ok(bytes.saturating_add(cost.bytes) <= limit)
}

pub(crate) fn activity_repair_continuation(
    cursor: ActivityRepairCursor,
    exhausted: bool,
) -> Option<ActivityRepairCursor> {
    if !exhausted {
        return Some(cursor);
    }
    match cursor.phase {
        ActivityRepairPhase::Wanted => Some(ActivityRepairCursor {
            phase: ActivityRepairPhase::Existing,
            after: None,
            ..cursor
        }),
        ActivityRepairPhase::Existing => None,
    }
}

/// Result of one fenced bounded repair transaction; never grants serving rights.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityRepairPage {
    /// Next slice, or None when this run is completely reconciled.
    pub continuation: Option<ActivityRepairCursor>,
    /// Physical or wanted keys examined, including unchanged rows.
    pub examined: usize,
    /// Rows inserted, replaced or removed by this transaction.
    pub changed: usize,
}

/// Preserve the existing durable activity slot encoding across the cutover.
#[must_use]
pub fn activity_dispatch_key(run_key: RunKey, activity_id: &str) -> Uuid {
    dsql_spread_uuid(&[
        b"activity-dispatch",
        run_key.0.as_bytes(),
        activity_id.as_bytes(),
    ])
}

/// Derive the entire wanted set without I/O, old rows or wall-clock defaulting.
pub fn derive_activity_dispatch(
    state: &WorkflowState,
    home: ShardId,
) -> Result<BTreeMap<Uuid, ActivityDispatchRow>, ActivityDispatchDataError> {
    let mut rows = BTreeMap::new();
    if state.status != ExecutionStatus::Running {
        return Ok(rows);
    }
    for (id, activity) in &state.activities {
        let invalid = |reason: &str| ActivityDispatchDataError {
            run_key: state.run_key,
            activity_id: id.clone(),
            reason: reason.into(),
        };
        if activity.started_at.is_some() != activity.started_event_id.is_some() {
            return Err(invalid("started timestamp and event markers disagree"));
        }
        if activity.started_at.is_some() || activity.pause_info.is_some() {
            continue;
        }
        if id != &activity.activity_id {
            return Err(invalid("activity map key disagrees with its identity"));
        }
        let scheduled = activity
            .current_attempt_scheduled_at
            .ok_or_else(|| invalid("unstarted activity has no current attempt schedule"))?;
        let row = ActivityDispatchRow {
            key: activity_dispatch_key(state.run_key, id),
            incarnation: ActivityIncarnation {
                run_key: state.run_key,
                activity_id: id.clone(),
                schedule_event_id: activity.schedule_event_id,
                attempt: activity.attempt,
                stamp: activity.stamp,
            },
            execution_home: home,
            queue: QueueKey {
                namespace_id: state.namespace_id,
                task_queue: activity.task_queue.clone(),
                task_kind: TaskKind::Activity,
                deployment: activity
                    .deployment
                    .clone()
                    .or_else(|| state.deployment.clone()),
                build_id: activity.build_id.clone().or_else(|| state.build_id.clone()),
            },
            queue_key: lookup_digest(b"queue", Some(&activity.task_queue.0)),
            priority: merge_priority(state.priority.as_ref(), activity.priority.as_ref()),
            dispatch_revision: state
                .versioning_info
                .as_ref()
                .map_or(0, |info| info.revision_number),
            dispatch_at: dispatch_timestamp(scheduled),
        };
        row.validate()?;
        if rows.insert(row.key, row).is_some() {
            return Err(invalid(
                "distinct activities collide on the durable slot key",
            ));
        }
    }
    Ok(rows)
}
