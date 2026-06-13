//! The activity tasks and their validators (Requirement 11.7).
//!
//! Ground truth: `chasm/lib/activity/activity_tasks.go` and the task scheduling in
//! `statemachine.go @ v1.31.0`. An activity schedules:
//!
//! - one **side-effect** task, [`DispatchTask`], that enqueues the activity to
//!   matching (the engine's dispatch sink), and
//! - the **pure** timer tasks [`ScheduleToStartTimer`], [`ScheduleToCloseTimer`],
//!   [`StartToCloseTimer`], and [`HeartbeatTimer`].
//!
//! Every task is **stamp-fenced**: it carries the attempt `stamp` it was scheduled
//! for, and its validator drops it (validate-then-drop) when the live attempt has
//! advanced past that stamp or the activity has left the state the task applies to
//! (Requirement 11.6, 11.7; `tokeira_chasm` Property 5). The validators are pure
//! and component-typed; the runtime wires them into the transition-close
//! re-validation loop.
//!
//! Each task type has a stable `u32` id the engine threads through
//! `MutableContext::add_task`; the activity library owns this small id space.

use serde::{Deserialize, Serialize};
use tokeira_chasm::{ChasmError, Context, Task, TaskKind, TaskValidator, TaskValidity};

use crate::{component::ActivityExecution, state::ActivityStatus};

/// Registry id of the [`DispatchTask`] side-effect task.
pub const DISPATCH_TASK_ID: u32 = 1;
/// Registry id of the [`ScheduleToStartTimer`] pure task.
pub const SCHEDULE_TO_START_TASK_ID: u32 = 2;
/// Registry id of the [`ScheduleToCloseTimer`] pure task.
pub const SCHEDULE_TO_CLOSE_TASK_ID: u32 = 3;
/// Registry id of the [`StartToCloseTimer`] pure task.
pub const START_TO_CLOSE_TASK_ID: u32 = 4;
/// Registry id of the [`HeartbeatTimer`] pure task.
pub const HEARTBEAT_TASK_ID: u32 = 5;

/// The side-effect task that enqueues the activity to matching for a worker to poll
/// (`ActivityDispatchTask @ v1.31.0`). Stamp-fenced: dropped once the attempt
/// advances or the activity leaves `Scheduled`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchTask {
    /// The attempt stamp this dispatch was scheduled for.
    pub stamp: i64,
    /// The task queue the attempt is enqueued on, so the dispatch sink can route it
    /// to the matching per-task-queue FIFO a worker polls. Carried on the task (not
    /// re-read from state) so the sink stays a pure consumer of the committed task.
    pub task_queue: String,
}

impl Task for DispatchTask {
    const KIND: TaskKind = TaskKind::SideEffect;
    fn fire_at(&self) -> Option<i64> {
        None
    }
}

impl DispatchTask {
    /// Decode a dispatch-task payload handed to the engine's dispatch sink. The
    /// payload is the postcard encoding the state machine produced via
    /// `MutableContext::add_task`; this is the inverse, exposed so a sink outside
    /// this crate (the edge's activity dispatch queue) can recover the routing
    /// `task_queue` and fencing `stamp` without depending on postcard directly.
    pub fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode dispatch task: {e}")))
    }
}

/// Pure timer: fails the activity with a schedule-to-start timeout if it is not
/// started in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleToStartTimer {
    /// The attempt stamp this timer was scheduled for.
    pub stamp: i64,
    /// When the timer fires, in Unix nanoseconds.
    pub fire_at_nanos: i64,
}

impl Task for ScheduleToStartTimer {
    const KIND: TaskKind = TaskKind::Pure;
    fn fire_at(&self) -> Option<i64> {
        Some(self.fire_at_nanos)
    }
}

/// Pure timer: fails the activity with a schedule-to-close timeout if it does not
/// close in time. Independent of attempts, so it is fenced only on terminal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleToCloseTimer {
    /// The attempt stamp this timer was scheduled for.
    pub stamp: i64,
    /// When the timer fires, in Unix nanoseconds.
    pub fire_at_nanos: i64,
}

impl Task for ScheduleToCloseTimer {
    const KIND: TaskKind = TaskKind::Pure;
    fn fire_at(&self) -> Option<i64> {
        Some(self.fire_at_nanos)
    }
}

/// Pure timer: fails the started attempt with a start-to-close timeout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartToCloseTimer {
    /// The attempt stamp this timer was scheduled for.
    pub stamp: i64,
    /// When the timer fires, in Unix nanoseconds.
    pub fire_at_nanos: i64,
}

impl Task for StartToCloseTimer {
    const KIND: TaskKind = TaskKind::Pure;
    fn fire_at(&self) -> Option<i64> {
        Some(self.fire_at_nanos)
    }
}

/// Pure timer: fails the started attempt with a heartbeat timeout if no heartbeat
/// arrives in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatTimer {
    /// The attempt stamp this timer was scheduled for.
    pub stamp: i64,
    /// When the timer fires, in Unix nanoseconds.
    pub fire_at_nanos: i64,
}

impl Task for HeartbeatTimer {
    const KIND: TaskKind = TaskKind::Pure;
    fn fire_at(&self) -> Option<i64> {
        Some(self.fire_at_nanos)
    }
}

/// `true` iff the activity's live stamp matches `stamp` and its status is exactly
/// `expected` — the common stamp-fence used by the timer validators.
fn valid_in_status(component: &ActivityExecution, stamp: i64, expected: ActivityStatus) -> bool {
    component
        .activity_state()
        .is_some_and(|s| s.stamp == stamp && s.status() == expected)
}

/// Validator for [`DispatchTask`]: valid only while the activity is still
/// `Scheduled` on the same attempt; once it starts, closes, or the attempt
/// advances, the dispatch is stale and dropped (Requirement 11.7).
#[derive(Debug, Clone, Copy, Default)]
pub struct DispatchValidator;

impl TaskValidator<ActivityExecution, DispatchTask> for DispatchValidator {
    fn validate(
        &self,
        component: &ActivityExecution,
        task: &DispatchTask,
        _ctx: &dyn Context,
    ) -> TaskValidity {
        if valid_in_status(component, task.stamp, ActivityStatus::Scheduled) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}

/// Validator for [`ScheduleToStartTimer`]: valid only while `Scheduled` on the same
/// attempt (a started or rescheduled activity drops it).
#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleToStartValidator;

impl TaskValidator<ActivityExecution, ScheduleToStartTimer> for ScheduleToStartValidator {
    fn validate(
        &self,
        component: &ActivityExecution,
        task: &ScheduleToStartTimer,
        _ctx: &dyn Context,
    ) -> TaskValidity {
        if valid_in_status(component, task.stamp, ActivityStatus::Scheduled) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}

/// Validator for [`ScheduleToCloseTimer`]: spans attempts, so it is fenced only on
/// terminal state — valid until the activity closes.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleToCloseValidator;

impl TaskValidator<ActivityExecution, ScheduleToCloseTimer> for ScheduleToCloseValidator {
    fn validate(
        &self,
        component: &ActivityExecution,
        _task: &ScheduleToCloseTimer,
        _ctx: &dyn Context,
    ) -> TaskValidity {
        match component.activity_state() {
            Some(s) if !s.status().is_terminal() => TaskValidity::Valid,
            _ => TaskValidity::Drop,
        }
    }
}

/// Validator for [`StartToCloseTimer`]: valid only while `Started` on the same
/// attempt.
#[derive(Debug, Clone, Copy, Default)]
pub struct StartToCloseValidator;

impl TaskValidator<ActivityExecution, StartToCloseTimer> for StartToCloseValidator {
    fn validate(
        &self,
        component: &ActivityExecution,
        task: &StartToCloseTimer,
        _ctx: &dyn Context,
    ) -> TaskValidity {
        if valid_in_status(component, task.stamp, ActivityStatus::Started) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}

/// Validator for [`HeartbeatTimer`]: valid only while `Started` on the same
/// attempt.
#[derive(Debug, Clone, Copy, Default)]
pub struct HeartbeatValidator;

impl TaskValidator<ActivityExecution, HeartbeatTimer> for HeartbeatValidator {
    fn validate(
        &self,
        component: &ActivityExecution,
        task: &HeartbeatTimer,
        _ctx: &dyn Context,
    ) -> TaskValidity {
        if valid_in_status(component, task.stamp, ActivityStatus::Started) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}
