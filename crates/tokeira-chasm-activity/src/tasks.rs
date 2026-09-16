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
use tokeira_chasm::{
    ChasmError, Context, MutableContext, PureTaskHandler, SideEffectTaskHandler, Task, TaskKind,
    TaskOutcome, TaskValidator, TaskValidity,
};

use crate::{TimeoutType, component::ActivityExecution, state::ActivityStatus, timeout_event};

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
    const FQN: &'static str = "activity.dispatch";
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|e| ChasmError::Internal(format!("encode {}: {e}", Self::FQN)))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
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
    const FQN: &'static str = "activity.schedule_to_start";
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|e| ChasmError::Internal(format!("encode {}: {e}", Self::FQN)))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
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
    const FQN: &'static str = "activity.schedule_to_close";
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|e| ChasmError::Internal(format!("encode {}: {e}", Self::FQN)))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
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
    const FQN: &'static str = "activity.start_to_close";
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|e| ChasmError::Internal(format!("encode {}: {e}", Self::FQN)))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
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
    const FQN: &'static str = "activity.heartbeat";
    fn encode(&self) -> Result<Vec<u8>, ChasmError> {
        postcard::to_allocvec(self)
            .map_err(|e| ChasmError::Internal(format!("encode {}: {e}", Self::FQN)))
    }
    fn decode(bytes: &[u8]) -> Result<Self, ChasmError> {
        postcard::from_bytes(bytes)
            .map_err(|e| ChasmError::Validation(format!("decode {}: {e}", Self::FQN)))
    }
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

// Cancellation does not end a running attempt; both timers stay live until the
// worker acknowledges or a timeout fires (activity_tasks.go @ v1.31.0).
fn valid_running_attempt(component: &ActivityExecution, stamp: i64) -> bool {
    component.activity_state().is_some_and(|state| {
        state.stamp == stamp
            && matches!(
                state.status(),
                ActivityStatus::Started | ActivityStatus::CancelRequested
            )
    })
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

/// Validator for [`StartToCloseTimer`]: valid while `Started` or `CancelRequested` on the same
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
        if valid_running_attempt(component, task.stamp) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}

/// Validator for [`HeartbeatTimer`]: valid while `Started` or `CancelRequested` on the same
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
        // A heartbeat supersedes this deadline without ending the attempt
        // (`activity_tasks.go` heartbeat Validate @ v1.31.0).
        if valid_running_attempt(component, task.stamp)
            && component.activity_state().is_some_and(|state| {
                state
                    .last_heartbeat_time_nanos
                    .max(state.started_time_nanos)
                    <= task.fire_at_nanos.saturating_sub(state.heartbeat_nanos)
            })
        {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }
}

/// Worker dispatch is performed by the bridge; generic outcomes cannot bypass its
/// token and attempt checks. Stage 9 owns any future routing through this handler.
#[derive(Debug, Clone, Copy, Default)]
pub struct DispatchHandler;

impl SideEffectTaskHandler for DispatchHandler {
    type Component = ActivityExecution;
    type Task = DispatchTask;
    fn validate(&self, c: &ActivityExecution, t: &DispatchTask, ctx: &dyn Context) -> TaskValidity {
        DispatchValidator.validate(c, t, ctx)
    }
    fn on_outcome(
        &self,
        _c: &mut ActivityExecution,
        _t: &DispatchTask,
        _outcome: &TaskOutcome,
        _ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        Err(ChasmError::Unsupported(
            "worker outcomes reach the activity through the bridge".to_owned(),
        ))
    }
}

/// Registered ScheduleToStart transition, sharing the evaluator's event construction
/// (`chasm/lib/activity/activity_tasks.go @ v1.31.0`).
#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleToStartHandler;

impl PureTaskHandler for ScheduleToStartHandler {
    type Component = ActivityExecution;
    type Task = ScheduleToStartTimer;
    fn validate(
        &self,
        c: &ActivityExecution,
        t: &ScheduleToStartTimer,
        ctx: &dyn Context,
    ) -> TaskValidity {
        ScheduleToStartValidator.validate(c, t, ctx)
    }
    fn execute(
        &self,
        c: &mut ActivityExecution,
        _t: &ScheduleToStartTimer,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = c.activity_state().cloned().unwrap_or_default();
        c.apply(
            timeout_event(&state, TimeoutType::ScheduleToStart, ctx.now_unix_nanos()),
            ctx,
        )
    }
}

/// Registered ScheduleToClose transition, sharing the evaluator's event construction
/// (`chasm/lib/activity/activity_tasks.go @ v1.31.0`).
#[derive(Debug, Clone, Copy, Default)]
pub struct ScheduleToCloseHandler;

impl PureTaskHandler for ScheduleToCloseHandler {
    type Component = ActivityExecution;
    type Task = ScheduleToCloseTimer;
    fn validate(
        &self,
        c: &ActivityExecution,
        t: &ScheduleToCloseTimer,
        ctx: &dyn Context,
    ) -> TaskValidity {
        ScheduleToCloseValidator.validate(c, t, ctx)
    }
    fn execute(
        &self,
        c: &mut ActivityExecution,
        _t: &ScheduleToCloseTimer,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = c.activity_state().cloned().unwrap_or_default();
        c.apply(
            timeout_event(&state, TimeoutType::ScheduleToClose, ctx.now_unix_nanos()),
            ctx,
        )
    }
}

/// Registered StartToClose transition, sharing the evaluator's event construction
/// (`chasm/lib/activity/activity_tasks.go @ v1.31.0`).
#[derive(Debug, Clone, Copy, Default)]
pub struct StartToCloseHandler;

impl PureTaskHandler for StartToCloseHandler {
    type Component = ActivityExecution;
    type Task = StartToCloseTimer;
    fn validate(
        &self,
        c: &ActivityExecution,
        t: &StartToCloseTimer,
        ctx: &dyn Context,
    ) -> TaskValidity {
        StartToCloseValidator.validate(c, t, ctx)
    }
    fn execute(
        &self,
        c: &mut ActivityExecution,
        _t: &StartToCloseTimer,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = c.activity_state().cloned().unwrap_or_default();
        c.apply(
            timeout_event(&state, TimeoutType::StartToClose, ctx.now_unix_nanos()),
            ctx,
        )
    }
}

/// Registered Heartbeat transition, sharing the evaluator's event construction
/// (`chasm/lib/activity/activity_tasks.go @ v1.31.0`).
#[derive(Debug, Clone, Copy, Default)]
pub struct HeartbeatHandler;

impl PureTaskHandler for HeartbeatHandler {
    type Component = ActivityExecution;
    type Task = HeartbeatTimer;
    fn validate(
        &self,
        c: &ActivityExecution,
        t: &HeartbeatTimer,
        ctx: &dyn Context,
    ) -> TaskValidity {
        HeartbeatValidator.validate(c, t, ctx)
    }
    fn execute(
        &self,
        c: &mut ActivityExecution,
        _t: &HeartbeatTimer,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = c.activity_state().cloned().unwrap_or_default();
        c.apply(
            timeout_event(&state, TimeoutType::Heartbeat, ctx.now_unix_nanos()),
            ctx,
        )
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    fn preserved<T: Task + PartialEq + std::fmt::Debug>(task: T, fqn: &str) {
        let legacy = postcard::to_allocvec(&task).unwrap();
        assert_eq!(Task::encode(&task).unwrap(), legacy);
        assert_eq!(T::decode(&legacy).unwrap(), task);
        assert_eq!(T::FQN, fqn);
        assert!(T::decode(&[0xff]).is_err());
    }

    #[test]
    fn task_identity_preserves_existing_activity_ids_and_postcard_bytes() {
        assert_eq!(
            [
                DISPATCH_TASK_ID,
                SCHEDULE_TO_START_TASK_ID,
                SCHEDULE_TO_CLOSE_TASK_ID,
                START_TO_CLOSE_TASK_ID,
                HEARTBEAT_TASK_ID
            ],
            [1, 2, 3, 4, 5]
        );
        preserved(
            DispatchTask {
                stamp: 3,
                task_queue: "queue".into(),
            },
            "activity.dispatch",
        );
        preserved(
            ScheduleToStartTimer {
                stamp: 3,
                fire_at_nanos: 10,
            },
            "activity.schedule_to_start",
        );
        preserved(
            ScheduleToCloseTimer {
                stamp: 3,
                fire_at_nanos: 20,
            },
            "activity.schedule_to_close",
        );
        preserved(
            StartToCloseTimer {
                stamp: 3,
                fire_at_nanos: 30,
            },
            "activity.start_to_close",
        );
        preserved(
            HeartbeatTimer {
                stamp: 3,
                fire_at_nanos: 40,
            },
            "activity.heartbeat",
        );
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use crate::{ActivityEvent, ActivityLibrary, ActivityState, RetryOutcome, retry_decision};
    use tokeira_chasm::{
        ExecutionInfo, ExecutionKey, Library, NodeTree, Registry, RegistryOutboxValidator, TaskId,
        VersionedTransition,
    };

    struct TestContext {
        key: ExecutionKey,
        now: i64,
        staged: Vec<(TaskKind, u32, Vec<u8>, Option<i64>)>,
    }
    impl TestContext {
        fn new(now: i64) -> Self {
            Self {
                key: ExecutionKey::new("ns", "activity", "run"),
                now,
                staged: vec![],
            }
        }
    }
    impl Context for TestContext {
        fn execution_key(&self) -> &ExecutionKey {
            &self.key
        }
        fn execution_info(&self) -> ExecutionInfo {
            ExecutionInfo::default()
        }
        fn now_unix_nanos(&self) -> i64 {
            self.now
        }
    }
    impl MutableContext for TestContext {
        fn resolve_task(&mut self, _: TaskId) {}
        fn mark_dirty(&mut self) -> Result<(), ChasmError> {
            Ok(())
        }
        fn add_task(
            &mut self,
            kind: TaskKind,
            id: u32,
            bytes: Vec<u8>,
            at: Option<i64>,
        ) -> Result<(), ChasmError> {
            self.staged.push((kind, id, bytes, at));
            Ok(())
        }
    }
    fn state(status: ActivityStatus) -> ActivityState {
        let mut state = ActivityState {
            attempt: 2,
            stamp: 2,
            started_time_nanos: 100,
            heartbeat_nanos: 20,
            start_to_close_nanos: 50,
            schedule_to_close_nanos: 1_000,
            retry_initial_interval_nanos: 10,
            retry_maximum_interval_nanos: 100,
            retry_backoff_coefficient: 2.0,
            maximum_attempts: 4,
            ..Default::default()
        };
        state.set_status(status);
        state
    }

    #[test]
    fn running_timer_validators_preserve_cancel_requested_and_fence_stamps() {
        let ctx = TestContext::new(120);
        for status in [
            ActivityStatus::Started,
            ActivityStatus::CancelRequested,
            ActivityStatus::Scheduled,
            ActivityStatus::Completed,
        ] {
            for stamp in [1, 2] {
                let activity = ActivityExecution::new(state(status));
                let expected = if stamp == 2
                    && matches!(
                        status,
                        ActivityStatus::Started | ActivityStatus::CancelRequested
                    ) {
                    TaskValidity::Valid
                } else {
                    TaskValidity::Drop
                };
                assert_eq!(
                    StartToCloseValidator.validate(
                        &activity,
                        &StartToCloseTimer {
                            stamp,
                            fire_at_nanos: 150
                        },
                        &ctx
                    ),
                    expected
                );
                assert_eq!(
                    HeartbeatValidator.validate(
                        &activity,
                        &HeartbeatTimer {
                            stamp,
                            fire_at_nanos: 120
                        },
                        &ctx
                    ),
                    expected
                );
            }
        }
    }

    #[test]
    fn heartbeat_supersedes_its_anchor_but_not_the_attempt_timeout() {
        let ctx = TestContext::new(140);
        for status in [ActivityStatus::Started, ActivityStatus::CancelRequested] {
            let mut state = state(status);
            for heartbeat in [0, 90, 100, 101, 120] {
                state.last_heartbeat_time_nanos = heartbeat;
                let activity = ActivityExecution::new(state.clone());
                assert_eq!(
                    HeartbeatValidator.validate(
                        &activity,
                        &HeartbeatTimer {
                            stamp: 2,
                            fire_at_nanos: 120
                        },
                        &ctx
                    ),
                    if heartbeat <= 100 {
                        TaskValidity::Valid
                    } else {
                        TaskValidity::Drop
                    }
                );
                assert_eq!(
                    StartToCloseValidator.validate(
                        &activity,
                        &StartToCloseTimer {
                            stamp: 2,
                            fire_at_nanos: 150
                        },
                        &ctx
                    ),
                    TaskValidity::Valid
                );
            }
        }
    }

    fn compare<H: PureTaskHandler<Component = ActivityExecution>>(
        handler: H,
        task: H::Task,
        state: ActivityState,
        kind: TimeoutType,
    ) {
        let now = 200;
        let mut direct = ActivityExecution::new(state.clone());
        let mut registered = ActivityExecution::new(state.clone());
        let mut direct_ctx = TestContext::new(now);
        let mut handler_ctx = TestContext::new(now);
        // The pre-extraction evaluator decision is retained as the regression oracle.
        let event = if matches!(kind, TimeoutType::StartToClose | TimeoutType::Heartbeat) {
            match retry_decision(&state, now, 0) {
                RetryOutcome::Reschedule(interval) => ActivityEvent::Rescheduled {
                    failure: format!("activity {} timeout", kind.as_str()),
                    identity: state.last_worker_identity.clone(),
                    last_heartbeat_details: vec![],
                    interval_nanos: interval,
                },
                RetryOutcome::Terminal => timeout_event(&state, kind, now),
            }
        } else {
            timeout_event(&state, kind, now)
        };
        direct.apply(event, &mut direct_ctx).unwrap();
        handler
            .execute(&mut registered, &task, &mut handler_ctx)
            .unwrap();
        assert_eq!(direct.activity_state(), registered.activity_state());
        assert_eq!(direct_ctx.staged, handler_ctx.staged);
    }

    #[test]
    fn timer_handlers_match_the_evaluator_events_and_staged_tasks() {
        for attempt in [1, 4] {
            let mut scheduled = state(ActivityStatus::Scheduled);
            scheduled.attempt = attempt;
            compare(
                ScheduleToStartHandler,
                ScheduleToStartTimer {
                    stamp: 2,
                    fire_at_nanos: 200,
                },
                scheduled.clone(),
                TimeoutType::ScheduleToStart,
            );
            for status in [
                ActivityStatus::Scheduled,
                ActivityStatus::Started,
                ActivityStatus::CancelRequested,
            ] {
                let mut state = state(status);
                state.attempt = attempt;
                compare(
                    ScheduleToCloseHandler,
                    ScheduleToCloseTimer {
                        stamp: 2,
                        fire_at_nanos: 200,
                    },
                    state.clone(),
                    TimeoutType::ScheduleToClose,
                );
                if status != ActivityStatus::Scheduled {
                    compare(
                        StartToCloseHandler,
                        StartToCloseTimer {
                            stamp: 2,
                            fire_at_nanos: 200,
                        },
                        state.clone(),
                        TimeoutType::StartToClose,
                    );
                    compare(
                        HeartbeatHandler,
                        HeartbeatTimer {
                            stamp: 2,
                            fire_at_nanos: 200,
                        },
                        state,
                        TimeoutType::Heartbeat,
                    );
                }
            }
        }
    }

    #[test]
    fn heartbeat_replacement_survives_close_with_one_live_timer() {
        let mut b = Registry::builder();
        ActivityLibrary::register(&mut b).unwrap();
        let registry = b.build();
        let archetype = registry.archetype_id("activity.activity").unwrap();
        let mut activity = ActivityExecution::new(state(ActivityStatus::Started));
        let mut tree = NodeTree::new();
        tree.create_node(
            vec![],
            archetype,
            Some(tokeira_chasm::LifecycleState::Running),
            None,
        )
        .unwrap();
        tree.add_task(
            b"",
            TaskKind::Pure,
            HEARTBEAT_TASK_ID,
            Task::encode(&HeartbeatTimer {
                stamp: 2,
                fire_at_nanos: 120,
            })
            .unwrap(),
            Some(120),
        )
        .unwrap();
        for (step, now) in [110, 115, 130].into_iter().enumerate() {
            let mut ctx = TestContext::new(now);
            activity
                .apply(ActivityEvent::Heartbeat { details: vec![1] }, &mut ctx)
                .unwrap();
            for (kind, id, bytes, at) in std::mem::take(&mut ctx.staged) {
                tree.add_task(b"", kind, id, bytes, at).unwrap();
            }
            let bytes = prost::Message::encode_to_vec(activity.activity_state().unwrap());
            tree.set_data(b"", Some(bytes.clone())).unwrap();
            tree.close_transaction(
                VersionedTransition::new(0, step as i64 + 1),
                &RegistryOutboxValidator {
                    registry: &registry,
                    component_type_id: archetype,
                    data: &bytes,
                    ctx: &ctx,
                },
            )
            .unwrap();
            let outbox = &tree.node(b"").unwrap().metadata.outbox;
            assert_eq!(outbox.pure_tasks.len(), 1);
            assert_eq!(outbox.earliest_pure_deadline(), Some(now + 20));
        }
    }
}
