//! Callback graph tests with a recording logical-clock context and no runtime I/O.

use super::*;
use crate::{
    ActivityEvent, ActivityExecution, ActivityStatus, TimeoutType, due_callback_retries,
    lifecycle_for, lifecycle_of, next_callback_retry_deadline,
    tasks::{
        CallbackRetryHandler, CallbackRetryValidator, DeliverCallbackHandler,
        DeliverCallbackValidator,
    },
};
use proptest::prelude::*;
use prost::Message;
use tokeira_chasm::{
    Context, ExecutionInfo, ExecutionKey, Lifecycle, LifecycleState, PureTaskHandler,
    SideEffectTaskHandler, TaskId, TaskKind, TaskValidator, TaskValidity, VisibilityContributor,
};

#[derive(Debug, PartialEq, Eq)]
struct Staged {
    kind: TaskKind,
    id: u32,
    payload: Vec<u8>,
    fire_at: Option<i64>,
}

struct TestContext {
    key: ExecutionKey,
    now: i64,
    tasks: Vec<Staged>,
}
impl TestContext {
    fn new(now: i64) -> Self {
        Self {
            key: ExecutionKey::new("ns", "activity", "run"),
            now,
            tasks: Vec::new(),
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
    fn add_task(
        &mut self,
        kind: TaskKind,
        id: u32,
        payload: Vec<u8>,
        fire_at: Option<i64>,
    ) -> Result<(), ChasmError> {
        self.tasks.push(Staged {
            kind,
            id,
            payload,
            fire_at,
        });
        Ok(())
    }
    fn mark_dirty(&mut self) -> Result<(), ChasmError> {
        Ok(())
    }
    fn resolve_task(&mut self, _: TaskId) {
        panic!("callback handlers leave task resolution to the engine")
    }
}

fn spec(internal: bool) -> CallbackSpec {
    CallbackSpec {
        target: if internal {
            CallbackTarget::Internal {
                component_ref: vec![1, 2],
                task_type_id: 1024,
                task_id: vec![3, 4],
            }
        } else {
            CallbackTarget::Nexus {
                url: "https://example.test/complete".into(),
                header: BTreeMap::from([("z".into(), "last".into()), ("a".into(), "first".into())]),
            }
        },
        links: vec![vec![10, 1, 65], vec![10, 1, 66]],
    }
}

fn attach_event(
    request_id: &str,
    callbacks: Vec<CallbackSpec>,
    max_callbacks: usize,
) -> ActivityEvent {
    ActivityEvent::CallbacksAttached {
        request_id: request_id.into(),
        callbacks,
        max_callbacks,
    }
}

fn state(activity: &ActivityExecution) -> &ActivityState {
    activity.activity_state().unwrap()
}

fn complete(activity: &mut ActivityExecution, terminal: u8, ctx: &mut TestContext) {
    activity.apply(ActivityEvent::Scheduled, ctx).unwrap();
    activity
        .apply(
            ActivityEvent::Started {
                started_time_nanos: ctx.now,
                identity: "worker".into(),
            },
            ctx,
        )
        .unwrap();
    if terminal == 2 {
        activity
            .apply(
                ActivityEvent::CancelRequested {
                    identity: "client".into(),
                    request_id: "cancel".into(),
                    reason: "done".into(),
                },
                ctx,
            )
            .unwrap();
    }
    ctx.tasks.clear();
    let event = match terminal {
        0 => ActivityEvent::Completed {
            result: vec![9],
            identity: "worker".into(),
        },
        1 => ActivityEvent::Failed {
            failure: "failed".into(),
            failure_payload: vec![10],
            identity: "worker".into(),
            last_heartbeat_details: vec![],
        },
        2 => ActivityEvent::Canceled { details: vec![11] },
        3 => ActivityEvent::Terminated {
            reason: "stop".into(),
            identity: "client".into(),
            request_id: "terminate".into(),
        },
        _ => ActivityEvent::TimedOut {
            stamp: state(activity).stamp,
            timeout_type: TimeoutType::StartToClose,
            failure_payload: vec![12],
        },
    };
    activity.apply(event, ctx).unwrap();
}

fn delivery(ctx: &mut TestContext) -> DeliverCallback {
    assert_eq!(ctx.tasks.len(), 1);
    let task = ctx.tasks.pop().unwrap();
    assert_eq!(
        (task.kind, task.id, task.fire_at),
        (TaskKind::SideEffect, DELIVER_CALLBACK_TASK_ID, None)
    );
    DeliverCallback::decode(&task.payload).unwrap()
}

#[test]
fn attachment_preserves_upstream_error_order_and_messages() {
    let mut ctx = TestContext::new(100);
    let mut activity = ActivityExecution::new(ActivityState {
        status: ActivityStatus::Completed as i32,
        ..Default::default()
    });
    let before = state(&activity).encode_to_vec();
    activity
        .apply(attach_event("empty", vec![], 0), &mut ctx)
        .unwrap();
    assert_eq!(state(&activity).encode_to_vec(), before);
    let error = activity
        .apply(attach_event("closed", vec![spec(false)], 0), &mut ctx)
        .unwrap_err();
    assert!(
        matches!(error, ChasmError::FailedPrecondition(message) if message == "cannot attach callbacks to a closed activity")
    );
    assert_eq!(state(&activity).encode_to_vec(), before);

    let mut activity = ActivityExecution::new(ActivityState::default());
    activity
        .apply(attach_event("request", vec![spec(false)], 2), &mut ctx)
        .unwrap();
    let before = state(&activity).encode_to_vec();
    let error = activity
        .apply(
            attach_event("request", vec![spec(true), spec(false)], 2),
            &mut ctx,
        )
        .unwrap_err();
    assert!(
        matches!(error, ChasmError::FailedPrecondition(message) if message == "cannot attach more than 2 callbacks to an activity (1 callbacks already attached)")
    );
    assert_eq!(state(&activity).encode_to_vec(), before);
    assert!(ctx.tasks.is_empty());
}

#[test]
fn attachment_upserts_ids_without_changing_order_and_uses_one_batch_time() {
    let mut ctx = TestContext::new(100);
    let mut activity = ActivityExecution::new(ActivityState::default());
    activity
        .apply(
            attach_event("request", vec![spec(false), spec(true)], 10),
            &mut ctx,
        )
        .unwrap();
    for (index, callback) in state(&activity).callbacks.iter().enumerate() {
        assert_eq!(callback.id, format!("request-{index}"));
        assert_eq!(callback.registration_time_nanos, 100);
        assert_eq!(callback.state(), CallbackState::Standby);
        assert_eq!(callback.attempt, 0);
        assert_eq!(callback.links, spec(false).links);
    }
    ctx.now = 200;
    activity
        .apply(attach_event("request", vec![spec(true)], 10), &mut ctx)
        .unwrap();
    assert_eq!(state(&activity).callbacks.len(), 2);
    assert_eq!(state(&activity).callbacks[0].registration_time_nanos, 200);
    assert_eq!(state(&activity).callbacks[1].registration_time_nanos, 100);
    assert!(matches!(
        state(&activity).callbacks[0].target,
        Some(activity_callback::Target::Internal(_))
    ));
    assert!(ctx.tasks.is_empty());
}

#[test]
fn every_terminal_event_schedules_callbacks_and_preserves_public_outcome() {
    for terminal in 0..5 {
        let mut ctx = TestContext::new(100);
        let mut activity = ActivityExecution::new(ActivityState::default());
        activity
            .apply(
                attach_event("request", vec![spec(false), spec(true)], 2),
                &mut ctx,
            )
            .unwrap();
        complete(&mut activity, terminal, &mut ctx);
        assert_eq!(ctx.tasks.len(), 2);
        for (index, task) in ctx.tasks.iter().enumerate() {
            assert_eq!(
                DeliverCallback::decode(&task.payload).unwrap(),
                DeliverCallback {
                    callback_id: format!("request-{index}"),
                    stamp: 0
                }
            );
            assert_eq!(
                (task.id, task.kind, task.fire_at),
                (DELIVER_CALLBACK_TASK_ID, TaskKind::SideEffect, None)
            );
        }
        assert!(
            state(&activity)
                .callbacks
                .iter()
                .all(|callback| callback.state() == CallbackState::Scheduled)
        );
        let snapshot = activity.visibility_snapshot().unwrap();
        assert_eq!(snapshot.lifecycle_state, LifecycleState::Running);
        assert_eq!(snapshot.lifecycle_state, activity.lifecycle_state(&ctx));
        assert_eq!(snapshot.close_time_unix_nanos, Some(100));
        let expected =
            ["Completed", "Failed", "Canceled", "Terminated", "TimedOut"][usize::from(terminal)];
        assert_eq!(snapshot.status_keyword, expected);
        assert!(matches!(
            activity.apply(attach_event("late", vec![spec(false)], 20), &mut ctx),
            Err(ChasmError::FailedPrecondition(_))
        ));
    }
}

#[test]
fn lifecycle_waits_for_every_callback_for_every_activity_status() {
    for raw in 0..=8 {
        let status = ActivityStatus::try_from(raw).unwrap();
        let mut state = ActivityState {
            status: raw,
            ..Default::default()
        };
        assert_eq!(lifecycle_of(&state), lifecycle_for(status));
        for pending in [
            CallbackState::Standby,
            CallbackState::Scheduled,
            CallbackState::BackingOff,
        ] {
            state.callbacks = vec![
                ActivityCallback {
                    state: pending as i32,
                    ..Default::default()
                },
                ActivityCallback {
                    state: CallbackState::Succeeded as i32,
                    ..Default::default()
                },
            ];
            assert_eq!(lifecycle_of(&state), LifecycleState::Running);
        }
        state.callbacks = [CallbackState::Succeeded, CallbackState::Failed]
            .into_iter()
            .map(|s| ActivityCallback {
                state: s as i32,
                ..Default::default()
            })
            .collect();
        assert!(state.callbacks.iter().all(ActivityCallback::is_settled));
        assert_eq!(lifecycle_of(&state), lifecycle_for(status));
    }
}

#[test]
fn outcome_envelopes_round_trip_and_reject_unsupported_or_malformed_results() {
    for time in [i64::MIN, 0, 1234, i64::MAX] {
        let failure = vec![0, 1, 255];
        assert_eq!(
            callback_attempt_outcome(&retryable_delivery_failure(failure.clone(), time)).unwrap(),
            CallbackAttemptOutcome::RetryableFailure {
                failure: failure.clone(),
                next_attempt_time_nanos: time
            }
        );
        assert_eq!(
            callback_attempt_outcome(&non_retryable_delivery_failure(failure.clone())).unwrap(),
            CallbackAttemptOutcome::NonRetryableFailure { failure }
        );
    }
    assert_eq!(
        callback_attempt_outcome(&TaskOutcome::Completed { payload: vec![] }).unwrap(),
        CallbackAttemptOutcome::Succeeded
    );
    assert!(matches!(
        callback_attempt_outcome(&TaskOutcome::Failed {
            failure: vec![],
            retryable: true
        }),
        Err(ChasmError::Validation(_))
    ));
    for outcome in [
        TaskOutcome::Completed { payload: vec![1] },
        TaskOutcome::Canceled { details: vec![] },
        TaskOutcome::TimedOut { timeout_type: 1 },
        TaskOutcome::Terminated,
    ] {
        assert!(matches!(
            callback_attempt_outcome(&outcome),
            Err(ChasmError::Unsupported(_))
        ));
    }
}

#[test]
fn retries_are_derived_in_callback_order_and_include_zero_deadlines() {
    let mut state = ActivityState::default();
    assert_eq!(next_callback_retry_deadline(&state), None);
    for (index, (status, time)) in [
        (CallbackState::BackingOff, 30),
        (CallbackState::Succeeded, -10),
        (CallbackState::BackingOff, 0),
        (CallbackState::BackingOff, 20),
    ]
    .into_iter()
    .enumerate()
    {
        state.callbacks.push(ActivityCallback {
            id: index.to_string(),
            state: status as i32,
            next_attempt_time_nanos: time,
            ..Default::default()
        });
    }
    assert_eq!(next_callback_retry_deadline(&state), Some(0));
    assert_eq!(due_callback_retries(&state, -1), Vec::<String>::new());
    assert_eq!(due_callback_retries(&state, 20), vec!["2", "3"]);
    assert_eq!(due_callback_retries(&state, 30), vec!["0", "2", "3"]);
}

#[test]
fn missing_callback_ids_fail_without_mutation_and_drop_tasks() {
    let mut ctx = TestContext::new(0);
    let mut activity = ActivityExecution::new(ActivityState::default());
    for event in [
        ActivityEvent::CallbackRetryDue { id: "gone".into() },
        ActivityEvent::CallbackAttempted {
            id: "gone".into(),
            outcome: CallbackAttemptOutcome::Succeeded,
        },
    ] {
        let before = state(&activity).encode_to_vec();
        assert!(
            matches!(activity.apply(event, &mut ctx), Err(ChasmError::Internal(message)) if message.contains("gone"))
        );
        assert_eq!(state(&activity).encode_to_vec(), before);
    }
    let task = DeliverCallback {
        callback_id: "gone".into(),
        stamp: 0,
    };
    assert_eq!(
        DeliverCallbackValidator.validate(&activity, &task, &ctx),
        TaskValidity::Drop
    );
    let timer = CallbackRetryTimer {
        callback_id: "gone".into(),
        attempt: 0,
        fire_at_nanos: 0,
    };
    assert_eq!(
        CallbackRetryValidator.validate(&activity, &timer, &ctx),
        TaskValidity::Drop
    );
}

#[test]
fn callback_events_reject_the_wrong_callback_state_before_mutation() {
    for callback_state in [
        CallbackState::Standby,
        CallbackState::Scheduled,
        CallbackState::BackingOff,
        CallbackState::Succeeded,
        CallbackState::Failed,
    ] {
        for retry in [false, true] {
            if callback_state
                == if retry {
                    CallbackState::BackingOff
                } else {
                    CallbackState::Scheduled
                }
            {
                continue;
            }
            let mut ctx = TestContext::new(100);
            let mut activity = ActivityExecution::new(ActivityState {
                callbacks: vec![ActivityCallback {
                    id: "callback".into(),
                    state: callback_state as i32,
                    ..Default::default()
                }],
                ..Default::default()
            });
            let before = state(&activity).encode_to_vec();
            let event = if retry {
                ActivityEvent::CallbackRetryDue {
                    id: "callback".into(),
                }
            } else {
                ActivityEvent::CallbackAttempted {
                    id: "callback".into(),
                    outcome: CallbackAttemptOutcome::Succeeded,
                }
            };
            assert!(
                matches!(activity.apply(event, &mut ctx), Err(ChasmError::IllegalTransition { from, event })
                if from == format!("callback:{}", callback_state.as_str_name())
                    && event == if retry { "CallbackRetryDue" } else { "CallbackAttempted" })
            );
            assert_eq!(state(&activity).encode_to_vec(), before);
            assert!(ctx.tasks.is_empty());
            let validity = if retry {
                CallbackRetryValidator.validate(
                    &activity,
                    &CallbackRetryTimer {
                        callback_id: "callback".into(),
                        attempt: 0,
                        fire_at_nanos: 100,
                    },
                    &ctx,
                )
            } else {
                DeliverCallbackValidator.validate(
                    &activity,
                    &DeliverCallback {
                        callback_id: "callback".into(),
                        stamp: 0,
                    },
                    &ctx,
                )
            };
            assert_eq!(validity, TaskValidity::Drop);
        }
    }
}

#[test]
fn delivery_bookkeeping_preserves_a_zero_clock_activity_close() {
    let mut ctx = TestContext::new(0);
    let mut activity = ActivityExecution::new(ActivityState::default());
    activity
        .apply(attach_event("request", vec![spec(false)], 1), &mut ctx)
        .unwrap();
    complete(&mut activity, 0, &mut ctx);
    let task = delivery(&mut ctx);
    ctx.now = 100;
    DeliverCallbackHandler
        .on_outcome(
            &mut activity,
            &task,
            &TaskOutcome::Completed { payload: vec![] },
            &mut ctx,
        )
        .unwrap();
    assert_eq!(state(&activity).close_time_nanos, 0);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Model {
    Scheduled,
    BackingOff,
    Succeeded,
    Failed,
}

type CallbackPlan = (bool, Vec<(Vec<u8>, i64)>, u8);

fn check_delivery_plan(
    terminal: u8,
    plans: Vec<CallbackPlan>,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let mut ctx = TestContext::new(100);
    let mut activity = ActivityExecution::new(ActivityState::default());
    activity
        .apply(
            attach_event(
                "request",
                plans
                    .iter()
                    .map(|(internal, _, _)| spec(*internal))
                    .collect(),
                5,
            ),
            &mut ctx,
        )
        .unwrap();
    complete(&mut activity, terminal, &mut ctx);
    prop_assert_eq!(ctx.tasks.len(), plans.len());
    let mut deliveries: Vec<_> = ctx
        .tasks
        .drain(..)
        .map(|task| {
            assert_eq!(
                (task.id, task.kind, task.fire_at),
                (DELIVER_CALLBACK_TASK_ID, TaskKind::SideEffect, None)
            );
            DeliverCallback::decode(&task.payload).unwrap()
        })
        .collect();
    let status = state(&activity).status();
    let close_time = state(&activity).close_time_nanos;
    let mut model = vec![Model::Scheduled; plans.len()];
    for (index, (_, retries, final_kind)) in plans.iter().enumerate() {
        prop_assert_eq!(
            &deliveries[index],
            &DeliverCallback {
                callback_id: format!("request-{index}"),
                stamp: 0
            }
        );
        let mut outcomes: Vec<_> = retries
            .iter()
            .map(|(failure, delay)| (failure.clone(), *delay, 0u8))
            .collect();
        if *final_kind != 0 {
            outcomes.push((vec![42], 0, *final_kind));
        }
        for (attempt_index, (failure, delay, kind)) in outcomes.into_iter().enumerate() {
            let task = deliveries[index].clone();
            prop_assert_eq!(
                DeliverCallbackValidator.validate(&activity, &task, &ctx),
                TaskValidity::Valid
            );
            let stale = DeliverCallback {
                stamp: task.stamp - 1,
                ..task.clone()
            };
            prop_assert_eq!(
                DeliverCallbackValidator.validate(&activity, &stale, &ctx),
                TaskValidity::Drop
            );
            let now = ctx.now;
            let next = now + delay;
            let outcome = match kind {
                0 => retryable_delivery_failure(failure.clone(), next),
                1 => TaskOutcome::Completed { payload: vec![] },
                _ => non_retryable_delivery_failure(failure.clone()),
            };
            DeliverCallbackHandler
                .on_outcome(&mut activity, &task, &outcome, &mut ctx)
                .unwrap();
            model[index] = match kind {
                0 => Model::BackingOff,
                1 => Model::Succeeded,
                _ => Model::Failed,
            };
            let callback = &state(&activity).callbacks[index];
            let expected_state = match model[index] {
                Model::Scheduled => CallbackState::Scheduled,
                Model::BackingOff => CallbackState::BackingOff,
                Model::Succeeded => CallbackState::Succeeded,
                Model::Failed => CallbackState::Failed,
            };
            prop_assert_eq!(callback.state(), expected_state);
            prop_assert_eq!(callback.attempt, (attempt_index + 1) as i32);
            prop_assert_eq!(callback.last_attempt_complete_time_nanos, now);
            prop_assert_eq!(
                &callback.last_attempt_failure,
                &if kind == 1 { vec![] } else { failure }
            );
            prop_assert_eq!(
                DeliverCallbackValidator.validate(&activity, &task, &ctx),
                TaskValidity::Drop
            );
            if kind == 0 {
                prop_assert_eq!(callback.next_attempt_time_nanos, next);
                prop_assert_eq!(ctx.tasks.len(), 1);
                let staged = ctx.tasks.pop().unwrap();
                prop_assert_eq!(
                    (staged.id, staged.kind, staged.fire_at),
                    (CALLBACK_RETRY_TASK_ID, TaskKind::Pure, Some(next))
                );
                let timer = CallbackRetryTimer::decode(&staged.payload).unwrap();
                prop_assert_eq!(
                    &timer,
                    &CallbackRetryTimer {
                        callback_id: task.callback_id.clone(),
                        attempt: (attempt_index + 1) as i32,
                        fire_at_nanos: next
                    }
                );
                prop_assert_eq!(
                    CallbackRetryValidator.validate(&activity, &timer, &ctx),
                    TaskValidity::Valid
                );
                let stale = CallbackRetryTimer {
                    attempt: timer.attempt - 1,
                    ..timer.clone()
                };
                prop_assert_eq!(
                    CallbackRetryValidator.validate(&activity, &stale, &ctx),
                    TaskValidity::Drop
                );
                ctx.now = next;
                CallbackRetryHandler
                    .execute(&mut activity, &timer, &mut ctx)
                    .unwrap();
                model[index] = Model::Scheduled;
                prop_assert_eq!(
                    state(&activity).callbacks[index].state(),
                    CallbackState::Scheduled
                );
                prop_assert_eq!(state(&activity).callbacks[index].next_attempt_time_nanos, 0);
                prop_assert_eq!(
                    CallbackRetryValidator.validate(&activity, &timer, &ctx),
                    TaskValidity::Drop
                );
                deliveries[index] = delivery(&mut ctx);
                prop_assert_eq!(deliveries[index].stamp, (attempt_index + 1) as i32);
            } else {
                prop_assert!(ctx.tasks.is_empty());
                let before = state(&activity).encode_to_vec();
                prop_assert!(
                    matches!(
                        activity.apply(
                            ActivityEvent::CallbackAttempted {
                                id: task.callback_id.clone(),
                                outcome: CallbackAttemptOutcome::Succeeded
                            },
                            &mut ctx
                        ),
                        Err(ChasmError::IllegalTransition { .. })
                    ),
                    "settled is absorbing"
                );
                prop_assert_eq!(state(&activity).encode_to_vec(), before);
            }
            prop_assert_eq!(state(&activity).status(), status);
            prop_assert_eq!(state(&activity).close_time_nanos, close_time);
            let pending = model
                .iter()
                .any(|m| matches!(m, Model::Scheduled | Model::BackingOff));
            prop_assert_eq!(
                activity.lifecycle_state(&ctx),
                if pending {
                    LifecycleState::Running
                } else {
                    lifecycle_for(status)
                }
            );
            ctx.now += 1;
        }
    }
    Ok(())
}

// Feature: chasm-extension-archetypes, Property 10: callback delivery state machine
// The upstream callback graph counts attempts once and records supplied retry times verbatim.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn callback_delivery_state_machine(
        terminal in 0u8..5,
        plans in prop::collection::vec(
            (any::<bool>(), prop::collection::vec((prop::collection::vec(any::<u8>(), 0..12), 0i64..50), 0..5), 0u8..3),
            1..6,
        ),
    ) {
        check_delivery_plan(terminal, plans)?;
    }
}
