//! Heartbeat origins, timeout materialization and history-only reconstruction.

use super::*;
use proptest::prelude::*;
use tokeira_kernel::{CompletionWorkflowTaskStart, WorkflowTaskType};

fn state(original: Option<OffsetDateTime>) -> WorkflowState {
    let mut state = make_open_state_with_started_wft();
    state
        .pending_workflow_task
        .as_mut()
        .unwrap()
        .original_scheduled_at = original;
    state
}

fn heartbeat(state: &WorkflowState, at: OffsetDateTime) -> WorkflowTaskCompletedRequest {
    let mut request = speculative_completion_request(state, Vec::new());
    request.now = at;
    request.force_new_workflow_task = true;
    request.heartbeat.wire_empty = true;
    request.inline_workflow_task = Some(CompletionWorkflowTaskStart {
        scope: None,
        history_size_bytes: 0,
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
    });
    request
}

#[test]
fn heartbeat_threshold_carry_and_legacy_chain() {
    for original in [None, Some(OffsetDateTime::UNIX_EPOCH), Some(now())] {
        let mut state = state(original);
        for seconds in [1799, 1800] {
            let request = heartbeat(&state, now() + Duration::seconds(seconds));
            let result = kernel()
                .apply(
                    LoadedRun::Existing(state),
                    Command::WorkflowTaskCompleted(request),
                )
                .unwrap();
            assert!(!result.workflow_task_heartbeat_timed_out);
            state = result.next_state;
            assert_eq!(
                state
                    .pending_workflow_task
                    .as_ref()
                    .unwrap()
                    .original_scheduled_at,
                original
            );
        }
        let request = heartbeat(&state, now() + Duration::minutes(31));
        let result = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request),
            )
            .unwrap();
        assert_eq!(
            result.workflow_task_heartbeat_timed_out,
            original == Some(now())
        );
    }
}

#[test]
fn heartbeat_progress_and_nonheartbeat_reset_origin() {
    for progress in 0..3 {
        let state = state(Some(now()));
        let at = now() + Duration::minutes(31);
        let mut request = heartbeat(&state, at);
        match progress {
            0 => {
                request.heartbeat.wire_empty = false;
                request.commands.push(WorkflowCommand::RecordMarker {
                    marker_name: "progress".into(),
                    details: BTreeMap::new(),
                    header: None,
                    failure: None,
                });
            }
            1 => request.heartbeat.wire_empty = false,
            _ => {
                request.force_new_workflow_task = false;
                // Pending messages still demand a successor without force.
            }
        }
        let state = if progress == 2 {
            admit_update(state, "new")
        } else {
            state
        };
        let result = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request),
            )
            .unwrap();
        assert!(!result.workflow_task_heartbeat_timed_out);
        assert_eq!(
            result
                .next_state
                .pending_workflow_task
                .unwrap()
                .original_scheduled_at,
            Some(at)
        );
    }
}

#[test]
fn heartbeat_timeout_reuses_each_task_kinds_timeout_mutation() {
    for (kind, attempt) in [
        (WorkflowTaskType::Normal, 1),
        (WorkflowTaskType::Normal, 2),
        (WorkflowTaskType::Speculative, 1),
    ] {
        for sticky in [false, true] {
            let mut state = state(Some(now()));
            let task = state.pending_workflow_task.as_mut().unwrap();
            task.task_type = kind;
            task.attempt = attempt;
            state.workflow_task_attempt = attempt;
            if sticky {
                state.sticky = Some(StickyAffinity {
                    sticky_queue: TaskQueueName("old-cache".into()),
                    worker_identity: WorkerIdentity("a".into()),
                    schedule_to_start_timeout: Duration::seconds(5),
                });
            }
            let at = now() + Duration::minutes(31);
            let request = heartbeat(&state, at);
            let pending = state.pending_workflow_task.clone().unwrap();
            let timed = kernel()
                .apply(
                    LoadedRun::Existing(state.clone()),
                    Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                        logical_seq: pending.logical_seq,
                        started_event_id: pending.started_event_id.unwrap(),
                        timeout_type: WorkflowTaskTimeoutType::StartToClose,
                        now: at,
                    }),
                )
                .unwrap();
            let result = kernel()
                .apply(
                    LoadedRun::Existing(state),
                    Command::WorkflowTaskCompleted(request),
                )
                .unwrap();
            assert!(result.workflow_task_heartbeat_timed_out);
            assert!(result.history_events.iter().all(|event| !matches!(
                event.kind,
                HistoryEventKind::WorkflowTaskCompleted { .. }
            )));
            assert_eq!(
                &result.history_events[..timed.history_events.len()],
                &timed.history_events[..]
            );
            let next = result.next_state.pending_workflow_task.unwrap();
            assert_eq!(next.original_scheduled_at, Some(at));
            assert_eq!(next.task_type, WorkflowTaskType::Normal);
            assert_eq!(next.attempt, if sticky { 1 } else { attempt + 1 });
            assert!(next.started_event_id.is_some());
        }
    }
}

#[test]
fn heartbeat_paused_force_rejected_after_token_checks() {
    let mut state = state(Some(now()));
    state.status = ExecutionStatus::Paused;
    let mut request = heartbeat(&state, now() + Duration::hours(1));
    assert!(matches!(
        kernel().apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskCompleted(request.clone())
        ),
        Err(Reject::ForcedWorkflowTaskWhilePaused)
    ));
    request.token.attempt += 1;
    assert!(matches!(
        kernel().apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskCompleted(request)
        ),
        Err(Reject::WorkflowTaskTokenMismatch)
    ));
}

#[test]
fn heartbeat_history_rebuild_uses_latest_schedule_not_chain_origin() {
    let start = make_start_request();
    let context = replay_context_from_start(&start);
    let initial = kernel()
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    let original = initial
        .next_state
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .original_scheduled_at;
    assert_eq!(original, Some(now()));
    let replayed = kernel()
        .replay_history_prefix(context, &initial.history_events)
        .unwrap();
    assert_eq!(
        replayed
            .pending_workflow_task
            .unwrap()
            .original_scheduled_at,
        original
    );
    // A history-only rebuild has no heartbeat-chain field to recover.
    let context = replay_context_from_start(&make_start_request());
    let mut events = initial.history_events.to_vec();
    let scheduled = events.last_mut().unwrap();
    assert!(matches!(
        scheduled.kind,
        HistoryEventKind::WorkflowTaskScheduled { .. }
    ));
    scheduled.happened_at += Duration::minutes(20);
    let replayed = kernel().replay_history_prefix(context, &events).unwrap();
    assert_eq!(
        replayed
            .pending_workflow_task
            .unwrap()
            .original_scheduled_at,
        Some(now() + Duration::minutes(20))
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: transient-wft, Property C1
    // The wire progress predicate and strict positive-origin deadline determine expiry.
    #[test]
    fn heartbeat_window_and_compatibility(
        origin_kind in 0u8..4, elapsed in 0i64..3601,
        force in any::<bool>(), wire_empty in any::<bool>(), has_command in any::<bool>(),
    ) {
        let original = match origin_kind { 0 => None, 1 => Some(OffsetDateTime::UNIX_EPOCH), 2 => Some(OffsetDateTime::UNIX_EPOCH - Duration::seconds(1)), _ => Some(now()) };
        let state = admit_update(state(original), "pending");
        let at = now() + Duration::seconds(elapsed);
        let mut request = heartbeat(&state, at);
        request.force_new_workflow_task = force;
        request.heartbeat.wire_empty = wire_empty && !has_command;
        if has_command { request.commands.push(WorkflowCommand::RecordMarker { marker_name: "progress".into(), details: BTreeMap::new(), header: None, failure: None }); }
        let expected = force && wire_empty && !has_command && origin_kind == 3 && elapsed > 1800;
        let result = kernel().apply(LoadedRun::Existing(state), Command::WorkflowTaskCompleted(request)).unwrap();
        prop_assert_eq!(result.workflow_task_heartbeat_timed_out, expected);
        let next = result.next_state.pending_workflow_task.unwrap();
        let carried = force && wire_empty && !has_command && !expected;
        prop_assert_eq!(next.original_scheduled_at, if carried { original } else { Some(at) });
    }
}

#[test]
fn heartbeat_speculative_conversion_preserves_origin() {
    let state = admit_update(make_quiescent_state_after_first_wft(), "update-1");
    let original = state
        .pending_workflow_task
        .as_ref()
        .unwrap()
        .original_scheduled_at;
    assert_eq!(original, Some(now()));
    let signaled = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::Signal(SignalRequest {
                signal_name: "progress".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request: request_context("convert"),
                now: now() + Duration::seconds(5),
            }),
        )
        .unwrap();
    let task = signaled.next_state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(task.task_type, WorkflowTaskType::Normal);
    assert_eq!(task.original_scheduled_at, original);
    let started = start_pending_task(signaled.next_state);
    assert_eq!(
        started
            .next_state
            .pending_workflow_task
            .unwrap()
            .original_scheduled_at,
        original
    );
}

#[test]
fn heartbeat_failure_retry_is_fresh_but_history_only_transient_has_no_origin() {
    let state = state(Some(now() - Duration::minutes(20)));
    let pending = state.pending_workflow_task.clone().unwrap();
    let at = now() + Duration::seconds(5);
    let failed = kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                history_size_bytes: 0,
                advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap(),
                failure_cause: WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
                failure_details: None,
                worker_identity: WorkerIdentity("a".into()),
                request: request_context("fail"),
                now: at,
                reset_reapply: Vec::new(),
                terminate_reason: None,
            }),
        )
        .unwrap();
    let task = failed.next_state.pending_workflow_task.unwrap();
    assert_eq!(task.original_scheduled_at, Some(at));
    assert_eq!(task.scheduled_at, at);

    let start = make_start_request();
    let context = replay_context_from_start(&start);
    let initial = kernel()
        .apply(LoadedRun::Absent, Command::Start(start))
        .unwrap();
    let started = start_pending_task(initial.next_state);
    let mut events = initial.history_events.to_vec();
    events.extend(started.history_events);
    let pending = started.next_state.pending_workflow_task.unwrap();
    events.push(HistoryEvent {
        event_id: events.last().unwrap().event_id + 1,
        happened_at: at,
        kind: HistoryEventKind::WorkflowTaskTimedOut {
            logical_seq: pending.logical_seq,
            scheduled_event_id: pending.scheduled_event_id,
            started_event_id: pending.started_event_id.unwrap(),
            timeout_type: WorkflowTaskTimeoutType::StartToClose,
        },
    });
    let replayed = kernel().replay_history_prefix(context, &events).unwrap();
    assert_eq!(
        replayed
            .pending_workflow_task
            .unwrap()
            .original_scheduled_at,
        None
    );
}

#[derive(Clone, Copy)]
enum PausedTaskProblem {
    Failure,
    StartToClose,
    ScheduleToStart,
}

fn pause_at(state: WorkflowState, at: OffsetDateTime) -> WorkflowState {
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::PauseWorkflow(PauseWorkflowRequest {
                identity: "operator".into(),
                reason: "pause".into(),
                request: RequestContext::unattributed(at),
                now: at,
            }),
        )
        .unwrap()
        .next_state
}

fn unpause_at(state: WorkflowState, at: OffsetDateTime) -> WorkflowState {
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::UnpauseWorkflow(UnpauseWorkflowRequest {
                identity: "operator".into(),
                reason: "resume".into(),
                request: RequestContext::unattributed(at),
                now: at,
            }),
        )
        .unwrap()
        .next_state
}

fn start_at(state: WorkflowState, at: OffsetDateTime) -> WorkflowState {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    kernel()
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: pending.logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: "resume-start".into(),
                history_size_bytes: 0,
                advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                target_version_changed_enabled: false,
                target_deployment_version: None,
                polled_task_queue: state.task_queue.clone(),
                now: at,
            }),
        )
        .unwrap()
        .next_state
}

fn fail_during_pause(
    state: WorkflowState,
    path: PausedTaskProblem,
    at: OffsetDateTime,
) -> WorkflowState {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    let command = match path {
        PausedTaskProblem::Failure => Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
            failure_details: None,
            worker_identity: WorkerIdentity("worker".into()),
            request: RequestContext::unattributed(at),
            now: at,
            history_size_bytes: 0,
            advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
            reset_reapply: Vec::new(),
            terminate_reason: None,
        }),
        PausedTaskProblem::StartToClose | PausedTaskProblem::ScheduleToStart => {
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap_or(0),
                timeout_type: if matches!(path, PausedTaskProblem::StartToClose) {
                    WorkflowTaskTimeoutType::StartToClose
                } else {
                    WorkflowTaskTimeoutType::ScheduleToStart
                },
                now: at,
            })
        }
    };
    kernel()
        .apply(LoadedRun::Existing(state), command)
        .unwrap()
        .next_state
}

fn resumed_window(path: PausedTaskProblem, with_heartbeat: bool, cycles: u32) {
    let mut state = state(Some(now()));
    if matches!(path, PausedTaskProblem::ScheduleToStart) {
        let pending = state.pending_workflow_task.as_mut().unwrap();
        pending.started_at = None;
        pending.started_event_id = None;
        pending.schedule_to_start_deadline = Some(now() + Duration::seconds(5));
        state.sticky = Some(StickyAffinity {
            sticky_queue: TaskQueueName("cache".into()),
            worker_identity: WorkerIdentity("worker".into()),
            schedule_to_start_timeout: Duration::seconds(5),
        });
    }
    for cycle in 0..cycles {
        let base = now() + Duration::hours(i64::from(cycle));
        let failed_at = base + Duration::minutes(2);
        let resumed_at = failed_at + Duration::minutes(30);
        state = fail_during_pause(
            pause_at(state, base + Duration::minutes(1)),
            path,
            failed_at,
        );
        assert!(state.pending_workflow_task.is_none());
        state = unpause_at(state, resumed_at);
        if !with_heartbeat {
            let resumed = state.pending_workflow_task.as_ref().unwrap();
            assert_eq!(resumed.scheduled_at, resumed_at, "unpause schedule time");
            assert_eq!(
                resumed.original_scheduled_at,
                Some(resumed_at),
                "unpause original time"
            );
        }
        let heartbeat_at = failed_at + Duration::minutes(31);
        state = start_at(state, heartbeat_at - Duration::seconds(1));
        if with_heartbeat {
            let request = heartbeat(&state, heartbeat_at);
            let result = kernel()
                .apply(
                    LoadedRun::Existing(state),
                    Command::WorkflowTaskCompleted(request),
                )
                .unwrap();
            assert!(
                !result.workflow_task_heartbeat_timed_out,
                "a heartbeat inside the resumed window must succeed"
            );
            state = result.next_state;
            assert_eq!(
                state
                    .pending_workflow_task
                    .as_ref()
                    .unwrap()
                    .original_scheduled_at,
                Some(resumed_at)
            );
        }
    }
}

macro_rules! paused_window_test {
    ($name:ident, $path:ident, $heartbeat:expr, $cycles:expr) => {
        #[test]
        fn $name() {
            resumed_window(PausedTaskProblem::$path, $heartbeat, $cycles);
        }
    };
}

paused_window_test!(paused_failure_resets_times_at_unpause, Failure, false, 1);
paused_window_test!(
    paused_failure_allows_heartbeat_after_unpause,
    Failure,
    true,
    1
);
paused_window_test!(
    paused_start_to_close_resets_times_at_unpause,
    StartToClose,
    false,
    1
);
paused_window_test!(
    paused_start_to_close_allows_heartbeat_after_unpause,
    StartToClose,
    true,
    1
);
paused_window_test!(
    paused_schedule_to_start_resets_times_at_unpause,
    ScheduleToStart,
    false,
    1
);
paused_window_test!(
    paused_schedule_to_start_allows_heartbeat_after_unpause,
    ScheduleToStart,
    true,
    1
);
paused_window_test!(paused_two_cycles_reset_each_window, Failure, false, 2);
paused_window_test!(paused_two_cycles_allow_each_heartbeat, Failure, true, 2);

#[test]
fn paused_preexisting_schedule_keeps_both_times() {
    let mut state = state(Some(now() - Duration::minutes(1)));
    let task = state.pending_workflow_task.as_mut().unwrap();
    task.started_at = None;
    task.started_event_id = None;
    let state = unpause_at(
        pause_at(state, now() + Duration::minutes(1)),
        now() + Duration::minutes(20),
    );
    let task = state.pending_workflow_task.unwrap();
    assert_eq!(task.scheduled_at, now());
    assert_eq!(
        task.original_scheduled_at,
        Some(now() - Duration::minutes(1))
    );
}

#[test]
fn paused_preexisting_schedule_heartbeat_uses_unchanged_window() {
    for elapsed in [21, 31] {
        let mut state = state(Some(now()));
        let task = state.pending_workflow_task.as_mut().unwrap();
        task.started_at = None;
        task.started_event_id = None;
        let state = unpause_at(
            pause_at(state, now() + Duration::minutes(1)),
            now() + Duration::minutes(20),
        );
        let at = now() + Duration::minutes(elapsed);
        let state = start_at(state, at - Duration::seconds(1));
        let request = heartbeat(&state, at);
        let result = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request),
            )
            .unwrap();
        assert_eq!(result.workflow_task_heartbeat_timed_out, elapsed > 30);
    }
}

#[test]
fn paused_previous_failure_does_not_reset_on_a_second_pause_without_failure() {
    let state = fail_during_pause(
        pause_at(state(Some(now())), now() + Duration::minutes(1)),
        PausedTaskProblem::Failure,
        now() + Duration::minutes(2),
    );
    let first_resume = now() + Duration::minutes(32);
    let state = unpause_at(state, first_resume);
    let state = unpause_at(
        pause_at(state, now() + Duration::minutes(33)),
        now() + Duration::minutes(40),
    );
    let task = state.pending_workflow_task.as_ref().unwrap();
    assert_eq!(task.scheduled_at, first_resume);
    assert_eq!(task.original_scheduled_at, Some(first_resume));
    let at = now() + Duration::minutes(41);
    let state = start_at(state, at - Duration::seconds(1));
    let request = heartbeat(&state, at);
    assert!(
        !kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request)
            )
            .unwrap()
            .workflow_task_heartbeat_timed_out
    );
}
