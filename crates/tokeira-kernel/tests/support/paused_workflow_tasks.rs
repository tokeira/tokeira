//! Generated pause/failure/resume traces shared by the suppression and wakeup properties.

use super::*;

fn start(state: WorkflowState, at: OffsetDateTime) -> WorkflowState {
    let pending = state.pending_workflow_task.as_ref().unwrap();
    let request = StartWorkflowTaskRequest {
        logical_seq: pending.logical_seq,
        worker_identity: WorkerIdentity("worker".into()),
        request_id: "start".into(),
        history_size_bytes: 0,
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        deployment_transition: None,
        deployment_transition_revision_number: None,
        target_version_changed_enabled: false,
        target_deployment_version: None,
        polled_task_queue: state.current_workflow_task_queue().clone(),
        now: at,
    };
    kernel()
        .apply(
            LoadedRun::Existing(state),
            Command::WorkflowTaskStarted(request),
        )
        .unwrap()
        .next_state
}

pub(super) fn trace(steps: &[(u8, bool, u8)]) {
    let mut state = with_pending_wft(make_open_state(fixed_now()), 77, None, 1);
    for (index, &(problem, sticky, signals)) in steps.iter().enumerate() {
        let at = fixed_now() + Duration::minutes(index as i64);
        let sticky = sticky || problem == 1;
        if sticky {
            state.sticky = Some(StickyAffinity {
                sticky_queue: TaskQueueName("cache".into()),
                worker_identity: WorkerIdentity("worker".into()),
                schedule_to_start_timeout: Duration::seconds(5),
            });
        }
        if problem == 1 {
            state
                .pending_workflow_task
                .as_mut()
                .unwrap()
                .schedule_to_start_deadline = Some(at + Duration::seconds(5));
        } else {
            state = start(state, at);
        }
        state = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::PauseWorkflow(PauseWorkflowRequest {
                    identity: "operator".into(),
                    reason: "pause".into(),
                    request: request_context("pause", at),
                    now: at,
                }),
            )
            .unwrap()
            .next_state;
        for _ in 0..signals {
            state = kernel()
                .apply(
                    LoadedRun::Existing(state),
                    Command::Signal(SignalRequest {
                        signal_name: "signal".into(),
                        input: Payloads::default(),
                        header: None,
                        links: Vec::new(),
                        request: request_context("signal", at),
                        now: at,
                    }),
                )
                .unwrap()
                .next_state;
        }
        let pending = state.pending_workflow_task.as_ref().unwrap();
        let expected_attempt = if sticky {
            1
        } else {
            state.workflow_task_attempt + 1
        };
        let expected_count = state.workflow_task_attempts_since_last_success + u32::from(!sticky);
        let command = if problem == 0 {
            Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap(),
                failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                request: request_context("failed", at),
                now: at,
                reset_reapply: Vec::new(),
                history_size_bytes: 0,
                advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
                terminate_reason: None,
            })
        } else {
            Command::WorkflowTaskTimedOut(WorkflowTaskTimedOutRequest {
                logical_seq: pending.logical_seq,
                started_event_id: pending.started_event_id.unwrap_or(0),
                timeout_type: if problem == 1 {
                    WorkflowTaskTimeoutType::ScheduleToStart
                } else {
                    WorkflowTaskTimeoutType::StartToClose
                },
                now: at + Duration::seconds(10),
            })
        };
        let failed = kernel().apply(LoadedRun::Existing(state), command).unwrap();
        assert!(failed.next_state.pending_workflow_task.is_none());
        assert!(
            failed
                .dispatch_ops
                .iter()
                .all(|op| !matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
        );
        assert_eq!(failed.next_state.workflow_task_attempt, expected_attempt);
        assert_eq!(
            failed.next_state.workflow_task_attempts_since_last_success,
            expected_count
        );
        assert!(failed.next_state.buffered_events.is_empty());
        assert!(failed.next_state.sticky.is_none());
        let resume_at = at + Duration::seconds(20);
        let resumed = kernel()
            .apply(
                LoadedRun::Existing(failed.next_state),
                Command::UnpauseWorkflow(UnpauseWorkflowRequest {
                    identity: "operator".into(),
                    reason: "resume".into(),
                    request: request_context("resume", resume_at),
                    now: resume_at,
                }),
            )
            .unwrap();
        let pending = resumed.next_state.pending_workflow_task.as_ref().unwrap();
        assert_eq!(pending.attempt, expected_attempt);
        assert_eq!(pending.scheduled_at, resume_at);
        assert_eq!(pending.original_scheduled_at, Some(resume_at));
        assert_eq!(
            resumed
                .dispatch_ops
                .iter()
                .filter(|op| matches!(op, DispatchOp::EnqueueWorkflowTask { .. }))
                .count(),
            1
        );
        assert_eq!(
            resumed
                .history_events
                .iter()
                .filter(|event| matches!(
                    event.kind,
                    HistoryEventKind::WorkflowTaskScheduled { .. }
                ))
                .count(),
            usize::from(expected_attempt == 1)
        );
        state = start(resumed.next_state, resume_at + Duration::seconds(1));
        let request = completion_request(
            &state,
            Vec::new(),
            Default::default(),
            None,
            None,
            true,
            resume_at + Duration::seconds(2),
        );
        state = kernel()
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request),
            )
            .unwrap()
            .next_state;
    }
}
