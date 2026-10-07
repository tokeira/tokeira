//! Incarnation traces preserve the observable transient-task contract while
//! delayed offers are rejected across retries, resume, and supersession.

use super::*;

fn start(state: &WorkflowState) -> Command {
    Command::WorkflowTaskStarted(advice_start_wft_request(
        state,
        0,
        tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        fixed_now(),
    ))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: workflow-dispatch, Property 2: Incarnation fencing and observable retry preservation
    // Every new generation rejects all older offers without changing retry history or attempts.
    #[test]
    fn retained_retries_resume_and_supersession_fence_old_offers(
        actions in prop::collection::vec(0u8..4, 1..16),
        initial in 1u64..10_000,
    ) {
        let kernel = BasicKernel;
        let mut state = with_pending_wft(make_open_state(fixed_now()), initial, None, 1);
        let mut old_offers = Vec::new();
        for action in actions {
            let offer = start(&state);
            let old = state.pending_workflow_task.as_ref().unwrap().clone();
            old_offers.push(offer.clone());
            let transition = match action {
                0 => {
                    // Intervening pause/options history converts a transient
                    // task back to attempt 1 when it starts (v1.31.0).
                    let start_attempt = if old.attempt > 1 && old.scheduled_event_id != state.last_event_id + 1 { 1 } else { old.attempt };
                    let started = kernel.apply(LoadedRun::Existing(state.clone()), offer.clone()).unwrap();
                    let repeated = kernel.apply(LoadedRun::Existing(started.next_state.clone()), offer);
                    prop_assert!(matches!(repeated, Err(Reject::WorkflowTaskAlreadyStarted { .. })), "repeat start must be rejected");
                    let request = advice_wft_failed_request(&started.next_state,
                        WorkflowTaskFailedCause::NonDeterminismError, 0,
                        tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0, fixed_now());
                    let retry = kernel.apply(LoadedRun::Existing(started.next_state), Command::WorkflowTaskFailed(request)).unwrap();
                    prop_assert!(!retry.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskScheduled { .. })), "retained retry must suppress scheduling history");
                    let pending = retry.next_state.pending_workflow_task.as_ref().unwrap();
                    prop_assert_eq!(pending.attempt, start_attempt + 1);
                    prop_assert_eq!(pending.scheduled_event_id, retry.next_state.last_event_id + 1);
                    prop_assert_eq!(pending.scheduled_at, old.scheduled_at);
                    retry
                }
                1 => {
                    let paused = kernel.apply(LoadedRun::Existing(state.clone()), Command::PauseWorkflow(PauseWorkflowRequest {
                        identity: "operator".into(), reason: "pause".into(),
                        request: request_context("pause", fixed_now()), now: fixed_now(),
                    })).unwrap().next_state;
                    prop_assert!(matches!(kernel.apply(LoadedRun::Existing(paused.clone()), offer), Err(Reject::WorkflowPaused)));
                    let resumed = kernel.apply(LoadedRun::Existing(paused), Command::UnpauseWorkflow(UnpauseWorkflowRequest {
                        identity: "operator".into(), reason: "resume".into(),
                        request: request_context("resume", fixed_now()), now: fixed_now(),
                    })).unwrap();
                    let pending = resumed.next_state.pending_workflow_task.as_ref().unwrap();
                    prop_assert_eq!(pending.attempt, old.attempt);
                    prop_assert_eq!(pending.scheduled_at, old.scheduled_at);
                    resumed
                }
                2 => kernel.apply(LoadedRun::Existing(state.clone()), workflow_priority_update(Priority {
                    priority_key: if state.priority.as_ref().is_some_and(|priority| priority.priority_key == 1) { 2 } else { 1 },
                    fairness_key: "tenant".into(), fairness_weight: 1.0,
                }, fixed_now())).unwrap(),
                _ => {
                    let started = kernel.apply(LoadedRun::Existing(state.clone()), offer).unwrap().next_state;
                    let pending = started.pending_workflow_task.as_ref().unwrap();
                    let request = WorkflowTaskTimedOutRequest {
                        logical_seq: pending.logical_seq, started_event_id: pending.started_event_id.unwrap(),
                        timeout_type: WorkflowTaskTimeoutType::StartToClose, now: fixed_now(),
                    };
                    kernel.apply(LoadedRun::Existing(started), Command::WorkflowTaskTimedOut(request)).unwrap()
                }
            };
            state = transition.next_state;
            let current = state.pending_workflow_task.as_ref().unwrap();
            prop_assert!(current.logical_seq > old.logical_seq);
            prop_assert!(state.next_workflow_task_seq > current.logical_seq);
            for old_offer in &old_offers {
                prop_assert!(matches!(kernel.apply(LoadedRun::Existing(state.clone()), old_offer.clone()), Err(Reject::WorkflowTaskSeqMismatch { .. })), "old generation must be stale");
            }
            prop_assert!(transition.dispatch_ops.iter().any(|op| matches!(op,
                DispatchOp::EnqueueWorkflowTask { logical_seq, .. } if *logical_seq == current.logical_seq)), "publication must carry the new generation");
        }
    }
}

#[test]
fn sequence_exhaustion_rejects_without_a_transition() {
    let kernel = BasicKernel;
    let mut state = with_pending_wft(make_open_state(fixed_now()), i64::MAX as u64, None, 1);
    let started = kernel
        .apply(LoadedRun::Existing(state.clone()), start(&state))
        .unwrap()
        .next_state;
    let failure = advice_wft_failed_request(
        &started,
        WorkflowTaskFailedCause::NonDeterminismError,
        0,
        tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        fixed_now(),
    );
    assert!(matches!(
        kernel.apply(
            LoadedRun::Existing(started),
            Command::WorkflowTaskFailed(failure)
        ),
        Err(Reject::WorkflowTaskSequenceExhausted)
    ));
    state.pending_workflow_task = None;
    let signal = Command::Signal(SignalRequest {
        signal_name: "wake".into(),
        input: Payloads::default(),
        links: Vec::new(),
        header: None,
        request: request_context("exhaustion", fixed_now()),
        now: fixed_now(),
    });
    assert!(matches!(
        kernel.apply(LoadedRun::Existing(state), signal),
        Err(Reject::WorkflowTaskSequenceExhausted)
    ));
}
