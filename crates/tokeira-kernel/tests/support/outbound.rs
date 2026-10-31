//! Child confirmation identities remain stable across delivery retries and buffering.

use super::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 13: Reset retry preserves request and semantic order
    #[test]
    fn reset_orders_complete_suffix_and_preserves_reapply_principal(started in any::<bool>(), signals in 1usize..8, seed in any::<u64>()) {
        let now = fixed_now();
        let reset_at = now + Duration::hours(1);
        let mut state = with_pending_wft(with_activity(with_activity(make_open_state(now), "waiting"), "running"), 5, started.then_some(19), 1);
        let activity = state.activities.get_mut("running").unwrap();
        activity.started_event_id = Some(17);
        activity.started_at = Some(now);
        activity.started_identity = Some(WorkerIdentity("activity-worker".into()));
        let origin = EventPrincipal { principal_type: "jwt".into(), name: format!("origin-{seed}") };
        let caller = EventPrincipal { principal_type: "jwt".into(), name: "reset-caller".into() };
        let reapply = (0..signals).map(|index| (HistoryEventKind::WorkflowExecutionSignaled {
            signal_name: format!("signal-{index}"), input: Payloads::default(), header: None, links: Vec::new(),
            request_id: format!("signal-request-{index}"), identity: Some("sender".into()),
        }, Some(origin.clone()))).collect();
        let mut context = RequestContext::unattributed(reset_at);
        context.request_id = RequestId(format!("reset-{seed}")); context.principal = Some(caller.clone());
        let fork = tokeira_kernel::ResetFork {
            base_run_id: RunId::new(), request: ResetRequest {
                fork_event_id: state.last_event_id + 1, new_run_id: state.run_id,
                reapply_exclude_signal: false, reapply_exclude_update: false,
                post_reset_versioning_overrides: vec![VersioningOverrideChange::Set(VersioningOverride::AutoUpgrade), VersioningOverrideChange::Clear],
                expected_current_run_key: None, reason: "reason".into(), request: context.clone(), now: reset_at,
            }, reapply, workflow_task_failure: Payload::new("fork"),
            activity_failures: [("running".into(), Payload::new("activity"))].into_iter().collect(),
            history_size_bytes: 2048, advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        };
        let transition = BasicKernel.prepare_reset_fork(state.clone(), fork.clone()).unwrap();
        prop_assert_eq!(&transition, &BasicKernel.prepare_reset_fork(state, fork).unwrap());
        let events = &transition.history_events;
        let failure_index = usize::from(!started);
        prop_assert!(matches!(events[failure_index].kind, HistoryEventKind::WorkflowTaskFailed { failure_cause: WorkflowTaskFailedCause::ResetWorkflow, base_run_id: Some(_), new_run_id: Some(_), .. }), "expected reset event order");
        prop_assert!(matches!(events[failure_index + 1].kind, HistoryEventKind::ActivityTaskFailed { retry_state: RetryState::NonRetryableFailure, .. }), "expected reset event order");
        for index in 0..signals {
            prop_assert!(matches!(events[failure_index + 2 + index].kind, HistoryEventKind::WorkflowExecutionSignaled { .. }), "expected reset event order");
            prop_assert_eq!(&transition.event_principals[failure_index + 2 + index], &Some(origin.clone()));
        }
        prop_assert!(matches!(events[events.len() - 3].kind, HistoryEventKind::WorkflowExecutionOptionsUpdated { .. }), "expected reset event order");
        prop_assert!(matches!(events[events.len() - 2].kind, HistoryEventKind::WorkflowExecutionOptionsUpdated { .. }), "expected reset event order");
        prop_assert!(matches!(events.last().unwrap().kind, HistoryEventKind::WorkflowTaskScheduled { attempt: 1, .. }), "expected reset event order");
        prop_assert_eq!(&transition.event_principals[events.len() - 2], &Some(caller));
        let state = &transition.next_state;
        prop_assert!(!state.activities.contains_key("running"));
        prop_assert_eq!(state.activities["waiting"].scheduled_at, reset_at);
        prop_assert_eq!(state.activities["waiting"].current_attempt_scheduled_at, Some(reset_at));
        prop_assert_eq!(state.started_at, reset_at);
        prop_assert_eq!(state.first_run_started_at, Some(reset_at));
        prop_assert_eq!(state.outbound.as_ref().unwrap().reset_request_id.as_ref(), Some(&context.request_id));
    }

    // Feature: outbound-dispatch-recovery, Property 11: Child results resolve the correct initiation
    #[test]
    fn child_terminal_identity_and_attributes_survive_buffering(
        buffered in any::<bool>(), started in any::<bool>(), kind in 0u8..5, seed in any::<u64>(),
    ) {
        let now = fixed_now();
        let mut state = with_child(make_open_state(now), "child", 21, ParentClosePolicy::Abandon, started);
        let first = state.children[&WorkflowId("child".into())].child_run_id.unwrap_or_default();
        let final_run = RunId(format!("{:032x}", u128::from(seed) + 1).parse().unwrap());
        if buffered { state = with_pending_wft(state, 45, Some(20), 1); }
        let payload = tokeira_types::Payload::new(seed.to_le_bytes().to_vec());
        let details = Payloads(vec![payload.clone()]);
        let resolution = match kind {
            0 => ChildResolution::Completed { result: details.clone() },
            1 => ChildResolution::Failed { failure: payload.clone() },
            2 => ChildResolution::Canceled,
            3 => ChildResolution::Terminated,
            _ => ChildResolution::TimedOut,
        };
        let metadata = tokeira_kernel::ChildOutcomeMetadata {
            identity: tokeira_kernel::ChildOutcomeIdentity { parent_initiated_event: 21,
                child_first_run: first, child_final_run: final_run },
            retry_state: Some(RetryState::MaximumAttemptsReached), cancellation_details: Some(details.clone()),
        };
        let request = ChildResolvedRequest { child_workflow_id: WorkflowId("child".into()),
            resolved_run_id: Some(final_run), resolution, now };
        let mut wrong = metadata.clone(); wrong.identity.parent_initiated_event += 1;
        prop_assert_eq!(BasicKernel.apply(LoadedRun::Existing(state.clone()), Command::ChildResolvedV2 {
            request: request.clone(), metadata: wrong }), Err(Reject::StaleChildResult));
        if started {
            let mut wrong = metadata.clone(); wrong.identity.child_first_run = RunId::new();
            prop_assert_eq!(BasicKernel.apply(LoadedRun::Existing(state.clone()), Command::ChildResolvedV2 {
                request: request.clone(), metadata: wrong }), Err(Reject::StaleChildResult));
        }
        let mut result = BasicKernel.apply(LoadedRun::Existing(state), Command::ChildResolvedV2 { request, metadata }).unwrap();
        prop_assert!(result.next_state.children.is_empty());
        if buffered {
            let request = completion_request(&result.next_state, Vec::new(), Default::default(), None, None, false, now);
            result = BasicKernel.apply(LoadedRun::Existing(result.next_state), Command::WorkflowTaskCompleted(request)).unwrap();
        }
        let synthesized = result.history_events.iter().find(|event| matches!(event.kind,
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionStarted { .. }));
        prop_assert_eq!(synthesized.is_some(), !started);
        let terminal = result.history_events.iter().find_map(|event| match &event.kind {
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionCompleted { child_run_id, result, started_event_id, .. } => {
                assert_eq!(result, &details); Some((*child_run_id, *started_event_id))
            }
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionFailed { child_run_id, failure, retry_state, started_event_id, .. } => {
                assert_eq!(failure, &payload); assert_eq!(*retry_state, RetryState::MaximumAttemptsReached);
                Some((*child_run_id, *started_event_id))
            }
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionCanceled { child_run_id, details: got, started_event_id, .. } => {
                assert_eq!(got, &Some(details.clone())); Some((*child_run_id, *started_event_id))
            }
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionTerminatedV2 { child_run_id, started_event_id, .. } =>
                Some((*child_run_id, *started_event_id)),
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionTimedOutV2 { child_run_id, retry_state, started_event_id, .. } => {
                assert_eq!(*retry_state, RetryState::MaximumAttemptsReached); Some((*child_run_id, *started_event_id))
            }
            _ => None,
        }).unwrap();
        prop_assert_eq!(terminal.0, Some(final_run));
        prop_assert_eq!(terminal.1, synthesized.map_or(22, |event| event.event_id));
    }

    // Feature: outbound-dispatch-recovery, Property 7: External delivery records only definitive outcomes.
    #[test]
    fn recovered_external_metadata_survives_buffering(
        control in "[a-z0-9]{0,64}", completed in 1i64..20, kind in 0u8..3, buffered in any::<bool>(),
    ) {
        let now = fixed_now();
        let metadata = tokeira_kernel::ExternalResolutionMetadata { workflow_task_completed_event_id: completed, control };
        let mut state = if kind == 2 { with_pending_external_cancel(make_open_state(now), 21) }
            else { with_pending_external_signal(make_open_state(now), 21) };
        if buffered { state = with_pending_wft(state, 45, Some(20), 1); }
        let command = if kind == 2 {
            Command::ExternalCancelResolvedV2 { metadata: metadata.clone(), request: ExternalCancelResolvedRequest {
                initiated_event_id: 21, result: ExternalCancelResult::Failed { cause: "NAMESPACE_NOT_FOUND".into() },
                target_namespace: Some("destination".into()), now,
            } }
        } else {
            Command::ExternalSignalResolvedV2 { metadata: metadata.clone(), request: ExternalSignalResolvedRequest {
                initiated_event_id: 21, result: if kind == 0 { ExternalSignalResult::Signaled }
                    else { ExternalSignalResult::Failed { cause: "SIGNAL_COUNT_LIMIT_EXCEEDED".into() } },
                target_namespace: Some("destination".into()), now,
            } }
        };
        let mut transition = BasicKernel.apply(LoadedRun::Existing(state), command).unwrap();
        prop_assert!(transition.next_state.pending_external_signals.is_empty());
        prop_assert!(transition.next_state.pending_external_cancels.is_empty());
        if buffered {
            prop_assert!(transition.history_events.is_empty());
            let request = completion_request(&transition.next_state, Vec::new(), Default::default(), None, None, false, now);
            transition = BasicKernel.apply(LoadedRun::Existing(transition.next_state), Command::WorkflowTaskCompleted(request)).unwrap();
        }
        let resolved = transition.history_events.iter().filter_map(|event| event.kind.external_resolution_metadata()).collect::<Vec<_>>();
        prop_assert_eq!(resolved, vec![&metadata]);
    }

    // Feature: outbound-dispatch-recovery, Property 6: Child start precedes execution exactly once.
    #[test]
    fn parent_confirmation_gates_signals_and_original_delay(
        delay_seconds in 0i64..60, elapsed_seconds in 0i64..120, signal_count in 0usize..4,
    ) {
        let now = fixed_now();
        let parent = RunKey::new();
        let mut start = advice_start_request(now, None);
        start.parent_run_key = Some(parent);
        start.parent_run_id = Some(RunId::new());
        start.parent_workflow_id = Some(WorkflowId("parent".into()));
        start.parent_namespace_id = Some(start.namespace_id);
        start.parent_initiated_event_id = 17;
        start.workflow_start_delay = Some(Duration::seconds(delay_seconds));
        let child_id = start.run_id;
        let created = BasicKernel.apply(LoadedRun::Absent, Command::Start(start)).unwrap();
        prop_assert!(created.next_state.pending_workflow_task.is_none());
        prop_assert!(created.dispatch_ops.is_empty());
        let mut state = created.next_state;
        for index in 0..signal_count {
            let signaled = BasicKernel.apply(LoadedRun::Existing(state), Command::Signal(SignalRequest {
                signal_name: "held".into(), input: Payloads::default(), header: None,
                links: Vec::new(),
                request: RequestContext { request_id: RequestId(format!("signal-{index}")), ..RequestContext::unattributed(now) }, now,
            })).unwrap();
            prop_assert!(signaled.next_state.pending_workflow_task.is_none());
            state = signaled.next_state;
        }
        let release = tokeira_kernel::ParentChildStartRecordedRequest {
            parent_run_key: parent, initiating_event: 17, first_child_run_id: child_id,
            parent_started_event_id: 22, now: now + Duration::seconds(elapsed_seconds),
        };
        let mut wrong = release.clone();
        wrong.initiating_event += 1;
        prop_assert_eq!(BasicKernel.apply(LoadedRun::Existing(state.clone()), Command::ParentChildStartRecorded(wrong)), Err(Reject::StaleChildRelease));
        let released = BasicKernel.apply(LoadedRun::Existing(state), Command::ParentChildStartRecorded(release.clone())).unwrap();
        prop_assert_eq!(released.next_state.started_at, now);
        prop_assert_eq!(released.next_state.pending_workflow_task.is_some(), elapsed_seconds >= delay_seconds);
        let again = BasicKernel.apply(LoadedRun::Existing(released.next_state), Command::ParentChildStartRecorded(release)).unwrap();
        prop_assert!(again.is_noop());
    }

    // Feature: outbound-dispatch-recovery, Property 6: Child start precedes execution exactly once.
    #[test]
    fn repeated_confirmation_is_event_free_and_buffered_proof_waits_for_flush(
        buffered in any::<bool>(), copies in 1usize..8, seed in any::<u64>(),
    ) {
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut state = with_child(make_open_state(now), "child", 21, ParentClosePolicy::Abandon, false);
        if buffered { state = with_pending_wft(state, 45, Some(20), 1); }
        let child_run_id = RunId(format!("{:032x}", u128::from(seed) + 1).parse().unwrap());
        let command = Command::ChildStartConfirmed(ChildStartConfirmedRequest {
            child_workflow_id: WorkflowId("child".into()), initiated_event_id: 21,
            result: ChildStartResult::Started { child_run_id, workflow_type: WorkflowType("child".into()) }, now,
        });
        let first = BasicKernel.apply(LoadedRun::Existing(state), command.clone()).unwrap();
        let confirmed = first.next_state;
        let release = &confirmed.outbound.as_ref().unwrap().child_releases[&21];
        prop_assert_eq!(release.child_run_id, child_run_id);
        prop_assert_eq!(release.parent_started_event_id.is_none(), buffered);
        for _ in 0..copies {
            let repeated = BasicKernel.apply(LoadedRun::Existing(confirmed.clone()), command.clone()).unwrap();
            prop_assert!(repeated.is_noop());
            prop_assert_eq!(repeated.next_state, confirmed.clone());
        }
        let stale = Command::ChildStartConfirmed(ChildStartConfirmedRequest {
            child_workflow_id: WorkflowId("child".into()), initiated_event_id: 21,
            result: ChildStartResult::Failed { cause: "WORKFLOW_ALREADY_EXISTS".into() }, now,
        });
        prop_assert_eq!(BasicKernel.apply(LoadedRun::Existing(confirmed.clone()), stale), Err(Reject::ChildStartAlreadyConfirmed(WorkflowId("child".into()))));
        if buffered {
            let request = completion_request(&confirmed, Vec::new(), Default::default(), None, None, false, now);
            let flushed = BasicKernel.apply(LoadedRun::Existing(confirmed), Command::WorkflowTaskCompleted(request)).unwrap();
            let started_id = flushed.history_events.iter().find_map(|event| matches!(event.kind, tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionStarted { .. }).then_some(event.event_id)).unwrap();
            prop_assert_eq!(flushed.next_state.outbound.unwrap().child_releases[&21].parent_started_event_id, Some(started_id));
        }
    }

    // Feature: outbound-dispatch-recovery, Property 8: Nexus phase and attempt fencing
    #[test]
    fn late_nexus_results_cannot_acknowledge_a_new_generation(old in 0u32..1000) {
        let now = fixed_now();
        let mut state = with_pending_nexus_operation(make_open_state(now), "nexus");
        let pending = state.pending_nexus_operations.get_mut("nexus").unwrap();
        pending.attempt = old + 1;
        pending.cancellation = Some(tokeira_kernel::NexusOperationCancellation {
            requested_event_id: 25,
            requested_time: Some(now),
            state: tokeira_kernel::NexusOperationCancellationState::Scheduled,
            attempt: old + 1,
            last_attempt_complete_time: Some(now),
            last_attempt_failure: None,
            next_attempt_at: None,
        });
        let start = NexusOperationResolvedRequest {
            operation_id: "nexus".into(), scheduled_event_id: 12,
            expected_attempt: Some(old),
            resolution: NexusResolution::Started { operation_token: "token".into(), links: Vec::new() }, now,
        };
        let cancel = tokeira_kernel::NexusCancellationAttemptedRequest {
            operation_id: "nexus".into(), scheduled_event_id: 12, requested_event_id: 25,
            expected_attempt: old, outcome: tokeira_kernel::NexusCancellationAttemptOutcome::Succeeded, now,
        };
        let stale = || Err(tokeira_kernel::Reject::StaleNexusAttempt { operation_id: "nexus".into(), expected_attempt: old + 1 });
        prop_assert_eq!(kernel().apply(LoadedRun::Existing(state.clone()), Command::NexusOperationResolved(start.clone())), stale());
        prop_assert_eq!(kernel().apply(LoadedRun::Existing(state.clone()), Command::NexusCancellationAttempted(cancel.clone())), stale());
        let current = NexusOperationResolvedRequest { expected_attempt: Some(old + 1), ..start };
        let accepted = kernel().apply(LoadedRun::Existing(state.clone()), Command::NexusOperationResolved(current.clone())).unwrap();
        prop_assert!(accepted.next_state.pending_nexus_operations["nexus"].started);
        prop_assert!(kernel().apply(LoadedRun::Existing(accepted.next_state), Command::NexusOperationResolved(current.clone())).is_err());
        let current_cancel = tokeira_kernel::NexusCancellationAttemptedRequest { expected_attempt: old + 1, ..cancel };
        let acknowledged = kernel().apply(LoadedRun::Existing(state.clone()), Command::NexusCancellationAttempted(current_cancel)).unwrap();
        prop_assert_eq!(acknowledged.next_state.pending_nexus_operations["nexus"].cancellation.as_ref().unwrap().attempt, old + 2);
        state.pending_nexus_operations.get_mut("nexus").unwrap().next_attempt_at = Some(now + Duration::seconds(1));
        prop_assert!(kernel().apply(LoadedRun::Existing(state), Command::NexusOperationResolved(current)).is_err());
    }
}
