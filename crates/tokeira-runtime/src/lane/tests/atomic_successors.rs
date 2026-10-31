//! Successor preparation preserves lineage and fails before publication.

use super::*;
use crate::successor::{
    SuccessorCause, SuccessorPlan, SuccessorStartedEvent, SuccessorTiming, build_successor_start,
};
use tokeira_kernel::{HistoryEventKind, RetryState};
use tokeira_types::{Payload, RetryPolicy};

fn closing(state: &WorkflowState, kind: u8) -> Transition {
    let mut transition = ContinueAsNewKernel::continued_as_new()
        .apply(LoadedRun::Existing(state.clone()), sample_command("close"))
        .unwrap();
    let HistoryEventKind::WorkflowExecutionContinuedAsNew { new_run_id, .. } =
        transition.history_events[0].kind
    else {
        unreachable!()
    };
    let event = match kind {
        1 => HistoryEventKind::WorkflowExecutionFailed {
            attempt: state.attempt,
            workflow_task_completed_event_id: 0,
            failure: Payload::new(b"failure"),
            retry_state: RetryState::InProgress,
            new_execution_run_id: Some(new_run_id),
        },
        2 => HistoryEventKind::WorkflowExecutionCompleted {
            workflow_task_completed_event_id: 0,
            result: Payloads(vec![Payload::new(b"result")]),
            new_execution_run_id: Some(new_run_id),
        },
        3 | 4 => HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: tokeira_kernel::WorkflowTimeoutType::RunTimeout,
            retry_state: RetryState::InProgress,
            new_execution_run_id: Some(new_run_id),
        },
        _ => return transition,
    };
    transition.next_state.status = match kind {
        1 => ExecutionStatus::Failed,
        2 => ExecutionStatus::Completed,
        _ => ExecutionStatus::TimedOut,
    };
    transition.next_state.close_failure = (kind == 1).then(|| Payload::new(b"failure"));
    transition.next_state.close_result =
        (kind == 2).then(|| Payloads(vec![Payload::new(b"result")]));
    transition.history_events[0].kind = event;
    transition
}

fn plan(
    state: &WorkflowState,
    kind: u8,
    input: Payloads,
    version: Option<WorkflowVersioningInfo>,
    now: OffsetDateTime,
) -> Option<SuccessorPlan> {
    let started = Box::new(SuccessorStartedEvent {
        input,
        versioning_info: version,
        cron_schedule: Some("* * * * *".into()),
        last_completion_result: state.last_completion_result.clone(),
        ..Default::default()
    });
    let cause = if kind >= 3 {
        SuccessorCause::RunTimeout
    } else {
        SuccessorCause::WorkflowTask
    };
    let timing = SuccessorTiming {
        now,
        backoff: Duration::seconds(2),
    };
    Some(match kind {
        0 => SuccessorPlan::ContinueAsNew { links: Vec::new() },
        1 | 3 => SuccessorPlan::Retry {
            started,
            timing,
            cause,
        },
        _ => SuccessorPlan::Cron {
            started,
            timing,
            cause,
        },
    })
}

fn policy() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Duration::seconds(2),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(30)),
        maximum_attempts: 8,
        non_retryable_error_types: Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: successor-semantics, Property 5: corrected starts keep their identity and fields across OCC and fallback.
    #[test]
    fn corrected_start_retains_commit_safety(kind in 0u8..5, conflicts in 0usize..4, outcome in 0u8..4, bytes in prop::collection::vec(any::<u8>(), 0..48)) {
        Runtime::new().unwrap().block_on(async {
            let mut state = sample_state(RunKey::new());
            state.started_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(20);
            state.retry_policy = Some(policy());
            let transition = closing(&state, kind);
            let kernel = FrozenClose(transition.clone());
            let mut plan = plan(&state, kind, Payloads(vec![Payload::new(bytes.clone())]), None, state.started_at);
            let header = Some(tokeira_types::Headers([("original".into(), Payload::new(bytes.clone()))].into()));
            let links = vec![tokeira_kernel::Link::BatchJob { job_id: format!("job-{bytes:?}") }];
            match plan.as_mut().unwrap() {
                SuccessorPlan::Retry { started, .. } | SuccessorPlan::Cron { started, .. } => {
                    started.header = header.clone(); started.links = links.clone();
                    started.memo = Memo([("memo".into(), Payload::new(bytes))].into());
                    started.search_attributes = SearchAttributes([("key".into(), tokeira_types::SearchAttrValue::Int(7))].into());
                }
                SuccessorPlan::ContinueAsNew { links: original, .. } => *original = links.clone(),
            }
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), vec![CommitBehavior::Applied]);
            repo.state.lock().await.pair_behaviors = std::iter::repeat_n(PairBehavior::Conflict, conflicts).chain([match outcome { 0 => PairBehavior::Applied, 1 => PairBehavior::SizeRefused, 2 => PairBehavior::Duplicate, _ => PairBehavior::Error }]).collect();
            let mut effect = None;
            let result = handle_message_with_successor(&kernel, &repo, &test_shard_owner(), &UpdateRegistry::new(), state.run_key, sample_command("close"), plan.as_ref(), &mut effect, 5, &mut LaneCache::new(&LaneConfig::default()), &mut None).await;
            let recorded = repo.state.lock().await;
            prop_assert_eq!(recorded.pairs.len(), conflicts + 1);
            let first = &recorded.pairs[0].1;
            for (_, successor) in &recorded.pairs { prop_assert_eq!(successor, first); }
            let inputs = crate::successor::started_event_inputs(&first.history_events[0]).unwrap();
            prop_assert_eq!(inputs.links, links);
            if kind != 0 { prop_assert_eq!(inputs.header, header); }
            match outcome {
                0 => {
                    prop_assert!(matches!(result, Ok((CommitResult::Applied { .. }, _, _))), "applied close");
                    let Some(SuccessorEffect::Committed(actual)) = effect else { panic!("pair effects") };
                    prop_assert_eq!(actual.state, first.next_state.clone());
                    prop_assert_eq!(recorded.commit_calls, 0);
                }
                1 => {
                    prop_assert!(matches!(result, Ok((CommitResult::Applied { .. }, _, _))), "applied close");
                    let Some(SuccessorEffect::Deferred(start)) = effect else { panic!("fallback start") };
                    let fallback = tokeira_kernel::BasicKernel.apply(LoadedRun::Absent, Command::Start(*start)).unwrap();
                    prop_assert_eq!(&fallback.next_state, &first.next_state);
                    prop_assert_eq!(&fallback.history_events, &first.history_events);
                    prop_assert_eq!(&fallback.timer_ops, &first.timer_ops);
                    prop_assert_eq!(&fallback.dispatch_ops, &first.dispatch_ops);
                    prop_assert_eq!(recorded.commit_calls, 1);
                }
                _ => {
                    prop_assert!(if outcome == 2 { matches!(result, Ok((CommitResult::Duplicate, _, _))) } else { result.is_err() }, "duplicate or rejected pair");
                    prop_assert!(effect.is_none());
                    prop_assert_eq!(&recorded.loaded, &LoadedRun::Existing(state));
                    prop_assert_eq!(recorded.commit_calls, 0);
                }
            }
            Ok(())
        })?;
    }
    // Feature: atomic-successors, Property 3: identity, lineage and versioning remain unchanged.
    #[test]
    fn successor_preserves_identity_and_lineage(kind in 0u8..5, attempt in 1u32..7, bytes in prop::collection::vec(any::<u8>(), 0..128), child in any::<bool>(), versioned in any::<bool>()) {
        let mut state = sample_state(RunKey::new());
        let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(20);
        state.started_at = now;
        state.retry_policy = Some(policy()); state.attempt = attempt;
        state.last_completion_result = Some(Payloads(vec![Payload::new(bytes.clone())]));
        state.first_execution_run_id = Some(RunId::new()); state.first_run_started_at = Some(now - Duration::hours(1));
        if child { state.parent_run_key = Some(RunKey::new()); state.parent_run_id = Some(RunId::new()); state.parent_workflow_id = Some(WorkflowId("parent".into())); state.root_run_id = Some(RunId::new()); }
        let version = versioned.then(|| WorkflowVersioningInfo { behavior: VersioningBehavior::Pinned, deployment_version: Some(WorkerDeploymentVersionRef { deployment_name: "deployment".into(), build_id: "v1".into() }), ..Default::default() });
        state.versioning_info = version.clone();
        let input = Payloads(vec![Payload::new(bytes)]);
        let transition = closing(&state, kind);
        let plan = plan(&state, kind, input.clone(), version.clone(), now);
        let actual = build_successor_start(&transition, plan.as_ref(), now).unwrap().unwrap();
        prop_assert_eq!(actual.namespace_id, state.namespace_id);
        prop_assert_eq!(&actual.workflow_id, &state.workflow_id);
        prop_assert_eq!(actual.continued_execution_run_id, Some(state.run_id));
        prop_assert_eq!(actual.first_execution_run_id, state.first_execution_run_id);
        prop_assert_eq!(actual.first_run_started_at, state.first_run_started_at);
        prop_assert_eq!(actual.parent_run_key, state.parent_run_key);
        prop_assert_eq!(actual.parent_run_id, state.parent_run_id);
        prop_assert_eq!(actual.root_run_id, state.root_run_id);
        prop_assert_eq!(actual.attempt, if kind == 1 || kind == 3 { attempt + 1 } else { 1 });
        if kind == 0 {
            let expected = crate::successor::legacy_tests::continue_as_new_start(&transition.next_state, &transition.history_events, now).unwrap();
            prop_assert_eq!(actual, expected);
        } else {
            prop_assert_eq!(actual.input, input);
            prop_assert_eq!(actual.retry_policy, state.retry_policy);
            prop_assert_eq!(actual.now, now);
            prop_assert_eq!(actual.request.received_at, now);
            prop_assert_eq!(actual.cron_schedule.as_deref(), Some("* * * * *"));
            prop_assert_eq!(actual.inherited_versioning_info, if kind == 1 || kind == 3 { version } else { None });
        }
    }

    // Feature: atomic-successors, Property 5: A rejected successor fails the close.
    #[test]
    fn rejected_successor_leaves_close_uncommitted(kind in 0u8..5, attempt in 1u32..7) {
        Runtime::new().unwrap().block_on(async {
            let mut state = sample_state(RunKey::new()); state.retry_policy = Some(policy()); state.attempt = attempt;
            let run_key = state.run_key;
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), vec![CommitBehavior::Applied]);
            let kernel = RejectSuccessor { kind };
            let plan = plan(&state, kind, Payloads::default(), None, state.started_at);
            let result = Box::pin(handle_message_with_successor(&kernel, &repo, &test_shard_owner(), &UpdateRegistry::new(), run_key, sample_command("close"), plan.as_ref(), &mut None, 5, &mut LaneCache::new(&LaneConfig::default()), &mut None)).await;
            prop_assert!(result.is_err(), "a rejected successor must fail the close command");
            let (_, commits, loaded) = repo.snapshot().await;
            prop_assert_eq!(commits, 0);
            prop_assert_eq!(loaded, LoadedRun::Existing(state));
            Ok(())
        })?;
    }
}

struct FrozenClose(Transition);
impl Kernel for FrozenClose {
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject> {
        if matches!(command, Command::Start(_)) {
            tokeira_kernel::BasicKernel.apply(loaded, command)
        } else {
            Ok(self.0.clone())
        }
    }
}

struct RejectSuccessor {
    kind: u8,
}
impl Kernel for RejectSuccessor {
    fn apply(&self, loaded: LoadedRun, command: Command) -> Result<Transition, Reject> {
        if matches!(command, Command::Start(_)) {
            assert_eq!(loaded, LoadedRun::Absent);
            return Err(Reject::WorkflowPaused);
        }
        let LoadedRun::Existing(state) = loaded else {
            panic!("close needs a run")
        };
        Ok(closing(&state, self.kind))
    }
}
