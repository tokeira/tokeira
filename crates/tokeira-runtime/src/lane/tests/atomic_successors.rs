//! Successor preparation preserves old builders and fails before publication.

use super::*;
use crate::{
    runtime::workflow_task::{
        build_cron_successor_start, build_retry_successor_start, workflow_run_timeout_failure,
    },
    successor::{SuccessorPlan, build_successor_start},
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
    match kind {
        0 => None,
        1 | 3 => Some(SuccessorPlan::Retry {
            input,
            started_versioning_info: version.map(Box::new),
            timeout_prior_outcome: (kind == 3)
                .then(|| (state.close_failure.clone(), state.close_result.clone())),
        }),
        _ => Some(SuccessorPlan::Cron {
            schedule: "* * * * *".into(),
            input,
            now,
            timeout: kind == 4,
        }),
    }
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
    // Feature: atomic-successors, Property 3: The successor is today's.
    #[test]
    fn successor_matches_separate_start(kind in 0u8..5, attempt in 1u32..7, bytes in prop::collection::vec(any::<u8>(), 0..128), child in any::<bool>(), versioned in any::<bool>()) {
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
        // The former timeout path built retry starts from its pre-close load;
        // completion paths reloaded the committed close.
        let committed = &transition.next_state;
        let mut expected = match kind {
            0 => crate::successor::legacy_tests::continue_as_new_start(committed, &transition.history_events, now).unwrap(),
            1 | 3 => build_retry_successor_start(if kind == 3 { &state } else { committed }, version.as_ref(), &policy(), input, actual.run_id),
            _ => build_cron_successor_start(committed, "* * * * *".into(), input, actual.run_id, now, now,
                if kind == 4 { Some(workflow_run_timeout_failure()) } else { committed.close_failure.clone() },
                if kind == 4 { state.last_completion_result.clone() } else { committed.close_result.clone().or_else(|| committed.last_completion_result.clone()) }).unwrap(),
        };
        expected.now = now; expected.request.received_at = now;
        prop_assert_eq!(actual, expected);
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
