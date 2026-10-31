//! Concrete cron handoffs remain deterministic and preserve the evaluated retry outcome.

use super::*;
use tokeira_kernel::CronContinuation;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: successor-semantics, Property 8: identical concrete input yields identical close history.
    #[test]
    fn cron_failure_is_deterministic(retry_case in 0u8..5, delay_ms in 0i64..10000, bytes in prop::collection::vec(any::<u8>(), 0..48)) {
        let state = with_pending_wft(make_open_state(fixed_now()), 1, Some(2), 1);
        let retry_state = match retry_case {
            0 => RetryState::RetryPolicyNotSet,
            1 => RetryState::MaximumAttemptsReached,
            2 => RetryState::NonRetryableFailure,
            3 => RetryState::Timeout,
            _ => RetryState::InProgress,
        };
        let successor_id = RunId::new();
        let command = Command::WorkflowTaskCompletedWithCron {
            request: completion_request(&state, vec![WorkflowCommand::FailWorkflow { failure: Payload::new(bytes.clone()) }], Default::default(), None, None, false, fixed_now()),
            cron_continuation: CronContinuation { failure_retry_state: retry_state.clone(), new_run_id: successor_id, first_workflow_task_backoff: Duration::milliseconds(delay_ms), input: Payloads(vec![Payload::new(bytes)]), cron_schedule: "@every 1s".into() },
        };
        let encoded = postcard::to_allocvec(&command).unwrap();
        let decoded: Command = postcard::from_bytes(&encoded).unwrap();
        prop_assert_eq!(&decoded, &command);
        let first = BasicKernel.apply(LoadedRun::Existing(state.clone()), command.clone()).unwrap();
        let second = BasicKernel.apply(LoadedRun::Existing(state), decoded).unwrap();
        prop_assert_eq!(&first, &second);
        let (recorded_retry, recorded_id) = first.history_events.iter().find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionFailed { retry_state, new_execution_run_id, .. } => Some((retry_state, *new_execution_run_id)),
            _ => None,
        }).unwrap();
        prop_assert_eq!(recorded_retry, &retry_state);
        prop_assert_eq!(recorded_id, Some(successor_id));
    }
}
