//! Kernel-authored close fixtures shared by memory and live pair contracts.

use crate::memory::{
    projection_accumulator_tests::{fresh_transition, reset_history},
    used_deployment_versions_tests::{completion, with_started_task},
};
use time::Duration;
use tokeira_kernel::{
    BasicKernel, Command, CronContinuation, HistoryEventKind, Kernel, LoadedRun, RequestDedupeOp,
    RetryContinuation, RetryState, Transition, WorkflowCommand, WorkflowExecutionTimedOutRequest,
    WorkflowState, WorkflowTimeoutType,
};
use tokeira_types::{Headers, Memo, Payload, Payloads, RequestId, RunKey};

pub(crate) fn start() -> Transition {
    let mut start = fresh_transition(RunKey::new());
    start.next_state.last_event_id = 1;
    start.history_events = vec![reset_history(&start.next_state).remove(0)].into();
    start.event_principals = vec![None].into();
    start
}

pub(crate) fn pair(
    state: &WorkflowState,
    kind: u8,
    input: Payloads,
    memo: Memo,
    header: Option<Headers>,
) -> (Transition, Transition) {
    let mut successor = start();
    successor.next_state.namespace_id = state.namespace_id;
    successor.next_state.workflow_id = state.workflow_id.clone();
    successor.next_state.memo = memo.clone();
    successor.next_state.search_attributes = state.search_attributes.clone();
    successor.next_state.links = state.links.clone();
    successor.next_state.first_execution_run_id = Some(state.run_id);
    let successor_id = successor.next_state.run_id;
    successor.history_events = vec![reset_history(&successor.next_state).remove(0)].into();
    if let HistoryEventKind::WorkflowExecutionStarted {
        input: stored_input,
        header: stored_header,
        links,
        continued_execution_run_id,
        ..
    } = &mut successor.history_events[0].kind
    {
        *stored_input = input.clone();
        *stored_header = header.clone();
        *links = state.links.clone();
        *continued_execution_run_id = Some(state.run_id);
    }
    successor.request_dedupe_ops.push(RequestDedupeOp {
        request_id: RequestId(format!("successor:{}", successor_id.0)),
    });
    let (started, events) = with_started_task(state.clone());
    let workflow_command = match kind {
        0 => WorkflowCommand::ContinueAsNew {
            new_run_id: successor_id,
            workflow_type: state.workflow_type.clone(),
            task_queue: state.task_queue.clone(),
            input: input.clone(),
            memo,
            search_attributes: state.search_attributes.clone(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: state.workflow_task_timeout,
            retry_policy: None,
            header,
            initial_versioning_behavior: Default::default(),
            successor_versioning_info: None,
        },
        1 => WorkflowCommand::FailWorkflow {
            failure: Payload::new(b"retryable"),
        },
        _ => WorkflowCommand::CompleteWorkflow {
            result: Payloads::default(),
        },
    };
    let request = completion(&started, None, Default::default(), vec![workflow_command]);
    let command = match kind {
        0 => Command::WorkflowTaskCompleted(request),
        1 => Command::WorkflowTaskCompletedWithRetry {
            request,
            retry_continuation: RetryContinuation::Retry {
                new_run_id: successor_id,
            },
        },
        2 => Command::WorkflowTaskCompletedWithCron {
            request,
            cron_continuation: CronContinuation {
                failure_retry_state: RetryState::RetryPolicyNotSet,
                new_run_id: successor_id,
                first_workflow_task_backoff: Duration::seconds(30),
                input,
                cron_schedule: "* * * * *".into(),
            },
        },
        _ => Command::WorkflowExecutionTimedOut(WorkflowExecutionTimedOutRequest {
            timeout_type: WorkflowTimeoutType::RunTimeout,
            retry_state: RetryState::InProgress,
            new_execution_run_id: Some(successor_id),
            now: state.started_at,
        }),
    };
    let mut closing = BasicKernel
        .apply(
            LoadedRun::Existing(if kind == 3 { state.clone() } else { started }),
            command,
        )
        .unwrap();
    if kind != 3 {
        for (index, event) in events.into_iter().enumerate() {
            closing.history_events.insert(index, event);
            closing.event_principals.insert(index, None);
        }
    }
    (closing, successor)
}
