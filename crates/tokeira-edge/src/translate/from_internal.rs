use anyhow::Result;
use std::collections::HashMap;
use tokeira_kernel::StartRequest;
use tokeira_runtime::StartedWorkflowTask;
use tokeira_storage::RunRepository;
use tokeira_types::NamespaceId;

use crate::{
    translate::{
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionResponse, WorkflowTaskPayloadDto,
    },
    workflow_service::WorkflowMutationOutcome,
};

pub fn start_response(
    req: &StartRequest,
    outcome: WorkflowMutationOutcome,
) -> StartWorkflowExecutionResponse {
    StartWorkflowExecutionResponse {
        run_key: req.run_key,
        run_id: req.run_id,
        transition_seq: outcome.transition_seq,
        last_event_id: outcome.last_event_id,
        started: true,
        status: outcome.execution_status,
        attached_request_id: None,
        eager_workflow_task: None,
    }
}

pub fn signal_response(outcome: WorkflowMutationOutcome) -> SignalWorkflowExecutionResponse {
    SignalWorkflowExecutionResponse {
        accepted: !outcome.was_duplicate,
        transition_seq: outcome.transition_seq,
        last_event_id: outcome.last_event_id,
    }
}

pub async fn poll_response(
    started: StartedWorkflowTask,
    repo: &dyn RunRepository,
    namespace_id: NamespaceId,
) -> Result<PollWorkflowTaskQueueResponse> {
    let after_event_id = workflow_task_history_after_event_id(&started);
    // The history as it stood when the task started. The runtime commits the
    // start before this read, so without the bound an event committed in
    // between would reach the worker ahead of the synthesized pair below;
    // v1.31.0 reads `[firstEventID, nextEventID)` with `nextEventID` fixed when
    // the task starts (recordworkflowtaskstarted/api.go:272-276, 419 @ v1.31.0).
    let limit = usize::try_from(
        started
            .last_event_id_at_start
            .saturating_sub(after_event_id),
    )
    .unwrap_or(0);
    let attributed_history = repo
        .read_attributed_history(started.run_key, after_event_id, limit)
        .await?;
    let (history, history_principals) = poll_history(&started, after_event_id, attributed_history);
    Ok(PollWorkflowTaskQueueResponse {
        task_token: crate::task_token::encode(started.token.clone(), namespace_id)?,
        started_event_id: started.token.started_event_id,
        previous_started_event_id: started.previous_started_event_id,
        attempt: started.token.attempt,
        scheduled_time: Some(started.scheduled_time),
        started_time: Some(started.started_time),
        payload: WorkflowTaskPayloadDto {
            run_id: started.run_id,
            workflow_id: started.workflow_id.0,
            run_key: started.run_key,
            task_queue: started.task_queue.0,
            history,
            history_principals,
        },
        query: None,
        queries: HashMap::new(),
        messages: Vec::new(),
        poller_scaling_decision: None,
    })
}

/// Assemble a polled task's history from the events read after
/// `after_event_id`: only those up to the run's last event at start, then —
/// for a task whose Scheduled/Started were never persisted — the synthesized
/// pair, when it continues exactly from them.
fn poll_history(
    started: &StartedWorkflowTask,
    after_event_id: i64,
    read: Vec<tokeira_storage::AttributedHistoryEvent>,
) -> (
    Vec<tokeira_kernel::HistoryEvent>,
    Vec<Option<tokeira_types::EventPrincipal>>,
) {
    let history_end = started.last_event_id_at_start;
    let (mut history, mut history_principals): (Vec<_>, Vec<_>) = read
        .into_iter()
        .filter(|attributed| attributed.event.event_id <= history_end)
        .map(|attributed| (attributed.event, attributed.principal))
        .unzip();
    // Transient-suffix synthesis (spec transient-wft Req B.7): a transient
    // (attempt>1) task's Scheduled/Started events are never persisted — the
    // poll response synthesizes them at the virtual ids so the worker's
    // history matches the task token it must respond with
    // (`GetTransientWorkflowTaskInfo`, mutable_state_impl.go:1189-1250;
    // `response.TransientWorkflowTask`, recordworkflowtaskstarted/api.go:430
    // @ v1.31.0). Nothing is persisted; ids continue past the last real event.
    // A started id beyond the history at start marks the task virtual —
    // transient (attempt>1) or speculative; a normal attempt-1 task's started
    // event is persisted and never trips this.
    let scheduled_event_id = started.token.started_event_id - 1;
    let last_read = history
        .last()
        .map(|event| event.event_id)
        .unwrap_or(after_event_id);
    let synthesize = started.token.started_event_id > history_end;
    // The pair goes on only when it continues exactly from the history read;
    // otherwise the worker would get two events with one id or a gap. v1.31.0
    // likewise skips a suffix that fails `ValidateTransientWorkflowTaskEvents`
    // without failing the request (get_history_util.go:118-128, 234-246,
    // 438-457 @ v1.31.0). The kernel's transition check keeps the reserved ids
    // free, so this firing means stored history and run state disagree.
    let continues = scheduled_event_id == history_end + 1 && last_read == history_end;
    if synthesize && !continues {
        tracing::error!(
            run_key = %started.run_key.0,
            scheduled_event_id,
            history_end,
            last_read,
            "synthesized workflow task events do not continue from the stored history; omitted"
        );
    }
    if synthesize && continues {
        history.push(tokeira_kernel::HistoryEvent {
            event_id: scheduled_event_id,
            happened_at: started.scheduled_time,
            kind: tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: started.token.logical_seq,
                task_queue: started.task_queue.clone(),
                workflow_task_timeout: started.workflow_task_timeout,
                attempt: started.token.attempt,
            },
        });
        history_principals.push(None);
        history.push(tokeira_kernel::HistoryEvent {
            event_id: started.token.started_event_id,
            happened_at: started.started_time,
            kind: tokeira_kernel::HistoryEventKind::WorkflowTaskStarted {
                logical_seq: started.token.logical_seq,
                scheduled_event_id,
                attempt: started.token.attempt,
                identity: started.worker_identity.clone(),
                request_id: format!(
                    "transient-{}-{}",
                    started.token.logical_seq.0, started.token.attempt
                ),
                // The Advice the start transition recorded for this attempt,
                // so the worker acts on what the server decided even though
                // the event is synthesized (continue-as-new-advice,
                // Requirement 4.2).
                history_size_bytes: started.advice.history_size_bytes,
                suggest_continue_as_new: started.advice.suggest_continue_as_new,
                suggest_continue_as_new_reasons: started
                    .advice
                    .suggest_continue_as_new_reasons
                    .clone(),
                target_worker_deployment_version_changed: started
                    .target_worker_deployment_version_changed,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            },
        });
        history_principals.push(None);
    }
    (history, history_principals)
}

fn workflow_task_history_after_event_id(started: &StartedWorkflowTask) -> i64 {
    if started.previous_started_event_id > 0 && started.is_sticky_match {
        started.previous_started_event_id
    } else {
        0
    }
}

pub fn completed_response(
    outcome: WorkflowMutationOutcome,
) -> RespondWorkflowTaskCompletedResponse {
    RespondWorkflowTaskCompletedResponse {
        transition_seq: outcome.transition_seq,
        last_event_id: outcome.last_event_id,
        execution_status: outcome.execution_status,
        new_run_id: outcome.new_run_id,
        was_duplicate: outcome.was_duplicate,
        workflow_task: None,
        activity_tasks: Vec::new(),
        reset_history_event_id: 0,
    }
}

/// Project a started activity into the public poll response.
///
/// `workflow_namespace` is the admitted current namespace name, while
/// `namespace_id` is sealed only into the task-token envelope. Keeping those
/// inputs distinct prevents stable identity from leaking into the SDK-visible
/// namespace field (`recordactivitytaskstarted/api.go:270 @ v1.31.0`).
pub fn poll_activity_response(
    started: tokeira_runtime::StartedActivityTask,
    namespace_id: NamespaceId,
    workflow_namespace: &str,
) -> Result<crate::translate::PollActivityTaskQueueResponse> {
    Ok(crate::translate::PollActivityTaskQueueResponse {
        task_token: crate::task_token::encode(started.token.clone(), namespace_id)?,
        activity_id: started.activity_id,
        run_id: started.run_id,
        activity_type: started.activity_type,
        input: started.input,
        attempt: started.attempt,
        workflow_id: started.workflow_id,
        workflow_type: started.workflow_type,
        workflow_namespace: workflow_namespace.to_owned(),
        run_key: started.run_key,
        header: started.header,
        retry_policy: started.retry_policy,
        heartbeat_details: started.heartbeat_details,
        scheduled_time: Some(started.scheduled_time),
        current_attempt_scheduled_time: started.current_attempt_scheduled_time,
        started_time: Some(started.started_time),
        schedule_to_close_timeout: started
            .schedule_to_close_timeout
            .and_then(|d| d.try_into().ok()),
        start_to_close_timeout: started
            .start_to_close_timeout
            .and_then(|d| d.try_into().ok()),
        heartbeat_timeout: started.heartbeat_timeout.and_then(|d| d.try_into().ok()),
        poller_scaling_decision: None,
    })
}

pub fn terminate_response(
    _outcome: WorkflowMutationOutcome,
) -> crate::translate::TerminateWorkflowExecutionResponse {
    crate::translate::TerminateWorkflowExecutionResponse
}

pub fn cancel_response(
    _outcome: WorkflowMutationOutcome,
) -> crate::translate::RequestCancelWorkflowExecutionResponse {
    crate::translate::RequestCancelWorkflowExecutionResponse
}

pub fn reset_response(
    outcome: tokeira_runtime::ResetWorkflowResult,
) -> crate::translate::ResetWorkflowExecutionResponse {
    crate::translate::ResetWorkflowExecutionResponse {
        run_id: outcome.successor_run_id,
    }
}

pub fn query_response(
    result: tokeira_runtime::QueryResult,
) -> crate::translate::QueryWorkflowResponse {
    match result {
        tokeira_runtime::QueryResult::Completed { result } => {
            crate::translate::QueryWorkflowResponse {
                result: Some(result),
                rejected_status: None,
            }
        }
        // Unreachable from the edge handler (a Failed result returns the
        // typed QueryFailed error, and an oversized one InvalidArgument,
        // before response translation); kept exhaustive-and-inert for any
        // other caller.
        tokeira_runtime::QueryResult::Failed { .. }
        | tokeira_runtime::QueryResult::ResultTooLarge => crate::translate::QueryWorkflowResponse {
            result: None,
            rejected_status: None,
        },
        tokeira_runtime::QueryResult::Rejected { status } => {
            crate::translate::QueryWorkflowResponse {
                result: None,
                rejected_status: Some(status),
            }
        }
    }
}

pub fn update_response(
    snapshot: tokeira_runtime::UpdateLifecycleSnapshot,
) -> crate::translate::UpdateWorkflowExecutionResponse {
    let outcome = snapshot.outcome.map(|outcome| match outcome {
        tokeira_runtime::UpdateOutcome::Completed {
            accepted_event_id,
            result,
        } => crate::translate::UpdateOutcomeDto::Completed {
            accepted_event_id,
            result,
        },
        tokeira_runtime::UpdateOutcome::Rejected {
            accepted_event_id,
            failure,
        } => crate::translate::UpdateOutcomeDto::Rejected {
            accepted_event_id,
            failure,
        },
        tokeira_runtime::UpdateOutcome::AcceptedRunClosed => {
            crate::translate::UpdateOutcomeDto::AcceptedRunClosed
        }
        tokeira_runtime::UpdateOutcome::RejectedUnprocessed => {
            crate::translate::UpdateOutcomeDto::RejectedUnprocessed
        }
    });
    crate::translate::UpdateWorkflowExecutionResponse {
        update_ref: crate::translate::UpdateRefDto {
            workflow_id: snapshot.workflow_execution.workflow_id.0,
            run_id: snapshot
                .workflow_execution
                .run_id
                .map(|run_id| run_id.0.to_string())
                .unwrap_or_default(),
            update_id: snapshot.update_id,
        },
        stage: match snapshot.stage {
            tokeira_runtime::UpdateLifecycleStage::Unspecified => {
                crate::translate::UpdateLifecycleStageDto::Unspecified
            }
            tokeira_runtime::UpdateLifecycleStage::Admitted => {
                crate::translate::UpdateLifecycleStageDto::Admitted
            }
            tokeira_runtime::UpdateLifecycleStage::Accepted => {
                crate::translate::UpdateLifecycleStageDto::Accepted
            }
            tokeira_runtime::UpdateLifecycleStage::Completed => {
                crate::translate::UpdateLifecycleStageDto::Completed
            }
        },
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_runtime::{StartedActivityTask, StartedWorkflowTask};
    use tokeira_types::{
        ActivityTaskToken, BuildId, DeploymentId, LogicalTaskSeq, NamespaceId, Payloads, RunId,
        RunKey, ShardEpoch, TaskQueueName, WorkerTaskClass, WorkerTaskOrigin, WorkflowId,
        WorkflowTaskToken,
    };

    use super::{poll_activity_response, poll_history, workflow_task_history_after_event_id};

    fn task_origin(task_class: WorkerTaskClass) -> WorkerTaskOrigin {
        WorkerTaskOrigin {
            namespace_id: NamespaceId::new(),
            normal_task_queue: TaskQueueName("queue".to_string()),
            task_class,
            deployment: DeploymentId(String::new()),
            build_id: BuildId(String::new()),
        }
    }

    fn started_task(previous_started_event_id: i64, is_sticky_match: bool) -> StartedWorkflowTask {
        let run_key = RunKey::new();
        StartedWorkflowTask {
            advice: Default::default(),
            token: WorkflowTaskToken {
                run_key,
                logical_seq: LogicalTaskSeq::ONE,
                started_event_id: 2,
                attempt: 1,
                shard_epoch: ShardEpoch(1),
            },
            run_key,
            run_id: tokeira_types::RunId(uuid::Uuid::nil()),
            workflow_task_timeout: time::Duration::seconds(10),
            worker_identity: tokeira_types::WorkerIdentity("worker".to_string()),
            workflow_id: WorkflowId("workflow".to_string()),
            task_queue: TaskQueueName("queue".to_string()),
            previous_started_event_id,
            last_event_id_at_start: 2,
            is_sticky_match,
            scheduled_time: OffsetDateTime::UNIX_EPOCH,
            started_time: OffsetDateTime::UNIX_EPOCH,
            target_worker_deployment_version_changed: false,
            origin: task_origin(WorkerTaskClass::Workflow),
        }
    }

    fn started_activity_task() -> StartedActivityTask {
        let run_key = RunKey::new();
        StartedActivityTask {
            run_key,
            run_id: RunId(uuid::Uuid::nil()),
            activity_id: "activity".to_owned(),
            activity_type: "activity-type".to_owned(),
            task_queue: TaskQueueName("queue".to_owned()),
            token: ActivityTaskToken {
                run_key,
                activity_id: "activity".to_owned(),
                schedule_event_id: 5,
                attempt: 1,
                shard_epoch: ShardEpoch::ZERO,
            },
            input: Payloads::default(),
            attempt: 1,
            workflow_id: "workflow".to_owned(),
            workflow_type: "workflow-type".to_owned(),
            header: None,
            retry_policy: None,
            heartbeat_details: None,
            scheduled_time: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_time: Some(OffsetDateTime::UNIX_EPOCH),
            started_time: OffsetDateTime::UNIX_EPOCH,
            schedule_to_close_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            origin: task_origin(WorkerTaskClass::Activity),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn partial_history_offset_requires_sticky_match(previous_started_event_id in -10i64..10_000, is_sticky_match in any::<bool>()) {
            let started = started_task(previous_started_event_id, is_sticky_match);
            let expected = if previous_started_event_id > 0 && is_sticky_match {
                previous_started_event_id
            } else {
                0
            };

            prop_assert_eq!(workflow_task_history_after_event_id(&started), expected);
        }

        // Feature: authorization-foundation, correction property: activity namespace projection
        #[test]
        fn activity_response_uses_namespace_name_not_stable_id(
            namespace in "[a-z][a-z0-9-]{0,31}"
        ) {
            let namespace_id = crate::translate::to_internal::namespace_id_for(&namespace);
            let response = poll_activity_response(
                started_activity_task(),
                namespace_id,
                &namespace,
            )
            .expect("activity response translates");

            prop_assert_eq!(response.workflow_namespace, namespace);
            let (_, token_namespace_id) = crate::task_token::decode::<ActivityTaskToken>(
                &response.task_token,
            )
            .expect("activity token decodes");
            prop_assert_eq!(token_namespace_id, Some(namespace_id));
        }

        // Feature: speculative-wft, Property 7 (poll side): the history a
        // polled task carries is the history as it stood at start, and its
        // synthesized pair continues it without a gap or a repeated id.
        #[test]
        fn poll_history_is_the_history_at_start(
            history_end in 2i64..40,
            committed_after in 0i64..4,
            missing in 0i64..2,
        ) {
            let mut started = started_task(0, false);
            started.last_event_id_at_start = history_end;
            started.token.started_event_id = history_end + 2;
            // Stored history: everything up to the start, minus `missing`
            // trailing events, plus events committed after the start.
            let stored_last = history_end - missing + committed_after;
            let read = (1..=stored_last)
                .filter(|id| *id <= history_end - missing || *id > history_end)
                .map(signaled)
                .collect();

            let (history, principals) = poll_history(&started, 0, read);

            prop_assert_eq!(history.len(), principals.len());
            let ids: Vec<i64> = history.iter().map(|event| event.event_id).collect();
            if missing == 0 {
                let expected: Vec<i64> = (1..=history_end + 2).collect();
                prop_assert_eq!(ids, expected);
                let pair_starts_with_scheduled = matches!(
                    history[history.len() - 2].kind,
                    tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled { .. }
                );
                prop_assert!(pair_starts_with_scheduled);
            } else {
                // Short of the start-time history: the pair would leave a
                // gap, so it is left off.
                let expected: Vec<i64> = (1..=history_end - missing).collect();
                prop_assert_eq!(ids, expected);
            }
        }
    }

    fn signaled(event_id: i64) -> tokeira_storage::AttributedHistoryEvent {
        tokeira_storage::AttributedHistoryEvent {
            event: tokeira_kernel::HistoryEvent {
                event_id,
                happened_at: OffsetDateTime::UNIX_EPOCH,
                kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled {
                    signal_name: "sig".to_string(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request_id: format!("signal-{event_id}"),
                    identity: None,
                },
            },
            principal: None,
        }
    }

    #[test]
    fn poll_history_leaves_a_normal_task_unchanged() {
        let started = started_task(0, false);
        let read = (1..=2).map(signaled).collect();
        let (history, _) = poll_history(&started, 0, read);
        let ids: Vec<i64> = history.iter().map(|event| event.event_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
