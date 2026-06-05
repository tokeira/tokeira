use anyhow::Result;
use std::collections::HashMap;
use tokeira_kernel::StartRequest;
use tokeira_runtime::StartedWorkflowTask;
use tokeira_storage::RunRepository;

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
) -> Result<PollWorkflowTaskQueueResponse> {
    let after_event_id = workflow_task_history_after_event_id(&started);
    let history = repo
        .read_history(started.run_key, after_event_id, usize::MAX)
        .await?;
    Ok(PollWorkflowTaskQueueResponse {
        task_token: serde_json::to_vec(&started.token)?,
        started_event_id: started.token.started_event_id,
        previous_started_event_id: started.previous_started_event_id,
        attempt: started.token.attempt,
        scheduled_time: Some(started.scheduled_time),
        started_time: Some(started.started_time),
        payload: WorkflowTaskPayloadDto {
            workflow_id: started.workflow_id.0,
            run_key: started.run_key,
            task_queue: started.task_queue.0,
            history,
        },
        queries: HashMap::new(),
        messages: Vec::new(),
    })
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
    }
}

pub fn poll_activity_response(
    started: tokeira_runtime::StartedActivityTask,
) -> Result<crate::translate::PollActivityTaskQueueResponse> {
    Ok(crate::translate::PollActivityTaskQueueResponse {
        task_token: serde_json::to_vec(&started.token)?,
        activity_id: started.activity_id,
        activity_type: started.activity_type,
        input: started.input,
        attempt: started.attempt,
        workflow_id: started.workflow_id,
        workflow_type: started.workflow_type,
        workflow_namespace: started.workflow_namespace,
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
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_runtime::StartedWorkflowTask;
    use tokeira_types::{
        LogicalTaskSeq, RunKey, ShardEpoch, TaskQueueName, WorkflowId, WorkflowTaskToken,
    };

    use super::workflow_task_history_after_event_id;

    fn started_task(previous_started_event_id: i64, is_sticky_match: bool) -> StartedWorkflowTask {
        let run_key = RunKey::new();
        StartedWorkflowTask {
            token: WorkflowTaskToken {
                run_key,
                logical_seq: LogicalTaskSeq::ONE,
                started_event_id: 2,
                attempt: 1,
                shard_epoch: ShardEpoch(1),
            },
            run_key,
            workflow_id: WorkflowId("workflow".to_string()),
            task_queue: TaskQueueName("queue".to_string()),
            previous_started_event_id,
            is_sticky_match,
            scheduled_time: OffsetDateTime::UNIX_EPOCH,
            started_time: OffsetDateTime::UNIX_EPOCH,
        }
    }

    proptest! {
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
    }
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
        tokeira_runtime::QueryResult::Failed { message: _ } => {
            crate::translate::QueryWorkflowResponse {
                result: None,
                rejected_status: None,
            }
        }
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
