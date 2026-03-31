use anyhow::Result;
use tokeira_kernel::StartRequest;
use tokeira_runtime::StartedWorkflowTask;

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
    }
}

pub fn signal_response(outcome: WorkflowMutationOutcome) -> SignalWorkflowExecutionResponse {
    SignalWorkflowExecutionResponse {
        accepted: !outcome.was_duplicate,
        transition_seq: outcome.transition_seq,
        last_event_id: outcome.last_event_id,
    }
}

pub fn poll_response(started: StartedWorkflowTask) -> Result<PollWorkflowTaskQueueResponse> {
    Ok(PollWorkflowTaskQueueResponse {
        task_token: serde_json::to_vec(&started.token)?,
        started_event_id: started.token.started_event_id,
        attempt: started.token.attempt,
        payload: WorkflowTaskPayloadDto {
            workflow_id: started.workflow_id.0,
            run_key: started.run_key,
            task_queue: started.task_queue.0,
            history: Vec::new(),
        },
    })
}

pub fn completed_response(outcome: WorkflowMutationOutcome) -> RespondWorkflowTaskCompletedResponse {
    RespondWorkflowTaskCompletedResponse {
        transition_seq: outcome.transition_seq,
        last_event_id: outcome.last_event_id,
    }
}
