use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{SignalRequest, StartRequest, WorkflowTaskCompletedRequest};
use tokeira_types::{
    NamespaceId, QueueKey, RequestContext, RequestId as CoreRequestId, RunId, RunKey, SearchAttributes,
    ShardEpoch, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken, WorkflowType,
};

use crate::{
    request_id::RequestId,
    translate::{
        PollWorkflowTaskQueueRequest, RespondWorkflowTaskCompletedRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest,
    },
};

#[derive(Clone, Debug)]
pub struct PollInternalRequest {
    pub queue: QueueKey,
    pub worker_identity: WorkerIdentity,
    pub timeout: std::time::Duration,
}

pub fn start_request(req: StartWorkflowExecutionRequest, request_id: &RequestId) -> StartRequest {
    let now = req.now.unwrap_or_else(OffsetDateTime::now_utc);
    StartRequest {
        run_key: req.run_key.unwrap_or_else(RunKey::new),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId(req.workflow_id),
        run_id: req.run_id.unwrap_or_else(RunId::new),
        workflow_type: WorkflowType(req.workflow_type),
        task_queue: TaskQueueName(req.task_queue),
        input: req.input,
        memo: req.memo,
        search_attributes: req.search_attributes,
        request: RequestContext {
            request_id: CoreRequestId(req.request_id.unwrap_or_else(|| request_id.as_str().to_string())),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
    }
}

pub fn signal_request(
    req: SignalWorkflowExecutionRequest,
    request_id: &RequestId,
) -> SignalRequest {
    let now = req.now.unwrap_or_else(OffsetDateTime::now_utc);
    SignalRequest {
        signal_name: req.signal_name,
        input: req.input,
        request: RequestContext {
            request_id: CoreRequestId(req.request_id.unwrap_or_else(|| request_id.as_str().to_string())),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
    }
}

pub fn poll_request(req: PollWorkflowTaskQueueRequest) -> PollInternalRequest {
    PollInternalRequest {
        queue: QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName(req.task_queue),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        },
        worker_identity: WorkerIdentity(req.worker_identity),
        timeout: req.timeout,
    }
}

pub fn workflow_task_completed_request(
    req: RespondWorkflowTaskCompletedRequest,
) -> Result<WorkflowTaskCompletedRequest> {
    let token: WorkflowTaskToken = serde_json::from_slice(&req.task_token)?;
    Ok(WorkflowTaskCompletedRequest {
        token: WorkflowTaskToken {
            shard_epoch: ShardEpoch::ZERO,
            ..token
        },
        identity: WorkerIdentity(req.identity),
        commands: req.commands,
        force_new_workflow_task: req.force_new_workflow_task,
        now: OffsetDateTime::now_utc(),
    })
}
