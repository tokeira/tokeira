use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{SignalRequest, StartRequest, WorkflowTaskCompletedRequest};
use tokeira_types::{
    NamespaceId, QueueKey, RequestContext, RequestId as CoreRequestId, RunId, RunKey,
    ShardEpoch, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowTaskToken,
    WorkflowType,
};
use uuid::Uuid;

use crate::{
    request_id::RequestId,
    translate::{
        PollWorkflowTaskQueueRequest, RespondWorkflowTaskCompletedRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest,
    },
};

#[derive(Clone, Debug)]
pub struct PollInternalRequest {
    pub queue: QueueKey,
    pub worker_identity: WorkerIdentity,
    pub timeout: std::time::Duration,
}

pub fn namespace_id_for(name: &str) -> NamespaceId {
    let mut bytes = *b"tokeira-edge-ns!";
    for (idx, byte) in name.as_bytes().iter().enumerate() {
        let slot = idx % 16;
        bytes[slot] = bytes[slot]
            .wrapping_add(*byte)
            .rotate_left((idx % 8) as u32);
    }
    NamespaceId(Uuid::from_bytes(bytes))
}

pub fn start_request(
    req: StartWorkflowExecutionRequest,
    request_id: &RequestId,
) -> StartRequest {
    let now = req.now.unwrap_or_else(OffsetDateTime::now_utc);
    let run_id = req.run_id.unwrap_or_else(RunId::new);
    StartRequest {
        run_key: req.run_key.unwrap_or_else(RunKey::new),
        namespace_id: namespace_id_for(&req.namespace),
        workflow_id: WorkflowId(req.workflow_id),
        run_id,
        workflow_type: WorkflowType(req.workflow_type),
        task_queue: TaskQueueName(req.task_queue),
        input: req.input,
        memo: req.memo,
        search_attributes: req.search_attributes,
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: time::Duration::seconds(10),
        retry_policy: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
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
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
    }
}

pub fn poll_request(req: PollWorkflowTaskQueueRequest) -> PollInternalRequest {
    PollInternalRequest {
        queue: QueueKey {
            namespace_id: namespace_id_for(&req.namespace),
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
