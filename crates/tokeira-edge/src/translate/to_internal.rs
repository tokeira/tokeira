use anyhow::Result;
use time::OffsetDateTime;
use tokeira_kernel::{
    ResetRequest, SignalRequest, SignalWithStartRequest, StartRequest,
    WorkflowTaskCompletedRequest,
};
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
        ResetWorkflowExecutionRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest,
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
        workflow_execution_timeout: req.workflow_execution_timeout,
        workflow_run_timeout: req.workflow_run_timeout,
        workflow_task_timeout: req
            .workflow_task_timeout
            .unwrap_or(time::Duration::seconds(10)),
        retry_policy: req.retry_policy,
        conflict_policy: req.conflict_policy,
        reuse_policy: req.reuse_policy,
        deployment: None,
        build_id: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        first_run_started_at: None,
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

pub fn signal_with_start_request(
    req: crate::translate::SignalWithStartWorkflowExecutionRequest,
    request_id: &RequestId,
) -> SignalWithStartRequest {
    let now = OffsetDateTime::now_utc();
    let run_id = RunId::new();
    SignalWithStartRequest {
        run_key: RunKey::new(),
        namespace_id: namespace_id_for(&req.namespace),
        workflow_id: WorkflowId(req.workflow_id),
        run_id,
        workflow_type: WorkflowType(req.workflow_type),
        task_queue: TaskQueueName(req.task_queue),
        deployment: None,
        build_id: None,
        input: req.input,
        memo: req.memo,
        search_attributes: req.search_attributes,
        workflow_execution_timeout: req.workflow_execution_timeout,
        workflow_run_timeout: req.workflow_run_timeout,
        workflow_task_timeout: req
            .workflow_task_timeout
            .unwrap_or(time::Duration::seconds(10)),
        retry_policy: req.retry_policy,
        conflict_policy: req.conflict_policy,
        reuse_policy: req.reuse_policy,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: req.identity,
            received_at: now,
        },
        now,
        signal_name: req.signal_name,
        signal_input: req.signal_input,
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
            deployment: req.deployment,
            build_id: req.build_id,
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

pub fn poll_activity_request(
    req: crate::translate::PollActivityTaskQueueRequest,
) -> PollInternalRequest {
    PollInternalRequest {
        queue: QueueKey {
            namespace_id: namespace_id_for(&req.namespace),
            task_queue: TaskQueueName(req.task_queue),
            task_kind: TaskKind::Activity,
            deployment: None,
            build_id: None,
        },
        worker_identity: WorkerIdentity(req.worker_identity),
        timeout: req.timeout,
    }
}

pub fn terminate_request(
    req: crate::translate::TerminateWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::TerminateRequest {
    let now = OffsetDateTime::now_utc();
    tokeira_kernel::TerminateRequest {
        reason: req.reason,
        details: req.details,
        identity: req.identity,
        request: RequestContext {
            request_id: CoreRequestId(
                request_id.as_str().to_string(),
            ),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

pub fn cancel_request(
    req: crate::translate::RequestCancelWorkflowExecutionRequest,
    request_id: &RequestId,
) -> tokeira_kernel::CancelRequest {
    let now = OffsetDateTime::now_utc();
    tokeira_kernel::CancelRequest {
        reason: req.reason,
        external_initiator: None,
        request: RequestContext {
            request_id: CoreRequestId(
                request_id.as_str().to_string(),
            ),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}

pub fn reset_request(
    req: ResetWorkflowExecutionRequest,
    request_id: &RequestId,
) -> ResetRequest {
    let now = OffsetDateTime::now_utc();
    ResetRequest {
        fork_event_id: req.workflow_task_finish_event_id,
        new_run_id: RunId::new(),
        reason: req.reason,
        request: RequestContext {
            request_id: CoreRequestId(
                req.request_id
                    .unwrap_or_else(|| request_id.as_str().to_string()),
            ),
            caller_identity: None,
            received_at: now,
        },
        now,
    }
}
