//! Workflow-service-specific conversion helpers.
//!
//! The goal here is not to define the runtime's internal request/response model.
//! The goal is to keep the most repetitive wire-level mappings out of `tokeira-edge`:
//!
//! - execution status enum translation
//! - page token wrapping
//! - list/count response row construction
//! - request-context extraction from public API requests

use crate::conversions::common::{
    memo_from_domain, payloads_from_domain, search_attributes_from_domain, task_queue_from_domain,
};
use crate::public::{enums, workflowservice};
use time::OffsetDateTime;
use tokeira_types::{
    CallerIdentity, CountGroup, ExecutionStatus, ExecutionSummary, IdempotencyKey, NamespaceName,
    PageToken, RequestContext, RequestId,
};

pub fn request_context_from_start_request(
    req: &workflowservice::StartWorkflowExecutionRequest,
) -> RequestContext {
    RequestContext {
        request_id: if req.request_id.is_empty() {
            None
        } else {
            Some(RequestId::from(req.request_id.clone()))
        },
        idempotency_key: if req.request_id.is_empty() {
            None
        } else {
            // For start requests, Temporal-style callers often use `request_id` as the idempotency
            // key. We keep the field distinct in the domain layer so the engine can later represent
            // "same semantic operation, retried transport attempt" if needed.
            Some(IdempotencyKey::from(req.request_id.clone()))
        },
        caller_identity: None,
    }
}

pub fn request_context_from_signal_request(
    req: &workflowservice::SignalWorkflowExecutionRequest,
) -> RequestContext {
    RequestContext {
        request_id: if req.request_id.is_empty() {
            None
        } else {
            Some(RequestId::from(req.request_id.clone()))
        },
        idempotency_key: if req.request_id.is_empty() {
            None
        } else {
            Some(IdempotencyKey::from(req.request_id.clone()))
        },
        caller_identity: if req.identity.is_empty() {
            None
        } else {
            Some(CallerIdentity::from(req.identity.clone()))
        },
    }
}

pub fn execution_status_to_proto(value: ExecutionStatus) -> i32 {
    use enums::WorkflowExecutionStatus as Proto;
    match value {
        ExecutionStatus::Running => Proto::Running as i32,
        ExecutionStatus::Completed => Proto::Completed as i32,
        ExecutionStatus::Failed => Proto::Failed as i32,
        ExecutionStatus::Canceled => Proto::Canceled as i32,
        ExecutionStatus::Terminated => Proto::Terminated as i32,
        ExecutionStatus::TimedOut => Proto::TimedOut as i32,
        ExecutionStatus::ContinuedAsNew => Proto::ContinuedAsNew as i32,
    }
}

pub fn execution_status_from_proto(value: i32) -> ExecutionStatus {
    use enums::WorkflowExecutionStatus as Proto;
    match Proto::from_i32(value).unwrap_or(Proto::Unspecified) {
        Proto::Running => ExecutionStatus::Running,
        Proto::Completed => ExecutionStatus::Completed,
        Proto::Failed => ExecutionStatus::Failed,
        Proto::Canceled => ExecutionStatus::Canceled,
        Proto::Terminated => ExecutionStatus::Terminated,
        Proto::TimedOut => ExecutionStatus::TimedOut,
        Proto::ContinuedAsNew => ExecutionStatus::ContinuedAsNew,
        Proto::Unspecified => ExecutionStatus::Running,
    }
}

pub fn workflow_execution_info_from_summary(
    value: &ExecutionSummary,
) -> workflowservice::WorkflowExecutionInfo {
    workflowservice::WorkflowExecutionInfo {
        namespace: value.namespace.as_str().to_owned(),
        workflow_id: value.workflow_id.as_str().to_owned(),
        run_id: value.run_id.to_string(),
        workflow_type: value.workflow_type.as_str().to_owned(),
        task_queue: Some(task_queue_from_domain(&value.task_queue)),
        status: execution_status_to_proto(value.status),
        start_time_unix_nanos: to_unix_nanos(value.start_time),
        execution_time_unix_nanos: value.execution_time.map(to_unix_nanos),
        close_time_unix_nanos: value.close_time.map(to_unix_nanos),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
    }
}

pub fn count_group_to_proto(value: &CountGroup) -> workflowservice::CountGroup {
    workflowservice::CountGroup {
        group_value: value.group_value.clone(),
        count: value.count as i64,
    }
}

pub fn page_token_to_bytes(value: &PageToken) -> Vec<u8> {
    value.0.clone()
}

pub fn page_token_from_bytes(value: Vec<u8>) -> PageToken {
    PageToken::new(value)
}

pub fn namespace_name_from_list_request(
    req: &workflowservice::ListWorkflowExecutionsRequest,
) -> NamespaceName {
    NamespaceName::from(req.namespace.clone())
}

fn to_unix_nanos(value: OffsetDateTime) -> i64 {
    let nanos = value.unix_timestamp_nanos();
    if nanos > i64::MAX as i128 {
        i64::MAX
    } else if nanos < i64::MIN as i128 {
        i64::MIN
    } else {
        nanos as i64
    }
}
