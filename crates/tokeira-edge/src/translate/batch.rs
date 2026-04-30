//! Translation for Temporal batch operation transport messages.
//!
//! Batch operation execution stays in the edge layer, but the stored operation
//! shape is runtime-owned. These functions keep generated protobuf details out
//! of the runtime store.

use thiserror::Error;
use tokeira_proto::{
    common as proto_common, enums, public::temporal::api::batch::v1 as proto_batch, workflowservice,
};
use tokeira_runtime::{
    BatchOperationInfo, BatchOperationParams, BatchOperationSnapshot, BatchOperationState,
    BatchOperationType, BatchResetTarget, JobId, WorkflowExecutionRef,
};

#[derive(Debug, Error)]
pub enum BatchTranslateError {
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid batch operation request: {0}")]
    InvalidArgument(String),
    #[error("unsupported batch operation field: {0}")]
    Unsupported(&'static str),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartBatchOperationRequest {
    pub namespace: String,
    pub job_id: JobId,
    pub reason: String,
    pub visibility_query: Option<String>,
    pub executions: Option<Vec<WorkflowExecutionRef>>,
    pub max_operations_per_second: f32,
    pub operation_type: BatchOperationType,
    pub operation_params: BatchOperationParams,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StopBatchOperationRequest {
    pub namespace: String,
    pub job_id: JobId,
    pub reason: String,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeBatchOperationRequest {
    pub namespace: String,
    pub job_id: JobId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListBatchOperationsRequest {
    pub namespace: String,
    pub page_size: usize,
    pub next_page_token: Vec<u8>,
}

pub fn start_batch_request_to_edge(
    req: workflowservice::StartBatchOperationRequest,
) -> Result<StartBatchOperationRequest, BatchTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("namespace"));
    }
    if req.job_id.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("job_id"));
    }
    if !req.visibility_query.trim().is_empty() && !req.executions.is_empty() {
        return Err(BatchTranslateError::InvalidArgument(
            "visibility_query and executions are mutually exclusive".to_string(),
        ));
    }
    if req.visibility_query.trim().is_empty() && req.executions.is_empty() {
        return Err(BatchTranslateError::MissingField(
            "visibility_query or executions",
        ));
    }

    let visibility_query =
        (!req.visibility_query.trim().is_empty()).then(|| req.visibility_query.clone());
    let executions = (!req.executions.is_empty()).then(|| {
        req.executions
            .iter()
            .map(workflow_execution_to_ref)
            .collect::<Vec<_>>()
    });

    let (operation_type, operation_params) = match req
        .operation
        .ok_or(BatchTranslateError::MissingField("operation"))?
    {
        workflowservice::start_batch_operation_request::Operation::TerminationOperation(
            op,
        ) => {
            let details = op
                .details
                .as_ref()
                .map(tokeira_proto::conversions::common::payloads_to_domain);
            (
                BatchOperationType::Terminate,
                BatchOperationParams::Terminate {
                    details,
                    identity: op.identity,
                },
            )
        }
        workflowservice::start_batch_operation_request::Operation::CancellationOperation(
            op,
        ) => (
            BatchOperationType::Cancel,
            BatchOperationParams::Cancel {
                identity: op.identity,
            },
        ),
        workflowservice::start_batch_operation_request::Operation::SignalOperation(op) => {
            let input = op
                .input
                .as_ref()
                .map(tokeira_proto::conversions::common::payloads_to_domain);
            (
                BatchOperationType::Signal,
                BatchOperationParams::Signal {
                    signal_name: op.signal,
                    input,
                    identity: op.identity,
                },
            )
        }
        workflowservice::start_batch_operation_request::Operation::DeletionOperation(
            op,
        ) => (
            BatchOperationType::Delete,
            BatchOperationParams::Delete {
                identity: op.identity,
            },
        ),
        workflowservice::start_batch_operation_request::Operation::ResetOperation(op) => {
            let target = reset_target_from_proto(&op)?;
            (
                BatchOperationType::Reset,
                BatchOperationParams::Reset {
                    identity: op.identity,
                    target,
                    reason: req.reason.clone(),
                },
            )
        }
        workflowservice::start_batch_operation_request::Operation::UpdateWorkflowOptionsOperation(
            _,
        ) => {
            return Err(BatchTranslateError::Unsupported(
                "BatchOperationUpdateWorkflowExecutionOptions",
            ));
        }
    };

    Ok(StartBatchOperationRequest {
        namespace: req.namespace,
        job_id: JobId(req.job_id),
        reason: req.reason,
        visibility_query,
        executions,
        max_operations_per_second: req.max_operations_per_second,
        operation_type,
        operation_params,
    })
}

pub fn stop_batch_request_to_edge(
    req: workflowservice::StopBatchOperationRequest,
) -> Result<StopBatchOperationRequest, BatchTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("namespace"));
    }
    if req.job_id.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("job_id"));
    }
    Ok(StopBatchOperationRequest {
        namespace: req.namespace,
        job_id: JobId(req.job_id),
        reason: req.reason,
        identity: req.identity,
    })
}

pub fn describe_batch_request_to_edge(
    req: workflowservice::DescribeBatchOperationRequest,
) -> Result<DescribeBatchOperationRequest, BatchTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("namespace"));
    }
    if req.job_id.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("job_id"));
    }
    Ok(DescribeBatchOperationRequest {
        namespace: req.namespace,
        job_id: JobId(req.job_id),
    })
}

pub fn list_batch_request_to_edge(
    req: workflowservice::ListBatchOperationsRequest,
) -> Result<ListBatchOperationsRequest, BatchTranslateError> {
    if req.namespace.trim().is_empty() {
        return Err(BatchTranslateError::MissingField("namespace"));
    }
    Ok(ListBatchOperationsRequest {
        namespace: req.namespace,
        page_size: req.page_size.max(1) as usize,
        next_page_token: req.next_page_token,
    })
}

pub fn describe_batch_response_to_proto(
    snapshot: BatchOperationSnapshot,
) -> workflowservice::DescribeBatchOperationResponse {
    workflowservice::DescribeBatchOperationResponse {
        operation_type: batch_operation_type_to_proto(snapshot.operation_type) as i32,
        job_id: snapshot.job_id.0,
        state: batch_operation_state_to_proto(snapshot.state) as i32,
        start_time: Some(tokeira_proto::conversions::common::to_proto_timestamp(
            snapshot.start_time,
        )),
        close_time: snapshot
            .close_time
            .map(tokeira_proto::conversions::common::to_proto_timestamp),
        total_operation_count: snapshot.total_operation_count as i64,
        complete_operation_count: snapshot.complete_operation_count as i64,
        failure_operation_count: snapshot.failure_operation_count as i64,
        identity: snapshot.identity,
        reason: snapshot.reason,
    }
}

pub fn list_batch_response_to_proto(
    entries: Vec<BatchOperationInfo>,
    next_page_token: Option<Vec<u8>>,
) -> workflowservice::ListBatchOperationsResponse {
    workflowservice::ListBatchOperationsResponse {
        operation_info: entries.into_iter().map(batch_info_to_proto).collect(),
        next_page_token: next_page_token.unwrap_or_default(),
    }
}

pub fn batch_operation_type_to_proto(value: BatchOperationType) -> enums::BatchOperationType {
    match value {
        BatchOperationType::Terminate => enums::BatchOperationType::Terminate,
        BatchOperationType::Cancel => enums::BatchOperationType::Cancel,
        BatchOperationType::Signal => enums::BatchOperationType::Signal,
        BatchOperationType::Delete => enums::BatchOperationType::Delete,
        BatchOperationType::Reset => enums::BatchOperationType::Reset,
    }
}

pub fn batch_operation_state_to_proto(value: BatchOperationState) -> enums::BatchOperationState {
    match value {
        BatchOperationState::Running => enums::BatchOperationState::Running,
        BatchOperationState::Completed => enums::BatchOperationState::Completed,
        BatchOperationState::Failed => enums::BatchOperationState::Failed,
    }
}

fn workflow_execution_to_ref(value: &proto_common::WorkflowExecution) -> WorkflowExecutionRef {
    WorkflowExecutionRef {
        workflow_id: value.workflow_id.clone(),
        run_id: (!value.run_id.trim().is_empty()).then(|| value.run_id.clone()),
    }
}

fn batch_info_to_proto(value: BatchOperationInfo) -> proto_batch::BatchOperationInfo {
    proto_batch::BatchOperationInfo {
        job_id: value.job_id.0,
        state: batch_operation_state_to_proto(value.state) as i32,
        start_time: Some(tokeira_proto::conversions::common::to_proto_timestamp(
            value.start_time,
        )),
        close_time: value
            .close_time
            .map(tokeira_proto::conversions::common::to_proto_timestamp),
    }
}

fn reset_target_from_proto(
    value: &proto_batch::BatchOperationReset,
) -> Result<BatchResetTarget, BatchTranslateError> {
    if let Some(options) = &value.options {
        use proto_common::reset_options::Target;
        return match options.target.as_ref() {
            Some(Target::WorkflowTaskId(event_id)) => {
                Ok(BatchResetTarget::WorkflowTaskId(*event_id))
            }
            Some(Target::FirstWorkflowTask(_)) => Ok(BatchResetTarget::FirstWorkflowTask),
            Some(Target::LastWorkflowTask(_)) => Ok(BatchResetTarget::LastWorkflowTask),
            Some(Target::BuildId(build_id)) => Ok(BatchResetTarget::BuildId(build_id.clone())),
            None => Err(BatchTranslateError::MissingField("reset.options.target")),
        };
    }

    match enums::ResetType::try_from(value.reset_type).unwrap_or(enums::ResetType::Unspecified) {
        enums::ResetType::FirstWorkflowTask => Ok(BatchResetTarget::FirstWorkflowTask),
        enums::ResetType::LastWorkflowTask => Ok(BatchResetTarget::LastWorkflowTask),
        enums::ResetType::Unspecified => Err(BatchTranslateError::MissingField(
            "reset.options.target or reset_type",
        )),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::*;

    fn sample_snapshot(
        operation_type: BatchOperationType,
        state: BatchOperationState,
    ) -> BatchOperationSnapshot {
        BatchOperationSnapshot {
            job_id: JobId("job-1".to_string()),
            namespace_id: tokeira_types::NamespaceId(Uuid::nil()),
            operation_type,
            state,
            start_time: OffsetDateTime::UNIX_EPOCH,
            close_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5)),
            total_operation_count: 10,
            complete_operation_count: 7,
            failure_operation_count: 3,
            identity: "starter".to_string(),
            reason: "reason".to_string(),
        }
    }

    fn arb_operation_type() -> impl Strategy<Value = BatchOperationType> {
        prop_oneof![
            Just(BatchOperationType::Terminate),
            Just(BatchOperationType::Cancel),
            Just(BatchOperationType::Signal),
            Just(BatchOperationType::Delete),
            Just(BatchOperationType::Reset),
        ]
    }

    fn arb_operation_state() -> impl Strategy<Value = BatchOperationState> {
        prop_oneof![
            Just(BatchOperationState::Running),
            Just(BatchOperationState::Completed),
            Just(BatchOperationState::Failed),
        ]
    }

    // Feature: edge-batch-operations-transport, Property 3: Proto translation round-trip for batch types
    proptest! {
        #[test]
        fn property_batch_proto_projection(
            operation_type in arb_operation_type(),
            state in arb_operation_state(),
        ) {
            let snapshot = sample_snapshot(operation_type, state);
            let describe = describe_batch_response_to_proto(snapshot.clone());
            prop_assert_eq!(describe.job_id, snapshot.job_id.0.clone());
            prop_assert_eq!(describe.operation_type, batch_operation_type_to_proto(operation_type) as i32);
            prop_assert_eq!(describe.state, batch_operation_state_to_proto(state) as i32);
            prop_assert_eq!(describe.total_operation_count, snapshot.total_operation_count as i64);
            prop_assert_eq!(describe.complete_operation_count, snapshot.complete_operation_count as i64);
            prop_assert_eq!(describe.failure_operation_count, snapshot.failure_operation_count as i64);
            prop_assert_eq!(describe.identity, snapshot.identity);
            prop_assert_eq!(describe.reason, snapshot.reason);

            let list = list_batch_response_to_proto(
                vec![BatchOperationInfo {
                    job_id: snapshot.job_id.clone(),
                    state,
                    start_time: snapshot.start_time,
                    close_time: snapshot.close_time,
                }],
                Some(vec![1, 2, 3]),
            );
            prop_assert_eq!(list.operation_info.len(), 1);
            prop_assert_eq!(list.operation_info[0].job_id.as_str(), snapshot.job_id.0.as_str());
            prop_assert_eq!(list.operation_info[0].state, batch_operation_state_to_proto(state) as i32);
            prop_assert_eq!(list.next_page_token, vec![1, 2, 3]);
        }
    }

    // Feature: edge-batch-operations-transport, Property 4: Proto validation rejects invalid inputs
    proptest! {
        #[test]
        fn property_batch_validation_rejects_invalid_inputs(choice in 0u8..5u8) {
            let mut request = workflowservice::StartBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: "job-1".to_string(),
                reason: "reason".to_string(),
                visibility_query: "WorkflowType = 'demo'".to_string(),
                executions: Vec::new(),
                max_operations_per_second: 1.0,
                operation: Some(
                    workflowservice::start_batch_operation_request::Operation::CancellationOperation(
                        proto_batch::BatchOperationCancellation {
                            identity: "starter".to_string(),
                        }
                    )
                ),
                ..Default::default()
            };

            match choice {
                0 => request.namespace.clear(),
                1 => request.job_id.clear(),
                2 => {
                    request.visibility_query.clear();
                    request.executions.clear();
                }
                3 => request.operation = None,
                4 => request.executions.push(proto_common::WorkflowExecution {
                    workflow_id: "wf-1".to_string(),
                    run_id: String::new(),
                    ..Default::default()
                }),
                _ => unreachable!(),
            }

            prop_assert!(start_batch_request_to_edge(request).is_err());
        }
    }
}
