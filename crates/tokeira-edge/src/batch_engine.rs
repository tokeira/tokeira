//! Batch operation execution loop.
//!
//! A batch is finite work started by an edge request. The runtime owns the
//! persisted progress state, while this edge task owns visibility discovery and
//! the pre-validated dispatch context needed to mutate matching workflows.

use std::sync::{Arc, atomic::Ordering};

use time::OffsetDateTime;
use tokeira_runtime::{
    BatchOperationParams, BatchOperationState, BatchOperationStore, BatchResetTarget, JobId,
    WorkflowExecutionRef,
};
use tokeira_types::NamespaceId;
use tokio_util::sync::CancellationToken;

use crate::{
    errors::{EdgeError, EdgeResult},
    translate::ListWorkflowExecutionsResponse,
    workflow_service::{BatchDispatchContext, WorkflowService},
};

const DEFAULT_RATE_LIMIT: f32 = 50.0;

pub async fn run_batch_operation(
    store: Arc<BatchOperationStore>,
    service: WorkflowService,
    dispatch_ctx: BatchDispatchContext,
    namespace_id: NamespaceId,
    job_id: JobId,
    cancellation_token: CancellationToken,
) {
    let result = run_batch_operation_inner(
        store.clone(),
        service,
        dispatch_ctx,
        namespace_id,
        job_id.clone(),
        cancellation_token,
    )
    .await;

    let state = if result.is_ok() {
        BatchOperationState::Completed
    } else {
        BatchOperationState::Failed
    };
    let _ = store.set_state(
        namespace_id,
        &job_id,
        state,
        Some(OffsetDateTime::now_utc()),
    );
}

async fn run_batch_operation_inner(
    store: Arc<BatchOperationStore>,
    service: WorkflowService,
    dispatch_ctx: BatchDispatchContext,
    namespace_id: NamespaceId,
    job_id: JobId,
    cancellation_token: CancellationToken,
) -> EdgeResult<()> {
    let entry = store
        .entry(namespace_id, &job_id)
        .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
    let workflows = discover_workflows(&service, &dispatch_ctx, &entry).await?;
    entry
        .counters
        .total
        .store(workflows.len() as u64, Ordering::Relaxed);

    let sleep_duration = compute_sleep_duration(entry.max_operations_per_second);
    for workflow_ref in workflows {
        if cancellation_token.is_cancelled() {
            break;
        }
        let result = apply_operation(
            &service,
            &dispatch_ctx,
            &workflow_ref,
            &entry.operation_params,
        )
        .await;
        match result {
            Ok(()) => {
                entry.counters.complete.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                entry.counters.failure.fetch_add(1, Ordering::Relaxed);
            }
        }
        tokio::time::sleep(sleep_duration).await;
    }

    Ok(())
}

async fn discover_workflows(
    service: &WorkflowService,
    dispatch_ctx: &BatchDispatchContext,
    entry: &tokeira_runtime::BatchOperationEntry,
) -> EdgeResult<Vec<WorkflowExecutionRef>> {
    if let Some(executions) = &entry.executions {
        return Ok(executions.clone());
    }

    let query = entry.visibility_query.clone().unwrap_or_default();
    let mut next_page_token = None;
    let mut workflows = Vec::new();
    loop {
        let page: ListWorkflowExecutionsResponse = service
            .list_workflows_batch_internal(dispatch_ctx, Some(query.clone()), next_page_token)
            .await?;
        workflows.extend(
            page.executions
                .into_iter()
                .map(|execution| WorkflowExecutionRef {
                    workflow_id: execution.workflow_id,
                    run_id: Some(execution.run_id.0.to_string()),
                }),
        );
        match page.next_page_token {
            Some(token) if !token.is_empty() => next_page_token = Some(token),
            _ => break,
        }
    }
    Ok(workflows)
}

async fn apply_operation(
    service: &WorkflowService,
    ctx: &BatchDispatchContext,
    workflow_ref: &WorkflowExecutionRef,
    params: &BatchOperationParams,
) -> EdgeResult<()> {
    match params {
        BatchOperationParams::Terminate { details, identity } => {
            service
                .terminate_workflow_batch_internal(
                    ctx,
                    workflow_ref,
                    details.clone(),
                    identity.clone(),
                )
                .await
        }
        BatchOperationParams::Cancel { .. } => {
            service
                .cancel_workflow_batch_internal(ctx, workflow_ref)
                .await
        }
        BatchOperationParams::Signal {
            signal_name, input, ..
        } => {
            service
                .signal_workflow_batch_internal(
                    ctx,
                    workflow_ref,
                    signal_name.clone(),
                    input.clone().unwrap_or_default(),
                )
                .await
        }
        BatchOperationParams::Delete { identity } => {
            service
                .delete_workflow_batch_internal(ctx, workflow_ref, identity.clone())
                .await
        }
        BatchOperationParams::Reset {
            target,
            reason,
            identity: _,
        } => {
            let fork_event_id = service
                .resolve_reset_target_batch_internal(ctx, workflow_ref, target)
                .await?;
            service
                .reset_workflow_batch_internal(ctx, workflow_ref, fork_event_id, reason.clone())
                .await
        }
        BatchOperationParams::UpdateWorkflowExecutionOptions {
            versioning_override,
            priority,
            ..
        } => {
            service
                .update_workflow_execution_options_batch_internal(
                    ctx,
                    workflow_ref,
                    versioning_override.clone(),
                    priority.clone(),
                )
                .await
        }
        BatchOperationParams::UnpauseActivity {
            target,
            reset_attempts,
            reset_heartbeat,
            jitter,
            ..
        } => {
            service
                .unpause_activity_batch_internal(
                    ctx,
                    workflow_ref,
                    target.clone(),
                    *reset_attempts,
                    *reset_heartbeat,
                    *jitter,
                )
                .await
        }
        BatchOperationParams::UpdateActivityOptions { patch, .. } => {
            service
                .update_activity_options_batch_internal(ctx, workflow_ref, patch.clone())
                .await
        }
    }
}

pub fn compute_sleep_duration(max_ops_per_second: f32) -> tokio::time::Duration {
    let rate = if max_ops_per_second > 0.0 {
        max_ops_per_second
    } else {
        DEFAULT_RATE_LIMIT
    };
    tokio::time::Duration::from_secs_f64(1.0 / f64::from(rate))
}

pub(crate) fn is_reset_target_event(kind: &tokeira_kernel::HistoryEventKind) -> bool {
    matches!(
        kind,
        tokeira_kernel::HistoryEventKind::WorkflowTaskCompleted { .. }
            | tokeira_kernel::HistoryEventKind::WorkflowTaskFailed { .. }
            | tokeira_kernel::HistoryEventKind::WorkflowTaskTimedOut { .. }
            | tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { .. }
    )
}

pub(crate) fn resolve_reset_target_from_history(
    history: &[tokeira_kernel::HistoryEvent],
    target: &BatchResetTarget,
) -> EdgeResult<i64> {
    match target {
        BatchResetTarget::WorkflowTaskId(event_id) => {
            let event = history
                .iter()
                .find(|event| event.event_id == *event_id)
                .ok_or_else(|| {
                    EdgeError::BadRequest(format!("reset target event_id {} not found", event_id))
                })?;
            if is_reset_target_event(&event.kind) {
                Ok(*event_id)
            } else {
                Err(EdgeError::BadRequest(format!(
                    "reset target event_id {} must be a workflow task event",
                    event_id
                )))
            }
        }
        BatchResetTarget::FirstWorkflowTask => history
            .iter()
            .find(|event| is_reset_target_event(&event.kind))
            .map(|event| event.event_id)
            .ok_or_else(|| EdgeError::BadRequest("no workflow task event found".to_string())),
        BatchResetTarget::LastWorkflowTask => history
            .iter()
            .rev()
            .find(|event| is_reset_target_event(&event.kind))
            .map(|event| event.event_id)
            .ok_or_else(|| EdgeError::BadRequest("no workflow task event found".to_string())),
        BatchResetTarget::BuildId(_) => Err(EdgeError::BadRequest(
            "batch reset BuildId target is not supported yet".to_string(),
        )),
    }
}
