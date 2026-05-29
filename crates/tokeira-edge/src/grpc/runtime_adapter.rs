use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::{
    CancelRequest, Command, LoadedRun, NexusResolution, ResetRequest, SignalRequest, StartRequest,
    TerminateRequest, UpdateActivityOptionsRequest as KernelUpdateActivityOptionsRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTokenResolutionError, PendingUpdateTransport, QueryResult, ResetWorkflowResult,
    SignalWithStartResult, StartWorkflowResult, StartedActivityTask, TokeiraRuntime, UpdateOutcome,
    UpdateTransportResolution, UpdateWaitPolicy,
};
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::{ActivityTaskToken, ExecutionRef, Payload, Payloads, RequestContext, RunKey};

use crate::workflow_service::{WorkflowMutationOutcome, WorkflowRuntimeApi};

#[derive(Clone)]
pub struct RuntimeAdapter<R> {
    runtime: Arc<TokeiraRuntime<R>>,
}

impl<R> RuntimeAdapter<R> {
    pub fn new(runtime: Arc<TokeiraRuntime<R>>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl<R> WorkflowRuntimeApi for RuntimeAdapter<R>
where
    R: RunRepository + 'static,
{
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.start_workflow(req).await?;
        commit_result_to_outcome(result)
    }

    async fn start_workflow_with_policy(&self, req: StartRequest) -> Result<StartWorkflowResult> {
        self.runtime.start_workflow_with_policy(req).await
    }

    async fn signal_with_start_workflow(
        &self,
        req: tokeira_kernel::SignalWithStartRequest,
    ) -> Result<SignalWithStartResult> {
        self.runtime.signal_with_start_workflow(req).await
    }

    async fn signal_workflow(
        &self,
        run_key: tokeira_types::RunKey,
        req: SignalRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self.runtime.signal_workflow(execution, req).await?;
        commit_result_to_outcome(result)
    }

    async fn poll_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
        self.runtime
            .poll_workflow_task(queue, worker_identity, timeout)
            .await
    }

    async fn try_claim_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: tokeira_types::RunKey,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
        self.runtime
            .try_claim_workflow_task(queue, run_key, worker_identity)
            .await
    }

    async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.complete_workflow_task(req).await?;
        commit_result_to_outcome(result)
    }

    async fn poll_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<StartedActivityTask>> {
        self.runtime
            .poll_activity_task(queue, worker_identity, timeout)
            .await
    }

    async fn try_claim_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: tokeira_types::RunKey,
        activity_id: String,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        self.runtime
            .try_claim_activity_task(queue, run_key, activity_id, worker_identity)
            .await
    }

    async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<WorkflowMutationOutcome> {
        let commit = self
            .runtime
            .complete_activity_task(token, result, worker_identity)
            .await?;
        commit_result_to_outcome(commit)
    }

    async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<()> {
        self.runtime
            .fail_activity_task(
                token,
                failure,
                failure_error_type,
                is_non_retryable,
                worker_identity,
            )
            .await
    }

    async fn cancel_activity_task(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self
            .runtime
            .cancel_activity_task(token, details, worker_identity)
            .await?;
        commit_result_to_outcome(result)
    }

    async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
    ) -> Result<bool> {
        self.runtime.record_activity_heartbeat(token, details).await
    }

    async fn resolve_activity_token(
        &self,
        run_key: RunKey,
        activity_id: &str,
    ) -> std::result::Result<ActivityTaskToken, ActivityTokenResolutionError> {
        self.runtime
            .resolve_activity_token(run_key, activity_id)
            .await
    }

    async fn update_activity_options(
        &self,
        run_key: RunKey,
        req: KernelUpdateActivityOptionsRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self
            .runtime
            .submit(run_key, Command::UpdateActivityOptions(req))
            .await?;
        commit_result_to_outcome(result)
    }

    async fn terminate_workflow(
        &self,
        run_key: tokeira_types::RunKey,
        req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self.runtime.terminate_workflow(execution, req).await?;
        commit_result_to_outcome(result)
    }

    async fn cancel_workflow(
        &self,
        run_key: tokeira_types::RunKey,
        req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self.runtime.cancel_workflow(execution, req).await?;
        commit_result_to_outcome(result)
    }

    async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        req: ResetRequest,
    ) -> Result<ResetWorkflowResult> {
        self.runtime.reset_workflow(execution, req).await
    }

    async fn query_workflow(
        &self,
        execution: ExecutionRef,
        query_type: String,
        query_args: Payloads,
        timeout: std::time::Duration,
    ) -> Result<QueryResult> {
        let timeout: time::Duration =
            time::Duration::try_from(timeout).map_err(|_| anyhow!("invalid timeout"))?;
        self.runtime
            .query_workflow(execution, query_type, query_args, timeout)
            .await
    }

    async fn update_workflow(
        &self,
        execution: ExecutionRef,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout: std::time::Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateOutcome> {
        let timeout: time::Duration =
            time::Duration::try_from(timeout).map_err(|_| anyhow!("invalid timeout"))?;
        self.runtime
            .update_workflow(
                execution,
                update_id,
                update_name,
                input,
                request,
                timeout,
                wait_policy,
            )
            .await
    }

    async fn pending_update_transports(
        &self,
        run_key: tokeira_types::RunKey,
    ) -> Result<Vec<PendingUpdateTransport>> {
        Ok(self.runtime.pending_update_transports(run_key))
    }

    async fn resolve_update_transport(
        &self,
        run_key: tokeira_types::RunKey,
        update_id: String,
        resolution: UpdateTransportResolution,
    ) -> Result<bool> {
        Ok(self
            .runtime
            .resolve_update_transport(run_key, &update_id, resolution))
    }

    async fn peek_update_info(
        &self,
        run_key: tokeira_types::RunKey,
        update_id: String,
    ) -> Result<Option<(String, Payloads)>> {
        Ok(self
            .runtime
            .update_registry()
            .peek_update_info(run_key, &update_id))
    }

    async fn resolve_nexus_operation(
        &self,
        run_key: tokeira_types::RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        resolution: NexusResolution,
    ) -> Result<bool> {
        self.runtime
            .resolve_nexus_operation(run_key, operation_id, scheduled_event_id, resolution)
            .await
    }
}

pub fn commit_result_to_outcome(result: CommitResult) -> Result<WorkflowMutationOutcome> {
    match result {
        CommitResult::Applied { new_state } => {
            let new_run_id = if new_state.status == tokeira_types::ExecutionStatus::ContinuedAsNew {
                // The new run ID is not directly
                // available on WorkflowState; the
                // caller should extract it from the
                // ContinuedAsNew history event. For
                // now we leave it as None.
                None
            } else {
                None
            };
            Ok(WorkflowMutationOutcome {
                transition_seq: new_state.transition_seq.0,
                last_event_id: new_state.last_event_id,
                was_duplicate: false,
                execution_status: new_state.status,
                new_run_id,
            })
        }
        CommitResult::Duplicate => Ok(WorkflowMutationOutcome {
            transition_seq: 0,
            last_event_id: 0,
            was_duplicate: true,
            execution_status: tokeira_types::ExecutionStatus::Running,
            new_run_id: None,
        }),
        CommitResult::Conflict { reason } => Err(anyhow!("conflict: {reason}")),
    }
}

async fn execution_for_run<R>(
    runtime: &TokeiraRuntime<R>,
    run_key: tokeira_types::RunKey,
) -> Result<ExecutionRef>
where
    R: RunRepository + 'static,
{
    let loaded = runtime.repo().load_run(run_key).await?;
    match loaded {
        LoadedRun::Existing(state) => Ok(ExecutionRef {
            namespace_id: state.namespace_id,
            workflow_id: state.workflow_id,
            run_id: Some(state.run_id),
        }),
        LoadedRun::Absent => Err(anyhow!("run not found: {:?}", run_key)),
    }
}
