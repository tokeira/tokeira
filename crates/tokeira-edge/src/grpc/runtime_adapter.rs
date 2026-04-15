use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::{
    CancelRequest, LoadedRun, ResetRequest, SignalRequest, StartRequest,
    TerminateRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    PendingUpdateTransport, QueryResult, ResetWorkflowResult, SignalWithStartResult,
    StartWorkflowResult, StartedActivityTask, TokeiraRuntime, UpdateOutcome,
    UpdateTransportResolution, UpdateWaitPolicy,
};
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::{ActivityTaskToken, ExecutionRef, Payloads, RequestContext};

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

    async fn start_workflow_with_policy(
        &self,
        req: StartRequest,
    ) -> Result<StartWorkflowResult> {
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

    async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
    ) -> Result<WorkflowMutationOutcome> {
        let commit = self.runtime.complete_activity_task(token, result).await?;
        commit_result_to_outcome(commit)
    }

    async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure_message: String,
        failure_error_type: Option<String>,
    ) -> Result<()> {
        self.runtime
            .fail_activity_task(token, failure_message, failure_error_type)
            .await
    }

    async fn record_activity_heartbeat(&self, token: ActivityTaskToken) -> Result<bool> {
        self.runtime.record_activity_heartbeat(token).await
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
}

pub fn commit_result_to_outcome(result: CommitResult) -> Result<WorkflowMutationOutcome> {
    match result {
        CommitResult::Applied { new_state } => {
            let new_run_id =
                if new_state.status == tokeira_types::ExecutionStatus::ContinuedAsNew {
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
