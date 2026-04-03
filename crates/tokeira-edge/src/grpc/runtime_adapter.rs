use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::{
    LoadedRun, SignalRequest, StartRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::TokeiraRuntime;
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::ExecutionRef;

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
}

pub fn commit_result_to_outcome(result: CommitResult) -> Result<WorkflowMutationOutcome> {
    match result {
        CommitResult::Applied { new_state } => Ok(WorkflowMutationOutcome {
            transition_seq: new_state.transition_seq.0,
            last_event_id: new_state.last_event_id,
            was_duplicate: false,
        }),
        CommitResult::Duplicate => Ok(WorkflowMutationOutcome {
            transition_seq: 0,
            last_event_id: 0,
            was_duplicate: true,
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
