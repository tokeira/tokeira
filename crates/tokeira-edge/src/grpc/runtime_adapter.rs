use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::{
    CancelRequest, LoadedRun, NexusResolution, ResetRequest, SignalRequest, StartRequest,
    TerminateRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTokenResolutionError, CreateDeployment, CreateVersion, DeleteDeployment, DeleteVersion,
    DeleteWorkflowRequest, DeploymentPage, DeploymentRegistry, DeploymentView, DescribeVersion,
    ListDeployments, MultiOperationResult, PendingUpdateTransport, QueryResult,
    RegisterPolledDeployment, RegistryError, ResetWorkflowResult, SetCurrent, SetCurrentOutcome,
    SetManager, SetManagerOutcome, SetRamping, SetRampingOutcome, SignalWithStartResult,
    StartWorkflowResult, StartedActivityTask, TaskQueueVersioningView, TokeiraRuntime,
    UpdateActivitiesOptionsRequest, UpdateComputeConfig, UpdateLifecycleSnapshot, UpdateMetadata,
    UpdateTransportResolution, UpdateWaitPolicy, ValidateComputeConfig, VersionMetadataView,
    VersionView, WorkflowDeletion,
};
use tokeira_storage::{
    CommitResult, ConflictToken, DeploymentKey, DeploymentTaskQueueType, RunRepository,
    WorkerDeploymentVersionStatus,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionRef, NamespaceId, Payload, Payloads,
    QueueKey, RequestContext, RunKey, TaskKind, TaskQueueName, WorkerIdentity,
};

use crate::{
    errors::{EdgeError, EdgeResult},
    workflow_service::{
        DeploymentMutationOutcome, WorkerDeploymentRuntimeApi, WorkflowMutationOutcome,
        WorkflowRuntimeApi,
    },
};

#[derive(Clone, Debug)]
pub struct RuntimeAdapter<R> {
    runtime: Arc<TokeiraRuntime<R>>,
}

impl<R> RuntimeAdapter<R> {
    pub fn new(runtime: Arc<TokeiraRuntime<R>>) -> Self {
        Self { runtime }
    }

    fn worker_deployment_registry(&self) -> EdgeResult<DeploymentRegistry>
    where
        R: RunRepository + 'static,
    {
        self.runtime.deployment_registry().ok_or_else(|| {
            EdgeError::FailedPrecondition(
                "worker deployment registry is not configured for this runtime".to_string(),
            )
        })
    }

    async fn promote_unversioned_backlog(
        &self,
        deployment: &DeploymentView,
        build_id: &tokeira_storage::BuildId,
    ) where
        R: RunRepository + 'static,
    {
        let Some(version) = deployment
            .versions
            .iter()
            .find(|version| &version.build_id == build_id)
        else {
            return;
        };
        for membership in &version.task_queues {
            let task_kind = match membership.task_queue.task_queue_type {
                DeploymentTaskQueueType::Workflow => TaskKind::Workflow,
                DeploymentTaskQueueType::Activity => TaskKind::Activity,
                DeploymentTaskQueueType::Nexus | DeploymentTaskQueueType::Unspecified => continue,
            };
            let target = QueueKey {
                namespace_id: deployment.namespace_id,
                task_queue: TaskQueueName(membership.task_queue.name.clone()),
                task_kind,
                deployment: Some(DeploymentId(deployment.name.0.clone())),
                build_id: Some(BuildId(build_id.0.clone())),
            };
            match task_kind {
                TaskKind::Workflow => {
                    self.runtime
                        .broker()
                        .promote_unversioned_backlog(&target)
                        .await;
                }
                TaskKind::Activity => {
                    self.runtime
                        .activity_broker()
                        .promote_unversioned_backlog(&target)
                        .await;
                }
            }
        }
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

    async fn reset_sticky_task_queue(&self, run_key: tokeira_types::RunKey) -> Result<()> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        self.runtime.reset_sticky_task_queue(execution).await?;
        Ok(())
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

    async fn fail_workflow_task(
        &self,
        token: tokeira_types::WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<tokeira_types::Payload>,
        worker_identity: tokeira_types::WorkerIdentity,
        request: tokeira_types::RequestContext,
        now: time::OffsetDateTime,
    ) -> Result<()> {
        self.runtime
            .fail_workflow_task(
                token,
                failure_cause,
                failure_details,
                worker_identity,
                request,
                now,
            )
            .await?;
        Ok(())
    }

    async fn poll_workflow_activation(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::WorkflowActivation>> {
        self.runtime
            .poll_workflow_activation(queue, worker_identity, timeout)
            .await
    }

    async fn workflow_poller_scaling_decision(
        &self,
        queue: &tokeira_types::QueueKey,
    ) -> Option<i32> {
        self.runtime
            .broker()
            .has_runnable_backlog(queue)
            .await
            .then_some(1)
    }

    async fn workflow_poll_cancelled(
        &self,
        queue: &tokeira_types::QueueKey,
        worker: &WorkerIdentity,
    ) -> bool {
        self.runtime.broker().is_worker_denied(queue, worker).await
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

    async fn poll_activity_task_offer(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::ActivityTaskOffer>> {
        self.runtime
            .poll_activity_task_offer(queue, worker_identity, timeout)
            .await
    }

    async fn start_activity_task_offer(
        &self,
        offer: tokeira_runtime::ActivityTaskOffer,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        self.runtime
            .start_activity_task_offer(offer, worker_identity)
            .await
    }

    async fn republish_activity_offer(&self, offer: tokeira_runtime::ActivityTaskOffer) {
        self.runtime.republish_activity_offer(offer).await;
    }

    async fn activity_poller_scaling_decision(
        &self,
        queue: &tokeira_types::QueueKey,
    ) -> Option<i32> {
        self.runtime
            .activity_broker()
            .has_runnable_backlog(queue)
            .await
            .then_some(1)
    }

    async fn task_queue_backlog_stats(
        &self,
        queue: &tokeira_types::QueueKey,
    ) -> tokeira_runtime::BrokerBacklogStats {
        match queue.task_kind {
            tokeira_types::TaskKind::Activity => {
                self.runtime.activity_broker().backlog_stats(queue).await
            }
            tokeira_types::TaskKind::Workflow => self.runtime.broker().backlog_stats(queue).await,
        }
    }

    async fn absorb_unversioned_backlog(&self, queue: &tokeira_types::QueueKey) {
        if queue.task_kind == TaskKind::Activity {
            if let Err(error) = self
                .runtime
                .reprocess_unversioned_activity_backlog(queue)
                .await
            {
                // Poll admission remains available: the durable run/dispatch
                // state survives and a later poll or recovery sweep retries
                // this derived-coordinate reconciliation.
                tracing::warn!(
                    ?error,
                    ?queue,
                    "failed to reprocess unversioned activity backlog"
                );
            }
            return;
        }
        let (Some(deployment), Some(build_id), Some(registry)) = (
            queue.deployment.as_ref(),
            queue.build_id.as_ref(),
            self.runtime.deployment_registry(),
        ) else {
            return;
        };
        let Ok(Some(versioning)) = registry
            .task_queue_versioning(queue.namespace_id, &queue.task_queue.0)
            .await
        else {
            return;
        };
        let is_target = |version: &tokeira_storage::WorkerDeploymentVersionKey| {
            version.deployment_name.0 == deployment.0 && version.build_id.0 == build_id.0
        };
        let routes_to_worker = versioning.current_version.as_ref().is_some_and(is_target)
            || (versioning.ramping_percentage > 0.0
                && versioning.ramping_version.as_ref().is_some_and(is_target));
        if !routes_to_worker {
            return;
        }
        self.runtime
            .broker()
            .promote_unversioned_backlog(queue)
            .await;
    }

    async fn cancel_outstanding_worker_polls(
        &self,
        namespace_id: NamespaceId,
        task_queue: TaskQueueName,
        worker: WorkerIdentity,
    ) -> bool {
        self.runtime
            .cancel_outstanding_worker_polls(namespace_id, task_queue, worker)
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
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let commit = self
            .runtime
            .complete_activity_task(token, result, worker_identity, request)
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
        request: RequestContext,
    ) -> Result<()> {
        self.runtime
            .fail_activity_task(
                token,
                failure,
                failure_error_type,
                is_non_retryable,
                worker_identity,
                request,
            )
            .await
    }

    async fn cancel_activity_task(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self
            .runtime
            .cancel_activity_task(token, details, worker_identity, request)
            .await?;
        commit_result_to_outcome(result)
    }

    async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome> {
        self.runtime
            .record_activity_heartbeat(token, details, identity)
            .await
    }

    async fn pause_activities(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::PauseActivityRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.pause_activities(run_key, req).await?;
        commit_result_to_outcome(result)
    }

    async fn unpause_activities(
        &self,
        run_key: RunKey,
        req: tokeira_runtime::UnpauseActivitiesRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.unpause_activities(run_key, req).await?;
        commit_result_to_outcome(result)
    }

    async fn reset_activities(
        &self,
        run_key: RunKey,
        req: tokeira_runtime::ResetActivitiesRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.reset_activities(run_key, req).await?;
        commit_result_to_outcome(result)
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

    async fn force_start_activity_for_completion(
        &self,
        run_key: RunKey,
        activity_id: &str,
        identity: tokeira_types::WorkerIdentity,
        request: RequestContext,
    ) -> Result<ActivityTaskToken> {
        self.runtime
            .force_start_activity_for_completion(run_key, activity_id, identity, request)
            .await
    }

    async fn update_activity_options(
        &self,
        run_key: RunKey,
        req: UpdateActivitiesOptionsRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let result = self.runtime.update_activity_options(run_key, req).await?;
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

    async fn delete_workflow(
        &self,
        run_key: RunKey,
        request: DeleteWorkflowRequest,
    ) -> Result<WorkflowDeletion> {
        self.runtime.delete_workflow(run_key, request).await
    }

    async fn update_workflow_execution_options(
        &self,
        run_key: tokeira_types::RunKey,
        versioning_override: tokeira_kernel::VersioningOverrideChange,
        request: tokeira_types::RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self
            .runtime
            .update_workflow_execution_options(execution, versioning_override, request)
            .await?;
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

    async fn pause_workflow(
        &self,
        run_key: tokeira_types::RunKey,
        req: tokeira_kernel::PauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self.runtime.pause_workflow(execution, req).await?;
        commit_result_to_outcome(result)
    }

    async fn unpause_workflow(
        &self,
        run_key: tokeira_types::RunKey,
        req: tokeira_kernel::UnpauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let execution = execution_for_run(self.runtime.as_ref(), run_key).await?;
        let result = self.runtime.unpause_workflow(execution, req).await?;
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
    ) -> Result<UpdateLifecycleSnapshot> {
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

    async fn execute_multi_operation(
        &self,
        start: StartRequest,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout: std::time::Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<MultiOperationResult> {
        let timeout: time::Duration =
            time::Duration::try_from(timeout).map_err(|_| anyhow!("invalid timeout"))?;
        self.runtime
            .execute_multi_operation(
                start,
                update_id,
                update_name,
                input,
                request,
                timeout,
                wait_policy,
            )
            .await
    }

    async fn poll_workflow_update(
        &self,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout: std::time::Duration,
    ) -> Result<UpdateLifecycleSnapshot> {
        let timeout: time::Duration =
            time::Duration::try_from(timeout).map_err(|_| anyhow!("invalid timeout"))?;
        self.runtime
            .poll_workflow_update(execution, update_id, wait_policy, timeout)
            .await
    }

    async fn pending_update_transports(
        &self,
        run_key: tokeira_types::RunKey,
        include_sent: bool,
    ) -> Result<Vec<PendingUpdateTransport>> {
        Ok(self
            .runtime
            .pending_update_transports(run_key, include_sent))
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

    async fn record_nexus_cancellation_attempt(
        &self,
        run_key: tokeira_types::RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        requested_event_id: i64,
        outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
    ) -> Result<bool> {
        self.runtime
            .record_nexus_cancellation_attempt(
                run_key,
                operation_id,
                scheduled_event_id,
                requested_event_id,
                outcome,
            )
            .await
    }
}

#[async_trait]
impl<R> WorkerDeploymentRuntimeApi for RuntimeAdapter<R>
where
    R: RunRepository + 'static,
{
    async fn create_worker_deployment(
        &self,
        req: CreateDeployment,
    ) -> EdgeResult<DeploymentMutationOutcome<DeploymentView>> {
        let registry = self.worker_deployment_registry()?;
        // Capture the name before `req` is consumed: an explicit create collision must
        // name the deployment in the error, matching v1.31.0
        // `ErrWorkerDeploymentAlreadyExists = "Worker Deployment with name %q already exists"`
        // (`service/worker/workerdeployment/util.go:105 @ v1.31.0`).
        let deployment_name = req.deployment_name.0.clone();
        let view = registry.create_deployment(req).await.map_err(|error| match error {
            RegistryError::AlreadyExists => EdgeError::AlreadyExists(format!(
                "Worker Deployment with name {deployment_name:?} already exists"
            )),
            RegistryError::AlreadyExistsAutoCreated => EdgeError::AlreadyExists(format!(
                "Worker Deployment with name {deployment_name:?} already exists (auto-created from worker polls)"
            )),
            other => registry_error_to_edge(other),
        })?;
        Ok(deployment_mutation_outcome(view.conflict_token, view))
    }

    async fn describe_worker_deployment(&self, key: DeploymentKey) -> EdgeResult<DeploymentView> {
        self.worker_deployment_registry()?
            .describe_deployment(key)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn delete_worker_deployment(&self, req: DeleteDeployment) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .delete_deployment(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn list_worker_deployments(&self, req: ListDeployments) -> EdgeResult<DeploymentPage> {
        self.worker_deployment_registry()?
            .list_deployments(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn create_worker_deployment_version(&self, req: CreateVersion) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .create_version(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn describe_worker_deployment_version(
        &self,
        req: DescribeVersion,
    ) -> EdgeResult<VersionView> {
        let report_stats = req.report_task_queue_stats;
        let namespace_id = req.namespace_id;
        let mut view = self
            .worker_deployment_registry()?
            .describe_version(req)
            .await
            .map_err(registry_error_to_edge)?;
        if report_stats {
            for membership in &mut view.task_queues {
                let task_kind = match membership.task_queue.task_queue_type {
                    DeploymentTaskQueueType::Workflow => TaskKind::Workflow,
                    DeploymentTaskQueueType::Activity => TaskKind::Activity,
                    DeploymentTaskQueueType::Nexus | DeploymentTaskQueueType::Unspecified => {
                        continue;
                    }
                };
                let queue = QueueKey {
                    namespace_id,
                    task_queue: TaskQueueName(membership.task_queue.name.clone()),
                    task_kind,
                    deployment: Some(DeploymentId(view.deployment_name.0.clone())),
                    build_id: Some(BuildId(view.build_id.0.clone())),
                };
                let mut stats = match task_kind {
                    TaskKind::Workflow => self.runtime.broker().backlog_stats(&queue).await,
                    TaskKind::Activity => {
                        self.runtime.activity_broker().backlog_stats(&queue).await
                    }
                };
                if matches!(
                    view.record.status,
                    WorkerDeploymentVersionStatus::Current | WorkerDeploymentVersionStatus::Ramping
                ) {
                    let unversioned = QueueKey {
                        deployment: None,
                        build_id: None,
                        ..queue.clone()
                    };
                    let absorbed = match task_kind {
                        TaskKind::Workflow => {
                            self.runtime.broker().backlog_stats(&unversioned).await
                        }
                        TaskKind::Activity => {
                            self.runtime
                                .activity_broker()
                                .backlog_stats(&unversioned)
                                .await
                        }
                    };
                    stats.count += absorbed.count;
                    stats.oldest_age = stats.oldest_age.max(absorbed.oldest_age);
                }
                membership.stats = Some(stats);
            }
        }
        Ok(view)
    }

    async fn delete_worker_deployment_version(&self, req: DeleteVersion) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .delete_version(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn set_worker_deployment_current_version(
        &self,
        req: SetCurrent,
    ) -> EdgeResult<DeploymentMutationOutcome<SetCurrentOutcome>> {
        let target_build_id = req.build_id.clone();
        let view = self
            .worker_deployment_registry()?
            .set_current_version(req)
            .await
            .map_err(registry_error_to_edge)?;
        if let Some(build_id) = target_build_id.as_ref() {
            self.promote_unversioned_backlog(&view.deployment, build_id)
                .await;
        }
        Ok(deployment_mutation_outcome(view.conflict_token, view))
    }

    async fn set_worker_deployment_ramping_version(
        &self,
        req: SetRamping,
    ) -> EdgeResult<DeploymentMutationOutcome<SetRampingOutcome>> {
        let promote_all = (req.ramping_percentage - 100.0).abs() < f32::EPSILON;
        let target_build_id = req.build_id.clone();
        let view = self
            .worker_deployment_registry()?
            .set_ramping_version(req)
            .await
            .map_err(registry_error_to_edge)?;
        if promote_all && let Some(build_id) = target_build_id.as_ref() {
            self.promote_unversioned_backlog(&view.deployment, build_id)
                .await;
        }
        Ok(deployment_mutation_outcome(view.conflict_token, view))
    }

    async fn update_worker_deployment_version_compute_config(
        &self,
        req: UpdateComputeConfig,
    ) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .update_compute_config(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn validate_worker_deployment_version_compute_config(
        &self,
        req: ValidateComputeConfig,
    ) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .validate_compute_config(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn update_worker_deployment_version_metadata(
        &self,
        req: UpdateMetadata,
    ) -> EdgeResult<VersionMetadataView> {
        self.worker_deployment_registry()?
            .update_version_metadata(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn set_worker_deployment_manager(
        &self,
        req: SetManager,
    ) -> EdgeResult<DeploymentMutationOutcome<SetManagerOutcome>> {
        let view = self
            .worker_deployment_registry()?
            .set_manager(req)
            .await
            .map_err(registry_error_to_edge)?;
        Ok(deployment_mutation_outcome(view.conflict_token, view))
    }

    async fn register_polled_deployment(&self, req: RegisterPolledDeployment) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .register_polled_deployment(req)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn apply_version_drainage(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        deployment_name: tokeira_storage::DeploymentName,
        build_id: tokeira_storage::BuildId,
        status: tokeira_storage::VersionDrainageStatus,
    ) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .apply_version_drainage(namespace_id, deployment_name, build_id, status)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn task_queue_versioning(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: String,
    ) -> EdgeResult<Option<TaskQueueVersioningView>> {
        self.worker_deployment_registry()?
            .task_queue_versioning(namespace_id, &task_queue)
            .await
            .map_err(registry_error_to_edge)
    }

    async fn validate_pinned_workflow_version(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: String,
        deployment_name: String,
        build_id: String,
    ) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .validate_pinned_workflow_version(
                namespace_id,
                &task_queue,
                &deployment_name,
                &build_id,
            )
            .await
            .map_err(registry_error_to_edge)
    }

    async fn reactivate_pinned_version(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        deployment_name: String,
        build_id: String,
    ) -> EdgeResult<()> {
        self.worker_deployment_registry()?
            .reactivate_pinned_version(namespace_id, &deployment_name, &build_id)
            .await
            .map_err(registry_error_to_edge)
    }
}

fn deployment_mutation_outcome<T>(
    conflict_token: ConflictToken,
    view: T,
) -> DeploymentMutationOutcome<T> {
    DeploymentMutationOutcome {
        conflict_token,
        view,
    }
}

fn registry_error_to_edge(error: RegistryError) -> EdgeError {
    match error {
        RegistryError::AlreadyExists => {
            EdgeError::AlreadyExists("worker deployment already exists".to_string())
        }
        RegistryError::AlreadyExistsAutoCreated => EdgeError::AlreadyExists(
            "worker deployment already exists (auto-created from worker polls)".to_string(),
        ),
        RegistryError::NotFound => EdgeError::NotFound("worker deployment not found".to_string()),
        RegistryError::NotFoundMessage(message) => EdgeError::NotFound(message),
        RegistryError::FailedPrecondition(message) => EdgeError::FailedPrecondition(message),
        RegistryError::ResourceExhausted(message) => EdgeError::ResourceExhausted(message),
        RegistryError::InvalidArgument(message) => EdgeError::BadRequest(message),
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
        CommitResult::CurrentExecutionConflict {
            existing_run_key, ..
        } => {
            // Wave 2 maps this to a typed already-started EdgeError + the
            // request-id detail; for now surface it as an error rather than
            // silently dropping it.
            Err(anyhow!(
                "current execution already exists: {existing_run_key:?}"
            ))
        }
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
