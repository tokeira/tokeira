//! Dispatch publisher that forwards kernel-produced operations to brokers and lanes.
//!
//! [`RuntimeDispatchPublisher`] is the concrete [`DispatchPublisher`] used by
//! the runtime. It translates [`DispatchOp`]s into broker publications,
//! child-workflow orchestration, external signal/cancel delivery, and Nexus
//! operation scheduling.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    CancelRequest, ChildStartConfirmedRequest, ChildStartResult, Command, DispatchOp,
    ExternalCancelResolvedRequest, ExternalCancelResult, ExternalSignalResolvedRequest,
    ExternalSignalResult, ExternalWorkflowExecution, SignalRequest, StartRequest,
    TerminateRequest,
};
use tokeira_storage::{
    CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, RunRepository,
};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskQueueName, WorkflowId,
};

use crate::{
    activity_timeout::ActivityTrackingState,
    broker::{InMemoryActivityBroker, InMemoryBroker},
    fairness::DeliveryMetrics,
    lane::{DispatchPublisher, LaneHandle},
    nexus::{
        NexusEndpointRegistry, NexusHttpClient, NexusStartResult, NexusTimeoutEntry,
        NexusTimeoutTrackingState,
    },
    scanner::pick_lane,
    shard::shard_for,
};

/// [`DispatchPublisher`] that forwards dispatch ops to
/// the runtime's in-memory brokers.
pub struct RuntimeDispatchPublisher<R> {
    broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    repo: Arc<R>,
    lanes: Arc<Mutex<Vec<LaneHandle>>>,
    lane_count: usize,
    shard_count: u32,
    nexus_client: Arc<dyn NexusHttpClient>,
    nexus_registry: NexusEndpointRegistry,
    nexus_timeout_tracking: NexusTimeoutTrackingState,
    activity_tracking: ActivityTrackingState,
    delivery_metrics: DeliveryMetrics,
}

impl<R> Clone for RuntimeDispatchPublisher<R> {
    fn clone(&self) -> Self {
        Self {
            broker: self.broker.clone(),
            activity_broker: self.activity_broker.clone(),
            repo: self.repo.clone(),
            lanes: self.lanes.clone(),
            lane_count: self.lane_count,
            shard_count: self.shard_count,
            nexus_client: self.nexus_client.clone(),
            nexus_registry: self.nexus_registry.clone(),
            nexus_timeout_tracking: self.nexus_timeout_tracking.clone(),
            activity_tracking: self.activity_tracking.clone(),
            delivery_metrics: self.delivery_metrics.clone(),
        }
    }
}

impl<R> RuntimeDispatchPublisher<R>
where
    R: RunRepository + 'static,
{
    /// Create a publisher wired to the given brokers.
    pub fn new(
        broker: InMemoryBroker,
        activity_broker: InMemoryActivityBroker,
        repo: Arc<R>,
        lanes: Arc<Mutex<Vec<LaneHandle>>>,
        lane_count: usize,
        shard_count: u32,
        nexus_client: Arc<dyn NexusHttpClient>,
        nexus_registry: NexusEndpointRegistry,
        nexus_timeout_tracking: NexusTimeoutTrackingState,
        activity_tracking: ActivityTrackingState,
        delivery_metrics: DeliveryMetrics,
    ) -> Self {
        Self {
            broker,
            activity_broker,
            repo,
            lanes,
            lane_count,
            shard_count,
            nexus_client,
            nexus_registry,
            nexus_timeout_tracking,
            activity_tracking,
            delivery_metrics,
        }
    }

    fn pick_lane(&self, run_key: RunKey) -> LaneHandle {
        let lanes = self.lanes.lock().unwrap();
        pick_lane(&lanes, self.lane_count, run_key).clone()
    }

    async fn resolve_child_run_key(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: &WorkflowId,
        child_run_id: RunId,
    ) -> Result<Option<RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: child_workflow_id.clone(),
                run_id: Some(child_run_id),
            })
            .await
    }

    async fn handle_start_child_workflow(
        &self,
        child_workflow_id: WorkflowId,
        namespace_id: NamespaceId,
        workflow_type: tokeira_types::WorkflowType,
        task_queue: TaskQueueName,
        input: Payloads,
        parent_run_key: RunKey,
        parent_workflow_id: WorkflowId,
        initiated_event_id: i64,
    ) {
        let child_run_key = RunKey::new();
        let child_run_id = RunId::new();
        let start_request = StartRequest {
            run_key: child_run_key,
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: child_run_id,
            workflow_type: workflow_type.clone(),
            task_queue,
            deployment: None,
            build_id: None,
            input,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: Some(parent_run_key),
            parent_workflow_id: Some(parent_workflow_id),
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId(format!("child-start-{child_run_key:?}")),
                caller_identity: Some("runtime-child-orchestrator".to_string()),
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        };

        let result = self
            .pick_lane(child_run_key)
            .submit(child_run_key, Command::Start(start_request))
            .await;
        let confirmation = match result {
            Ok(CommitResult::Applied { .. }) => ChildStartResult::Started {
                child_run_id,
                workflow_type,
            },
            Ok(CommitResult::Conflict { reason }) => {
                ChildStartResult::Failed { cause: reason }
            }
            Ok(CommitResult::Duplicate) => ChildStartResult::Failed {
                cause: "duplicate start request".to_string(),
            },
            Err(error) => ChildStartResult::Failed {
                cause: error.to_string(),
            },
        };

        let confirm = Command::ChildStartConfirmed(ChildStartConfirmedRequest {
            child_workflow_id: child_workflow_id.clone(),
            initiated_event_id,
            result: confirmation,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(parent_run_key)
            .submit(parent_run_key, confirm)
            .await
        {
            tracing::warn!(
                ?error,
                parent_run_key = ?parent_run_key,
                child_workflow_id = ?child_workflow_id,
                "failed to deliver child start confirmation"
            );
        }
    }

    async fn handle_terminate_child(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    ) {
        match self
            .resolve_child_run_key(namespace_id, &child_workflow_id, child_run_id)
            .await
        {
            Ok(Some(child_run_key)) => {
                let command = Command::Terminate(TerminateRequest {
                    reason,
                    details: None,
                    identity: "parent-close-policy".to_string(),
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "terminate-child-{child_run_id:?}"
                        )),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                if let Err(error) = self
                    .pick_lane(child_run_key)
                    .submit(child_run_key, command)
                    .await
                {
                    let message = error.to_string();
                    if message.contains("kernel rejected")
                        || message.contains("not found")
                    {
                        tracing::debug!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "terminate child no-op"
                        );
                    } else {
                        tracing::warn!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "terminate child dispatch failed"
                        );
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "terminate child skipped because child was not found"
                );
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "terminate child resolution failed"
                );
            }
        }
    }

    async fn handle_cancel_child(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: WorkflowId,
        child_run_id: RunId,
        reason: String,
    ) {
        match self
            .resolve_child_run_key(namespace_id, &child_workflow_id, child_run_id)
            .await
        {
            Ok(Some(child_run_key)) => {
                let command = Command::Cancel(CancelRequest {
                    reason,
                    external_initiator: None,
                    request: RequestContext {
                        request_id: RequestId(format!("cancel-child-{child_run_id:?}")),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                if let Err(error) = self
                    .pick_lane(child_run_key)
                    .submit(child_run_key, command)
                    .await
                {
                    let message = error.to_string();
                    if message.contains("kernel rejected")
                        || message.contains("not found")
                    {
                        tracing::debug!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "cancel child no-op"
                        );
                    } else {
                        tracing::warn!(
                            ?error,
                            child_run_key = ?child_run_key,
                            child_workflow_id = ?child_workflow_id,
                            "cancel child dispatch failed"
                        );
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "cancel child skipped because child was not found"
                );
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    child_workflow_id = ?child_workflow_id,
                    child_run_id = ?child_run_id,
                    "cancel child resolution failed"
                );
            }
        }
    }

    async fn handle_signal_external_workflow(
        &self,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        signal_name: String,
        input: Payloads,
        originator_run_key: RunKey,
        namespace_id: NamespaceId,
        initiated_event_id: i64,
    ) {
        let signal_result = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: target_workflow_id.clone(),
                run_id: target_run_id,
            })
            .await
        {
            Ok(Some(target_run_key)) => {
                let command = Command::Signal(SignalRequest {
                    signal_name,
                    input,
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-signal-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some(
                            "runtime-external-signal-orchestrator".to_string(),
                        ),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                match self
                    .pick_lane(target_run_key)
                    .submit(target_run_key, command)
                    .await
                {
                    Ok(CommitResult::Applied { .. }) | Ok(CommitResult::Duplicate) => {
                        ExternalSignalResult::Signaled
                    }
                    Ok(CommitResult::Conflict { reason }) => {
                        ExternalSignalResult::Failed { cause: reason }
                    }
                    Err(error) => ExternalSignalResult::Failed {
                        cause: error.to_string(),
                    },
                }
            }
            Ok(None) => ExternalSignalResult::Failed {
                cause: format!("target workflow not found: {}", target_workflow_id.0),
            },
            Err(error) => ExternalSignalResult::Failed {
                cause: error.to_string(),
            },
        };

        let resolve = Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
            initiated_event_id,
            result: signal_result,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(originator_run_key)
            .submit(originator_run_key, resolve)
            .await
        {
            tracing::warn!(
                ?error,
                originator_run_key = ?originator_run_key,
                initiated_event_id,
                "failed to deliver ExternalSignalResolved to originator"
            );
        }
    }

    async fn handle_cancel_external_workflow(
        &self,
        target_workflow_id: WorkflowId,
        target_run_id: Option<RunId>,
        originator_run_key: RunKey,
        originator_namespace_id: NamespaceId,
        originator_workflow_id: WorkflowId,
        originator_run_id: RunId,
        namespace_id: NamespaceId,
        initiated_event_id: i64,
        reason: String,
    ) {
        let cancel_result = match self
            .repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: target_workflow_id.clone(),
                run_id: target_run_id,
            })
            .await
        {
            Ok(Some(target_run_key)) => {
                let command = Command::Cancel(CancelRequest {
                    reason,
                    external_initiator: Some(ExternalWorkflowExecution {
                        namespace_id: originator_namespace_id,
                        workflow_id: originator_workflow_id,
                        run_id: originator_run_id,
                    }),
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-cancel-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some(
                            "runtime-external-cancel-orchestrator".to_string(),
                        ),
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                });
                match self
                    .pick_lane(target_run_key)
                    .submit(target_run_key, command)
                    .await
                {
                    Ok(CommitResult::Applied { .. }) | Ok(CommitResult::Duplicate) => {
                        ExternalCancelResult::CancelRequested
                    }
                    Ok(CommitResult::Conflict { reason }) => {
                        ExternalCancelResult::Failed { cause: reason }
                    }
                    Err(error) => ExternalCancelResult::Failed {
                        cause: error.to_string(),
                    },
                }
            }
            Ok(None) => ExternalCancelResult::Failed {
                cause: format!("target workflow not found: {}", target_workflow_id.0),
            },
            Err(error) => ExternalCancelResult::Failed {
                cause: error.to_string(),
            },
        };

        let resolve = Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
            initiated_event_id,
            result: cancel_result,
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self
            .pick_lane(originator_run_key)
            .submit(originator_run_key, resolve)
            .await
        {
            tracing::warn!(
                ?error,
                originator_run_key = ?originator_run_key,
                initiated_event_id,
                "failed to deliver ExternalCancelResolved to originator"
            );
        }
    }

    async fn handle_schedule_nexus_operation(
        &self,
        operation_id: String,
        endpoint_name: String,
        service: String,
        operation: String,
        input: Payloads,
        schedule_to_close_timeout: Option<Duration>,
        originator_run_key: RunKey,
        scheduled_event_id: i64,
    ) {
        let resolution = match self.nexus_registry.resolve(&endpoint_name) {
            Some(config) => {
                match self
                    .nexus_client
                    .start_operation(
                        &config.address,
                        &operation_id,
                        &service,
                        &operation,
                        &input,
                        schedule_to_close_timeout,
                    )
                    .await
                {
                    Ok(NexusStartResult::SyncCompleted { result }) => {
                        tokeira_kernel::NexusResolution::Completed { result }
                    }
                    Ok(NexusStartResult::SyncFailed { message }) => {
                        tokeira_kernel::NexusResolution::Failed { failure: message }
                    }
                    Ok(NexusStartResult::AsyncAccepted) => {
                        tokeira_kernel::NexusResolution::Started
                    }
                    Err(error) => tokeira_kernel::NexusResolution::Failed {
                        failure: error.to_string(),
                    },
                }
            }
            None => tokeira_kernel::NexusResolution::Failed {
                failure: format!("nexus endpoint not found: {endpoint_name}"),
            },
        };

        let command = Command::NexusOperationResolved(
            tokeira_kernel::NexusOperationResolvedRequest {
                operation_id,
                scheduled_event_id,
                resolution,
                now: OffsetDateTime::now_utc(),
            },
        );
        if let Err(error) = self
            .pick_lane(originator_run_key)
            .submit(originator_run_key, command)
            .await
        {
            tracing::warn!(
                ?error,
                originator_run_key = ?originator_run_key,
                scheduled_event_id,
                "failed to deliver NexusOperationResolved to originator"
            );
        }
    }

    async fn handle_cancel_nexus_operation(
        &self,
        originator_run_key: RunKey,
        operation_id: String,
        endpoint_name: String,
        service: String,
        scheduled_event_id: i64,
    ) {
        let Some(config) = self.nexus_registry.resolve(&endpoint_name) else {
            tracing::warn!(
                endpoint = endpoint_name,
                operation_id,
                "cancel nexus operation skipped: endpoint not found"
            );
            return;
        };

        match self
            .nexus_client
            .cancel_operation(&config.address, &operation_id, &service)
            .await
        {
            Ok(()) => {
                let command = Command::NexusOperationResolved(
                    tokeira_kernel::NexusOperationResolvedRequest {
                        operation_id,
                        scheduled_event_id,
                        resolution: tokeira_kernel::NexusResolution::Canceled,
                        now: OffsetDateTime::now_utc(),
                    },
                );
                if let Err(error) = self
                    .pick_lane(originator_run_key)
                    .submit(originator_run_key, command)
                    .await
                {
                    tracing::warn!(
                        ?error,
                        originator_run_key = ?originator_run_key,
                        scheduled_event_id,
                        "failed to deliver NexusOperationResolved(Canceled) to originator"
                    );
                }
            }
            Err(error) => {
                tracing::debug!(
                    ?error,
                    operation_id,
                    endpoint = endpoint_name,
                    "cancel nexus operation failed (treating as no-op)"
                );
            }
        }
    }
}

#[async_trait]
impl<R> DispatchPublisher for RuntimeDispatchPublisher<R>
where
    R: RunRepository + 'static,
{
    async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()> {
        for op in ops {
            match op {
                DispatchOp::EnqueueWorkflowTask {
                    queue,
                    logical_seq,
                    sticky_preferred,
                } => {
                    self.broker
                        .publish_workflow_task(DispatchableWorkflowTask {
                            run_key,
                            queue: queue.clone(),
                            logical_seq: *logical_seq,
                            sticky_preferred: sticky_preferred.clone(),
                            sticky_expires_at: None,
                        }, Some(&self.delivery_metrics))
                        .await;
                }
                DispatchOp::EnqueueActivityTask { .. } => {
                    if let DispatchOp::EnqueueActivityTask {
                        queue,
                        activity_id,
                        input,
                        schedule_event_id,
                        attempt,
                        ..
                    } = op
                    {
                        if let Err(error) = self
                            .activity_broker
                            .publish_activity_task(DispatchableActivityTask {
                                run_key,
                                queue: queue.clone(),
                                activity_id: activity_id.clone(),
                                input: input.clone(),
                                schedule_event_id: *schedule_event_id,
                                attempt: *attempt,
                            }, Some(&self.delivery_metrics))
                            .await
                        {
                            tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish activity task");
                        } else {
                            self.activity_tracking.record_scheduled(
                                run_key,
                                shard_for(run_key, self.shard_count),
                                activity_id.clone(),
                                OffsetDateTime::now_utc(),
                            );
                        }
                    }
                }
                DispatchOp::StartChildWorkflow {
                    child_workflow_id,
                    namespace_id,
                    workflow_type,
                    task_queue,
                    input,
                    parent_run_key,
                    parent_workflow_id,
                    initiated_event_id,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let child_workflow_id = child_workflow_id.clone();
                    let workflow_type = workflow_type.clone();
                    let task_queue = task_queue.clone();
                    let input = input.clone();
                    let parent_workflow_id = parent_workflow_id.clone();
                    let namespace_id = *namespace_id;
                    let parent_run_key = *parent_run_key;
                    let initiated_event_id = *initiated_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_start_child_workflow(
                                child_workflow_id,
                                namespace_id,
                                workflow_type,
                                task_queue,
                                input,
                                parent_run_key,
                                parent_workflow_id,
                                initiated_event_id,
                            )
                            .await;
                    });
                }
                DispatchOp::TerminateChild {
                    namespace_id,
                    child_workflow_id,
                    child_run_id,
                    reason,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let namespace_id = *namespace_id;
                    let child_workflow_id = child_workflow_id.clone();
                    let child_run_id = *child_run_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_terminate_child(
                                namespace_id,
                                child_workflow_id,
                                child_run_id,
                                reason,
                            )
                            .await;
                    });
                }
                DispatchOp::CancelChild {
                    namespace_id,
                    child_workflow_id,
                    child_run_id,
                    reason,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let namespace_id = *namespace_id;
                    let child_workflow_id = child_workflow_id.clone();
                    let child_run_id = *child_run_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_cancel_child(
                                namespace_id,
                                child_workflow_id,
                                child_run_id,
                                reason,
                            )
                            .await;
                    });
                }
                DispatchOp::SignalExternalWorkflow {
                    originator_run_key,
                    namespace_id,
                    initiated_event_id,
                    target_workflow_id,
                    target_run_id,
                    signal_name,
                    input,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let target_workflow_id = target_workflow_id.clone();
                    let target_run_id = *target_run_id;
                    let signal_name = signal_name.clone();
                    let input = input.clone();
                    let originator_run_key = *originator_run_key;
                    let namespace_id = *namespace_id;
                    let initiated_event_id = *initiated_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_signal_external_workflow(
                                target_workflow_id,
                                target_run_id,
                                signal_name,
                                input,
                                originator_run_key,
                                namespace_id,
                                initiated_event_id,
                            )
                            .await;
                    });
                }
                DispatchOp::RequestCancelExternalWorkflow {
                    originator_run_key,
                    originator_namespace_id,
                    originator_workflow_id,
                    originator_run_id,
                    namespace_id,
                    initiated_event_id,
                    reason,
                    target_workflow_id,
                    target_run_id,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let target_workflow_id = target_workflow_id.clone();
                    let target_run_id = *target_run_id;
                    let originator_run_key = *originator_run_key;
                    let originator_namespace_id = *originator_namespace_id;
                    let originator_workflow_id = originator_workflow_id.clone();
                    let originator_run_id = *originator_run_id;
                    let namespace_id = *namespace_id;
                    let initiated_event_id = *initiated_event_id;
                    let reason = reason.clone();
                    tokio::spawn(async move {
                        publisher
                            .handle_cancel_external_workflow(
                                target_workflow_id,
                                target_run_id,
                                originator_run_key,
                                originator_namespace_id,
                                originator_workflow_id,
                                originator_run_id,
                                namespace_id,
                                initiated_event_id,
                                reason,
                            )
                            .await;
                    });
                }
                DispatchOp::ScheduleNexusOperation {
                    operation_id,
                    endpoint,
                    service,
                    operation,
                    input,
                    schedule_to_close_timeout,
                    originator_run_key,
                    scheduled_event_id,
                    scheduled_at,
                } => {
                    if let Some(timeout) = schedule_to_close_timeout {
                        self.nexus_timeout_tracking.insert(NexusTimeoutEntry {
                            run_key: *originator_run_key,
                            shard_id: shard_for(
                                *originator_run_key,
                                self.shard_count,
                            ),
                            operation_id: operation_id.clone(),
                            scheduled_event_id: *scheduled_event_id,
                            schedule_to_close_timeout: *timeout,
                            scheduled_at: *scheduled_at,
                        });
                    }
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let operation_id = operation_id.clone();
                    let endpoint = endpoint.clone();
                    let service = service.clone();
                    let operation = operation.clone();
                    let input = input.clone();
                    let schedule_to_close_timeout = *schedule_to_close_timeout;
                    let originator_run_key = *originator_run_key;
                    let scheduled_event_id = *scheduled_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_schedule_nexus_operation(
                                operation_id,
                                endpoint,
                                service,
                                operation,
                                input,
                                schedule_to_close_timeout,
                                originator_run_key,
                                scheduled_event_id,
                            )
                            .await;
                    });
                }
                DispatchOp::CancelNexusOperation {
                    scheduled_event_id,
                    originator_run_key,
                    operation_id,
                    endpoint,
                    service,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let originator_run_key = *originator_run_key;
                    let operation_id = operation_id.clone();
                    let endpoint = endpoint.clone();
                    let service = service.clone();
                    let scheduled_event_id = *scheduled_event_id;
                    tokio::spawn(async move {
                        publisher
                            .handle_cancel_nexus_operation(
                                originator_run_key,
                                operation_id,
                                endpoint,
                                service,
                                scheduled_event_id,
                            )
                            .await;
                    });
                }
            }
        }
        Ok(())
    }

    async fn submit_to_run(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        self.pick_lane(run_key).submit(run_key, command).await
    }
}
