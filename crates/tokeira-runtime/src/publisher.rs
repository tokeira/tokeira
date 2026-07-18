//! Dispatch publisher that forwards kernel-produced operations to brokers and lanes.
//!
//! [`RuntimeDispatchPublisher`] is the concrete [`DispatchPublisher`] used by
//! the runtime. It translates [`DispatchOp`]s into broker publications,
//! child-workflow orchestration, external signal/cancel delivery, and Nexus
//! operation scheduling.

use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use opentelemetry::{
    KeyValue,
    propagation::{Injector, TextMapPropagator},
};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    CallbackAttemptOutcome, CallbackCompletionOutcome, CallbackSpec, CallbackState, CancelRequest,
    ChildStartConfirmedRequest, ChildStartResult, Command, CompletionCallback,
    CompletionCallbackAttemptedRequest, DispatchOp, ExternalCancelResolvedRequest,
    ExternalCancelResult, ExternalSignalResolvedRequest, ExternalSignalResult,
    ExternalWorkflowExecution, LoadedRun, SignalRequest, StartRequest, TerminateRequest,
    callback_completion_outcome,
};
use tokeira_proto::{
    conversions::common::{failure_to_payload, payloads_from_domain},
    public::temporal::api::failure::v1 as failure_proto,
};
use tokeira_storage::{
    CommitResult, DispatchableActivityTask, DispatchableWorkflowTask, RunRepository,
};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, RunId,
    RunKey, SearchAttributes, ShardId, TaskQueueName, WorkflowId,
};
use tokio_util::sync::CancellationToken;

use crate::{
    activity_timeout::ActivityTrackingState,
    broker::{InMemoryActivityBroker, InMemoryBroker},
    fairness::DeliveryMetrics,
    lane::{DispatchPublisher, LaneHandle},
    metrics as runtime_metrics,
    nexus::{
        CompletionCallbackScannerConfig, CompletionCallbackTrackingEntry,
        CompletionCallbackTrackingState, CompletionDeliveryOutcome, EndpointTarget,
        NexusCompletion, NexusCompletionClient, NexusCompletionFailureBody,
        NexusCompletionRuntimeConfig, NexusEndpointRegistry, NexusHttpClient, NexusHttpFailureBody,
        NexusNamespaceResolver, NexusStartResult, NexusTaskBroker, NexusTaskRequest,
        NexusTimeoutEntry, NexusTimeoutTrackingState, SYSTEM_CALLBACK_URL,
        TEMPORAL_CALLBACK_TOKEN_HEADER, nexus_completion_backoff,
    },
    scanner::pick_lane_for_run_key,
    shard::{ShardOwner, shard_for},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Owned child-start parameters carried from a `DispatchOp::StartChildWorkflow`
/// into the spawned orchestration task — the parent linkage plus the child
/// configuration threaded onto the child's own `StartRequest`.
struct StartChildDispatch {
    child_workflow_id: WorkflowId,
    namespace_id: NamespaceId,
    workflow_type: tokeira_types::WorkflowType,
    task_queue: TaskQueueName,
    input: Payloads,
    parent_run_key: RunKey,
    parent_workflow_id: WorkflowId,
    parent_run_id: RunId,
    parent_namespace_id: NamespaceId,
    parent_root_workflow_id: Option<WorkflowId>,
    parent_root_run_id: Option<RunId>,
    initiated_event_id: i64,
    header: Option<tokeira_types::Headers>,
    memo: Memo,
    search_attributes: SearchAttributes,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
    workflow_task_timeout: Duration,
    retry_policy: Option<tokeira_types::RetryPolicy>,
    cron_schedule: Option<String>,
    reuse_policy: tokeira_kernel::WorkflowIdReusePolicy,
}

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
    nexus_completion_client: Arc<dyn NexusCompletionClient>,
    nexus_completion_config: NexusCompletionRuntimeConfig,
    nexus_registry: NexusEndpointRegistry,
    nexus_broker: NexusTaskBroker,
    nexus_timeout_tracking: NexusTimeoutTrackingState,
    completion_callback_tracking: CompletionCallbackTrackingState,
    activity_tracking: ActivityTrackingState,
    delivery_metrics: DeliveryMetrics,
    /// Resolves the originator namespace name for the External-endpoint outbound metric.
    /// `None` outside the full server (unit harnesses), where the metric is then omitted.
    namespace_resolver: Option<Arc<dyn NexusNamespaceResolver>>,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl<R> std::fmt::Debug for RuntimeDispatchPublisher<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeDispatchPublisher")
            .field("lane_count", &self.lane_count)
            .field("shard_count", &self.shard_count)
            .finish_non_exhaustive()
    }
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
            nexus_completion_client: self.nexus_completion_client.clone(),
            nexus_completion_config: self.nexus_completion_config.clone(),
            nexus_registry: self.nexus_registry.clone(),
            nexus_broker: self.nexus_broker.clone(),
            nexus_timeout_tracking: self.nexus_timeout_tracking.clone(),
            completion_callback_tracking: self.completion_callback_tracking.clone(),
            activity_tracking: self.activity_tracking.clone(),
            delivery_metrics: self.delivery_metrics.clone(),
            namespace_resolver: self.namespace_resolver.clone(),
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
        nexus_completion_client: Arc<dyn NexusCompletionClient>,
        nexus_completion_config: NexusCompletionRuntimeConfig,
        nexus_registry: NexusEndpointRegistry,
        nexus_broker: NexusTaskBroker,
        nexus_timeout_tracking: NexusTimeoutTrackingState,
        completion_callback_tracking: CompletionCallbackTrackingState,
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
            nexus_completion_client,
            nexus_completion_config,
            nexus_registry,
            nexus_broker,
            nexus_timeout_tracking,
            completion_callback_tracking,
            activity_tracking,
            delivery_metrics,
            namespace_resolver: None,
        }
    }

    /// Attach the namespace-name resolver used to tag the External-endpoint outbound metric.
    /// Set by the server bootstrap; left unset (and the metric omitted) in unit harnesses.
    pub fn with_namespace_resolver(
        mut self,
        resolver: Option<Arc<dyn NexusNamespaceResolver>>,
    ) -> Self {
        self.namespace_resolver = resolver;
        self
    }

    fn pick_lane(&self, run_key: RunKey) -> LaneHandle {
        let lanes = self.lanes.lock().unwrap();
        pick_lane_for_run_key(&lanes, self.lane_count, run_key).clone()
    }

    fn nexus_trace_headers(&self) -> Vec<KeyValue> {
        struct KeyValueInjector {
            values: Vec<KeyValue>,
        }

        impl Injector for KeyValueInjector {
            fn set(&mut self, key: &str, value: String) {
                self.values.push(KeyValue::new(key.to_string(), value));
            }
        }

        let mut injector = KeyValueInjector { values: Vec::new() };
        opentelemetry_sdk::propagation::TraceContextPropagator::new()
            .inject_context(&tracing::Span::current().context(), &mut injector);
        injector.values
    }

    /// Record the External-endpoint outbound StartOperation metric
    /// (`nexus_outbound_requests` + `_latency`), resolving the originator namespace *name*
    /// via the injected resolver (the publisher holds only the [`RunKey`]/[`NamespaceId`]).
    /// A no-op when no resolver is wired (unit harnesses) or the namespace cannot be
    /// resolved — the metric is observability and never blocks the operation. Runs in the
    /// schedule dispatch's spawned task, off the commit path.
    async fn record_external_outbound_metric(
        &self,
        originator_run_key: RunKey,
        outcome: &str,
        latency: std::time::Duration,
    ) {
        let Some(resolver) = self.namespace_resolver.as_ref() else {
            return;
        };
        let namespace_id = match self.repo.load_run(originator_run_key).await {
            Ok(LoadedRun::Existing(state)) => state.namespace_id,
            // Absent run or load error: skip rather than tag with an unresolved namespace.
            _ => return,
        };
        let Some(namespace) = resolver.name_for_id(namespace_id).await else {
            return;
        };
        // External endpoints are not worker-via-frontend, so the failure source is unset.
        runtime_metrics::record_nexus_outbound_request(
            &namespace,
            "StartOperation",
            "_unknown_",
            outcome,
        );
        runtime_metrics::record_nexus_outbound_latency(&namespace, "StartOperation", latency);
    }

    /// Resolve a namespace id to its registered NAME for metric labelling (M.1)
    /// — the fork's metric bridge filters the speculative-WFT counters on the
    /// namespace name. Returns `None` when no resolver is wired (unit tests),
    /// which drops the metric rather than mislabelling it.
    async fn resolve_namespace_name(&self, namespace_id: NamespaceId) -> Option<String> {
        self.namespace_resolver
            .as_ref()?
            .name_for_id(namespace_id)
            .await
    }

    /// Whether a live workflow poller is currently parked on `queue`. Used to
    /// decide the speculative-task sticky→normal fallback (R.1): a sticky queue
    /// with no waiter means the sticky worker is gone, so the task is published
    /// on the normal queue instead of waiting out the schedule-to-start timeout.
    async fn has_workflow_poller(&self, queue: &QueueKey) -> bool {
        self.broker
            .workflow_waiter_counts()
            .await
            .get(queue)
            .copied()
            .unwrap_or(0)
            > 0
    }

    /// Resolve the child's CURRENT execution for a parent-close-policy op
    /// (terminate / request-cancel). The `first_run_id` recorded at initiation
    /// is the lineage anchor, NOT the target run: a child that continued-as-new
    /// must have its LATEST generation terminated/cancelled, so resolution keys
    /// on the workflow id's current execution rather than the started run
    /// (v1.31.0 targets `{WorkflowId}` with `FirstExecutionRunId` as an
    /// across-runs guard, transfer_queue_active_task_executor.go:1878-1883;
    /// TestChildWorkflowWithContinueAsNewParentTerminate).
    async fn resolve_child_run_key(
        &self,
        namespace_id: NamespaceId,
        child_workflow_id: &WorkflowId,
        _first_run_id: RunId,
    ) -> Result<Option<RunKey>> {
        self.repo
            .resolve_execution(&ExecutionRef {
                namespace_id,
                workflow_id: child_workflow_id.clone(),
                run_id: None,
            })
            .await
    }

    async fn handle_start_child_workflow(&self, spec: StartChildDispatch) {
        let StartChildDispatch {
            child_workflow_id,
            namespace_id,
            workflow_type,
            task_queue,
            input,
            parent_run_key,
            parent_workflow_id,
            parent_run_id,
            parent_namespace_id,
            parent_root_workflow_id,
            parent_root_run_id,
            initiated_event_id,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            reuse_policy,
        } = spec;
        let child_run_id = RunId::new();
        let child_run_key = RunKey::derive(namespace_id, &child_workflow_id, child_run_id);
        let task_queue_name = task_queue.0.clone();
        // A TERMINATE_IF_RUNNING reuse policy migrates to a TERMINATE_EXISTING
        // conflict on the child start: a running same-id child is terminated
        // and a fresh run started, rather than failing the start
        // (workflow_id_dedup.go:251-262 @ v1.31.0;
        // TestChildWorkflowExecution_AlreadyRunning_TerminateIfRunningStartsNewChild).
        let (conflict_policy, effective_reuse_policy) = match reuse_policy {
            tokeira_kernel::WorkflowIdReusePolicy::TerminateIfRunning => (
                tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting,
                tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            ),
            other => (tokeira_kernel::WorkflowIdConflictPolicy::Fail, other),
        };
        // The child's root execution is the top-level ancestor: inherit the
        // parent's root, or — when the parent is itself top-level (no root) —
        // the parent execution IS the root (event_factory.go:78 +
        // mutable_state_impl.go:2855-2856 @ v1.31.0).
        let root_workflow_id = parent_root_workflow_id.or_else(|| Some(parent_workflow_id.clone()));
        let root_run_id = parent_root_run_id.or(Some(parent_run_id));
        // The child's WorkflowExecutionStarted carries the PARENT namespace by
        // NAME; resolve it from the parent namespace id.
        let parent_namespace_name = self.resolve_namespace_name(parent_namespace_id).await;
        let start_request = StartRequest {
            run_key: child_run_key,
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: child_run_id,
            workflow_type: workflow_type.clone(),
            task_queue,
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            conflict_policy,
            reuse_policy: effective_reuse_policy,
            // A child start is a fresh execution, not a CaN/retry/cron successor.
            initiator: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: Some(parent_run_key),
            parent_workflow_id: Some(parent_workflow_id),
            parent_run_id: Some(parent_run_id),
            parent_namespace_id: Some(parent_namespace_id),
            parent_namespace_name,
            parent_initiated_event_id: initiated_event_id,
            root_workflow_id,
            root_run_id,
            original_execution_run_id: None,
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId(format!("child-start-{child_run_key:?}")),
                caller_identity: Some("runtime-child-orchestrator".to_string()),
                principal: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            client_cron_schedule: None,
            cron_schedule,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
        };

        // A running same-id conflict is resolved by the child's conflict policy,
        // mirroring v1.31.0 ResolveWorkflowIDConflictPolicy
        // (workflow_id_dedup.go:83-107): FAIL — the default child policy —
        // records StartChildWorkflowExecutionFailed(WORKFLOW_ALREADY_EXISTS),
        // while TERMINATE_EXISTING — migrated from the deprecated
        // TERMINATE_IF_RUNNING reuse policy — terminates the running incumbent
        // and starts a fresh child run (terminateWorkflowAction, :202-231). The
        // raw start reports CurrentExecutionConflict only while the incumbent is
        // OPEN (memory.rs), so terminating it frees the current-execution slot
        // for the retry.
        let terminate_on_conflict = matches!(
            conflict_policy,
            tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting
        );
        let mut terminated_incumbent = false;
        let confirmation = loop {
            let result = self
                .pick_lane(child_run_key)
                .submit(child_run_key, Command::Start(start_request.clone()))
                .await;
            match result {
                Ok(CommitResult::Applied { .. }) => {
                    tracing::debug!(
                        ?child_workflow_id,
                        ?child_run_key,
                        ?child_run_id,
                        task_queue = %task_queue_name,
                        "child workflow started successfully"
                    );
                    break ChildStartResult::Started {
                        child_run_id,
                        workflow_type: workflow_type.clone(),
                    };
                }
                Ok(CommitResult::CurrentExecutionConflict {
                    existing_run_key, ..
                }) => {
                    if terminate_on_conflict && !terminated_incumbent {
                        terminated_incumbent = true;
                        self.terminate_conflicting_incumbent(existing_run_key, child_run_id)
                            .await;
                        continue;
                    }
                    tracing::warn!(
                        ?child_workflow_id,
                        ?existing_run_key,
                        "child workflow start hit a running duplicate workflow id"
                    );
                    break ChildStartResult::Failed {
                        cause: "WORKFLOW_ALREADY_EXISTS".to_string(),
                    };
                }
                Ok(CommitResult::Conflict { reason }) => {
                    // A closed-run reuse-policy rejection (RejectDuplicate /
                    // AllowDuplicateFailedOnly) also surfaces to the parent as
                    // WORKFLOW_ALREADY_EXISTS (generateWorkflowAlreadyStartedError,
                    // workflow_id_dedup.go:233-249).
                    tracing::warn!(?child_workflow_id, %reason, "child workflow start conflict");
                    break ChildStartResult::Failed {
                        cause: "WORKFLOW_ALREADY_EXISTS".to_string(),
                    };
                }
                Ok(CommitResult::Duplicate) => {
                    break ChildStartResult::Failed {
                        cause: "WORKFLOW_ALREADY_EXISTS".to_string(),
                    };
                }
                Err(error) => {
                    tracing::warn!(?child_workflow_id, ?error, "child workflow start error");
                    break ChildStartResult::Failed {
                        cause: error.to_string(),
                    };
                }
            }
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

    /// Terminate the running incumbent a TERMINATE_IF_RUNNING child start is
    /// displacing, so the retried start claims the current-execution slot.
    /// Mirrors v1.31.0 terminateWorkflowAction (workflow_id_dedup.go:202-231):
    /// the terminate runs under the history-service identity and does NOT
    /// consume the start's request id — a distinct id keeps the terminate
    /// idempotent while the start's id stays free to author the new run's
    /// WorkflowExecutionStarted event.
    async fn terminate_conflicting_incumbent(&self, run_key: RunKey, new_run_id: RunId) {
        let command = Command::Terminate(TerminateRequest {
            reason: "TerminateIfRunning WorkflowIdReusePolicy".to_string(),
            details: None,
            identity: "history-service".to_string(),
            links: Vec::new(),
            request: RequestContext {
                request_id: RequestId(format!("conflict-terminate:{run_key:?}:{new_run_id:?}")),
                caller_identity: Some("history-service".to_string()),
                principal: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        });
        if let Err(error) = self.pick_lane(run_key).submit(run_key, command).await {
            let message = error.to_string();
            if message.contains("kernel rejected") || message.contains("not found") {
                tracing::debug!(
                    ?error,
                    ?run_key,
                    "conflicting child incumbent already closed"
                );
            } else {
                tracing::warn!(
                    ?error,
                    ?run_key,
                    "failed to terminate conflicting child incumbent"
                );
            }
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
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId(format!("terminate-child-{child_run_id:?}")),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        principal: None,
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
                    if message.contains("kernel rejected") || message.contains("not found") {
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
                    external_initiated_event_id: 0,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId(format!("cancel-child-{child_run_id:?}")),
                        caller_identity: Some("runtime-child-orchestrator".to_string()),
                        principal: None,
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
                    if message.contains("kernel rejected") || message.contains("not found") {
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
        header: Option<tokeira_types::Headers>,
        originator_run_key: RunKey,
        namespace_id: NamespaceId,
        initiated_event_id: i64,
    ) {
        // A workflow cannot signal-external ITSELF: same target namespace and
        // workflow id fails with EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND before
        // any delivery — "it does not matter if the run ID is a mismatch"
        // (transfer_queue_active_task_executor.go:683-697 @ v1.31.0).
        let originator_identity = match self.repo.load_run(originator_run_key).await {
            Ok(LoadedRun::Existing(state)) => Some((state.namespace_id, state.workflow_id)),
            _ => None,
        };
        if originator_identity
            .as_ref()
            .is_some_and(|(origin_ns, origin_wf)| {
                *origin_ns == namespace_id && *origin_wf == target_workflow_id
            })
        {
            let resolve = Command::ExternalSignalResolved(ExternalSignalResolvedRequest {
                initiated_event_id,
                result: ExternalSignalResult::Failed {
                    cause: "external workflow execution not found (self-signal)".to_string(),
                },
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
                    "failed to deliver self-signal ExternalSignalResolved"
                );
            }
            return;
        }
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
                // The delivered signal carries the command's header and the
                // history service's identity, exactly as the transfer executor
                // forwards them (Identity=consts.IdentityHistoryService +
                // Header: attributes.Header,
                // transfer_queue_active_task_executor.go:1574-1604 @ v1.31.0).
                let command = Command::Signal(SignalRequest {
                    signal_name,
                    input,
                    header,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-signal-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some("history-service".to_string()),
                        principal: None,
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
                    Ok(CommitResult::CurrentExecutionConflict { .. }) => {
                        // Unreachable for a signal (not a zero-seq start); mapped
                        // defensively rather than panicking.
                        ExternalSignalResult::Failed {
                            cause: "unexpected current-execution conflict".to_string(),
                        }
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
        // A workflow cannot cancel-external ITSELF: same target namespace and
        // workflow id fails with EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND before
        // any delivery — "it does not matter if the run ID is a mismatch"
        // (transfer_queue_active_task_executor.go:546-559 @ v1.31.0). Both
        // ids must match: the corpus cancels the SAME workflow id in a
        // DIFFERENT namespace, which must go through.
        if originator_namespace_id == namespace_id && originator_workflow_id == target_workflow_id {
            let resolve = Command::ExternalCancelResolved(ExternalCancelResolvedRequest {
                initiated_event_id,
                result: ExternalCancelResult::Failed {
                    cause: "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND".to_string(),
                },
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
                    "failed to deliver self-cancel ExternalCancelResolved"
                );
            }
            return;
        }
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
                // The delivered cancel carries the initiator's initiated
                // event id and the history service's identity, exactly as
                // the transfer executor forwards them
                // (ExternalInitiatedEventId: task.InitiatedEventID,
                // Identity=consts.IdentityHistoryService,
                // transfer_queue_active_task_executor.go:1544-1567 @ v1.31.0).
                let command = Command::Cancel(CancelRequest {
                    reason,
                    external_initiator: Some(ExternalWorkflowExecution {
                        namespace_id: originator_namespace_id,
                        workflow_id: originator_workflow_id,
                        run_id: originator_run_id,
                    }),
                    external_initiated_event_id: initiated_event_id,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId(format!(
                            "ext-cancel-{originator_run_key:?}-{initiated_event_id}"
                        )),
                        caller_identity: Some("history-service".to_string()),
                        principal: None,
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
                    Ok(CommitResult::CurrentExecutionConflict { .. }) => {
                        // Unreachable for a cancel (not a zero-seq start); mapped
                        // defensively rather than panicking.
                        ExternalCancelResult::Failed {
                            cause: "unexpected current-execution conflict".to_string(),
                        }
                    }
                    Err(error) => ExternalCancelResult::Failed {
                        cause: error.to_string(),
                    },
                }
            }
            // The canonical cause string is what the history serializer maps
            // to CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_
            // WORKFLOW_EXECUTION_NOT_FOUND on the Failed event (NotFound from
            // the target lookup, transfer_queue_active_task_executor.go:
            // 577-599 @ v1.31.0).
            Ok(None) => ExternalCancelResult::Failed {
                cause: "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND".to_string(),
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
        scheduled_at: OffsetDateTime,
    ) {
        let resolution = match self.nexus_registry.resolve(&endpoint_name) {
            Some(config) => match &config.target {
                EndpointTarget::External { address } => {
                    let trace_headers = self.nexus_trace_headers();
                    let started_at = std::time::Instant::now();
                    let start_result = self
                        .nexus_client
                        .start_operation(
                            address,
                            &operation_id,
                            &service,
                            &operation,
                            &input,
                            schedule_to_close_timeout,
                            &trace_headers,
                        )
                        .await;
                    let latency = started_at.elapsed();
                    // Record the outbound StartOperation outcome (`startCallOutcomeTag @
                    // v1.31.0`). External calls do not flow through tokeira's own frontend,
                    // so the failure source stays unset → `_unknown_`
                    // (`SetFailureSourceOnContext` is worker-path only,
                    // `components/nexusoperations/fx.go @ v1.31.0`).
                    let outcome = match &start_result {
                        Ok(NexusStartResult::SyncCompleted { .. }) => "successful".to_string(),
                        Ok(NexusStartResult::AsyncAccepted { .. }) => "pending".to_string(),
                        Ok(NexusStartResult::SyncFailed { .. }) => {
                            "operation-unsuccessful:failed".to_string()
                        }
                        Ok(NexusStartResult::HandlerError { error_type, .. }) => {
                            format!("handler-error:{error_type}")
                        }
                        Err(_) => "unknown-error".to_string(),
                    };
                    self.record_external_outbound_metric(originator_run_key, &outcome, latency)
                        .await;

                    // SyncFailed (operation-unsuccessful), HandlerError, and a transport
                    // Err all resolve the caller's operation as failed; only the metric
                    // outcome distinguishes them (single attempt, no retry — Req 5.3).
                    let failed_from_message =
                        |message: String| tokeira_kernel::NexusResolution::Failed {
                            failure: failure_to_payload(&failure_proto::Failure {
                                message,
                                failure_info: Some(
                                    failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                                        failure_proto::ApplicationFailureInfo {
                                            r#type: "NexusOperationFailure".to_string(),
                                            non_retryable: false,
                                            ..Default::default()
                                        },
                                    ),
                                ),
                                ..Default::default()
                            }),
                        };
                    match start_result {
                        Ok(NexusStartResult::SyncCompleted { result, links }) => {
                            tokeira_kernel::NexusResolution::Completed { result, links }
                        }
                        Ok(NexusStartResult::AsyncAccepted {
                            operation_token,
                            links,
                        }) => tokeira_kernel::NexusResolution::Started {
                            operation_token,
                            links,
                        },
                        Ok(NexusStartResult::SyncFailed { message }) => {
                            failed_from_message(message)
                        }
                        Ok(NexusStartResult::HandlerError {
                            error_type,
                            failure,
                        }) => external_handler_error_resolution(
                            &error_type,
                            failure.as_ref(),
                            &endpoint_name,
                            &service,
                            &operation,
                            scheduled_event_id,
                        ),
                        Err(error) => failed_from_message(error.to_string()),
                    }
                }
                EndpointTarget::Worker {
                    namespace_id,
                    task_queue,
                } => {
                    // Attach a completion callback so the Worker handler delivers the
                    // eventual async outcome back to this originator's pending op. The
                    // handler POSTs that completion itself, so the callback URL must be a
                    // concrete `http(s)://…/nexus/callback` address it can reach — our own
                    // inbound listener — NOT the `temporal://system` sentinel (a Worker SDK
                    // rejects a non-HTTP scheme: "unknown scheme: temporal://system"). This
                    // is the `UseSystemCallbackURL = false` shape (the callback-URL-template
                    // mode, `components/nexusoperations/executors.go:122-160 @ v1.31.0`):
                    // tokeira delivers Worker completions over HTTP to its own listener
                    // rather than via the SDK's system-callback internal route, so it always
                    // resolves the address up front. `request_id` reuses `operation_id` — the
                    // only routing key in scope here — matching how `StartOperation.request_id`
                    // is already populated below (a tokeira convention; v1.31.0's
                    // `NexusOperationCompletion.request_id` is a distinct field, documented in
                    // nexus-async-completion 3.1).
                    let token = crate::nexus::NexusCompletionToken {
                        originator_run_key,
                        operation_id: operation_id.clone(),
                        scheduled_event_id,
                        request_id: operation_id.clone(),
                        // Carried so the inbound completion handler can wrap a failed
                        // completion in NexusOperationFailureInfo without loading the run.
                        endpoint: endpoint_name.clone(),
                        service: service.clone(),
                        operation: operation.clone(),
                    };
                    let (callback_url, callback_token) = match token.encode() {
                        Ok(encoded) => (
                            Some(crate::nexus::system_callback_post_url(
                                &self.nexus_completion_config.system_callback_url,
                            )),
                            Some(encoded),
                        ),
                        Err(error) => {
                            // Encoding a small struct cannot realistically fail; if it
                            // somehow does, dispatch without a callback rather than
                            // dropping the operation (the caller would then rely on its
                            // schedule-to-close timeout). Never abort the dispatch.
                            tracing::warn!(
                                ?error,
                                operation_id = %operation_id,
                                "failed to encode nexus completion token; dispatching without callback"
                            );
                            (None, None)
                        }
                    };
                    let request = NexusTaskRequest::StartOperation {
                        service,
                        operation,
                        request_id: operation_id.clone(),
                        payload: input.0.first().cloned(),
                        scheduled_time: Some(scheduled_at),
                        callback_url,
                        callback_token,
                    };
                    self.nexus_broker
                        .publish_workflow(
                            *namespace_id,
                            task_queue.clone(),
                            originator_run_key,
                            operation_id,
                            scheduled_event_id,
                            request,
                        )
                        .await;
                    return;
                }
            },
            None => tokeira_kernel::NexusResolution::Failed {
                failure: failure_to_payload(&failure_proto::Failure {
                    message: format!("nexus endpoint not found: {endpoint_name}"),
                    failure_info: Some(
                        failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                            failure_proto::ApplicationFailureInfo {
                                r#type: "NexusOperationFailure".to_string(),
                                non_retryable: false,
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }),
            },
        };

        let command =
            Command::NexusOperationResolved(tokeira_kernel::NexusOperationResolvedRequest {
                operation_id,
                scheduled_event_id,
                resolution,
                now: OffsetDateTime::now_utc(),
            });
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

        match &config.target {
            EndpointTarget::External { address } => {
                // The cancel needs the operation *name* (not tokeira's operation id) and the
                // handler-issued async token, both resolved from the pending op. The handler
                // unmarshals the token (e.g. a WorkflowRunOperation token); fall back to
                // tokeira's operation id only when the op never started (no handler token).
                let (operation, operation_token) = match self
                    .lookup_nexus_operation_cancel_target(originator_run_key, &operation_id)
                    .await
                {
                    Ok(Some((operation, token))) => (operation, token),
                    Ok(None) => {
                        tracing::warn!(
                            originator_run_key = ?originator_run_key,
                            operation_id,
                            "cancel nexus operation skipped: pending operation not found"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            operation_id,
                            "cancel nexus operation skipped: failed to load run"
                        );
                        return;
                    }
                };
                let cancel_token = if operation_token.is_empty() {
                    operation_id.clone()
                } else {
                    operation_token
                };
                let trace_headers = self.nexus_trace_headers();
                match self
                    .nexus_client
                    .cancel_operation(address, &service, &operation, &cancel_token, &trace_headers)
                    .await
                {
                    Ok(()) => {
                        // A successful cancel request is only an ack; it must NOT resolve the
                        // operation. v1.31.0 decouples cancel-ack from resolution — the
                        // operation resolves via its completion when the handler closes it
                        // (EventCancelationSucceeded only advances the cancelation sub-machine,
                        // statemachine.go:671; a completion that already resolved the op wins,
                        // statemachine.go:424). The inbound /nexus/callback delivers that
                        // completion.
                        tracing::debug!(
                            originator_run_key = ?originator_run_key,
                            scheduled_event_id,
                            "nexus cancel request acked; awaiting completion to resolve the operation"
                        );
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
            EndpointTarget::Worker {
                namespace_id,
                task_queue,
            } => {
                let (operation, operation_token) = match self
                    .lookup_nexus_operation_cancel_target(originator_run_key, &operation_id)
                    .await
                {
                    Ok(Some((operation, token))) => (operation, token),
                    Ok(None) => {
                        tracing::warn!(
                            originator_run_key = ?originator_run_key,
                            operation_id,
                            "cancel nexus operation skipped: pending operation not found"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            originator_run_key = ?originator_run_key,
                            operation_id,
                            "cancel nexus operation skipped: failed to load pending operation"
                        );
                        return;
                    }
                };
                // The handler unmarshals the async token (e.g. a WorkflowRunOperation
                // token); fall back to tokeira's operation id only for a not-yet-started op.
                let cancel_token = if operation_token.is_empty() {
                    operation_id.clone()
                } else {
                    operation_token
                };
                let request = NexusTaskRequest::CancelOperation {
                    service,
                    operation_id: operation_id.clone(),
                    operation,
                    operation_token: cancel_token,
                };
                self.nexus_broker
                    .publish_workflow(
                        *namespace_id,
                        task_queue.clone(),
                        originator_run_key,
                        operation_id,
                        scheduled_event_id,
                        request,
                    )
                    .await;
            }
        }
    }

    /// The pending op's (operation name, handler async token) for a cancel dispatch. The
    /// token is what a WorkflowRunOperation handler unmarshals; it is empty until the op
    /// started, so callers fall back to tokeira's operation id.
    async fn lookup_nexus_operation_cancel_target(
        &self,
        run_key: RunKey,
        operation_id: &str,
    ) -> Result<Option<(String, String)>> {
        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(state
                .pending_nexus_operations
                .get(operation_id)
                .map(|pending| (pending.operation.clone(), pending.operation_token.clone()))),
            LoadedRun::Absent => Ok(None),
        }
    }

    /// Fire one completion callback: build the Nexus completion from the terminal
    /// `outcome`, POST it to the callback URL (resolving `temporal://system` to the
    /// configured local listener), and record the attempt outcome on the closed run via
    /// `CompletionCallbackAttempted`. Reused by the dispatch loop (first attempt) and the
    /// retry scanner (re-fire). Mirrors v1.31.0's callbacks-component invocation
    /// (`components/callbacks/nexus_invocation.go @ v1.31.0`).
    pub(crate) async fn deliver_completion_callback(
        &self,
        run_key: RunKey,
        callback_index: usize,
        callback: &CompletionCallback,
        outcome: &CallbackCompletionOutcome,
    ) {
        let CallbackSpec::Nexus { url, header } = &callback.spec;
        // Look the token header up case-INSENSITIVELY. HTTP header names are
        // case-insensitive, and the inbound `StartWorkflowExecution` path lowercases
        // every callback header key (`callback_to_edge`, mirroring v1.31.0's
        // `nexus.Header`, `common/nexus/nexusrpc/api.go:56,110 @ v1.31.0`) — so a callback
        // attached by a Worker handler is stored as `temporal-callback-token`, while a
        // callback tokeira authors directly uses the mixed-case `Temporal-Callback-Token`
        // const. A case-sensitive `get` would miss the lowercased form and fire with an
        // empty token, which the inbound listener then rejects (400) — silently breaking
        // the loopback.
        let token = header
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(TEMPORAL_CALLBACK_TOKEN_HEADER))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();

        // Map the kernel outcome onto the wire completion, synthesizing the Nexus
        // failure body for the non-success kinds (the kernel forwards bare variants; the
        // runtime owns failure synthesis — kernel `transition.rs` `CallbackCompletionOutcome`
        // doc). Mirrors `GetNexusCompletion @ v1.31.0`.
        let completion = match outcome {
            CallbackCompletionOutcome::Success { result } => {
                NexusCompletion::Succeeded(Payloads(result.iter().cloned().collect()))
            }
            CallbackCompletionOutcome::Failed { failure } => NexusCompletion::Failed(
                completion_failure_body("operation failed", failure.clone()),
            ),
            CallbackCompletionOutcome::Canceled { details } => NexusCompletion::Canceled(
                completion_failure_body("operation canceled", synth_canceled_failure(details)),
            ),
            CallbackCompletionOutcome::Terminated => NexusCompletion::Failed(
                completion_failure_body("operation terminated", synth_terminated_failure()),
            ),
            CallbackCompletionOutcome::TimedOut => NexusCompletion::Failed(
                completion_failure_body("operation timed out", synth_timed_out_failure()),
            ),
            CallbackCompletionOutcome::ContinuedAsNew => {
                NexusCompletion::Failed(completion_failure_body(
                    "operation continued as new",
                    synth_continued_as_new_failure(),
                ))
            }
        };

        // Resolve the `temporal://system` sentinel to the configured local listener (the
        // loopback v1.31.0 performs via `routeSystemCallbackRequest`), appending the fixed
        // completion path; external URLs already encode their own path and are posted as-is.
        let target = if url == SYSTEM_CALLBACK_URL {
            crate::nexus::system_callback_post_url(
                &self.nexus_completion_config.system_callback_url,
            )
        } else {
            url.clone()
        };

        let delivery = self
            .nexus_completion_client
            .complete_operation(&target, &token, completion, &callback.links)
            .await;

        let now = OffsetDateTime::now_utc();
        let attempt_outcome = match delivery {
            Ok(CompletionDeliveryOutcome::Delivered) => {
                self.completion_callback_tracking
                    .remove(run_key, callback_index);
                CallbackAttemptOutcome::Succeeded
            }
            Ok(CompletionDeliveryOutcome::NonRetryableError { detail }) => {
                self.completion_callback_tracking
                    .remove(run_key, callback_index);
                CallbackAttemptOutcome::NonRetryableFailure {
                    failure: attempt_failure_payload(&detail),
                }
            }
            // A retryable status or a pre-flight `Err` (treated as a transient transport
            // failure): back off, unless the attempt cap is reached.
            other => {
                let detail = match other {
                    Ok(CompletionDeliveryOutcome::RetryableError { detail }) => detail,
                    Err(error) => format!("nexus completion delivery error: {error}"),
                    // Delivered / NonRetryableError already handled above.
                    Ok(_) => unreachable!("delivered/non-retryable handled above"),
                };
                let next_attempt = callback.attempt + 1;
                let max = self.nexus_completion_config.retry_max_attempts;
                if max != 0 && next_attempt >= max {
                    // Attempts exhausted → terminal Failed (design Error Handling).
                    self.completion_callback_tracking
                        .remove(run_key, callback_index);
                    CallbackAttemptOutcome::NonRetryableFailure {
                        failure: attempt_failure_payload(&detail),
                    }
                } else {
                    // Per-callback jitter seed so distinct callbacks de-correlate their
                    // retries (anti-synchronized-storm), derived from the run key + index.
                    let jitter_seed =
                        (run_key.0.as_u128() as u64) ^ (callback_index as u64).rotate_left(1);
                    let next_attempt_at = now
                        + nexus_completion_backoff(
                            &self.nexus_completion_config,
                            next_attempt,
                            jitter_seed,
                        );
                    self.completion_callback_tracking
                        .insert(CompletionCallbackTrackingEntry {
                            run_key,
                            shard_id: shard_for(run_key, self.shard_count),
                            callback_index,
                        });
                    CallbackAttemptOutcome::RetryableFailure {
                        failure: attempt_failure_payload(&detail),
                        next_attempt_at,
                    }
                }
            }
        };

        let command = Command::CompletionCallbackAttempted(CompletionCallbackAttemptedRequest {
            callback_index,
            outcome: attempt_outcome,
            now,
        });
        if let Err(error) = self.submit_to_run(run_key, command).await {
            // A kernel rejection means the callback already advanced to a terminal state
            // (another attempt won, or the callback index is gone): the index entry is
            // stale, drop it. Any other error is transient and the attempt outcome was
            // NOT recorded — re-seed the tracking index so the scanner re-drives this
            // callback (it is still `Scheduled`/`BackingOff` on the closed run). Without
            // this, a transient submit failure would strand the callback until a shard
            // takeover rebuild.
            if error.to_string().contains("kernel rejected") {
                self.completion_callback_tracking
                    .remove(run_key, callback_index);
            } else {
                self.completion_callback_tracking
                    .insert(CompletionCallbackTrackingEntry {
                        run_key,
                        shard_id: shard_for(run_key, self.shard_count),
                        callback_index,
                    });
                tracing::warn!(
                    ?error,
                    run_key = ?run_key,
                    callback_index,
                    "failed to submit completion callback attempt; re-queued for the scanner"
                );
            }
        }
    }
}

/// Rehydrate an External-endpoint Nexus *handler* error (a non-2xx HTTP response) into the
/// caller's failure chain so the SDK decodes it as `NexusOperationError -> HandlerError ->
/// ApplicationError(+details)`.
///
/// Mirrors v1.31.0's history-service conversion (`NexusFailureToTemporalFailure`,
/// `common/nexus/failure.go:181-305 @ v1.31.0`) and the worker-path
/// `wrap_handler_failure_as_resolution`: an outer `NexusOperationFailureInfo` (the operation
/// failed) wraps a `NexusHandlerFailureInfo{type}` whose cause is a generic
/// `ApplicationFailureInfo{type:"NexusFailure"}` carrying the handler's `nexus.Failure`
/// (`{metadata, details}`, message cleared) as a `json/plain` details payload — so the SDK's
/// `appErr.Details(&nexus.Failure)` recovers the original metadata and details.
fn external_handler_error_resolution(
    error_type: &str,
    failure: Option<&NexusHttpFailureBody>,
    endpoint: &str,
    service: &str,
    operation: &str,
    scheduled_event_id: i64,
) -> tokeira_kernel::NexusResolution {
    let app_cause = failure.map(|body| {
        let details = if body.metadata.is_empty() && body.details.is_null() {
            None
        } else {
            // `json.Marshal(nexus.Failure{Metadata,Details})` with message/stacktrace cleared
            // (`nexusFailureMetadataToApplicationFailureInfo @ v1.31.0`).
            let mut obj = serde_json::Map::new();
            obj.insert(
                "message".to_string(),
                serde_json::Value::String(String::new()),
            );
            if !body.metadata.is_empty() {
                obj.insert(
                    "metadata".to_string(),
                    serde_json::to_value(&body.metadata).unwrap_or(serde_json::Value::Null),
                );
            }
            if !body.details.is_null() {
                obj.insert("details".to_string(), body.details.clone());
            }
            let data = serde_json::to_vec(&serde_json::Value::Object(obj)).unwrap_or_default();
            let payload = Payload {
                data,
                metadata: std::collections::BTreeMap::from([(
                    "encoding".to_string(),
                    "json/plain".to_string(),
                )]),
                external_payloads: Vec::new(),
            };
            Some(payloads_from_domain(&Payloads(vec![payload])))
        };
        failure_proto::Failure {
            message: body.message.clone(),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "NexusFailure".to_string(),
                    details,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    });

    let handler_failure = failure_proto::Failure {
        message: failure.map(|body| body.message.clone()).unwrap_or_default(),
        failure_info: Some(
            failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(
                failure_proto::NexusHandlerFailureInfo {
                    r#type: error_type.to_string(),
                    // UNSPECIFIED: the per-attempt retryability is decided by status, not echoed here.
                    retry_behavior: 0,
                },
            ),
        ),
        cause: app_cause.map(Box::new),
        ..Default::default()
    };

    let wrapped = failure_proto::Failure {
        message: "nexus operation completed unsuccessfully".to_string(),
        failure_info: Some(
            failure_proto::failure::FailureInfo::NexusOperationExecutionFailureInfo(
                failure_proto::NexusOperationFailureInfo {
                    scheduled_event_id,
                    endpoint: endpoint.to_string(),
                    service: service.to_string(),
                    operation: operation.to_string(),
                    // operation_id / operation_token are deprecated wire fields; leave them
                    // defaulted (empty) rather than naming the deprecated fields.
                    ..Default::default()
                },
            ),
        ),
        cause: Some(Box::new(handler_failure)),
        ..Default::default()
    };

    tokeira_kernel::NexusResolution::Failed {
        failure: failure_to_payload(&wrapped),
    }
}

/// Wrap an originating failure `Payload` into the completion failure body bytes.
fn completion_failure_body(message: &str, failure: Payload) -> Vec<u8> {
    NexusCompletionFailureBody {
        message: message.to_string(),
        failure,
    }
    .encode()
}

/// A small application failure recording why a delivery attempt failed (carried on the
/// callback's `last_attempt_failure` for the Describe surface; not the operation outcome).
fn attempt_failure_payload(detail: &str) -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: detail.to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
            failure_proto::ApplicationFailureInfo {
                r#type: "NexusCompletionDeliveryFailure".to_string(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    })
}

/// Synthesize the failure `Payload` for a canceled handler workflow
/// (`CanceledFailureInfo` carrying the cancel details, `GetNexusCompletion` canceled arm
/// @ v1.31.0).
fn synth_canceled_failure(details: &Option<Payloads>) -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: "operation canceled".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::CanceledFailureInfo(
            failure_proto::CanceledFailureInfo {
                details: details.as_ref().map(payloads_from_domain),
                identity: String::new(),
            },
        )),
        ..Default::default()
    })
}

/// Synthesize the failure `Payload` for a terminated handler workflow
/// (`TerminatedFailureInfo`, `GetNexusCompletion` @ v1.31.0).
fn synth_terminated_failure() -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: "operation terminated".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::TerminatedFailureInfo(
            failure_proto::TerminatedFailureInfo {
                identity: String::new(),
            },
        )),
        ..Default::default()
    })
}

/// Synthesize the failure `Payload` for a timed-out handler workflow. Verbatim from
/// `GetNexusCompletion`'s timed-out arm (`service/history/workflow/mutable_state_impl.go:124-133
/// @ v1.31.0`): message `"operation exceeded internal timeout"` with an **empty**
/// `TimeoutFailureInfo` — v1.31.0 deliberately does not fill the timeout type ("it's not
/// particularly interesting to a Nexus caller").
fn synth_timed_out_failure() -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: "operation exceeded internal timeout".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::TimeoutFailureInfo(
            failure_proto::TimeoutFailureInfo::default(),
        )),
        ..Default::default()
    })
}

/// Synthesize the failure `Payload` for a continued-as-new handler workflow. v1.31.0's
/// `GetNexusCompletion` returns an internal error here; tokeira deliberately maps it to a
/// `failed` completion so the caller resolves rather than hanging (documented deviation,
/// nexus-async-completion task 1.1).
fn synth_continued_as_new_failure() -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: "operation completed by continue-as-new".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
            failure_proto::ApplicationFailureInfo {
                r#type: "NexusOperationContinuedAsNew".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    })
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
                    normal_task_queue,
                    speculative,
                } => {
                    let _ = speculative;
                    let workflow_queue = queue.clone();
                    // R.1: a SPECULATIVE task dispatched sticky-first falls back
                    // to the normal queue immediately when no sticky poller is
                    // waiting, so a lost sticky worker does not strand the update
                    // behind the 5s schedule-to-start timeout
                    // (`StickyWorkerUnavailable` → swap to normal + retry,
                    // updateworkflow/api.go:224-233 @ v1.31.0). Normal-queue
                    // fallback drops the sticky preference; the in-memory
                    // schedule-to-start timer still guards the normal dispatch.
                    let (final_queue, final_sticky) = if *speculative
                        && sticky_preferred.is_some()
                        && !self.has_workflow_poller(&workflow_queue).await
                        && let Some(normal_name) = normal_task_queue
                    {
                        let normal = QueueKey {
                            task_queue: normal_name.clone(),
                            ..queue.clone()
                        };
                        (normal, None)
                    } else {
                        (workflow_queue, sticky_preferred.clone())
                    };
                    self.broker
                        .publish_workflow_task(
                            DispatchableWorkflowTask {
                                run_key,
                                queue: final_queue,
                                logical_seq: *logical_seq,
                                sticky_preferred: final_sticky,
                                sticky_expires_at: None,
                            },
                            Some(&self.delivery_metrics),
                        )
                        .await;
                }
                DispatchOp::RecordSpeculativeOutcome {
                    namespace_id,
                    committed,
                } => {
                    // M.1: the kernel is metric-free, so it signals a speculative
                    // completion's commit/rollback outcome as a post-commit op we
                    // count here (renamed to Temporal's speculative_workflow_task
                    // _commits / _rollbacks by the fork's metric bridge). The
                    // counter is labelled with the namespace NAME the corpus
                    // filters on.
                    if let Some(namespace) = self.resolve_namespace_name(*namespace_id).await {
                        crate::metrics::record_speculative_workflow_task_outcome(
                            &namespace, *committed,
                        );
                    }
                }
                DispatchOp::RecordSpeculativeTimeout {
                    namespace_id,
                    start_to_close,
                } => {
                    // M.1: a speculative in-memory timer applied its timeout —
                    // count the timer-task request (+ start-to-close timeout),
                    // both tagged with the operation the corpus filters on.
                    if let Some(namespace) = self.resolve_namespace_name(*namespace_id).await {
                        crate::metrics::record_speculative_timer_fired(&namespace, *start_to_close);
                    }
                }
                DispatchOp::EnqueueActivityTask { .. } => {
                    if let DispatchOp::EnqueueActivityTask {
                        queue,
                        activity_id,
                        input,
                        schedule_event_id,
                        attempt,
                        dispatch_revision,
                        stamp,
                        dispatch_at,
                        ..
                    } = op
                    {
                        let task = DispatchableActivityTask {
                            run_key,
                            queue: queue.clone(),
                            activity_id: activity_id.clone(),
                            input: input.clone(),
                            schedule_event_id: *schedule_event_id,
                            attempt: *attempt,
                            dispatch_revision: *dispatch_revision,
                            stamp: *stamp,
                        };
                        let now = OffsetDateTime::now_utc();
                        let delay = *dispatch_at - now;
                        // The offer becomes eligible at `dispatch_at`; anchor the
                        // schedule-to-start clock there (immediate dispatch uses
                        // `now`). record_scheduled preserves the schedule-to-close
                        // anchor across re-dispatch.
                        let effective = if delay.is_positive() {
                            *dispatch_at
                        } else {
                            now
                        };
                        self.activity_tracking.record_scheduled(
                            run_key,
                            shard_for(run_key, self.shard_count),
                            activity_id.clone(),
                            effective,
                        );
                        if delay.is_positive() {
                            // Jitter/eligibility delay (unpause/reset): hold the
                            // broker offer until the runtime-chosen time, matching
                            // v1.31.0's RegenerateActivityRetryTask timer. The
                            // durable dispatch row was already committed, so a
                            // crash during the delay is recovered by the sweeper.
                            let broker = self.activity_broker.clone();
                            let metrics = self.delivery_metrics.clone();
                            let activity_id = activity_id.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    delay.whole_milliseconds().max(0) as u64,
                                ))
                                .await;
                                if let Err(error) =
                                    broker.publish_activity_task(task, Some(&metrics)).await
                                {
                                    tracing::warn!(
                                        ?error,
                                        ?run_key,
                                        activity_id,
                                        "failed to publish delayed activity task"
                                    );
                                }
                            });
                        } else if let Err(error) = self
                            .activity_broker
                            .publish_activity_task(task, Some(&self.delivery_metrics))
                            .await
                        {
                            tracing::warn!(?error, run_key = ?run_key, activity_id, "failed to publish activity task");
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
                    parent_run_id,
                    parent_namespace_id,
                    parent_root_workflow_id,
                    parent_root_run_id,
                    initiated_event_id,
                    header,
                    memo,
                    search_attributes,
                    workflow_execution_timeout,
                    workflow_run_timeout,
                    workflow_task_timeout,
                    retry_policy,
                    cron_schedule,
                    reuse_policy,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let spec = StartChildDispatch {
                        child_workflow_id: child_workflow_id.clone(),
                        namespace_id: *namespace_id,
                        workflow_type: workflow_type.clone(),
                        task_queue: task_queue.clone(),
                        input: input.clone(),
                        parent_run_key: *parent_run_key,
                        parent_workflow_id: parent_workflow_id.clone(),
                        parent_run_id: *parent_run_id,
                        parent_namespace_id: *parent_namespace_id,
                        parent_root_workflow_id: parent_root_workflow_id.clone(),
                        parent_root_run_id: *parent_root_run_id,
                        initiated_event_id: *initiated_event_id,
                        header: header.clone(),
                        memo: memo.clone(),
                        search_attributes: search_attributes.clone(),
                        workflow_execution_timeout: *workflow_execution_timeout,
                        workflow_run_timeout: *workflow_run_timeout,
                        workflow_task_timeout: *workflow_task_timeout,
                        retry_policy: retry_policy.clone(),
                        cron_schedule: cron_schedule.clone(),
                        reuse_policy: *reuse_policy,
                    };
                    tokio::spawn(async move {
                        publisher.handle_start_child_workflow(spec).await;
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
                    header,
                } => {
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let target_workflow_id = target_workflow_id.clone();
                    let target_run_id = *target_run_id;
                    let signal_name = signal_name.clone();
                    let input = input.clone();
                    let header = header.clone();
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
                                header,
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
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    originator_run_key,
                    scheduled_event_id,
                    scheduled_at,
                } => {
                    // Watch the operation if it has any timeout. start-to-close
                    // only arms once started, but the entry must exist from
                    // schedule so the scanner reloads durable state and notices
                    // the started transition (the deadlines live in state, AGENTS §3).
                    if schedule_to_close_timeout.is_some()
                        || schedule_to_start_timeout.is_some()
                        || start_to_close_timeout.is_some()
                    {
                        self.nexus_timeout_tracking.insert(NexusTimeoutEntry {
                            run_key: *originator_run_key,
                            shard_id: shard_for(*originator_run_key, self.shard_count),
                            operation_id: operation_id.clone(),
                            scheduled_event_id: *scheduled_event_id,
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
                    let scheduled_at = *scheduled_at;
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
                                scheduled_at,
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
                DispatchOp::DispatchCompletionCallback {
                    callback_index,
                    callback,
                    outcome,
                } => {
                    // Fire the callback off the dispatch loop (it does HTTP I/O), mirror
                    // the cancel-nexus arm above.
                    let publisher = RuntimeDispatchPublisher::clone(self);
                    let callback_index = *callback_index;
                    let callback = callback.clone();
                    let outcome = outcome.clone();
                    tokio::spawn(async move {
                        publisher
                            .deliver_completion_callback(
                                run_key,
                                callback_index,
                                &callback,
                                &outcome,
                            )
                            .await;
                    });
                }
            }
        }
        Ok(())
    }

    async fn submit_to_run(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        self.pick_lane(run_key).submit(run_key, command).await
    }
}

/// Re-fire `BackingOff` completion callbacks whose `next_attempt_at` has passed, capped at
/// `max_per_scan`. Mirrors `scan_nexus_timeouts_once`: the durable `CompletionCallback` is
/// the authority for the current lifecycle state and deadline, so each entry's run is
/// reloaded; a stale entry (run absent, callback gone, or no longer backing off) is
/// dropped. Delivery reuses [`RuntimeDispatchPublisher::deliver_completion_callback`],
/// which re-seeds or clears the tracking index based on the new attempt outcome.
pub(crate) async fn scan_completion_callbacks_once<R>(
    repo: &R,
    tracking: &CompletionCallbackTrackingState,
    shard_id: Option<ShardId>,
    publisher: &RuntimeDispatchPublisher<R>,
    config: &CompletionCallbackScannerConfig,
) where
    R: RunRepository + 'static,
{
    let now = OffsetDateTime::now_utc();
    let entries = match shard_id {
        Some(shard_id) => tracking.snapshot_for_shard(shard_id),
        None => tracking.snapshot(),
    };
    let mut fired = 0usize;

    for entry in entries {
        if fired >= config.max_per_scan {
            break;
        }
        let state = match repo.load_run(entry.run_key).await {
            Ok(LoadedRun::Existing(state)) => state,
            Ok(LoadedRun::Absent) => {
                tracking.remove(entry.run_key, entry.callback_index);
                continue;
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    run_key = ?entry.run_key,
                    callback_index = entry.callback_index,
                    "completion callback scanner failed to load run"
                );
                continue;
            }
        };
        let Some(callback) = state.completion_callbacks.get(entry.callback_index) else {
            tracking.remove(entry.run_key, entry.callback_index);
            continue;
        };
        match callback.state {
            // `Scheduled` means the first delivery was lost (the firing task died, or its
            // attempt-commit failed) — re-drive it immediately. This is what closes the
            // crash-before-first-attempt recovery gap (mirrors v1.31.0 `RegenerateTasks`
            // re-issuing an invocation task for a `SCHEDULED` callback,
            // `components/callbacks/statemachine.go:76-96 @ v1.31.0`).
            CallbackState::Scheduled => {}
            // `BackingOff` is re-fired only once its backoff deadline has passed.
            CallbackState::BackingOff => match callback.next_attempt_at {
                Some(due) if now >= due => {}
                _ => continue,
            },
            CallbackState::Succeeded | CallbackState::Failed => {
                // Terminal: another attempt won or it failed out — drop the entry.
                tracking.remove(entry.run_key, entry.callback_index);
                continue;
            }
            // Standby (open run) / Blocked are not the scanner's to fire.
            _ => continue,
        }
        // Re-derive the outcome from the run's terminal event — the *same* event and the
        // *same* kernel fn the close path used for the first attempt — so a re-fire is
        // byte-identical to the first attempt (incl. canceled details), with no
        // re-derivation from `WorkflowState` and no persisted copy to drift. The terminal
        // event is the run's last event; read a small tail and take the latest mapped one.
        let events = match repo
            .read_history(
                entry.run_key,
                state.last_event_id.saturating_sub(4).max(0),
                16,
            )
            .await
        {
            Ok(events) => events,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    run_key = ?entry.run_key,
                    callback_index = entry.callback_index,
                    "completion callback scanner failed to read terminal history"
                );
                continue;
            }
        };
        let Some(outcome) = events
            .iter()
            .rev()
            .find_map(|event| callback_completion_outcome(&event.kind))
        else {
            // No mapped terminal event (a backing-off callback should always be on a
            // closed run, so this is defensive) — skip without dropping the entry.
            continue;
        };
        runtime_metrics::record_scanner_dispatched(
            "completion_callback",
            shard_id.map(|s| s.0).unwrap_or(0),
        );
        // Retry deliveries must have the same concurrency shape as first-attempt
        // dispatch. A callback handler may wait until it has received every callback in
        // a batch before replying to any of them (the v1.31.0 callbacks corpus does
        // exactly this). Awaiting one HTTP exchange here would therefore prevent the
        // scanner from issuing the remaining callbacks and deadlock the batch. Temporal
        // executes each invocation task independently; spawn the derived delivery effect
        // just as `DispatchCompletionCallback` does above
        // (`components/callbacks/executors.go @ v1.31.0`).
        let callback = callback.clone();
        let publisher = RuntimeDispatchPublisher::clone(publisher);
        let run_key = entry.run_key;
        let callback_index = entry.callback_index;
        tokio::spawn(async move {
            publisher
                .deliver_completion_callback(run_key, callback_index, &callback, &outcome)
                .await;
        });
        fired += 1;
    }
}

/// Background loop that drives `scan_completion_callbacks_once` per active shard on a
/// fixed cadence until cancelled (mirrors `run_nexus_timeout_scanner`).
pub async fn run_completion_callback_scanner<R>(
    repo: Arc<R>,
    tracking: CompletionCallbackTrackingState,
    publisher: RuntimeDispatchPublisher<R>,
    shard_owner: Arc<RwLock<ShardOwner>>,
    config: CompletionCallbackScannerConfig,
    cancel: CancellationToken,
) where
    R: RunRepository + 'static,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(config.scan_interval) => {}
        }

        let active_shards: Vec<_> = shard_owner.read().unwrap().active_shards().collect();
        for shard_id in active_shards {
            runtime_metrics::record_scanner_tick("completion_callback", shard_id.0);
            scan_completion_callbacks_once(
                repo.as_ref(),
                &tracking,
                Some(shard_id),
                &publisher,
                &config,
            )
            .await;
        }
    }
}
