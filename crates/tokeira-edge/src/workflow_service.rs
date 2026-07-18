//! Business-logic layer between gRPC handlers and the workflow runtime.
//!
//! This module translates transport concerns into runtime calls, long-poll
//! coordination, and visibility lookups. The important boundary is what does
//! *not* belong here: authoritative workflow mutation rules still live in the
//! runtime/kernel, and durable execution state still lives in storage. The
//! edge is responsible for request shaping, polling semantics, and combining
//! read-side helpers into the APIs the Temporal surface expects.
//!
//! Query and update delivery are the most nuanced paths here. Queries use a
//! two-path dispatch (direct broker dispatch for idle runs, barrier-buffered
//! attachment for active runs) to guarantee consistency without unnecessary
//! WFT round-trips. Updates flow through the `UpdateRegistry` and are
//! surfaced to workers as `ProtocolMessage` entries on the poll response.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use prost::Message as _;
use serde::de::DeserializeOwned;
use time::OffsetDateTime;
use tokeira_compatibility::{FEATURE_MATRIX, FeatureState};
use tokeira_kernel::{
    ActivityRetryPolicyPatch, CancelRequest, FieldChange, HistoryEvent, HistoryEventKind,
    LoadedRun, NexusCancellationAttemptOutcome, NexusResolution, PendingNexusOperation,
    ResetRequest, SignalRequest, SignalWithStartRequest, StartRequest, TerminateRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_proto::{
    conversions::common::failure_to_payload,
    public::temporal::api::{
        failure::v1 as failure_proto,
        rules::v1::{WorkflowRule, WorkflowRuleSpec},
    },
};
use tokeira_runtime::{
    ActivityTokenResolutionError, BatchActivityOptionsPatch, BatchError, BatchOperationEntry,
    BatchOperationStore, BatchProgressCounters, BatchResetTarget, BufferedQueryRegistry,
    CreateDeployment, CreateVersion, DeleteDeployment, DeleteVersion, DeleteWorkflowRequest,
    DeploymentPage, DeploymentView, DescribeVersion, InMemoryBroker, ListDeployments,
    MultiOperationError, MultiOperationResult, NexusTaskBroker, NexusTaskCorrelation,
    NexusTaskToken, NexusWorkflowTaskKind, OverlapDecision, OverlapPolicy, PendingUpdateTransport,
    QueryResult, RegisterPolledDeployment, ResetWorkflowResult, ScheduleActionResult,
    SchedulePatch, ScheduleStore, SetCurrent, SetCurrentOutcome, SetManager, SetManagerOutcome,
    SetRamping, SetRampingOutcome, SignalWithStartResult, StartWorkflowResult, StartedActivityTask,
    StartedWorkflowTask, TaskQueueConfigEntry, TaskQueueConfigStore, TaskQueueVersioningView,
    UpdateActivitiesOptionsRequest, UpdateComputeConfig, UpdateLifecycleError,
    UpdateLifecycleSnapshot, UpdateMetadata, UpdateTransportResolution, UpdateWaitPolicy,
    ValidateComputeConfig, VersionMetadataView, VersionView, WorkerRegistry, WorkflowActivation,
    WorkflowDeletion, WorkflowDeletionNotFound, WorkflowExecution, WorkflowExecutionStatus,
    compute_matching_times, decide_overlap, nexus_operation_next_attempt_at, schedule_workflow_id,
    scheduled_workflow_search_attributes,
};
use tokeira_storage::{
    AttributedHistoryEvent, ConflictToken, DeploymentKey, DeploymentName, DeploymentTaskQueueType,
    RunRepository,
};
use tokeira_types::{
    ActivityTaskToken, ArchetypeId, BuildId, DeploymentId, ExecutionRef, ExecutionStatus,
    HeartbeatStore, Payload, Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey,
    TaskKind, TaskQueueName, WorkerIdentity, WorkflowId,
};
use uuid::Uuid;

use crate::{
    batch_engine::{resolve_reset_target_from_history, run_batch_operation},
    errors::{EdgeError, EdgeResult},
    grpc::tracing_interceptor,
    history_wait::HistoryWaitRegistry,
    interceptors::{Action, EdgeContext, EdgeInterceptors, cross_namespace_commands_enabled},
    long_poll::LongPollGate,
    metrics as edge_metrics,
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    nexus_http::{NexusHttpWaiterRegistry, NexusHttpWorkerOutcome},
    operator_service::{ClusterInfo, OperatorApi, SearchAttributeDefinition},
    pending_queries::{LEGACY_QUERY_ID, PendingQueryStore},
    poller_registry::{ActivePoller, PollerRegistry},
    routing::{EdgeRouter, ensure_local},
    translate::{
        ActivityTarget, CountActivityExecutionsRequest, CountActivityExecutionsResponse,
        CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
        DeleteWorkflowExecutionRequest, DescribeTaskQueueRequest, DescribeTaskQueueResponse,
        DescribeWorkflowExecutionRequest, ExecuteMultiOperationOutcome,
        ExecuteMultiOperationRequest, ExecuteMultiOperationResponse, ListActivityExecutionsRequest,
        ListActivityExecutionsResponse, ListNamespacesResponse as EdgeListNamespacesResponse,
        ListTaskQueuePartitionsRequest, ListTaskQueuePartitionsResponse,
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, MultiOperationFailure,
        NamespaceCapabilities, NamespaceDescription, NamespaceStateUpdate, PauseActivityRequest,
        PauseActivityResponse, PauseWorkflowExecutionRequest, PauseWorkflowExecutionResponse,
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, ProtocolMessageDto, QueryResultDto, QueryWorkflowRequest,
        QueryWorkflowResponse, RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse, RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse, RegisterNamespaceRequest,
        RequestCancelWorkflowExecutionRequest, RequestCancelWorkflowExecutionResponse,
        ResetActivityRequest, ResetActivityResponse, ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse, RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse, RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse, RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse, RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse, RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse, SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse, SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse, SystemCapabilities, SystemInfo, TaskQueueConfig,
        TaskQueuePartition, TerminateWorkflowExecutionRequest, TerminateWorkflowExecutionResponse,
        UnpauseActivityRequest, UnpauseActivityResponse, UnpauseWorkflowExecutionRequest,
        UnpauseWorkflowExecutionResponse, UpdateActivityOptionsRequest,
        UpdateActivityOptionsResponse, UpdateNamespaceRequest,
        UpdateWorkflowExecutionOptionsRequest, UpdateWorkflowExecutionOptionsResponse,
        UpdateWorkflowExecutionRequest, UpdateWorkflowExecutionResponse, VersioningOverrideChange,
        WorkflowExecutionDescription, WorkflowQueryDto, from_internal, to_internal,
    },
    workflow_rules::{WorkflowRuleError, WorkflowRuleStore},
};

#[cfg(feature = "conformance")]
const WORKFLOW_RULES_ENABLED_KEY: &str = "frontend.workflowRulesAPIsEnabled";

#[cfg(not(feature = "conformance"))]
pub(crate) fn workflow_rules_enabled() -> bool {
    false
}

pub(crate) fn workflow_rule_crud_admitted(enabled: bool) -> bool {
    enabled
}

#[cfg(feature = "conformance")]
pub(crate) fn workflow_rules_enabled() -> bool {
    // v1.31.0 reads this namespace policy at every CRUD request
    // (`service/frontend/workflow_handler.go:6985-7088 @ v1.31.0`).
    tokeira_conformance::overrides()
        .get_bool(WORKFLOW_RULES_ENABLED_KEY)
        .unwrap_or(false)
}

#[cfg(test)]
fn activity_offer_requires_rule_evaluation(_crud_gate_at_poll_admission: bool) -> bool {
    // The frontend flag gates only Workflow Rule CRUD. Temporal history reads
    // stored namespace rules at activity-start time without consulting it, so
    // a long poll admitted under a prior flag value must still evaluate the
    // current rules when work is offered
    // (`recordactivitytaskstarted/api.go:332-372 @ v1.31.0`).
    true
}

#[derive(Clone, Debug)]
pub struct BatchDispatchContext {
    pub namespace_id: tokeira_types::NamespaceId,
    pub namespace_name: String,
    pub identity: String,
    pub edge_context: EdgeContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowMutationOutcome {
    pub transition_seq: u64,
    pub last_event_id: i64,
    pub was_duplicate: bool,
    pub execution_status: ExecutionStatus,
    pub new_run_id: Option<RunId>,
}

/// Worker-deployment mutation response with the post-commit conflict token.
///
/// Temporal's v2 deployment RPCs are CAS-shaped: mutating calls return enough state for
/// the next caller to supply an optimistic conflict token. Keeping the token alongside
/// the operation-specific view lets gRPC translators build the exact protobuf response
/// without re-reading the registry.
#[derive(Clone, Debug, PartialEq)]
pub struct DeploymentMutationOutcome<T> {
    pub conflict_token: ConflictToken,
    pub view: T,
}

fn schedule_request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        caller_identity: Some("schedule-engine".to_string()),
        principal: None,
        received_at: now,
    }
}

fn worker_identity_from_request(identity: String) -> Option<WorkerIdentity> {
    if identity.is_empty() {
        None
    } else {
        Some(WorkerIdentity(identity))
    }
}

fn activity_control_target(
    target: ActivityTarget,
) -> EdgeResult<tokeira_kernel::ActivityControlTarget> {
    match target {
        ActivityTarget::Id(activity_id) => {
            Ok(tokeira_kernel::ActivityControlTarget::Id(activity_id))
        }
        ActivityTarget::Type(activity_type) => {
            Ok(tokeira_kernel::ActivityControlTarget::Type(activity_type))
        }
        // Temporal v1.31.0 serves the `match_all` / `unpause_all` selectors on the
        // wire, but every history handler resolves activities by switching only on
        // Id/Type (`getActivityIDs` in unpauseactivity/resetactivity/
        // updateactivityoptions api.go @ v1.31.0). An all-target request therefore
        // resolves to an empty id set and returns activity-not-found — even for
        // the server's own batch worker. Match that rather than silently mutating
        // every pending activity.
        ActivityTarget::MatchAll => Err(EdgeError::NotFound(
            "activity not found: match_all/unpause_all selectors are not served by v1.31.0"
                .to_string(),
        )),
    }
}

fn activity_control_request_context(
    ctx: &EdgeContext,
    identity: &str,
    now: OffsetDateTime,
) -> RequestContext {
    RequestContext {
        request_id: RequestId(ctx.request_id.as_str().to_string()),
        caller_identity: (!identity.is_empty()).then(|| identity.to_string()),
        principal: ctx.event_principal(),
        received_at: now,
    }
}

fn validate_activity_jitter(jitter: Option<time::Duration>) -> EdgeResult<()> {
    if jitter.is_some_and(time::Duration::is_negative) {
        return Err(EdgeError::BadRequest(
            "activity jitter must not be negative".to_string(),
        ));
    }
    Ok(())
}

fn build_update_activity_options_command(
    ctx: &EdgeContext,
    req: &UpdateActivityOptionsRequest,
) -> EdgeResult<UpdateActivitiesOptionsRequest> {
    let patch = build_activity_options_patch(
        req.target.clone(),
        req.activity_options.as_ref(),
        &req.update_mask,
        req.restore_original,
        // Direct RPC: v1.31.0's updateactivityoptions only rejects
        // restore_original combined with a non-empty parsed field mask; a
        // populated ActivityOptions body alongside restore is accepted and
        // ignored (updateactivityoptions/api.go:37-43 @ v1.31.0).
        false,
    )?;
    Ok(UpdateActivitiesOptionsRequest {
        target: patch.target,
        task_queue: patch.task_queue,
        schedule_to_close_timeout: patch.schedule_to_close_timeout,
        schedule_to_start_timeout: patch.schedule_to_start_timeout,
        start_to_close_timeout: patch.start_to_close_timeout,
        heartbeat_timeout: patch.heartbeat_timeout,
        retry_policy: patch.retry_policy,
        restore_original_options: patch.restore_original_options,
        request: RequestContext {
            request_id: RequestId(ctx.request_id.as_str().to_string()),
            caller_identity: worker_identity_from_request(req.identity.clone())
                .map(|identity| identity.0),
            principal: ctx.event_principal(),
            received_at: ctx.received_at,
        },
        now: OffsetDateTime::now_utc(),
    })
}

pub(crate) fn build_activity_options_patch(
    target: ActivityTarget,
    activity_options: Option<&crate::translate::ActivityOptions>,
    update_mask: &[String],
    restore_original: bool,
    // Whether a populated ActivityOptions body alongside `restore_original` is an
    // error. StartBatchOperation validates this up front and rejects it
    // (`TestActivityBatchUpdateOptionsFailed`), while the direct per-workflow RPC
    // accepts and ignores it. The mask conflict is rejected on both paths.
    restore_forbids_options: bool,
) -> EdgeResult<BatchActivityOptionsPatch> {
    if restore_original {
        if !update_mask.is_empty() || (restore_forbids_options && activity_options.is_some()) {
            return Err(EdgeError::BadRequest(
                "Both UpdateMask and RestoreOriginal are provided".to_string(),
            ));
        }
        return Ok(BatchActivityOptionsPatch {
            target: activity_control_target(target)?,
            task_queue: FieldChange::Unchanged,
            schedule_to_close_timeout: FieldChange::Unchanged,
            schedule_to_start_timeout: FieldChange::Unchanged,
            start_to_close_timeout: FieldChange::Unchanged,
            heartbeat_timeout: FieldChange::Unchanged,
            retry_policy: ActivityRetryPolicyPatch::default(),
            restore_original_options: true,
        });
    }
    let options = activity_options
        .ok_or_else(|| EdgeError::BadRequest("activity_options is required".to_string()))?;
    let task_queue_selected = option_field_selected(update_mask, "task_queue");
    let schedule_to_close_selected =
        option_field_selected(update_mask, "schedule_to_close_timeout");
    let schedule_to_start_selected =
        option_field_selected(update_mask, "schedule_to_start_timeout");
    let start_to_close_selected = option_field_selected(update_mask, "start_to_close_timeout");
    let heartbeat_selected = option_field_selected(update_mask, "heartbeat_timeout");

    let task_queue = if task_queue_selected {
        match options.task_queue.as_ref() {
            Some(task_queue) => FieldChange::Set(TaskQueueName(task_queue.clone())),
            None => {
                return Err(EdgeError::BadRequest(
                    "task_queue cannot be cleared".to_string(),
                ));
            }
        }
    } else {
        FieldChange::Unchanged
    };

    let retry_policy = build_activity_retry_policy_patch(update_mask, options)?;
    let patch = BatchActivityOptionsPatch {
        target: activity_control_target(target)?,
        task_queue,
        schedule_to_close_timeout: optional_duration_change(
            schedule_to_close_selected,
            options.schedule_to_close_timeout,
        ),
        schedule_to_start_timeout: optional_duration_change(
            schedule_to_start_selected,
            options.schedule_to_start_timeout,
        ),
        start_to_close_timeout: optional_duration_change(
            start_to_close_selected,
            options.start_to_close_timeout,
        ),
        heartbeat_timeout: optional_duration_change(heartbeat_selected, options.heartbeat_timeout),
        retry_policy,
        restore_original_options: false,
    };
    if matches!(patch.task_queue, FieldChange::Unchanged)
        && matches!(patch.schedule_to_close_timeout, FieldChange::Unchanged)
        && matches!(patch.schedule_to_start_timeout, FieldChange::Unchanged)
        && matches!(patch.start_to_close_timeout, FieldChange::Unchanged)
        && matches!(patch.heartbeat_timeout, FieldChange::Unchanged)
        && patch.retry_policy == ActivityRetryPolicyPatch::default()
    {
        return Err(EdgeError::BadRequest(
            "update_activity_options requires at least one changed option".to_string(),
        ));
    }
    Ok(patch)
}

fn build_activity_retry_policy_patch(
    update_mask: &[String],
    options: &crate::translate::ActivityOptions,
) -> EdgeResult<ActivityRetryPolicyPatch> {
    let replacement_selected = option_field_selected(update_mask, "retry_policy");
    let nested_selected = [
        "retry_policy.initial_interval",
        "retry_policy.backoff_coefficient",
        "retry_policy.maximum_interval",
        "retry_policy.maximum_attempts",
        "retry_policy.non_retryable_error_types",
    ]
    .into_iter()
    .any(|field| option_field_selected(update_mask, field));
    let source = (replacement_selected || nested_selected)
        .then_some(options.retry_policy.as_ref())
        .flatten();
    if nested_selected && source.is_none() {
        return Err(EdgeError::BadRequest(
            "RetryPolicy is not provided".to_string(),
        ));
    }
    let mut patch = ActivityRetryPolicyPatch::default();
    if replacement_selected {
        patch.replacement = FieldChange::Set(options.retry_policy.clone());
        return Ok(patch);
    }
    let Some(source) = source else {
        return Ok(patch);
    };
    if option_field_selected(update_mask, "retry_policy.initial_interval") {
        patch.initial_interval = FieldChange::Set(source.initial_interval);
    }
    if option_field_selected(update_mask, "retry_policy.backoff_coefficient") {
        patch.backoff_coefficient = FieldChange::Set(source.backoff_coefficient);
    }
    if option_field_selected(update_mask, "retry_policy.maximum_interval") {
        patch.maximum_interval = FieldChange::Set(source.maximum_interval);
    }
    if option_field_selected(update_mask, "retry_policy.maximum_attempts") {
        patch.maximum_attempts = FieldChange::Set(source.maximum_attempts);
    }
    if option_field_selected(update_mask, "retry_policy.non_retryable_error_types") {
        patch.non_retryable_error_types =
            FieldChange::Set(source.non_retryable_error_types.clone());
    }
    Ok(patch)
}

fn optional_duration_change(
    selected: bool,
    value: Option<time::Duration>,
) -> FieldChange<Option<time::Duration>> {
    if selected {
        FieldChange::Set(value)
    } else {
        FieldChange::Unchanged
    }
}

fn option_field_selected(update_mask: &[String], field: &str) -> bool {
    if update_mask.is_empty() {
        return false;
    }
    let camel = field
        .split('_')
        .enumerate()
        .map(|(index, segment)| {
            if index == 0 {
                segment.to_string()
            } else {
                let mut chars = segment.chars();
                chars
                    .next()
                    .map(|value| value.to_ascii_uppercase())
                    .into_iter()
                    .chain(chars)
                    .collect::<String>()
            }
        })
        .collect::<String>();
    update_mask.iter().any(|path| {
        path == field
            || path == &camel
            || path == &format!("activity_options.{field}")
            || path == &format!("activityOptions.{camel}")
            // A nested mask path selects its parent field. Temporal's canonical
            // path for the task-queue message is `taskQueue.name` (the field is a
            // `TaskQueue` message, not a scalar), which `util.ParseFieldMask`
            // camelizes segment-by-segment; a bare `taskQueue` is ignored by
            // v1.31.0's merge. Accepting the `.name` subpaths keeps the canonical
            // client mask working. Scalar timeout fields never carry a subpath.
            || path.starts_with(&format!("{field}."))
            || path.starts_with(&format!("{camel}."))
            || path.starts_with(&format!("activity_options.{field}."))
            || path.starts_with(&format!("activityOptions.{camel}."))
    })
}

/// Classify a worker-reported Nexus cancellation failure using the same runtime
/// policy and schedule-to-close cap as External-endpoint delivery. The edge sees
/// the worker's public `Failure`; the kernel receives only the durable outcome and
/// a precomputed deadline.
fn worker_cancellation_failure_outcome(
    pending: &PendingNexusOperation,
    failure: failure_proto::Failure,
    retryable: bool,
    now: OffsetDateTime,
) -> NexusCancellationAttemptOutcome {
    let failure = failure_to_payload(&failure);
    let failed_attempts = pending
        .cancellation
        .as_ref()
        .map(|cancellation| cancellation.attempt)
        .unwrap_or_default();
    if retryable
        && let Some(next_attempt_at) = nexus_operation_next_attempt_at(
            failed_attempts,
            pending.scheduled_at,
            pending.schedule_to_close_timeout,
            now,
        )
    {
        return NexusCancellationAttemptOutcome::RetryableFailure {
            failure,
            next_attempt_at,
        };
    }
    NexusCancellationAttemptOutcome::NonRetryableFailure { failure }
}

#[async_trait]
pub trait WorkflowRuntimeApi: Send + Sync + 'static {
    /// Start a workflow and return mutation metadata for callers that only
    /// care about the committed transition, not conflict-policy nuance.
    async fn start_workflow(&self, req: StartRequest) -> Result<WorkflowMutationOutcome>;

    /// Start a workflow while preserving richer conflict/reuse results needed
    /// by edge APIs such as `SignalWithStartWorkflowExecution`.
    async fn start_workflow_with_policy(&self, req: StartRequest) -> Result<StartWorkflowResult>;

    /// Start a new execution or signal an existing one according to the
    /// workflow-id conflict policy carried in the request.
    async fn signal_with_start_workflow(
        &self,
        req: SignalWithStartRequest,
    ) -> Result<SignalWithStartResult>;

    async fn signal_workflow(
        &self,
        run_key: RunKey,
        req: SignalRequest,
    ) -> Result<WorkflowMutationOutcome>;

    /// Clear a run's sticky affinity (`ResetStickyTaskQueue` @ v1.31.0;
    /// sticky raise S5).
    async fn reset_sticky_task_queue(&self, run_key: RunKey) -> Result<()> {
        let _ = run_key;
        Err(anyhow::anyhow!(
            "reset_sticky_task_queue unsupported by this runtime"
        ))
    }

    async fn poll_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>>;

    /// Poll the worker-facing workflow queue for either a started WFT or a direct query.
    ///
    /// The default keeps older test doubles workflow-task-only. The real
    /// runtime adapter overrides this because Temporal-compatible workers
    /// receive legacy direct queries through `PollWorkflowTaskQueue`, not a
    /// separate query-poll RPC (`service/matching/matching_engine.go:1084 @
    /// v1.31.0`).
    async fn poll_workflow_activation(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<WorkflowActivation>> {
        self.poll_workflow_task(queue, worker_identity, timeout)
            .await
            .map(|task| task.map(WorkflowActivation::WorkflowTask))
    }

    /// Return a poller-count delta after a successful workflow poll, if the
    /// matching plane currently observes pressure on that physical queue.
    async fn workflow_poller_scaling_decision(
        &self,
        _queue: &tokeira_types::QueueKey,
    ) -> Option<i32> {
        None
    }

    /// Whether a workflow poll returned empty because worker shutdown cancelled
    /// it, rather than because its long-poll deadline elapsed.
    async fn workflow_poll_cancelled(
        &self,
        _queue: &tokeira_types::QueueKey,
        _worker: &tokeira_types::WorkerIdentity,
    ) -> bool {
        false
    }

    async fn try_claim_workflow_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: RunKey,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedWorkflowTask>> {
        let _ = (queue, run_key, worker_identity);
        Ok(None)
    }

    async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<WorkflowMutationOutcome>;

    /// `RespondWorkflowTaskFailed`: fail the workflow task identified by
    /// `token`, or — for cause `GrpcMessageTooLarge` — force-close-terminate
    /// the run (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`). Defaulted so
    /// workflow-task-only test doubles need not implement it.
    async fn fail_workflow_task(
        &self,
        token: tokeira_types::WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<tokeira_types::Payload>,
        worker_identity: tokeira_types::WorkerIdentity,
        request: tokeira_types::RequestContext,
        now: time::OffsetDateTime,
    ) -> Result<()> {
        let _ = (
            token,
            failure_cause,
            failure_details,
            worker_identity,
            request,
            now,
        );
        Err(anyhow::anyhow!(
            "fail_workflow_task is not supported by this runtime"
        ))
    }

    async fn poll_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<StartedActivityTask>>;

    /// Poll an activity offer without starting it so workflow rules can run at
    /// the v1.31.0 activity-start boundary.
    async fn poll_activity_task_offer(
        &self,
        _queue: tokeira_types::QueueKey,
        _worker_identity: tokeira_types::WorkerIdentity,
        _timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::ActivityTaskOffer>> {
        Err(anyhow!(
            "activity offer polling is not supported by this runtime"
        ))
    }

    /// Commit Started after edge policy accepts a previously polled offer.
    async fn start_activity_task_offer(
        &self,
        _offer: tokeira_runtime::ActivityTaskOffer,
        _worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        Err(anyhow!(
            "activity offer start is not supported by this runtime"
        ))
    }

    /// Return a previously polled offer to the broker when the edge cannot
    /// decide its fate — e.g. a transient failure while evaluating workflow
    /// rules — so the task is not stranded until a shard takeover. Defaults to a
    /// no-op for runtimes that do not support offer polling.
    async fn republish_activity_offer(&self, _offer: tokeira_runtime::ActivityTaskOffer) {}

    /// Return a poller-count delta after a successful activity poll, if the
    /// matching plane currently observes pressure on that physical queue.
    async fn activity_poller_scaling_decision(
        &self,
        _queue: &tokeira_types::QueueKey,
    ) -> Option<i32> {
        None
    }

    /// Snapshot one physical queue's live matching backlog.
    async fn task_queue_backlog_stats(
        &self,
        _queue: &tokeira_types::QueueKey,
    ) -> tokeira_runtime::BrokerBacklogStats {
        tokeira_runtime::BrokerBacklogStats::default()
    }

    /// Absorb already-unversioned ready work into a promoted deployment queue.
    async fn absorb_unversioned_backlog(&self, _queue: &tokeira_types::QueueKey) {}

    /// Apply the v1.31.0 worker-shutdown cancellation policy to outstanding
    /// matching polls. Returns whether the policy was enabled and applied.
    async fn cancel_outstanding_worker_polls(
        &self,
        _namespace_id: tokeira_types::NamespaceId,
        _task_queue: tokeira_types::TaskQueueName,
        _worker: tokeira_types::WorkerIdentity,
    ) -> bool {
        false
    }

    async fn try_claim_activity_task(
        &self,
        queue: tokeira_types::QueueKey,
        run_key: RunKey,
        activity_id: String,
        worker_identity: tokeira_types::WorkerIdentity,
    ) -> Result<Option<StartedActivityTask>> {
        let _ = (queue, run_key, activity_id, worker_identity);
        Ok(None)
    }

    async fn complete_activity_task(
        &self,
        token: ActivityTaskToken,
        result: Payloads,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome>;

    async fn fail_activity_task(
        &self,
        token: ActivityTaskToken,
        failure: Payload,
        failure_error_type: Option<String>,
        is_non_retryable: bool,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
        request: RequestContext,
    ) -> Result<()>;

    async fn cancel_activity_task(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        worker_identity: Option<tokeira_types::WorkerIdentity>,
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (token, details, worker_identity, request);
        Err(anyhow!("cancel_activity_task is not implemented"))
    }

    async fn record_activity_heartbeat(
        &self,
        token: ActivityTaskToken,
        details: Option<Payloads>,
        identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome>;

    async fn resolve_activity_token(
        &self,
        run_key: RunKey,
        activity_id: &str,
    ) -> std::result::Result<ActivityTaskToken, ActivityTokenResolutionError> {
        let _ = activity_id;
        Err(ActivityTokenResolutionError::RunNotFound { run_key })
    }

    /// Fabricate the started event for a not-yet-started activity so
    /// completed-by-id can force-complete it
    /// (`respondactivitytaskcompleted/api.go:89-105 @ v1.31.0`).
    async fn force_start_activity_for_completion(
        &self,
        run_key: RunKey,
        activity_id: &str,
        identity: tokeira_types::WorkerIdentity,
        request: RequestContext,
    ) -> Result<ActivityTaskToken> {
        let _ = (run_key, activity_id, identity, request);
        Err(tokeira_runtime::ActivityTaskNotFound {
            reason: "force start unsupported by this runtime",
        }
        .into())
    }

    async fn update_activity_options(
        &self,
        run_key: RunKey,
        req: UpdateActivitiesOptionsRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("update_activity_options is not implemented"))
    }

    async fn pause_activities(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::PauseActivityRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("pause_activities is not implemented"))
    }

    async fn unpause_activities(
        &self,
        run_key: RunKey,
        req: tokeira_runtime::UnpauseActivitiesRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("unpause_activities is not implemented"))
    }

    async fn reset_activities(
        &self,
        run_key: RunKey,
        req: tokeira_runtime::ResetActivitiesRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("reset_activities is not implemented"))
    }

    async fn terminate_workflow(
        &self,
        run_key: RunKey,
        req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome>;

    /// Delete one already-resolved run through the authoritative runtime path.
    ///
    /// The default preserves isolation for test doubles that never exercise
    /// deletion; the production runtime adapter overrides it.
    async fn delete_workflow(
        &self,
        run_key: RunKey,
        request: DeleteWorkflowRequest,
    ) -> Result<WorkflowDeletion> {
        let _ = (run_key, request);
        Err(anyhow!("delete_workflow is not implemented"))
    }

    /// Apply an `UpdateWorkflowExecutionOptions` change (currently the
    /// `versioning_override`) to a running execution. Defaults to unimplemented so test
    /// doubles need no change; the runtime adapter overrides it.
    async fn update_workflow_execution_options(
        &self,
        run_key: RunKey,
        versioning_override: FieldChange<tokeira_kernel::VersioningOverride>,
        request: RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, versioning_override, request);
        Err(anyhow!(
            "update_workflow_execution_options is not implemented"
        ))
    }

    async fn cancel_workflow(
        &self,
        run_key: RunKey,
        req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome>;

    async fn pause_workflow(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::PauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("pause_workflow is not implemented"))
    }

    async fn unpause_workflow(
        &self,
        run_key: RunKey,
        req: tokeira_kernel::UnpauseWorkflowRequest,
    ) -> Result<WorkflowMutationOutcome> {
        let _ = (run_key, req);
        Err(anyhow!("unpause_workflow is not implemented"))
    }

    async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        req: ResetRequest,
    ) -> Result<ResetWorkflowResult>;

    async fn query_workflow(
        &self,
        execution: ExecutionRef,
        query_type: String,
        query_args: Payloads,
        timeout: std::time::Duration,
    ) -> Result<QueryResult>;

    async fn update_workflow(
        &self,
        execution: ExecutionRef,
        update_id: String,
        update_name: String,
        input: Payloads,
        request: RequestContext,
        timeout: std::time::Duration,
        wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateLifecycleSnapshot>;

    /// Execute the composed Update-with-Start (`ExecuteMultiOperation`,
    /// exactly `[Start, Update]` — `multioperation/api.go @ v1.31.0`).
    /// Defaulted so workflow-task-only test doubles need no change; the
    /// runtime adapter overrides it.
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
        let _ = (
            start,
            update_id,
            update_name,
            input,
            request,
            timeout,
            wait_policy,
        );
        Err(anyhow!("execute_multi_operation is not implemented"))
    }

    async fn poll_workflow_update(
        &self,
        execution: ExecutionRef,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
        timeout: std::time::Duration,
    ) -> Result<UpdateLifecycleSnapshot> {
        let _ = (execution, update_id, wait_policy, timeout);
        Err(anyhow!("poll_workflow_update is not implemented"))
    }

    async fn pending_update_transports(
        &self,
        run_key: RunKey,
        include_sent: bool,
    ) -> Result<Vec<PendingUpdateTransport>>;

    async fn resolve_update_transport(
        &self,
        run_key: RunKey,
        update_id: String,
        resolution: UpdateTransportResolution,
    ) -> Result<bool>;

    /// Read the update_name and input for a registered update.
    async fn peek_update_info(
        &self,
        run_key: RunKey,
        update_id: String,
    ) -> Result<Option<(String, Payloads)>>;

    async fn resolve_nexus_operation(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        resolution: NexusResolution,
    ) -> Result<bool>;

    /// Record a worker-owned cancellation delivery outcome without resolving the
    /// parent Nexus operation.
    async fn record_nexus_cancellation_attempt(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        requested_event_id: i64,
        outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
    ) -> Result<bool>;
}

/// Runtime-facing Worker Deployment API consumed by the edge handlers.
///
/// The edge owns protobuf/defaulting/status translation; the runtime registry owns
/// durable deployment state and CAS validation. This trait keeps that split explicit so
/// future handlers can be tested against fakes without reaching into storage tables.
#[async_trait]
pub trait WorkerDeploymentRuntimeApi: Send + Sync + 'static {
    async fn create_worker_deployment(
        &self,
        req: CreateDeployment,
    ) -> EdgeResult<DeploymentMutationOutcome<DeploymentView>>;

    async fn describe_worker_deployment(&self, key: DeploymentKey) -> EdgeResult<DeploymentView>;

    async fn delete_worker_deployment(&self, req: DeleteDeployment) -> EdgeResult<()>;

    async fn list_worker_deployments(&self, req: ListDeployments) -> EdgeResult<DeploymentPage>;

    async fn create_worker_deployment_version(&self, req: CreateVersion) -> EdgeResult<()>;

    async fn describe_worker_deployment_version(
        &self,
        req: DescribeVersion,
    ) -> EdgeResult<VersionView>;

    async fn delete_worker_deployment_version(&self, req: DeleteVersion) -> EdgeResult<()>;

    async fn set_worker_deployment_current_version(
        &self,
        req: SetCurrent,
    ) -> EdgeResult<DeploymentMutationOutcome<SetCurrentOutcome>>;

    async fn set_worker_deployment_ramping_version(
        &self,
        req: SetRamping,
    ) -> EdgeResult<DeploymentMutationOutcome<SetRampingOutcome>>;

    async fn update_worker_deployment_version_compute_config(
        &self,
        req: UpdateComputeConfig,
    ) -> EdgeResult<()>;

    async fn validate_worker_deployment_version_compute_config(
        &self,
        req: ValidateComputeConfig,
    ) -> EdgeResult<()>;

    async fn update_worker_deployment_version_metadata(
        &self,
        req: UpdateMetadata,
    ) -> EdgeResult<VersionMetadataView>;

    async fn set_worker_deployment_manager(
        &self,
        req: SetManager,
    ) -> EdgeResult<DeploymentMutationOutcome<SetManagerOutcome>>;

    /// Lazily register the deployment/version implied by a versioned worker poll.
    /// A no-op for unversioned polls. Idempotent.
    async fn register_polled_deployment(&self, req: RegisterPolledDeployment) -> EdgeResult<()>;

    /// Apply a `sync-drainage-status` signal addressed to a version entity
    /// workflow onto the registry. No-op for an absent deployment/version or a
    /// version that is currently Current/Ramping.
    async fn apply_version_drainage(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        deployment_name: DeploymentName,
        build_id: tokeira_storage::BuildId,
        status: tokeira_storage::VersionDrainageStatus,
    ) -> EdgeResult<()>;

    /// Resolve the Worker Deployment versioning view for one task queue, for
    /// `DescribeTaskQueue.versioning_info`. `None` when no version polls it.
    async fn task_queue_versioning(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: String,
    ) -> EdgeResult<Option<TaskQueueVersioningView>>;
}

#[async_trait]
pub trait ExecutionResolver: Send + Sync + 'static {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>>;

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>>;
}

// Visibility API re-exported from tokeira-projection (the authoritative owner).
pub use tokeira_projection::{EmptyVisibilityApi, VisibilityApi};

#[derive(Debug, Default)]
pub struct InMemoryExecutionResolver {
    current: tokio::sync::RwLock<std::collections::HashMap<(String, String), RunKey>>,
    descriptions: tokio::sync::RwLock<
        std::collections::HashMap<(String, String), WorkflowExecutionDescription>,
    >,
    descriptions_by_run: tokio::sync::RwLock<
        std::collections::HashMap<(String, String, String), WorkflowExecutionDescription>,
    >,
}

impl InMemoryExecutionResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_current_run(
        &self,
        namespace: impl Into<String>,
        workflow_id: impl Into<String>,
        run_key: RunKey,
    ) {
        self.current
            .write()
            .await
            .insert((namespace.into(), workflow_id.into()), run_key);
    }

    pub async fn set_description(&self, description: WorkflowExecutionDescription) {
        let run_id = description.run_id.0.to_string();
        self.descriptions.write().await.insert(
            (
                description.namespace.clone(),
                description.workflow_id.clone(),
            ),
            description.clone(),
        );
        self.descriptions_by_run.write().await.insert(
            (
                description.namespace.clone(),
                description.workflow_id.clone(),
                run_id,
            ),
            description,
        );
    }
}

#[async_trait]
impl ExecutionResolver for InMemoryExecutionResolver {
    async fn current_run_key(&self, namespace: &str, workflow_id: &str) -> Result<Option<RunKey>> {
        Ok(self
            .current
            .read()
            .await
            .get(&(namespace.to_string(), workflow_id.to_string()))
            .copied())
    }

    async fn describe_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<RunId>,
    ) -> Result<Option<WorkflowExecutionDescription>> {
        if let Some(run_id) = run_id {
            return Ok(self
                .descriptions_by_run
                .read()
                .await
                .get(&(
                    namespace.to_string(),
                    workflow_id.to_string(),
                    run_id.0.to_string(),
                ))
                .cloned());
        }
        Ok(self
            .descriptions
            .read()
            .await
            .get(&(namespace.to_string(), workflow_id.to_string()))
            .cloned())
    }
}

/// The subset of a run's internal mutable state exposed by the AdminService's
/// `DescribeMutableState` — only what the reset conformance suite reads.
#[derive(Clone, Debug, PartialEq)]
pub struct MutableStateSummary {
    /// The run's raw execution status.
    pub status: ExecutionStatus,
    /// The run this run was reset into, if any (`ExecutionInfo.ResetRunId`).
    pub reset_run_id: Option<RunId>,
    /// The original run of the chain (`ExecutionInfo.OriginalExecutionRunId`).
    pub original_execution_run_id: Option<RunId>,
}

#[derive(Clone)]
pub struct WorkflowService {
    runtime: Arc<dyn WorkflowRuntimeApi>,
    worker_deployments: Option<Arc<dyn WorkerDeploymentRuntimeApi>>,
    resolver: Arc<dyn ExecutionResolver>,
    visibility: Arc<dyn VisibilityApi>,
    repo: Arc<dyn RunRepository>,
    operator_api: Arc<dyn OperatorApi>,
    namespaces: Arc<dyn NamespaceCache>,
    interceptors: Arc<EdgeInterceptors>,
    poller_registry: PollerRegistry,
    pending_queries: PendingQueryStore,
    buffered_queries: BufferedQueryRegistry,
    broker: InMemoryBroker,
    nexus_broker: NexusTaskBroker,
    nexus_http_waiters: NexusHttpWaiterRegistry,
    long_polls: LongPollGate,
    router: Arc<dyn EdgeRouter>,
    history_waiters: HistoryWaitRegistry,
    worker_registry: WorkerRegistry,
    heartbeat_store: Arc<dyn HeartbeatStore>,
    schedule_store: Arc<ScheduleStore>,
    task_queue_config_store: Arc<dyn TaskQueueConfigStore>,
    batch_store: Arc<BatchOperationStore>,
    workflow_rules: WorkflowRuleStore,
    eager_dispatch_config: EagerDispatchConfig,
    task_queue_rate_limiter: TaskQueueRateLimiter,
}

const MAX_NEXUS_OPERATION_TOKEN_LENGTH: usize = 4096;

fn validate_nexus_task_token(context: &EdgeContext, token: &NexusTaskToken) -> EdgeResult<()> {
    if token.task_queue.is_empty() || token.task_id.is_empty() {
        return Err(EdgeError::BadRequest("Invalid TaskToken.".to_string()));
    }
    if token.namespace_id.is_empty() {
        return Err(EdgeError::BadRequest(
            "Namespace not set on request.".to_string(),
        ));
    }

    let namespace_id = Uuid::parse_str(&token.namespace_id)
        .map(tokeira_types::NamespaceId)
        .map_err(|_| EdgeError::BadRequest("Invalid TaskToken.".to_owned()))?;
    crate::interceptors::validate_task_token_namespace(context, Some(namespace_id))
}

fn decode_nexus_task_token(task_token: &[u8]) -> EdgeResult<NexusTaskToken> {
    if task_token.is_empty() {
        return Err(EdgeError::BadRequest(
            "Task token not set on request.".to_owned(),
        ));
    }
    NexusTaskToken::decode(task_token)
        .map_err(|_| EdgeError::BadRequest("Error deserializing task token.".to_owned()))
}

fn query_task_namespace_id(task_token: &[u8]) -> EdgeResult<tokeira_types::NamespaceId> {
    let token = std::str::from_utf8(task_token)
        .map_err(|_| EdgeError::BadRequest("invalid task token".to_owned()))?;
    let namespace_id = token
        .strip_prefix("query-task:")
        .and_then(|remainder| remainder.split(':').next())
        .ok_or_else(|| EdgeError::BadRequest("invalid task token".to_owned()))?;
    Uuid::parse_str(namespace_id)
        .map(tokeira_types::NamespaceId)
        .map_err(|_| EdgeError::BadRequest("invalid task token".to_owned()))
}

fn validate_nexus_failure_details(details: &[u8]) -> EdgeResult<()> {
    if !details.is_empty() && serde_json::from_slice::<serde_json::Value>(details).is_err() {
        return Err(EdgeError::BadRequest(
            "failure details must be JSON serializable".to_string(),
        ));
    }
    Ok(())
}

#[allow(deprecated)]
fn validate_nexus_completed_response_token(
    response: &tokeira_proto::public::temporal::api::nexus::v1::Response,
) -> EdgeResult<()> {
    use tokeira_proto::public::temporal::api::nexus::v1::{response, start_operation_response};

    let Some(variant) = response.variant.as_ref() else {
        return Ok(());
    };
    let response::Variant::StartOperation(start) = variant else {
        return Ok(());
    };
    if let Some(start_operation_response::Variant::AsyncSuccess(success)) = start.variant.as_ref() {
        let operation_token = if success.operation_token.is_empty() {
            &success.operation_id
        } else {
            &success.operation_token
        };
        if operation_token.is_empty() {
            return Err(EdgeError::BadRequest(
                "missing opration token in response".to_string(),
            ));
        }
        if operation_token.len() > MAX_NEXUS_OPERATION_TOKEN_LENGTH {
            return Err(EdgeError::BadRequest(format!(
                "operation token length exceeds allowed limit ({}/{})",
                operation_token.len(),
                MAX_NEXUS_OPERATION_TOKEN_LENGTH
            )));
        }
    }
    Ok(())
}

#[allow(deprecated)]
fn validate_nexus_completed_response_failure_details(
    response: &tokeira_proto::public::temporal::api::nexus::v1::Response,
) -> EdgeResult<()> {
    use tokeira_proto::public::temporal::api::nexus::v1::{response, start_operation_response};

    let Some(response::Variant::StartOperation(start)) = response.variant.as_ref() else {
        return Ok(());
    };
    if let Some(start_operation_response::Variant::OperationError(error)) = start.variant.as_ref()
        && let Some(failure) = error.failure.as_ref()
    {
        validate_nexus_failure_details(&failure.details)?;
    }
    Ok(())
}

impl std::fmt::Debug for WorkflowService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowService").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EagerDispatchConfig {
    pub max_eager_activity_tasks_per_response: usize,
}

impl Default for EagerDispatchConfig {
    fn default() -> Self {
        Self {
            max_eager_activity_tasks_per_response: 3,
        }
    }
}

/// Process-local dispatch pacing for matching task queues.
///
/// Temporal matching applies the API queue rate limit ahead of a worker's
/// advertised rate (`task_queue_partition_manager.go @ v1.31.0`). The limiter
/// is deliberately ephemeral: it affects delivery timing only and never owns
/// task correctness.
#[derive(Clone, Debug, Default)]
struct TaskQueueRateLimiter {
    next_dispatch:
        Arc<tokio::sync::Mutex<HashMap<(tokeira_types::NamespaceId, TaskQueueName), Instant>>>,
}

impl TaskQueueRateLimiter {
    async fn acquire(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: TaskQueueName,
        requests_per_second: f64,
        max_wait: Duration,
    ) -> bool {
        if requests_per_second <= 0.0 {
            tokio::time::sleep(max_wait).await;
            return false;
        }
        let interval = Duration::from_secs_f64(1.0 / requests_per_second);
        let now = Instant::now();
        let wait = {
            let mut next_dispatch = self.next_dispatch.lock().await;
            let next = next_dispatch
                .entry((namespace_id, task_queue))
                .or_insert(now);
            let dispatch_at = (*next).max(now);
            *next = dispatch_at + interval;
            dispatch_at.saturating_duration_since(now)
        };
        if wait >= max_wait {
            tokio::time::sleep(max_wait).await;
            return false;
        }
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        true
    }
}

fn system_capabilities_with_matrix_overlay(
    mut capabilities: SystemCapabilities,
) -> SystemCapabilities {
    // TODO(temporal-compatibility): once the matrix covers all capabilities with
    // conformance evidence, remove the hardcoded baseline and derive entirely
    // from FEATURE_MATRIX. Until then, the matrix overlay only restricts
    // capabilities that are explicitly Stubbed/Unsupported AND were already false.
    for feature in FEATURE_MATRIX {
        for field in feature.capability_fields() {
            apply_matrix_capability_field(&mut capabilities, field, feature.state);
        }
    }
    capabilities
}

fn apply_matrix_capability_field(
    _capabilities: &mut SystemCapabilities,
    field: &str,
    state: FeatureState,
) {
    if !matches!(state, FeatureState::Stubbed | FeatureState::Unsupported) {
        return;
    }

    match field {
        "signal_and_query_header"
        | "internal_error_differentiation"
        | "activity_failure_include_heartbeat"
        | "supports_schedules"
        | "encoded_failure_attributes"
        | "build_id_based_versioning"
        | "upsert_memo"
        | "eager_workflow_start"
        | "sdk_metadata"
        | "count_group_by_execution_status"
        | "nexus"
        | "server_scaled_deployments"
        | "worker_heartbeats" => {}
        _ => {}
    }
}

impl WorkflowService {
    /// Admit a transport handler that cannot yet delegate through a domain DTO.
    ///
    /// Standalone-activity and AdminService adapters use this narrow bridge so
    /// they still pass through the same authn/authz ordering as every domain
    /// method. The returned context is held for the call lifetime, preventing a
    /// long poll from re-evaluating policy after admission.
    pub async fn admit_request(
        &self,
        headers: &HeaderMap,
        namespace: Option<&str>,
        action: Action,
        is_long_poll: bool,
    ) -> EdgeResult<EdgeContext> {
        self.interceptors
            .begin(headers, namespace, action, is_long_poll)
            .await
    }

    async fn observe_edge_call<T, F>(
        &self,
        headers: &HeaderMap,
        method: &'static str,
        namespace: Option<&str>,
        workflow_id: Option<&str>,
        fut: F,
    ) -> EdgeResult<T>
    where
        F: Future<Output = EdgeResult<T>>,
    {
        let _active = edge_metrics::track_grpc_active_request(method);
        let namespace = namespace.unwrap_or_default().to_string();
        let started = Instant::now();
        let result = tracing_interceptor::instrument_grpc_call(
            headers,
            method,
            if namespace.is_empty() {
                None
            } else {
                Some(namespace.as_str())
            },
            workflow_id,
            fut,
        )
        .await;
        let status = if result.is_ok() { "ok" } else { "error" };
        edge_metrics::record_grpc_request(method, &namespace, status);
        edge_metrics::record_grpc_request_duration(method, &namespace, started.elapsed());
        if let Err(error) = &result {
            edge_metrics::record_grpc_error(method, &namespace, grpc_error_code(error));
        }
        result
    }

    pub fn new(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self::new_with_stores_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            BufferedQueryRegistry::default(),
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_buffered_queries(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
    ) -> Self {
        Self::new_with_stores_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            HistoryWaitRegistry::default(),
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
    ) -> Self {
        Self::new_with_stores_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            BufferedQueryRegistry::default(),
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            history_waiters,
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    pub fn new_with_stores_and_buffered_queries_and_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        nexus_broker: NexusTaskBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
        worker_registry: WorkerRegistry,
        heartbeat_store: Arc<dyn HeartbeatStore>,
        schedule_store: Arc<ScheduleStore>,
        task_queue_config_store: Arc<dyn TaskQueueConfigStore>,
        batch_store: Arc<BatchOperationStore>,
    ) -> Self {
        let workflow_rules = WorkflowRuleStore::new(repo.clone());
        Self {
            runtime,
            worker_deployments: None,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            nexus_broker,
            nexus_http_waiters: NexusHttpWaiterRegistry::default(),
            long_polls,
            router,
            history_waiters,
            worker_registry,
            heartbeat_store,
            schedule_store,
            task_queue_config_store,
            batch_store,
            workflow_rules,
            eager_dispatch_config: EagerDispatchConfig::default(),
            task_queue_rate_limiter: TaskQueueRateLimiter::default(),
        }
    }

    pub fn with_eager_dispatch_config(
        mut self,
        eager_dispatch_config: EagerDispatchConfig,
    ) -> Self {
        self.eager_dispatch_config = eager_dispatch_config;
        self
    }

    /// Attach the edge-owned waiters used by caller-facing Nexus HTTP dispatch.
    ///
    /// The runtime broker retains only opaque waiter IDs; sharing this registry
    /// with the HTTP handler keeps public response types and caller lifetimes in
    /// the compatibility plane.
    pub fn with_nexus_http_waiters(mut self, waiters: NexusHttpWaiterRegistry) -> Self {
        self.nexus_http_waiters = waiters;
        self
    }

    /// Attach the runtime-backed Worker Deployment registry API.
    ///
    /// Most tests and legacy deployments do not configure v2 Worker Deployment storage. Keeping
    /// this as an explicit attachment prevents accidental calls from silently constructing an
    /// in-memory registry that would not match production durability.
    pub fn with_worker_deployment_runtime(
        mut self,
        runtime: Arc<dyn WorkerDeploymentRuntimeApi>,
    ) -> Self {
        self.worker_deployments = Some(runtime);
        self
    }

    pub fn worker_deployment_runtime(&self) -> EdgeResult<Arc<dyn WorkerDeploymentRuntimeApi>> {
        self.worker_deployments.clone().ok_or_else(|| {
            EdgeError::FailedPrecondition(
                "worker deployment registry is not configured for this service".to_string(),
            )
        })
    }

    pub fn worker_registry(&self) -> WorkerRegistry {
        self.worker_registry.clone()
    }

    pub fn heartbeat_store(&self) -> Arc<dyn HeartbeatStore> {
        self.heartbeat_store.clone()
    }

    pub fn with_heartbeat_store(mut self, heartbeat_store: Arc<dyn HeartbeatStore>) -> Self {
        self.heartbeat_store = heartbeat_store;
        self
    }

    pub fn schedule_store(&self) -> Arc<ScheduleStore> {
        self.schedule_store.clone()
    }

    pub fn task_queue_config_store(&self) -> Arc<dyn TaskQueueConfigStore> {
        self.task_queue_config_store.clone()
    }

    pub fn batch_store(&self) -> Arc<BatchOperationStore> {
        self.batch_store.clone()
    }

    /// Return the shared namespace workflow-rule registry.
    pub fn workflow_rule_store(&self) -> WorkflowRuleStore {
        self.workflow_rules.clone()
    }

    pub async fn resolve_namespace_id(
        &self,
        namespace: &str,
    ) -> EdgeResult<tokeira_types::NamespaceId> {
        match self
            .namespaces
            .get(namespace)
            .await
            .map_err(EdgeError::from)?
        {
            Some(resolved) if !resolved.deleted => {
                Ok(to_internal::namespace_id_for(&resolved.name))
            }
            Some(_) => Err(EdgeError::NamespaceDeleted(namespace.to_string())),
            None => Err(EdgeError::NamespaceNotFound(namespace.to_string())),
        }
    }

    /// Reject a Start request whose search attributes include any key not
    /// registered for the namespace (system predefined or custom). Returns the
    /// verbatim v1.31.0 admission error for the first unknown key
    /// (`InvalidArgument "search attribute <key> is not defined"`,
    /// `common/searchattribute/validator.go:101 @ v1.31.0`;
    /// `standalone_activity_test.go:521`). A no-op when there are no keys or the
    /// deployment has no search-attribute registry (permissive default).
    pub async fn validate_search_attribute_keys(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        keys: &[String],
    ) -> EdgeResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        if let Some(unknown) = self
            .visibility
            .unknown_search_attribute(namespace_id, keys)
            .await
            .map_err(EdgeError::from)?
        {
            return Err(EdgeError::BadRequest(format!(
                "search attribute {unknown} is not defined"
            )));
        }
        Ok(())
    }

    async fn admit_json_task_token<T: DeserializeOwned>(
        &self,
        headers: &HeaderMap,
        request_namespace: &str,
        task_token: &[u8],
        action: Action,
    ) -> EdgeResult<(T, Option<tokeira_types::NamespaceId>, EdgeContext, String)> {
        if request_namespace.is_empty() {
            // The namespace validator back-fills before authentication in
            // v1.31.0, so malformed bytes on this branch are intentionally
            // observable before any authorization decision.
            let (token, token_namespace_id) = crate::task_token::decode(task_token)
                .map_err(|error| EdgeError::BadRequest(format!("invalid task token: {error}")))?;
            let (context, namespace) = self
                .interceptors
                .begin_with_task_token_backfill(headers, token_namespace_id, action)
                .await?;
            Ok((token, token_namespace_id, context, namespace))
        } else {
            // The explicit request namespace is the authorization target. Only
            // an allowed caller may learn that its token is malformed or names
            // a different namespace (`fx.go:256-290 @ v1.31.0`).
            let context = self
                .interceptors
                .begin(headers, Some(request_namespace), action, false)
                .await?;
            let (token, token_namespace_id) = crate::task_token::decode(task_token)
                .map_err(|error| EdgeError::BadRequest(format!("invalid task token: {error}")))?;
            crate::interceptors::validate_task_token_namespace(&context, token_namespace_id)?;
            Ok((
                token,
                token_namespace_id,
                context,
                request_namespace.to_owned(),
            ))
        }
    }

    async fn validate_legacy_task_namespace(
        &self,
        effective_namespace: &str,
        token_namespace_id: Option<tokeira_types::NamespaceId>,
        run_key: RunKey,
    ) -> EdgeResult<()> {
        if token_namespace_id.is_none()
            && !effective_namespace.is_empty()
            && let LoadedRun::Existing(state) =
                self.repo.load_run(run_key).await.map_err(EdgeError::from)?
        {
            validate_authoritative_task_namespace(effective_namespace, state.namespace_id)?;
        }
        Ok(())
    }

    async fn admit_nexus_task_token(
        &self,
        headers: &HeaderMap,
        request_namespace: &str,
        task_token: &[u8],
        action: Action,
    ) -> EdgeResult<(NexusTaskToken, EdgeContext, String)> {
        if request_namespace.is_empty() {
            let token = decode_nexus_task_token(task_token)?;
            let namespace_id = Uuid::parse_str(&token.namespace_id)
                .map(tokeira_types::NamespaceId)
                .map_err(|_| EdgeError::BadRequest("Invalid TaskToken.".to_owned()))?;
            let (context, namespace) = self
                .interceptors
                .begin_with_task_token_backfill(headers, Some(namespace_id), action)
                .await?;
            validate_nexus_task_token(&context, &token)?;
            Ok((token, context, namespace))
        } else {
            let context = self
                .interceptors
                .begin(headers, Some(request_namespace), action, false)
                .await?;
            let token = decode_nexus_task_token(task_token)?;
            validate_nexus_task_token(&context, &token)?;
            Ok((token, context, request_namespace.to_owned()))
        }
    }

    pub async fn poll_nexus_task_queue(
        &self,
        headers: &HeaderMap,
        req: crate::translate::nexus::PollNexusTaskQueueRequest,
    ) -> EdgeResult<Option<crate::translate::nexus::PollNexusTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_nexus_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollNexusTaskQueue,
                        true,
                    )
                    .await?;
                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Workflow)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let (namespace_id, task_queue) =
                    crate::translate::nexus::broker_queue(&req.namespace, &req.task_queue);
                let task = self
                    .nexus_broker
                    .poll(
                        namespace_id,
                        task_queue.clone(),
                        std::time::Duration::from_secs(60),
                    )
                    .await;

                match task {
                    Some(task) => {
                        // A Nexus task handed to a worker — the dispatch equivalent of
                        // v1.31.0's matching `nexus_task_requests`.
                        crate::metrics::record_nexus_task_request(&req.namespace, "dispatched");
                        Ok(Some(crate::translate::nexus::PollNexusTaskQueueResponse {
                            task_token: task.token.encode().map_err(EdgeError::from)?,
                            request: task.request,
                            poller_scaling_decision: self
                                .nexus_broker
                                .has_runnable_backlog(namespace_id, &task_queue)
                                .await
                                .then_some(1),
                        }))
                    }
                    None => {
                        crate::metrics::record_nexus_task_request(&req.namespace, "timeout");
                        Ok(None)
                    }
                }
            },
        )
        .await
    }

    pub async fn respond_nexus_task_completed(
        &self,
        headers: &HeaderMap,
        mut req: crate::translate::nexus::RespondNexusTaskCompletedRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_nexus_task_completed",
            Some(namespace_label.as_str()),
            None,
            async move {
                let response = req.response.unwrap_or_default();
                let token = if req.namespace.is_empty() {
                    // Namespace back-fill is an earlier interceptor than auth;
                    // decoding therefore precedes handler-level operation-token
                    // validation on the omitted-name branch.
                    let (token, _ctx, effective_namespace) = self
                        .admit_nexus_task_token(
                            headers,
                            &req.namespace,
                            &req.task_token,
                            Action::RespondNexusTaskCompleted,
                        )
                        .await?;
                    req.namespace = effective_namespace;
                    validate_nexus_completed_response_token(&response)?;
                    token
                } else {
                    let context = self
                        .interceptors
                        .begin(
                            headers,
                            Some(&req.namespace),
                            Action::RespondNexusTaskCompleted,
                            false,
                        )
                        .await?;
                    // v1.31.0's handler checks the async operation token before
                    // deserializing the worker task token
                    // (`workflow_handler.go:6035-6058 @ v1.31.0`).
                    validate_nexus_completed_response_token(&response)?;
                    let token = decode_nexus_task_token(&req.task_token)?;
                    validate_nexus_task_token(&context, &token)?;
                    token
                };
                // v1.31.0 validates operation-error JSON only after the task token
                // has been decoded and namespace-fenced, but before consuming its
                // delivery correlation (`workflow_handler.go:6058-6077 @ v1.31.0`).
                validate_nexus_completed_response_failure_details(&response)?;
                let correlation =
                    self.nexus_broker
                        .consume(&token.task_id)
                        .await
                        .ok_or_else(|| {
                            EdgeError::NotFound(
                                "Nexus task not found or already expired".to_string(),
                            )
                        })?;
                let (run_key, operation_id, scheduled_event_id, task_kind) = match correlation {
                    NexusTaskCorrelation::Http { waiter_id } => {
                        // Consuming the delivery correlation acknowledges the worker.
                        // A concurrent caller disconnect may remove the edge waiter after
                        // that point without turning the worker response into an RPC error,
                        // which preserves RespondNexusTaskCompleted's v1.31.0 behavior.
                        let _ = self
                            .nexus_http_waiters
                            .complete(&waiter_id, NexusHttpWorkerOutcome::Completed(response));
                        return Ok(());
                    }
                    NexusTaskCorrelation::Workflow {
                        run_key,
                        operation_id,
                        scheduled_event_id,
                        task_kind,
                    } => (run_key, operation_id, scheduled_event_id, task_kind),
                };

                // Workflow-originated worker tasks are outbound Nexus attempts. HTTP
                // dispatches above are caller-facing requests and are measured by the
                // Nexus HTTP handler instead.
                if let Some(tags) =
                    crate::translate::nexus::nexus_completed_outbound_tags(&response)
                {
                    tokeira_runtime::metrics::record_nexus_outbound_request(
                        &req.namespace,
                        tags.method,
                        tags.failure_source,
                        &tags.outcome,
                    );
                }

                if task_kind == NexusWorkflowTaskKind::CancelOperation {
                    if !matches!(
                        response.variant.as_ref(),
                        Some(tokeira_proto::public::temporal::api::nexus::v1::response::Variant::CancelOperation(_))
                    ) {
                        return Err(EdgeError::BadRequest(
                            "cancel operation task requires a cancel operation response".to_string(),
                        ));
                    }
                    let pending = match self
                        .repo
                        .load_run(run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        LoadedRun::Existing(state) => {
                            state.pending_nexus_operations.get(&operation_id).cloned()
                        }
                        LoadedRun::Absent => None,
                    };
                    let Some(pending) = pending else {
                        return Ok(());
                    };
                    let Some(cancellation) = pending.cancellation.as_ref() else {
                        return Ok(());
                    };
                    let applied = self
                        .runtime
                        .record_nexus_cancellation_attempt(
                            run_key,
                            operation_id,
                            scheduled_event_id,
                            cancellation.requested_event_id,
                            NexusCancellationAttemptOutcome::Succeeded,
                        )
                        .await
                        .map_err(EdgeError::from)?;
                    if applied {
                        self.notify_history_run_key(
                            run_key,
                            read_last_event_id(self.repo.as_ref(), run_key).await?,
                        )
                        .await;
                    }
                    return Ok(());
                }

                // Load the pending op so an operation-unsuccessful response can be wrapped
                // in NexusOperationFailureInfo (endpoint/service/operation), exactly as the
                // worker handler-error path does. A missing/raced pending op leaves them
                // empty — the inner cause chain the SDK decodes is still intact.
                let op_ctx = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                    LoadedRun::Existing(state) => state
                        .pending_nexus_operations
                        .get(&operation_id)
                        .map(|op| crate::translate::nexus::NexusOperationContext {
                            endpoint: op.endpoint.clone(),
                            service: op.service.clone(),
                            operation: op.operation.clone(),
                            scheduled_event_id,
                        })
                        .unwrap_or_default(),
                    LoadedRun::Absent => Default::default(),
                };
                let resolution = match crate::translate::nexus::proto_response_to_resolution(
                    response, &op_ctx,
                ) {
                    Ok(resolution) => resolution,
                    // RespondNexusTaskCompleted in v1.31.0 accepts a response with no
                    // variant and delegates it to the waiting consumer. Tokeira has no
                    // separate history consumer to reject that outcome, so acknowledge
                    // the worker while leaving the authoritative operation pending.
                    Err(crate::translate::nexus::NexusTranslateError::MissingField(_)) => {
                        return Ok(());
                    }
                    Err(error) => return Err(EdgeError::BadRequest(error.to_string())),
                };

                // A cancel-ack (None) does not resolve the operation — the operation resolves
                // only via its completion when the backing workflow closes (v1.31.0 decouples
                // EventCancelationSucceeded from operation resolution, statemachine.go:671).
                let Some(resolution) = resolution else {
                    return Ok(());
                };

                let applied = self
                    .runtime
                    .resolve_nexus_operation(run_key, operation_id, scheduled_event_id, resolution)
                    .await
                    .map_err(EdgeError::from)?;
                if applied {
                    self.notify_history_run_key(
                        run_key,
                        read_last_event_id(self.repo.as_ref(), run_key).await?,
                    )
                    .await;
                }

                Ok(())
            },
        )
        .await
    }

    pub async fn respond_nexus_task_failed(
        &self,
        headers: &HeaderMap,
        mut req: crate::translate::nexus::RespondNexusTaskFailedRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_nexus_task_failed",
            Some(namespace_label.as_str()),
            None,
            async move {
                let (token, _ctx, effective_namespace) = self
                    .admit_nexus_task_token(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RespondNexusTaskFailed,
                    )
                    .await?;
                req.namespace = effective_namespace;
                if req.error.is_none() && req.failure.is_none() {
                    return Err(EdgeError::BadRequest(
                        "request must contain error or failure".to_string(),
                    ));
                }
                if let Some(error) = req.error.as_ref()
                    && let Some(failure) = error.failure.as_ref()
                {
                    validate_nexus_failure_details(&failure.details)?;
                }
                if let Some(failure) = req.failure.as_ref()
                    && !crate::translate::nexus::failure_has_nexus_handler_info(failure)
                {
                    return Err(EdgeError::BadRequest(
                        "request Failure must contain error or failure with NexusHandlerFailureInfo"
                            .to_string(),
                    ));
                }
                let correlation = self
                    .nexus_broker
                    .consume(&token.task_id)
                    .await
                    .ok_or_else(|| {
                        EdgeError::NotFound(
                            "Nexus task not found or already expired".to_string(),
                        )
                    })?;
                let (run_key, operation_id, scheduled_event_id, task_kind) = match correlation {
                    NexusTaskCorrelation::Http { waiter_id } => {
                        let _ = self.nexus_http_waiters.complete(
                            &waiter_id,
                            NexusHttpWorkerOutcome::Failed {
                                error: req.error,
                                failure: req.failure,
                            },
                        );
                        return Ok(());
                    }
                    NexusTaskCorrelation::Workflow {
                        run_key,
                        operation_id,
                        scheduled_event_id,
                        task_kind,
                    } => (run_key, operation_id, scheduled_event_id, task_kind),
                };
                // A failed worker response is the terminal result of an outbound
                // StartOperation (a worker-reported handler error); capture its
                // `nexus_outbound_requests` outcome before the failure is consumed into the
                // resolution below.
                let outbound_tags = crate::translate::nexus::nexus_failed_outbound_tags(
                    req.failure.as_ref(),
                    req.error.as_ref(),
                );
                if task_kind == NexusWorkflowTaskKind::CancelOperation {
                    let pending = match self
                        .repo
                        .load_run(run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        LoadedRun::Existing(state) => {
                            state.pending_nexus_operations.get(&operation_id).cloned()
                        }
                        LoadedRun::Absent => None,
                    };
                    let Some(pending) = pending else {
                        return Ok(());
                    };
                    let Some(cancellation) = pending.cancellation.as_ref() else {
                        return Ok(());
                    };
                    let failure = if let Some(failure) = req.failure {
                        failure
                    } else {
                        crate::translate::nexus::legacy_handler_error_to_failure(
                            req.error.ok_or_else(|| {
                                EdgeError::BadRequest(
                                    "request must contain error or failure".to_string(),
                                )
                            })?,
                        )
                    };
                    let retryable =
                        crate::translate::nexus::nexus_handler_failure_retryable(&failure);
                    let outcome = worker_cancellation_failure_outcome(
                        &pending,
                        failure,
                        retryable,
                        OffsetDateTime::now_utc(),
                    );
                    tokeira_runtime::metrics::record_nexus_outbound_request(
                        &req.namespace,
                        "CancelOperation",
                        outbound_tags.failure_source,
                        &outbound_tags.outcome,
                    );
                    let applied = self
                        .runtime
                        .record_nexus_cancellation_attempt(
                            run_key,
                            operation_id,
                            scheduled_event_id,
                            cancellation.requested_event_id,
                            outcome,
                        )
                        .await
                        .map_err(EdgeError::from)?;
                    if applied {
                        self.notify_history_run_key(
                            run_key,
                            read_last_event_id(self.repo.as_ref(), run_key).await?,
                        )
                        .await;
                    }
                    return Ok(());
                }
                // Prefer the v1.62 structured `failure` (field 5) modern SDKs send;
                // fall back to the deprecated `error` (field 4). v1.31.0 requires
                // one of them, and a `failure` must carry a NexusHandlerFailureInfo
                // (`workflow_handler.go:6096 @ v1.31.0`).
                let resolution = if let Some(failure) = req.failure {
                    // Load the pending op once: it supplies both the NexusOperationFailureInfo
                    // wrap context (endpoint/service/operation) for a terminal failure AND the
                    // backoff inputs (attempt/scheduled_at/schedule-to-close) for a retryable
                    // one. A missing pending op (already resolved/raced) forces a terminal
                    // resolution — there is nothing to back off.
                    let pending = match self
                        .repo
                        .load_run(run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        LoadedRun::Existing(state) => {
                            state.pending_nexus_operations.get(&operation_id).cloned()
                        }
                        LoadedRun::Absent => None,
                    };
                    // v1.31.0 (`components/nexusoperations/executors.go:499-532`): a *retryable*
                    // handler error backs the operation off (BACKING_OFF) — it stays pending
                    // with the failure on LastAttemptFailure — while a non-retryable one (or a
                    // retry past schedule-to-close) fails the operation terminally.
                    let next_attempt_at = if crate::translate::nexus::nexus_handler_failure_retryable(
                        &failure,
                    ) {
                        pending.as_ref().and_then(|op| {
                            tokeira_runtime::nexus::nexus_operation_next_attempt_at(
                                op.attempt,
                                op.scheduled_at,
                                op.schedule_to_close_timeout,
                                OffsetDateTime::now_utc(),
                            )
                        })
                    } else {
                        None
                    };
                    match next_attempt_at {
                        Some(next_attempt_at) => NexusResolution::AttemptFailed {
                            // LastAttemptFailure is the handler's own failure (the Describe
                            // surface), NOT the terminal NexusOperationFailureInfo wrapper.
                            failure: tokeira_proto::conversions::common::failure_to_payload(
                                &failure,
                            ),
                            next_attempt_at,
                        },
                        None => {
                            let (endpoint, service, operation) = pending
                                .map(|op| (op.endpoint, op.service, op.operation))
                                .unwrap_or_default();
                            crate::translate::nexus::wrap_handler_failure_as_resolution(
                                failure,
                                endpoint,
                                service,
                                operation,
                                scheduled_event_id,
                            )
                        }
                    }
                } else {
                    let error = req.error.ok_or_else(|| {
                        EdgeError::BadRequest(
                            "request Failure must contain error or failure with NexusHandlerFailureInfo"
                                .to_string(),
                        )
                    })?;
                    crate::translate::nexus::proto_handler_error_to_resolution(error)
                        .map_err(|error| EdgeError::BadRequest(error.to_string()))?
                };
                tokeira_runtime::metrics::record_nexus_outbound_request(
                    &req.namespace,
                    outbound_tags.method,
                    outbound_tags.failure_source,
                    &outbound_tags.outcome,
                );

                let applied = self
                    .runtime
                    .resolve_nexus_operation(
                        run_key,
                        operation_id,
                        scheduled_event_id,
                        resolution,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                if applied {
                    self.notify_history_run_key(
                        run_key,
                        read_last_event_id(self.repo.as_ref(), run_key).await?,
                    )
                    .await;
                }

                Ok(())
            },
        )
        .await
    }

    /// Create one namespace workflow rule after authorization.
    pub async fn create_workflow_rule(
        &self,
        headers: &HeaderMap,
        namespace: String,
        spec: WorkflowRuleSpec,
        identity: String,
        description: String,
    ) -> EdgeResult<WorkflowRule> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "create_workflow_rule",
            Some(namespace_label.as_str()),
            None,
            async move {
                self.interceptors
                    .begin(headers, Some(&namespace), Action::WorkflowRulesWrite, false)
                    .await?;
                self.workflow_rules
                    .create(
                        to_internal::namespace_id_for(&namespace),
                        spec,
                        identity,
                        description,
                        OffsetDateTime::now_utc(),
                    )
                    .await
                    .map_err(workflow_rule_error_to_edge)
            },
        )
        .await
    }

    /// Read one namespace workflow rule after authorization.
    pub async fn describe_workflow_rule(
        &self,
        headers: &HeaderMap,
        namespace: String,
        rule_id: String,
    ) -> EdgeResult<WorkflowRule> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_workflow_rule",
            Some(namespace_label.as_str()),
            None,
            async move {
                self.interceptors
                    .begin(headers, Some(&namespace), Action::WorkflowRulesRead, false)
                    .await?;
                self.workflow_rules
                    .describe(to_internal::namespace_id_for(&namespace), &rule_id)
                    .await
                    .map_err(workflow_rule_error_to_edge)
            },
        )
        .await
    }

    /// Delete one namespace workflow rule after authorization.
    pub async fn delete_workflow_rule(
        &self,
        headers: &HeaderMap,
        namespace: String,
        rule_id: String,
    ) -> EdgeResult<()> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "delete_workflow_rule",
            Some(namespace_label.as_str()),
            None,
            async move {
                self.interceptors
                    .begin(headers, Some(&namespace), Action::WorkflowRulesWrite, false)
                    .await?;
                self.workflow_rules
                    .delete(to_internal::namespace_id_for(&namespace), &rule_id)
                    .await
                    .map_err(workflow_rule_error_to_edge)
            },
        )
        .await
    }

    /// List active namespace workflow rules after authorization.
    pub async fn list_workflow_rules(
        &self,
        headers: &HeaderMap,
        namespace: String,
    ) -> EdgeResult<Vec<WorkflowRule>> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "list_workflow_rules",
            Some(namespace_label.as_str()),
            None,
            async move {
                self.interceptors
                    .begin(headers, Some(&namespace), Action::WorkflowRulesRead, false)
                    .await?;
                self.workflow_rules
                    .list(to_internal::namespace_id_for(&namespace))
                    .await
                    .map_err(workflow_rule_error_to_edge)
            },
        )
        .await
    }

    pub async fn start_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::StartBatchOperationRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "start_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::StartBatchOperation,
                        false,
                    )
                    .await?;
                let namespace_id = to_internal::namespace_id_for(&req.namespace);
                let identity = if req.operation_params.identity().trim().is_empty() {
                    ctx.claims
                        .as_ref()
                        .map(|claims| claims.subject.clone())
                        // The permissive adapter intentionally has no claims,
                        // but pre-auth Tokeira recorded its synthetic `root`
                        // principal as the batch identity. Preserve that
                        // stock-default wire behavior without manufacturing a
                        // durable attribution principal.
                        .unwrap_or_else(|| "root".to_owned())
                } else {
                    req.operation_params.identity().to_string()
                };
                let cancellation_token = tokio_util::sync::CancellationToken::new();
                let entry = BatchOperationEntry {
                    job_id: req.job_id.clone(),
                    namespace_id,
                    operation_type: req.operation_type,
                    operation_params: req.operation_params,
                    state: tokeira_runtime::BatchOperationState::Running,
                    start_time: OffsetDateTime::now_utc(),
                    close_time: None,
                    counters: Arc::new(BatchProgressCounters::default()),
                    visibility_query: req.visibility_query,
                    executions: req.executions,
                    reason: req.reason,
                    identity: identity.clone(),
                    max_operations_per_second: req.max_operations_per_second,
                    cancellation_token: cancellation_token.clone(),
                    stop_reason: None,
                    stop_identity: None,
                };
                self.batch_store
                    .create(entry)
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))?;

                let dispatch_ctx = BatchDispatchContext {
                    namespace_id,
                    namespace_name: req.namespace,
                    identity,
                    edge_context: ctx,
                };
                tokio::spawn(run_batch_operation(
                    self.batch_store.clone(),
                    self.clone(),
                    dispatch_ctx,
                    namespace_id,
                    req.job_id,
                    cancellation_token,
                ));
                Ok(())
            },
        )
        .await
    }

    pub async fn stop_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::StopBatchOperationRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "stop_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::StopBatchOperation,
                        false,
                    )
                    .await?;
                self.batch_store
                    .stop(
                        to_internal::namespace_id_for(&req.namespace),
                        &req.job_id,
                        req.reason,
                        req.identity,
                    )
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))
            },
        )
        .await
    }

    pub async fn describe_batch_operation(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::DescribeBatchOperationRequest,
    ) -> EdgeResult<tokeira_runtime::BatchOperationSnapshot> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_batch_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeBatchOperation,
                        false,
                    )
                    .await?;
                self.batch_store
                    .describe(to_internal::namespace_id_for(&req.namespace), &req.job_id)
                    .map_err(|err| batch_error_to_edge(err, &req.namespace, &req.job_id))
            },
        )
        .await
    }

    pub async fn list_batch_operations(
        &self,
        headers: &HeaderMap,
        req: crate::translate::batch::ListBatchOperationsRequest,
    ) -> EdgeResult<(Vec<tokeira_runtime::BatchOperationInfo>, Option<Vec<u8>>)> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_batch_operations",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListBatchOperations,
                        false,
                    )
                    .await?;
                Ok(self.batch_store.list(
                    to_internal::namespace_id_for(&req.namespace),
                    req.page_size,
                    &req.next_page_token,
                ))
            },
        )
        .await
    }

    pub(crate) async fn list_workflows_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        query: Option<String>,
        next_page_token: Option<String>,
    ) -> EdgeResult<ListWorkflowExecutionsResponse> {
        self.visibility
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: ctx.namespace_name.clone(),
                query,
                page_size: 100,
                next_page_token,
            })
            .await
            .map_err(EdgeError::from)
    }

    pub(crate) async fn terminate_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        details: Option<Payloads>,
        identity: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .terminate_workflow(
                run_key,
                TerminateRequest {
                    reason: "batch terminate".to_string(),
                    details,
                    identity,
                    links: Vec::new(),
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn cancel_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .cancel_workflow(
                run_key,
                CancelRequest {
                    reason: "batch cancel".to_string(),
                    external_initiator: None,
                    external_initiated_event_id: 0,
                    links: Vec::new(),
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn signal_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        signal_name: String,
        input: Payloads,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let outcome = self
            .runtime
            .signal_workflow(
                run_key,
                SignalRequest {
                    signal_name,
                    input,
                    header: None,
                    links: Vec::new(),
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn delete_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        _identity: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let deletion = self
            .runtime
            .delete_workflow(
                run_key,
                DeleteWorkflowRequest {
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(|error| {
                map_workflow_deletion_error(error, &ctx.namespace_name, &workflow_ref.workflow_id)
            })?;
        self.visibility
            .apply_deletion(deletion.tombstone)
            .await
            .map_err(EdgeError::from)
    }

    pub(crate) async fn reset_workflow_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        fork_event_id: i64,
        reason: String,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let execution = self.execution_ref_from_batch(ctx, workflow_ref)?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let history = self
            .repo
            .read_history(run_key, 0, usize::MAX)
            .await
            .map_err(EdgeError::from)?;
        validate_reset_target(&history, fork_event_id)?;
        let new_run_id = RunId::new();
        let result = self
            .runtime
            .reset_workflow(
                execution,
                ResetRequest {
                    fork_event_id,
                    new_run_id,
                    // Batch reset does not model reapply exclusion yet (UNSUPPORTED_FIELDS).
                    reapply_exclude_signal: false,
                    reapply_exclude_update: false,
                    reason,
                    request: batch_request_context(ctx),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(result.successor_run_key, 0)
            .await;
        Ok(())
    }

    pub(crate) async fn unpause_activity_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        target: tokeira_kernel::ActivityControlTarget,
        reset_attempts: bool,
        reset_heartbeat: bool,
        jitter: Option<time::Duration>,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        validate_activity_jitter(jitter)?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let now = OffsetDateTime::now_utc();
        let outcome = self
            .runtime
            .unpause_activities(
                run_key,
                tokeira_runtime::UnpauseActivitiesRequest {
                    target,
                    reset_attempts,
                    reset_heartbeat,
                    jitter,
                    request: batch_request_context(ctx),
                    now,
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn update_activity_options_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        patch: BatchActivityOptionsPatch,
    ) -> EdgeResult<()> {
        ensure_local(
            self.router
                .route_workflow(&ctx.namespace_name, &workflow_ref.workflow_id)
                .await?,
        )?;
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let now = OffsetDateTime::now_utc();
        let outcome = self
            .runtime
            .update_activity_options(
                run_key,
                UpdateActivitiesOptionsRequest {
                    target: patch.target,
                    task_queue: patch.task_queue,
                    schedule_to_close_timeout: patch.schedule_to_close_timeout,
                    schedule_to_start_timeout: patch.schedule_to_start_timeout,
                    start_to_close_timeout: patch.start_to_close_timeout,
                    heartbeat_timeout: patch.heartbeat_timeout,
                    retry_policy: patch.retry_policy,
                    restore_original_options: patch.restore_original_options,
                    request: batch_request_context(ctx),
                    now,
                },
            )
            .await
            .map_err(EdgeError::from)?;
        self.notify_history_run_key(run_key, outcome.last_event_id)
            .await;
        Ok(())
    }

    pub(crate) async fn resolve_reset_target_batch_internal(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
        target: &BatchResetTarget,
    ) -> EdgeResult<i64> {
        let run_key = self.resolve_batch_run_key(ctx, workflow_ref).await?;
        let history = self
            .repo
            .read_history(run_key, 0, usize::MAX)
            .await
            .map_err(EdgeError::from)?;
        if let BatchResetTarget::BuildId(build_id) = target {
            let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
            let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: ctx.namespace_name.clone(),
                    workflow_id: workflow_ref.workflow_id.clone(),
                });
            };
            if state
                .build_id
                .as_ref()
                .is_none_or(|value| value.0 != *build_id)
            {
                return Err(EdgeError::BadRequest(format!(
                    "workflow was not processed by build id `{build_id}`"
                )));
            }
            return resolve_reset_target_from_history(
                &history,
                &BatchResetTarget::FirstWorkflowTask,
            );
        }
        resolve_reset_target_from_history(&history, target)
    }

    pub async fn apply_schedule_patch(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        patch: SchedulePatch,
    ) -> EdgeResult<()> {
        let now = OffsetDateTime::now_utc();
        self.schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;

        self.schedule_store
            .update(namespace_id, schedule_id, &[], |entry| {
                if let Some(note) = patch.pause.clone() {
                    entry.state.paused = true;
                    entry.state.notes = note;
                }
                if let Some(note) = patch.unpause.clone() {
                    entry.state.paused = false;
                    entry.state.notes = note;
                }
            })
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;

        if let Some(trigger) = patch.trigger_immediately {
            self.handle_schedule_due_action(
                namespace_id,
                schedule_id,
                now,
                Some(trigger.overlap_policy),
                now,
            )
            .await?;
        }
        for backfill in patch.backfill_request {
            let entry = self
                .schedule_store
                .describe(namespace_id, schedule_id)
                .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
            let times = compute_matching_times(
                &entry.spec,
                backfill.start_time,
                backfill.end_time,
                schedule_id,
            );
            for nominal_time in times {
                self.handle_schedule_due_action(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    Some(backfill.overlap_policy),
                    now,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn handle_schedule_due_action(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        nominal_time: OffsetDateTime,
        overlap_override: Option<OverlapPolicy>,
        actual_time: OffsetDateTime,
    ) -> EdgeResult<()> {
        let entry = self
            .schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        let policy = overlap_override.unwrap_or(entry.policies.overlap_policy);
        match decide_overlap(
            policy,
            &entry.info.running_workflows,
            entry.info.buffered_actions.len(),
        ) {
            OverlapDecision::Allow => {
                self.trigger_scheduled_workflow(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    actual_time,
                )
                .await
            }
            OverlapDecision::Skip => {
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        entry.info.overlap_skipped += 1;
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                Ok(())
            }
            OverlapDecision::Buffer => {
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        if policy == OverlapPolicy::BufferOne
                            && !entry.info.buffered_actions.is_empty()
                        {
                            entry.info.buffered_actions.pop_front();
                            entry.info.buffer_dropped += 1;
                        }
                        entry
                            .info
                            .buffered_actions
                            .push_back(tokeira_runtime::BufferedAction {
                                nominal_time,
                                overlap_policy_override: overlap_override,
                            });
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                Ok(())
            }
            OverlapDecision::CancelOther(workflows) => {
                for workflow in workflows {
                    self.runtime
                        .cancel_workflow(
                            workflow.run_key,
                            CancelRequest {
                                reason: "schedule overlap policy".to_string(),
                                external_initiator: None,
                                external_initiated_event_id: 0,
                                links: Vec::new(),
                                request: schedule_request_context(actual_time),
                                now: actual_time,
                            },
                        )
                        .await
                        .map_err(EdgeError::from)?;
                }
                Ok(())
            }
            OverlapDecision::TerminateOther(workflows) => {
                for workflow in workflows {
                    self.runtime
                        .terminate_workflow(
                            workflow.run_key,
                            TerminateRequest {
                                reason: "schedule overlap policy".to_string(),
                                details: Some(Payloads::default()),
                                identity: "schedule-engine".to_string(),
                                links: Vec::new(),
                                request: schedule_request_context(actual_time),
                                now: actual_time,
                            },
                        )
                        .await
                        .map_err(EdgeError::from)?;
                }
                self.schedule_store
                    .update(namespace_id, schedule_id, &[], |entry| {
                        entry.info.running_workflows.clear();
                    })
                    .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
                self.trigger_scheduled_workflow(
                    namespace_id,
                    schedule_id,
                    nominal_time,
                    actual_time,
                )
                .await
            }
        }
    }

    async fn trigger_scheduled_workflow(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        schedule_id: &tokeira_runtime::ScheduleId,
        nominal_time: OffsetDateTime,
        actual_time: OffsetDateTime,
    ) -> EdgeResult<()> {
        let entry = self
            .schedule_store
            .describe(namespace_id, schedule_id)
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        let workflow_id = schedule_workflow_id(
            &entry.action.start_workflow.workflow_id,
            nominal_time,
            entry.policies.keep_original_workflow_id,
        );
        let run_id = RunId::new();
        let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
        let request = StartRequest {
            run_key,
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id,
            workflow_type: entry.action.start_workflow.workflow_type.clone(),
            task_queue: entry.action.start_workflow.task_queue.clone(),
            input: entry.action.start_workflow.input.clone(),
            header: entry.action.start_workflow.header.clone(),
            memo: entry.action.start_workflow.memo.clone(),
            search_attributes: scheduled_workflow_search_attributes(
                &entry.action.start_workflow.search_attributes,
                schedule_id,
                nominal_time,
            ),
            workflow_execution_timeout: entry.action.start_workflow.workflow_execution_timeout,
            workflow_run_timeout: entry.action.start_workflow.workflow_run_timeout,
            workflow_task_timeout: entry
                .action
                .start_workflow
                .workflow_task_timeout
                .unwrap_or(time::Duration::seconds(10)),
            retry_policy: entry.action.start_workflow.retry_policy.clone(),
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            // A Schedule action starts a fresh execution (Initiator UNSPECIFIED).
            initiator: None,
            deployment: None,
            build_id: None,
            versioning_override: None,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: entry.action.start_workflow.user_metadata.clone(),
            links: Vec::new(),
            on_conflict_options: None,
            priority: None,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            original_execution_run_id: Some(run_id),
            continued_failure: entry.continued_failure.clone(),
            last_completion_result: entry.last_completion_result.clone(),
            first_run_started_at: None,
            request: schedule_request_context(actual_time),
            now: actual_time,
            client_cron_schedule: None,
            // Schedule actions are ordinary starts, not Workflow Cron starts
            // (`service/worker/scheduler/workflow.go @ v1.31.0`).
            cron_schedule: None,
            reserved_poller_identity: None,
            eager_execution_accepted: false,
        };
        self.schedule_store.acquire_start_permit(namespace_id).await;
        let outcome = self
            .runtime
            .start_workflow_with_policy(request)
            .await
            .map_err(EdgeError::from)?;
        let result = match outcome {
            StartWorkflowResult::Started {
                run_key, run_id, ..
            } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::Running,
            },
            StartWorkflowResult::Deduped {
                run_key, run_id, ..
            } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::Running,
            },
            StartWorkflowResult::UsedExisting { run_key, run_id }
            | StartWorkflowResult::Rejected {
                run_key, run_id, ..
            } => ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(WorkflowExecution {
                    namespace_id,
                    workflow_id,
                    run_id,
                    run_key,
                }),
                start_workflow_status: WorkflowExecutionStatus::StartFailed,
            },
        };
        self.schedule_store
            .update(namespace_id, schedule_id, &[], |entry| {
                if let Some(workflow) = result.start_workflow_result.clone()
                    && result.start_workflow_status == WorkflowExecutionStatus::Running
                {
                    entry.info.running_workflows.push(workflow);
                }
                entry.info.action_count += 1;
                entry.info.recent_actions.push(result);
                if entry.info.recent_actions.len() > 10 {
                    entry.info.recent_actions.remove(0);
                }
                entry.info.update_time = actual_time;
            })
            .map_err(|err| EdgeError::BadRequest(err.to_string()))?;
        Ok(())
    }

    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    /// Cancel and suppress a worker's task-queue polls when the release-pinned
    /// shutdown policy is enabled, and eagerly remove its Describe poller row.
    pub async fn cancel_outstanding_worker_polls(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        task_queue: tokeira_types::TaskQueueName,
        worker: tokeira_types::WorkerIdentity,
    ) -> bool {
        let applied = self
            .runtime
            .cancel_outstanding_worker_polls(namespace_id, task_queue.clone(), worker.clone())
            .await;
        if applied {
            self.poller_registry
                .remove_worker(namespace_id, &task_queue, &worker);
        }
        applied
    }

    pub fn new_with_buffered_queries_and_history_wait_registry(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        resolver: Arc<dyn ExecutionResolver>,
        visibility: Arc<dyn VisibilityApi>,
        repo: Arc<dyn RunRepository>,
        operator_api: Arc<dyn OperatorApi>,
        namespaces: Arc<dyn NamespaceCache>,
        interceptors: Arc<EdgeInterceptors>,
        poller_registry: PollerRegistry,
        pending_queries: PendingQueryStore,
        buffered_queries: BufferedQueryRegistry,
        broker: InMemoryBroker,
        long_polls: LongPollGate,
        router: Arc<dyn EdgeRouter>,
        history_waiters: HistoryWaitRegistry,
    ) -> Self {
        Self::new_with_stores_and_buffered_queries_and_history_wait_registry(
            runtime,
            resolver,
            visibility,
            repo,
            operator_api,
            namespaces,
            interceptors,
            poller_registry,
            pending_queries,
            buffered_queries,
            broker,
            NexusTaskBroker::default(),
            long_polls,
            router,
            history_waiters,
            WorkerRegistry::default(),
            Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
            Arc::new(ScheduleStore::default()),
            Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
            Arc::new(BatchOperationStore::default()),
        )
    }

    /// Attach buffered queries whose consistency barrier has been met.
    ///
    /// Queries in the `BufferedQueryRegistry` each carry a `required_barrier`
    /// (the `last_event_id` the caller must observe). We only drain queries
    /// whose barrier is at or below `observable_barrier` — this guarantees
    /// the worker will evaluate the query against state that includes the
    /// write the caller was waiting on. Queries whose barrier is still ahead
    /// stay buffered until the next WFT completion advances the watermark.
    async fn attach_buffered_queries(
        &self,
        run_key: RunKey,
        observable_barrier: i64,
        task_token: &[u8],
        target: &mut std::collections::HashMap<String, WorkflowQueryDto>,
    ) {
        for query in self
            .buffered_queries
            .drain_satisfied(run_key, observable_barrier)
        {
            let query_id = Uuid::new_v4().to_string();
            self.pending_queries
                .insert(task_token, query_id.clone(), query.response_tx)
                .await;
            target.insert(
                query_id,
                WorkflowQueryDto {
                    query_type: query.query_type,
                    query_args: query.query_args,
                },
            );
        }
    }

    /// Dispatch barrier-satisfied queries directly through the broker for
    /// runs that are currently quiescent (no pending WFT).
    ///
    /// When a run has no in-flight workflow task, there is no poll response
    /// to piggyback queries onto. Instead we publish each query as a
    /// standalone `QueryTask` through the broker, which will route it to a
    /// poller (preferring the sticky worker if the affinity hasn't expired).
    /// This avoids the query sitting in the buffer indefinitely when no
    /// further mutations are expected.
    async fn dispatch_queries_direct(
        &self,
        run_key: RunKey,
        state: &tokeira_kernel::WorkflowState,
        barrier: i64,
    ) {
        let now = OffsetDateTime::now_utc();
        let sticky_preferred = state.sticky.as_ref().and_then(|affinity| {
            (affinity.expires_at > now).then_some(affinity.worker_identity.clone())
        });
        let sticky_deadline = state
            .sticky
            .as_ref()
            // The kernel stores the SDK sticky
            // `schedule_to_start_timeout` as the affinity expiry. Buffered
            // queries released after WFT completion use that same concrete
            // deadline for sticky-first direct query fallback
            // (`service/history/api/queryworkflow/api.go:350-410 @ v1.31.0`).
            .and_then(|affinity| (affinity.expires_at > now).then_some(affinity.expires_at));
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };

        for query in self.buffered_queries.drain_satisfied(run_key, barrier) {
            self.broker
                .publish_query_task(tokeira_runtime::QueryTask {
                    run_key,
                    query_type: query.query_type,
                    query_args: query.query_args,
                    queue: queue.clone(),
                    sticky_preferred: sticky_preferred.clone(),
                    sticky_deadline,
                    response_tx: query.response_tx,
                })
                .await;
        }
    }

    async fn build_direct_query_poll_response(
        &self,
        query: tokeira_runtime::QueryTask,
        worker: &WorkerIdentity,
    ) -> EdgeResult<PollWorkflowTaskQueueResponse> {
        let state = match self
            .repo
            .load_run(query.run_key)
            .await
            .map_err(EdgeError::from)?
        {
            LoadedRun::Existing(state) => state,
            LoadedRun::Absent => {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: query.queue.namespace_id.0.to_string(),
                    workflow_id: query.run_key.0.to_string(),
                });
            }
        };
        // v1.31.0's matching engine sends EMPTY history on a sticky query
        // delivery (the worker answers from its cache) and FULL history from
        // event 1 on a normal-queue delivery (the worker replays)
        // (getHistoryForQueryTask, matching_engine.go:839-899 @ v1.31.0).
        // tokeira's query tasks route sticky-ness by worker identity rather
        // than a sticky queue key, so a delivery claimed by the
        // sticky-preferred worker IS the sticky case: attach nothing — a full
        // history here makes the SDK replay instead of using its cache
        // (TestQueryWorkflow_Sticky pins replayCount == 1). Partial history
        // (the old shape) matches neither mode.
        let sticky_delivery = query.sticky_preferred.as_ref() == Some(worker);
        let (history, history_principals) = if sticky_delivery {
            (Vec::new(), Vec::new())
        } else {
            let attributed = self
                .repo
                .read_attributed_history(query.run_key, 0, usize::MAX)
                .await
                .map_err(EdgeError::from)?;
            attributed
                .into_iter()
                .map(|attributed| (attributed.event, attributed.principal))
                .unzip()
        };

        // Temporal returns direct queries as workflow-poll tasks with
        // `started_event_id = 0` and a query task token, because no history
        // event is authored for the read-only query
        // (`proto/upstream/temporal/api/workflowservice/v1/request_response.proto`,
        // `service/matching/matching_engine.go:1084 @ v1.31.0`). The token is
        // opaque to the SDK; the edge keys it to the parked caller in
        // `PendingQueryStore` and resolves it via `RespondQueryTaskCompleted`.
        let task_token = format!(
            "query-task:{}:{}:{}",
            query.queue.namespace_id.0,
            query.queue.task_queue.0,
            Uuid::new_v4()
        )
        .into_bytes();
        self.pending_queries
            .insert(&task_token, LEGACY_QUERY_ID.to_string(), query.response_tx)
            .await;

        Ok(PollWorkflowTaskQueueResponse {
            task_token,
            started_event_id: 0,
            previous_started_event_id: state.previous_started_event_id,
            attempt: 1,
            scheduled_time: None,
            started_time: None,
            payload: crate::translate::WorkflowTaskPayloadDto {
                workflow_id: state.workflow_id.0,
                run_key: state.run_key,
                run_id: state.run_id,
                task_queue: state.task_queue.0,
                history,
                history_principals,
            },
            query: Some(WorkflowQueryDto {
                query_type: query.query_type,
                query_args: query.query_args,
            }),
            queries: std::collections::HashMap::new(),
            messages: Vec::new(),
            poller_scaling_decision: None,
        })
    }

    pub async fn start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: StartWorkflowExecutionRequest,
    ) -> EdgeResult<StartWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "start_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let namespace = req.namespace.clone();
                let workflow_id = req.workflow_id.clone();
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&namespace),
                        Action::StartWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(self.router.route_workflow(&namespace, &workflow_id).await?)?;

                let internal = to_internal::start_request(req, &ctx);
                let outcome = self
                    .runtime
                    .start_workflow_with_policy(internal.clone())
                    .await
                    .map_err(EdgeError::from)?;
                match outcome {
                    StartWorkflowResult::Started {
                        mutation_metadata,
                        eager_workflow_task,
                        ..
                    } => {
                        self.notify_history_run_key(
                            internal.run_key,
                            mutation_metadata.last_event_id,
                        )
                        .await;
                        let mut response = from_internal::start_response(
                            &internal,
                            WorkflowMutationOutcome {
                                transition_seq: mutation_metadata.transition_seq.0,
                                last_event_id: mutation_metadata.last_event_id,
                                was_duplicate: false,
                                execution_status: mutation_metadata.execution_status,
                                new_run_id: None,
                            },
                        );
                        response.eager_workflow_task = match eager_workflow_task {
                            Some(started) => Some(
                                from_internal::poll_response(
                                    started,
                                    self.repo.as_ref(),
                                    internal.namespace_id,
                                )
                                .await
                                .map_err(EdgeError::from)?,
                            ),
                            None => None,
                        };
                        Ok(response)
                    }
                    StartWorkflowResult::UsedExisting { run_key, run_id } => {
                        // UseExisting attached to a running incumbent rather than
                        // creating a new run. v1.31.0 returns success here — RunId =
                        // the existing run, Started = false, Status = RUNNING — not an
                        // AlreadyStarted error; only the Fail policy errors
                        // (handleUseExistingWorkflowOnConflictOptions vs the Fail arm,
                        // service/history/api/startworkflow/api.go @ v1.31.0). The Nexus
                        // WorkflowRunOperation relies on this: with
                        // WorkflowExecutionErrorWhenAlreadyStarted set, a UseExisting
                        // caller must see success so its operation starts against the
                        // attached run (temporalnexus/operation.go @ sdk v1.41.1).
                        Ok(StartWorkflowExecutionResponse {
                            run_key,
                            run_id,
                            transition_seq: 0,
                            last_event_id: 0,
                            started: false,
                            // Attached to a running incumbent → RUNNING (api.go:343 returns the
                            // incumbent's status, which for UseExisting-on-running is RUNNING).
                            status: ExecutionStatus::Running,
                            // When the attach recorded a WorkflowExecutionOptionsUpdated event
                            // (OnConflictOptions{AttachRequestId}), v1.31.0 returns a RequestIdRef
                            // link to it rather than the EventRef-to-start link
                            // (generateRequestIdRefLink, startworkflow/api.go:660-668/833).
                            attached_request_id: internal
                                .on_conflict_options
                                .as_ref()
                                .filter(|options| options.attach_request_id)
                                .map(|_| internal.request.request_id.0.clone()),
                            eager_workflow_task: None,
                        })
                    }
                    StartWorkflowResult::Deduped {
                        run_key,
                        run_id,
                        execution_status,
                        eager_workflow_task,
                    } => {
                        // A retried start whose RequestId already authored this run's
                        // WorkflowExecutionStarted: v1.31.0 respondToRetriedRequest returns the
                        // existing run with Started=true and the incumbent's Status
                        // (startworkflow/api.go:332-336, 563/567). The EventRef self-link to
                        // event 1 is synthesised by the proto layer from run_id.
                        let eager_workflow_task = match eager_workflow_task {
                            Some(started) => Some(
                                from_internal::poll_response(
                                    started,
                                    self.repo.as_ref(),
                                    internal.namespace_id,
                                )
                                .await
                                .map_err(EdgeError::from)?,
                            ),
                            None => None,
                        };
                        Ok(StartWorkflowExecutionResponse {
                            run_key,
                            run_id,
                            transition_seq: 0,
                            last_event_id: 0,
                            started: true,
                            status: execution_status,
                            attached_request_id: None,
                            eager_workflow_task,
                        })
                    }
                    StartWorkflowResult::Rejected { run_id, reason, .. } => {
                        Err(EdgeError::WorkflowStartRejected {
                            message: start_reject_message(reason, &workflow_id, run_id),
                            run_id: run_id.0.to_string(),
                        })
                    }
                }
            },
        )
        .await
    }

    pub async fn signal_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "signal_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::SignalWorkflowExecution,
                        false,
                    )
                    .await?;

                // Worker Deployment entity-workflow surface: a `sync-drainage-status`
                // signal addressed to `temporal-sys-worker-deployment-version:<name>:<build>`
                // drives registry drainage state rather than a per-run workflow (Tokeira
                // backs the entity-workflow surface with the registry; see
                // `deployment_registry`). Mirrors the version entity workflow's signal
                // handler (`version_workflow.go:119 @ v1.31.0`). A `force-continue-as-new`
                // signal to either the version or deployment entity is a no-op success:
                // tokeira holds the registry as durable state, so there is no per-run
                // history to continue-as-new. Other signals to these ids fall through to
                // normal routing (and surface NotFound).
                if req.signal_name == crate::grpc::translate::SYNC_DRAINAGE_SIGNAL_NAME
                    && let Some((deployment_name, build_id)) =
                        crate::grpc::translate::parse_worker_deployment_version_workflow_id(
                            &req.workflow_id,
                        )
                {
                    if let Some(worker_deployments) = self.worker_deployments.as_ref() {
                        let status =
                            crate::grpc::translate::decode_version_drainage_status(&req.input)
                                .map_err(|error| EdgeError::BadRequest(error.to_string()))?;
                        worker_deployments
                            .apply_version_drainage(
                                to_internal::namespace_id_for(&req.namespace),
                                tokeira_storage::DeploymentName(deployment_name),
                                tokeira_storage::BuildId(build_id),
                                status,
                            )
                            .await?;
                    }
                    return Ok(SignalWorkflowExecutionResponse {
                        accepted: true,
                        transition_seq: 0,
                        last_event_id: 0,
                    });
                }
                if req.signal_name == crate::grpc::translate::FORCE_CAN_SIGNAL_NAME
                    && crate::grpc::translate::is_worker_deployment_entity_workflow_id(
                        &req.workflow_id,
                    )
                {
                    return Ok(SignalWorkflowExecutionResponse {
                        accepted: true,
                        transition_seq: 0,
                        last_event_id: 0,
                    });
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                // Temporal keys SignalWorkflowExecution by the caller-supplied
                // workflow ID and run ID when present
                // (`service/history/api/signalworkflow/api.go @ v1.31.0`).
                // Empty run_id keeps the SDK-compatible current-run fallback;
                // a non-empty malformed run_id must fail before lookup.
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;

                let internal = to_internal::signal_request(req, &ctx);
                let outcome = self
                    .runtime
                    .signal_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::signal_response(outcome))
            },
        )
        .await
    }

    /// Poll for a workflow task, attaching buffered queries and pending
    /// update messages to the response.
    ///
    /// After the runtime returns a started WFT, we do two things before
    /// handing the response to the caller:
    ///
    /// 1. **Barrier-gated query attachment** — drain queries from the
    ///    `BufferedQueryRegistry` whose `required_barrier` is satisfied by
    ///    the history included in this response. These come from the
    ///    buffered registry (not the broker) because they need consistency
    ///    guarantees that the broker's fire-and-forget dispatch cannot
    ///    provide.
    ///
    /// 2. **Update message construction** — for each pending update
    ///    transport, we build a `ProtocolMessage` with the update request
    ///    body and a `sequencing_event_id` set to `started_event_id - 1`.
    ///    The SDK uses this to determine where in the history replay the
    ///    update should be processed.
    pub async fn poll_workflow_task_queue(
        &self,
        headers: &HeaderMap,
        req: PollWorkflowTaskQueueRequest,
    ) -> EdgeResult<Option<PollWorkflowTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_workflow_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollWorkflowTaskQueue,
                        true,
                    )
                    .await?;
                let namespace_id = resolved_namespace_id(&ctx, &req.namespace)?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Workflow)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let poller = self.poller_registry.register(
                    queue_key_for_poll(
                        &req.namespace,
                        &req.task_queue,
                        TaskKind::Workflow,
                        req.deployment.clone(),
                        req.build_id.clone(),
                    ),
                    WorkerIdentity(req.worker_identity.clone()),
                );
                // A versioned worker poll lazily registers its deployment and version,
                // matching v1.31.0's matching-driven auto-create
                // (`service/worker/workerdeployment/client.go:1230 @ v1.31.0`). This is
                // best-effort bookkeeping on the control plane: a registry hiccup must
                // not fail the poll itself, so failures are logged rather than
                // propagated. Unversioned polls carry no deployment/build id and skip it.
                if let (Some(worker_deployments), Some(deployment), Some(build_id)) = (
                    self.worker_deployments.as_ref(),
                    req.deployment.as_ref(),
                    req.build_id.as_ref(),
                ) && let Err(error) = worker_deployments
                    .register_polled_deployment(RegisterPolledDeployment {
                        namespace_id: to_internal::namespace_id_for(&req.namespace),
                        deployment_name: DeploymentName(deployment.0.clone()),
                        build_id: tokeira_storage::BuildId(build_id.0.clone()),
                        task_queue: req.task_queue.clone(),
                        task_queue_type: DeploymentTaskQueueType::Workflow,
                        identity: req.worker_identity.clone(),
                    })
                    .await
                {
                    tracing::warn!(
                        %error,
                        deployment = %deployment.0,
                        build_id = %build_id.0,
                        "failed to auto-register polled worker deployment"
                    );
                }
                let internal = to_internal::poll_request(req);
                let scaling_queue = internal.queue.clone();
                let activation = self
                    .runtime
                    .poll_workflow_activation(
                        internal.queue,
                        internal.worker_identity.clone(),
                        internal.timeout,
                    )
                    .await;
                // Reaching this point distinguishes a matched task, long-poll
                // timeout, or runtime error from tonic cancellation, which
                // drops the handler future before this finalizer can run
                // (`task_queue_partition_manager.go:617-621 @ v1.31.0`).
                let activation = activation.map_err(EdgeError::from)?;
                if activation.is_none()
                    && self
                        .runtime
                        .workflow_poll_cancelled(&scaling_queue, &internal.worker_identity)
                        .await
                {
                    poller.cancelled();
                } else {
                    poller.completed();
                }
                let scaling_decision = if activation.is_some() {
                    self.runtime
                        .workflow_poller_scaling_decision(&scaling_queue)
                        .await
                } else {
                    None
                };

                match activation {
                    Some(WorkflowActivation::WorkflowTask(started)) => {
                        let mut response = from_internal::poll_response(
                            started.clone(),
                            self.repo.as_ref(),
                            namespace_id,
                        )
                        .await
                        .map_err(EdgeError::from)?;
                        self.decorate_workflow_task_response(&started, &mut response)
                            .await?;
                        response.poller_scaling_decision = scaling_decision;

                        Ok(Some(response))
                    }
                    Some(WorkflowActivation::QueryTask(query)) => {
                        let mut response = self
                            .build_direct_query_poll_response(query, &internal.worker_identity)
                            .await?;
                        response.poller_scaling_decision = scaling_decision;
                        Ok(Some(response))
                    }
                    None => Ok(None),
                }
            },
        )
        .await
    }

    async fn decorate_workflow_task_response(
        &self,
        started: &StartedWorkflowTask,
        response: &mut PollWorkflowTaskQueueResponse,
    ) -> EdgeResult<()> {
        let task_token = response.task_token.clone();
        let observable_barrier = response
            .payload
            .history
            .last()
            .map(|event| event.event_id)
            .unwrap_or(response.started_event_id);
        self.attach_buffered_queries(
            started.run_key,
            observable_barrier,
            &task_token,
            &mut response.queries,
        )
        .await;

        // Each update ships to the worker exactly once; already-sent updates
        // are re-included only on transient retry attempts (`Send` with
        // includeAlreadySent = attempt > 1, update.go:404-540 @ v1.31.0).
        for update in self
            .runtime
            .pending_update_transports(started.run_key, response.attempt > 1)
            .await
            .map_err(EdgeError::from)?
        {
            let request = tokeira_proto::public::temporal::api::update::v1::Request {
                meta: Some(tokeira_proto::public::temporal::api::update::v1::Meta {
                    update_id: update.update_id.clone(),
                    identity: update.identity,
                }),
                input: Some(tokeira_proto::public::temporal::api::update::v1::Input {
                    header: None,
                    name: update.update_name,
                    args: Some(tokeira_proto::conversions::common::payloads_from_domain(
                        &update.input,
                    )),
                }),
            };
            let body = prost_types::Any {
                type_url: "type.googleapis.com/temporal.api.update.v1.Request".to_string(),
                value: request.encode_to_vec(),
            };
            // The SDK requires sequencing_event_id to determine where in the
            // history replay the update should be processed. Temporal sets
            // this to workflowTaskStartedEventID - 1.
            let sequencing_event_id = started.token.started_event_id - 1;
            response.messages.push(ProtocolMessageDto {
                id: format!("{}/request", update.update_id),
                protocol_instance_id: update.update_id,
                body: body.encode_to_vec(),
                sequencing_event_id: Some(sequencing_event_id),
            });
        }

        Ok(())
    }

    /// Process a WFT completion from the SDK.
    ///
    /// Three non-obvious things happen here:
    ///
    /// 1. **ProtocolMessage command resolution** — the translate layer has
    ///    already decoded `ProtocolMessage` commands from the `messages`
    ///    field. For `Accepted` bodies we fill in `update_name`/`input`
    ///    from the `UpdateRegistry` (the SDK doesn't echo these back). For
    ///    `Completed`/`Rejected` bodies we notify the registry so the
    ///    original `UpdateWorkflowExecution` caller gets unblocked.
    ///
    /// 2. **Query-only short-circuit** — if the task token has
    ///    `logical_seq = 0` (a synthetic query-only WFT) and there are no
    ///    commands, we return immediately without touching the runtime.
    ///
    /// 3. **Post-completion quiescence check** — after committing the
    ///    completion, if the run is still open, has buffered queries, and
    ///    is now quiescent (no pending WFT), the remaining buffered queries
    ///    are UNBLOCKED into direct broker dispatch (v1.31.0's
    ///    QueryCompletionTypeUnblocked model), regardless of
    ///    `return_new_workflow_task`. This avoids queries sitting in the
    ///    buffer until the next unrelated mutation.
    pub async fn respond_workflow_task_completed(
        &self,
        headers: &HeaderMap,
        mut req: RespondWorkflowTaskCompletedRequest,
    ) -> EdgeResult<RespondWorkflowTaskCompletedResponse> {
        self.observe_edge_call(
            headers,
            "respond_workflow_task_completed",
            None,
            None,
            async move {
                let (task_token, token_namespace_id, context, namespace) = self
                    .admit_json_task_token::<tokeira_types::WorkflowTaskToken>(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RespondWorkflowTaskCompleted,
                    )
                    .await?;
                req.namespace = namespace;
                if token_namespace_id.is_none()
                    && !req.namespace.is_empty()
                    && let tokeira_kernel::LoadedRun::Existing(state) = self
                        .repo
                        .load_run(task_token.run_key)
                        .await
                        .map_err(EdgeError::from)?
                {
                    validate_authoritative_task_namespace(&req.namespace, state.namespace_id)?;
                }
                let namespace_id = match context.namespace.as_ref() {
                    Some(_) => resolved_namespace_id(&context, &req.namespace)?,
                    None => match self
                        .repo
                        .load_run(task_token.run_key)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        LoadedRun::Existing(state) => state.namespace_id,
                        LoadedRun::Absent => to_internal::namespace_id_for(&req.namespace),
                    },
                };
                if cross_namespace_commands_enabled() {
                    for (target_namespace, action) in cross_namespace_authorization_targets(
                        &req.namespace,
                        &req.commands,
                    ) {
                        self.interceptors
                            .authorize_existing_context(&context, action, &target_namespace)
                            .await?;
                    }
                }
                let query_only = task_token.logical_seq.0 == 0;

                for (query_id, result) in &req.query_results {
                    if let Some(sender) = self.pending_queries.take(&req.task_token, query_id).await
                    {
                        let _ = sender.send(match result {
                            QueryResultDto::Answered { result } => QueryResult::Completed {
                                result: result.clone(),
                            },
                            QueryResultDto::Failed {
                                error_message,
                                failure,
                            } => QueryResult::Failed {
                                message: error_message.clone(),
                                failure: failure.clone(),
                            },
                        });
                    }
                }

                // Search-attribute registration is namespace-scoped edge state,
                // so validate here and turn only the offending command into a
                // kernel sentinel. The completion must still enter the
                // authoritative transition: v1.31.0 records WorkflowTaskFailed
                // with BAD_SEARCH_ATTRIBUTES and schedules the replacement WFT
                // before returning InvalidArgument
                // (`workflow_task_completed_handler.go @ v1.31.0`).
                for command_index in 0..req.commands.len() {
                    let keys = match &req.commands[command_index] {
                        tokeira_kernel::WorkflowCommand::UpsertSearchAttributesPatch(patch) => {
                            patch.0.keys().cloned().collect::<Vec<_>>()
                        }
                        _ => continue,
                    };
                    if let Some(unknown) = self
                        .visibility
                        .unknown_search_attribute(namespace_id, &keys)
                        .await
                        .map_err(EdgeError::from)?
                    {
                        req.commands[command_index] =
                            tokeira_kernel::WorkflowCommand::InvalidSearchAttributes {
                                message: format!(
                                    "Namespace {} has no mapping defined for search attribute {unknown}",
                                    req.namespace
                                ),
                            };
                        break;
                    }
                }

                // Hydrate an Acceptance message's command body with the update's
                // name/input from the registry (the worker echoes only the message
                // id). We deliberately do NOT resolve Completed/Rejected waiters
                // here: the update outcome must be published only after the
                // `WorkflowExecutionUpdateCompleted`/`Rejected` event durably
                // commits, which the lane does post-commit. v1.31.0 sets the
                // outcome future in `OnAfterCommit` for exactly this reason — a
                // waiter woken before the event is durable would re-read history
                // and see only `Accepted` (`update.go:onResponseMsg @ v1.31.0`).
                // Notifying here pre-commit removed the registry waiter and made
                // the lane's correct post-commit notify a no-op, stranding the
                // COMPLETED caller at the Accepted stage.
                for cmd in &mut req.commands {
                    if let tokeira_kernel::WorkflowCommand::ProtocolMessage {
                        body:
                            tokeira_kernel::UpdateProtocolBody::Accepted {
                                update_id,
                                update_name,
                                input,
                                ..
                            },
                        ..
                    } = cmd
                        && let Ok(Some((name, inp))) = self
                            .runtime
                            .peek_update_info(task_token.run_key, update_id.clone())
                            .await
                    {
                        *update_name = name;
                        *input = inp;
                    }
                }

                if query_only && req.commands.is_empty() {
                    return Ok(RespondWorkflowTaskCompletedResponse {
                        transition_seq: 0,
                        last_event_id: 0,
                        execution_status: ExecutionStatus::Running,
                        new_run_id: None,
                        was_duplicate: false,
                        workflow_task: None,
                        activity_tasks: Vec::new(),
                        reset_history_event_id: 0,
                    });
                }

                let eager_activity_specs = collect_eager_activity_specs(
                    &req.commands,
                    self.eager_dispatch_config
                        .max_eager_activity_tasks_per_response,
                );
                let eager_activity_namespace = if eager_activity_specs.is_empty() {
                    None
                } else if let Some(namespace) = context.namespace.as_ref() {
                    Some(namespace.name.clone())
                } else {
                    // Legacy Tokeira task tokens did not carry a namespace ID.
                    // Resolve the run's already-established stable ID before the
                    // completion commits so eager delivery cannot fail after the
                    // authoritative transition. v1.31.0 exposes the current name,
                    // never the ID, on this path
                    // (`workflow_task_completed_handler.go:613 @ v1.31.0`).
                    let namespace_id_string = namespace_id.0.to_string();
                    Some(
                        self.namespaces
                            .get_by_id(&namespace_id_string)
                            .await
                            .map_err(EdgeError::from)?
                            .ok_or_else(|| {
                                EdgeError::NamespaceNotFound(namespace_id_string.clone())
                            })?
                            .name,
                    )
                };
                let completion_identity = req.identity.clone();
                let saved_task_token = req.task_token.clone();
                let wants_eager_return = req.return_new_workflow_task;

                let internal = to_internal::workflow_task_completed_request(req, &context)
                    .map_err(EdgeError::from)?;
                let run_key = internal.token.run_key;
                let speculative_token_started_event_id = internal.token.started_event_id;
                let outcome = self
                    .runtime
                    .complete_workflow_task(internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                let mut resp = from_internal::completed_response(outcome);
                // Speculative DROP (spec speculative-wft E2): the completion
                // committed but persisted nothing for this task — its virtual
                // started id sits beyond the run's last event. The SDK rewinds
                // to the last completed WFT's started event
                // (respondworkflowtaskcompleted/api.go:770 @ v1.31.0).
                if resp.last_event_id < speculative_token_started_event_id
                    && let tokeira_kernel::LoadedRun::Existing(state) =
                        self.repo.load_run(run_key).await.map_err(EdgeError::from)?
                {
                    resp.reset_history_event_id = state.previous_started_event_id;
                }

                if let Some(workflow_namespace) = eager_activity_namespace.as_deref() {
                    for (activity_id, task_queue, deployment, build_id) in eager_activity_specs {
                        let queue = tokeira_types::QueueKey {
                            namespace_id,
                            task_queue,
                            task_kind: TaskKind::Activity,
                            deployment,
                            build_id,
                        };
                        if let Some(started) = self
                            .runtime
                            .try_claim_activity_task(
                                queue,
                                run_key,
                                activity_id,
                                WorkerIdentity(completion_identity.clone()),
                            )
                            .await
                            .map_err(EdgeError::from)?
                        {
                            resp.activity_tasks
                                .push(from_internal::poll_activity_response(
                                    started,
                                    namespace_id,
                                    workflow_namespace,
                                )?);
                        }
                    }
                }

                if resp.execution_status.is_open()
                    && (wants_eager_return || self.buffered_queries.has_buffered(run_key))
                {
                    let (token, _) = crate::task_token::decode::<
                        tokeira_types::WorkflowTaskToken,
                    >(&saved_task_token)
                    .map_err(EdgeError::from)?;
                    let loaded = self
                        .repo
                        .load_run(token.run_key)
                        .await
                        .map_err(EdgeError::from)?;
                    if let tokeira_kernel::LoadedRun::Existing(state) = loaded {
                        if wants_eager_return && state.pending_workflow_task.is_some() {
                            // The pending WFT was dispatched onto the STICKY
                            // queue when this completion set a sticky affinity
                            // (sticky raise S2) — claim it from where the
                            // kernel actually enqueued it.
                            let sticky_dispatched =
                                state.pending_workflow_task.as_ref().is_some_and(|pending| {
                                    pending.schedule_to_start_deadline.is_some()
                                });
                            let task_queue = state
                                .sticky
                                .as_ref()
                                .filter(|sticky| {
                                    sticky_dispatched && !sticky.sticky_queue.0.is_empty()
                                })
                                .map(|sticky| sticky.sticky_queue.clone())
                                .unwrap_or_else(|| state.task_queue.clone());
                            let queue = tokeira_types::QueueKey {
                                namespace_id: state.namespace_id,
                                task_queue,
                                task_kind: TaskKind::Workflow,
                                deployment: state.deployment.clone(),
                                build_id: state.build_id.clone(),
                            };
                            if let Some(mut started) = self
                                .runtime
                                .try_claim_workflow_task(
                                    queue,
                                    run_key,
                                    WorkerIdentity(completion_identity.clone()),
                                )
                                .await
                                .map_err(EdgeError::from)?
                            {
                                // A new WFT returned inline from RespondWorkflowTaskCompleted is
                                // delivered to the same worker and carries incremental history from
                                // the previous started event — v1.31.0 treats it as sticky
                                // (respondworkflowtaskcompleted/api.go:759-760), so the SDK receives
                                // only the events after PreviousStartedEventId rather than the full
                                // history.
                                started.is_sticky_match = true;
                                let mut workflow_task = from_internal::poll_response(
                                    started.clone(),
                                    self.repo.as_ref(),
                                    state.namespace_id,
                                )
                                .await
                                .map_err(EdgeError::from)?;
                                self.decorate_workflow_task_response(&started, &mut workflow_task)
                                    .await?;
                                resp.workflow_task = Some(workflow_task);
                            }
                        } else if state.pending_workflow_task.is_none() {
                            // The completing WFT created no follow-up task:
                            // remaining buffered queries are UNBLOCKED and
                            // dispatch directly through the broker as
                            // standalone query tasks — v1.31.0 does the same
                            // regardless of ReturnNewWorkflowTask
                            // (QueryCompletionTypeUnblocked,
                            // respondworkflowtaskcompleted/api.go:1010-1029 +
                            // queryworkflow/api.go:242-260). The old inline
                            // "eager query WFT" (empty history, no legacy
                            // query field) was a shape v1.31.0 never emits
                            // and crashed SDK workers without a cache entry.
                            self.dispatch_queries_direct(
                                state.run_key,
                                &state,
                                state.last_event_id,
                            )
                            .await;
                        }
                    }
                }

                Ok(resp)
            },
        )
        .await
    }

    pub async fn respond_query_task_completed(
        &self,
        headers: &HeaderMap,
        namespace: String,
        task_token: Vec<u8>,
        result: QueryResult,
    ) -> EdgeResult<()> {
        self.observe_edge_call(
            headers,
            "respond_query_task_completed",
            None,
            None,
            async move {
                let token_namespace_id = query_task_namespace_id(&task_token)?;
                if namespace.is_empty() {
                    let _ = self
                        .interceptors
                        .begin_with_task_token_backfill(
                            headers,
                            Some(token_namespace_id),
                            Action::RespondQueryTaskCompleted,
                        )
                        .await?;
                } else {
                    let context = self
                        .interceptors
                        .begin(
                            headers,
                            Some(&namespace),
                            Action::RespondQueryTaskCompleted,
                            false,
                        )
                        .await?;
                    crate::interceptors::validate_task_token_namespace(
                        &context,
                        Some(token_namespace_id),
                    )?;
                }

                if let Some(sender) = self
                    .pending_queries
                    .take(&task_token, LEGACY_QUERY_ID)
                    .await
                {
                    let _ = sender.send(result);
                }
                Ok(())
            },
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn insert_legacy_query_waiter(
        &self,
        task_token: Vec<u8>,
        tx: tokio::sync::oneshot::Sender<QueryResult>,
    ) {
        self.pending_queries
            .insert(&task_token, LEGACY_QUERY_ID.to_string(), tx)
            .await;
    }

    pub async fn describe_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: DescribeWorkflowExecutionRequest,
    ) -> EdgeResult<WorkflowExecutionDescription> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                self.resolver
                    .describe_execution(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id
                            .as_deref()
                            .map(|value| Uuid::parse_str(value).map(RunId))
                            .transpose()
                            .map_err(|err| {
                                EdgeError::BadRequest(format!(
                                    "invalid run_id `{}`: {err}",
                                    req.run_id.as_deref().unwrap_or_default()
                                ))
                            })?,
                    )
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or(EdgeError::WorkflowNotFound {
                        namespace: req.namespace,
                        workflow_id: req.workflow_id,
                    })
            },
        )
        .await
    }

    pub async fn list_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: ListWorkflowExecutionsRequest,
    ) -> EdgeResult<ListWorkflowExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_workflow_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListWorkflowExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .list_workflows(req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn count_workflow_executions(
        &self,
        headers: &HeaderMap,
        req: CountWorkflowExecutionsRequest,
    ) -> EdgeResult<CountWorkflowExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "count_workflow_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::CountWorkflowExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .count_workflows(req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// List standalone-activity executions, scoped to the `archetype_id` the gRPC
    /// layer resolved from the activity bridge (the visibility plane is
    /// archetype-neutral; Requirement 13.1).
    pub async fn list_activity_executions(
        &self,
        headers: &HeaderMap,
        archetype_id: ArchetypeId,
        req: ListActivityExecutionsRequest,
    ) -> EdgeResult<ListActivityExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_activity_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListActivityExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .list_activities(archetype_id, req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// Count standalone-activity executions, scoped to the activity archetype.
    pub async fn count_activity_executions(
        &self,
        headers: &HeaderMap,
        archetype_id: ArchetypeId,
        req: CountActivityExecutionsRequest,
    ) -> EdgeResult<CountActivityExecutionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "count_activity_executions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::CountActivityExecutions,
                        false,
                    )
                    .await?;

                self.visibility
                    .count_activities(archetype_id, req)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn get_cluster_info(&self, headers: &HeaderMap) -> EdgeResult<ClusterInfo> {
        self.observe_edge_call(headers, "get_cluster_info", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::GetClusterInfo, false)
                .await?;

            self.operator_api
                .cluster_info()
                .await
                .map_err(EdgeError::from)
        })
        .await
    }

    pub async fn get_system_info(&self, headers: &HeaderMap) -> EdgeResult<SystemInfo> {
        self.observe_edge_call(headers, "get_system_info", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::GetSystemInfo, false)
                .await?;

            let cluster = self
                .operator_api
                .cluster_info()
                .await
                .map_err(EdgeError::from)?;

            Ok(SystemInfo {
                server_version: cluster.version,
                capabilities: system_capabilities_with_matrix_overlay(SystemCapabilities {
                    signal_and_query_header: true,
                    internal_error_differentiation: true,
                    activity_failure_include_heartbeat: false,
                    supports_schedules: false,
                    encoded_failure_attributes: true,
                    build_id_based_versioning: true,
                    upsert_memo: false,
                    // v1.31.0 advertises this capability unconditionally from
                    // GetSystemInfo (workflow_handler.go:3385 @ v1.31.0).
                    eager_workflow_start: true,
                    // Gates every SDK lang-flag behavior (sdkFlags.tryUse
                    // returns false without it — internal_flags.go @ sdk
                    // v1.41.1), including SDKPriorityUpdateHandling: without
                    // it the Go SDK REJECTS updates delivered before any
                    // handler is registered instead of queueing them. The
                    // round-trip it implies is already real: the completion's
                    // sdk_metadata (lang_used_flags) persists on the
                    // WorkflowTaskCompleted event and returns in history.
                    sdk_metadata: true,
                    count_group_by_execution_status: true,
                    nexus: true,
                    server_scaled_deployments: false,
                    worker_heartbeats: true,
                }),
            })
        })
        .await
    }

    pub async fn list_namespaces(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<EdgeListNamespacesResponse> {
        self.observe_edge_call(headers, "list_namespaces", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::ListNamespaces, false)
                .await?;

            let mut namespaces = self.namespaces.list_all().await.map_err(EdgeError::from)?;
            namespaces.sort_by(|left, right| left.name.cmp(&right.name));

            Ok(EdgeListNamespacesResponse {
                namespaces: namespaces
                    .into_iter()
                    .filter(|namespace| !namespace.deleted)
                    .map(namespace_to_description)
                    .collect(),
                next_page_token: None,
            })
        })
        .await
    }

    pub async fn describe_namespace(
        &self,
        headers: &HeaderMap,
        namespace_name: &str,
    ) -> EdgeResult<NamespaceDescription> {
        let namespace_label = namespace_name.to_string();
        self.observe_edge_call(
            headers,
            "describe_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(namespace_name),
                        Action::DescribeNamespace,
                        false,
                    )
                    .await?;

                let namespace = self
                    .namespaces
                    .get(namespace_name)
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or_else(|| EdgeError::NamespaceNotFound(namespace_name.to_string()))?;

                Ok(namespace_to_description(namespace))
            },
        )
        .await
    }

    /// Describe a namespace by stable ID, including its deletion tombstone.
    ///
    /// `DescribeNamespace` is a metadata read that bypasses ordinary active-state
    /// admission in v1.31.0. That exception is required so callers can observe
    /// `NAMESPACE_STATE_DELETED` between mark-and-rename and final namespace removal
    /// (`tests/namespace_delete_test.go @ v1.31.0`).
    pub async fn describe_namespace_by_id(
        &self,
        headers: &HeaderMap,
        namespace_id: &str,
    ) -> EdgeResult<NamespaceDescription> {
        self.observe_edge_call(headers, "describe_namespace", None, None, async move {
            // Stable-ID resolution precedes auth because the authorizer contract
            // is name-scoped; this is the same namespace-validator ordering used
            // for task-token back-fill in v1.31.0.
            let namespace = self
                .namespaces
                .get_by_id(namespace_id)
                .await
                .map_err(EdgeError::from)?
                .ok_or_else(|| EdgeError::NamespaceNotFound(namespace_id.to_owned()))?;
            let _ctx = self
                .interceptors
                .begin(
                    headers,
                    Some(&namespace.name),
                    Action::DescribeNamespace,
                    false,
                )
                .await?;
            Ok(namespace_to_description(namespace))
        })
        .await
    }

    pub async fn register_namespace(
        &self,
        headers: &HeaderMap,
        req: RegisterNamespaceRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "register_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RegisterNamespace,
                        false,
                    )
                    .await?;

                if !is_valid_namespace_name(&req.namespace) {
                    return Err(EdgeError::BadRequest(format!(
                        "invalid namespace name `{}`",
                        req.namespace
                    )));
                }

                if self
                    .namespaces
                    .get(&req.namespace)
                    .await
                    .map_err(EdgeError::from)?
                    .is_some()
                {
                    return Err(EdgeError::NamespaceAlreadyExists(req.namespace));
                }

                self.namespaces
                    .insert(ResolvedNamespace {
                        namespace_id: Some(
                            to_internal::namespace_id_for(&req.namespace).0.to_string(),
                        ),
                        retention: req.retention,
                        ..ResolvedNamespace::active(req.namespace.clone())
                    })
                    .await
                    .map_err(EdgeError::from)?;

                // Seed the namespace's predefined search attributes so visibility
                // queries in it resolve the map-backed predefined fields, matching the
                // bootstrapped `default` namespace. Without this, list/count in a
                // runtime-created namespace rejects predefined attributes as unknown.
                self.operator_api
                    .seed_predefined_search_attributes(&req.namespace)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    /// Update a namespace's lifecycle state and/or description.
    ///
    /// Tokeira runs a single non-global cluster, so the replication, config,
    /// and security-token request fields are accepted at the wire layer but
    /// ignored here. Only the state transition and description are honoured.
    ///
    /// State-transition validity mirrors v1.31.0 `validateStateUpdate`
    /// (`service/frontend/namespace_handler.go @ v1.31.0`): `Unspecified` or a
    /// same-state target is a no-op; `Registered → {Deleted, Deprecated}` and
    /// `Deprecated → Deleted` are allowed; every other transition (notably any
    /// transition out of `Deleted`) is rejected with `INVALID_ARGUMENT`.
    pub async fn update_namespace(
        &self,
        headers: &HeaderMap,
        req: UpdateNamespaceRequest,
    ) -> EdgeResult<NamespaceDescription> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_namespace",
            Some(namespace_label.as_str()),
            None,
            async move {
                // Resolve before `interceptors.begin`: begin() rejects a deleted
                // namespace with NamespaceDeleted, but UpdateNamespace is the very
                // RPC operators use to manage already-deleted namespaces. We must
                // observe the current (possibly deleted) state to validate the
                // transition rather than fail the lookup outright.
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateNamespace,
                        false,
                    )
                    .await?;

                let mut namespace = self
                    .namespaces
                    .get(&req.namespace)
                    .await
                    .map_err(EdgeError::from)?
                    .ok_or_else(|| EdgeError::NamespaceNotFound(req.namespace.clone()))?;

                validate_namespace_state_update(namespace.deleted, req.state)?;

                if matches!(req.state, NamespaceStateUpdate::Deleted) {
                    namespace.deleted = true;
                }

                let mut description = namespace_to_description(namespace.clone());
                if let Some(new_description) = req.description {
                    description.description = new_description;
                }

                self.namespaces
                    .insert(namespace)
                    .await
                    .map_err(EdgeError::from)?;

                Ok(description)
            },
        )
        .await
    }

    pub async fn describe_task_queue(
        &self,
        headers: &HeaderMap,
        req: DescribeTaskQueueRequest,
    ) -> EdgeResult<DescribeTaskQueueResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "describe_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DescribeTaskQueue,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, req.task_kind)
                        .await?,
                )?;

                let queue =
                    queue_key_for_poll(&req.namespace, &req.task_queue, req.task_kind, None, None);
                let namespace_id = to_internal::namespace_id_for(&req.namespace);
                let task_queue = TaskQueueName(req.task_queue.clone());
                let config = self
                    .task_queue_config_store
                    .get(&namespace_id, &task_queue)
                    .map(task_queue_config_to_edge)
                    .unwrap_or_default();

                let versioning_view = match self.worker_deployments.as_ref() {
                    Some(worker_deployments) => {
                        worker_deployments
                            .task_queue_versioning(namespace_id, req.task_queue.clone())
                            .await?
                    }
                    None => None,
                };
                let stats_version = versioning_view.as_ref().and_then(|view| {
                    if view.ramping_percentage >= 100.0 {
                        view.ramping_version.as_ref()
                    } else {
                        view.current_version.as_ref()
                    }
                });
                let stats_deployment =
                    stats_version.map(|version| DeploymentId(version.deployment_name.0.clone()));
                let stats_build_id =
                    stats_version.map(|version| BuildId(version.build_id.0.clone()));
                let workflow_queue = queue_key_for_poll(
                    &req.namespace,
                    &req.task_queue,
                    TaskKind::Workflow,
                    stats_deployment.clone(),
                    stats_build_id.clone(),
                );
                let activity_queue = queue_key_for_poll(
                    &req.namespace,
                    &req.task_queue,
                    TaskKind::Activity,
                    stats_deployment,
                    stats_build_id,
                );
                let (workflow_stats, activity_stats) = if req.report_stats {
                    let mut workflow = self.runtime.task_queue_backlog_stats(&workflow_queue).await;
                    let mut activity = self.runtime.task_queue_backlog_stats(&activity_queue).await;
                    // Current and 100%-ramping versions absorb work that was
                    // queued while the family was still unversioned. Matching's
                    // adjusted stats include both physical queues
                    // (`GetPhysicalQueueAdjustedStats`,
                    // `physical_task_queue_manager.go @ v1.31.0`).
                    if stats_version.is_some() {
                        let unversioned_workflow = self
                            .runtime
                            .task_queue_backlog_stats(&queue_key_for_poll(
                                &req.namespace,
                                &req.task_queue,
                                TaskKind::Workflow,
                                None,
                                None,
                            ))
                            .await;
                        workflow.count += unversioned_workflow.count;
                        workflow.oldest_age =
                            workflow.oldest_age.max(unversioned_workflow.oldest_age);
                        let unversioned_activity = self
                            .runtime
                            .task_queue_backlog_stats(&queue_key_for_poll(
                                &req.namespace,
                                &req.task_queue,
                                TaskKind::Activity,
                                None,
                                None,
                            ))
                            .await;
                        activity.count += unversioned_activity.count;
                        activity.oldest_age =
                            activity.oldest_age.max(unversioned_activity.oldest_age);
                    }
                    (
                        Some(crate::translate::TaskQueueStatsDto {
                            approximate_backlog_count: workflow.count as i64,
                            approximate_backlog_age: workflow.oldest_age,
                        }),
                        Some(crate::translate::TaskQueueStatsDto {
                            approximate_backlog_count: activity.count as i64,
                            approximate_backlog_age: activity.oldest_age,
                        }),
                    )
                } else {
                    (None, None)
                };
                let stats = match req.task_kind {
                    TaskKind::Activity => activity_stats,
                    _ => workflow_stats,
                };

                // Surface Worker Deployment versioning for this task queue (current /
                // ramping version) the way Temporal's matching layer does from synced
                // task-queue user data (`task_queue_partition_manager.go:976 @ v1.31.0`).
                // Derived from the registry; absent when no deployment version has
                // polled the queue or when the registry is not configured.
                let versioning_info = versioning_view.map(task_queue_versioning_view_to_edge);

                Ok(DescribeTaskQueueResponse {
                    pollers: self
                        .poller_registry
                        .pollers(&queue)
                        .into_iter()
                        .map(active_poller_to_edge)
                        .collect(),
                    backlog_count_hint: req
                        .include_status
                        .then_some(stats.map_or(0, |value| value.approximate_backlog_count)),
                    config,
                    versioning_info,
                    stats,
                    workflow_stats: if req.enhanced { workflow_stats } else { None },
                    activity_stats: if req.enhanced { activity_stats } else { None },
                })
            },
        )
        .await
    }

    /// List the partition topology of a task queue.
    ///
    /// tokeira runs a single (root) partition per task queue per task type. v1.31.0's
    /// matching engine returns one `TaskQueuePartitionMetadata` per partition for the
    /// activity and workflow types (`matching_engine.go:1609 @ v1.31.0`); with a single
    /// partition the root key is the bare task-queue name (no `/_sys/<name>/<n>` suffix,
    /// which v1.31.0 only adds for partitions 1..N). `owner_host_name` is left empty: the
    /// edge plane has no matching-host membership to attribute, and the field is purely
    /// diagnostic — SDKs discover topology from `key`. Validation (namespace / task-queue
    /// presence, recognized kind) runs at the gRPC translation boundary before this call.
    pub async fn list_task_queue_partitions(
        &self,
        headers: &HeaderMap,
        req: ListTaskQueuePartitionsRequest,
    ) -> EdgeResult<ListTaskQueuePartitionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "list_task_queue_partitions",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ListTaskQueuePartitions,
                        false,
                    )
                    .await?;

                let root = TaskQueuePartition {
                    key: req.task_queue,
                    owner_host_name: String::new(),
                };
                Ok(ListTaskQueuePartitionsResponse {
                    activity_partitions: vec![root.clone()],
                    workflow_partitions: vec![root],
                })
            },
        )
        .await
    }

    /// Update a running workflow's execution options (`versioning_override`).
    ///
    /// Validates the run id and resolves the target execution before mutating
    /// (`NOT_FOUND` for an absent execution; `INVALID_ARGUMENT` for a malformed run id —
    /// both surfaced by `resolve_execution_run_key`). The change has already been reduced
    /// from the `update_mask` at the gRPC boundary, so here we only translate the override
    /// to the kernel and submit the per-run command. The response echoes the post-update
    /// options.
    pub async fn update_workflow_execution_options(
        &self,
        headers: &HeaderMap,
        req: UpdateWorkflowExecutionOptionsRequest,
    ) -> EdgeResult<UpdateWorkflowExecutionOptionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_workflow_execution_options",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateWorkflowExecutionOptions,
                        false,
                    )
                    .await?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;

                let versioning_override = match &req.versioning_override {
                    VersioningOverrideChange::Set(override_) => {
                        FieldChange::Set(to_internal::versioning_override_to_kernel(override_))
                    }
                    VersioningOverrideChange::Clear => FieldChange::Clear,
                };
                let request = RequestContext {
                    request_id: RequestId(Uuid::new_v4().to_string()),
                    caller_identity: (!req.identity.is_empty()).then(|| req.identity.clone()),
                    principal: ctx.event_principal(),
                    received_at: OffsetDateTime::now_utc(),
                };

                self.runtime
                    .update_workflow_execution_options(run_key, versioning_override, request)
                    .await
                    .map_err(EdgeError::from)?;

                // The post-update value mirrors the applied change (the only mutable
                // option tokeira models): `Some` after a Set, `None` after a Clear.
                let versioning_override = match req.versioning_override {
                    VersioningOverrideChange::Set(override_) => Some(override_),
                    VersioningOverrideChange::Clear => None,
                };
                Ok(UpdateWorkflowExecutionOptionsResponse {
                    versioning_override,
                })
            },
        )
        .await
    }

    /// Reads the custom search-attribute catalog for WorkflowService callers.
    ///
    /// Temporal exposes this catalog on WorkflowService even though custom
    /// attribute mutation lives on OperatorService. The edge therefore
    /// authorizes it as an operator read and delegates to the same catalog
    /// source instead of creating a second registry.
    pub async fn get_search_attributes(
        &self,
        headers: &HeaderMap,
    ) -> EdgeResult<Vec<SearchAttributeDefinition>> {
        self.observe_edge_call(headers, "get_search_attributes", None, None, async move {
            let _ctx = self
                .interceptors
                .begin(headers, None, Action::GetSearchAttributes, false)
                .await?;

            self.operator_api
                .list_search_attributes(None)
                .await
                .map_err(EdgeError::from)
        })
        .await
    }

    pub async fn delete_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: DeleteWorkflowExecutionRequest,
    ) -> EdgeResult<()> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "delete_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::DeleteWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let now = OffsetDateTime::now_utc();
                let deletion = self
                    .runtime
                    .delete_workflow(
                        run_key,
                        DeleteWorkflowRequest {
                            request: RequestContext {
                                request_id: RequestId(ctx.request_id.as_str().to_string()),
                                caller_identity: None,
                                principal: ctx.event_principal(),
                                received_at: ctx.received_at,
                            },
                            now,
                        },
                    )
                    .await
                    .map_err(|error| {
                        map_workflow_deletion_error(error, &req.namespace, &req.workflow_id)
                    })?;
                self.visibility
                    .apply_deletion(deletion.tombstone)
                    .await
                    .map_err(EdgeError::from)
            },
        )
        .await
    }

    pub async fn reset_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: ResetWorkflowExecutionRequest,
    ) -> EdgeResult<ResetWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "reset_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::ResetWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let history = self
                    .repo
                    .read_history(run_key, 0, usize::MAX)
                    .await
                    .map_err(EdgeError::from)?;
                validate_reset_target(&history, req.workflow_task_finish_event_id)?;

                // Reset targets the RESOLVED base run explicitly. A no-run-id reset
                // of a closed current resolves via `find_latest_run` above, but the
                // request itself carries no run id — pass the resolved run's id so
                // the runtime does not re-resolve open-only (which would miss a
                // closed base).
                let base_run_id =
                    match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                        tokeira_kernel::LoadedRun::Existing(state) => Some(state.run_id),
                        tokeira_kernel::LoadedRun::Absent => None,
                    };
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
                    run_id: base_run_id,
                };
                let internal = to_internal::reset_request(req, &ctx);
                let outcome = self
                    .runtime
                    .reset_workflow(execution, internal)
                    .await
                    .map_err(EdgeError::from)?;

                let last_event_id =
                    read_last_event_id(self.repo.as_ref(), outcome.successor_run_key).await?;
                self.notify_history_run_key(outcome.successor_run_key, last_event_id)
                    .await;

                Ok(from_internal::reset_response(outcome))
            },
        )
        .await
    }

    pub async fn signal_with_start_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: SignalWithStartWorkflowExecutionRequest,
    ) -> EdgeResult<SignalWithStartWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "signal_with_start_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::SignalWithStartWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;
                let internal = to_internal::signal_with_start_request(req.clone(), &ctx);
                match self
                    .runtime
                    .signal_with_start_workflow(internal)
                    .await
                    .map_err(EdgeError::from)?
                {
                    SignalWithStartResult::Started { run_key, run_id } => {
                        let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                        self.notify_history_run_key(run_key, last_event_id).await;
                        Ok(SignalWithStartWorkflowExecutionResponse {
                            run_id,
                            started: true,
                        })
                    }
                    SignalWithStartResult::Signaled { run_key, run_id } => {
                        let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                        self.notify_history_run_key(run_key, last_event_id).await;
                        Ok(SignalWithStartWorkflowExecutionResponse {
                            run_id,
                            started: false,
                        })
                    }
                    SignalWithStartResult::Rejected { run_id, reason, .. } => {
                        Err(EdgeError::WorkflowStartRejected {
                            message: start_reject_message(reason, &req.workflow_id, run_id),
                            run_id: run_id.0.to_string(),
                        })
                    }
                }
            },
        )
        .await
    }

    /// Execute the composed Update-with-Start: exactly `[Start, Update]`
    /// against one workflow id (`ExecuteMultiOperation` @ v1.31.0).
    ///
    /// Shape and per-operation field validation already happened at the gRPC
    /// boundary (validate before mutate, Req 1 / Property 1); this mirrors
    /// [`signal_with_start_workflow_execution`](Self::signal_with_start_workflow_execution)'s
    /// structure — interceptor begin, local-route check, start translation —
    /// then delegates path selection to the runtime composition. A
    /// post-validation leg failure surfaces as
    /// [`ExecuteMultiOperationOutcome::Failed`] rather than an `EdgeError`
    /// because the wire shape needs the failing leg's own status *plus* the
    /// aborted sibling (`MultiOperationExecutionFailure`, Req 4).
    pub async fn execute_multi_operation(
        &self,
        headers: &HeaderMap,
        req: ExecuteMultiOperationRequest,
    ) -> EdgeResult<ExecuteMultiOperationOutcome> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "execute_multi_operation",
            Some(namespace_label.as_str()),
            None,
            async move {
                // No dedicated multi-operation Action exists: the composition
                // is authorized as its opening start leg, matching how the
                // request routes by `operations[0].workflow_id`.
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::StartWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.start.workflow_id)
                        .await?,
                )?;

                let workflow_id = req.start.workflow_id.clone();
                let internal = to_internal::start_request(req.start, &ctx);

                // Wait-policy defaulting mirrors standalone update
                // (`update_workflow_execution` above): Unspecified waits for
                // Completed; ADMITTED was already rejected at translation, so
                // hitting it here is a defensive rejection, not a new path.
                let wait_policy = match req.update.wait_policy {
                    crate::translate::UpdateWaitPolicyDto::Unspecified
                    | crate::translate::UpdateWaitPolicyDto::Completed => {
                        UpdateWaitPolicy::Completed
                    }
                    crate::translate::UpdateWaitPolicyDto::Accepted => UpdateWaitPolicy::Accepted,
                    crate::translate::UpdateWaitPolicyDto::Admitted => {
                        return Err(EdgeError::BadRequest(
                            "UpdateWorkflowExecution does not support waiting for ADMITTED"
                                .to_string(),
                        ));
                    }
                };
                // Same per-update RequestContext shape as standalone update,
                // plus the update `Meta.identity` threaded into
                // `caller_identity` (v1.31.0 records the update's identity on
                // the admitted transport, not the start identity).
                let update_request = RequestContext {
                    request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()),
                    caller_identity: req.update_identity,
                    principal: ctx.event_principal(),
                    received_at: time::OffsetDateTime::now_utc(),
                };

                match self
                    .runtime
                    .execute_multi_operation(
                        internal,
                        req.update.update_id,
                        req.update.update_name,
                        req.update.input,
                        update_request,
                        req.update.timeout,
                        wait_policy,
                    )
                    .await
                {
                    Ok(result) => {
                        let last_event_id =
                            read_last_event_id(self.repo.as_ref(), result.run_key).await?;
                        self.notify_history_run_key(result.run_key, last_event_id)
                            .await;
                        Ok(ExecuteMultiOperationOutcome::Completed(
                            ExecuteMultiOperationResponse {
                                run_id: result.run_id,
                                started: result.started,
                                status: result.execution_status,
                                update: from_internal::update_response(result.update),
                            },
                        ))
                    }
                    Err(error) => match error.downcast::<MultiOperationError>() {
                        // Start leg rejected by conflict/reuse policy: op0
                        // carries the SAME `WorkflowExecutionAlreadyStarted`
                        // error standalone start produces for that reason
                        // (Req 4.2); op1 aborts as the sibling.
                        Ok(MultiOperationError::StartRejected { run_id, reason, .. }) => {
                            Ok(ExecuteMultiOperationOutcome::Failed(
                                MultiOperationFailure::Start(EdgeError::WorkflowStartRejected {
                                    message: start_reject_message(reason, &workflow_id, run_id),
                                    run_id: run_id.0.to_string(),
                                }),
                            ))
                        }
                        // Update leg failed: run its source through the same
                        // anyhow→EdgeError pipeline standalone update uses so
                        // typed aborts (NotFound closing-abort,
                        // ResourceExhausted WorkflowClosing) keep their codes
                        // and details (Req 4.6).
                        Ok(MultiOperationError::UpdateFailed { started, source }) => Ok(
                            ExecuteMultiOperationOutcome::Failed(MultiOperationFailure::Update {
                                started,
                                error: map_update_lifecycle_error(
                                    source,
                                    &req.namespace,
                                    &workflow_id,
                                ),
                            }),
                        ),
                        // Anything else is a plain edge failure with no
                        // structured multi-operation detail, matching the
                        // frontend, which only wraps per-operation errors.
                        Err(error) => Err(EdgeError::from(error)),
                    },
                }
            },
        )
        .await
    }

    // ── Activity endpoints ──

    pub async fn poll_activity_task_queue(
        &self,
        headers: &HeaderMap,
        req: PollActivityTaskQueueRequest,
    ) -> EdgeResult<Option<PollActivityTaskQueueResponse>> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_activity_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PollActivityTaskQueue,
                        true,
                    )
                    .await?;
                let namespace_id = resolved_namespace_id(&ctx, &req.namespace)?;
                let workflow_namespace = ctx
                    .namespace
                    .as_ref()
                    .map(|namespace| namespace.name.clone())
                    .ok_or_else(|| {
                        EdgeError::Internal(
                            "activity poll admission returned no namespace".to_owned(),
                        )
                    })?;

                ensure_local(
                    self.router
                        .route_task_queue(&req.namespace, &req.task_queue, TaskKind::Activity)
                        .await?,
                )?;

                let _permit = self.long_polls.acquire().await?;
                let poller = self.poller_registry.register(
                    queue_key_for_poll(
                        &req.namespace,
                        &req.task_queue,
                        TaskKind::Activity,
                        req.deployment.clone(),
                        req.build_id.clone(),
                    ),
                    WorkerIdentity(req.worker_identity.clone()),
                );
                // Activity polls participate in the same matching-driven Worker
                // Deployment auto-registration as workflow polls. v1.31.0 reports
                // both queue types in DescribeWorkerDeploymentVersion once each has
                // polled (`service/worker/workerdeployment/client.go:1230 @ v1.31.0`).
                if let (Some(worker_deployments), Some(deployment), Some(build_id)) = (
                    self.worker_deployments.as_ref(),
                    req.deployment.as_ref(),
                    req.build_id.as_ref(),
                ) && let Err(error) = worker_deployments
                    .register_polled_deployment(RegisterPolledDeployment {
                        namespace_id: to_internal::namespace_id_for(&req.namespace),
                        deployment_name: DeploymentName(deployment.0.clone()),
                        build_id: tokeira_storage::BuildId(build_id.0.clone()),
                        task_queue: req.task_queue.clone(),
                        task_queue_type: DeploymentTaskQueueType::Activity,
                        identity: req.worker_identity.clone(),
                    })
                    .await
                {
                    tracing::warn!(
                        %error,
                        deployment = %deployment.0,
                        build_id = %build_id.0,
                        "failed to auto-register activity worker deployment"
                    );
                }
                let internal = to_internal::poll_activity_request(req);
                let scaling_queue = internal.queue.clone();
                if internal.queue.deployment.is_some() {
                    self.runtime
                        .absorb_unversioned_backlog(&internal.queue)
                        .await;
                }
                let configured_rate = self
                    .task_queue_config_store
                    .get(&internal.queue.namespace_id, &internal.queue.task_queue)
                    .and_then(|config| config.queue_rate_limit)
                    .map(f64::from);
                let effective_rate = configured_rate.or(internal.worker_rate_limit);
                if let Some(rate) = effective_rate
                    && !self
                        .task_queue_rate_limiter
                        .acquire(
                            internal.queue.namespace_id,
                            internal.queue.task_queue.clone(),
                            rate,
                            internal.timeout,
                        )
                        .await
                {
                    poller.completed();
                    return Ok(None);
                }
                // The runtime evaluates durable rules when the broker offer is about to become a
                // Started transition. Poll admission deliberately carries no CRUD-gate decision.
                let started = self
                    .runtime
                    .poll_activity_task(internal.queue, internal.worker_identity, internal.timeout)
                    .await;
                // A normal empty result is the long-poll timeout path and must
                // refresh, while client cancellation drops the future before
                // this explicit completion (`task_queue_partition_manager.go:
                // 617-621 @ v1.31.0`).
                poller.completed();
                let started = started.map_err(EdgeError::from)?;
                let scaling_decision = if started.is_some() {
                    self.runtime
                        .activity_poller_scaling_decision(&scaling_queue)
                        .await
                } else {
                    None
                };

                match started {
                    Some(started) => {
                        let mut response = from_internal::poll_activity_response(
                            started,
                            namespace_id,
                            &workflow_namespace,
                        )
                        .map_err(EdgeError::from)?;
                        response.poller_scaling_decision = scaling_decision;
                        Ok(Some(response))
                    }
                    None => Ok(None),
                }
            },
        )
        .await
    }

    pub async fn respond_activity_task_completed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCompletedRequest,
    ) -> EdgeResult<RespondActivityTaskCompletedResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_completed",
            None,
            None,
            async move {
                let (token, token_namespace_id, ctx, effective_namespace) = self
                    .admit_json_task_token::<ActivityTaskToken>(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RespondActivityTaskCompleted,
                    )
                    .await?;
                self.validate_legacy_task_namespace(
                    &effective_namespace,
                    token_namespace_id,
                    token.run_key,
                )
                .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                let _outcome = self
                    .runtime
                    .complete_activity_task(
                        token.clone(),
                        req.result,
                        Some(tokeira_types::WorkerIdentity(req.identity)),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    token.run_key,
                    read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                )
                .await;

                Ok(RespondActivityTaskCompletedResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_failed(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskFailedRequest,
    ) -> EdgeResult<RespondActivityTaskFailedResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_failed",
            None,
            None,
            async move {
                let (token, token_namespace_id, ctx, effective_namespace) = self
                    .admit_json_task_token::<ActivityTaskToken>(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RespondActivityTaskFailed,
                    )
                    .await?;
                self.validate_legacy_task_namespace(
                    &effective_namespace,
                    token_namespace_id,
                    token.run_key,
                )
                .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                self.runtime
                    .fail_activity_task(
                        token.clone(),
                        req.failure,
                        req.failure_error_type,
                        req.is_non_retryable,
                        Some(tokeira_types::WorkerIdentity(req.identity)),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    token.run_key,
                    read_last_event_id(self.repo.as_ref(), token.run_key).await?,
                )
                .await;

                Ok(RespondActivityTaskFailedResponse)
            },
        )
        .await
    }

    /// `RespondWorkflowTaskFailed`. Routing on cause happens in the runtime:
    /// `GrpcMessageTooLarge` force-close-terminates the run
    /// (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`); other causes take
    /// the WFT-failed retry path.
    pub async fn respond_workflow_task_failed(
        &self,
        headers: &HeaderMap,
        namespace: String,
        task_token: Vec<u8>,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<tokeira_types::Payload>,
        identity: String,
    ) -> EdgeResult<()> {
        self.observe_edge_call(
            headers,
            "respond_workflow_task_failed",
            None,
            None,
            async move {
                let (token, token_namespace_id, ctx, effective_namespace) = self
                    .admit_json_task_token::<tokeira_types::WorkflowTaskToken>(
                        headers,
                        &namespace,
                        &task_token,
                        Action::RespondWorkflowTaskFailed,
                    )
                    .await?;
                self.validate_legacy_task_namespace(
                    &effective_namespace,
                    token_namespace_id,
                    token.run_key,
                )
                .await?;

                let run_key = token.run_key;
                self.runtime
                    .fail_workflow_task(
                        token,
                        failure_cause,
                        failure_details,
                        tokeira_types::WorkerIdentity(identity),
                        tokeira_types::RequestContext {
                            request_id: tokeira_types::RequestId(
                                ctx.request_id.as_str().to_string(),
                            ),
                            caller_identity: None,
                            principal: ctx.event_principal(),
                            received_at: time::OffsetDateTime::now_utc(),
                        },
                        time::OffsetDateTime::now_utc(),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    run_key,
                    read_last_event_id(self.repo.as_ref(), run_key).await?,
                )
                .await;

                Ok(())
            },
        )
        .await
    }

    pub async fn record_activity_task_heartbeat(
        &self,
        headers: &HeaderMap,
        req: RecordActivityTaskHeartbeatRequest,
    ) -> EdgeResult<RecordActivityTaskHeartbeatResponse> {
        self.observe_edge_call(
            headers,
            "record_activity_task_heartbeat",
            None,
            None,
            async move {
                let (token, token_namespace_id, _ctx, effective_namespace) = self
                    .admit_json_task_token::<ActivityTaskToken>(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RecordActivityTaskHeartbeat,
                    )
                    .await?;
                self.validate_legacy_task_namespace(
                    &effective_namespace,
                    token_namespace_id,
                    token.run_key,
                )
                .await?;

                let outcome = self
                    .runtime
                    .record_activity_heartbeat(
                        token,
                        req.details,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;

                Ok(RecordActivityTaskHeartbeatResponse {
                    cancel_requested: outcome.cancel_requested,
                    activity_paused: outcome.activity_paused,
                    activity_reset: outcome.activity_reset,
                })
            },
        )
        .await
    }

    pub async fn respond_activity_task_canceled(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCanceledRequest,
    ) -> EdgeResult<RespondActivityTaskCanceledResponse> {
        self.observe_edge_call(
            headers,
            "respond_activity_task_canceled",
            None,
            None,
            async move {
                let (token, token_namespace_id, ctx, effective_namespace) = self
                    .admit_json_task_token::<ActivityTaskToken>(
                        headers,
                        &req.namespace,
                        &req.task_token,
                        Action::RespondActivityTaskCanceled,
                    )
                    .await?;
                self.validate_legacy_task_namespace(
                    &effective_namespace,
                    token_namespace_id,
                    token.run_key,
                )
                .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                let outcome = self
                    .runtime
                    .cancel_activity_task(
                        token.clone(),
                        req.details,
                        worker_identity_from_request(req.identity),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(token.run_key, outcome.last_event_id)
                    .await;

                Ok(RespondActivityTaskCanceledResponse)
            },
        )
        .await
    }

    pub async fn record_activity_task_heartbeat_by_id(
        &self,
        headers: &HeaderMap,
        req: RecordActivityTaskHeartbeatByIdRequest,
    ) -> EdgeResult<RecordActivityTaskHeartbeatByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "record_activity_task_heartbeat_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RecordActivityTaskHeartbeat,
                        false,
                    )
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = match self
                    .runtime
                    .resolve_activity_token(run_key, &req.activity_id)
                    .await
                {
                    Ok(token) => token,
                    Err(ActivityTokenResolutionError::ActivityNotStarted { .. }) => {
                        // v1.31.0's by-id heartbeat builds a token with an empty
                        // scheduled/started event id and calls the same history
                        // RPC as the token path. `IsActivityTaskNotFoundForToken`
                        // (activity_util.go:58 @ v1.31.0, invoked with nil
                        // `isCompletedByID`) returns not-found whenever
                        // `StartedEventId` is empty — there is no by-id exemption
                        // for an unstarted activity, and the token heartbeat path
                        // already rejects it the same way.
                        return Err(EdgeError::NotFound(
                            "invalid activityID or activity already timed out or invoking \
                             workflow is completed"
                                .to_string(),
                        ));
                    }
                    Err(error) => {
                        return Err(self.map_activity_resolution_error(
                            error,
                            &req.namespace,
                            &req.workflow_id,
                            &req.activity_id,
                        ));
                    }
                };
                let outcome = self
                    .runtime
                    .record_activity_heartbeat(
                        token,
                        req.details,
                        worker_identity_from_request(req.identity),
                    )
                    .await
                    .map_err(EdgeError::from)?;
                Ok(RecordActivityTaskHeartbeatByIdResponse {
                    cancel_requested: outcome.cancel_requested,
                    activity_paused: outcome.activity_paused,
                    activity_reset: outcome.activity_reset,
                })
            },
        )
        .await
    }

    pub async fn respond_activity_task_completed_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCompletedByIdRequest,
    ) -> EdgeResult<RespondActivityTaskCompletedByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_completed_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RespondActivityTaskCompleted,
                        false,
                    )
                    .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = match self
                    .runtime
                    .resolve_activity_token(run_key, &req.activity_id)
                    .await
                {
                    Ok(token) => token,
                    // Completed-by-id FORCE-completes a not-yet-started
                    // activity: v1.31.0 fabricates the started event with the
                    // completing caller's identity and proceeds
                    // (`respondactivitytaskcompleted/api.go:89-105 @ v1.31.0`).
                    // Only this verb does — failed/canceled/heartbeat by-id
                    // reject unstarted activities (nil `isCompletedByID`,
                    // activity_util.go:58-67).
                    Err(ActivityTokenResolutionError::ActivityNotStarted { .. }) => self
                        .runtime
                        .force_start_activity_for_completion(
                            run_key,
                            &req.activity_id,
                            tokeira_types::WorkerIdentity(req.identity.clone()),
                            request.clone(),
                        )
                        .await
                        .map_err(EdgeError::from)?,
                    Err(error) => {
                        return Err(self.map_activity_resolution_error(
                            error,
                            &req.namespace,
                            &req.workflow_id,
                            &req.activity_id,
                        ));
                    }
                };
                let outcome = self
                    .runtime
                    .complete_activity_task(
                        token,
                        req.result,
                        worker_identity_from_request(req.identity),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(RespondActivityTaskCompletedByIdResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_failed_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskFailedByIdRequest,
    ) -> EdgeResult<RespondActivityTaskFailedByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_failed_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RespondActivityTaskFailed,
                        false,
                    )
                    .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = self
                    .resolve_activity_token_for_edge(
                        run_key,
                        &req.activity_id,
                        &req.namespace,
                        &req.workflow_id,
                    )
                    .await?;
                self.runtime
                    .fail_activity_task(
                        token,
                        req.failure,
                        req.failure_error_type,
                        req.is_non_retryable,
                        worker_identity_from_request(req.identity),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(
                    run_key,
                    read_last_event_id(self.repo.as_ref(), run_key).await?,
                )
                .await;
                Ok(RespondActivityTaskFailedByIdResponse)
            },
        )
        .await
    }

    pub async fn respond_activity_task_canceled_by_id(
        &self,
        headers: &HeaderMap,
        req: RespondActivityTaskCanceledByIdRequest,
    ) -> EdgeResult<RespondActivityTaskCanceledByIdResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "respond_activity_task_canceled_by_id",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RespondActivityTaskCanceled,
                        false,
                    )
                    .await?;
                let request =
                    activity_control_request_context(&ctx, &req.identity, ctx.received_at);
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let token = self
                    .resolve_activity_token_for_edge(
                        run_key,
                        &req.activity_id,
                        &req.namespace,
                        &req.workflow_id,
                    )
                    .await?;
                let outcome = self
                    .runtime
                    .cancel_activity_task(
                        token,
                        req.details,
                        worker_identity_from_request(req.identity),
                        request,
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(RespondActivityTaskCanceledByIdResponse)
            },
        )
        .await
    }

    pub async fn update_activity_options(
        &self,
        headers: &HeaderMap,
        req: UpdateActivityOptionsRequest,
    ) -> EdgeResult<UpdateActivityOptionsResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_activity_options",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateActivityOptions,
                        false,
                    )
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let command = build_update_activity_options_command(&ctx, &req)?;
                let outcome = self
                    .runtime
                    .update_activity_options(run_key, command)
                    .await
                    .map_err(EdgeError::from)?;
                let activity_options = self
                    .load_activity_options_for_target(
                        run_key,
                        &req.namespace,
                        &req.workflow_id,
                        &req.target,
                    )
                    .await?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(UpdateActivityOptionsResponse {
                    activity_options: Some(activity_options),
                })
            },
        )
        .await
    }

    pub async fn pause_activity(
        &self,
        headers: &HeaderMap,
        req: PauseActivityRequest,
    ) -> EdgeResult<PauseActivityResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "pause_activity",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(headers, Some(&req.namespace), Action::PauseActivity, false)
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let now = time::OffsetDateTime::now_utc();
                let outcome = self
                    .runtime
                    .pause_activities(
                        run_key,
                        tokeira_kernel::PauseActivityRequest {
                            target: activity_control_target(req.target)?,
                            identity: req.identity.clone(),
                            reason: req.reason,
                            rule_id: None,
                            request: activity_control_request_context(&ctx, &req.identity, now),
                            now,
                        },
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(PauseActivityResponse)
            },
        )
        .await
    }

    pub async fn unpause_activity(
        &self,
        headers: &HeaderMap,
        req: UnpauseActivityRequest,
    ) -> EdgeResult<UnpauseActivityResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "unpause_activity",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UnpauseActivity,
                        false,
                    )
                    .await?;
                validate_activity_jitter(req.jitter)?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let now = time::OffsetDateTime::now_utc();
                let outcome = self
                    .runtime
                    .unpause_activities(
                        run_key,
                        tokeira_runtime::UnpauseActivitiesRequest {
                            target: activity_control_target(req.target)?,
                            reset_attempts: req.reset_attempts,
                            reset_heartbeat: req.reset_heartbeat,
                            jitter: req.jitter,
                            request: activity_control_request_context(&ctx, &req.identity, now),
                            now,
                        },
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(UnpauseActivityResponse)
            },
        )
        .await
    }

    pub async fn reset_activity(
        &self,
        headers: &HeaderMap,
        req: ResetActivityRequest,
    ) -> EdgeResult<ResetActivityResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "reset_activity",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(headers, Some(&req.namespace), Action::ResetActivity, false)
                    .await?;
                validate_activity_jitter(req.jitter)?;
                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let now = time::OffsetDateTime::now_utc();
                let outcome = self
                    .runtime
                    .reset_activities(
                        run_key,
                        tokeira_runtime::ResetActivitiesRequest {
                            target: activity_control_target(req.target)?,
                            reset_heartbeat: req.reset_heartbeat,
                            keep_paused: req.keep_paused,
                            jitter: req.jitter,
                            restore_original_options: req.restore_original_options,
                            request: activity_control_request_context(&ctx, &req.identity, now),
                            now,
                        },
                    )
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;
                Ok(ResetActivityResponse)
            },
        )
        .await
    }

    // ── Advanced workflow endpoints ──

    /// `ResetStickyTaskQueue`: clear the run's sticky affinity (sticky raise
    /// S5). Deliberately leaves any pending sticky-dispatched WFT and its
    /// schedule-to-start deadline in place — v1.31.0's reset only clears
    /// mutable-state stickiness; the dispatched task still times out onto the
    /// normal queue (stickytq leaf 2).
    pub async fn reset_sticky_task_queue(
        &self,
        headers: &HeaderMap,
        namespace: String,
        workflow_id: String,
        run_id: Option<String>,
    ) -> EdgeResult<()> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "reset_sticky_task_queue",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&namespace),
                        Action::ResetStickyTaskQueue,
                        false,
                    )
                    .await?;
                let run_key = self
                    .resolve_execution_run_key(&namespace, &workflow_id, run_id.as_deref())
                    .await?;
                self.runtime
                    .reset_sticky_task_queue(run_key)
                    .await
                    .map_err(EdgeError::from)?;
                Ok(())
            },
        )
        .await
    }

    pub async fn terminate_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: TerminateWorkflowExecutionRequest,
    ) -> EdgeResult<TerminateWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "terminate_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::TerminateWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::terminate_request(req, &ctx);
                let outcome = self
                    .runtime
                    .terminate_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::terminate_response(outcome))
            },
        )
        .await
    }

    pub async fn pause_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: PauseWorkflowExecutionRequest,
    ) -> EdgeResult<PauseWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "pause_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::PauseWorkflowExecution,
                        false,
                    )
                    .await?;

                if req.workflow_id.is_empty() {
                    return Err(EdgeError::BadRequest(
                        "pause_workflow_execution requires workflow_id".to_string(),
                    ));
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::pause_request(req, &ctx);
                let outcome = self
                    .runtime
                    .pause_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(PauseWorkflowExecutionResponse)
            },
        )
        .await
    }

    pub async fn unpause_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: UnpauseWorkflowExecutionRequest,
    ) -> EdgeResult<UnpauseWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "unpause_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UnpauseWorkflowExecution,
                        false,
                    )
                    .await?;

                if req.workflow_id.is_empty() {
                    return Err(EdgeError::BadRequest(
                        "unpause_workflow_execution requires workflow_id".to_string(),
                    ));
                }

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::unpause_request(req, &ctx);
                let outcome = self
                    .runtime
                    .unpause_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(UnpauseWorkflowExecutionResponse)
            },
        )
        .await
    }

    pub async fn request_cancel_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: RequestCancelWorkflowExecutionRequest,
    ) -> EdgeResult<RequestCancelWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "request_cancel_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::RequestCancelWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let internal = to_internal::cancel_request(req, &ctx);
                let outcome = self
                    .runtime
                    .cancel_workflow(run_key, internal)
                    .await
                    .map_err(EdgeError::from)?;
                self.notify_history_run_key(run_key, outcome.last_event_id)
                    .await;

                Ok(from_internal::cancel_response(outcome))
            },
        )
        .await
    }

    /// Execute a synchronous query against a workflow.
    ///
    /// This delegates to the runtime's `query_workflow`, which internally
    /// uses a two-path dispatch: if the run is idle (quiescent), the query
    /// is sent directly through the broker to a poller; if the run has an
    /// active WFT, the query is buffered behind a consistency barrier and
    /// attached to the next poll response. The edge layer doesn't need to
    /// know which path was taken — the runtime handles the routing and
    /// returns the result through the same `QueryResult` channel.
    pub async fn query_workflow(
        &self,
        headers: &HeaderMap,
        req: QueryWorkflowRequest,
    ) -> EdgeResult<QueryWorkflowResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "query_workflow",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(headers, Some(&req.namespace), Action::QueryWorkflow, false)
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let _run_key = self
                    .resolve_run_key(&req.namespace, &req.workflow_id)
                    .await?;

                let workflow_id = req.workflow_id.clone();
                let execution =
                    ExecutionRef {
                        namespace_id: to_internal::namespace_id_for(&req.namespace),
                        workflow_id: tokeira_types::WorkflowId(workflow_id),
                        // A run-id-pinned query targets exactly that run
                        // (queryworkflow/api.go:53-62 resolves the current run
                        // only when the request's run id is empty). A malformed
                        // run id is rejected up front, as the frontend's
                        // validateExecution does (workflow_handler.go:3134
                        // @ v1.31.0).
                        run_id: match req.run_id.as_deref().filter(|value| !value.is_empty()) {
                            Some(value) => Some(RunId(Uuid::parse_str(value).map_err(|_| {
                                EdgeError::BadRequest("Invalid RunId.".to_string())
                            })?)),
                            None => None,
                        },
                    };

                let result = self
                    .runtime
                    .query_workflow(execution, req.query_type, req.query_args, req.timeout)
                    .await
                    .map_err(EdgeError::from)?;

                // A worker-failed query is the typed `QueryFailed` ERROR on
                // the wire (INVALID_ARGUMENT + QueryFailedFailure detail),
                // never an empty success (matching_engine.go:1126-1127 +
                // serviceerror/query_failed.go @ v1.62).
                if let tokeira_runtime::QueryResult::Failed { message, failure } = result {
                    return Err(EdgeError::QueryFailed { message, failure });
                }

                Ok(from_internal::query_response(result))
            },
        )
        .await
    }

    /// Submit a workflow update and optionally wait for its outcome.
    ///
    /// The `wait_policy` controls how long the caller blocks. The update RPC
    /// defaults an absent/unspecified policy to `Completed` and rejects
    /// `Admitted`; poll requests preserve all stages so callers can ask for the
    /// current lifecycle state without blocking.
    pub async fn update_workflow_execution(
        &self,
        headers: &HeaderMap,
        req: UpdateWorkflowExecutionRequest,
    ) -> EdgeResult<UpdateWorkflowExecutionResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "update_workflow_execution",
            Some(namespace_label.as_str()),
            None,
            async move {
                let ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::UpdateWorkflowExecution,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let state = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                    LoadedRun::Existing(state) => state,
                    LoadedRun::Absent => {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace: req.namespace.clone(),
                            workflow_id: req.workflow_id.clone(),
                        });
                    }
                };
                if let Some(first_execution_run_id) = req
                    .first_execution_run_id
                    .as_deref()
                    .filter(|value| !value.is_empty())
                {
                    let first_execution_run_id = Uuid::parse_str(first_execution_run_id)
                        .map(RunId)
                        .map_err(|err| {
                            EdgeError::BadRequest(format!(
                                "invalid first_execution_run_id `{first_execution_run_id}`: {err}"
                            ))
                        })?;
                    if state.first_execution_run_id != Some(first_execution_run_id) {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace: req.namespace.clone(),
                            workflow_id: req.workflow_id.clone(),
                        });
                    }
                }

                let update_id = if req.update_id.is_empty() {
                    Uuid::new_v4().to_string()
                } else {
                    req.update_id
                };
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&req.namespace),
                    workflow_id: tokeira_types::WorkflowId(req.workflow_id.clone()),
                    run_id: Some(state.run_id),
                };

                let wait_policy = match req.wait_policy {
                    crate::translate::UpdateWaitPolicyDto::Unspecified => {
                        UpdateWaitPolicy::Completed
                    }
                    crate::translate::UpdateWaitPolicyDto::Admitted => {
                        return Err(EdgeError::BadRequest(
                            "UpdateWorkflowExecution does not support waiting for ADMITTED"
                                .to_string(),
                        ));
                    }
                    crate::translate::UpdateWaitPolicyDto::Accepted => UpdateWaitPolicy::Accepted,
                    crate::translate::UpdateWaitPolicyDto::Completed => UpdateWaitPolicy::Completed,
                };

                let request = RequestContext {
                    request_id: tokeira_types::RequestId(uuid::Uuid::new_v4().to_string()),
                    caller_identity: None,
                    principal: ctx.event_principal(),
                    received_at: time::OffsetDateTime::now_utc(),
                };

                let outcome = self
                    .runtime
                    .update_workflow(
                        execution,
                        update_id,
                        req.update_name,
                        req.input,
                        request,
                        req.timeout,
                        wait_policy,
                    )
                    .await
                    .map_err(|error| {
                        map_update_lifecycle_error(error, &req.namespace, &req.workflow_id)
                    })?;
                let last_event_id = read_last_event_id(self.repo.as_ref(), run_key).await?;
                self.notify_history_run_key(run_key, last_event_id).await;

                Ok(from_internal::update_response(outcome))
            },
        )
        .await
    }

    pub async fn poll_workflow_execution_update(
        &self,
        headers: &HeaderMap,
        namespace: String,
        workflow_id: String,
        run_id_str: String,
        update_id: String,
        wait_policy: UpdateWaitPolicy,
    ) -> EdgeResult<UpdateLifecycleSnapshot> {
        let namespace_label = namespace.clone();
        self.observe_edge_call(
            headers,
            "poll_workflow_execution_update",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&namespace),
                        Action::PollWorkflowExecutionUpdate,
                        false,
                    )
                    .await?;

                ensure_local(self.router.route_workflow(&namespace, &workflow_id).await?)?;

                let run_key = self
                    .resolve_execution_run_key(
                        &namespace,
                        &workflow_id,
                        Some(run_id_str.as_str()).filter(|value| !value.is_empty()),
                    )
                    .await?;
                let state = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
                    LoadedRun::Existing(state) => state,
                    LoadedRun::Absent => {
                        return Err(EdgeError::WorkflowNotFound {
                            namespace,
                            workflow_id,
                        });
                    }
                };
                let execution = ExecutionRef {
                    namespace_id: to_internal::namespace_id_for(&namespace),
                    workflow_id: tokeira_types::WorkflowId(workflow_id.clone()),
                    run_id: Some(state.run_id),
                };

                self.runtime
                    .poll_workflow_update(
                        execution,
                        update_id,
                        wait_policy,
                        std::time::Duration::from_secs(60),
                    )
                    .await
                    .map_err(|error| map_update_lifecycle_error(error, &namespace, &workflow_id))
            },
        )
        .await
    }

    // ── History ──

    pub async fn get_workflow_execution_history(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "get_workflow_execution_history",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::GetWorkflowExecutionHistory,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let caller_last_event_id = decode_history_page_token(&req.next_page_token)
                    .map_err(EdgeError::BadRequest)?;
                // Transient events go to every client except the CLI/UI
                // (`ClientSupportsTranOrSpecEvents`, get_history_util.go:427 @ v1.31.0).
                let client_supports_transient_events = !matches!(
                    headers
                        .get("client-name")
                        .and_then(|value| value.to_str().ok()),
                    Some("temporal-cli") | Some("temporal-ui")
                );
                let limit = if req.maximum_page_size > 0 {
                    req.maximum_page_size
                } else {
                    usize::MAX
                };

                loop {
                    let history = self
                        .repo
                        .read_attributed_history(run_key, caller_last_event_id, limit)
                        .await
                        .map_err(EdgeError::from)?;
                    let current_last_event_id = history
                        .last()
                        .map(|attributed| attributed.event.event_id)
                        .unwrap_or(caller_last_event_id);
                    let filtered =
                        filter_attributed_history_events(&history, req.history_event_filter_type);

                    tracing::debug!(
                        run_key = ?run_key,
                        caller_last_event_id,
                        current_last_event_id,
                        total_events = history.len(),
                        filtered_count = filtered.len(),
                        filter_type = req.history_event_filter_type,
                        wait_new_event = req.wait_new_event,
                        "get_workflow_execution_history loop iteration"
                    );

                    if !filtered.is_empty() || !req.wait_new_event {
                        // v1.31.0 emits an empty next_page_token once there is no more history to
                        // return AND (the workflow is closed OR this is not a long-poll); a
                        // non-empty token tells the client to keep paging / following, an empty
                        // token tells it to stop. tokeira previously always encoded a token, so
                        // the Go `GetHistory` helper (loops until len(token)==0) span-looped
                        // forever against finished workflows — the source of the apparent suite
                        // hang. service/history/api/getworkflowexecutionhistory/api.go:488 (v1.31.0).
                        let more_events = history.len() >= limit;
                        let reached_close = history
                            .iter()
                            .any(|attributed| is_close_history_event(&attributed.event.kind));
                        let next_page_token =
                            if more_events || (req.wait_new_event && !reached_close) {
                                encode_history_page_token(current_last_event_id)
                            } else {
                                Vec::new()
                            };
                        let (mut events, mut history_principals): (Vec<_>, Vec<_>) = filtered
                            .into_iter()
                            .take(limit)
                            .map(|attributed| (attributed.event, attributed.principal))
                            .unzip();
                        // Transient-suffix synthesis (spec transient-wft Req B.7): on the
                        // FINAL page of an unfiltered read, append the transient (attempt>1)
                        // pending task's unpersisted Scheduled(+Started) at their virtual
                        // ids so a mid-retry reader sees the task Temporal would show
                        // (`appendTransientTasks`, getworkflowexecutionhistory/api.go:32-116
                        // @ v1.31.0). Gated off for CLI/UI clients
                        // (`ClientSupportsTranOrSpecEvents`, get_history_util.go:427: every
                        // client EXCEPT temporal-cli / temporal-ui receives them).
                        if next_page_token.is_empty()
                            && req.history_event_filter_type != 2
                            && client_supports_transient_events
                        {
                            append_transient_suffix(
                                &mut events,
                                &mut history_principals,
                                self.repo.as_ref(),
                                run_key,
                                current_last_event_id,
                            )
                            .await?;
                        }
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: events,
                            history_principals,
                            next_page_token,
                        });
                    }

                    if req.history_event_filter_type != 2
                        && current_last_event_id > caller_last_event_id
                    {
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: Vec::new(),
                            history_principals: Vec::new(),
                            next_page_token: encode_history_page_token(current_last_event_id),
                        });
                    }

                    let mut wait = self
                        .history_waiters
                        .receiver(run_key, current_last_event_id)
                        .await;
                    // Long-poll hold interval mirrors v1.31.0's
                    // `history.longPollExpirationInterval` default of 20s
                    // (`HistoryLongPollExpirationInterval`,
                    // common/dynamicconfig/constants.go @ v1.31.0): on expiry
                    // the poll returns an empty page with a live token and the
                    // client re-polls.
                    if tokio::time::timeout(Duration::from_secs(20), wait.changed())
                        .await
                        .is_err()
                    {
                        return Ok(crate::translate::GetWorkflowExecutionHistoryResponse {
                            history: Vec::new(),
                            history_principals: Vec::new(),
                            next_page_token: encode_history_page_token(current_last_event_id),
                        });
                    }
                }
            },
        )
        .await
    }

    pub async fn get_workflow_execution_history_reverse(
        &self,
        headers: &HeaderMap,
        req: crate::translate::GetWorkflowExecutionHistoryReverseRequest,
    ) -> EdgeResult<crate::translate::GetWorkflowExecutionHistoryReverseResponse> {
        let namespace_label = req.namespace.clone();
        self.observe_edge_call(
            headers,
            "get_workflow_execution_history_reverse",
            Some(namespace_label.as_str()),
            None,
            async move {
                let _ctx = self
                    .interceptors
                    .begin(
                        headers,
                        Some(&req.namespace),
                        Action::GetWorkflowExecutionHistoryReverse,
                        false,
                    )
                    .await?;

                ensure_local(
                    self.router
                        .route_workflow(&req.namespace, &req.workflow_id)
                        .await?,
                )?;

                let run_key = self
                    .resolve_execution_run_key(
                        &req.namespace,
                        &req.workflow_id,
                        req.run_id.as_deref(),
                    )
                    .await?;
                let history = self
                    .repo
                    .read_attributed_history(run_key, 0, usize::MAX)
                    .await
                    .map_err(EdgeError::from)?;

                let before_event_id = decode_reverse_history_page_token(&req.next_page_token)
                    .map_err(EdgeError::BadRequest)?;
                let limit = if req.maximum_page_size > 0 {
                    req.maximum_page_size
                } else {
                    usize::MAX
                };

                let mut reversed: Vec<_> = history
                    .into_iter()
                    .filter(|attributed| {
                        before_event_id
                            .map(|value| attributed.event.event_id < value)
                            .unwrap_or(true)
                    })
                    .collect();
                reversed.sort_by_key(|attributed| std::cmp::Reverse(attributed.event.event_id));

                let page: Vec<_> = reversed.into_iter().take(limit).collect();
                let next_page_token = page
                    .last()
                    .map(|attributed| encode_reverse_history_page_token(attributed.event.event_id))
                    .unwrap_or_default();
                let (history, history_principals): (Vec<_>, Vec<_>) = page
                    .into_iter()
                    .map(|attributed| (attributed.event, attributed.principal))
                    .unzip();

                Ok(
                    crate::translate::GetWorkflowExecutionHistoryReverseResponse {
                        history,
                        history_principals,
                        next_page_token,
                    },
                )
            },
        )
        .await
    }

    // ── Helpers ──

    async fn resolve_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
    ) -> EdgeResult<tokeira_types::RunKey> {
        self.resolver
            .current_run_key(namespace, workflow_id)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            })
    }

    async fn resolve_activity_token_for_edge(
        &self,
        run_key: RunKey,
        activity_id: &str,
        namespace: &str,
        workflow_id: &str,
    ) -> EdgeResult<ActivityTaskToken> {
        self.runtime
            .resolve_activity_token(run_key, activity_id)
            .await
            .map_err(|error| {
                self.map_activity_resolution_error(error, namespace, workflow_id, activity_id)
            })
    }

    fn map_activity_resolution_error(
        &self,
        error: ActivityTokenResolutionError,
        namespace: &str,
        workflow_id: &str,
        activity_id: &str,
    ) -> EdgeError {
        match error {
            ActivityTokenResolutionError::RunNotFound { .. } => EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            },
            ActivityTokenResolutionError::ActivityNotFound { .. } => EdgeError::ActivityNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                activity_id: activity_id.to_string(),
            },
            ActivityTokenResolutionError::ActivityNotStarted { .. } => {
                EdgeError::ActivityNotStarted {
                    namespace: namespace.to_string(),
                    workflow_id: workflow_id.to_string(),
                    activity_id: activity_id.to_string(),
                }
            }
            ActivityTokenResolutionError::Runtime(message) => EdgeError::Internal(message),
        }
    }

    async fn load_activity_options_for_target(
        &self,
        run_key: RunKey,
        namespace: &str,
        workflow_id: &str,
        target: &ActivityTarget,
    ) -> EdgeResult<crate::translate::ActivityOptions> {
        let loaded = self.repo.load_run(run_key).await.map_err(EdgeError::from)?;
        let tokeira_kernel::LoadedRun::Existing(state) = loaded else {
            return Err(EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            });
        };
        // Reuse the kernel's selector predicate/label so this response-builder
        // resolves exactly the set the command mutated (MatchAll is already
        // rejected upstream at command construction, so it cannot reach here).
        let kernel_target = activity_control_target(target.clone())?;
        let activity = state
            .activities
            .values()
            .rfind(|activity| kernel_target.matches(activity))
            .ok_or_else(|| EdgeError::ActivityNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                activity_id: kernel_target.label(),
            })?;
        Ok(crate::translate::ActivityOptions {
            task_queue: Some(activity.task_queue.0.clone()),
            schedule_to_close_timeout: activity.schedule_to_close_timeout,
            schedule_to_start_timeout: activity.schedule_to_start_timeout,
            start_to_close_timeout: activity.start_to_close_timeout,
            heartbeat_timeout: activity.heartbeat_timeout,
            retry_policy: activity.retry_policy.clone(),
        })
    }

    async fn resolve_execution_run_key(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> EdgeResult<RunKey> {
        let run_id = match run_id.filter(|value| !value.is_empty()) {
            Some(value) => Some(Uuid::parse_str(value).map(RunId).map_err(|err| {
                EdgeError::BadRequest(format!("invalid run_id `{value}`: {err}"))
            })?),
            None => None,
        };
        let execution = ExecutionRef {
            namespace_id: to_internal::namespace_id_for(namespace),
            workflow_id: tokeira_types::WorkflowId(workflow_id.to_string()),
            run_id,
        };
        if let Some(run_key) = self
            .repo
            .resolve_execution(&execution)
            .await
            .map_err(EdgeError::from)?
        {
            return Ok(run_key);
        }
        // `resolve_execution(run_id=None)` is open-only by repo contract; history reads must
        // resolve the current execution (open or latest-closed) like v1.31.0 history-by-
        // workflow-id, which serves closed runs (`workflow_handler.go:898 @ v1.31.0`). This
        // mirrors the fallback StoreExecutionResolver already applies to describe. An explicit
        // run_id is an exact lookup and never falls back.
        if execution.run_id.is_none()
            && let Some(run_key) = self
                .repo
                .find_latest_run(execution.namespace_id, &execution.workflow_id)
                .await
                .map_err(EdgeError::from)?
        {
            return Ok(run_key);
        }
        Err(EdgeError::WorkflowNotFound {
            namespace: namespace.to_string(),
            workflow_id: workflow_id.to_string(),
        })
    }

    /// Read the internal mutable-state summary a run — its raw status and the
    /// `ResetRunId` link — for the AdminService's `DescribeMutableState`. Only the
    /// fields the reset conformance suite reads are surfaced; this is deliberately
    /// not the full persistence image.
    pub async fn describe_mutable_state(
        &self,
        namespace: &str,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> EdgeResult<MutableStateSummary> {
        let run_key = self
            .resolve_execution_run_key(namespace, workflow_id, run_id)
            .await?;
        let state = match self.repo.load_run(run_key).await.map_err(EdgeError::from)? {
            tokeira_kernel::LoadedRun::Existing(state) => state,
            tokeira_kernel::LoadedRun::Absent => {
                return Err(EdgeError::WorkflowNotFound {
                    namespace: namespace.to_string(),
                    workflow_id: workflow_id.to_string(),
                });
            }
        };
        Ok(MutableStateSummary {
            status: state.status,
            reset_run_id: state.reset_run_id,
            original_execution_run_id: state.original_execution_run_id,
        })
    }

    fn execution_ref_from_batch(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<ExecutionRef> {
        Ok(ExecutionRef {
            namespace_id: ctx.namespace_id,
            workflow_id: WorkflowId(workflow_ref.workflow_id.clone()),
            run_id: workflow_ref
                .run_id
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|err| EdgeError::BadRequest(err.to_string()))?
                .map(RunId),
        })
    }

    async fn resolve_batch_run_key(
        &self,
        ctx: &BatchDispatchContext,
        workflow_ref: &tokeira_runtime::WorkflowExecutionRef,
    ) -> EdgeResult<RunKey> {
        let execution = self.execution_ref_from_batch(ctx, workflow_ref)?;
        self.repo
            .resolve_execution(&execution)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::WorkflowNotFound {
                namespace: ctx.namespace_name.clone(),
                workflow_id: workflow_ref.workflow_id.clone(),
            })
    }

    async fn notify_history_run_key(&self, run_key: RunKey, last_event_id: i64) {
        self.history_waiters.notify(run_key, last_event_id).await;
    }
}

#[async_trait]
impl crate::operator_service::NamespaceDeletionApi for WorkflowService {
    async fn reclaim_namespace_runs(&self, namespace_id: tokeira_types::NamespaceId) -> Result<()> {
        loop {
            let run_keys = self.repo.list_runs_for_namespace(namespace_id).await?;
            if run_keys.is_empty() {
                return Ok(());
            }

            for run_key in run_keys {
                let deletion = match self
                    .runtime
                    .delete_workflow(
                        run_key,
                        DeleteWorkflowRequest {
                            request: RequestContext {
                                request_id: RequestId(format!(
                                    "namespace-delete-{}-{}",
                                    namespace_id.0, run_key.0
                                )),
                                caller_identity: Some("tokeira-namespace-reclaimer".to_owned()),
                                principal: None,
                                received_at: OffsetDateTime::now_utc(),
                            },
                            now: OffsetDateTime::now_utc(),
                        },
                    )
                    .await
                {
                    Ok(deletion) => deletion,
                    Err(error) if error.downcast_ref::<WorkflowDeletionNotFound>().is_some() => {
                        // A concurrent/retried namespace reclaimer may have removed a
                        // key from the enumeration snapshot. Re-listing below proves the
                        // namespace is empty before final removal.
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                self.visibility.apply_deletion(deletion.tombstone).await?;
            }
        }
    }
}

fn validate_authoritative_task_namespace(
    request_namespace: &str,
    token_namespace_id: tokeira_types::NamespaceId,
) -> EdgeResult<()> {
    // Task-token namespace takes precedence, but v1.31.0 still rejects a request
    // that names a different namespace instead of silently applying the token there
    // (`checkNamespaceMatch`, `common/rpc/interceptor/namespace_validator.go @
    // v1.31.0`). Calling this before consuming query results keeps rejection free of
    // edge effects. An omitted namespace deliberately defers to the token.
    if !request_namespace.is_empty()
        && to_internal::namespace_id_for(request_namespace) != token_namespace_id
    {
        return Err(EdgeError::BadRequest(
            "Operation requested with a token from a different namespace.".to_owned(),
        ));
    }
    Ok(())
}

fn resolved_namespace_id(
    context: &EdgeContext,
    request_namespace: &str,
) -> EdgeResult<tokeira_types::NamespaceId> {
    let Some(namespace) = context.namespace.as_ref() else {
        return Err(EdgeError::Internal(
            "namespace-scoped admission returned no namespace".to_owned(),
        ));
    };
    match namespace.namespace_id.as_deref() {
        Some(namespace_id) => Uuid::parse_str(namespace_id)
            .map(tokeira_types::NamespaceId)
            .map_err(|error| {
                EdgeError::Internal(format!(
                    "namespace registry contains invalid stable id {namespace_id}: {error}"
                ))
            }),
        // Legacy/bootstrap entries predate explicit stable-ID storage. Their
        // deterministic identity is the same value used by run construction
        // and `NamespaceCache::get_by_id`, so issued tokens remain resolvable.
        None => Ok(to_internal::namespace_id_for(request_namespace)),
    }
}

/// Render v1.31.0's `WorkflowExecutionAlreadyStarted` message for a start
/// rejected by the workflow-id conflict/reuse policy — the corpus asserts the
/// policy suffixes verbatim (`workflow_id_dedup.go:95-129 @ v1.31.0`).
fn start_reject_message(
    reason: tokeira_runtime::StartRejectReason,
    workflow_id: &str,
    run_id: tokeira_types::RunId,
) -> String {
    match reason {
        tokeira_runtime::StartRejectReason::ConflictPolicyFail => format!(
            "Workflow execution is already running. WorkflowId: {workflow_id}, RunId: {run_id}.",
            run_id = run_id.0
        ),
        tokeira_runtime::StartRejectReason::ReuseRejectDuplicate => format!(
            "Workflow execution already finished. WorkflowId: {workflow_id}, RunId: {run_id}. \
             Workflow Id reuse policy: reject duplicate workflow Id.",
            run_id = run_id.0
        ),
        tokeira_runtime::StartRejectReason::ReuseAllowFailedOnly => format!(
            "Workflow execution already finished successfully. WorkflowId: {workflow_id}, \
             RunId: {run_id}. Workflow Id reuse policy: allow duplicate workflow Id if last \
             run failed.",
            run_id = run_id.0
        ),
    }
}

fn grpc_error_code(error: &EdgeError) -> &'static str {
    match error {
        EdgeError::BadRequest(_) => "invalid_argument",
        EdgeError::Unimplemented(_) => "unimplemented",
        EdgeError::NotFound(_) => "not_found",
        EdgeError::AlreadyExists(_) => "already_exists",
        EdgeError::ResourceExhausted(_) => "resource_exhausted",
        EdgeError::WorkflowClosing => "resource_exhausted",
        EdgeError::ConsistentQueryBufferExceeded => "resource_exhausted",
        EdgeError::WorkflowNotReady(_) => "failed_precondition",
        EdgeError::QueryFailed { .. } => "invalid_argument",
        EdgeError::QueryTimedOut => "deadline_exceeded",
        EdgeError::Unauthorized(_) => "unauthenticated",
        EdgeError::PermissionDenied { .. } => "permission_denied",
        EdgeError::Forbidden { .. } => "permission_denied",
        EdgeError::NamespaceNotFound(_)
        | EdgeError::WorkflowNotFound { .. }
        | EdgeError::ActivityNotFound { .. }
        | EdgeError::BatchOperationNotFound { .. } => "not_found",
        EdgeError::ActivityNotStarted { .. } => "failed_precondition",
        EdgeError::WorkflowAlreadyStarted { .. }
        | EdgeError::WorkflowStartRejected { .. }
        | EdgeError::BatchOperationAlreadyExists { .. }
        | EdgeError::NamespaceAlreadyExists(_) => "already_exists",
        EdgeError::ActivityExecutionAlreadyStarted { .. } => "already_exists",
        EdgeError::NamespaceDeleted(_) => "failed_precondition",
        EdgeError::TooManyLongPolls => "resource_exhausted",
        EdgeError::LongPollAdmissionTimeout => "deadline_exceeded",
        EdgeError::RemoteRouteUnsupported { .. } => "unavailable",
        EdgeError::NotShardOwner { .. } => "aborted",
        EdgeError::FailedPrecondition(_) => "failed_precondition",
        EdgeError::Internal(_) => "internal",
    }
}

/// Append the unpersisted transient-WFT suffix to a final-page history read
/// (spec transient-wft Req B.7). A transient (attempt>1) pending task's
/// Scheduled/Started events exist only virtually (`GetTransientWorkflowTaskInfo`
/// mutable_state_impl.go:1189-1250 @ v1.31.0); mid-retry readers see them
/// appended after the last persisted event, and they vanish once the retry
/// chain materializes or the run closes. Synthesizes Scheduled always and
/// Started only when the task is started, at ids last+1 / last+2.
async fn append_transient_suffix(
    events: &mut Vec<tokeira_kernel::HistoryEvent>,
    principals: &mut Vec<Option<tokeira_types::EventPrincipal>>,
    repo: &dyn tokeira_storage::RunRepository,
    run_key: tokeira_types::RunKey,
    read_position: i64,
) -> EdgeResult<()> {
    let tokeira_kernel::LoadedRun::Existing(state) =
        repo.load_run(run_key).await.map_err(EdgeError::from)?
    else {
        return Ok(());
    };
    if !state.is_open() {
        return Ok(());
    }
    let Some(pending) = state.pending_workflow_task.as_ref() else {
        return Ok(());
    };
    // Virtual task = transient (attempt>1) or speculative (attempt-1 with
    // the existence bit; spec speculative-wft E1) — both carry an
    // unpersisted scheduled id one past the last real event.
    let is_virtual =
        pending.attempt > 1 || pending.task_type == tokeira_kernel::WorkflowTaskType::Speculative;
    if !is_virtual || pending.scheduled_event_id != state.last_event_id + 1 {
        return Ok(());
    }
    // Only append when the read actually reached the end of persisted
    // history. The reader's position — last event id covered by this page,
    // or the page-token position when the final page is EMPTY (the previous
    // page's events exactly filled its size limit) — must sit at the run's
    // last event, mirroring v1.31.0's suffix validation that the transient
    // ids continue from nextEventID (`ValidateTransientWorkflowTaskEvents`,
    // get_history_util.go:438-457 @ v1.31.0).
    if read_position != state.last_event_id {
        return Ok(());
    }
    events.push(tokeira_kernel::HistoryEvent {
        event_id: pending.scheduled_event_id,
        happened_at: pending.scheduled_at,
        kind: tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled {
            logical_seq: pending.logical_seq,
            task_queue: state.task_queue.clone(),
            workflow_task_timeout: state.workflow_task_timeout,
            attempt: pending.attempt,
        },
    });
    principals.push(None);
    if let (Some(started_event_id), Some(started_at)) =
        (pending.started_event_id, pending.started_at)
    {
        events.push(tokeira_kernel::HistoryEvent {
            event_id: started_event_id,
            happened_at: started_at,
            kind: tokeira_kernel::HistoryEventKind::WorkflowTaskStarted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                attempt: pending.attempt,
                identity: tokeira_types::WorkerIdentity(String::new()),
                request_id: format!("transient-{}-{}", pending.logical_seq.0, pending.attempt),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
            },
        });
        principals.push(None);
    }
    Ok(())
}

fn decode_history_page_token(token: &[u8]) -> std::result::Result<i64, String> {
    if token.is_empty() {
        return Ok(0);
    }
    if token.len() != 8 {
        return Err("invalid history next_page_token".to_string());
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(token);
    Ok(i64::from_be_bytes(bytes))
}

fn encode_history_page_token(last_event_id: i64) -> Vec<u8> {
    last_event_id.to_be_bytes().to_vec()
}

fn decode_reverse_history_page_token(token: &[u8]) -> std::result::Result<Option<i64>, String> {
    if token.is_empty() {
        return Ok(None);
    }
    if token.len() != 8 {
        return Err("invalid reverse history next_page_token".to_string());
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(token);
    Ok(Some(i64::from_be_bytes(bytes)))
}

fn encode_reverse_history_page_token(before_event_id: i64) -> Vec<u8> {
    before_event_id.to_be_bytes().to_vec()
}

fn filter_attributed_history_events(
    history: &[AttributedHistoryEvent],
    filter_type: i32,
) -> Vec<AttributedHistoryEvent> {
    if filter_type != 2 {
        return history.to_vec();
    }
    history
        .iter()
        .filter(|attributed| is_close_history_event(&attributed.event.kind))
        .cloned()
        .collect()
}

fn is_close_history_event(kind: &HistoryEventKind) -> bool {
    matches!(
        kind,
        HistoryEventKind::WorkflowExecutionCompleted { .. }
            | HistoryEventKind::WorkflowExecutionFailed { .. }
            | HistoryEventKind::WorkflowExecutionTimedOut { .. }
            | HistoryEventKind::WorkflowExecutionCanceled { .. }
            | HistoryEventKind::WorkflowExecutionTerminated { .. }
            | HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
    )
}

fn validate_reset_target(history: &[HistoryEvent], fork_event_id: i64) -> EdgeResult<()> {
    // v1.31.0 accepts any `WorkflowTaskFinishEventId ∈ [2, NextEventID-1]`
    // (resetworkflow/api.go:61-64) that resolves to a pending workflow task: the
    // resetter rebuilds to `finish - 1` and requires a WFT there, i.e. the id must
    // fall in some WFT's `[WorkflowTaskScheduled+1, WorkflowTaskStarted+1]` range
    // (workflow_resetter.go:520-529). A `finish` inside that range but not itself a
    // WFT event (e.g. a `WorkflowExecutionSignaled` between Scheduled and Started)
    // is valid — it forks before the signal and re-drives the same task.
    let last_event_id = history.last().map(|event| event.event_id).unwrap_or(0);
    if fork_event_id < 2 || fork_event_id > last_event_id {
        return Err(EdgeError::BadRequest(format!(
            "reset target event_id {fork_event_id} must be in range [2, {last_event_id}]",
        )));
    }

    let mut scheduled: Option<i64> = None;
    for event in history {
        match event.kind {
            HistoryEventKind::WorkflowTaskScheduled { .. } => {
                // A trailing scheduled-not-started task only covers `[Scheduled+1]`.
                if scheduled.is_some_and(|s| fork_event_id == s + 1) {
                    return Ok(());
                }
                scheduled = Some(event.event_id);
            }
            HistoryEventKind::WorkflowTaskStarted { .. } => {
                if let Some(s) = scheduled
                    && fork_event_id > s
                    && fork_event_id <= event.event_id + 1
                {
                    return Ok(());
                }
                scheduled = None;
            }
            _ => {}
        }
    }
    if scheduled.is_some_and(|s| fork_event_id == s + 1) {
        return Ok(());
    }

    Err(EdgeError::BadRequest(format!(
        "reset target event_id {fork_event_id} does not resolve to a workflow task boundary",
    )))
}

fn batch_request_context(ctx: &BatchDispatchContext) -> RequestContext {
    RequestContext {
        request_id: RequestId(ctx.edge_context.request_id.as_str().to_string()),
        caller_identity: Some(ctx.identity.clone()),
        principal: ctx.edge_context.event_principal(),
        received_at: ctx.edge_context.received_at,
    }
}

fn map_workflow_deletion_error(
    error: anyhow::Error,
    namespace: &str,
    workflow_id: &str,
) -> EdgeError {
    if error.downcast_ref::<WorkflowDeletionNotFound>().is_some() {
        return EdgeError::WorkflowNotFound {
            namespace: namespace.to_string(),
            workflow_id: workflow_id.to_string(),
        };
    }
    EdgeError::from(error)
}

fn batch_error_to_edge(
    error: BatchError,
    namespace: &str,
    job_id: &tokeira_runtime::JobId,
) -> EdgeError {
    match error {
        BatchError::AlreadyExists => EdgeError::BatchOperationAlreadyExists {
            namespace: namespace.to_string(),
            job_id: job_id.0.clone(),
        },
        BatchError::NotFound => EdgeError::BatchOperationNotFound {
            namespace: namespace.to_string(),
            job_id: job_id.0.clone(),
        },
        BatchError::InvalidArgument(message) => EdgeError::BadRequest(message),
    }
}

fn workflow_rule_error_to_edge(error: WorkflowRuleError) -> EdgeError {
    match error {
        WorkflowRuleError::AlreadyExists => {
            EdgeError::BadRequest("Workflow Rule with this ID already exists.".to_string())
        }
        WorkflowRuleError::NotFound => {
            EdgeError::BadRequest("Workflow Rule with this ID not Found.".to_string())
        }
        WorkflowRuleError::LimitExceeded => {
            EdgeError::BadRequest("Workflow Rule limit exceeded. Max: 10".to_string())
        }
        WorkflowRuleError::InvalidExpiration => {
            EdgeError::BadRequest("workflow rule expiration time is invalid".to_string())
        }
        WorkflowRuleError::Storage(message) => EdgeError::Internal(message),
    }
}

fn map_update_lifecycle_error(
    error: anyhow::Error,
    namespace: &str,
    workflow_id: &str,
) -> EdgeError {
    match error.downcast::<UpdateLifecycleError>() {
        Ok(UpdateLifecycleError::UpdateNotFound { update_id, .. }) => {
            EdgeError::NotFound(format!("update {update_id} not found"))
        }
        Err(error) if error.to_string().contains("execution not found") => {
            EdgeError::WorkflowNotFound {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
            }
        }
        Err(error) => EdgeError::from(error),
    }
}

async fn read_last_event_id(repo: &dyn RunRepository, run_key: RunKey) -> Result<i64> {
    Ok(repo
        .read_history(run_key, 0, usize::MAX)
        .await?
        .last()
        .map(|event| event.event_id)
        .unwrap_or(0))
}

/// Validate a namespace state transition against the v1.31.0 rules.
///
/// Tokeira's scoped namespace model only tracks a boolean `deleted` flag, so
/// the live states reduce to `Registered` (not deleted) and `Deleted`. The
/// `Deprecated` intermediate state is accepted as a request target but, since
/// it is not persisted, behaves as a no-op against a live namespace. The
/// rejection surface still matches v1.31.0 `validateStateUpdate`
/// (`service/frontend/namespace_handler.go @ v1.31.0`): any transition out of
/// `Deleted` is rejected, and `Unspecified`/same-state targets are no-ops.
fn validate_namespace_state_update(deleted: bool, target: NamespaceStateUpdate) -> EdgeResult<()> {
    match (deleted, target) {
        // No state change requested.
        (_, NamespaceStateUpdate::Unspecified) => Ok(()),
        // A deleted namespace cannot transition to any other state. This also
        // covers the same-state `Deleted → Deleted` no-op, which is harmless.
        (true, NamespaceStateUpdate::Deleted) => Ok(()),
        (true, _) => Err(EdgeError::BadRequest(
            "invalid namespace state update: namespace is deleted".to_string(),
        )),
        // Registered (live) → {Registered, Deprecated, Deleted} are all
        // permitted: Registered is a same-state no-op, Deprecated is accepted
        // but not persisted, and Deleted is the real transition operators use.
        (false, _) => Ok(()),
    }
}

fn namespace_to_description(namespace: ResolvedNamespace) -> NamespaceDescription {
    // Report the namespace's real id. Tokeira derives it by hashing the name
    // (the same value `resolve_namespace_id` returns and every history event
    // carries), so Describe/List must surface it rather than the unset stored
    // id — otherwise callers that read `NamespaceInfo.Id` (e.g. to compare
    // against a child event's `NamespaceId`) see an empty string.
    let namespace_id = namespace
        .namespace_id
        .or_else(|| Some(to_internal::namespace_id_for(&namespace.name).0.to_string()));
    NamespaceDescription {
        name: namespace.name,
        namespace_id,
        is_global: namespace.is_global,
        visibility_enabled: namespace.visibility_enabled,
        deleted: namespace.deleted,
        description: String::new(),
        owner_email: String::new(),
        cluster_name: "local".to_string(),
        custom_search_attribute_aliases: std::collections::BTreeMap::new(),
        capabilities: NamespaceCapabilities {
            worker_heartbeats: true,
            // v1.31.0 advertises this whenever the consecutive-problem
            // threshold is non-zero (`namespace_handler.go:851-862`). Tokeira
            // pins the enabled release default of five.
            reported_problems_search_attribute: true,
        },
        retention: namespace.retention,
    }
}

fn queue_key_for_poll(
    namespace: &str,
    task_queue: &str,
    task_kind: TaskKind,
    deployment: Option<tokeira_types::DeploymentId>,
    build_id: Option<tokeira_types::BuildId>,
) -> tokeira_types::QueueKey {
    tokeira_types::QueueKey {
        namespace_id: to_internal::namespace_id_for(namespace),
        task_queue: TaskQueueName(task_queue.to_string()),
        task_kind,
        deployment,
        build_id,
    }
}

fn collect_eager_activity_specs(
    commands: &[tokeira_kernel::WorkflowCommand],
    limit: usize,
) -> Vec<(
    String,
    TaskQueueName,
    Option<tokeira_types::DeploymentId>,
    Option<tokeira_types::BuildId>,
)> {
    commands
        .iter()
        .filter_map(|command| match command {
            tokeira_kernel::WorkflowCommand::ScheduleActivity {
                activity_id,
                task_queue,
                deployment,
                build_id,
                request_eager_execution: true,
                ..
            } => Some((
                activity_id.clone(),
                task_queue.clone(),
                deployment.clone(),
                build_id.clone(),
            )),
            _ => None,
        })
        .take(limit)
        .collect()
}

fn cross_namespace_authorization_targets(
    source_namespace: &str,
    commands: &[tokeira_kernel::WorkflowCommand],
) -> Vec<(String, Action)> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();
    for command in commands {
        let target = match command {
            tokeira_kernel::WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace,
                ..
            } => target_namespace
                .as_deref()
                .map(|namespace| (namespace, Action::SignalWorkflowExecution)),
            tokeira_kernel::WorkflowCommand::StartChildWorkflow { namespace, .. } => namespace
                .as_deref()
                .map(|namespace| (namespace, Action::StartWorkflowExecution)),
            tokeira_kernel::WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace,
                ..
            } => target_namespace
                .as_deref()
                .map(|namespace| (namespace, Action::RequestCancelWorkflowExecution)),
            _ => None,
        };
        let Some((namespace, action)) = target else {
            continue;
        };
        if namespace.is_empty() || namespace == source_namespace {
            continue;
        }
        let key = (namespace.to_owned(), action.api_name());
        if seen.insert(key) {
            targets.push((namespace.to_owned(), action));
        }
    }
    targets
}

fn active_poller_to_edge(poller: ActivePoller) -> crate::translate::PollerInfo {
    crate::translate::PollerInfo {
        identity: poller.identity.0,
        last_access_time: Some(poller.last_accessed_at),
        rate_per_second: 0.0,
    }
}

fn task_queue_config_to_edge(entry: TaskQueueConfigEntry) -> TaskQueueConfig {
    TaskQueueConfig {
        queue_rate_limit: entry.queue_rate_limit,
        queue_rate_limit_metadata: entry
            .queue_rate_limit_metadata
            .map(task_queue_config_metadata_to_edge),
        fairness_key_rate_limit_default: entry.fairness_key_rate_limit_default,
        fairness_key_rate_limit_metadata: entry
            .fairness_key_rate_limit_metadata
            .map(task_queue_config_metadata_to_edge),
        fairness_weight_overrides: entry.fairness_weight_overrides,
    }
}

pub(crate) fn task_queue_config_metadata_to_edge(
    metadata: tokeira_runtime::TaskQueueConfigMetadata,
) -> crate::translate::TaskQueueConfigMetadata {
    crate::translate::TaskQueueConfigMetadata {
        reason: metadata.reason,
        update_identity: metadata.update_identity,
        update_time: metadata.update_time,
    }
}

pub(crate) fn task_queue_config_metadata_to_runtime(
    metadata: crate::translate::TaskQueueConfigMetadata,
) -> tokeira_runtime::TaskQueueConfigMetadata {
    tokeira_runtime::TaskQueueConfigMetadata {
        reason: metadata.reason,
        update_identity: metadata.update_identity,
        update_time: metadata.update_time,
    }
}

/// Map the registry's task-queue versioning view onto the edge DTO. The storage
/// `WorkerDeploymentVersionKey` becomes a proto-free `(deployment_name, build_id)`
/// pair; the deprecated string fields are derived later in the gRPC layer.
fn task_queue_versioning_view_to_edge(
    view: TaskQueueVersioningView,
) -> crate::translate::TaskQueueVersioningInfo {
    let to_id = |version: tokeira_storage::WorkerDeploymentVersionKey| {
        crate::translate::WorkerDeploymentVersionId {
            deployment_name: version.deployment_name.0,
            build_id: version.build_id.0,
        }
    };
    crate::translate::TaskQueueVersioningInfo {
        current_deployment_version: view.current_version.map(to_id),
        ramping_deployment_version: view.ramping_version.map(to_id),
        ramping_to_unversioned: view.ramping_to_unversioned,
        ramping_version_percentage: view.ramping_percentage,
        update_time: view.update_time,
    }
}

/// v1.31.0 applies NO character-set restriction to namespace names — its own
/// registry tests register names with spaces, and the conformance corpus
/// derives per-leaf namespaces from Go subtest names containing parentheses.
/// `RegisterNamespace` validates only retention + already-exists
/// (namespace_handler.go @ v1.31.0); the frontend caps the name length at
/// `MaxIDLengthLimit` (default 1000). The previous alnum+`_-` rule here was
/// stricter than ground truth and failed every parenthesised subtest
/// namespace.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 1000
}

impl From<serde_json::Error> for EdgeError {
    fn from(value: serde_json::Error) -> Self {
        EdgeError::BadRequest(value.to_string())
    }
}

impl From<std::io::Error> for EdgeError {
    fn from(value: std::io::Error) -> Self {
        EdgeError::Internal(value.to_string())
    }
}

pub fn not_wired_runtime() -> anyhow::Error {
    anyhow!("tokeira-edge runtime adapter is not wired to the current runtime yet")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        EmptyVisibilityApi, ExecutionResolver, WorkflowService,
        activity_offer_requires_rule_evaluation, apply_matrix_capability_field,
        build_update_activity_options_command, collect_eager_activity_specs,
        cross_namespace_authorization_targets, system_capabilities_with_matrix_overlay,
        worker_identity_from_request, workflow_rule_crud_admitted,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use http::HeaderMap;
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_compatibility::FeatureState;
    use tokeira_kernel::{FieldChange, ParentClosePolicy, StartRequest, WorkflowCommand};
    use tokeira_runtime::{
        BacklogConfig, InMemoryBroker, LaneConfig, TimerScannerConfig, TokeiraRuntime,
        UpdateLifecycleStage, UpdateWaitPolicy, WorkflowTimeoutScannerConfig,
    };
    use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
    use tokeira_types::{
        ExecutionRef, Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId,
        RunKey, SearchAttributes, TaskQueueName, WorkflowId, WorkflowType,
    };

    use crate::{
        errors::EdgeError,
        grpc::runtime_adapter::RuntimeAdapter,
        interceptors::{Action, EdgeContext},
        long_poll::{LongPollConfig, LongPollGate},
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace},
        operator_service::InMemoryOperatorApi,
        poller_registry::PollerRegistry,
        routing::LocalOnlyRouter,
        to_internal::namespace_id_for,
        translate::{
            ActivityOptions, SignalWorkflowExecutionRequest, SystemCapabilities,
            UpdateActivityOptionsRequest, UpdateWaitPolicyDto, UpdateWorkflowExecutionRequest,
        },
    };

    #[test]
    fn cross_namespace_targets_skip_local_and_deduplicate_by_api() {
        let target_namespace_id = NamespaceId::new();
        let external_signal = WorkflowCommand::SignalExternalWorkflowExecution {
            target_namespace_id,
            target_namespace: Some("target".into()),
            target_workflow_id: WorkflowId("external".into()),
            target_run_id: None,
            signal_name: "signal".into(),
            input: Payloads::default(),
            header: None,
            control: String::new(),
        };
        let child = WorkflowCommand::StartChildWorkflow {
            child_workflow_id: WorkflowId("child".into()),
            namespace_id: target_namespace_id,
            namespace: Some("target".into()),
            workflow_type: WorkflowType("child-type".into()),
            task_queue: TaskQueueName("queue".into()),
            input: Payloads::default(),
            header: None,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            cron_schedule: None,
            parent_close_policy: ParentClosePolicy::Terminate,
            reuse_policy: Default::default(),
        };
        let local_cancel = WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_namespace_id: NamespaceId::new(),
            target_namespace: Some("source".into()),
            target_workflow_id: WorkflowId("local".into()),
            target_run_id: None,
            control: String::new(),
        };

        let targets = cross_namespace_authorization_targets(
            "source",
            &[
                external_signal.clone(),
                external_signal,
                child,
                local_cancel,
            ],
        );

        assert_eq!(
            targets,
            vec![
                ("target".into(), Action::SignalWorkflowExecution),
                ("target".into(), Action::StartWorkflowExecution),
            ]
        );
    }

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

    #[test]
    fn namespace_state_update_matches_v1_31_0_rules() {
        use super::validate_namespace_state_update;
        use crate::translate::NamespaceStateUpdate;

        // Unspecified is always a no-op, regardless of current state.
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Unspecified).is_ok());
        assert!(validate_namespace_state_update(true, NamespaceStateUpdate::Unspecified).is_ok());

        // Registered (live) → {Registered, Deprecated, Deleted} all permitted.
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Registered).is_ok());
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Deprecated).is_ok());
        assert!(validate_namespace_state_update(false, NamespaceStateUpdate::Deleted).is_ok());

        // Deleted → Deleted is a harmless same-state no-op.
        assert!(validate_namespace_state_update(true, NamespaceStateUpdate::Deleted).is_ok());

        // Any other transition out of Deleted is rejected (INVALID_ARGUMENT).
        assert!(matches!(
            validate_namespace_state_update(true, NamespaceStateUpdate::Registered),
            Err(EdgeError::BadRequest(_))
        ));
        assert!(matches!(
            validate_namespace_state_update(true, NamespaceStateUpdate::Deprecated),
            Err(EdgeError::BadRequest(_))
        ));
    }

    proptest! {
        /// A non-empty request namespace is accepted exactly when its identity equals
        /// the task token's; rejection is a pure admission decision.
        // Feature: api-conformance-namespace-full, Property 5: task-token namespace mismatch is side-effect free
        #[test]
        fn task_token_namespace_guard_matches_identity(
            request_namespace in "[a-z][a-z0-9-]{0,20}",
            token_namespace in "[a-z][a-z0-9-]{0,20}",
        ) {
            let token_namespace_id = namespace_id_for(&token_namespace);
            let result = super::validate_authoritative_task_namespace(
                &request_namespace,
                token_namespace_id,
            );
            prop_assert_eq!(
                result.is_ok(),
                namespace_id_for(&request_namespace) == token_namespace_id
            );
        }
    }

    fn arb_workflow_command() -> impl Strategy<Value = WorkflowCommand> {
        prop_oneof![
            (arb_small_string(), arb_small_string(), any::<bool>(),).prop_map(
                |(activity_id, task_queue, request_eager_execution)| {
                    WorkflowCommand::ScheduleActivity {
                        activity_id,
                        activity_type: "activity-type".into(),
                        task_queue: TaskQueueName(task_queue),
                        input: Payloads::default(),
                        header: None,
                        request_eager_execution,
                        retry_policy: None,
                        deployment: None,
                        build_id: None,
                        schedule_to_close_timeout: None,
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                        heartbeat_timeout: None,
                    }
                }
            ),
            arb_small_string().prop_map(|timer_id| WorkflowCommand::CancelTimer { timer_id }),
            Just(WorkflowCommand::CancelWorkflow { details: None }),
        ]
    }

    fn baseline_capabilities() -> SystemCapabilities {
        SystemCapabilities {
            signal_and_query_header: true,
            internal_error_differentiation: true,
            activity_failure_include_heartbeat: false,
            supports_schedules: false,
            encoded_failure_attributes: true,
            build_id_based_versioning: true,
            upsert_memo: false,
            eager_workflow_start: true,
            sdk_metadata: false,
            count_group_by_execution_status: true,
            nexus: true,
            server_scaled_deployments: false,
            worker_heartbeats: true,
        }
    }

    fn test_edge_context() -> EdgeContext {
        EdgeContext {
            request_id: crate::request_id::RequestId::new("edge-request"),
            claims: None,
            auth_principal: None,
            namespace: None,
            received_at: time::OffsetDateTime::UNIX_EPOCH,
            is_long_poll: false,
        }
    }

    #[derive(Default)]
    struct NoopResolver;

    #[async_trait]
    impl ExecutionResolver for NoopResolver {
        async fn current_run_key(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }

        async fn describe_execution(
            &self,
            _namespace: &str,
            _workflow_id: &str,
            _run_id: Option<RunId>,
        ) -> Result<Option<crate::WorkflowExecutionDescription>> {
            Ok(None)
        }
    }

    async fn update_test_service() -> Result<(
        WorkflowService,
        Arc<TokeiraRuntime<InMemoryStore>>,
        NamespaceId,
        WorkflowId,
        RunId,
    )> {
        let store = Arc::new(InMemoryStore::default());
        let runtime = Arc::new(TokeiraRuntime::new(
            store.clone(),
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        ));
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache.insert(ResolvedNamespace::active("default")).await?;
        let service = WorkflowService::new(
            Arc::new(RuntimeAdapter::new(runtime.clone())),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            store,
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(crate::interceptors::EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );

        let namespace_id = namespace_id_for("default");
        let workflow_id = WorkflowId("update-edge-workflow".to_string());
        let run_id = RunId::new();
        let result = runtime
            .start_workflow(StartRequest {
                initiator: None,
                run_key: RunKey::new(),
                namespace_id,
                workflow_id: workflow_id.clone(),
                run_id,
                workflow_type: WorkflowType("workflow-type".to_string()),
                task_queue: TaskQueueName("queue-a".to_string()),
                deployment: None,
                build_id: None,
                versioning_override: None,
                workflow_start_delay: None,
                client_cron_schedule: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                on_conflict_options: None,
                priority: None,
                input: Payloads::default(),
                header: None,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: Duration::seconds(10),
                retry_policy: None,
                conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
                continued_execution_run_id: None,
                attempt: 1,
                first_execution_run_id: Some(run_id),
                first_run_started_at: None,
                parent_run_key: None,
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_namespace_name: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: Some(run_id),
                continued_failure: None,
                last_completion_result: None,
                request: RequestContext {
                    request_id: RequestId("start-edge-update".to_string()),
                    caller_identity: None,
                    principal: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
                cron_schedule: None,
                reserved_poller_identity: None,
                eager_execution_accepted: false,
            })
            .await?;
        assert!(matches!(result, CommitResult::Applied { .. }));

        Ok((service, runtime, namespace_id, workflow_id, run_id))
    }

    fn update_request(
        workflow_id: &WorkflowId,
        run_id: Option<RunId>,
        wait_policy: UpdateWaitPolicyDto,
        update_id: &str,
    ) -> UpdateWorkflowExecutionRequest {
        UpdateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            run_id: run_id.map(|id| id.0.to_string()),
            first_execution_run_id: None,
            update_id: update_id.to_string(),
            update_name: "update-handler".to_string(),
            input: Payloads(vec![Payload {
                metadata: Default::default(),
                data: b"input".to_vec(),
                external_payloads: Vec::new(),
            }]),
            wait_policy,
            timeout: std::time::Duration::from_millis(20),
        }
    }

    fn signal_request(
        workflow_id: &WorkflowId,
        run_id: Option<String>,
    ) -> SignalWorkflowExecutionRequest {
        signal_request_with_request_id(workflow_id, run_id, "signal-1")
    }

    fn signal_request_with_request_id(
        workflow_id: &WorkflowId,
        run_id: Option<String>,
        request_id: &str,
    ) -> SignalWorkflowExecutionRequest {
        SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            run_id,
            signal_name: "poke".to_string(),
            input: Payloads::default(),
            header: None,
            links: Vec::new(),
            request_id: Some(request_id.to_string()),
            identity: Some("tester".to_string()),
            now: None,
        }
    }

    fn start_request_for(
        workflow_id: &WorkflowId,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy,
        request_id: &str,
    ) -> crate::translate::StartWorkflowExecutionRequest {
        crate::translate::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.0.clone(),
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue-a".to_string(),
            input: Payloads::default(),
            request_id: Some(request_id.to_string()),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            identity: Some("tester".to_string()),
            request_eager_execution: false,
            workflow_start_delay: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            eager_worker_deployment_options: None,
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Some(Duration::seconds(10)),
            retry_policy: None,
            conflict_policy,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            header: None,
            versioning_override: None,
            on_conflict_options: None,
            priority: None,
            cron_schedule: None,
            run_key: None,
            run_id: None,
            now: None,
        }
    }

    // Feature: edge-eager-dispatch, Properties 2/4/5. The v1.31.0 caller is
    // the intended eager worker; server-observed long-poll registration is not
    // an admission condition (service/history/api/startworkflow/api.go and
    // create_workflow_util.go @ v1.31.0).
    #[tokio::test]
    async fn eager_start_does_not_require_registered_poller() -> Result<()> {
        let (service, runtime, _namespace_id, _workflow_id, _run_id) =
            update_test_service().await?;
        let workflow_id = WorkflowId("eager-without-poller".to_string());
        let mut request = start_request_for(
            &workflow_id,
            tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            "eager-start-request",
        );
        request.request_eager_execution = true;
        let retry = request.clone();

        let response = service
            .start_workflow_execution(&HeaderMap::new(), request)
            .await?;

        assert!(response.eager_workflow_task.is_some());
        let history = runtime
            .repo()
            .read_history(response.run_key, 0, usize::MAX)
            .await?;
        assert_eq!(
            history[0].kind.eager_execution_accepted(),
            response.eager_workflow_task.is_some()
        );
        let retried = service
            .start_workflow_execution(&HeaderMap::new(), retry)
            .await?;
        assert_eq!(retried.run_id, response.run_id);
        assert!(retried.eager_workflow_task.is_some());

        let non_eager_workflow_id = WorkflowId("non-eager-history-agreement".to_string());
        let non_eager = service
            .start_workflow_execution(
                &HeaderMap::new(),
                start_request_for(
                    &non_eager_workflow_id,
                    tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                    "non-eager-start-request",
                ),
            )
            .await?;
        assert!(non_eager.eager_workflow_task.is_none());
        let non_eager_history = runtime
            .repo()
            .read_history(non_eager.run_key, 0, usize::MAX)
            .await?;
        assert_eq!(
            non_eager_history[0].kind.eager_execution_accepted(),
            non_eager.eager_workflow_task.is_some()
        );
        Ok(())
    }

    // Conformance: a UseExisting start that attaches to a running incumbent
    // returns success (existing run id, started=false), NOT AlreadyStarted; only
    // the Fail policy errors (handleUseExistingWorkflowOnConflictOptions vs the
    // Fail arm, service/history/api/startworkflow/api.go @ v1.31.0). The Nexus
    // WorkflowRunOperation depends on this — with
    // WorkflowExecutionErrorWhenAlreadyStarted set, a UseExisting caller must see
    // success to count its operation as started (temporalnexus, sdk v1.41.1).
    #[tokio::test]
    async fn start_use_existing_attaches_without_already_started_error() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, existing_run_id) =
            update_test_service().await?;

        let resp = service
            .start_workflow_execution(
                &HeaderMap::new(),
                start_request_for(
                    &workflow_id,
                    tokeira_kernel::WorkflowIdConflictPolicy::UseExisting,
                    "use-existing-attach",
                ),
            )
            .await?;

        assert!(
            !resp.started,
            "attach must report started=false, not a fresh start"
        );
        assert_eq!(
            resp.run_id, existing_run_id,
            "attach must return the running incumbent's run id"
        );
        Ok(())
    }

    // Negative control: the Fail policy against the same running incumbent must
    // still surface AlreadyStarted (the conflict-policy-fail Nexus losers).
    #[tokio::test]
    async fn start_fail_policy_rejects_running_incumbent() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, _existing_run_id) =
            update_test_service().await?;

        let err = service
            .start_workflow_execution(
                &HeaderMap::new(),
                start_request_for(
                    &workflow_id,
                    tokeira_kernel::WorkflowIdConflictPolicy::Fail,
                    "fail-policy-reject",
                ),
            )
            .await
            .expect_err("Fail policy must reject a running incumbent");

        // The Fail-policy rejection renders v1.31.0's exact
        // WorkflowExecutionAlreadyStarted message (workflow_id_dedup.go:95-97).
        match err {
            EdgeError::WorkflowStartRejected { message, .. } => {
                assert!(
                    message.starts_with("Workflow execution is already running. WorkflowId:"),
                    "got message {message:?}"
                );
            }
            other => panic!("got {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_rejects_malformed_run_id_before_lookup() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, _run_id) =
            update_test_service().await?;
        let error = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request(&workflow_id, Some("not-a-uuid".to_string())),
            )
            .await
            .expect_err("malformed run_id must not be silently ignored");

        assert!(matches!(error, EdgeError::BadRequest(_)));
        assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_missing_execution_returns_not_found() -> Result<()> {
        let (service, _runtime, _namespace_id, _workflow_id, _run_id) =
            update_test_service().await?;
        let missing = WorkflowId("missing-workflow".to_string());
        let error = service
            .signal_workflow_execution(&HeaderMap::new(), signal_request(&missing, None))
            .await
            .expect_err("missing execution must map to NOT_FOUND");

        assert!(matches!(error, EdgeError::WorkflowNotFound { .. }));
        assert_eq!(error.status_code(), http::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn signal_path_targets_exact_or_current_run() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;

        let current_response = service
            .signal_workflow_execution(&HeaderMap::new(), signal_request(&workflow_id, None))
            .await?;
        assert!(current_response.accepted);

        let exact_response = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request_with_request_id(
                    &workflow_id,
                    Some(run_id.0.to_string()),
                    "signal-2",
                ),
            )
            .await?;
        assert!(exact_response.accepted);

        let missing_run = RunId::new();
        let error = service
            .signal_workflow_execution(
                &HeaderMap::new(),
                signal_request(&workflow_id, Some(missing_run.0.to_string())),
            )
            .await
            .expect_err("valid but unknown run_id must not fall back to current");
        assert!(matches!(error, EdgeError::WorkflowNotFound { .. }));
        assert_eq!(error.status_code(), http::StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn update_path_rejects_admitted_wait_policy() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;
        let error = service
            .update_workflow_execution(
                &HeaderMap::new(),
                update_request(
                    &workflow_id,
                    Some(run_id),
                    UpdateWaitPolicyDto::Admitted,
                    "update-1",
                ),
            )
            .await
            .expect_err("update path must reject ADMITTED wait policy");

        assert!(matches!(error, EdgeError::BadRequest(_)));
        assert_eq!(error.status_code(), http::StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn poll_path_accepts_admitted_and_returns_current_stage() -> Result<()> {
        let (service, runtime, namespace_id, workflow_id, run_id) = update_test_service().await?;
        let snapshot = runtime
            .update_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id.clone(),
                    run_id: Some(run_id),
                },
                "update-1".to_string(),
                "update-handler".to_string(),
                Payloads::default(),
                RequestContext {
                    request_id: RequestId("update-1".to_string()),
                    caller_identity: None,
                    principal: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                Duration::milliseconds(20),
                UpdateWaitPolicy::Admitted,
            )
            .await?;
        assert_eq!(snapshot.stage, UpdateLifecycleStage::Admitted);

        let polled = service
            .poll_workflow_execution_update(
                &HeaderMap::new(),
                "default".to_string(),
                workflow_id.0.clone(),
                run_id.0.to_string(),
                "update-1".to_string(),
                UpdateWaitPolicy::Admitted,
            )
            .await?;

        assert_eq!(polled.stage, UpdateLifecycleStage::Admitted);
        assert_eq!(polled.workflow_execution.run_id, Some(run_id));
        Ok(())
    }

    #[tokio::test]
    async fn update_path_targets_exact_run_and_returns_stable_ref() -> Result<()> {
        let (service, _runtime, _namespace_id, workflow_id, run_id) = update_test_service().await?;
        let response = service
            .update_workflow_execution(
                &HeaderMap::new(),
                update_request(
                    &workflow_id,
                    Some(run_id),
                    UpdateWaitPolicyDto::Unspecified,
                    "update-1",
                ),
            )
            .await?;

        assert_eq!(response.update_ref.workflow_id, workflow_id.0);
        assert_eq!(response.update_ref.run_id, run_id.0.to_string());
        assert_eq!(response.update_ref.update_id, "update-1");
        assert_eq!(
            response.stage,
            crate::translate::UpdateLifecycleStageDto::Admitted
        );
        assert!(response.outcome.is_none());
        Ok(())
    }

    #[test]
    fn matrix_capability_overlay_preserves_unmapped_and_experimental_baseline() {
        let capabilities = system_capabilities_with_matrix_overlay(baseline_capabilities());

        assert!(capabilities.signal_and_query_header);
        assert!(capabilities.build_id_based_versioning);
        assert!(capabilities.eager_workflow_start);
    }

    #[test]
    fn mapped_stubbed_capability_preserves_true_baseline() {
        let mut capabilities = baseline_capabilities();

        apply_matrix_capability_field(
            &mut capabilities,
            "signal_and_query_header",
            FeatureState::Stubbed,
        );

        assert!(capabilities.signal_and_query_header);
        assert!(capabilities.encoded_failure_attributes);
    }

    #[test]
    fn empty_worker_identity_is_not_propagated_to_runtime() {
        assert_eq!(worker_identity_from_request(String::new()), None);
        assert_eq!(
            worker_identity_from_request("worker-a".to_string()),
            Some(tokeira_types::WorkerIdentity("worker-a".to_string()))
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        // Feature: workflow-rules, Property 6: poll-admission independence
        fn property_activity_offer_policy_ignores_crud_gate_at_poll_admission(
            crud_gate_at_poll_admission in any::<bool>(),
        ) {
            prop_assert!(activity_offer_requires_rule_evaluation(
                crud_gate_at_poll_admission
            ));
        }

        #[test]
        // Feature: workflow-rules, Property 2: gate-before-validation precedence
        fn property_workflow_rule_crud_admission_depends_only_on_gate(
            enabled in any::<bool>(),
            _request_body_is_valid in any::<bool>(),
        ) {
            prop_assert_eq!(workflow_rule_crud_admitted(enabled), enabled);
        }

        #[test]
        fn property_collect_eager_activity_specs_respects_limit(
            commands in prop::collection::vec(arb_workflow_command(), 0..20),
            limit in 0usize..8usize,
        ) {
            let eager_commands: Vec<_> = commands
                .iter()
                .filter_map(|command| match command {
                    WorkflowCommand::ScheduleActivity {
                        activity_id,
                        task_queue,
                        deployment,
                        build_id,
                        request_eager_execution: true,
                        ..
                    } => Some((
                        activity_id.clone(),
                        task_queue.clone(),
                        deployment.clone(),
                        build_id.clone(),
                    )),
                    _ => None,
                })
                .collect();

            let specs = collect_eager_activity_specs(&commands, limit);
            prop_assert!(specs.len() <= limit);
            prop_assert_eq!(
                specs,
                eager_commands.into_iter().take(limit).collect::<Vec<_>>()
            );
        }

        #[test]
        fn property_update_activity_options_command_respects_update_mask(
            mask_bits in 1u8..32u8,
        ) {
            let mut update_mask = Vec::new();
            if mask_bits & 0b00001 != 0 {
                update_mask.push("task_queue".to_string());
            }
            if mask_bits & 0b00010 != 0 {
                update_mask.push("activity_options.schedule_to_close_timeout".to_string());
            }
            if mask_bits & 0b00100 != 0 {
                update_mask.push("schedule_to_start_timeout".to_string());
            }
            if mask_bits & 0b01000 != 0 {
                update_mask.push("start_to_close_timeout".to_string());
            }
            if mask_bits & 0b10000 != 0 {
                update_mask.push("heartbeat_timeout".to_string());
            }

            let req = UpdateActivityOptionsRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                run_id: None,
                identity: "operator".to_string(),
                target: crate::translate::ActivityTarget::Id("activity-1".to_string()),
                activity_options: Some(ActivityOptions {
                    task_queue: Some("queue-b".to_string()),
                    schedule_to_close_timeout: Some(time::Duration::seconds(10)),
                    schedule_to_start_timeout: Some(time::Duration::seconds(20)),
                    start_to_close_timeout: Some(time::Duration::seconds(30)),
                    heartbeat_timeout: None,
                    retry_policy: None,
                }),
                update_mask: update_mask.clone(),
                restore_original: false,
                activity_type: None,
            };

            let command = build_update_activity_options_command(&test_edge_context(), &req)
            .expect("non-empty mask should select at least one field");

            prop_assert_eq!(
                matches!(command.task_queue, FieldChange::Set(TaskQueueName(ref name)) if name == "queue-b"),
                mask_bits & 0b00001 != 0
            );
            prop_assert_eq!(
                matches!(command.schedule_to_close_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(10)),
                mask_bits & 0b00010 != 0
            );
            prop_assert_eq!(
                matches!(command.schedule_to_start_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(20)),
                mask_bits & 0b00100 != 0
            );
            prop_assert_eq!(
                matches!(command.start_to_close_timeout, FieldChange::Set(Some(value)) if value == time::Duration::seconds(30)),
                mask_bits & 0b01000 != 0
            );
            prop_assert_eq!(
                matches!(command.heartbeat_timeout, FieldChange::Set(None)),
                mask_bits & 0b10000 != 0
            );
        }
    }
}
