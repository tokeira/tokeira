//! gRPC <-> edge DTO translation.
//!
//! This module is where we normalize the upstream Temporal proto surface into
//! the smaller edge-facing DTOs used by the rest of the crate. It is allowed to
//! carry compatibility policy: proto enums are migrated into kernel/runtime
//! policies here, missing transport fields receive edge defaults here, and
//! version-specific transport quirks are collapsed before they leak deeper into
//! the system.
//!
//! That also means defaults here must be treated carefully. A default is only
//! acceptable when the upstream API truly omits the concept and the edge needs
//! an internal policy value. If upstream already carries a field, translation
//! should preserve it rather than silently inventing a replacement.
//!
//! The update protocol is the most complex translation path: SDK completions
//! carry `ProtocolMessage` commands that reference entries in the `messages`
//! list by `message_id`. This module resolves those references and decodes
//! the `Any`-typed bodies into kernel `UpdateProtocolBody` variants before
//! the workflow service layer ever sees them.

// Translation mirrors fields Temporal has deprecated but still ships on the wire
// (e.g. `worker_version_capabilities`, `PendingNexusOperationInfo::operation_id`);
// reading/writing them is required for v1.31.0 wire compatibility.
#![allow(deprecated)]

use std::{collections::BTreeMap, time::Duration};

use prost::Message as _;
use time::OffsetDateTime;
use tokeira_kernel::{
    FieldChange, MemoPatch, SearchAttributesPatch, WorkerVersionStamp, WorkflowCommand,
    WorkflowTaskWorkerVersion,
    state::{
        CallbackSpec as KernelCallbackSpec, CallbackState as KernelCallbackState,
        CallbackTrigger as KernelCallbackTrigger, CompletionCallback as KernelCompletionCallback,
        ContinueAsNewVersioningBehavior, Link as KernelLink,
        LinkWorkflowEventReference as KernelLinkWorkflowEventReference, VersioningBehavior,
        VersioningOverride as KernelVersioningOverride, WorkerDeploymentVersionRef,
    },
};
use tokeira_proto::{
    conversions::{
        ProtoConversionError,
        common::{
            failure_to_payload, headers_from_domain, headers_to_domain, is_temporal_nil_payload,
            memo_from_domain, memo_to_domain, payload_from_domain, payload_to_domain,
            payload_to_failure, payloads_from_domain, payloads_to_domain,
            search_attr_payload_to_domain, search_attr_value_to_payload,
            search_attributes_from_domain, search_attributes_to_domain, task_queue_from_domain,
            task_queue_to_domain, to_proto_duration, to_proto_timestamp,
            workflow_execution_from_ids,
        },
    },
    enums,
    public::temporal::api::{
        activity::v1 as activity_proto, command::v1 as command, common::v1 as proto_common,
        compute::v1 as compute_proto, deployment::v1 as deployment_proto,
        errordetails::v1 as errordetails_proto, failure::v1 as failure_proto,
        namespace::v1 as namespace_proto, replication::v1 as replication_proto,
        sdk::v1 as sdk_proto, taskqueue::v1 as taskqueue_proto, version::v1 as version_proto,
        workflow::v1 as workflow,
    },
    workflowservice,
};
use tokeira_runtime::{
    ComputeConfigScalingGroupUpdate, CreateDeployment, CreateVersion, DeleteDeployment,
    DeleteVersion, DeploymentPage, DeploymentView, DescribeVersion, ListDeployments,
    NewManagerIdentity, ScheduleError, SetCurrent, SetCurrentOutcome, SetManager,
    SetManagerOutcome, SetRamping, SetRampingOutcome, UpdateComputeConfig, UpdateMetadata,
    ValidateComputeConfig, VersionMetadataView, VersionView, cron_initial_backoff,
};
use tokeira_storage::{
    BuildId as DeploymentBuildId, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider,
    ComputeScaler, ConflictToken, DeploymentKey, DeploymentName, DeploymentTaskQueueType,
    DrainageInfo, RoutingConfigUpdateState, StoredRoutingConfig, VersionDrainageStatus,
    VersionMetadata, WorkerDeploymentVersionKey, WorkerDeploymentVersionStatus,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionStatus, NamespaceId, Payloads, RetryPolicy,
    RunId, TaskKind, TaskQueueName, WorkflowId, WorkflowType,
};
use tonic::{Code, Status, metadata::MetadataMap};
use uuid::Uuid;

use crate::translate::{
    ActivityExecutionSummary, CompletionCallback as EdgeCompletionCallback,
    CountActivityExecutionsRequest, CountActivityExecutionsResponse,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
    DeleteWorkflowExecutionRequest as EdgeDeleteWorkflowExecutionRequest,
    DescribeTaskQueueRequest as EdgeDescribeTaskQueueRequest,
    DescribeTaskQueueResponse as EdgeDescribeTaskQueueResponse, DescribeWorkflowExecutionRequest,
    ExecuteMultiOperationRequest as EdgeExecuteMultiOperationRequest,
    ExecuteMultiOperationResponse as EdgeExecuteMultiOperationResponse, Link as EdgeLink,
    LinkWorkflowEventReference, ListActivityExecutionsRequest, ListActivityExecutionsResponse,
    ListNamespacesResponse as EdgeListNamespacesResponse,
    ListTaskQueuePartitionsRequest as EdgeListTaskQueuePartitionsRequest,
    ListTaskQueuePartitionsResponse as EdgeListTaskQueuePartitionsResponse,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, MultiOperationFailure,
    NamespaceDescription, NamespaceStateUpdate, OnConflictOptions as EdgeOnConflictOptions,
    PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse, Priority as EdgePriority,
    ProtocolMessageDto, QueryResultDto, RegisterNamespaceRequest as EdgeRegisterNamespaceRequest,
    ResetWorkflowExecutionRequest as EdgeResetWorkflowExecutionRequest,
    ResetWorkflowExecutionResponse as EdgeResetWorkflowExecutionResponse,
    RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
    SignalWithStartWorkflowExecutionRequest as EdgeSignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse as EdgeSignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse, SystemInfo, TaskQueueConfig,
    TaskQueuePartition as EdgeTaskQueuePartition,
    UpdateNamespaceRequest as EdgeUpdateNamespaceRequest,
    UpdateWorkflowExecutionOptionsRequest as EdgeUpdateWorkflowExecutionOptionsRequest,
    UpdateWorkflowExecutionOptionsResponse as EdgeUpdateWorkflowExecutionOptionsResponse,
    UserMetadata, VersioningOverride, VersioningOverrideChange, WorkflowExecutionDescription,
    WorkflowExecutionSummary,
    to_internal::{namespace_id_for, versioning_override_to_kernel},
};
use tokeira_kernel::state::ParentClosePolicy;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STICKY_TTL: Duration = Duration::from_secs(30);
const NON_RETRYABLE_ACTIVITY_SENTINEL: &str = "__tokeira_non_retryable__";
// These are Temporal's v1.31.0 admission defaults, not deployment tuning:
// `common/dynamicconfig/constants.go:988 @ v1.31.0`.
const CALLBACK_URL_MAX_LENGTH: usize = 1000;
const CALLBACK_HEADER_MAX_SIZE: usize = 8 * 1024;
const MAX_CALLBACKS_PER_WORKFLOW: usize = 32;
// `frontend.maxlinksPerRequest` / `frontend.linkMaxSize` defaults
// (`common/dynamicconfig/constants.go:1010,1015 @ v1.31.0`). Behavioural limits,
// so source-cited constants per the callback-validation decision note.
const MAX_LINKS_PER_REQUEST: usize = 10;
const LINK_MAX_SIZE: usize = 4000;

fn proto_duration_to_time(value: Option<&prost_types::Duration>) -> Option<time::Duration> {
    value.map(|duration| {
        time::Duration::seconds(duration.seconds)
            + time::Duration::nanoseconds(i64::from(duration.nanos))
    })
}

/// Translate a workflow **execution/run** timeout, applying Temporal's
/// "zero means unlimited" convention: a zero (or absent) duration yields `None`
/// (no timeout, no timer). v1.31.0 generates a timeout timer only when the
/// expiration is non-zero (`service/history/workflow/task_generator.go:153-185 @
/// v1.31.0`). Without this, the SDK's encoding of an unset timeout as `0s` would be
/// read as `Some(ZERO)` and reaped as an immediately-due deadline by the workflow
/// timeout scanner. NB: this is only for the execution/run timeouts — the
/// workflow-task timeout has its own non-zero default and must not pass through here.
fn workflow_timeout_to_time(value: Option<&prost_types::Duration>) -> Option<time::Duration> {
    proto_duration_to_time(value).filter(|duration| !duration.is_zero())
}

/// Translate an **activity** timeout with the same "zero means unset"
/// convention: the Go SDK always serializes all four activity timeouts, zero
/// when the user left them unset (`internal_event_handlers.go:616 @ sdk
/// v1.41.1`), and v1.31.0 creates a timer only for positive durations
/// (`timer_sequence.go:268-271 @ v1.31.0`). Mapping present-zero to
/// `Some(ZERO)` had the activity-timeout scanner reaping every activity as an
/// immediately-due ScheduleToClose.
fn activity_timeout_to_time(value: Option<&prost_types::Duration>) -> Option<time::Duration> {
    proto_duration_to_time(value).filter(|duration| !duration.is_zero())
}

/// Translate the **workflow-task** timeout with the same "zero means unset"
/// convention so the 10s default applies downstream: the Go SDK serializes the
/// field as a present zero duration when the caller left it unset
/// (`durationpb.New(0)`), and v1.31.0 replaces any non-positive value with the
/// namespace default (`common.OverrideWorkflowTaskTimeout` @ v1.31.0). Mapping
/// present-zero to `Some(ZERO)` put a zero task timeout on the
/// WorkflowExecutionStarted event, which the SDK reads as its local-activity
/// heartbeat budget — `0 × 0.8 = fire immediately`, a busy loop of empty
/// force-created workflow tasks.
fn workflow_task_timeout_to_time(value: Option<&prost_types::Duration>) -> Option<time::Duration> {
    proto_duration_to_time(value).filter(|duration| duration.is_positive())
}

/// Normalized activity timeouts per v1.31.0 `validateAndNormalizeTimeouts`
/// (`chasm/lib/activity/validator.go:142-206`): with ScheduleToClose set,
/// ScheduleToStart/StartToClose default to it (and are capped by it);
/// HeartbeatTimeout never exceeds StartToClose. The run-timeout-derived
/// ScheduleToClose fill (validator.go:178-181) needs workflow state and is
/// intentionally not applied at this stateless boundary.
fn normalized_activity_timeouts(
    schedule_to_close: Option<&prost_types::Duration>,
    schedule_to_start: Option<&prost_types::Duration>,
    start_to_close: Option<&prost_types::Duration>,
    heartbeat: Option<&prost_types::Duration>,
) -> (
    Option<time::Duration>,
    Option<time::Duration>,
    Option<time::Duration>,
    Option<time::Duration>,
) {
    let schedule_to_close = activity_timeout_to_time(schedule_to_close);
    let mut schedule_to_start = activity_timeout_to_time(schedule_to_start);
    let mut start_to_close = activity_timeout_to_time(start_to_close);
    let mut heartbeat = activity_timeout_to_time(heartbeat);
    if let Some(s2c) = schedule_to_close {
        schedule_to_start = Some(schedule_to_start.map_or(s2c, |s2s| s2s.min(s2c)));
        start_to_close = Some(start_to_close.map_or(s2c, |stc| stc.min(s2c)));
    }
    if let (Some(hb), Some(stc)) = (heartbeat, start_to_close) {
        heartbeat = Some(hb.min(stc));
    }
    (
        schedule_to_close,
        schedule_to_start,
        start_to_close,
        heartbeat,
    )
}

fn valid_non_negative_duration(
    value: Option<&prost_types::Duration>,
    field: &'static str,
) -> Result<Option<time::Duration>, ProtoConversionError> {
    match value {
        Some(duration) if duration.seconds < 0 || duration.nanos < 0 => {
            Err(ProtoConversionError::MissingField(field))
        }
        Some(duration) => Ok(Some(
            time::Duration::seconds(duration.seconds)
                + time::Duration::nanoseconds(i64::from(duration.nanos)),
        )),
        None => Ok(None),
    }
}

fn time_skipping_requests_behavior(config: &workflow::TimeSkippingConfig) -> bool {
    config.enabled || config.disable_propagation || config.bound.is_some()
}

fn reject_behavioral_time_skipping(
    config: Option<&workflow::TimeSkippingConfig>,
    field: &'static str,
) -> Result<(), ProtoConversionError> {
    if config.is_some_and(time_skipping_requests_behavior) {
        return Err(ProtoConversionError::MissingField(field));
    }
    Ok(())
}

fn validate_client_cron_schedule(cron_schedule: Option<&str>) -> Result<(), ProtoConversionError> {
    if let Some(cron_schedule) = cron_schedule {
        // Mirror `backoff.ValidateSchedule @ v1.31.0`: an unparseable or
        // unsatisfiable cron is rejected with `InvalidArgument` and the verbatim
        // message "invalid CronSchedule." (or "…, no time can be found to satisfy
        // the schedule"). `cron_initial_backoff` already produces that exact text,
        // so surface it rather than masking a valid-but-rejected cron as a missing
        // field (`common/backoff/cron.go:14 @ v1.31.0`).
        cron_initial_backoff(cron_schedule, OffsetDateTime::now_utc()).map_err(
            |err| match err {
                ScheduleError::InvalidArgument(message) => {
                    ProtoConversionError::InvalidArgument(message)
                }
                other => ProtoConversionError::InvalidArgument(other.to_string()),
            },
        )?;
    }
    Ok(())
}

fn user_metadata_to_edge(
    metadata: Option<&tokeira_proto::public::temporal::api::sdk::v1::UserMetadata>,
) -> Option<UserMetadata> {
    metadata.map(|metadata| UserMetadata {
        summary: metadata.summary.as_ref().map(payload_to_domain),
        details: metadata.details.as_ref().map(payload_to_domain),
    })
}

fn link_to_edge(link: &proto_common::Link) -> Result<EdgeLink, ProtoConversionError> {
    use proto_common::link::{Variant, workflow_event::Reference};

    match link.variant.as_ref() {
        Some(Variant::WorkflowEvent(event)) => {
            let reference = match event.reference.as_ref() {
                Some(Reference::EventRef(event_ref)) => Some(LinkWorkflowEventReference::Event {
                    event_id: event_ref.event_id,
                    event_type: event_ref.event_type,
                }),
                Some(Reference::RequestIdRef(request_ref)) => {
                    Some(LinkWorkflowEventReference::RequestId {
                        request_id: request_ref.request_id.clone(),
                        event_type: request_ref.event_type,
                    })
                }
                None => None,
            };
            Ok(EdgeLink::WorkflowEvent {
                namespace: event.namespace.clone(),
                workflow_id: event.workflow_id.clone(),
                run_id: event.run_id.clone(),
                reference,
            })
        }
        Some(Variant::BatchJob(job)) => Ok(EdgeLink::BatchJob {
            job_id: job.job_id.clone(),
        }),
        Some(Variant::Activity(activity)) => Ok(EdgeLink::Activity {
            namespace: activity.namespace.clone(),
            activity_id: activity.activity_id.clone(),
            run_id: activity.run_id.clone(),
        }),
        Some(Variant::NexusOperation(operation)) => Ok(EdgeLink::NexusOperation {
            namespace: operation.namespace.clone(),
            operation_id: operation.operation_id.clone(),
            run_id: operation.run_id.clone(),
        }),
        None => Err(ProtoConversionError::MissingField("Link.variant")),
    }
}

fn links_to_edge(links: &[proto_common::Link]) -> Result<Vec<EdgeLink>, ProtoConversionError> {
    links.iter().map(link_to_edge).collect()
}

fn callback_to_edge(
    callback: &proto_common::Callback,
) -> Result<EdgeCompletionCallback, ProtoConversionError> {
    use proto_common::callback::Variant;

    match callback.variant.as_ref() {
        Some(Variant::Nexus(nexus)) => Ok(EdgeCompletionCallback {
            url: nexus.url.clone(),
            header: nexus
                .header
                .iter()
                .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
                .collect(),
            links: links_to_edge(&callback.links)?,
        }),
        // Temporal exposes the internal callback variant for replication of
        // already-authored history, not for external start authorship
        // (`Callback.Internal` comment in common/v1/message.proto @ API v1.62.11).
        Some(Variant::Internal(_)) => Err(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.completion_callbacks.internal",
        )),
        None => Err(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.completion_callbacks.variant",
        )),
    }
}

fn validate_completion_callbacks(
    callbacks: &[proto_common::Callback],
) -> Result<(), ProtoConversionError> {
    // Temporal performs callback admission at the frontend before history is
    // written (`service/frontend/workflow_handler.go:6299 @ v1.31.0`). Keeping
    // this in the edge preserves that validation order without making callback
    // policy part of workflow semantics.
    if callbacks.len() > MAX_CALLBACKS_PER_WORKFLOW {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "cannot attach more than {MAX_CALLBACKS_PER_WORKFLOW} callbacks to a workflow"
        )));
    }
    for callback in callbacks {
        if let Some(proto_common::callback::Variant::Nexus(nexus)) = callback.variant.as_ref() {
            validate_callback_url(&nexus.url)?;
            let header_size: usize = nexus
                .header
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum();
            if header_size > CALLBACK_HEADER_MAX_SIZE {
                return Err(ProtoConversionError::InvalidArgument(format!(
                    "invalid header: header size longer than max allowed size of {CALLBACK_HEADER_MAX_SIZE}"
                )));
            }
        }
    }
    Ok(())
}

/// Builds the link set v1.31.0 validates on admission: the request's own links
/// (with any that exactly match a Nexus-callback link removed) followed by every
/// callback's links. Mirrors `dedupLinksFromCallbacks` + the `allLinks`
/// assembly in `StartWorkflowExecution` (`service/frontend/workflow_handler.go:6230,675 @ v1.31.0`).
fn collect_admission_links(
    links: &[proto_common::Link],
    callbacks: &[proto_common::Callback],
) -> Vec<proto_common::Link> {
    let nexus_callback_links: Vec<&proto_common::Link> = callbacks
        .iter()
        .filter(|cb| {
            matches!(
                cb.variant.as_ref(),
                Some(proto_common::callback::Variant::Nexus(_))
            )
        })
        .flat_map(|cb| cb.links.iter())
        .collect();
    // Dedup is by proto equality, only against Nexus-callback links; prost types
    // derive structural `PartialEq`, which is the wire-equality v1.31.0 uses.
    let mut combined: Vec<proto_common::Link> = links
        .iter()
        .filter(|link| !nexus_callback_links.contains(link))
        .cloned()
        .collect();
    for callback in callbacks {
        combined.extend(callback.links.iter().cloned());
    }
    combined
}

fn validate_links(links: &[proto_common::Link]) -> Result<(), ProtoConversionError> {
    // Mirror `WorkflowHandler.validateLinks` (`service/frontend/workflow_handler.go:6260 @ v1.31.0`):
    // bound the combined link set by count and per-link serialized size, admit
    // only WorkflowEvent and BatchJob variants, and require their identity
    // fields. Messages match v1.31.0 verbatim (the corpus asserts on err text).
    if links.len() > MAX_LINKS_PER_REQUEST {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "cannot attach more than {MAX_LINKS_PER_REQUEST} links per request, got {}",
            links.len()
        )));
    }
    for link in links {
        let size = link.encoded_len();
        if size > LINK_MAX_SIZE {
            return Err(ProtoConversionError::InvalidArgument(format!(
                "link exceeds allowed size of {LINK_MAX_SIZE}, got {size}"
            )));
        }
        match link.variant.as_ref() {
            Some(proto_common::link::Variant::WorkflowEvent(event)) => {
                if event.namespace.is_empty() {
                    return Err(ProtoConversionError::InvalidArgument(
                        "workflow event link must not have an empty namespace field".to_string(),
                    ));
                }
                if event.workflow_id.is_empty() {
                    return Err(ProtoConversionError::InvalidArgument(
                        "workflow event link must not have an empty workflow ID field".to_string(),
                    ));
                }
                if event.run_id.is_empty() {
                    return Err(ProtoConversionError::InvalidArgument(
                        "workflow event link must not have an empty run ID field".to_string(),
                    ));
                }
                // EVENT_TYPE_UNSPECIFIED == 0; an event ref that names an id but
                // not a type is rejected (`workflow_handler.go:6285 @ v1.31.0`).
                if let Some(proto_common::link::workflow_event::Reference::EventRef(event_ref)) =
                    event.reference.as_ref()
                    && event_ref.event_type == 0
                    && event_ref.event_id != 0
                {
                    return Err(ProtoConversionError::InvalidArgument(
                        "workflow event link ref cannot have an unspecified event type and a non-zero event ID"
                            .to_string(),
                    ));
                }
            }
            Some(proto_common::link::Variant::BatchJob(job)) => {
                if job.job_id.is_empty() {
                    return Err(ProtoConversionError::InvalidArgument(
                        "batch job link must not have an empty job ID".to_string(),
                    ));
                }
            }
            // v1.31.0 admits only WorkflowEvent and BatchJob on these paths;
            // Activity / NexusOperation / unset variants are rejected.
            _ => {
                return Err(ProtoConversionError::InvalidArgument(
                    "unsupported link variant".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_callback_url(raw_url: &str) -> Result<(), ProtoConversionError> {
    if raw_url.len() > CALLBACK_URL_MAX_LENGTH {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "invalid url: url length longer than max length allowed of {CALLBACK_URL_MAX_LENGTH}"
        )));
    }
    let Some(rest) = raw_url
        .strip_prefix("http://")
        .or_else(|| raw_url.strip_prefix("https://"))
    else {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "invalid url: unknown scheme: {raw_url}"
        )));
    };
    let host_end = rest
        .find(|ch| ['/', '?', '#'].contains(&ch))
        .unwrap_or(rest.len());
    if rest[..host_end].is_empty() {
        return Err(ProtoConversionError::InvalidArgument(
            "invalid url: missing host".to_string(),
        ));
    }
    let uri = raw_url.parse::<http::Uri>().map_err(|err| {
        ProtoConversionError::InvalidArgument(format!("invalid callback url: {err}"))
    })?;
    if uri.host().is_none_or(str::is_empty) {
        return Err(ProtoConversionError::InvalidArgument(
            "invalid url: missing host".to_string(),
        ));
    }
    // Temporal also evaluates dynamic address allow-list policy after URL
    // shape validation. Tokeira has no callback address-policy config surface
    // yet, so hard-coding that deployment policy here would make admission
    // stricter than the configured server rather than more compatible.
    Ok(())
}

fn callbacks_to_edge(
    callbacks: &[proto_common::Callback],
) -> Result<Vec<EdgeCompletionCallback>, ProtoConversionError> {
    callbacks.iter().map(callback_to_edge).collect()
}

fn priority_to_edge(priority: Option<&proto_common::Priority>) -> Option<EdgePriority> {
    priority.map(|priority| EdgePriority {
        priority_key: priority.priority_key,
        fairness_key: priority.fairness_key.clone(),
        fairness_weight: priority.fairness_weight,
    })
}

fn on_conflict_options_to_edge(
    options: Option<&workflow::OnConflictOptions>,
) -> Result<Option<EdgeOnConflictOptions>, ProtoConversionError> {
    match options {
        Some(options) if options.attach_completion_callbacks && !options.attach_request_id => {
            Err(ProtoConversionError::MissingField(
                "StartWorkflowExecutionRequest.on_conflict_options.attach_request_id",
            ))
        }
        Some(options) => Ok(Some(EdgeOnConflictOptions {
            attach_request_id: options.attach_request_id,
            attach_completion_callbacks: options.attach_completion_callbacks,
            attach_links: options.attach_links,
        })),
        None => Ok(None),
    }
}

fn activity_retry_classification(failure: &failure_proto::Failure) -> (Option<String>, bool) {
    let mut cursor = Some(failure);
    while let Some(current) = cursor {
        if let Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(info)) =
            &current.failure_info
        {
            let error_type = non_empty(info.r#type.clone())
                .or_else(|| Some(NON_RETRYABLE_ACTIVITY_SENTINEL.to_string()));
            return (error_type, info.non_retryable);
        }
        cursor = current.cause.as_deref();
    }
    (None, false)
}

fn retry_policy_to_domain(value: &tokeira_proto::common::RetryPolicy) -> RetryPolicy {
    RetryPolicy {
        initial_interval: proto_duration_to_time(value.initial_interval.as_ref())
            .unwrap_or(time::Duration::ZERO),
        backoff_coefficient: if value.backoff_coefficient > 0.0 {
            value.backoff_coefficient
        } else {
            1.0
        },
        maximum_interval: proto_duration_to_time(value.maximum_interval.as_ref()),
        maximum_attempts: value.maximum_attempts.max(0) as u32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

fn retry_policy_from_domain(value: &RetryPolicy) -> tokeira_proto::common::RetryPolicy {
    tokeira_proto::common::RetryPolicy {
        initial_interval: Some(to_proto_duration(value.initial_interval)),
        backoff_coefficient: value.backoff_coefficient,
        maximum_interval: value.maximum_interval.map(to_proto_duration),
        maximum_attempts: value.maximum_attempts as i32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

/// Materialize v1.31.0's default activity retry policy and fill unset subfields
/// (`common/retrypolicy/retry_policy.go` `DefaultDefaultRetrySettings` + `EnsureDefaults`:
/// InitialInterval 1s, BackoffCoefficient 2.0, MaximumInterval 100×InitialInterval,
/// MaximumAttempts 0 = unlimited). Activities always carry a retry policy in v1.31.0, so the
/// ScheduleActivity command records this even when the SDK omits one — unlike the generic
/// `retry_policy_to_domain`, which defaults BackoffCoefficient to 1.0.
fn activity_retry_policy_with_defaults(
    proto: Option<&tokeira_proto::common::RetryPolicy>,
) -> RetryPolicy {
    let initial_interval = proto
        .and_then(|p| proto_duration_to_time(p.initial_interval.as_ref()))
        .filter(|interval| !interval.is_zero())
        .unwrap_or(time::Duration::seconds(1));
    let backoff_coefficient = proto
        .map(|p| p.backoff_coefficient)
        .filter(|coefficient| *coefficient > 0.0)
        .unwrap_or(2.0);
    let maximum_interval = proto
        .and_then(|p| proto_duration_to_time(p.maximum_interval.as_ref()))
        .filter(|interval| !interval.is_zero())
        .unwrap_or(initial_interval * 100);
    RetryPolicy {
        initial_interval,
        backoff_coefficient,
        maximum_interval: Some(maximum_interval),
        maximum_attempts: proto.map(|p| p.maximum_attempts.max(0) as u32).unwrap_or(0),
        non_retryable_error_types: proto
            .map(|p| p.non_retryable_error_types.clone())
            .unwrap_or_default(),
    }
}

/// Apply v1.31.0 workflow-start retry-policy `EnsureDefaults` to a
/// client-supplied policy, filling only unset subfields: InitialInterval 1s,
/// BackoffCoefficient 2.0, MaximumInterval 100×InitialInterval, MaximumAttempts 0
/// (unlimited) (`common/retrypolicy/retry_policy.go` `EnsureDefaults`, applied at
/// StartWorkflowExecution via `workflow_handler.go:6600 @ v1.31.0`). Only called
/// when the client actually supplied a policy, so an absent policy still means
/// "no retry" (unlike activities, which always carry one). The runtime's retry
/// evaluation depends on these defaults (e.g. the 1s InitialInterval drives the
/// backoff that `TestWorkflowRetry` asserts), so defaulting here — not the
/// generic `retry_policy_to_domain`, which uses BackoffCoefficient 1.0 — is what
/// makes the workflow retry chain match the release.
fn workflow_retry_policy_with_defaults(proto: &tokeira_proto::common::RetryPolicy) -> RetryPolicy {
    let initial_interval = proto_duration_to_time(proto.initial_interval.as_ref())
        .filter(|interval| !interval.is_zero())
        .unwrap_or(time::Duration::seconds(1));
    let backoff_coefficient = if proto.backoff_coefficient > 0.0 {
        proto.backoff_coefficient
    } else {
        2.0
    };
    let maximum_interval = proto_duration_to_time(proto.maximum_interval.as_ref())
        .filter(|interval| !interval.is_zero())
        .unwrap_or(initial_interval * 100);
    RetryPolicy {
        initial_interval,
        backoff_coefficient,
        maximum_interval: Some(maximum_interval),
        maximum_attempts: proto.maximum_attempts.max(0) as u32,
        non_retryable_error_types: proto.non_retryable_error_types.clone(),
    }
}

fn parent_close_policy_to_domain(value: i32) -> ParentClosePolicy {
    match value {
        2 => ParentClosePolicy::Abandon,
        3 => ParentClosePolicy::RequestCancel,
        _ => ParentClosePolicy::Terminate,
    }
}

fn parent_close_policy_from_domain(value: ParentClosePolicy) -> i32 {
    match value {
        ParentClosePolicy::Terminate => 1,
        ParentClosePolicy::Abandon => 2,
        ParentClosePolicy::RequestCancel => 3,
    }
}

fn namespace_name_to_domain(value: &str) -> NamespaceId {
    if value.is_empty() {
        NamespaceId(Uuid::nil())
    } else if let Ok(uuid) = Uuid::parse_str(value) {
        NamespaceId(uuid)
    } else {
        namespace_id_for(value)
    }
}

fn parse_run_id(value: &str) -> Result<RunId, ProtoConversionError> {
    Ok(RunId(Uuid::parse_str(value)?))
}

fn extract_conflict_policy(value: i32) -> tokeira_kernel::WorkflowIdConflictPolicy {
    match enums::WorkflowIdConflictPolicy::try_from(value).ok() {
        Some(enums::WorkflowIdConflictPolicy::UseExisting) => {
            tokeira_kernel::WorkflowIdConflictPolicy::UseExisting
        }
        Some(enums::WorkflowIdConflictPolicy::TerminateExisting) => {
            tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting
        }
        _ => tokeira_kernel::WorkflowIdConflictPolicy::Fail,
    }
}

fn extract_reuse_policy(value: i32) -> tokeira_kernel::WorkflowIdReusePolicy {
    match enums::WorkflowIdReusePolicy::try_from(value).ok() {
        Some(enums::WorkflowIdReusePolicy::AllowDuplicateFailedOnly) => {
            tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicateFailedOnly
        }
        Some(enums::WorkflowIdReusePolicy::RejectDuplicate) => {
            tokeira_kernel::WorkflowIdReusePolicy::RejectDuplicate
        }
        Some(enums::WorkflowIdReusePolicy::TerminateIfRunning) => {
            tokeira_kernel::WorkflowIdReusePolicy::TerminateIfRunning
        }
        _ => tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
    }
}

/// Map the activity `ActivityIdReusePolicy`/`ActivityIdConflictPolicy` request
/// fields onto the CHASM business-id policy, mirroring `businessIDReusePolicyMap`
/// / `businessIDConflictPolicyMap` (`chasm/lib/activity/handler.go:19-27 @
/// v1.31.0`). An unspecified policy is normalized to the v1.31.0 defaults
/// (`AllowDuplicate`/`Fail`, `validator.go:210 @ v1.31.0`); any value outside the
/// mapped set is rejected with `InvalidArgument`, matching the handler's
/// "unsupported ID … policy" error. `TerminateExisting` is deliberately absent
/// from the activity conflict map and so is rejected here.
pub fn activity_id_policy_to_chasm(
    reuse: i32,
    conflict: i32,
) -> Result<tokeira_chasm::BusinessIdPolicy, ProtoConversionError> {
    use enums::{ActivityIdConflictPolicy as Conflict, ActivityIdReusePolicy as Reuse};
    use tokeira_chasm::{BusinessIdConflictPolicy, BusinessIdPolicy, BusinessIdReusePolicy};

    let reuse_policy = match Reuse::try_from(reuse).ok() {
        Some(Reuse::Unspecified) | Some(Reuse::AllowDuplicate) => {
            BusinessIdReusePolicy::AllowDuplicate
        }
        Some(Reuse::AllowDuplicateFailedOnly) => BusinessIdReusePolicy::AllowDuplicateFailedOnly,
        Some(Reuse::RejectDuplicate) => BusinessIdReusePolicy::RejectDuplicate,
        _ => {
            return Err(ProtoConversionError::InvalidArgument(format!(
                "unsupported ID reuse policy: {reuse}"
            )));
        }
    };
    let conflict_policy = match Conflict::try_from(conflict).ok() {
        Some(Conflict::Unspecified) | Some(Conflict::Fail) => BusinessIdConflictPolicy::Fail,
        Some(Conflict::UseExisting) => BusinessIdConflictPolicy::UseExisting,
        _ => {
            return Err(ProtoConversionError::InvalidArgument(format!(
                "unsupported ID conflict policy: {conflict}"
            )));
        }
    };
    Ok(BusinessIdPolicy {
        reuse: reuse_policy,
        conflict: conflict_policy,
    })
}

fn migrate_reuse_policy(
    reuse: &mut tokeira_kernel::WorkflowIdReusePolicy,
    conflict: &mut tokeira_kernel::WorkflowIdConflictPolicy,
    raw_reuse_value: i32,
) {
    if matches!(
        enums::WorkflowIdReusePolicy::try_from(raw_reuse_value).ok(),
        Some(enums::WorkflowIdReusePolicy::TerminateIfRunning)
    ) {
        *reuse = tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate;
        *conflict = tokeira_kernel::WorkflowIdConflictPolicy::TerminateExisting;
    }
}

// Read the modern oneof first, then the deprecated v0.30/v0.31 fields. Temporal
// v1.31.0 accepts both shapes and validates the modern pinned version before
// consulting the deprecated behavior (`ValidateVersioningOverride`,
// `common/worker_versioning/worker_versioning.go:672-718 @ v1.31.0`).
#[allow(deprecated)]
fn versioning_override_to_edge(
    override_: Option<workflow::VersioningOverride>,
) -> Result<Option<VersioningOverride>, ProtoConversionError> {
    let Some(override_) = override_ else {
        return Ok(None);
    };
    if let Some(modern) = override_.r#override {
        return match modern {
            workflow::versioning_override::Override::Pinned(pinned) => {
                if pinned.behavior
                    != workflow::versioning_override::PinnedOverrideBehavior::Pinned as i32
                {
                    return Err(ProtoConversionError::InvalidArgument(
                        "must specify pinned override behavior if override is pinned.".to_string(),
                    ));
                }
                let version = pinned.version.ok_or_else(|| {
                    ProtoConversionError::InvalidArgument(
                        "must provide version if override is pinned.".to_string(),
                    )
                })?;
                if version.deployment_name.is_empty() || version.build_id.is_empty() {
                    return Err(ProtoConversionError::MissingField(
                        "VersioningOverride.pinned.version.deployment_name/build_id",
                    ));
                }
                Ok(Some(VersioningOverride::Pinned {
                    deployment_series: version.deployment_name,
                    build_id: version.build_id,
                }))
            }
            workflow::versioning_override::Override::AutoUpgrade(enabled) if enabled => {
                Ok(Some(VersioningOverride::AutoUpgrade))
            }
            workflow::versioning_override::Override::AutoUpgrade(_) => Err(
                ProtoConversionError::InvalidArgument("override behavior is required".to_string()),
            ),
        };
    }
    match enums::VersioningBehavior::try_from(override_.behavior).ok() {
        Some(enums::VersioningBehavior::Pinned) => {
            let deployment = override_
                .deployment
                .ok_or(ProtoConversionError::MissingField(
                    "VersioningOverride.deployment",
                ))?;
            if deployment.series_name.is_empty() || deployment.build_id.is_empty() {
                return Err(ProtoConversionError::MissingField(
                    "VersioningOverride.deployment.series_name/build_id",
                ));
            }
            Ok(Some(VersioningOverride::Pinned {
                deployment_series: deployment.series_name,
                build_id: deployment.build_id,
            }))
        }
        Some(enums::VersioningBehavior::AutoUpgrade) => Ok(Some(VersioningOverride::AutoUpgrade)),
        _ => Ok(None),
    }
}

/// Validate + reduce an `UpdateWorkflowExecutionOptions` request to the supported change.
///
/// tokeira persists exactly one mutable execution option, `versioning_override`. The
/// `update_mask` selects which options to apply (`mergeWorkflowExecutionOptions`,
/// `service/history/api/updateworkflowoptions/api.go @ v1.31.0`); we recognize
/// `versioning_override` and its deprecated `versioning_override.{behavior,deployment}`
/// sub-paths (which v1.31.0 requires be masked together). An empty mask, or a mask naming
/// any other option (e.g. `priority`, `time_skipping_config` — valid v1.31.0 fields tokeira
/// does not yet model), is rejected with `INVALID_ARGUMENT` rather than silently dropped.
pub fn update_workflow_execution_options_request_to_edge(
    req: workflowservice::UpdateWorkflowExecutionOptionsRequest,
) -> Result<EdgeUpdateWorkflowExecutionOptionsRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .ok_or(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionOptionsRequest.workflow_execution",
        ))?;
    if execution.workflow_id.trim().is_empty() {
        return Err(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionOptionsRequest.workflow_execution.workflow_id",
        ));
    }

    let paths = req.update_mask.map(|mask| mask.paths).unwrap_or_default();
    if paths.is_empty() {
        return Err(ProtoConversionError::InvalidArgument(
            "update_mask must name at least one option to update".to_string(),
        ));
    }
    let (mut behavior_masked, mut deployment_masked) = (false, false);
    for path in &paths {
        match path.as_str() {
            // The whole field, or both deprecated sub-fields together.
            "versioning_override" => {}
            "versioning_override.behavior" => behavior_masked = true,
            "versioning_override.deployment" => deployment_masked = true,
            other => {
                return Err(ProtoConversionError::InvalidArgument(format!(
                    "unsupported update_mask path: {other}"
                )));
            }
        }
    }
    // Deprecated sub-fields must be masked together (v1.31.0 `mergeWorkflowExecutionOptions`).
    if behavior_masked != deployment_masked {
        return Err(ProtoConversionError::InvalidArgument(
            "versioning_override fields must be updated together".to_string(),
        ));
    }

    // The mask is guaranteed to touch `versioning_override`, so the change is Set-or-Clear:
    // a recognized override present in the options is Set; an absent (or unrepresentable)
    // override clears it (`mergedOpts.GetVersioningOverride() == nil → unset` @ v1.31.0).
    let options = req.workflow_execution_options.unwrap_or_default();
    let versioning_override = match versioning_override_to_edge(options.versioning_override)? {
        Some(override_) => VersioningOverrideChange::Set(override_),
        None => VersioningOverrideChange::Clear,
    };

    Ok(EdgeUpdateWorkflowExecutionOptionsRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id,
        run_id: (!execution.run_id.is_empty()).then_some(execution.run_id),
        versioning_override,
        identity: req.identity,
    })
}

/// Project the post-update execution options back onto the wire response.
pub fn update_workflow_execution_options_response_to_proto(
    resp: EdgeUpdateWorkflowExecutionOptionsResponse,
) -> workflowservice::UpdateWorkflowExecutionOptionsResponse {
    workflowservice::UpdateWorkflowExecutionOptionsResponse {
        workflow_execution_options: Some(workflow::WorkflowExecutionOptions {
            versioning_override: resp.versioning_override.map(|override_| {
                versioning_override_from_edge(&versioning_override_to_kernel(&override_))
            }),
            ..Default::default()
        }),
    }
}

/// Translate an inbound `RespondWorkflowTaskFailed` cause
/// (`temporal.api.enums.v1.WorkflowTaskFailedCause`) to the kernel enum.
/// v1.31.0 records the reported cause verbatim on the `WorkflowTaskFailed`
/// event (`respondworkflowtaskfailed/api.go @ v1.31.0`); causes the kernel
/// does not model yet fall back to `WorkflowWorkerUnhandledFailure`, the
/// generic worker-reported failure. `GrpcMessageTooLarge` (36) selects the
/// runtime's force-close-terminate route.
pub fn wft_failed_cause_from_proto(value: i32) -> tokeira_kernel::WorkflowTaskFailedCause {
    use tokeira_kernel::WorkflowTaskFailedCause as K;
    use tokeira_proto::enums::WorkflowTaskFailedCause as P;
    match P::try_from(value) {
        Ok(P::UnhandledCommand) => K::UnhandledCommand,
        Ok(P::BadScheduleActivityAttributes) => K::BadScheduleActivityAttributes,
        Ok(P::BadRequestCancelActivityAttributes) => K::BadRequestCancelActivityAttributes,
        Ok(P::BadStartTimerAttributes) => K::BadStartTimerAttributes,
        Ok(P::BadCancelTimerAttributes) => K::BadCancelTimerAttributes,
        Ok(P::BadRecordMarkerAttributes) => K::BadRecordMarkerAttributes,
        Ok(P::BadSignalWorkflowExecutionAttributes) => K::BadSignalWorkflowExecutionAttributes,
        Ok(P::BadRequestCancelExternalWorkflowExecutionAttributes) => {
            K::BadRequestCancelExternalWorkflowExecutionAttributes
        }
        Ok(P::ResetWorkflow) => K::ResetWorkflow,
        Ok(P::NonDeterministicError) => K::NonDeterminismError,
        Ok(P::ForceCloseCommand) => K::ForceCloseCommand,
        Ok(P::GrpcMessageTooLarge) => K::GrpcMessageTooLarge,
        // `WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE
        // = 30` (failed_cause.proto; spec speculative-wft K5).
        Ok(P::BadUpdateWorkflowExecutionMessage) => K::BadUpdateWorkflowExecutionMessage,
        _ => K::WorkflowWorkerUnhandledFailure,
    }
}

/// The engine's reserved internal per-namespace worker task queue
/// (`primitives.PerNSWorkerTaskQueue @ v1.31.0 common/primitives/task_queues.go:12`).
const PER_NS_WORKER_TASK_QUEUE: &str = "temporal-sys-per-ns-tq";

/// Reject a user-issued Start / SignalWithStart targeting the reserved internal
/// per-namespace worker task queue. Mirrors `CheckInternalPerNsTaskQueueAllowed`
/// (task_queues.go:24-43 @ v1.31.0): with no internal parent component, scheduling
/// onto the per-ns task queue is illegal and surfaces as InvalidArgument with the
/// verbatim message the SDK conformance suite asserts on.
fn reject_internal_per_ns_task_queue(task_queue: &str) -> Result<(), ProtoConversionError> {
    if task_queue == PER_NS_WORKER_TASK_QUEUE {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "cannot use internal per-namespace task queue:{task_queue}"
        )));
    }
    Ok(())
}

pub fn start_request_to_edge(
    req: workflowservice::StartWorkflowExecutionRequest,
) -> Result<StartWorkflowExecutionRequest, ProtoConversionError> {
    reject_behavioral_time_skipping(
        req.time_skipping_config.as_ref(),
        "StartWorkflowExecutionRequest.time_skipping_config",
    )?;
    if req.continued_failure.is_some() {
        return Err(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.continued_failure",
        ));
    }
    if req.last_completion_result.is_some() {
        return Err(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.last_completion_result",
        ));
    }
    if req.workflow_start_delay.is_some() && !req.cron_schedule.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.workflow_start_delay/cron_schedule",
        ));
    }

    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "StartWorkflowExecutionRequest.task_queue",
        ))?;
    reject_internal_per_ns_task_queue(&task_queue.name)?;

    let mut conflict_policy = extract_conflict_policy(req.workflow_id_conflict_policy);
    let mut reuse_policy = extract_reuse_policy(req.workflow_id_reuse_policy);
    migrate_reuse_policy(
        &mut reuse_policy,
        &mut conflict_policy,
        req.workflow_id_reuse_policy,
    );

    let cron_schedule = non_empty(req.cron_schedule);
    validate_client_cron_schedule(cron_schedule.as_deref())?;
    validate_completion_callbacks(&req.completion_callbacks)?;
    validate_links(&collect_admission_links(
        &req.links,
        &req.completion_callbacks,
    ))?;

    Ok(StartWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        workflow_type: req.workflow_type.map(|wt| wt.name).unwrap_or_default(),
        task_queue: task_queue.name.clone(),
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        memo: req.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        search_attributes: req
            .search_attributes
            .as_ref()
            .map(search_attributes_to_domain)
            .transpose()?
            .unwrap_or_default(),
        identity: non_empty(req.identity),
        request_eager_execution: req.request_eager_execution,
        workflow_start_delay: valid_non_negative_duration(
            req.workflow_start_delay.as_ref(),
            "StartWorkflowExecutionRequest.workflow_start_delay",
        )?,
        completion_callbacks: callbacks_to_edge(&req.completion_callbacks)?,
        user_metadata: user_metadata_to_edge(req.user_metadata.as_ref()),
        links: links_to_edge(&req.links)?,
        eager_worker_deployment_options: worker_deployment_version_from_options(
            req.eager_worker_deployment_options.as_ref(),
            "StartWorkflowExecutionRequest.eager_worker_deployment_options",
        )?,
        workflow_execution_timeout: workflow_timeout_to_time(
            req.workflow_execution_timeout.as_ref(),
        ),
        workflow_run_timeout: workflow_timeout_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: workflow_task_timeout_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req
            .retry_policy
            .as_ref()
            .map(workflow_retry_policy_with_defaults),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        versioning_override: versioning_override_to_edge(req.versioning_override)?,
        on_conflict_options: on_conflict_options_to_edge(req.on_conflict_options.as_ref())?,
        priority: priority_to_edge(req.priority.as_ref()),
        cron_schedule,
        run_key: None,
        run_id: None,
        now: None,
    })
}

pub fn signal_request_to_edge(
    req: workflowservice::SignalWorkflowExecutionRequest,
) -> Result<SignalWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "SignalWorkflowExecutionRequest.workflow_execution",
        ))?;
    validate_links(&req.links)?;
    Ok(SignalWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        signal_name: req.signal_name,
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        header: req.header.as_ref().map(headers_to_domain),
        links: links_to_edge(&req.links)?,
        request_id: non_empty(req.request_id),
        identity: non_empty(req.identity),
        now: None,
    })
}

// v1.62-sync: reads deprecated `PollWorkflowTaskQueueRequest.worker_version_capabilities`
// for wire-compat with v0.4-era SDK workers. v1.62 replaces it with
// `deployment_options`; migration is tracked under `runtime-worker-versioning`.
#[allow(deprecated)]
pub fn poll_request_to_edge(
    req: workflowservice::PollWorkflowTaskQueueRequest,
) -> Result<PollWorkflowTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollWorkflowTaskQueueRequest.task_queue",
        ))?;

    let (deployment, build_id) = req
        .deployment_options
        .as_ref()
        .and_then(|options| {
            // Only a versioned worker auto-registers and routes by deployment; an
            // unversioned `deployment_options` carries no `(deployment, build_id)`
            // identity (`worker_versioning.go` versioning-mode gate @ v1.31.0).
            let mode =
                enums::WorkerVersioningMode::try_from(options.worker_versioning_mode).ok()?;
            if mode != enums::WorkerVersioningMode::Versioned {
                return None;
            }
            let deployment = non_empty(options.deployment_name.clone()).map(DeploymentId);
            let build_id = non_empty(options.build_id.clone()).map(BuildId);
            Some((deployment, build_id))
        })
        .or_else(|| {
            // Fall back to the deprecated capabilities for v0.4-era SDK workers that
            // predate `deployment_options`.
            req.worker_version_capabilities
                .as_ref()
                .filter(|caps| caps.use_versioning)
                .map(|caps| {
                    let deployment =
                        non_empty(caps.deployment_series_name.clone()).map(DeploymentId);
                    let build_id = non_empty(caps.build_id.clone()).map(BuildId);
                    (deployment, build_id)
                })
        })
        .unwrap_or((None, None));

    Ok(PollWorkflowTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        worker_instance_key: req.worker_instance_key,
        deployment,
        build_id,
        sticky_run: None,
        timeout: DEFAULT_POLL_TIMEOUT,
        sticky_ttl: DEFAULT_STICKY_TTL,
    })
}

/// Translate a WFT completion from the proto wire format into the edge DTO.
///
/// The critical step here is resolving `ProtocolMessage` commands. The SDK
/// sends update protocol responses as two correlated pieces: a
/// `ProtocolMessageCommandAttributes` (carrying only a `message_id`) and a
/// corresponding entry in the top-level `messages` list (carrying the actual
/// `Any`-typed body). We index the messages by ID, then for each
/// `ProtocolMessage` command we pop the matching message and decode its body
/// via `resolve_protocol_message_body`. Any messages not claimed by a command
/// are passed through as-is for edge-layer processing.
fn versioning_behavior_from_proto(value: i32) -> Result<VersioningBehavior, ProtoConversionError> {
    Ok(
        match enums::VersioningBehavior::try_from(value).map_err(|_| {
            ProtoConversionError::MissingField(
                "RespondWorkflowTaskCompletedRequest.versioning_behavior",
            )
        })? {
            enums::VersioningBehavior::Pinned => VersioningBehavior::Pinned,
            enums::VersioningBehavior::AutoUpgrade => VersioningBehavior::AutoUpgrade,
            enums::VersioningBehavior::Unspecified => VersioningBehavior::Unspecified,
        },
    )
}

fn worker_deployment_version_from_options(
    options: Option<&deployment_proto::WorkerDeploymentOptions>,
    field: &'static str,
) -> Result<Option<WorkerDeploymentVersionRef>, ProtoConversionError> {
    let Some(options) = options else {
        return Ok(None);
    };
    let mode = enums::WorkerVersioningMode::try_from(options.worker_versioning_mode)
        .map_err(|_| ProtoConversionError::MissingField(field))?;
    if mode != enums::WorkerVersioningMode::Versioned {
        return Ok(None);
    }
    if options.deployment_name.is_empty() || options.build_id.is_empty() {
        return Err(ProtoConversionError::MissingField(field));
    }
    Ok(Some(WorkerDeploymentVersionRef {
        deployment_name: options.deployment_name.clone(),
        build_id: options.build_id.clone(),
    }))
}

fn worker_deployment_name_from_options(
    options: Option<&deployment_proto::WorkerDeploymentOptions>,
    field: &'static str,
) -> Result<Option<String>, ProtoConversionError> {
    worker_deployment_version_from_options(options, field)
        .map(|version| version.map(|version| version.deployment_name))
}

fn worker_deployment_version_from_deprecated_deployment(
    deployment: Option<&deployment_proto::Deployment>,
) -> Option<WorkerDeploymentVersionRef> {
    let deployment = deployment?;
    if deployment.series_name.is_empty() || deployment.build_id.is_empty() {
        return None;
    }
    Some(WorkerDeploymentVersionRef {
        deployment_name: deployment.series_name.clone(),
        build_id: deployment.build_id.clone(),
    })
}

fn worker_deployment_name_from_deprecated_deployment(
    deployment: Option<&deployment_proto::Deployment>,
) -> Option<String> {
    deployment
        .filter(|deployment| !deployment.series_name.is_empty())
        .map(|deployment| deployment.series_name.clone())
}

fn sticky_spec_from_attributes(
    attrs: Option<&taskqueue_proto::StickyExecutionAttributes>,
) -> Result<Option<tokeira_kernel::StickySpec>, ProtoConversionError> {
    let Some(attrs) = attrs else {
        return Ok(None);
    };
    // v1.31.0 treats a missing `worker_task_queue` (or an empty queue name) as
    // "clear stickiness", NOT an error: `StickyAttributes.WorkerTaskQueue == nil`
    // → `ClearStickyTaskQueue`, and `SetStickyTaskQueue("")` leaves stickiness
    // unset (respondworkflowtaskcompleted/api.go:324-340 +
    // mutable_state_impl.go:1328-1339 @ v1.31.0). A worker completing a
    // speculative task without a sticky queue — e.g. a follow-up task delivered
    // inline in the completion response — simply drops its affinity, so this
    // must not fail the completion (`WorkerSkippedProcessing_RejectByServer`).
    let queue_name = match attrs.worker_task_queue.as_ref() {
        Some(queue) if !queue.name.is_empty() => queue.name.clone(),
        _ => return Ok(None),
    };
    // v1.31.0 defaults an unset sticky schedule-to-start timeout to 5s
    // (`stickyScheduleToStartTimeout` handling in matching); 30s was the old
    // tokeira TTL default and is kept — no corpus leaf pins the default.
    let schedule_to_start_timeout = valid_non_negative_duration(
        attrs.schedule_to_start_timeout.as_ref(),
        "RespondWorkflowTaskCompletedRequest.sticky_attributes.schedule_to_start_timeout",
    )?
    .unwrap_or(time::Duration::seconds(30));
    Ok(Some(tokeira_kernel::StickySpec {
        queue: tokeira_types::TaskQueueName(queue_name),
        schedule_to_start_timeout,
    }))
}

pub fn create_worker_deployment_to_edge(
    req: workflowservice::CreateWorkerDeploymentRequest,
) -> Result<CreateDeployment, ProtoConversionError> {
    validate_deployment_name(&req.deployment_name)?;
    Ok(CreateDeployment {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
        request_id: req.request_id,
        identity: req.identity,
    })
}

pub fn describe_worker_deployment_to_edge(
    req: workflowservice::DescribeWorkerDeploymentRequest,
) -> Result<DeploymentKey, ProtoConversionError> {
    require_non_empty(
        &req.deployment_name,
        "DescribeWorkerDeploymentRequest.deployment_name",
    )?;
    Ok(DeploymentKey {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
    })
}

pub fn delete_worker_deployment_to_edge(
    req: workflowservice::DeleteWorkerDeploymentRequest,
) -> Result<DeleteDeployment, ProtoConversionError> {
    require_non_empty(
        &req.deployment_name,
        "DeleteWorkerDeploymentRequest.deployment_name",
    )?;
    Ok(DeleteDeployment {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
        conflict_token: None,
    })
}

pub fn list_worker_deployments_to_edge(
    req: workflowservice::ListWorkerDeploymentsRequest,
) -> Result<ListDeployments, ProtoConversionError> {
    Ok(ListDeployments {
        namespace_id: namespace_id_for(&req.namespace),
        page_size: req.page_size,
        next_page_token: String::from_utf8(req.next_page_token)
            .map_err(|_| ProtoConversionError::MissingField("next_page_token utf8"))?,
    })
}

pub fn create_worker_deployment_version_to_edge(
    req: workflowservice::CreateWorkerDeploymentVersionRequest,
) -> Result<CreateVersion, ProtoConversionError> {
    let version = worker_deployment_version_to_domain(req.deployment_version.as_ref())?;
    Ok(CreateVersion {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version.deployment_name,
        build_id: version.build_id,
        compute_config: compute_config_to_domain(req.compute_config.as_ref()),
        request_id: req.request_id,
        identity: req.identity,
    })
}

#[allow(deprecated)]
pub fn describe_worker_deployment_version_to_edge(
    req: workflowservice::DescribeWorkerDeploymentVersionRequest,
) -> Result<DescribeVersion, ProtoConversionError> {
    let version = req
        .deployment_version
        .as_ref()
        .map(|version| worker_deployment_version_to_domain(Some(version)))
        .transpose()?;
    Ok(DescribeVersion {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version
            .as_ref()
            .map(|version| version.deployment_name.clone())
            .unwrap_or_else(|| DeploymentName(String::new())),
        build_id: version.map(|version| version.build_id),
        version: req.version,
        report_task_queue_stats: req.report_task_queue_stats,
    })
}

#[allow(deprecated)]
pub fn delete_worker_deployment_version_to_edge(
    req: workflowservice::DeleteWorkerDeploymentVersionRequest,
) -> Result<DeleteVersion, ProtoConversionError> {
    let version = req
        .deployment_version
        .as_ref()
        .map(|version| worker_deployment_version_to_domain(Some(version)))
        .transpose()?;
    Ok(DeleteVersion {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version
            .as_ref()
            .map(|version| version.deployment_name.clone())
            .unwrap_or_else(|| DeploymentName(String::new())),
        build_id: version.map(|version| version.build_id),
        version: req.version,
        skip_drainage: req.skip_drainage,
        conflict_token: None,
        identity: req.identity,
    })
}

/// Resolve the target build id for `SetWorkerDeploymentCurrentVersion` /
/// `SetWorkerDeploymentRampingVersion` from the (deprecated) `version` string and
/// the `build_id` field, mirroring v1.31.0
/// (`workflow_handler.go:3967,4012 @ v1.31.0`): the deprecated `version` string
/// takes precedence, then `build_id`. `__unversioned__` or an empty result
/// resolves to unversioned (`None`). The V31 version string is
/// `<deployment_name>.<build_id>`; deployment names cannot contain '.', so the
/// first '.' splits the parts.
fn resolve_set_target_build_id(
    deployment_name: &str,
    version: &str,
    build_id: &str,
) -> Option<DeploymentBuildId> {
    let version_string = if !version.is_empty() {
        version.to_string()
    } else if !build_id.is_empty() {
        format!("{deployment_name}.{build_id}")
    } else {
        return None;
    };
    if version_string == UNVERSIONED_VERSION_ID {
        return None;
    }
    version_string
        .split_once('.')
        .map(|(_, build)| DeploymentBuildId(build.to_string()))
        .filter(|build| !build.0.is_empty())
}

#[allow(deprecated)]
pub fn set_worker_deployment_current_version_to_edge(
    req: workflowservice::SetWorkerDeploymentCurrentVersionRequest,
) -> Result<SetCurrent, ProtoConversionError> {
    require_non_empty(
        &req.deployment_name,
        "SetWorkerDeploymentCurrentVersionRequest.deployment_name",
    )?;
    let build_id = resolve_set_target_build_id(&req.deployment_name, &req.version, &req.build_id);
    Ok(SetCurrent {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
        build_id,
        conflict_token: conflict_token_from_proto(req.conflict_token)?,
        identity: req.identity,
        allow_no_pollers: req.allow_no_pollers,
        ignore_missing_task_queues: req.ignore_missing_task_queues,
    })
}

#[allow(deprecated)]
pub fn set_worker_deployment_ramping_version_to_edge(
    req: workflowservice::SetWorkerDeploymentRampingVersionRequest,
) -> Result<SetRamping, ProtoConversionError> {
    require_non_empty(
        &req.deployment_name,
        "SetWorkerDeploymentRampingVersionRequest.deployment_name",
    )?;
    if !(0.0..=100.0).contains(&req.percentage) {
        return Err(ProtoConversionError::MissingField(
            "SetWorkerDeploymentRampingVersionRequest.percentage",
        ));
    }
    let build_id = resolve_set_target_build_id(&req.deployment_name, &req.version, &req.build_id);
    Ok(SetRamping {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
        build_id,
        ramping_percentage: req.percentage,
        conflict_token: conflict_token_from_proto(req.conflict_token)?,
        identity: req.identity,
        allow_no_pollers: req.allow_no_pollers,
        ignore_missing_task_queues: req.ignore_missing_task_queues,
    })
}

pub fn update_worker_deployment_version_compute_config_to_edge(
    req: workflowservice::UpdateWorkerDeploymentVersionComputeConfigRequest,
) -> Result<UpdateComputeConfig, ProtoConversionError> {
    let version = worker_deployment_version_to_domain(req.deployment_version.as_ref())?;
    Ok(UpdateComputeConfig {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version.deployment_name,
        build_id: Some(version.build_id),
        updates: compute_config_scaling_group_updates_to_domain(req.compute_config_scaling_groups),
        removals: req
            .remove_compute_config_scaling_groups
            .into_iter()
            .collect(),
        request_id: req.request_id,
        identity: req.identity,
    })
}

pub fn validate_worker_deployment_version_compute_config_to_edge(
    req: workflowservice::ValidateWorkerDeploymentVersionComputeConfigRequest,
) -> Result<ValidateComputeConfig, ProtoConversionError> {
    let version = worker_deployment_version_to_domain(req.deployment_version.as_ref())?;
    Ok(ValidateComputeConfig {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version.deployment_name,
        build_id: Some(version.build_id),
        updates: compute_config_scaling_group_updates_to_domain(req.compute_config_scaling_groups),
        removals: req
            .remove_compute_config_scaling_groups
            .into_iter()
            .collect(),
        identity: req.identity,
    })
}

#[allow(deprecated)]
pub fn update_worker_deployment_version_metadata_to_edge(
    req: workflowservice::UpdateWorkerDeploymentVersionMetadataRequest,
) -> Result<UpdateMetadata, ProtoConversionError> {
    let version = req
        .deployment_version
        .as_ref()
        .map(|version| worker_deployment_version_to_domain(Some(version)))
        .transpose()?;
    Ok(UpdateMetadata {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: version
            .as_ref()
            .map(|version| version.deployment_name.clone())
            .unwrap_or_else(|| DeploymentName(String::new())),
        build_id: version.map(|version| version.build_id),
        version: req.version,
        upsert_entries: req
            .upsert_entries
            .iter()
            .map(|(key, payload)| (key.clone(), payload_to_domain(payload)))
            .collect(),
        remove_entries: req.remove_entries.into_iter().collect(),
        identity: req.identity,
    })
}

pub fn set_worker_deployment_manager_to_edge(
    req: workflowservice::SetWorkerDeploymentManagerRequest,
) -> Result<SetManager, ProtoConversionError> {
    require_non_empty(
        &req.deployment_name,
        "SetWorkerDeploymentManagerRequest.deployment_name",
    )?;
    require_non_empty(&req.identity, "SetWorkerDeploymentManagerRequest.identity")?;
    let new_manager_identity = match req.new_manager_identity {
        Some(workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::ManagerIdentity(identity)) => {
            Some(NewManagerIdentity::ManagerIdentity(identity))
        }
        Some(workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::Self_(true)) => {
            Some(NewManagerIdentity::SelfIdentity)
        }
        Some(workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::Self_(false))
        | None => {
            return Err(ProtoConversionError::MissingField(
                "SetWorkerDeploymentManagerRequest.new_manager_identity",
            ));
        }
    };
    Ok(SetManager {
        namespace_id: namespace_id_for(&req.namespace),
        deployment_name: DeploymentName(req.deployment_name),
        new_manager_identity,
        conflict_token: conflict_token_from_proto(req.conflict_token)?,
        identity: req.identity,
    })
}

pub fn create_worker_deployment_response_from_edge(
    conflict_token: ConflictToken,
) -> workflowservice::CreateWorkerDeploymentResponse {
    workflowservice::CreateWorkerDeploymentResponse {
        conflict_token: conflict_token_to_proto(conflict_token),
    }
}

pub fn describe_worker_deployment_response_from_edge(
    view: &DeploymentView,
) -> workflowservice::DescribeWorkerDeploymentResponse {
    workflowservice::DescribeWorkerDeploymentResponse {
        conflict_token: conflict_token_to_proto(view.conflict_token),
        worker_deployment_info: Some(worker_deployment_info_from_edge(view)),
    }
}

pub fn list_worker_deployments_response_from_edge(
    page: &DeploymentPage,
) -> workflowservice::ListWorkerDeploymentsResponse {
    workflowservice::ListWorkerDeploymentsResponse {
        next_page_token: page.next_page_token.as_bytes().to_vec(),
        worker_deployments: page
            .deployments
            .iter()
            .map(worker_deployment_summary_from_edge)
            .collect(),
    }
}

pub fn describe_worker_deployment_version_response_from_edge(
    view: &VersionView,
) -> workflowservice::DescribeWorkerDeploymentVersionResponse {
    workflowservice::DescribeWorkerDeploymentVersionResponse {
        worker_deployment_version_info: Some(worker_deployment_version_info_from_edge(view)),
        version_task_queues: view
            .task_queues
            .iter()
            .map(version_task_queue_from_edge)
            .collect(),
    }
}

#[allow(deprecated)]
pub fn set_worker_deployment_current_version_response_from_edge(
    outcome: &SetCurrentOutcome,
) -> workflowservice::SetWorkerDeploymentCurrentVersionResponse {
    workflowservice::SetWorkerDeploymentCurrentVersionResponse {
        conflict_token: conflict_token_to_proto(outcome.conflict_token),
        previous_version: outcome
            .previous_current_version
            .as_ref()
            .map(|build_id| version_string(&outcome.deployment.name, build_id))
            .unwrap_or_default(),
        previous_deployment_version: outcome.previous_current_version.as_ref().map(|build_id| {
            worker_deployment_version_from_parts(&outcome.deployment.name, build_id)
        }),
    }
}

#[allow(deprecated)]
pub fn set_worker_deployment_ramping_version_response_from_edge(
    outcome: &SetRampingOutcome,
) -> workflowservice::SetWorkerDeploymentRampingVersionResponse {
    workflowservice::SetWorkerDeploymentRampingVersionResponse {
        conflict_token: conflict_token_to_proto(outcome.conflict_token),
        previous_version: outcome
            .previous_ramping_version
            .as_ref()
            .map(|build_id| version_string(&outcome.deployment.name, build_id))
            .unwrap_or_default(),
        previous_percentage: outcome.previous_ramping_percentage,
        previous_deployment_version: outcome.previous_ramping_version.as_ref().map(|build_id| {
            worker_deployment_version_from_parts(&outcome.deployment.name, build_id)
        }),
    }
}

pub fn update_worker_deployment_version_metadata_response_from_edge(
    view: &VersionMetadataView,
) -> workflowservice::UpdateWorkerDeploymentVersionMetadataResponse {
    workflowservice::UpdateWorkerDeploymentVersionMetadataResponse {
        metadata: Some(version_metadata_from_edge(&view.metadata)),
    }
}

#[allow(deprecated)]
pub fn set_worker_deployment_manager_response_from_edge(
    outcome: &SetManagerOutcome,
) -> workflowservice::SetWorkerDeploymentManagerResponse {
    workflowservice::SetWorkerDeploymentManagerResponse {
        conflict_token: conflict_token_to_proto(outcome.conflict_token),
        previous_manager_identity: outcome
            .previous_manager_identity
            .clone()
            .unwrap_or_default(),
    }
}

pub fn worker_deployment_info_from_edge(
    view: &DeploymentView,
) -> deployment_proto::WorkerDeploymentInfo {
    deployment_proto::WorkerDeploymentInfo {
        name: view.name.0.clone(),
        version_summaries: {
            // DescribeWorkerDeployment returns version summaries sorted by
            // create_time descending (newest first), matching v1.31.0
            // `client.go:1784 @ v1.31.0` (`sort.Slice(... CreateTime.After ...)`).
            // The registry hands them back in build-id order, so re-sort here on
            // the source records (their `OffsetDateTime` create_time is ordered;
            // the proto timestamp is not) before mapping.
            let mut ordered: Vec<_> = view.versions.iter().collect();
            ordered.sort_by(|left, right| {
                right
                    .record
                    .create_time
                    .cmp(&left.record.create_time)
                    .then_with(|| left.build_id.0.cmp(&right.build_id.0))
            });
            ordered
                .into_iter()
                .map(worker_deployment_version_summary_from_edge)
                .collect()
        },
        create_time: Some(to_proto_timestamp(view.create_time)),
        routing_config: Some(routing_config_from_edge(&view.routing_config)),
        last_modifier_identity: view.last_modifier_identity.clone(),
        manager_identity: view.manager_identity.clone().unwrap_or_default(),
        routing_config_update_state: routing_config_update_state_to_proto(
            view.routing_config_update_state,
        ),
    }
}

#[allow(deprecated)]
pub fn worker_deployment_version_info_from_edge(
    view: &VersionView,
) -> deployment_proto::WorkerDeploymentVersionInfo {
    let record = &view.record;
    deployment_proto::WorkerDeploymentVersionInfo {
        version: version_string(&view.deployment_name, &view.build_id),
        status: worker_deployment_version_status_to_proto(record.status),
        deployment_version: Some(worker_deployment_version_from_parts(
            &view.deployment_name,
            &view.build_id,
        )),
        deployment_name: view.deployment_name.0.clone(),
        create_time: Some(to_proto_timestamp(record.create_time)),
        routing_changed_time: record.routing_changed_time.map(to_proto_timestamp),
        current_since_time: record.current_since_time.map(to_proto_timestamp),
        ramping_since_time: record.ramping_since_time.map(to_proto_timestamp),
        ramp_percentage: record.ramp_percentage,
        task_queue_infos: view
            .task_queues
            .iter()
            .map(version_task_queue_info_from_edge)
            .collect(),
        drainage_info: record
            .drainage_info
            .as_ref()
            .map(version_drainage_info_from_edge),
        metadata: Some(version_metadata_from_edge(&record.metadata)),
        first_activation_time: record.first_activation_time.map(to_proto_timestamp),
        last_deactivation_time: record.last_deactivation_time.map(to_proto_timestamp),
        last_current_time: record.last_current_time.map(to_proto_timestamp),
        compute_config: Some(compute_config_from_edge(&record.compute_config)),
        last_modifier_identity: record.last_modifier_identity.clone(),
    }
}

#[allow(deprecated)]
pub fn routing_config_from_edge(config: &StoredRoutingConfig) -> deployment_proto::RoutingConfig {
    deployment_proto::RoutingConfig {
        current_deployment_version: config
            .current_version
            .as_ref()
            .map(worker_deployment_version_key_from_edge),
        current_version: config
            .current_version
            .as_ref()
            .map(worker_deployment_version_string_from_key)
            .unwrap_or_else(|| UNVERSIONED_VERSION_ID.to_string()),
        ramping_deployment_version: config
            .ramping_version
            .as_ref()
            .map(worker_deployment_version_key_from_edge),
        ramping_version: if config.ramping_to_unversioned {
            // A ramp to unversioned workers carries a nil structured version but the
            // `__unversioned__` sentinel in the deprecated string field
            // (`ExternalWorkerDeploymentVersionToStringV31` of nil @ v1.31.0).
            UNVERSIONED_VERSION_ID.to_string()
        } else {
            config
                .ramping_version
                .as_ref()
                .map(worker_deployment_version_string_from_key)
                .unwrap_or_default()
        },
        ramping_version_percentage: config.ramping_version_percentage,
        current_version_changed_time: config.current_version_changed_time.map(to_proto_timestamp),
        ramping_version_changed_time: config.ramping_version_changed_time.map(to_proto_timestamp),
        ramping_version_percentage_changed_time: config
            .ramping_version_percentage_changed_time
            .map(to_proto_timestamp),
        revision_number: config.revision_number,
    }
}

pub fn version_metadata_from_edge(metadata: &VersionMetadata) -> deployment_proto::VersionMetadata {
    deployment_proto::VersionMetadata {
        entries: metadata
            .entries
            .iter()
            .map(|(key, payload)| (key.clone(), payload_from_domain(payload)))
            .collect(),
    }
}

pub fn compute_config_from_edge(config: &ComputeConfig) -> compute_proto::ComputeConfig {
    compute_proto::ComputeConfig {
        scaling_groups: config
            .scaling_groups
            .iter()
            .map(|(key, group)| (key.clone(), compute_scaling_group_from_edge(group)))
            .collect(),
    }
}

fn worker_deployment_version_to_domain(
    value: Option<&deployment_proto::WorkerDeploymentVersion>,
) -> Result<WorkerDeploymentVersionKey, ProtoConversionError> {
    let value = value.ok_or(ProtoConversionError::MissingField(
        "WorkerDeploymentVersion",
    ))?;
    if value.deployment_name.is_empty() || value.build_id.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "WorkerDeploymentVersion.deployment_name/build_id",
        ));
    }
    Ok(WorkerDeploymentVersionKey {
        deployment_name: DeploymentName(value.deployment_name.clone()),
        build_id: DeploymentBuildId(value.build_id.clone()),
    })
}

fn require_non_empty(value: &str, field: &'static str) -> Result<(), ProtoConversionError> {
    if value.trim().is_empty() {
        return Err(ProtoConversionError::MissingField(field));
    }
    Ok(())
}

/// Maximum length of various server-side IDs (`limit.maxIDLength` default,
/// `common/dynamicconfig/constants.go:423 @ v1.31.0`). Tokeira holds this as a
/// constant rather than dynamic config: it is a behavioural limit, not a
/// deployment-environment policy.
const MAX_ID_LENGTH_LIMIT: usize = 1000;
/// Fixed overhead of a worker-deployment version workflow id
/// (`WorkerDeploymentVersionWorkflowIDInitialSize = 39`,
/// `common/worker_versioning/worker_versioning.go:59 @ v1.31.0`). The deployment
/// name and build id share the remaining budget, halved between them.
const WORKER_DEPLOYMENT_VERSION_WORKFLOW_ID_INITIAL_SIZE: usize = 39;

/// Sentinel version string Temporal returns for "no current version" / an
/// unversioned worker (`worker_versioning.UnversionedVersionId = "__unversioned__"`,
/// `common/worker_versioning/worker_versioning.go:42 @ v1.31.0`). The deprecated
/// `RoutingConfig.current_version` string carries this when no current version is
/// set, while the structured `current_deployment_version` stays nil
/// (`client.go:735` + `workflow.go:245 @ v1.31.0`).
const UNVERSIONED_VERSION_ID: &str = "__unversioned__";

/// Workflow-id prefix of the Worker Deployment Version entity workflow
/// (`GenerateVersionWorkflowID`, `service/worker/workerdeployment/util.go:159 @ v1.31.0`):
/// `temporal-sys-worker-deployment-version:<deployment_name>:<build_id>`. Note the
/// version string here uses the `:` `WorkerDeploymentVersionDelimiter`
/// (`ExternalWorkerDeploymentVersionToString`), not the V31 `.` delimiter.
const WORKER_DEPLOYMENT_VERSION_WORKFLOW_ID_PREFIX: &str =
    "temporal-sys-worker-deployment-version:";

/// Workflow-id prefix of the Worker Deployment entity workflow
/// (`GenerateDeploymentWorkflowID`, `service/worker/workerdeployment/util.go:153 @ v1.31.0`):
/// `temporal-sys-worker-deployment:<deployment_name>`.
const WORKER_DEPLOYMENT_WORKFLOW_ID_PREFIX: &str = "temporal-sys-worker-deployment:";

/// Signal name the version entity workflow listens on for drainage updates
/// (`SyncDrainageSignalName`, `service/worker/workerdeployment/util.go:63 @ v1.31.0`).
pub const SYNC_DRAINAGE_SIGNAL_NAME: &str = "sync-drainage-status";

/// Signal name both the deployment and version entity workflows accept to force a
/// continue-as-new (`ForceCANSignalName`, `util.go:62 @ v1.31.0`). Tokeira backs
/// these entities with registry state, so a forced CAN has no durable effect and
/// is acknowledged as a no-op.
pub const FORCE_CAN_SIGNAL_NAME: &str = "force-continue-as-new";

/// Whether a workflow id addresses a Worker Deployment or Version entity workflow.
pub fn is_worker_deployment_entity_workflow_id(workflow_id: &str) -> bool {
    workflow_id.starts_with(WORKER_DEPLOYMENT_VERSION_WORKFLOW_ID_PREFIX)
        || workflow_id.starts_with(WORKER_DEPLOYMENT_WORKFLOW_ID_PREFIX)
}

/// Parse a Worker Deployment Version entity-workflow id into its
/// `(deployment_name, build_id)`, or `None` if the id is not a version entity
/// workflow. The suffix after the prefix is `<name>:<build_id>` (the `:`
/// `WorkerDeploymentVersionDelimiter`); deployment names cannot contain ':'
/// (enforced by `validate_deployment_name`), so the first ':' splits the parts.
pub fn parse_worker_deployment_version_workflow_id(workflow_id: &str) -> Option<(String, String)> {
    let rest = workflow_id.strip_prefix(WORKER_DEPLOYMENT_VERSION_WORKFLOW_ID_PREFIX)?;
    let (deployment_name, build_id) = rest.split_once(':')?;
    if deployment_name.is_empty() || build_id.is_empty() {
        return None;
    }
    Some((deployment_name.to_string(), build_id.to_string()))
}

/// Decode the `VersionDrainageStatus` carried by a `sync-drainage-status` signal
/// payload (a `binary/protobuf`-encoded `VersionDrainageInfo`,
/// `service/worker/workerdeployment/version_workflow.go:124 @ v1.31.0`).
pub fn decode_version_drainage_status(
    input: &Payloads,
) -> Result<VersionDrainageStatus, ProtoConversionError> {
    let payload = input.0.first().ok_or(ProtoConversionError::MissingField(
        "sync-drainage-status signal input",
    ))?;
    let info = deployment_proto::VersionDrainageInfo::decode(payload.data.as_slice())
        .map_err(|_| ProtoConversionError::MissingField("VersionDrainageInfo"))?;
    Ok(
        match enums::VersionDrainageStatus::try_from(info.status)
            .unwrap_or(enums::VersionDrainageStatus::Unspecified)
        {
            enums::VersionDrainageStatus::Draining => VersionDrainageStatus::Draining,
            enums::VersionDrainageStatus::Drained => VersionDrainageStatus::Drained,
            enums::VersionDrainageStatus::Unspecified => VersionDrainageStatus::Unspecified,
        },
    )
}

/// Maximum worker-deployment name length: the per-field budget v1.31.0 derives as
/// `(maxIDLength - versionWorkflowIDInitialSize) / 2`
/// (`ValidateDeploymentVersionFields`, `worker_versioning.go:563 @ v1.31.0`).
const WORKER_DEPLOYMENT_NAME_MAX_LEN: usize =
    (MAX_ID_LENGTH_LIMIT - WORKER_DEPLOYMENT_VERSION_WORKFLOW_ID_INITIAL_SIZE) / 2;

/// Validate a worker-deployment name against the v1.31.0 contract.
///
/// Mirrors the effective `CreateWorkerDeployment` validation order: the handler
/// rejects an empty name with a bespoke message before the shared field validator
/// runs (`service/frontend/workflow_handler.go:4154 @ v1.31.0`), then
/// `ValidateDeploymentVersionFields` checks length, the `.`/`:` version-string
/// delimiters, and the reserved `__` prefix
/// (`common/worker_versioning/worker_versioning.go:555 @ v1.31.0`). Messages are
/// reproduced verbatim because the corpus asserts on them
/// (`tests/worker_deployment_test.go` `TestCreateWorkerDeployment_InvalidDeploymentName`).
fn validate_deployment_name(name: &str) -> Result<(), ProtoConversionError> {
    if name.is_empty() {
        return Err(ProtoConversionError::InvalidArgument(
            "deployment name cannot be empty".to_string(),
        ));
    }
    if name.len() > WORKER_DEPLOYMENT_NAME_MAX_LEN {
        return Err(ProtoConversionError::InvalidArgument(
            "size of WorkerDeploymentName larger than the maximum allowed".to_string(),
        ));
    }
    // `.` is the v3.1 version-id delimiter; a deployment name carrying it would be
    // ambiguous with a `<name>.<build_id>` version string.
    if name.contains('.') {
        return Err(ProtoConversionError::InvalidArgument(
            "worker deployment name cannot contain '.'".to_string(),
        ));
    }
    // `:` is the version delimiter, banned in names for the same reason.
    if name.contains(':') {
        return Err(ProtoConversionError::InvalidArgument(
            "worker deployment name cannot contain ':'".to_string(),
        ));
    }
    // `__` is reserved for server-internal identifiers.
    if name.starts_with("__") {
        return Err(ProtoConversionError::InvalidArgument(
            "WorkerDeploymentName cannot start with '__'".to_string(),
        ));
    }
    Ok(())
}

fn conflict_token_from_proto(
    value: Vec<u8>,
) -> Result<Option<ConflictToken>, ProtoConversionError> {
    if value.is_empty() {
        return Ok(None);
    }
    // v1.31.0 treats the conflict token as opaque bytes compared with
    // `bytes.Equal` (`workflow.go:756/1181/1235 @ v1.31.0`): a token of any shape
    // that does not equal the stored one is a mismatch, not a malformed request.
    // Tokeira stores an 8-byte generation token; a supplied token that is not
    // exactly 8 bytes can never equal it, so we map it to a sentinel that will
    // never match a real generation rather than rejecting the request — letting
    // the registry surface "conflict token mismatch".
    match <[u8; tokeira_storage::CONFLICT_TOKEN_BYTES]>::try_from(value.as_slice()) {
        Ok(bytes) => Ok(Some(ConflictToken(bytes))),
        Err(_) => Ok(Some(ConflictToken::from_generation(u64::MAX))),
    }
}

fn conflict_token_to_proto(value: ConflictToken) -> Vec<u8> {
    value.0.to_vec()
}

fn compute_config_to_domain(value: Option<&compute_proto::ComputeConfig>) -> ComputeConfig {
    ComputeConfig {
        scaling_groups: value
            .map(|config| {
                config
                    .scaling_groups
                    .iter()
                    .map(|(key, group)| (key.clone(), compute_scaling_group_to_domain(group)))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn compute_config_scaling_group_updates_to_domain(
    updates: BTreeMap<String, compute_proto::ComputeConfigScalingGroupUpdate>,
) -> BTreeMap<String, ComputeConfigScalingGroupUpdate> {
    updates
        .into_iter()
        .map(|(key, update)| {
            (
                key,
                ComputeConfigScalingGroupUpdate {
                    scaling_group: update
                        .scaling_group
                        .as_ref()
                        .map(compute_scaling_group_to_domain)
                        .unwrap_or_default(),
                    update_mask: update
                        .update_mask
                        .map(|mask| mask.paths)
                        .unwrap_or_default(),
                },
            )
        })
        .collect()
}

fn compute_scaling_group_to_domain(
    group: &compute_proto::ComputeConfigScalingGroup,
) -> ComputeConfigScalingGroup {
    ComputeConfigScalingGroup {
        task_queue_types: group
            .task_queue_types
            .iter()
            .map(|value| deployment_task_queue_type_to_domain(*value))
            .collect(),
        provider: group.provider.as_ref().map(compute_provider_to_domain),
        scaler: group.scaler.as_ref().map(compute_scaler_to_domain),
    }
}

fn compute_provider_to_domain(provider: &compute_proto::ComputeProvider) -> ComputeProvider {
    ComputeProvider {
        provider_type: provider.r#type.clone(),
        details: provider.details.as_ref().map(payload_to_domain),
        nexus_endpoint: provider.nexus_endpoint.clone(),
    }
}

fn compute_scaler_to_domain(scaler: &compute_proto::ComputeScaler) -> ComputeScaler {
    ComputeScaler {
        scaler_type: scaler.r#type.clone(),
        details: scaler.details.as_ref().map(payload_to_domain),
    }
}

fn compute_scaling_group_from_edge(
    group: &ComputeConfigScalingGroup,
) -> compute_proto::ComputeConfigScalingGroup {
    compute_proto::ComputeConfigScalingGroup {
        task_queue_types: group
            .task_queue_types
            .iter()
            .map(|task_queue_type| deployment_task_queue_type_to_proto(*task_queue_type))
            .collect(),
        provider: group.provider.as_ref().map(compute_provider_from_edge),
        scaler: group.scaler.as_ref().map(compute_scaler_from_edge),
    }
}

fn compute_provider_from_edge(provider: &ComputeProvider) -> compute_proto::ComputeProvider {
    compute_proto::ComputeProvider {
        r#type: provider.provider_type.clone(),
        details: provider.details.as_ref().map(payload_from_domain),
        nexus_endpoint: provider.nexus_endpoint.clone(),
    }
}

fn compute_scaler_from_edge(scaler: &ComputeScaler) -> compute_proto::ComputeScaler {
    compute_proto::ComputeScaler {
        r#type: scaler.scaler_type.clone(),
        details: scaler.details.as_ref().map(payload_from_domain),
    }
}

fn worker_deployment_summary_from_edge(
    view: &DeploymentView,
) -> workflowservice::list_worker_deployments_response::WorkerDeploymentSummary {
    let latest_version_summary = view
        .versions
        .iter()
        .max_by_key(|version| version.record.create_time)
        .map(worker_deployment_version_summary_from_edge);
    let current_version_summary = view
        .routing_config
        .current_version
        .as_ref()
        .and_then(|version| find_version(view, version))
        .map(worker_deployment_version_summary_from_edge);
    let ramping_version_summary = view
        .routing_config
        .ramping_version
        .as_ref()
        .and_then(|version| find_version(view, version))
        .map(worker_deployment_version_summary_from_edge);

    workflowservice::list_worker_deployments_response::WorkerDeploymentSummary {
        name: view.name.0.clone(),
        create_time: Some(to_proto_timestamp(view.create_time)),
        routing_config: Some(routing_config_from_edge(&view.routing_config)),
        latest_version_summary,
        current_version_summary,
        ramping_version_summary,
    }
}

#[allow(deprecated)]
fn worker_deployment_version_summary_from_edge(
    view: &VersionView,
) -> deployment_proto::worker_deployment_info::WorkerDeploymentVersionSummary {
    let record = &view.record;
    deployment_proto::worker_deployment_info::WorkerDeploymentVersionSummary {
        version: version_string(&view.deployment_name, &view.build_id),
        status: worker_deployment_version_status_to_proto(record.status),
        deployment_version: Some(worker_deployment_version_from_parts(
            &view.deployment_name,
            &view.build_id,
        )),
        create_time: Some(to_proto_timestamp(record.create_time)),
        drainage_status: record
            .drainage_info
            .as_ref()
            .map(|info| version_drainage_status_to_proto(info.status))
            .unwrap_or_else(|| {
                version_drainage_status_to_proto(VersionDrainageStatus::Unspecified)
            }),
        drainage_info: record
            .drainage_info
            .as_ref()
            .map(version_drainage_info_from_edge),
        current_since_time: record.current_since_time.map(to_proto_timestamp),
        ramping_since_time: record.ramping_since_time.map(to_proto_timestamp),
        routing_update_time: record.routing_changed_time.map(to_proto_timestamp),
        first_activation_time: record.first_activation_time.map(to_proto_timestamp),
        last_deactivation_time: record.last_deactivation_time.map(to_proto_timestamp),
        last_current_time: record.last_current_time.map(to_proto_timestamp),
        compute_config: Some(compute_config_summary_from_edge(&record.compute_config)),
    }
}

fn compute_config_summary_from_edge(config: &ComputeConfig) -> compute_proto::ComputeConfigSummary {
    compute_proto::ComputeConfigSummary {
        scaling_groups: config
            .scaling_groups
            .iter()
            .map(|(key, group)| {
                (
                    key.clone(),
                    compute_proto::ComputeConfigScalingGroupSummary {
                        task_queue_types: group
                            .task_queue_types
                            .iter()
                            .map(|task_queue_type| {
                                deployment_task_queue_type_to_proto(*task_queue_type)
                            })
                            .collect(),
                        provider_type: group
                            .provider
                            .as_ref()
                            .map(|provider| provider.provider_type.clone())
                            .unwrap_or_default(),
                    },
                )
            })
            .collect(),
    }
}

fn version_task_queue_from_edge(
    view: &tokeira_runtime::VersionTaskQueueView,
) -> workflowservice::describe_worker_deployment_version_response::VersionTaskQueue {
    workflowservice::describe_worker_deployment_version_response::VersionTaskQueue {
        name: view.task_queue.name.clone(),
        r#type: deployment_task_queue_type_to_proto(view.task_queue.task_queue_type),
        stats: view.stats.map(|stats| taskqueue_proto::TaskQueueStats {
            approximate_backlog_count: stats.count as i64,
            approximate_backlog_age: time::Duration::try_from(stats.oldest_age)
                .ok()
                .map(to_proto_duration),
            tasks_add_rate: 0.0,
            tasks_dispatch_rate: 0.0,
        }),
        stats_by_priority_key: BTreeMap::new(),
    }
}

fn version_task_queue_info_from_edge(
    view: &tokeira_runtime::VersionTaskQueueView,
) -> deployment_proto::worker_deployment_version_info::VersionTaskQueueInfo {
    deployment_proto::worker_deployment_version_info::VersionTaskQueueInfo {
        name: view.task_queue.name.clone(),
        r#type: deployment_task_queue_type_to_proto(view.task_queue.task_queue_type),
    }
}

fn version_drainage_info_from_edge(info: &DrainageInfo) -> deployment_proto::VersionDrainageInfo {
    deployment_proto::VersionDrainageInfo {
        status: version_drainage_status_to_proto(info.status),
        last_changed_time: Some(to_proto_timestamp(info.last_changed_time)),
        last_checked_time: Some(to_proto_timestamp(info.last_checked_time)),
    }
}

fn worker_deployment_version_from_parts(
    deployment_name: &DeploymentName,
    build_id: &DeploymentBuildId,
) -> deployment_proto::WorkerDeploymentVersion {
    deployment_proto::WorkerDeploymentVersion {
        build_id: build_id.0.clone(),
        deployment_name: deployment_name.0.clone(),
    }
}

fn worker_deployment_version_key_from_edge(
    value: &WorkerDeploymentVersionKey,
) -> deployment_proto::WorkerDeploymentVersion {
    worker_deployment_version_from_parts(&value.deployment_name, &value.build_id)
}

fn find_version<'a>(
    deployment: &'a DeploymentView,
    key: &WorkerDeploymentVersionKey,
) -> Option<&'a VersionView> {
    deployment
        .versions
        .iter()
        .find(|version| version.build_id == key.build_id)
}

fn worker_deployment_version_string_from_key(value: &WorkerDeploymentVersionKey) -> String {
    version_string(&value.deployment_name, &value.build_id)
}

fn version_string(deployment_name: &DeploymentName, build_id: &DeploymentBuildId) -> String {
    // Deprecated v2 deployment response fields retain the v1.31 external version
    // string shape (`common/worker_versioning/worker_versioning.go:1057,1082 @
    // v1.31.0`), while the authoritative fields use `WorkerDeploymentVersion`.
    format!("{}.{}", deployment_name.0, build_id.0)
}

fn deployment_task_queue_type_to_domain(value: i32) -> DeploymentTaskQueueType {
    match enums::TaskQueueType::try_from(value).unwrap_or(enums::TaskQueueType::Unspecified) {
        enums::TaskQueueType::Workflow => DeploymentTaskQueueType::Workflow,
        enums::TaskQueueType::Activity => DeploymentTaskQueueType::Activity,
        enums::TaskQueueType::Nexus => DeploymentTaskQueueType::Nexus,
        enums::TaskQueueType::Unspecified => DeploymentTaskQueueType::Unspecified,
    }
}

fn deployment_task_queue_type_to_proto(value: DeploymentTaskQueueType) -> i32 {
    match value {
        DeploymentTaskQueueType::Unspecified => enums::TaskQueueType::Unspecified as i32,
        DeploymentTaskQueueType::Workflow => enums::TaskQueueType::Workflow as i32,
        DeploymentTaskQueueType::Activity => enums::TaskQueueType::Activity as i32,
        DeploymentTaskQueueType::Nexus => enums::TaskQueueType::Nexus as i32,
    }
}

fn routing_config_update_state_to_proto(value: RoutingConfigUpdateState) -> i32 {
    match value {
        RoutingConfigUpdateState::Unspecified => {
            enums::RoutingConfigUpdateState::Unspecified as i32
        }
        RoutingConfigUpdateState::InProgress => enums::RoutingConfigUpdateState::InProgress as i32,
        RoutingConfigUpdateState::Completed => enums::RoutingConfigUpdateState::Completed as i32,
    }
}

fn worker_deployment_version_status_to_proto(value: WorkerDeploymentVersionStatus) -> i32 {
    match value {
        WorkerDeploymentVersionStatus::Unspecified => {
            enums::WorkerDeploymentVersionStatus::Unspecified as i32
        }
        WorkerDeploymentVersionStatus::Inactive => {
            enums::WorkerDeploymentVersionStatus::Inactive as i32
        }
        WorkerDeploymentVersionStatus::Current => {
            enums::WorkerDeploymentVersionStatus::Current as i32
        }
        WorkerDeploymentVersionStatus::Ramping => {
            enums::WorkerDeploymentVersionStatus::Ramping as i32
        }
        WorkerDeploymentVersionStatus::Draining => {
            enums::WorkerDeploymentVersionStatus::Draining as i32
        }
        WorkerDeploymentVersionStatus::Drained => {
            enums::WorkerDeploymentVersionStatus::Drained as i32
        }
        WorkerDeploymentVersionStatus::Created => {
            enums::WorkerDeploymentVersionStatus::Created as i32
        }
    }
}

fn version_drainage_status_to_proto(value: VersionDrainageStatus) -> i32 {
    match value {
        VersionDrainageStatus::Unspecified => enums::VersionDrainageStatus::Unspecified as i32,
        VersionDrainageStatus::Draining => enums::VersionDrainageStatus::Draining as i32,
        VersionDrainageStatus::Drained => enums::VersionDrainageStatus::Drained as i32,
    }
}

#[allow(deprecated)]
pub fn respond_completed_request_to_edge(
    req: workflowservice::RespondWorkflowTaskCompletedRequest,
) -> Result<RespondWorkflowTaskCompletedRequest, ProtoConversionError> {
    // Index messages by ID so ProtocolMessage commands can
    // look up their corresponding message body. The original request order
    // is preserved separately: messages NOT referenced by any command are
    // processed in request order (`msgs.TakeRemaining`,
    // workflow_task_completed_handler.go @ v1.31.0).
    let message_order: Vec<String> = req.messages.iter().map(|m| m.id.clone()).collect();
    let mut messages_by_id: std::collections::HashMap<String, _> = req
        .messages
        .into_iter()
        .map(|m| (m.id.clone(), m))
        .collect();

    // The completing worker's namespace: child commands that omit their own
    // namespace inherit it (workflow_task_completed_handler.go @ v1.31.0).
    let request_namespace = req.namespace.clone();

    let mut commands = Vec::new();
    for cmd in req.commands {
        match proto_command_to_workflow_command(cmd, &request_namespace) {
            Ok(WorkflowCommand::ProtocolMessage { message_id, .. }) => {
                // Resolve the body from the messages index. A command
                // referencing an ABSENT message id cannot be resolved here, so
                // it is forwarded as an `UnresolvedMessage` sentinel: the kernel
                // fails the workflow task with
                // `BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE` and the runtime aborts
                // the Sent update waiters, exactly as for every other bad update
                // message (v1.31.0's 'ProtocolMessageCommand referenced absent
                // message ID', workflow_task_completed_handler.go @ v1.31.0;
                // spec speculative-wft Req 6.1). Routing it through the kernel
                // seam keeps the caller-visible WorkflowNotReady abort uniform
                // (`TestValidateWorkerMessages/command-reference-missed-message`).
                if let Some(msg) = messages_by_id.remove(&message_id) {
                    let body = msg
                        .body
                        .map(|body| body.encode_to_vec())
                        .unwrap_or_default();
                    commands.push(WorkflowCommand::ProtocolMessage {
                        message_id,
                        body: resolve_protocol_message_body(&body, msg.protocol_instance_id)?,
                    });
                } else {
                    commands.push(WorkflowCommand::ProtocolMessage {
                        message_id: message_id.clone(),
                        body: tokeira_kernel::UpdateProtocolBody::UnresolvedMessage { message_id },
                    });
                }
            }
            Ok(cmd) => commands.push(cmd),
            Err(e) => return Err(e),
        }
    }

    // Messages not referenced by any PROTOCOL_MESSAGE command are still
    // PROCESSED — the command is optional ordering sugar; v1.31.0 applies the
    // leftovers in request order after the commands (`msgs.TakeRemaining`,
    // workflow_task_completed_handler.go @ v1.31.0). The SDK routinely ships
    // acceptance/rejection/response messages with ZERO commands. tokeira's
    // kernel rejects commands after a close, and the corpus pins update
    // events BEFORE the close event of the same completion
    // (TestLastWorkflowTask_HasUpdateMessage: 5 UpdateAccepted,
    // 6 WorkflowExecutionCompleted), so the leftovers splice in ahead of the
    // first close command instead of strictly last.
    let mut leftover_commands = Vec::new();
    for id in &message_order {
        if let Some(msg) = messages_by_id.remove(id) {
            let body = msg
                .body
                .map(|body| body.encode_to_vec())
                .unwrap_or_default();
            leftover_commands.push(WorkflowCommand::ProtocolMessage {
                message_id: msg.id,
                body: resolve_protocol_message_body(&body, msg.protocol_instance_id)?,
            });
        }
    }
    let close_at = commands
        .iter()
        .position(|command| {
            matches!(
                command,
                WorkflowCommand::CompleteWorkflow { .. }
                    | WorkflowCommand::FailWorkflow { .. }
                    | WorkflowCommand::CancelWorkflow { .. }
                    | WorkflowCommand::ContinueAsNew { .. }
            )
        })
        .unwrap_or(commands.len());
    for (offset, command) in leftover_commands.into_iter().enumerate() {
        commands.insert(close_at + offset, command);
    }
    let remaining_messages: Vec<ProtocolMessageDto> = Vec::new();
    let deployment_version = worker_deployment_version_from_options(
        req.deployment_options.as_ref(),
        "RespondWorkflowTaskCompletedRequest.deployment_options",
    )?
    .or_else(|| worker_deployment_version_from_deprecated_deployment(req.deployment.as_ref()));
    let worker_deployment_name = worker_deployment_name_from_options(
        req.deployment_options.as_ref(),
        "RespondWorkflowTaskCompletedRequest.deployment_options",
    )?
    .or_else(|| worker_deployment_name_from_deprecated_deployment(req.deployment.as_ref()));
    let sticky = sticky_spec_from_attributes(req.sticky_attributes.as_ref())?;

    Ok(RespondWorkflowTaskCompletedRequest {
        namespace: request_namespace,
        task_token: req.task_token,
        identity: req.identity,
        sdk_metadata: req.sdk_metadata.map(|metadata| metadata.encode_to_vec()),
        metering_metadata: req
            .metering_metadata
            .map(|metadata| metadata.encode_to_vec()),
        worker_version: {
            let stamp = req.worker_version_stamp.map(|stamp| WorkerVersionStamp {
                build_id: stamp.build_id,
                use_versioning: stamp.use_versioning,
            });
            if stamp.is_some() || !req.binary_checksum.is_empty() {
                Some(WorkflowTaskWorkerVersion {
                    binary_checksum: req.binary_checksum,
                    stamp,
                })
            } else {
                None
            }
        },
        versioning_behavior: versioning_behavior_from_proto(req.versioning_behavior)?,
        deployment_version,
        worker_deployment_name,
        sticky,
        resource_id: req.resource_id,
        worker_instance_key: req.worker_instance_key,
        worker_control_task_queue: req.worker_control_task_queue,
        client_discards_speculative_with_events: req
            .capabilities
            .is_some_and(|capabilities| capabilities.discard_speculative_workflow_task_with_events),
        commands,
        force_create_new_workflow_task: req.force_create_new_workflow_task,
        return_new_workflow_task: req.return_new_workflow_task,
        query_results: req
            .query_results
            .into_iter()
            .map(|(id, result)| {
                let dto = match enums::QueryResultType::try_from(result.result_type)
                    .unwrap_or(enums::QueryResultType::Failed)
                {
                    enums::QueryResultType::Answered => QueryResultDto::Answered {
                        result: result
                            .answer
                            .as_ref()
                            .map(payloads_to_domain)
                            .unwrap_or_default(),
                    },
                    enums::QueryResultType::Failed | enums::QueryResultType::Unspecified => {
                        QueryResultDto::Failed {
                            error_message: result.error_message,
                            failure: result
                                .failure
                                .as_ref()
                                .map(tokeira_proto::conversions::common::failure_to_payload),
                        }
                    }
                };
                Ok((id, dto))
            })
            .collect::<Result<_, ProtoConversionError>>()?,
        messages: remaining_messages,
    })
}

/// Decode the `Any`-typed body of a protocol message into a kernel
/// `UpdateProtocolBody` variant.
///
/// The Temporal update protocol uses three message types — `Acceptance`,
/// `Response`, and `Rejection` — each wrapped in a `prost_types::Any`.
/// We match on the `type_url` suffix to determine which variant to decode.
/// `Response` bodies carry an `Outcome` that can be either `Success` or
/// `Failure`; a `Failure` outcome is a COMPLETED update whose handler failed
/// post-acceptance — NOT a rejection (rejections arrive as `Rejection`
/// messages; spec speculative-wft Req 7.2,
/// mutable_state_impl.go:5288-5378 @ v1.31.0).
fn resolve_protocol_message_body(
    body_bytes: &[u8],
    protocol_instance_id: String,
) -> Result<tokeira_kernel::UpdateProtocolBody, ProtoConversionError> {
    use prost::Message as _;
    let any = prost_types::Any::decode(body_bytes)
        .map_err(|_| ProtoConversionError::MissingField("ProtocolMessage body decode failed"))?;
    match any.type_url.as_str() {
        url if url.ends_with("update.v1.Acceptance") => {
            let acceptance = tokeira_proto::public::temporal::api::update::v1::Acceptance::decode(
                any.value.as_slice(),
            )
            .map_err(|_| {
                ProtoConversionError::MissingField("update.v1.Acceptance decode failed")
            })?;
            // The embedded `accepted_request` is the worker's echo of the
            // original request — v1.31.0's resurrect source (`TryResurrect`,
            // update/registry.go:238-281). Decode its name/args as the
            // FALLBACK; the edge core's registry hydration
            // (workflow_service.rs) remains the primary source and
            // overwrites these when the update is still registered.
            let (update_name, input) = acceptance
                .accepted_request
                .and_then(|request| request.input)
                .map(|input| {
                    (
                        input.name,
                        input
                            .args
                            .as_ref()
                            .map(payloads_to_domain)
                            .unwrap_or_default(),
                    )
                })
                .unwrap_or_default();
            Ok(tokeira_kernel::UpdateProtocolBody::Accepted {
                update_id: protocol_instance_id,
                update_name,
                input,
                // Worker-provided sequencing anchor, validated and recorded
                // by the kernel (spec speculative-wft K6 + owner amendment
                // F5; `validateAcceptanceMsg`, update/validation.go:62-68
                // @ v1.31.0).
                sequencing_event_id: acceptance.accepted_request_sequencing_event_id,
            })
        }
        url if url.ends_with("update.v1.Response") => {
            let response = tokeira_proto::public::temporal::api::update::v1::Response::decode(
                any.value.as_slice(),
            )
            .map_err(|_| ProtoConversionError::MissingField("update.v1.Response decode failed"))?;
            match response.outcome.and_then(|o| o.value) {
                Some(
                    tokeira_proto::public::temporal::api::update::v1::outcome::Value::Success(
                        payloads,
                    ),
                ) => Ok(tokeira_kernel::UpdateProtocolBody::Completed {
                    update_id: protocol_instance_id,
                    result: payloads_to_domain(&payloads),
                    failure: None,
                }),
                Some(
                    tokeira_proto::public::temporal::api::update::v1::outcome::Value::Failure(
                        failure,
                    ),
                ) => Ok(tokeira_kernel::UpdateProtocolBody::Completed {
                    update_id: protocol_instance_id,
                    result: Payloads::default(),
                    failure: Some(failure_to_payload(&failure)),
                }),
                None => Err(ProtoConversionError::MissingField(
                    "update.v1.Response missing outcome",
                )),
            }
        }
        url if url.ends_with("update.v1.Rejection") => {
            let rejection = tokeira_proto::public::temporal::api::update::v1::Rejection::decode(
                any.value.as_slice(),
            )
            .map_err(|_| ProtoConversionError::MissingField("update.v1.Rejection decode failed"))?;
            Ok(tokeira_kernel::UpdateProtocolBody::Rejected {
                update_id: protocol_instance_id,
                failure: rejection
                    .failure
                    .as_ref()
                    .map(failure_to_payload)
                    .unwrap_or_else(|| {
                        failure_to_payload(&failure_proto::Failure {
                            message: "update rejected".to_string(),
                            ..Default::default()
                        })
                    }),
            })
        }
        _ => Err(ProtoConversionError::MissingField(
            "unknown protocol message type_url",
        )),
    }
}

pub fn describe_request_to_edge(
    req: workflowservice::DescribeWorkflowExecutionRequest,
) -> Result<DescribeWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DescribeWorkflowExecutionRequest.execution",
        ))?;
    let run_id = if execution.run_id.is_empty() {
        None
    } else {
        parse_run_id(&execution.run_id)?;
        Some(execution.run_id.clone())
    };
    Ok(DescribeWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id,
    })
}

pub fn list_request_to_edge(
    req: workflowservice::ListWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(ListWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: if req.next_page_token.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&req.next_page_token).into_owned())
        },
    })
}

/// Translates the deprecated open-visibility request into the modern query DTO.
pub fn list_open_request_to_edge(
    req: workflowservice::ListOpenWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    // v1.31.0 implements legacy visibility by constructing a modern query:
    // open lists force `ExecutionStatus = Running`, while closed lists force
    // `ExecutionStatus != Running` (`service/frontend/workflow_handler.go:2492`
    // and `:2593 @ v1.31.0`). The edge mirrors that wrapper instead of adding
    // a separate projection API.
    legacy_list_request_to_edge(
        req.namespace,
        req.maximum_page_size,
        req.next_page_token,
        "ExecutionStatus = 'Running'",
        legacy_start_time_query(req.start_time_filter.as_ref(), "StartTime")?,
        legacy_open_filter_query(req.filters.as_ref()),
    )
}

/// Translates the deprecated closed-visibility request into the modern query DTO.
pub fn list_closed_request_to_edge(
    req: workflowservice::ListClosedWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    legacy_list_request_to_edge(
        req.namespace,
        req.maximum_page_size,
        req.next_page_token,
        "ExecutionStatus != 'Running'",
        legacy_start_time_query(req.start_time_filter.as_ref(), "CloseTime")?,
        legacy_closed_filter_query(req.filters.as_ref())?,
    )
}

/// Translates archived listing as a compatibility wrapper over modern visibility.
pub fn list_archived_request_to_edge(
    req: workflowservice::ListArchivedWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(ListWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: bytes_page_token(req.next_page_token),
    })
}

/// Translates deprecated scan listing as a compatibility wrapper over modern visibility.
pub fn scan_request_to_edge(
    req: workflowservice::ScanWorkflowExecutionsRequest,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    Ok(ListWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: bytes_page_token(req.next_page_token),
    })
}

fn legacy_list_request_to_edge(
    namespace: String,
    page_size: i32,
    next_page_token: Vec<u8>,
    status_query: &str,
    time_query: Option<String>,
    filter_query: Option<String>,
) -> Result<ListWorkflowExecutionsRequest, ProtoConversionError> {
    let mut clauses = vec![status_query.to_string()];
    clauses.extend(time_query);
    clauses.extend(filter_query);
    Ok(ListWorkflowExecutionsRequest {
        namespace,
        query: Some(clauses.join(" AND ")),
        page_size: legacy_page_size(page_size),
        next_page_token: bytes_page_token(next_page_token),
    })
}

fn legacy_page_size(page_size: i32) -> usize {
    if page_size <= 0 {
        tokeira_projection::MAX_PAGE_SIZE
    } else {
        (page_size as usize).min(tokeira_projection::MAX_PAGE_SIZE)
    }
}

fn bytes_page_token(value: Vec<u8>) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&value).into_owned())
    }
}

fn legacy_start_time_query(
    filter: Option<&tokeira_proto::public::temporal::api::filter::v1::StartTimeFilter>,
    field: &str,
) -> Result<Option<String>, ProtoConversionError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let earliest = filter
        .earliest_time
        .as_ref()
        .map(proto_timestamp_to_offset)
        .transpose()?;
    let latest = filter
        .latest_time
        .as_ref()
        .map(proto_timestamp_to_offset)
        .transpose()?;
    match (earliest, latest) {
        (Some(earliest), Some(latest)) if earliest > latest => {
            Err(ProtoConversionError::InvalidArgument(
                "EarliestTime is greater than LatestTime.".to_string(),
            ))
        }
        (Some(earliest), Some(latest)) if earliest == latest => Ok(Some(format!(
            "{field} = '{}'",
            format_visibility_time(earliest)?
        ))),
        (Some(earliest), Some(latest)) => Ok(Some(format!(
            "{field} BETWEEN '{}' AND '{}'",
            format_visibility_time(earliest)?,
            format_visibility_time(latest)?
        ))),
        (Some(earliest), None) => Ok(Some(format!(
            "{field} >= '{}'",
            format_visibility_time(earliest)?
        ))),
        (None, Some(latest)) => Ok(Some(format!(
            "{field} <= '{}'",
            format_visibility_time(latest)?
        ))),
        (None, None) => Ok(None),
    }
}

fn legacy_open_filter_query(
    filter: Option<&workflowservice::list_open_workflow_executions_request::Filters>,
) -> Option<String> {
    use workflowservice::list_open_workflow_executions_request::Filters;
    match filter {
        Some(Filters::ExecutionFilter(filter)) => Some(format!(
            "WorkflowId = '{}'",
            quote_visibility_value(&filter.workflow_id)
        )),
        Some(Filters::TypeFilter(filter)) => Some(format!(
            "WorkflowType = '{}'",
            quote_visibility_value(&filter.name)
        )),
        None => None,
    }
}

fn legacy_closed_filter_query(
    filter: Option<&workflowservice::list_closed_workflow_executions_request::Filters>,
) -> Result<Option<String>, ProtoConversionError> {
    use workflowservice::list_closed_workflow_executions_request::Filters;
    match filter {
        Some(Filters::ExecutionFilter(filter)) => Ok(Some(format!(
            "WorkflowId = '{}'",
            quote_visibility_value(&filter.workflow_id)
        ))),
        Some(Filters::TypeFilter(filter)) => Ok(Some(format!(
            "WorkflowType = '{}'",
            quote_visibility_value(&filter.name)
        ))),
        Some(Filters::StatusFilter(filter)) => {
            let status = enums::WorkflowExecutionStatus::try_from(filter.status)
                .unwrap_or(enums::WorkflowExecutionStatus::Unspecified);
            let Some(status_name) = visibility_status_name(status) else {
                return Err(ProtoConversionError::InvalidArgument(
                    "StatusFilter must be specified and must be not Running.".to_string(),
                ));
            };
            Ok(Some(format!("ExecutionStatus = '{status_name}'")))
        }
        None => Ok(None),
    }
}

fn visibility_status_name(status: enums::WorkflowExecutionStatus) -> Option<&'static str> {
    use enums::WorkflowExecutionStatus as Status;
    match status {
        Status::Completed => Some("Completed"),
        Status::Failed => Some("Failed"),
        Status::Canceled => Some("Cancelled"),
        Status::Terminated => Some("Terminated"),
        Status::ContinuedAsNew => Some("ContinuedAsNew"),
        Status::TimedOut => Some("TimedOut"),
        Status::Paused => Some("Paused"),
        Status::Unspecified | Status::Running => None,
    }
}

fn proto_timestamp_to_offset(
    value: &prost_types::Timestamp,
) -> Result<OffsetDateTime, ProtoConversionError> {
    OffsetDateTime::from_unix_timestamp(value.seconds)
        .and_then(|time| time.replace_nanosecond(value.nanos as u32))
        .map_err(|err| ProtoConversionError::InvalidTimestamp(err.to_string()))
}

fn format_visibility_time(value: OffsetDateTime) -> Result<String, ProtoConversionError> {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|err| ProtoConversionError::InvalidTimestamp(err.to_string()))
}

fn quote_visibility_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

pub fn count_request_to_edge(
    req: workflowservice::CountWorkflowExecutionsRequest,
) -> Result<CountWorkflowExecutionsRequest, ProtoConversionError> {
    let (query, group_by) = split_count_group_by(&req.query)?;
    Ok(CountWorkflowExecutionsRequest {
        namespace: req.namespace,
        query,
        group_by,
    })
}

fn split_count_group_by(
    query: &str,
) -> Result<(Option<String>, Option<String>), ProtoConversionError> {
    let upper = query.to_ascii_uppercase();
    let group_start = upper
        .find(" GROUP BY ")
        .map(|index| (index, " GROUP BY ".len()))
        .or_else(|| {
            upper
                .starts_with("GROUP BY ")
                .then_some((0, "GROUP BY ".len()))
        });
    let Some((index, keyword_len)) = group_start else {
        return Ok((non_empty(query.to_string()), None));
    };
    let field = query[index + keyword_len..].trim();
    if field.contains(',') {
        return Err(ProtoConversionError::InvalidArgument(
            "'GROUP BY' clause supports only a single field".to_string(),
        ));
    }
    if field != "ExecutionStatus" {
        return Err(ProtoConversionError::InvalidArgument(
            "'GROUP BY' clause is only supported for ExecutionStatus".to_string(),
        ));
    }
    Ok((
        non_empty(query[..index].trim().to_string()),
        Some(field.to_string()),
    ))
}

pub fn list_activity_request_to_edge(
    req: workflowservice::ListActivityExecutionsRequest,
) -> Result<ListActivityExecutionsRequest, ProtoConversionError> {
    Ok(ListActivityExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        page_size: req.page_size.max(0) as usize,
        next_page_token: if req.next_page_token.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&req.next_page_token).into_owned())
        },
    })
}

pub fn count_activity_request_to_edge(
    req: workflowservice::CountActivityExecutionsRequest,
) -> Result<CountActivityExecutionsRequest, ProtoConversionError> {
    Ok(CountActivityExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        group_by: None,
    })
}

pub fn register_namespace_request_to_edge(
    req: workflowservice::RegisterNamespaceRequest,
) -> Result<EdgeRegisterNamespaceRequest, ProtoConversionError> {
    // Carry the client's requested retention through to the namespace model; a
    // missing or non-positive value falls back to the scoped-model default so the
    // field is always present (Temporal clients/UI require it).
    let retention = req
        .workflow_execution_retention_period
        .map(|d| {
            time::Duration::seconds(d.seconds) + time::Duration::nanoseconds(i64::from(d.nanos))
        })
        .filter(|d| d.is_positive())
        .unwrap_or_else(|| {
            time::Duration::seconds(crate::namespace_cache::DEFAULT_NAMESPACE_RETENTION_SECONDS)
        });
    Ok(EdgeRegisterNamespaceRequest {
        namespace: req.namespace,
        retention,
    })
}

/// Translate `UpdateNamespaceRequest` proto into the edge DTO.
///
/// `update_info` is optional on the wire; when absent the request changes
/// nothing, which maps to `NamespaceStateUpdate::Unspecified` with no
/// description change (v1.31.0 `validateStateUpdate` treats `UpdateInfo == nil`
/// as "no change"). An empty `description` string is the proto default and is
/// treated as "leave unchanged" rather than "set to empty".
pub fn update_namespace_request_to_edge(
    req: workflowservice::UpdateNamespaceRequest,
) -> Result<EdgeUpdateNamespaceRequest, ProtoConversionError> {
    let (state, description) = match req.update_info {
        Some(info) => {
            let state = match enums::NamespaceState::try_from(info.state).map_err(|_| {
                ProtoConversionError::MissingField("UpdateNamespaceRequest.update_info.state")
            })? {
                enums::NamespaceState::Unspecified => NamespaceStateUpdate::Unspecified,
                enums::NamespaceState::Registered => NamespaceStateUpdate::Registered,
                enums::NamespaceState::Deprecated => NamespaceStateUpdate::Deprecated,
                enums::NamespaceState::Deleted => NamespaceStateUpdate::Deleted,
            };
            (state, non_empty(info.description))
        }
        None => (NamespaceStateUpdate::Unspecified, None),
    };
    Ok(EdgeUpdateNamespaceRequest {
        namespace: req.namespace,
        state,
        description,
    })
}

/// Build the `UpdateNamespaceResponse` proto from the updated namespace.
///
/// v1.31.0 echoes the (possibly-updated) namespace info, config, and
/// replication config (`service/frontend/namespace_handler.go @ v1.31.0`,
/// response construction). We reuse [`namespace_to_proto`] to render the same
/// `NamespaceInfo`/`NamespaceConfig`/`NamespaceReplicationConfig` shapes used by
/// `DescribeNamespace`, then project them onto the update response.
pub fn update_namespace_response_to_proto(
    namespace: NamespaceDescription,
    standalone_activities: bool,
) -> workflowservice::UpdateNamespaceResponse {
    let described = namespace_to_proto(namespace, standalone_activities);
    workflowservice::UpdateNamespaceResponse {
        namespace_info: described.namespace_info,
        config: described.config,
        replication_config: described.replication_config,
        failover_version: described.failover_version,
        is_global_namespace: described.is_global_namespace,
    }
}

pub fn start_response_to_proto(
    resp: StartWorkflowExecutionResponse,
    namespace: String,
    workflow_id: String,
) -> workflowservice::StartWorkflowExecutionResponse {
    use proto_common::link::{
        Variant, WorkflowEvent,
        workflow_event::{EventReference, Reference, RequestIdReference},
    };
    let run_id = resp.run_id.0.to_string();
    // v1.31.0 returns a self-referential response link. For an OnConflictOptions attach that
    // recorded a WorkflowExecutionOptionsUpdated event, it is a RequestIdRef to that event keyed
    // by the attaching request id (generateRequestIdRefLink, startworkflow/api.go:833). Otherwise
    // — a fresh start, a dedup that maps to the original start request, or a plain UseExisting
    // attach — it is an EventRef to event 1 / WORKFLOW_EXECUTION_STARTED
    // (generateStartedEventRefLink, startworkflow/api.go:811).
    let reference = match resp.attached_request_id {
        Some(request_id) => Reference::RequestIdRef(RequestIdReference {
            request_id,
            event_type: tokeira_proto::enums::EventType::WorkflowExecutionOptionsUpdated as i32,
        }),
        None => Reference::EventRef(EventReference {
            event_id: 1,
            event_type: tokeira_proto::enums::EventType::WorkflowExecutionStarted as i32,
        }),
    };
    let link = proto_common::Link {
        variant: Some(Variant::WorkflowEvent(WorkflowEvent {
            namespace,
            workflow_id,
            run_id: run_id.clone(),
            reference: Some(reference),
        })),
    };
    workflowservice::StartWorkflowExecutionResponse {
        run_id,
        started: resp.started,
        status: execution_status_to_proto(resp.status),
        link: Some(link),
        eager_workflow_task: resp.eager_workflow_task.map(poll_response_to_proto),
        ..Default::default()
    }
}

pub fn signal_response_to_proto(
    _resp: SignalWorkflowExecutionResponse,
) -> workflowservice::SignalWorkflowExecutionResponse {
    workflowservice::SignalWorkflowExecutionResponse { link: None }
}

/// Build the proto poll response from the edge DTO.
pub fn poll_response_to_proto(
    resp: PollWorkflowTaskQueueResponse,
) -> workflowservice::PollWorkflowTaskQueueResponse {
    // The wire run id is the run's user-visible RunId — never the internal
    // storage RunKey (derived via dsql_spread_uuid, so the two differ). SDK
    // pollers echo this execution into follow-up RPCs (GetWorkflowExecutionHistory,
    // RespondActivityTaskCompletedById), which resolve by real run id.
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.payload.workflow_id),
        resp.payload.run_id,
    ));

    let history_bytes =
        crate::translate::history_serializer::serialize_history(&resp.payload.history);
    let history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    // Extract workflow_type from the first history event (WorkflowExecutionStarted)
    let workflow_type_name = resp
        .payload
        .history
        .first()
        .and_then(|ev| match &ev.kind {
            tokeira_kernel::event::HistoryEventKind::WorkflowExecutionStarted {
                workflow_type,
                ..
            }
            | tokeira_kernel::event::HistoryEventKind::WorkflowExecutionStartedV2 {
                workflow_type,
                ..
            } => Some(workflow_type.0.clone()),
            _ => None,
        })
        .unwrap_or_default();

    workflowservice::PollWorkflowTaskQueueResponse {
        task_token: resp.task_token,
        workflow_execution,
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: workflow_type_name,
        }),
        previous_started_event_id: resp.previous_started_event_id,
        started_event_id: resp.started_event_id,
        attempt: resp.attempt as i32,
        history,
        query: resp
            .query
            .map(|query| tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                query_type: query.query_type,
                query_args: Some(payloads_from_domain(&query.query_args)),
                header: None,
            }),
        scheduled_time: resp.scheduled_time.map(to_proto_timestamp),
        started_time: resp.started_time.map(to_proto_timestamp),
        workflow_execution_task_queue: Some(
            tokeira_proto::conversions::common::task_queue_from_domain(
                &tokeira_types::TaskQueueName(resp.payload.task_queue),
            ),
        ),
        queries: resp
            .queries
            .into_iter()
            .map(|(id, query)| {
                (
                    id,
                    tokeira_proto::public::temporal::api::query::v1::WorkflowQuery {
                        query_type: query.query_type,
                        query_args: Some(payloads_from_domain(&query.query_args)),
                        header: None,
                    },
                )
            })
            .collect(),
        messages: resp
            .messages
            .into_iter()
            .map(|message| {
                // The body is already an encoded prost_types::Any.
                // Decode it back to set on the proto Message.
                let body = match prost_types::Any::decode(message.body.as_slice()) {
                    Ok(any) => Some(any),
                    Err(e) => {
                        tracing::warn!("Failed to decode protocol message body: {e}");
                        None
                    }
                };
                Ok(tokeira_proto::public::temporal::api::protocol::v1::Message {
                    id: message.id,
                    protocol_instance_id: message.protocol_instance_id,
                    body,
                    sequencing_id: message.sequencing_event_id.map(
                        tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::EventId,
                    ),
                })
            })
            .collect::<Result<Vec<_>, ProtoConversionError>>()
            .unwrap_or_default(),
        poller_scaling_decision: resp.poller_scaling_decision.map(|delta| {
            taskqueue_proto::PollerScalingDecision {
                poll_request_delta_suggestion: delta,
            }
        }),
        ..Default::default()
    }
}

/// Build the proto WFT completion response.
///
/// The `workflow_task` field carries an optional inline poll response for
/// "eager return": when the SDK sets `return_new_workflow_task = true` and
/// the edge has a query-only WFT ready, it piggybacks the next task on the
/// completion response to avoid an extra poll round-trip.
pub fn completed_response_to_proto(
    resp: RespondWorkflowTaskCompletedResponse,
) -> workflowservice::RespondWorkflowTaskCompletedResponse {
    workflowservice::RespondWorkflowTaskCompletedResponse {
        // Speculative DROP rewind: non-zero only when the completed task
        // persisted nothing (respondworkflowtaskcompleted/api.go:770 @
        // v1.31.0; spec speculative-wft E2).
        reset_history_event_id: resp.reset_history_event_id,
        workflow_task: resp.workflow_task.map(poll_response_to_proto),
        activity_tasks: resp
            .activity_tasks
            .into_iter()
            .map(poll_activity_response_to_proto)
            .collect(),
        ..Default::default()
    }
}

pub fn describe_response_to_proto(
    resp: WorkflowExecutionDescription,
) -> workflowservice::DescribeWorkflowExecutionResponse {
    let execution_config = Some(execution_config_to_proto(&resp.execution_config));
    let pending_activities = resp
        .pending_activities
        .iter()
        .map(pending_activity_to_proto)
        .collect();
    let pending_children = resp
        .pending_children
        .iter()
        .map(pending_child_to_proto)
        .collect();
    let pending_workflow_task = resp
        .pending_workflow_task
        .as_ref()
        .map(pending_wft_to_proto);
    let pending_nexus_operations = resp
        .pending_nexus_operations
        .iter()
        .map(pending_nexus_operation_to_proto)
        .collect();
    let callbacks = resp
        .callbacks
        .iter()
        .map(workflow_callback_info_to_proto)
        .collect();
    let workflow_execution_info = Some(workflow_execution_info_from_description(&resp));
    let workflow_extended_info = Some(workflow_extended_info_to_proto(&resp));

    workflowservice::DescribeWorkflowExecutionResponse {
        execution_config,
        workflow_execution_info,
        pending_activities,
        pending_children,
        pending_workflow_task,
        callbacks,
        pending_nexus_operations,
        workflow_extended_info,
    }
}

fn workflow_callback_info_to_proto(callback: &KernelCompletionCallback) -> workflow::CallbackInfo {
    workflow::CallbackInfo {
        callback: Some(kernel_callback_to_proto(callback)),
        trigger: Some(workflow_callback_trigger_to_proto(&callback.trigger)),
        registration_time: callback.registration_time.map(to_proto_timestamp),
        state: kernel_callback_state_to_proto(&callback.state) as i32,
        attempt: callback.attempt as i32,
        last_attempt_complete_time: None,
        last_attempt_failure: callback
            .last_attempt_failure
            .as_ref()
            .map(payload_to_failure),
        // When the callback is backing off, the kernel records when its next delivery is
        // due (`CompletionCallback.next_attempt_at`); surface it as v1.31.0 does for a
        // `BackingOff` callback (`CallbackInfo.next_attempt_schedule_time`).
        next_attempt_schedule_time: callback.next_attempt_at.map(to_proto_timestamp),
        blocked_reason: String::new(),
    }
}

fn kernel_callback_to_proto(callback: &KernelCompletionCallback) -> proto_common::Callback {
    let variant = match &callback.spec {
        KernelCallbackSpec::Nexus { url, header } => Some(proto_common::callback::Variant::Nexus(
            proto_common::callback::Nexus {
                url: url.clone(),
                header: header.clone(),
            },
        )),
    };

    proto_common::Callback {
        variant,
        links: callback.links.iter().map(kernel_link_to_proto).collect(),
    }
}

fn workflow_callback_trigger_to_proto(
    trigger: &KernelCallbackTrigger,
) -> workflow::callback_info::Trigger {
    match trigger {
        KernelCallbackTrigger::WorkflowClosed => workflow::callback_info::Trigger {
            variant: Some(workflow::callback_info::trigger::Variant::WorkflowClosed(
                workflow::callback_info::WorkflowClosed {},
            )),
        },
    }
}

fn kernel_callback_state_to_proto(state: &KernelCallbackState) -> enums::CallbackState {
    match state {
        KernelCallbackState::Standby => enums::CallbackState::Standby,
        KernelCallbackState::Scheduled => enums::CallbackState::Scheduled,
        KernelCallbackState::BackingOff => enums::CallbackState::BackingOff,
        KernelCallbackState::Failed => enums::CallbackState::Failed,
        KernelCallbackState::Succeeded => enums::CallbackState::Succeeded,
        KernelCallbackState::Blocked => enums::CallbackState::Blocked,
    }
}

fn kernel_link_to_proto(link: &KernelLink) -> proto_common::Link {
    use proto_common::link::Variant;
    match link {
        KernelLink::WorkflowEvent {
            namespace,
            workflow_id,
            run_id,
            reference,
        } => proto_common::Link {
            variant: Some(Variant::WorkflowEvent(proto_common::link::WorkflowEvent {
                namespace: namespace.clone(),
                workflow_id: workflow_id.clone(),
                run_id: run_id.clone(),
                reference: reference.as_ref().map(kernel_link_reference_to_proto),
            })),
        },
        KernelLink::BatchJob { job_id } => proto_common::Link {
            variant: Some(Variant::BatchJob(proto_common::link::BatchJob {
                job_id: job_id.clone(),
            })),
        },
        KernelLink::Activity {
            namespace,
            activity_id,
            run_id,
        } => proto_common::Link {
            variant: Some(Variant::Activity(proto_common::link::Activity {
                namespace: namespace.clone(),
                activity_id: activity_id.clone(),
                run_id: run_id.clone(),
            })),
        },
        KernelLink::NexusOperation {
            namespace,
            operation_id,
            run_id,
        } => proto_common::Link {
            variant: Some(Variant::NexusOperation(
                proto_common::link::NexusOperation {
                    namespace: namespace.clone(),
                    operation_id: operation_id.clone(),
                    run_id: run_id.clone(),
                },
            )),
        },
    }
}

fn kernel_link_reference_to_proto(
    reference: &KernelLinkWorkflowEventReference,
) -> proto_common::link::workflow_event::Reference {
    use proto_common::link::workflow_event::{EventReference, Reference, RequestIdReference};
    match reference {
        KernelLinkWorkflowEventReference::Event {
            event_id,
            event_type,
        } => Reference::EventRef(EventReference {
            event_id: *event_id,
            event_type: *event_type,
        }),
        KernelLinkWorkflowEventReference::RequestId {
            request_id,
            event_type,
        } => Reference::RequestIdRef(RequestIdReference {
            request_id: request_id.clone(),
            event_type: *event_type,
        }),
    }
}

fn execution_config_to_proto(
    config: &crate::translate::ExecutionConfigDescription,
) -> workflow::WorkflowExecutionConfig {
    workflow::WorkflowExecutionConfig {
        task_queue: Some(task_queue_from_domain(&TaskQueueName(
            config.task_queue.clone(),
        ))),
        workflow_execution_timeout: config.workflow_execution_timeout.map(to_proto_duration),
        workflow_run_timeout: config.workflow_run_timeout.map(to_proto_duration),
        default_workflow_task_timeout: Some(to_proto_duration(
            config.default_workflow_task_timeout,
        )),
        user_metadata: config
            .user_metadata
            .as_ref()
            .map(|metadata| sdk_proto::UserMetadata {
                summary: metadata.summary.as_ref().map(payload_from_domain),
                details: metadata.details.as_ref().map(payload_from_domain),
            }),
    }
}

fn pending_activity_to_proto(
    act: &crate::translate::PendingActivityDescription,
) -> workflow::PendingActivityInfo {
    workflow::PendingActivityInfo {
        activity_id: act.activity_id.clone(),
        activity_type: Some(proto_common::ActivityType {
            name: act.activity_type.clone(),
        }),
        state: if act.is_started {
            enums::PendingActivityState::Started as i32
        } else {
            enums::PendingActivityState::Scheduled as i32
        },
        attempt: act.attempt as i32,
        maximum_attempts: act.maximum_attempts as i32,
        scheduled_time: Some(to_proto_timestamp(act.scheduled_at)),
        last_started_time: act.started_at.map(to_proto_timestamp),
        last_failure: act.last_failure.as_ref().map(payload_to_failure),
        // Present only once a heartbeat has been recorded
        // (`GetPendingActivityInfo`, activity.go:147-150 @ v1.31.0).
        heartbeat_details: act.heartbeat_details.as_ref().map(payloads_from_domain),
        last_worker_identity: act.last_worker_identity.clone(),
        paused: act.paused,
        pause_info: act.pause_info.as_ref().map(|info| {
            workflow::pending_activity_info::PauseInfo {
                pause_time: Some(to_proto_timestamp(info.paused_time)),
                paused_by: Some(
                    workflow::pending_activity_info::pause_info::PausedBy::Manual(
                        workflow::pending_activity_info::pause_info::Manual {
                            identity: info.identity.clone(),
                            reason: info.reason.clone(),
                        },
                    ),
                ),
            }
        }),
        ..Default::default()
    }
}

fn pending_child_to_proto(
    child: &crate::translate::PendingChildDescription,
) -> workflow::PendingChildExecutionInfo {
    workflow::PendingChildExecutionInfo {
        workflow_id: child.workflow_id.clone(),
        run_id: child.run_id.clone().unwrap_or_default(),
        workflow_type_name: child.workflow_type.clone(),
        initiated_id: child.initiated_event_id,
        parent_close_policy: parent_close_policy_from_domain(child.parent_close_policy),
    }
}

fn pending_wft_to_proto(
    wft: &crate::translate::PendingWorkflowTaskDescription,
) -> workflow::PendingWorkflowTaskInfo {
    workflow::PendingWorkflowTaskInfo {
        state: if wft.is_started {
            enums::PendingWorkflowTaskState::Started as i32
        } else {
            enums::PendingWorkflowTaskState::Scheduled as i32
        },
        scheduled_time: Some(to_proto_timestamp(wft.scheduled_at)),
        started_time: wft.started_at.map(to_proto_timestamp),
        attempt: wft.attempt as i32,
        ..Default::default()
    }
}

fn pending_nexus_operation_to_proto(
    op: &crate::translate::PendingNexusOperationDescription,
) -> workflow::PendingNexusOperationInfo {
    let operation_token = op.operation_token.clone().unwrap_or_default();
    // State precedence: a started async op is Started; otherwise a backing-off op
    // (next_attempt_at set) is BackingOff; otherwise Scheduled. `last_attempt_failure`
    // is surfaced only while backing off, matching v1.31.0's BACKING_OFF op data.
    let state = if op.started {
        enums::PendingNexusOperationState::Started
    } else if op.next_attempt_at.is_some() {
        enums::PendingNexusOperationState::BackingOff
    } else {
        enums::PendingNexusOperationState::Scheduled
    };
    workflow::PendingNexusOperationInfo {
        endpoint: op.endpoint.clone(),
        service: op.service.clone(),
        operation: op.operation.clone(),
        operation_id: operation_token.clone(),
        schedule_to_close_timeout: op.schedule_to_close_timeout.map(to_proto_duration),
        scheduled_time: Some(to_proto_timestamp(op.scheduled_time)),
        state: state as i32,
        attempt: op.attempt as i32,
        last_attempt_failure: op.last_attempt_failure.as_ref().map(payload_to_failure),
        next_attempt_schedule_time: op.next_attempt_at.map(to_proto_timestamp),
        scheduled_event_id: op.scheduled_event_id,
        schedule_to_start_timeout: op.schedule_to_start_timeout.map(to_proto_duration),
        start_to_close_timeout: op.start_to_close_timeout.map(to_proto_duration),
        operation_token,
        ..Default::default()
    }
}

pub fn list_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListWorkflowExecutionsResponse {
    workflowservice::ListWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

/// Renders modern visibility results into the deprecated open-list response.
pub fn list_open_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListOpenWorkflowExecutionsResponse {
    workflowservice::ListOpenWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

/// Renders modern visibility results into the deprecated closed-list response.
pub fn list_closed_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListClosedWorkflowExecutionsResponse {
    workflowservice::ListClosedWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

/// Renders modern visibility results into the archived-list compatibility response.
pub fn list_archived_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ListArchivedWorkflowExecutionsResponse {
    workflowservice::ListArchivedWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

/// Renders modern visibility results into the deprecated scan response.
pub fn scan_response_to_proto(
    resp: ListWorkflowExecutionsResponse,
) -> workflowservice::ScanWorkflowExecutionsResponse {
    workflowservice::ScanWorkflowExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(workflow_execution_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn count_response_to_proto(
    resp: CountWorkflowExecutionsResponse,
) -> workflowservice::CountWorkflowExecutionsResponse {
    use workflowservice::count_workflow_executions_response::AggregationGroup;
    workflowservice::CountWorkflowExecutionsResponse {
        count: resp.total_count,
        groups: resp
            .groups
            .into_iter()
            .map(|group| AggregationGroup {
                group_values: vec![search_attr_value_to_payload(
                    &tokeira_types::SearchAttrValue::Keyword(group.value),
                )],
                count: group.count,
            })
            .collect(),
    }
}

pub fn list_activity_response_to_proto(
    resp: ListActivityExecutionsResponse,
) -> workflowservice::ListActivityExecutionsResponse {
    workflowservice::ListActivityExecutionsResponse {
        executions: resp
            .executions
            .into_iter()
            .map(activity_execution_list_info_from_summary)
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn count_activity_response_to_proto(
    resp: CountActivityExecutionsResponse,
) -> workflowservice::CountActivityExecutionsResponse {
    use workflowservice::count_activity_executions_response::AggregationGroup;
    workflowservice::CountActivityExecutionsResponse {
        count: resp.total_count,
        groups: resp
            .groups
            .into_iter()
            .map(|group| AggregationGroup {
                group_values: vec![tokeira_proto::common::Payload {
                    data: group.value.into_bytes(),
                    ..Default::default()
                }],
                count: group.count,
            })
            .collect(),
    }
}

/// Maps the collapsed API `status_keyword` the index stores (23.7/24.3) to the
/// `ActivityExecutionStatus` wire enum. `Running` covers the non-terminal run states
/// (SCHEDULED/STARTED/CANCEL_REQUESTED) — the enum's own RUNNING semantics
/// (`enums/v1/activity.proto:ACTIVITY_EXECUTION_STATUS_RUNNING @ v1.31.0`); terminals
/// map 1:1.
fn activity_status_to_proto(status_keyword: &str) -> i32 {
    use enums::ActivityExecutionStatus as Proto;
    let proto = match status_keyword {
        "Running" => Proto::Running,
        "Completed" => Proto::Completed,
        "Failed" => Proto::Failed,
        "Canceled" => Proto::Canceled,
        "Terminated" => Proto::Terminated,
        "TimedOut" => Proto::TimedOut,
        _ => Proto::Unspecified,
    };
    proto as i32
}

fn activity_execution_list_info_from_summary(
    value: ActivityExecutionSummary,
) -> activity_proto::ActivityExecutionListInfo {
    // `execution_duration` is "close - schedule", populated only when closed (proto
    // field doc); derive it here since the activity persists no duration of its own.
    let execution_duration = match (value.schedule_time, value.close_time) {
        (Some(schedule), Some(close)) => Some(to_proto_duration(close - schedule)),
        _ => None,
    };
    activity_proto::ActivityExecutionListInfo {
        activity_id: value.activity_id,
        run_id: value.run_id.0.to_string(),
        activity_type: Some(proto_common::ActivityType {
            name: value.activity_type,
        }),
        schedule_time: value.schedule_time.map(to_proto_timestamp),
        close_time: value.close_time.map(to_proto_timestamp),
        status: activity_status_to_proto(&value.status_keyword),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
        task_queue: value.task_queue,
        state_transition_count: value.state_transition_count,
        state_size_bytes: value.state_size_bytes,
        execution_duration,
    }
}

pub fn cluster_info_to_proto(
    resp: crate::operator_service::ClusterInfo,
) -> workflowservice::GetClusterInfoResponse {
    workflowservice::GetClusterInfoResponse {
        supported_clients: resp.supported_clients,
        server_version: resp.version.clone(),
        cluster_id: resp.cluster_name.clone(),
        version_info: Some(version_proto::VersionInfo {
            current: Some(version_proto::ReleaseInfo {
                version: resp.version.clone(),
                release_time: None,
                notes: resp.notes.join("\n"),
            }),
            ..Default::default()
        }),
        cluster_name: resp.cluster_name,
        history_shard_count: resp.shard_count.max(1),
        persistence_store: "in-memory".to_string(),
        visibility_store: "in-memory".to_string(),
        initial_failover_version: 0,
        failover_version_increment: 0,
    }
}

pub fn system_info_to_proto(resp: SystemInfo) -> workflowservice::GetSystemInfoResponse {
    workflowservice::GetSystemInfoResponse {
        server_version: resp.server_version,
        capabilities: Some(workflowservice::get_system_info_response::Capabilities {
            signal_and_query_header: resp.capabilities.signal_and_query_header,
            internal_error_differentiation: resp.capabilities.internal_error_differentiation,
            activity_failure_include_heartbeat: resp
                .capabilities
                .activity_failure_include_heartbeat,
            supports_schedules: resp.capabilities.supports_schedules,
            encoded_failure_attributes: resp.capabilities.encoded_failure_attributes,
            build_id_based_versioning: resp.capabilities.build_id_based_versioning,
            upsert_memo: resp.capabilities.upsert_memo,
            eager_workflow_start: resp.capabilities.eager_workflow_start,
            sdk_metadata: resp.capabilities.sdk_metadata,
            count_group_by_execution_status: resp.capabilities.count_group_by_execution_status,
            nexus: resp.capabilities.nexus,
            server_scaled_deployments: resp.capabilities.server_scaled_deployments,
        }),
    }
}

pub fn namespace_to_proto(
    namespace: NamespaceDescription,
    standalone_activities: bool,
) -> workflowservice::DescribeNamespaceResponse {
    workflowservice::DescribeNamespaceResponse {
        namespace_info: Some(namespace_proto::NamespaceInfo {
            name: namespace.name,
            state: if namespace.deleted {
                enums::NamespaceState::Deleted as i32
            } else {
                enums::NamespaceState::Registered as i32
            },
            description: namespace.description,
            owner_email: namespace.owner_email,
            data: std::collections::BTreeMap::new(),
            id: namespace.namespace_id.unwrap_or_default(),
            capabilities: Some(namespace_proto::namespace_info::Capabilities {
                // Namespace capability mirrors the v1.31.0 enabled default
                // (namespace_handler.go:862 @ v1.31.0).
                eager_workflow_start: true,
                sync_update: true,
                async_update: true,
                // temporal-api-v1.62-sync accepts RecordWorkerHeartbeat as a
                // no-op; worker-heartbeat-observability owns persistence.
                // Advertising `true` keeps v0.4+ SDK workers running.
                worker_heartbeats: namespace.capabilities.worker_heartbeats,
                reported_problems_search_attribute: namespace
                    .capabilities
                    .reported_problems_search_attribute,
                // Workflow pause/unpause is implemented across the kernel,
                // runtime, and edge; advertise it so SDKs surface the feature.
                workflow_pause: true,
                // Server-uniform: reported from the effective `activity.enableStandalone`
                // (the gRPC layer derives it from the activity bridge), not hardcoded.
                // Ground-truth: `service/frontend/namespace_handler.go:868 @ v1.31.0`.
                standalone_activities,
                worker_poll_complete_on_shutdown: false,
                poller_autoscaling: false,
            }),
            limits: None,
            supports_schedules: false,
        }),
        config: Some(namespace_proto::NamespaceConfig {
            // Echo the namespace's stored retention. Always present (the register
            // path defaults it when omitted), so the Temporal UI's unconditional
            // `.toString()` on this field never sees `undefined`.
            workflow_execution_retention_ttl: Some(prost_types::Duration {
                seconds: namespace.retention.whole_seconds(),
                nanos: namespace.retention.subsec_nanoseconds(),
            }),
            bad_binaries: None,
            history_archival_state: enums::ArchivalState::Disabled as i32,
            history_archival_uri: String::new(),
            visibility_archival_state: enums::ArchivalState::Disabled as i32,
            visibility_archival_uri: String::new(),
            custom_search_attribute_aliases: namespace.custom_search_attribute_aliases,
        }),
        replication_config: Some(replication_proto::NamespaceReplicationConfig {
            active_cluster_name: namespace.cluster_name.clone(),
            clusters: vec![replication_proto::ClusterReplicationConfig {
                cluster_name: namespace.cluster_name,
            }],
            state: 0,
        }),
        failover_version: 1,
        is_global_namespace: namespace.is_global,
        failover_history: Vec::new(),
    }
}

pub fn list_namespaces_to_proto(
    resp: EdgeListNamespacesResponse,
    standalone_activities: bool,
) -> workflowservice::ListNamespacesResponse {
    workflowservice::ListNamespacesResponse {
        namespaces: resp
            .namespaces
            .into_iter()
            .map(|n| namespace_to_proto(n, standalone_activities))
            .collect(),
        next_page_token: resp
            .next_page_token
            .map(|token| token.into_bytes())
            .unwrap_or_default(),
    }
}

pub fn get_history_request_to_edge(
    req: workflowservice::GetWorkflowExecutionHistoryRequest,
) -> Result<crate::translate::GetWorkflowExecutionHistoryRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryRequest.execution",
        ))?;
    Ok(crate::translate::GetWorkflowExecutionHistoryRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        maximum_page_size: req.maximum_page_size.max(0) as usize,
        wait_new_event: req.wait_new_event,
        history_event_filter_type: req.history_event_filter_type,
        next_page_token: req.next_page_token,
    })
}

pub fn get_history_reverse_request_to_edge(
    req: workflowservice::GetWorkflowExecutionHistoryReverseRequest,
) -> Result<crate::translate::GetWorkflowExecutionHistoryReverseRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "GetWorkflowExecutionHistoryReverseRequest.execution",
        ))?;
    Ok(
        crate::translate::GetWorkflowExecutionHistoryReverseRequest {
            namespace: req.namespace,
            workflow_id: execution.workflow_id.clone(),
            run_id: non_empty(execution.run_id.clone()),
            maximum_page_size: req.maximum_page_size.max(0) as usize,
            next_page_token: req.next_page_token,
        },
    )
}

pub fn get_history_response_to_proto(
    resp: crate::translate::GetWorkflowExecutionHistoryResponse,
    filter_type: i32,
    client_follows_next_run_id: bool,
) -> workflowservice::GetWorkflowExecutionHistoryResponse {
    use prost::Message;
    let history_bytes = crate::translate::history_serializer::serialize_history(&resp.history);
    let mut history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    // HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT = 2
    if filter_type == 2
        && let Some(ref mut h) = history
    {
        h.events.retain(|event| is_close_event(event.event_type));
        // FixFollowEvents (`get_history_util.go:555-586 @ v1.31.0`): pre-Sept-2021
        // SDKs cannot follow `new_execution_run_id` off a Failed/TimedOut/Completed
        // close event, so a close-event-only read for a client NOT advertising the
        // `follows-next-run-id` feature rewrites the closing event into a synthetic
        // ContinuedAsNew carrying the successor run id. Capable clients get the
        // real event (an unconditional rewrite would be wrong for them).
        if !client_follows_next_run_id
            && let Some(last) = h.events.last_mut()
            && let Some(fake) = fake_continued_as_new_event(last)
        {
            *last = fake;
        }
    }

    workflowservice::GetWorkflowExecutionHistoryResponse {
        history,
        // The edge handler owns token semantics for BOTH filter types: a
        // close-event-filtered long poll on an OPEN run returns empty events
        // with a NON-empty token so the client keeps polling until the close
        // event arrives; only the page delivering the close event (or a
        // non-long-poll exhausted read) ends pagination
        // (`getworkflowexecutionhistory/api.go:488` @ v1.31.0;
        // TestGetWorkflowExecutionHistory_Close pins the open-run token).
        next_page_token: resp.next_page_token,
        ..Default::default()
    }
}

/// Build the synthetic `WorkflowExecutionContinuedAsNew` that `FixFollowEvents`
/// substitutes for a retry/cron-chained close event when serving a legacy
/// client (`makeFakeContinuedAsNewEvent`, `get_history_util.go:588-640 @
/// v1.31.0`): only Completed/Failed/TimedOut events with a non-empty
/// `new_execution_run_id` rewrite; the fake keeps the original event id/time
/// and copies the result/failure so the outcome stays visible.
fn fake_continued_as_new_event(
    last: &tokeira_proto::history::HistoryEvent,
) -> Option<tokeira_proto::history::HistoryEvent> {
    use tokeira_proto::history::{
        HistoryEvent, WorkflowExecutionContinuedAsNewEventAttributes, history_event::Attributes,
    };
    let mut new_attrs = WorkflowExecutionContinuedAsNewEventAttributes::default();
    match &last.attributes {
        Some(Attributes::WorkflowExecutionCompletedEventAttributes(attrs))
            if !attrs.new_execution_run_id.is_empty() =>
        {
            new_attrs.new_execution_run_id = attrs.new_execution_run_id.clone();
            new_attrs.last_completion_result = attrs.result.clone();
        }
        Some(Attributes::WorkflowExecutionFailedEventAttributes(attrs))
            if !attrs.new_execution_run_id.is_empty() =>
        {
            new_attrs.new_execution_run_id = attrs.new_execution_run_id.clone();
            new_attrs.failure = attrs.failure.clone();
        }
        Some(Attributes::WorkflowExecutionTimedOutEventAttributes(attrs))
            if !attrs.new_execution_run_id.is_empty() =>
        {
            new_attrs.new_execution_run_id = attrs.new_execution_run_id.clone();
            // failure.NewTimeoutFailure("workflow timeout", START_TO_CLOSE).
            new_attrs.failure = Some(failure_proto::Failure {
                message: "workflow timeout".to_string(),
                failure_info: Some(failure_proto::failure::FailureInfo::TimeoutFailureInfo(
                    failure_proto::TimeoutFailureInfo {
                        timeout_type: enums::TimeoutType::StartToClose as i32,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
        }
        _ => return None,
    }
    Some(HistoryEvent {
        event_id: last.event_id,
        event_time: last.event_time.clone(),
        event_type: enums::EventType::WorkflowExecutionContinuedAsNew as i32,
        version: last.version,
        task_id: last.task_id,
        attributes: Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
            new_attrs,
        )),
        ..Default::default()
    })
}

pub fn get_history_reverse_response_to_proto(
    resp: crate::translate::GetWorkflowExecutionHistoryReverseResponse,
) -> workflowservice::GetWorkflowExecutionHistoryReverseResponse {
    use prost::Message;
    let history_bytes = crate::translate::history_serializer::serialize_history(&resp.history);
    let history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    workflowservice::GetWorkflowExecutionHistoryReverseResponse {
        history,
        next_page_token: resp.next_page_token,
    }
}

// v1.62-sync: reads deprecated `DescribeTaskQueueRequest.include_task_queue_status`
// for wire-compat with v0.4-era SDK clients. v1.62 replaces it with explicit
// stats request fields; v0.4 callers still set the boolean so edge preserves the read.
#[allow(deprecated)]
pub fn describe_task_queue_request_to_edge(
    req: workflowservice::DescribeTaskQueueRequest,
) -> Result<EdgeDescribeTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DescribeTaskQueueRequest.task_queue",
        ))?;

    let task_kind = match req.task_queue_type {
        x if x == enums::TaskQueueType::Activity as i32 => TaskKind::Activity,
        _ => TaskKind::Workflow,
    };

    Ok(EdgeDescribeTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        task_kind,
        include_status: req.include_task_queue_status,
        report_stats: req.report_stats,
        enhanced: req.api_mode == enums::DescribeTaskQueueMode::Enhanced as i32,
    })
}

/// Validate a `ListTaskQueuePartitions` request before any runtime lookup
/// (api-conformance-task-queue Property 3): the namespace and task queue must be
/// present and the task-queue kind must be a recognized enum. All failures surface as
/// `INVALID_ARGUMENT` (via [`proto_conversion_status`](crate::grpc::errors::proto_conversion_status)).
pub fn list_task_queue_partitions_request_to_edge(
    req: workflowservice::ListTaskQueuePartitionsRequest,
) -> Result<EdgeListTaskQueuePartitionsRequest, ProtoConversionError> {
    if req.namespace.trim().is_empty() {
        return Err(ProtoConversionError::MissingField(
            "ListTaskQueuePartitionsRequest.namespace",
        ));
    }
    let task_queue = req.task_queue.ok_or(ProtoConversionError::MissingField(
        "ListTaskQueuePartitionsRequest.task_queue",
    ))?;
    if task_queue.name.trim().is_empty() {
        return Err(ProtoConversionError::MissingField(
            "ListTaskQueuePartitionsRequest.task_queue.name",
        ));
    }
    // Reject an unrecognized task-queue kind enum (Requirement 3.4). UNSPECIFIED /
    // NORMAL / STICKY (and WORKER_COMMANDS) all decode; any other wire value is rejected.
    if enums::TaskQueueKind::try_from(task_queue.kind).is_err() {
        return Err(ProtoConversionError::InvalidArgument(format!(
            "unrecognized task queue kind: {}",
            task_queue.kind
        )));
    }
    Ok(EdgeListTaskQueuePartitionsRequest {
        namespace: req.namespace,
        task_queue: task_queue.name,
    })
}

/// Project the edge partition topology onto the wire `ListTaskQueuePartitionsResponse`.
pub fn list_task_queue_partitions_response_to_proto(
    resp: EdgeListTaskQueuePartitionsResponse,
) -> workflowservice::ListTaskQueuePartitionsResponse {
    workflowservice::ListTaskQueuePartitionsResponse {
        activity_task_queue_partitions: resp
            .activity_partitions
            .into_iter()
            .map(task_queue_partition_to_proto)
            .collect(),
        workflow_task_queue_partitions: resp
            .workflow_partitions
            .into_iter()
            .map(task_queue_partition_to_proto)
            .collect(),
    }
}

fn task_queue_partition_to_proto(
    partition: EdgeTaskQueuePartition,
) -> taskqueue_proto::TaskQueuePartitionMetadata {
    taskqueue_proto::TaskQueuePartitionMetadata {
        key: partition.key,
        owner_host_name: partition.owner_host_name,
    }
}

// v1.62-sync: writes deprecated `PollerInfo.worker_version_capabilities` and
// `DescribeTaskQueueResponse.task_queue_status` for wire-compat with v0.4-era
// SDK readers. v1.62 replaces the former with `deployment_options` and the
// latter with `stats` on `DescribeTaskQueueResponse`; migration is owned by
// task 4.9 (wire-through additions) and `runtime-worker-versioning`.
#[allow(deprecated)]
pub fn describe_task_queue_response_to_proto(
    resp: EdgeDescribeTaskQueueResponse,
) -> workflowservice::DescribeTaskQueueResponse {
    // Worker-version capabilities are still absent, but queue-level Worker
    // Deployment versioning is published when the queue is owned by a deployment.
    let versioning_info = resp
        .versioning_info
        .map(task_queue_versioning_info_to_proto);
    let stats = resp.stats.map(task_queue_stats_to_proto);
    let mut versions_info = std::collections::BTreeMap::new();
    if resp.workflow_stats.is_some() || resp.activity_stats.is_some() {
        let mut types_info = std::collections::BTreeMap::new();
        if let Some(workflow_stats) = resp.workflow_stats {
            types_info.insert(
                enums::TaskQueueType::Workflow as i32,
                taskqueue_proto::TaskQueueTypeInfo {
                    pollers: Vec::new(),
                    stats: Some(task_queue_stats_to_proto(workflow_stats)),
                },
            );
        }
        if let Some(activity_stats) = resp.activity_stats {
            types_info.insert(
                enums::TaskQueueType::Activity as i32,
                taskqueue_proto::TaskQueueTypeInfo {
                    pollers: Vec::new(),
                    stats: Some(task_queue_stats_to_proto(activity_stats)),
                },
            );
        }
        versions_info.insert(
            String::new(),
            taskqueue_proto::TaskQueueVersionInfo {
                types_info,
                task_reachability: enums::BuildIdTaskReachability::Unspecified as i32,
            },
        );
    }
    workflowservice::DescribeTaskQueueResponse {
        pollers: resp
            .pollers
            .into_iter()
            .map(
                |poller| tokeira_proto::public::temporal::api::taskqueue::v1::PollerInfo {
                    last_access_time: poller.last_access_time.map(to_proto_timestamp),
                    identity: poller.identity,
                    rate_per_second: poller.rate_per_second,
                    worker_version_capabilities: None,
                    deployment_options: None,
                },
            )
            .collect(),
        stats,
        stats_by_priority_key: Default::default(),
        versioning_info,
        config: Some(task_queue_config_to_proto(resp.config)),
        effective_rate_limit: None,
        task_queue_status: resp.backlog_count_hint.map(|backlog_count_hint| {
            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueueStatus {
                backlog_count_hint,
                ..Default::default()
            }
        }),
        versions_info,
    }
}

fn task_queue_stats_to_proto(
    stats: crate::translate::TaskQueueStatsDto,
) -> taskqueue_proto::TaskQueueStats {
    taskqueue_proto::TaskQueueStats {
        approximate_backlog_count: stats.approximate_backlog_count,
        approximate_backlog_age: time::Duration::try_from(stats.approximate_backlog_age)
            .ok()
            .map(to_proto_duration),
        tasks_add_rate: 0.0,
        tasks_dispatch_rate: 0.0,
    }
}

/// Map the edge task-queue versioning DTO onto the proto message. The deprecated
/// string fields follow v1.31.0's matching layer
/// (`task_queue_partition_manager.go:976 @ v1.31.0`): the current-version string is
/// `WorkerDeploymentVersionToStringV31` (a nil current renders `__unversioned__`),
/// while the ramping string/struct are populated only when actually ramping (a nil
/// ramping renders as empty string / nil struct, not `__unversioned__`).
#[allow(deprecated)]
fn task_queue_versioning_info_to_proto(
    info: crate::translate::TaskQueueVersioningInfo,
) -> taskqueue_proto::TaskQueueVersioningInfo {
    fn version_struct(
        id: &crate::translate::WorkerDeploymentVersionId,
    ) -> deployment_proto::WorkerDeploymentVersion {
        deployment_proto::WorkerDeploymentVersion {
            deployment_name: id.deployment_name.clone(),
            build_id: id.build_id.clone(),
        }
    }
    fn version_string(id: &crate::translate::WorkerDeploymentVersionId) -> String {
        format!("{}.{}", id.deployment_name, id.build_id)
    }

    let current_version = info
        .current_deployment_version
        .as_ref()
        .map(version_string)
        .unwrap_or_else(|| UNVERSIONED_VERSION_ID.to_string());
    let ramping_version = if info.ramping_to_unversioned {
        // Ramp to unversioned workers: nil structured version, `__unversioned__`
        // in the deprecated string field (`ExternalWorkerDeploymentVersionToStringV31`
        // of nil @ v1.31.0).
        UNVERSIONED_VERSION_ID.to_string()
    } else {
        info.ramping_deployment_version
            .as_ref()
            .map(version_string)
            .unwrap_or_default()
    };
    taskqueue_proto::TaskQueueVersioningInfo {
        current_deployment_version: info.current_deployment_version.as_ref().map(version_struct),
        current_version,
        ramping_deployment_version: info.ramping_deployment_version.as_ref().map(version_struct),
        ramping_version,
        ramping_version_percentage: info.ramping_version_percentage,
        update_time: info.update_time.map(to_proto_timestamp),
    }
}

pub fn task_queue_config_from_update_request(
    req: &workflowservice::UpdateTaskQueueConfigRequest,
) -> TaskQueueConfig {
    let now = OffsetDateTime::now_utc();
    let metadata = |reason: String| crate::translate::TaskQueueConfigMetadata {
        reason,
        update_identity: req.identity.clone(),
        update_time: now,
    };
    TaskQueueConfig {
        queue_rate_limit: req
            .update_queue_rate_limit
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        queue_rate_limit_metadata: req
            .update_queue_rate_limit
            .as_ref()
            .map(|update| metadata(update.reason.clone())),
        fairness_key_rate_limit_default: req
            .update_fairness_key_rate_limit_default
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        fairness_key_rate_limit_metadata: req
            .update_fairness_key_rate_limit_default
            .as_ref()
            .map(|update| metadata(update.reason.clone())),
        fairness_weight_overrides: req.set_fairness_weight_overrides.clone(),
    }
}

pub fn task_queue_config_to_proto(config: TaskQueueConfig) -> taskqueue_proto::TaskQueueConfig {
    taskqueue_proto::TaskQueueConfig {
        queue_rate_limit: (config.queue_rate_limit.is_some()
            || config.queue_rate_limit_metadata.is_some())
        .then(|| {
            rate_limit_config_to_proto(config.queue_rate_limit, config.queue_rate_limit_metadata)
        }),
        fairness_keys_rate_limit_default: (config.fairness_key_rate_limit_default.is_some()
            || config.fairness_key_rate_limit_metadata.is_some())
        .then(|| {
            rate_limit_config_to_proto(
                config.fairness_key_rate_limit_default,
                config.fairness_key_rate_limit_metadata,
            )
        }),
        fairness_weight_overrides: config.fairness_weight_overrides,
    }
}

fn rate_limit_config_to_proto(
    requests_per_second: Option<f32>,
    metadata: Option<crate::translate::TaskQueueConfigMetadata>,
) -> taskqueue_proto::RateLimitConfig {
    taskqueue_proto::RateLimitConfig {
        rate_limit: requests_per_second.map(|requests_per_second| taskqueue_proto::RateLimit {
            requests_per_second,
        }),
        metadata: metadata.map(|metadata| taskqueue_proto::ConfigMetadata {
            reason: metadata.reason,
            update_identity: metadata.update_identity,
            update_time: Some(to_proto_timestamp(metadata.update_time)),
        }),
    }
}

pub fn delete_request_to_edge(
    req: workflowservice::DeleteWorkflowExecutionRequest,
) -> Result<EdgeDeleteWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req.workflow_execution.as_ref().ok_or_else(|| {
        ProtoConversionError::InvalidArgument("Execution is not set on request.".to_string())
    })?;
    if execution.workflow_id.is_empty() {
        return Err(ProtoConversionError::InvalidArgument(
            "WorkflowId is not set on request.".to_string(),
        ));
    }
    let run_id = if execution.run_id.is_empty() {
        None
    } else {
        parse_run_id(&execution.run_id)
            .map_err(|_| ProtoConversionError::InvalidArgument("Invalid RunId.".to_string()))?;
        Some(execution.run_id.clone())
    };

    Ok(EdgeDeleteWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id,
    })
}

pub fn reset_request_to_edge(
    req: workflowservice::ResetWorkflowExecutionRequest,
) -> Result<EdgeResetWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "ResetWorkflowExecutionRequest.workflow_execution",
        ))?;

    // Map the deprecated ResetReapplyType to the exclude set and union any explicit
    // ResetReapplyExcludeTypes (resetworkflow/api.go:199-219 @ v1.31.0):
    // ALL_ELIGIBLE(3)/UNSPECIFIED(0) -> {}, SIGNAL(1) -> {UPDATE}, NONE(2) ->
    // {SIGNAL,UPDATE}; exclude-type SIGNAL=1, UPDATE=2.
    let (mut reapply_exclude_signal, mut reapply_exclude_update) = match req.reset_reapply_type {
        1 => (false, true),
        2 => (true, true),
        _ => (false, false),
    };
    for exclude in &req.reset_reapply_exclude_types {
        match exclude {
            1 => reapply_exclude_signal = true,
            2 => reapply_exclude_update = true,
            _ => {}
        }
    }

    Ok(EdgeResetWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        workflow_task_finish_event_id: req.workflow_task_finish_event_id,
        request_id: non_empty(req.request_id),
        reapply_exclude_signal,
        reapply_exclude_update,
    })
}

pub fn reset_response_to_proto(
    resp: EdgeResetWorkflowExecutionResponse,
) -> workflowservice::ResetWorkflowExecutionResponse {
    workflowservice::ResetWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
    }
}

pub fn signal_with_start_request_to_edge(
    req: workflowservice::SignalWithStartWorkflowExecutionRequest,
) -> Result<EdgeSignalWithStartWorkflowExecutionRequest, ProtoConversionError> {
    reject_behavioral_time_skipping(
        req.time_skipping_config.as_ref(),
        "SignalWithStartWorkflowExecutionRequest.time_skipping_config",
    )?;
    if req.workflow_start_delay.is_some() && !req.cron_schedule.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "SignalWithStartWorkflowExecutionRequest.workflow_start_delay/cron_schedule",
        ));
    }

    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "SignalWithStartWorkflowExecutionRequest.task_queue",
        ))?;
    reject_internal_per_ns_task_queue(&task_queue.name)?;
    validate_links(&req.links)?;

    if matches!(
        enums::WorkflowIdConflictPolicy::try_from(req.workflow_id_conflict_policy).ok(),
        Some(enums::WorkflowIdConflictPolicy::Fail)
    ) {
        // Signal-with-start defaults to USE_EXISTING and rejects FAIL at the
        // frontend (`service/frontend/workflow_handler.go:2279,2332 @ v1.31.0`).
        // Accepting FAIL would turn an SDK-visible invalid request into a
        // mutation-path decision, which diverges from Temporal's validation order.
        return Err(ProtoConversionError::MissingField(
            "SignalWithStartWorkflowExecutionRequest.workflow_id_conflict_policy",
        ));
    }

    let mut conflict_policy = if matches!(
        enums::WorkflowIdConflictPolicy::try_from(req.workflow_id_conflict_policy).ok(),
        None | Some(enums::WorkflowIdConflictPolicy::Unspecified)
    ) {
        tokeira_kernel::WorkflowIdConflictPolicy::UseExisting
    } else {
        extract_conflict_policy(req.workflow_id_conflict_policy)
    };
    let mut reuse_policy = extract_reuse_policy(req.workflow_id_reuse_policy);
    migrate_reuse_policy(
        &mut reuse_policy,
        &mut conflict_policy,
        req.workflow_id_reuse_policy,
    );

    let cron_schedule = non_empty(req.cron_schedule);
    validate_client_cron_schedule(cron_schedule.as_deref())?;

    Ok(EdgeSignalWithStartWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        workflow_type: req.workflow_type.map(|wt| wt.name).unwrap_or_default(),
        task_queue: task_queue.name.clone(),
        input: req
            .input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        request_id: non_empty(req.request_id),
        memo: req.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
        search_attributes: req
            .search_attributes
            .as_ref()
            .map(search_attributes_to_domain)
            .transpose()?
            .unwrap_or_default(),
        identity: non_empty(req.identity),
        workflow_execution_timeout: workflow_timeout_to_time(
            req.workflow_execution_timeout.as_ref(),
        ),
        workflow_run_timeout: workflow_timeout_to_time(req.workflow_run_timeout.as_ref()),
        workflow_task_timeout: workflow_task_timeout_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req
            .retry_policy
            .as_ref()
            .map(workflow_retry_policy_with_defaults),
        conflict_policy,
        reuse_policy,
        header: req.header.as_ref().map(headers_to_domain),
        workflow_start_delay: valid_non_negative_duration(
            req.workflow_start_delay.as_ref(),
            "SignalWithStartWorkflowExecutionRequest.workflow_start_delay",
        )?,
        user_metadata: user_metadata_to_edge(req.user_metadata.as_ref()),
        links: links_to_edge(&req.links)?,
        versioning_override: versioning_override_to_edge(req.versioning_override)?,
        priority: priority_to_edge(req.priority.as_ref()),
        cron_schedule,
        signal_name: req.signal_name,
        signal_input: req
            .signal_input
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
    })
}

pub fn signal_with_start_response_to_proto(
    resp: EdgeSignalWithStartWorkflowExecutionResponse,
) -> workflowservice::SignalWithStartWorkflowExecutionResponse {
    workflowservice::SignalWithStartWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
        started: resp.started,
        signal_link: None,
    }
}

fn is_close_event(event_type: i32) -> bool {
    use tokeira_proto::enums::EventType;
    matches!(
        EventType::try_from(event_type),
        Ok(EventType::WorkflowExecutionCompleted
            | EventType::WorkflowExecutionFailed
            | EventType::WorkflowExecutionTimedOut
            | EventType::WorkflowExecutionCanceled
            | EventType::WorkflowExecutionTerminated
            | EventType::WorkflowExecutionContinuedAsNew)
    )
}

// v1.62-sync: reads deprecated `namespace` and `control` fields on
// `RequestCancelExternalWorkflowExecutionCommandAttributes`,
// `StartChildWorkflowExecutionCommandAttributes`, and
// `SignalExternalWorkflowExecutionCommandAttributes`. v1.62 replaces `namespace`
// with `namespace_id` (equivalent semantics) and moves `control` to a separate
// `input_payload` shape; edge preserves both reads so v0.4-era SDK completions
// that still send the old shape continue to dispatch correctly.
#[allow(deprecated)]
fn memo_patch_to_domain(value: &proto_common::Memo) -> MemoPatch {
    MemoPatch(
        value
            .fields
            .iter()
            .map(|(key, payload)| {
                let change = if is_temporal_nil_payload(payload) {
                    FieldChange::Clear
                } else {
                    FieldChange::Set(payload_to_domain(payload))
                };
                (key.clone(), change)
            })
            .collect(),
    )
}

fn search_attributes_patch_to_domain(
    value: &proto_common::SearchAttributes,
) -> Result<SearchAttributesPatch, ProtoConversionError> {
    let mut patch = BTreeMap::new();
    for (key, payload) in &value.indexed_fields {
        if tokeira_types::is_banned_predefined_search_attribute(key) {
            return Err(ProtoConversionError::InvalidArgument(format!(
                "{key} attribute can't be set in SearchAttributes"
            )));
        }
        let change = if is_temporal_nil_payload(payload) {
            FieldChange::Clear
        } else {
            FieldChange::Set(search_attr_payload_to_domain(payload)?)
        };
        patch.insert(key.clone(), change);
    }
    Ok(SearchAttributesPatch(patch))
}

#[allow(deprecated)]
pub fn proto_command_to_workflow_command(
    cmd: command::Command,
    default_namespace: &str,
) -> Result<WorkflowCommand, ProtoConversionError> {
    use command::command::Attributes;

    match cmd.attributes {
        Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) => {
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "ScheduleActivityCommandAttributes.task_queue",
                    ))?;
            let activity_timeouts = normalized_activity_timeouts(
                attrs.schedule_to_close_timeout.as_ref(),
                attrs.schedule_to_start_timeout.as_ref(),
                attrs.start_to_close_timeout.as_ref(),
                attrs.heartbeat_timeout.as_ref(),
            );
            Ok(WorkflowCommand::ScheduleActivity {
                activity_id: attrs.activity_id,
                activity_type: attrs
                    .activity_type
                    .as_ref()
                    .map(|activity_type| activity_type.name.clone())
                    .unwrap_or_default(),
                task_queue: task_queue_to_domain(task_queue),
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                request_eager_execution: attrs.request_eager_execution,
                // Activities always carry a retry policy in v1.31.0; default/ensure it here.
                retry_policy: Some(activity_retry_policy_with_defaults(
                    attrs.retry_policy.as_ref(),
                )),
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: activity_timeouts.0,
                schedule_to_start_timeout: activity_timeouts.1,
                start_to_close_timeout: activity_timeouts.2,
                heartbeat_timeout: activity_timeouts.3,
            })
        }
        Some(Attributes::StartTimerCommandAttributes(attrs)) => {
            let delay = attrs
                .start_to_fire_timeout
                .map(|d| time::Duration::new(d.seconds, d.nanos))
                .unwrap_or(time::Duration::ZERO);
            Ok(WorkflowCommand::StartTimer {
                timer_id: attrs.timer_id,
                fire_at: OffsetDateTime::now_utc() + delay,
            })
        }
        Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::UpsertSearchAttributesPatch(
                attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_patch_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
            ))
        }
        Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::UpsertMemoPatch(
                attrs
                    .upserted_memo
                    .as_ref()
                    .map(memo_patch_to_domain)
                    .unwrap_or_default(),
            ))
        }
        Some(Attributes::CompleteWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CompleteWorkflow {
                result: attrs
                    .result
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
            })
        }
        Some(Attributes::FailWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::FailWorkflow {
                failure: attrs
                    .failure
                    .as_ref()
                    .map(failure_to_payload)
                    .unwrap_or_else(|| failure_to_payload(&failure_proto::Failure::default())),
            })
        }
        Some(Attributes::RequestCancelActivityTaskCommandAttributes(attrs)) => {
            // The proto command's only key (message.proto:71-74 @ v1.31.0);
            // carried through verbatim (kernel raise K4).
            Ok(WorkflowCommand::RequestCancelActivity {
                scheduled_event_id: attrs.scheduled_event_id,
            })
        }
        Some(Attributes::CancelTimerCommandAttributes(attrs)) => Ok(WorkflowCommand::CancelTimer {
            timer_id: attrs.timer_id,
        }),
        Some(Attributes::CancelWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CancelWorkflow {
                details: attrs.details.as_ref().map(payloads_to_domain),
            })
        }
        Some(Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_namespace: non_empty(attrs.namespace),
                target_workflow_id: WorkflowId(attrs.workflow_id),
                target_run_id: non_empty(attrs.run_id)
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
                control: attrs.control,
            })
        }
        Some(Attributes::RecordMarkerCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::RecordMarker {
                marker_name: attrs.marker_name,
                details: attrs
                    .details
                    .iter()
                    .map(|(key, payloads)| (key.clone(), payloads_to_domain(payloads)))
                    .collect(),
                failure: attrs.failure.as_ref().map(failure_to_payload),
                header: attrs
                    .header
                    .as_ref()
                    .map(|header| headers_to_domain(header).0),
            })
        }
        Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs)) => {
            // A CaN command that omits task_queue / workflow_type inherits them
            // from the previous execution (v1.31.0 ValidateContinueAsNew
            // WorkflowExecutionAttributes: "Inherit … from previous execution if
            // not provided", command_attr_validator.go:397-430). The kernel
            // defaults an empty task queue / workflow type to the current run's,
            // so an absent one passes through as empty here.
            let task_queue = attrs
                .task_queue
                .as_ref()
                .map(task_queue_to_domain)
                .unwrap_or_default();
            Ok(WorkflowCommand::ContinueAsNew {
                new_run_id: RunId::new(),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue,
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                memo: attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
                search_attributes: attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: workflow_timeout_to_time(attrs.workflow_run_timeout.as_ref()),
                workflow_task_timeout: workflow_task_timeout_to_time(
                    attrs.workflow_task_timeout.as_ref(),
                )
                .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                header: attrs.header.as_ref().map(headers_to_domain),
            })
        }
        Some(Attributes::StartChildWorkflowExecutionCommandAttributes(attrs)) => {
            // A child command that omits `task_queue` inherits the parent's
            // (v1.31.0 ValidateStartChildExecutionAttributes: "Inherit taskqueue
            // from parent workflow execution if not provided", command_attr_
            // validator.go:80-89 — normalizes against parentInfo.TaskQueue). The
            // kernel defaults an empty child task queue to the parent's, so an
            // absent command task queue passes through as empty here.
            let task_queue = attrs
                .task_queue
                .as_ref()
                .map(task_queue_to_domain)
                .unwrap_or_default();
            // A child command that omits `namespace` inherits the parent's
            // namespace (the WFT-completing worker's namespace). tokeira derives
            // the namespace id by hashing the name, so an empty name would hash
            // to a bogus id and leave every downstream ChildWorkflowExecution*
            // event's `.Namespace`/`.NamespaceId` empty
            // (`TestChildWorkflowExecution` asserts both against the parent's).
            let child_namespace =
                non_empty(attrs.namespace).unwrap_or_else(|| default_namespace.to_string());
            Ok(WorkflowCommand::StartChildWorkflow {
                child_workflow_id: WorkflowId(attrs.workflow_id),
                namespace_id: namespace_name_to_domain(&child_namespace),
                namespace: non_empty(child_namespace),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue,
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                memo: attrs.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
                search_attributes: attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
                workflow_execution_timeout: workflow_timeout_to_time(
                    attrs.workflow_execution_timeout.as_ref(),
                ),
                workflow_run_timeout: workflow_timeout_to_time(attrs.workflow_run_timeout.as_ref()),
                workflow_task_timeout: workflow_task_timeout_to_time(
                    attrs.workflow_task_timeout.as_ref(),
                )
                .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                cron_schedule: non_empty(attrs.cron_schedule),
                parent_close_policy: parent_close_policy_to_domain(attrs.parent_close_policy),
                reuse_policy: extract_reuse_policy(attrs.workflow_id_reuse_policy),
            })
        }
        Some(Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs)) => {
            let execution = attrs
                .execution
                .as_ref()
                .ok_or(ProtoConversionError::MissingField(
                    "SignalExternalWorkflowExecutionCommandAttributes.execution",
                ))?;
            Ok(WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: namespace_name_to_domain(&attrs.namespace),
                target_namespace: non_empty(attrs.namespace),
                target_workflow_id: WorkflowId(execution.workflow_id.clone()),
                target_run_id: non_empty(execution.run_id.clone())
                    .map(|run_id| parse_run_id(&run_id))
                    .transpose()?,
                signal_name: attrs.signal_name,
                input: attrs
                    .input
                    .as_ref()
                    .map(payloads_to_domain)
                    .unwrap_or_default(),
                header: attrs.header.as_ref().map(headers_to_domain),
                control: attrs.control,
            })
        }
        Some(Attributes::ProtocolMessageCommandAttributes(attrs)) => {
            // ProtocolMessage commands reference a message in the
            // completion's `messages` field by message_id. The body
            // is resolved by the caller which has access to the
            // messages list. Return a placeholder that the caller
            // will resolve.
            Ok(WorkflowCommand::ProtocolMessage {
                message_id: attrs.message_id,
                body: tokeira_kernel::UpdateProtocolBody::Accepted {
                    update_id: String::new(),
                    update_name: String::new(),
                    input: Payloads::default(),
                    sequencing_event_id: 0,
                },
            })
        }
        Some(Attributes::ScheduleNexusOperationCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::ScheduleNexusOperation {
                operation_id: Uuid::new_v4().to_string(),
                endpoint: attrs.endpoint,
                service: attrs.service,
                operation: attrs.operation,
                input: attrs
                    .input
                    .as_ref()
                    .map(|payload| Payloads(vec![payload_to_domain(payload)]))
                    .unwrap_or_default(),
                schedule_to_close_timeout: proto_duration_to_time(
                    attrs.schedule_to_close_timeout.as_ref(),
                ),
                schedule_to_start_timeout: proto_duration_to_time(
                    attrs.schedule_to_start_timeout.as_ref(),
                ),
                start_to_close_timeout: proto_duration_to_time(
                    attrs.start_to_close_timeout.as_ref(),
                ),
            })
        }
        Some(Attributes::RequestCancelNexusOperationCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::CancelNexusOperation {
                scheduled_event_id: attrs.scheduled_event_id,
            })
        }
        None => Err(ProtoConversionError::MissingField("Command.attributes")),
    }
}

fn deletion_payload() -> proto_common::Payload {
    proto_common::Payload {
        metadata: BTreeMap::from([("encoding".to_string(), b"json/plain".to_vec())]),
        data: b"null".to_vec(),
        external_payloads: Vec::new(),
    }
}

fn memo_patch_from_domain(value: &MemoPatch) -> proto_common::Memo {
    proto_common::Memo {
        fields: value
            .0
            .iter()
            .filter_map(|(key, change)| match change {
                FieldChange::Unchanged => None,
                FieldChange::Set(payload) => Some((key.clone(), payload_from_domain(payload))),
                FieldChange::Clear => Some((key.clone(), deletion_payload())),
            })
            .collect(),
    }
}

fn search_attributes_patch_from_domain(
    value: &SearchAttributesPatch,
) -> proto_common::SearchAttributes {
    proto_common::SearchAttributes {
        indexed_fields: value
            .0
            .iter()
            .filter_map(|(key, change)| match change {
                FieldChange::Unchanged => None,
                FieldChange::Set(attribute) => {
                    Some((key.clone(), search_attr_value_to_payload(attribute)))
                }
                FieldChange::Clear => Some((key.clone(), deletion_payload())),
            })
            .collect(),
    }
}

// v1.62-sync: writes deprecated `namespace` and `control` fields on outbound
// `StartChildWorkflowExecutionCommandAttributes`,
// `SignalExternalWorkflowExecutionCommandAttributes`, and
// `RequestCancelExternalWorkflowExecutionCommandAttributes` for wire-compat
// with v0.4-era SDK readers. v1.62 introduces `namespace_id` and an
// `input_payload`-based control shape; the edge will emit both once the
// follow-up spec lands (see `runtime-worker-versioning` for namespace id
// unification; `control` retirement has no named follow-up and should track
// with the next command-attributes cleanup spec).
#[allow(deprecated)]
pub fn workflow_command_to_proto(
    cmd: &WorkflowCommand,
) -> Result<command::Command, ProtoConversionError> {
    use command::command::Attributes;
    let attributes = match cmd {
        WorkflowCommand::ScheduleActivity {
            activity_id,
            activity_type,
            task_queue,
            input,
            header,
            request_eager_execution,
            retry_policy,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            ..
        } => Some(Attributes::ScheduleActivityTaskCommandAttributes(
            command::ScheduleActivityTaskCommandAttributes {
                activity_id: activity_id.clone(),
                activity_type: Some(tokeira_proto::common::ActivityType {
                    name: activity_type.clone(),
                }),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                header: header.as_ref().map(headers_from_domain),
                input: Some(payloads_from_domain(input)),
                request_eager_execution: *request_eager_execution,
                schedule_to_close_timeout: schedule_to_close_timeout.map(to_proto_duration),
                schedule_to_start_timeout: schedule_to_start_timeout.map(to_proto_duration),
                start_to_close_timeout: start_to_close_timeout.map(to_proto_duration),
                heartbeat_timeout: heartbeat_timeout.map(to_proto_duration),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartTimer { timer_id, fire_at } => {
            let now = OffsetDateTime::now_utc();
            let delay = *fire_at - now;
            let delay = if delay.is_negative() {
                time::Duration::ZERO
            } else {
                delay
            };
            Some(Attributes::StartTimerCommandAttributes(
                command::StartTimerCommandAttributes {
                    timer_id: timer_id.clone(),
                    start_to_fire_timeout: Some(to_proto_duration(delay)),
                },
            ))
        }
        WorkflowCommand::UpsertSearchAttributes(search_attributes) => {
            Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                command::UpsertWorkflowSearchAttributesCommandAttributes {
                    search_attributes: Some(search_attributes_from_domain(search_attributes)),
                },
            ))
        }
        WorkflowCommand::UpsertMemo(memo) => {
            Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(
                command::ModifyWorkflowPropertiesCommandAttributes {
                    upserted_memo: Some(memo_from_domain(memo)),
                },
            ))
        }
        WorkflowCommand::UpsertSearchAttributesPatch(patch) => {
            Some(Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                command::UpsertWorkflowSearchAttributesCommandAttributes {
                    search_attributes: Some(search_attributes_patch_from_domain(patch)),
                },
            ))
        }
        WorkflowCommand::UpsertMemoPatch(patch) => {
            Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(
                command::ModifyWorkflowPropertiesCommandAttributes {
                    upserted_memo: Some(memo_patch_from_domain(patch)),
                },
            ))
        }
        WorkflowCommand::CompleteWorkflow { result } => {
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
                command::CompleteWorkflowExecutionCommandAttributes {
                    result: Some(payloads_from_domain(result)),
                },
            ))
        }
        WorkflowCommand::FailWorkflow { failure } => {
            Some(Attributes::FailWorkflowExecutionCommandAttributes(
                command::FailWorkflowExecutionCommandAttributes {
                    failure: Some(payload_to_failure(failure)),
                },
            ))
        }
        WorkflowCommand::RequestCancelActivity { scheduled_event_id } => {
            Some(Attributes::RequestCancelActivityTaskCommandAttributes(
                command::RequestCancelActivityTaskCommandAttributes {
                    scheduled_event_id: *scheduled_event_id,
                },
            ))
        }
        WorkflowCommand::CancelTimer { timer_id } => Some(
            Attributes::CancelTimerCommandAttributes(command::CancelTimerCommandAttributes {
                timer_id: timer_id.clone(),
            }),
        ),
        WorkflowCommand::CancelWorkflow { details } => {
            Some(Attributes::CancelWorkflowExecutionCommandAttributes(
                command::CancelWorkflowExecutionCommandAttributes {
                    details: details.as_ref().map(payloads_from_domain),
                },
            ))
        }
        WorkflowCommand::RecordMarker {
            marker_name,
            details,
            failure,
            header,
        } => Some(Attributes::RecordMarkerCommandAttributes(
            command::RecordMarkerCommandAttributes {
                marker_name: marker_name.clone(),
                details: details
                    .iter()
                    .map(|(key, payloads)| (key.clone(), payloads_from_domain(payloads)))
                    .collect(),
                header: header
                    .as_ref()
                    .map(|header| headers_from_domain(&tokeira_types::Headers(header.clone()))),
                failure: failure.as_ref().map(payload_to_failure),
            },
        )),
        WorkflowCommand::ContinueAsNew {
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            ..
        } => Some(Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(
            command::ContinueAsNewWorkflowExecutionCommandAttributes {
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                input: Some(payloads_from_domain(input)),
                workflow_run_timeout: workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                ..Default::default()
            },
        )),
        WorkflowCommand::StartChildWorkflow {
            child_workflow_id,
            namespace_id,
            namespace,
            workflow_type,
            task_queue,
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            parent_close_policy,
            reuse_policy: _,
        } => Some(Attributes::StartChildWorkflowExecutionCommandAttributes(
            command::StartChildWorkflowExecutionCommandAttributes {
                namespace: namespace
                    .clone()
                    .unwrap_or_else(|| namespace_id.0.to_string()),
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(tokeira_proto::conversions::common::task_queue_from_domain(
                    task_queue,
                )),
                input: Some(payloads_from_domain(input)),
                header: header.as_ref().map(headers_from_domain),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                workflow_execution_timeout: workflow_execution_timeout.map(to_proto_duration),
                workflow_run_timeout: workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                retry_policy: retry_policy.as_ref().map(retry_policy_from_domain),
                cron_schedule: cron_schedule.clone().unwrap_or_default(),
                parent_close_policy: parent_close_policy_from_domain(*parent_close_policy),
                ..Default::default()
            },
        )),
        WorkflowCommand::SignalExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
            header,
            control,
        } => Some(
            Attributes::SignalExternalWorkflowExecutionCommandAttributes(
                command::SignalExternalWorkflowExecutionCommandAttributes {
                    namespace: target_namespace
                        .clone()
                        .unwrap_or_else(|| target_namespace_id.0.to_string()),
                    execution: Some(workflow_execution_from_ids(
                        target_workflow_id,
                        target_run_id.unwrap_or(RunId(Uuid::nil())),
                    )),
                    signal_name: signal_name.clone(),
                    input: Some(payloads_from_domain(input)),
                    header: header.as_ref().map(headers_from_domain),
                    control: control.clone(),
                    ..Default::default()
                },
            ),
        ),
        WorkflowCommand::RequestCancelExternalWorkflowExecution {
            target_namespace_id,
            target_namespace,
            target_workflow_id,
            target_run_id,
            control,
        } => Some(
            Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
                command::RequestCancelExternalWorkflowExecutionCommandAttributes {
                    namespace: target_namespace
                        .clone()
                        .unwrap_or_else(|| target_namespace_id.0.to_string()),
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: target_run_id
                        .map(|run_id| run_id.0.to_string())
                        .unwrap_or_default(),
                    control: control.clone(),
                    ..Default::default()
                },
            ),
        ),
        WorkflowCommand::ScheduleNexusOperation {
            endpoint,
            service,
            operation,
            input,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            ..
        } => Some(Attributes::ScheduleNexusOperationCommandAttributes(
            command::ScheduleNexusOperationCommandAttributes {
                endpoint: endpoint.clone(),
                service: service.clone(),
                operation: operation.clone(),
                input: input.0.first().map(payload_from_domain),
                schedule_to_close_timeout: schedule_to_close_timeout.map(to_proto_duration),
                schedule_to_start_timeout: schedule_to_start_timeout.map(to_proto_duration),
                start_to_close_timeout: start_to_close_timeout.map(to_proto_duration),
                ..Default::default()
            },
        )),
        WorkflowCommand::CancelNexusOperation { scheduled_event_id } => {
            Some(Attributes::RequestCancelNexusOperationCommandAttributes(
                command::RequestCancelNexusOperationCommandAttributes {
                    scheduled_event_id: *scheduled_event_id,
                },
            ))
        }
        WorkflowCommand::ProtocolMessage {
            message_id,
            body: _,
        } => Some(Attributes::ProtocolMessageCommandAttributes(
            command::ProtocolMessageCommandAttributes {
                message_id: message_id.clone(),
            },
        )),
        WorkflowCommand::UpdateCompleted { .. }
        | WorkflowCommand::UpdateRejected { .. }
        | WorkflowCommand::RequestNewWorkflowTask
        | WorkflowCommand::InvalidSearchAttributes { .. } => {
            return Err(ProtoConversionError::MissingField(
                "WorkflowCommand has no proto Command equivalent",
            ));
        }
    };

    Ok(command::Command {
        attributes,
        ..Default::default()
    })
}

fn workflow_execution_info_from_description(
    value: &WorkflowExecutionDescription,
) -> workflow::WorkflowExecutionInfo {
    let execution_time = value.execution_time;
    let execution_duration = value
        .close_time
        .map(|close_time| to_proto_duration(close_time - execution_time));
    let fallback_root_workflow_id = WorkflowId(value.workflow_id.clone());
    let root_workflow_id = value
        .root_workflow_id
        .as_ref()
        .unwrap_or(&fallback_root_workflow_id);
    let root_run_id = value.root_run_id.unwrap_or(value.run_id);
    workflow::WorkflowExecutionInfo {
        execution: Some(workflow_execution_from_ids(
            &WorkflowId(value.workflow_id.clone()),
            value.run_id,
        )),
        r#type: Some(tokeira_proto::common::WorkflowType {
            name: value.workflow_type.clone(),
        }),
        task_queue: value.task_queue.clone(),
        status: execution_status_to_proto(value.status),
        start_time: value.start_time.map(to_proto_timestamp),
        execution_time: Some(to_proto_timestamp(execution_time)),
        close_time: value.close_time.map(to_proto_timestamp),
        execution_duration,
        history_length: value.history_length,
        // Temporal reads this from execution statistics
        // (`service/history/api/describeworkflow/api.go:126 @ v1.31.0`); the
        // resolver supplies Tokeira's history-authoritative equivalent.
        history_size_bytes: value.history_size_bytes,
        state_transition_count: value.state_transition_count,
        parent_namespace_id: value.parent_namespace_id.clone().unwrap_or_default(),
        parent_execution: value.parent_workflow_id.as_ref().map(|workflow_id| {
            workflow_execution_from_ids(workflow_id, value.parent_run_id.unwrap_or_default())
        }),
        root_execution: Some(workflow_execution_from_ids(root_workflow_id, root_run_id)),
        first_run_id: value
            .first_run_id
            .map(|run_id| run_id.0.to_string())
            .unwrap_or_default(),
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
        most_recent_worker_version_stamp: value.most_recent_worker_version_stamp.as_ref().map(
            |stamp| proto_common::WorkerVersionStamp {
                build_id: stamp.build_id.clone(),
                use_versioning: stamp.use_versioning,
            },
        ),
        versioning_info: value
            .versioning_info
            .as_ref()
            .map(workflow_versioning_info_from_edge),
        worker_deployment_name: value.worker_deployment_name.clone().unwrap_or_default(),
        // External-payload statistics accumulated over the run's history
        // (describeworkflow/api.go:166 @ v1.31.0).
        external_payload_count: value.external_payload_count,
        external_payload_size_bytes: value.external_payload_size_bytes,
        ..Default::default()
    }
}

fn workflow_extended_info_to_proto(
    value: &WorkflowExecutionDescription,
) -> workflow::WorkflowExecutionExtendedInfo {
    workflow::WorkflowExecutionExtendedInfo {
        execution_expiration_time: value.execution_expiration_time.map(to_proto_timestamp),
        run_expiration_time: value.run_expiration_time.map(to_proto_timestamp),
        cancel_requested: value.cancel_requested,
        original_start_time: Some(to_proto_timestamp(value.original_start_time)),
        // request_id_infos maps each request id that authored an event (the start
        // request → STARTED, each UseExisting attach → OPTIONS_UPDATED) to that
        // event (`WorkflowExecutionExtendedInfo.request_id_infos @ v1.31.0`).
        request_id_infos: value
            .request_id_infos
            .iter()
            .map(|(id, info)| {
                (
                    id.clone(),
                    workflow::RequestIdInfo {
                        event_type: info.event_type,
                        event_id: info.event_id,
                        buffered: info.buffered,
                    },
                )
            })
            .collect(),
        // Reset history linkage is not retained yet, so that field
        // remains default rather than fabricating reset metadata.
        pause_info: value
            .pause_info
            .as_ref()
            .map(|info| workflow::WorkflowExecutionPauseInfo {
                identity: info.identity.clone(),
                paused_time: Some(to_proto_timestamp(info.paused_time)),
                reason: info.reason.clone(),
            }),
        ..Default::default()
    }
}

fn workflow_execution_info_from_summary(
    value: WorkflowExecutionSummary,
) -> workflow::WorkflowExecutionInfo {
    let parent_execution = value
        .parent_workflow_id
        .as_ref()
        .zip(value.parent_run_id)
        .map(|(workflow_id, run_id)| workflow_execution_from_ids(workflow_id, run_id));
    workflow::WorkflowExecutionInfo {
        execution: Some(workflow_execution_from_ids(
            &WorkflowId(value.workflow_id),
            value.run_id,
        )),
        r#type: Some(tokeira_proto::common::WorkflowType {
            name: value.workflow_type,
        }),
        task_queue: value.task_queue,
        status: execution_status_to_proto(value.status),
        start_time: value.start_time.map(to_proto_timestamp),
        execution_time: value.execution_time.map(to_proto_timestamp),
        close_time: value.close_time.map(to_proto_timestamp),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
        parent_execution,
        root_execution: Some(workflow_execution_from_ids(
            &value.root_workflow_id,
            value.root_run_id,
        )),
        ..Default::default()
    }
}

fn workflow_versioning_info_from_edge(
    value: &tokeira_kernel::state::WorkflowVersioningInfo,
) -> workflow::WorkflowExecutionVersioningInfo {
    workflow::WorkflowExecutionVersioningInfo {
        behavior: versioning_behavior_to_proto(value.behavior),
        deployment_version: value
            .deployment_version
            .as_ref()
            .map(worker_deployment_version_ref_to_proto),
        versioning_override: value
            .versioning_override
            .as_ref()
            .map(versioning_override_from_edge),
        version_transition: value
            .version_transition
            .as_ref()
            .map(deployment_version_transition_from_edge),
        revision_number: value.revision_number,
        continue_as_new_initial_versioning_behavior: continue_as_new_behavior_to_proto(
            value.continue_as_new_initial_versioning_behavior,
        ),
        ..Default::default()
    }
}

fn deployment_version_transition_from_edge(
    value: &WorkerDeploymentVersionRef,
) -> workflow::DeploymentVersionTransition {
    workflow::DeploymentVersionTransition {
        deployment_version: Some(worker_deployment_version_ref_to_proto(value)),
        ..Default::default()
    }
}

fn versioning_override_from_edge(value: &KernelVersioningOverride) -> workflow::VersioningOverride {
    match value {
        KernelVersioningOverride::Pinned { version } => workflow::VersioningOverride {
            r#override: Some(workflow::versioning_override::Override::Pinned(
                workflow::versioning_override::PinnedOverride {
                    behavior: workflow::versioning_override::PinnedOverrideBehavior::Pinned as i32,
                    version: Some(worker_deployment_version_ref_to_proto(version)),
                },
            )),
            ..Default::default()
        },
        KernelVersioningOverride::AutoUpgrade => workflow::VersioningOverride {
            r#override: Some(workflow::versioning_override::Override::AutoUpgrade(true)),
            ..Default::default()
        },
    }
}

fn worker_deployment_version_ref_to_proto(
    value: &WorkerDeploymentVersionRef,
) -> deployment_proto::WorkerDeploymentVersion {
    deployment_proto::WorkerDeploymentVersion {
        deployment_name: value.deployment_name.clone(),
        build_id: value.build_id.clone(),
    }
}

fn versioning_behavior_to_proto(value: VersioningBehavior) -> i32 {
    match value {
        VersioningBehavior::Unspecified => enums::VersioningBehavior::Unspecified as i32,
        VersioningBehavior::Pinned => enums::VersioningBehavior::Pinned as i32,
        VersioningBehavior::AutoUpgrade => enums::VersioningBehavior::AutoUpgrade as i32,
    }
}

fn continue_as_new_behavior_to_proto(value: ContinueAsNewVersioningBehavior) -> i32 {
    match value {
        ContinueAsNewVersioningBehavior::Unspecified => {
            enums::ContinueAsNewVersioningBehavior::Unspecified as i32
        }
        ContinueAsNewVersioningBehavior::AutoUpgrade => {
            enums::ContinueAsNewVersioningBehavior::AutoUpgrade as i32
        }
        ContinueAsNewVersioningBehavior::UseRampingVersion => {
            enums::ContinueAsNewVersioningBehavior::UseRampingVersion as i32
        }
    }
}

pub(crate) fn execution_status_to_proto(value: ExecutionStatus) -> i32 {
    use enums::WorkflowExecutionStatus as Proto;

    match value {
        ExecutionStatus::Running => Proto::Running as i32,
        ExecutionStatus::Paused => Proto::Paused as i32,
        ExecutionStatus::Completed => Proto::Completed as i32,
        ExecutionStatus::Failed => Proto::Failed as i32,
        ExecutionStatus::Cancelled => Proto::Canceled as i32,
        ExecutionStatus::Terminated => Proto::Terminated as i32,
        ExecutionStatus::ContinuedAsNew => Proto::ContinuedAsNew as i32,
        ExecutionStatus::TimedOut => Proto::TimedOut as i32,
    }
}

fn _first_payload(payloads: Payloads) -> Option<tokeira_types::Payload> {
    payloads.0.into_iter().next()
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

// ── Activity endpoint translations ──

const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_UPDATE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn serialize_activity_token(token: &ActivityTaskToken) -> Vec<u8> {
    serde_json::to_vec(token).unwrap_or_default()
}

pub fn deserialize_activity_token(bytes: &[u8]) -> Result<ActivityTaskToken, ProtoConversionError> {
    if bytes.is_empty() {
        return Err(ProtoConversionError::InvalidTaskToken(
            "task_token is empty".to_string(),
        ));
    }
    serde_json::from_slice(bytes).map_err(|e| ProtoConversionError::InvalidTaskToken(e.to_string()))
}

// v1.62-sync: reads deprecated worker capabilities for SDKs predating
// `deployment_options`, matching the workflow-poll compatibility path above.
#[allow(deprecated)]
pub fn poll_activity_request_to_edge(
    req: workflowservice::PollActivityTaskQueueRequest,
) -> Result<crate::translate::PollActivityTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollActivityTaskQueueRequest.task_queue",
        ))?;

    let (deployment, build_id) = req
        .deployment_options
        .as_ref()
        .and_then(|options| {
            let mode =
                enums::WorkerVersioningMode::try_from(options.worker_versioning_mode).ok()?;
            if mode != enums::WorkerVersioningMode::Versioned {
                return None;
            }
            Some((
                non_empty(options.deployment_name.clone()).map(DeploymentId),
                non_empty(options.build_id.clone()).map(BuildId),
            ))
        })
        .or_else(|| {
            req.worker_version_capabilities
                .as_ref()
                .filter(|caps| caps.use_versioning)
                .map(|caps| {
                    (
                        non_empty(caps.deployment_series_name.clone()).map(DeploymentId),
                        non_empty(caps.build_id.clone()).map(BuildId),
                    )
                })
        })
        .unwrap_or((None, None));

    Ok(crate::translate::PollActivityTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        worker_instance_key: req.worker_instance_key,
        deployment,
        build_id,
        worker_rate_limit: req
            .task_queue_metadata
            .and_then(|metadata| metadata.max_tasks_per_second),
        timeout: DEFAULT_POLL_TIMEOUT,
    })
}

pub fn poll_activity_response_to_proto(
    resp: crate::translate::PollActivityTaskQueueResponse,
) -> workflowservice::PollActivityTaskQueueResponse {
    // Real RunId, not the internal RunKey (see poll_response_to_proto).
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.workflow_id),
        resp.run_id,
    ));

    workflowservice::PollActivityTaskQueueResponse {
        task_token: resp.task_token,
        workflow_namespace: resp.workflow_namespace,
        workflow_type: Some(tokeira_proto::common::WorkflowType {
            name: resp.workflow_type,
        }),
        activity_id: resp.activity_id,
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: resp.activity_type,
        }),
        header: resp.header.as_ref().map(headers_from_domain),
        input: Some(payloads_from_domain(&resp.input)),
        heartbeat_details: resp.heartbeat_details.as_ref().map(payloads_from_domain),
        scheduled_time: resp.scheduled_time.map(to_proto_timestamp),
        current_attempt_scheduled_time: resp.current_attempt_scheduled_time.map(to_proto_timestamp),
        started_time: resp.started_time.map(to_proto_timestamp),
        attempt: resp.attempt as i32,
        workflow_execution,
        retry_policy: resp.retry_policy.as_ref().map(retry_policy_from_domain),
        schedule_to_close_timeout: resp
            .schedule_to_close_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        start_to_close_timeout: resp
            .start_to_close_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        heartbeat_timeout: resp
            .heartbeat_timeout
            .and_then(|d| time::Duration::try_from(d).ok())
            .map(to_proto_duration),
        poller_scaling_decision: resp.poller_scaling_decision.map(|delta| {
            taskqueue_proto::PollerScalingDecision {
                poll_request_delta_suggestion: delta,
            }
        }),
        ..Default::default()
    }
}

pub fn respond_activity_completed_to_edge(
    req: workflowservice::RespondActivityTaskCompletedRequest,
) -> Result<crate::translate::RespondActivityTaskCompletedRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    Ok(crate::translate::RespondActivityTaskCompletedRequest {
        token,
        result: req
            .result
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        identity: req.identity,
    })
}

pub fn respond_activity_completed_to_proto() -> workflowservice::RespondActivityTaskCompletedResponse
{
    workflowservice::RespondActivityTaskCompletedResponse {}
}

pub fn respond_activity_completed_by_id_to_edge(
    req: workflowservice::RespondActivityTaskCompletedByIdRequest,
) -> Result<crate::translate::RespondActivityTaskCompletedByIdRequest, ProtoConversionError> {
    Ok(crate::translate::RespondActivityTaskCompletedByIdRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        activity_id: req.activity_id,
        result: req
            .result
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        identity: req.identity,
    })
}

pub fn respond_activity_completed_by_id_to_proto()
-> workflowservice::RespondActivityTaskCompletedByIdResponse {
    workflowservice::RespondActivityTaskCompletedByIdResponse {}
}

pub fn respond_activity_failed_to_edge(
    req: workflowservice::RespondActivityTaskFailedRequest,
) -> Result<crate::translate::RespondActivityTaskFailedRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    let (failure, failure_error_type, is_non_retryable) = match req.failure {
        Some(f) => {
            let (error_type, non_retryable) = activity_retry_classification(&f);
            (failure_to_payload(&f), error_type, non_retryable)
        }
        None => (
            failure_to_payload(&failure_proto::Failure::default()),
            None,
            false,
        ),
    };
    Ok(crate::translate::RespondActivityTaskFailedRequest {
        token,
        failure,
        failure_error_type,
        is_non_retryable,
        identity: req.identity,
    })
}

pub fn respond_activity_failed_to_proto() -> workflowservice::RespondActivityTaskFailedResponse {
    workflowservice::RespondActivityTaskFailedResponse {
        ..Default::default()
    }
}

pub fn respond_activity_failed_by_id_to_edge(
    req: workflowservice::RespondActivityTaskFailedByIdRequest,
) -> Result<crate::translate::RespondActivityTaskFailedByIdRequest, ProtoConversionError> {
    let (failure, failure_error_type, is_non_retryable) = match req.failure {
        Some(f) => {
            let (error_type, non_retryable) = activity_retry_classification(&f);
            (failure_to_payload(&f), error_type, non_retryable)
        }
        None => (
            failure_to_payload(&failure_proto::Failure::default()),
            None,
            false,
        ),
    };
    Ok(crate::translate::RespondActivityTaskFailedByIdRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        activity_id: req.activity_id,
        failure,
        failure_error_type,
        is_non_retryable,
        identity: req.identity,
    })
}

pub fn respond_activity_failed_by_id_to_proto()
-> workflowservice::RespondActivityTaskFailedByIdResponse {
    workflowservice::RespondActivityTaskFailedByIdResponse {
        ..Default::default()
    }
}

pub fn respond_activity_canceled_to_edge(
    req: workflowservice::RespondActivityTaskCanceledRequest,
) -> Result<crate::translate::RespondActivityTaskCanceledRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    Ok(crate::translate::RespondActivityTaskCanceledRequest {
        token,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
    })
}

pub fn respond_activity_canceled_to_proto() -> workflowservice::RespondActivityTaskCanceledResponse
{
    workflowservice::RespondActivityTaskCanceledResponse {}
}

pub fn respond_activity_canceled_by_id_to_edge(
    req: workflowservice::RespondActivityTaskCanceledByIdRequest,
) -> Result<crate::translate::RespondActivityTaskCanceledByIdRequest, ProtoConversionError> {
    Ok(crate::translate::RespondActivityTaskCanceledByIdRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        activity_id: req.activity_id,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
    })
}

pub fn respond_activity_canceled_by_id_to_proto()
-> workflowservice::RespondActivityTaskCanceledByIdResponse {
    workflowservice::RespondActivityTaskCanceledByIdResponse {}
}

pub fn record_heartbeat_to_edge(
    req: workflowservice::RecordActivityTaskHeartbeatRequest,
) -> Result<crate::translate::RecordActivityTaskHeartbeatRequest, ProtoConversionError> {
    let token = deserialize_activity_token(&req.task_token)?;
    Ok(crate::translate::RecordActivityTaskHeartbeatRequest {
        token,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
    })
}

pub fn record_heartbeat_to_proto(
    resp: crate::translate::RecordActivityTaskHeartbeatResponse,
) -> workflowservice::RecordActivityTaskHeartbeatResponse {
    workflowservice::RecordActivityTaskHeartbeatResponse {
        cancel_requested: resp.cancel_requested,
        activity_paused: false,
        activity_reset: false,
    }
}

pub fn record_activity_heartbeat_by_id_to_edge(
    req: workflowservice::RecordActivityTaskHeartbeatByIdRequest,
) -> Result<crate::translate::RecordActivityTaskHeartbeatByIdRequest, ProtoConversionError> {
    Ok(crate::translate::RecordActivityTaskHeartbeatByIdRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        activity_id: req.activity_id,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
    })
}

pub fn record_activity_heartbeat_by_id_to_proto(
    resp: crate::translate::RecordActivityTaskHeartbeatByIdResponse,
) -> workflowservice::RecordActivityTaskHeartbeatByIdResponse {
    workflowservice::RecordActivityTaskHeartbeatByIdResponse {
        cancel_requested: resp.cancel_requested,
        activity_paused: false,
        activity_reset: false,
    }
}

fn activity_options_to_edge(
    value: &activity_proto::ActivityOptions,
) -> crate::translate::ActivityOptions {
    crate::translate::ActivityOptions {
        task_queue: value.task_queue.as_ref().map(|queue| queue.name.clone()),
        schedule_to_close_timeout: proto_duration_to_time(value.schedule_to_close_timeout.as_ref()),
        schedule_to_start_timeout: proto_duration_to_time(value.schedule_to_start_timeout.as_ref()),
        start_to_close_timeout: proto_duration_to_time(value.start_to_close_timeout.as_ref()),
        heartbeat_timeout: proto_duration_to_time(value.heartbeat_timeout.as_ref()),
        retry_policy: value.retry_policy.as_ref().map(retry_policy_to_domain),
    }
}

fn activity_options_to_proto(
    value: &crate::translate::ActivityOptions,
) -> activity_proto::ActivityOptions {
    activity_proto::ActivityOptions {
        task_queue: value.task_queue.as_ref().map(|name| {
            tokeira_proto::conversions::common::task_queue_from_domain(
                &tokeira_types::TaskQueueName(name.clone()),
            )
        }),
        schedule_to_close_timeout: value.schedule_to_close_timeout.map(to_proto_duration),
        schedule_to_start_timeout: value.schedule_to_start_timeout.map(to_proto_duration),
        start_to_close_timeout: value.start_to_close_timeout.map(to_proto_duration),
        heartbeat_timeout: value.heartbeat_timeout.map(to_proto_duration),
        retry_policy: value.retry_policy.as_ref().map(retry_policy_from_domain),
        priority: None,
    }
}

pub fn update_activity_options_to_edge(
    req: workflowservice::UpdateActivityOptionsRequest,
) -> Result<crate::translate::UpdateActivityOptionsRequest, ProtoConversionError> {
    use workflowservice::update_activity_options_request::Activity;
    let execution = req.execution.ok_or(ProtoConversionError::MissingField(
        "UpdateActivityOptionsRequest.execution",
    ))?;
    let target = match req.activity.ok_or(ProtoConversionError::MissingField(
        "UpdateActivityOptionsRequest.activity",
    ))? {
        Activity::Id(activity_id) => crate::translate::ActivityTarget::Id(activity_id),
        Activity::Type(activity_type) => crate::translate::ActivityTarget::Type(activity_type),
        Activity::MatchAll(_) => crate::translate::ActivityTarget::MatchAll,
    };
    Ok(crate::translate::UpdateActivityOptionsRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id,
        run_id: non_empty(execution.run_id),
        identity: req.identity,
        activity_options: req.activity_options.as_ref().map(activity_options_to_edge),
        update_mask: req.update_mask.map(|mask| mask.paths).unwrap_or_default(),
        target,
        restore_original: req.restore_original,
        activity_type: None,
    })
}

pub fn update_activity_options_to_proto(
    resp: crate::translate::UpdateActivityOptionsResponse,
) -> workflowservice::UpdateActivityOptionsResponse {
    workflowservice::UpdateActivityOptionsResponse {
        activity_options: resp
            .activity_options
            .as_ref()
            .map(activity_options_to_proto),
    }
}

// ── Advanced workflow endpoint translations ──

pub fn terminate_request_to_edge(
    req: workflowservice::TerminateWorkflowExecutionRequest,
) -> Result<crate::translate::TerminateWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "TerminateWorkflowExecutionRequest.workflow_execution",
        ))?;
    validate_links(&req.links)?;
    Ok(crate::translate::TerminateWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        details: req.details.as_ref().map(payloads_to_domain),
        identity: req.identity,
        links: links_to_edge(&req.links)?,
    })
}

pub fn terminate_response_to_proto() -> workflowservice::TerminateWorkflowExecutionResponse {
    workflowservice::TerminateWorkflowExecutionResponse {}
}

pub fn pause_request_to_edge(
    req: workflowservice::PauseWorkflowExecutionRequest,
) -> Result<crate::translate::PauseWorkflowExecutionRequest, ProtoConversionError> {
    if req.workflow_id.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "PauseWorkflowExecutionRequest.workflow_id",
        ));
    }
    Ok(crate::translate::PauseWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        identity: req.identity,
        reason: req.reason,
        request_id: non_empty(req.request_id),
    })
}

pub fn pause_response_to_proto() -> workflowservice::PauseWorkflowExecutionResponse {
    workflowservice::PauseWorkflowExecutionResponse {}
}

pub fn unpause_request_to_edge(
    req: workflowservice::UnpauseWorkflowExecutionRequest,
) -> Result<crate::translate::UnpauseWorkflowExecutionRequest, ProtoConversionError> {
    if req.workflow_id.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "UnpauseWorkflowExecutionRequest.workflow_id",
        ));
    }
    Ok(crate::translate::UnpauseWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: req.workflow_id,
        run_id: non_empty(req.run_id),
        identity: req.identity,
        reason: req.reason,
        request_id: non_empty(req.request_id),
    })
}

pub fn unpause_response_to_proto() -> workflowservice::UnpauseWorkflowExecutionResponse {
    workflowservice::UnpauseWorkflowExecutionResponse {}
}

pub fn cancel_request_to_edge(
    req: workflowservice::RequestCancelWorkflowExecutionRequest,
) -> Result<crate::translate::RequestCancelWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "RequestCancelWorkflowExecutionRequest.workflow_execution",
        ))?;
    validate_links(&req.links)?;
    Ok(crate::translate::RequestCancelWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        identity: req.identity,
        links: links_to_edge(&req.links)?,
    })
}

pub fn cancel_response_to_proto() -> workflowservice::RequestCancelWorkflowExecutionResponse {
    workflowservice::RequestCancelWorkflowExecutionResponse {}
}

pub fn query_request_to_edge(
    req: workflowservice::QueryWorkflowRequest,
) -> Result<crate::translate::QueryWorkflowRequest, ProtoConversionError> {
    let execution = req
        .execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "QueryWorkflowRequest.execution",
        ))?;
    let query = req
        .query
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "QueryWorkflowRequest.query",
        ))?;

    Ok(crate::translate::QueryWorkflowRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        query_type: query.query_type.clone(),
        query_args: query
            .query_args
            .as_ref()
            .map(payloads_to_domain)
            .unwrap_or_default(),
        timeout: DEFAULT_QUERY_TIMEOUT,
    })
}

pub fn query_response_to_proto(
    resp: crate::translate::QueryWorkflowResponse,
) -> workflowservice::QueryWorkflowResponse {
    workflowservice::QueryWorkflowResponse {
        query_result: resp.result.map(|p| payloads_from_domain(&p)),
        query_rejected: resp.rejected_status.map(|status| {
            tokeira_proto::public::temporal::api::query::v1::QueryRejected {
                status: execution_status_to_proto(status),
            }
        }),
    }
}

pub fn update_request_to_edge(
    req: workflowservice::UpdateWorkflowExecutionRequest,
) -> Result<crate::translate::UpdateWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.workflow_execution",
        ))?;

    let request = req
        .request
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "UpdateWorkflowExecutionRequest.request",
        ))?;
    let meta = request.meta.as_ref();
    let input_msg = request.input.as_ref();

    let wait_policy = match req.wait_policy {
        Some(wp) => match wp.lifecycle_stage {
            0 => crate::translate::UpdateWaitPolicyDto::Unspecified,
            1 => crate::translate::UpdateWaitPolicyDto::Admitted,
            2 => crate::translate::UpdateWaitPolicyDto::Accepted,
            3 => crate::translate::UpdateWaitPolicyDto::Completed,
            _ => {
                return Err(ProtoConversionError::MissingField(
                    "valid update wait lifecycle_stage",
                ));
            }
        },
        None => crate::translate::UpdateWaitPolicyDto::Unspecified,
    };

    Ok(crate::translate::UpdateWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        first_execution_run_id: non_empty(req.first_execution_run_id),
        update_id: meta.map(|m| m.update_id.clone()).unwrap_or_default(),
        update_name: input_msg.map(|i| i.name.clone()).unwrap_or_default(),
        input: input_msg
            .and_then(|i| i.args.as_ref())
            .map(payloads_to_domain)
            .unwrap_or_default(),
        wait_policy,
        timeout: DEFAULT_UPDATE_TIMEOUT,
    })
}

/// v1.31.0's server-authored outcome for an update that was ACCEPTED but
/// never completed before its workflow closed
/// (`acceptedUpdateCompletedWorkflowFailure`,
/// service/history/workflow/update/errors_failures.go:10-35 @ v1.31.0). The
/// corpus asserts message, source, type, and non-retryability verbatim.
pub(crate) fn accepted_update_completed_workflow_failure() -> failure_proto::Failure {
    failure_proto::Failure {
        message: "Workflow Update failed because the Workflow completed before the Update \
                  completed."
            .to_string(),
        source: "Server".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
            failure_proto::ApplicationFailureInfo {
                r#type: "AcceptedUpdateCompletedWorkflow".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// v1.31.0's server-authored outcome for an update the worker was sent but
/// completed its workflow task without processing — server-side
/// RejectUnprocessed (`unprocessedUpdateFailure`,
/// service/history/workflow/update/errors_failures.go:18-25 @ v1.31.0; spec
/// speculative-wft Req 9). The corpus asserts message, source, type, and
/// non-retryability verbatim (`WorkerSkippedProcessing_RejectByServer`).
pub(crate) fn unprocessed_update_failure() -> failure_proto::Failure {
    failure_proto::Failure {
        message: "Workflow Update is rejected because it wasn't processed by worker. Probably, \
                  Workflow Update is not supported by the worker."
            .to_string(),
        source: "Server".to_string(),
        failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
            failure_proto::ApplicationFailureInfo {
                r#type: "UnprocessedUpdate".to_string(),
                non_retryable: true,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

pub fn update_response_to_proto(
    resp: crate::translate::UpdateWorkflowExecutionResponse,
) -> workflowservice::UpdateWorkflowExecutionResponse {
    use tokeira_proto::public::temporal::api::update::v1 as update;

    let outcome = match resp.outcome {
        Some(crate::translate::UpdateOutcomeDto::Completed { result, .. }) => {
            Some(update::Outcome {
                value: Some(update::outcome::Value::Success(payloads_from_domain(
                    &result,
                ))),
            })
        }
        Some(crate::translate::UpdateOutcomeDto::Rejected { failure, .. }) => {
            Some(update::Outcome {
                value: Some(update::outcome::Value::Failure(payload_to_failure(
                    &failure,
                ))),
            })
        }
        Some(crate::translate::UpdateOutcomeDto::AcceptedRunClosed) => Some(update::Outcome {
            value: Some(update::outcome::Value::Failure(
                accepted_update_completed_workflow_failure(),
            )),
        }),
        Some(crate::translate::UpdateOutcomeDto::RejectedUnprocessed) => Some(update::Outcome {
            value: Some(update::outcome::Value::Failure(unprocessed_update_failure())),
        }),
        None => None,
    };

    workflowservice::UpdateWorkflowExecutionResponse {
        update_ref: Some(update::UpdateRef {
            workflow_execution: Some(proto_common::WorkflowExecution {
                workflow_id: resp.update_ref.workflow_id,
                run_id: resp.update_ref.run_id,
            }),
            update_id: resp.update_ref.update_id,
        }),
        outcome,
        stage: update_lifecycle_stage_to_i32(resp.stage),
    }
}

fn update_lifecycle_stage_to_i32(stage: crate::translate::UpdateLifecycleStageDto) -> i32 {
    match stage {
        crate::translate::UpdateLifecycleStageDto::Unspecified => {
            enums::UpdateWorkflowExecutionLifecycleStage::Unspecified as i32
        }
        crate::translate::UpdateLifecycleStageDto::Admitted => {
            enums::UpdateWorkflowExecutionLifecycleStage::Admitted as i32
        }
        crate::translate::UpdateLifecycleStageDto::Accepted => {
            enums::UpdateWorkflowExecutionLifecycleStage::Accepted as i32
        }
        crate::translate::UpdateLifecycleStageDto::Completed => {
            enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32
        }
    }
}

// ── ExecuteMultiOperation (Update-with-Start) ──

/// Exact v1.31.0 frontend messages for the Update-with-Start gates
/// (`service/frontend/errors.go:81-83` and the `errMultiOp*` / `errUpdate*`
/// constants @ v1.31.0 — the corpus asserts them verbatim).
const MULTI_OP_NOT_START_AND_UPDATE: &str = "Operations have to be exactly [Start, Update].";
const MULTI_OP_ABORTED: &str = "Operation was aborted.";
const MULTI_OP_COULD_NOT_BE_EXECUTED: &str = "Update-with-Start could not be executed.";
const MULTI_OP_NAMESPACE_MISMATCH: &str = "Operation namespace did not match request's namespace.";
const MULTI_OP_START_CRON: &str = "CronSchedule is not allowed.";
const MULTI_OP_START_EAGER: &str = "RequestEagerExecution is not supported.";
const MULTI_OP_START_DELAY: &str = "WorkflowStartDelay is not supported.";
const MULTI_OP_UPDATE_FIRST_EXECUTION_RUN_ID: &str = "FirstExecutionRunId is not allowed.";
const MULTI_OP_UPDATE_RUN_ID: &str = "RunId is not allowed.";
const MULTI_OP_WORKFLOW_ID_INCONSISTENT: &str =
    "WorkflowId is not consistent with previous operation(s).";
const UPDATE_META_NOT_SET: &str = "Update meta is not set on request.";
const UPDATE_INPUT_NOT_SET: &str = "Update input is not set on request.";
const UPDATE_NAME_NOT_SET: &str = "Update name is not set on request.";

/// Pre-mutation validation failure for `ExecuteMultiOperation` (Req 1,
/// Property 1: no runtime call happens once this is returned).
#[derive(Debug, PartialEq, Eq)]
pub enum MultiOperationRequestError {
    /// The operation list is not exactly `[Start, Update]`. Serialized as a
    /// PLAIN `INVALID_ARGUMENT` with NO multi-operation detail — the frontend
    /// shape gate precedes per-operation status assembly
    /// (`workflow_handler.go:718-726 @ v1.31.0`; errors.go:82).
    Shape,
    /// Per-operation validation failed
    /// (`convertToHistoryMultiOperationRequest`, workflow_handler.go:766-785
    /// @ v1.31.0): each failing op carries its own `INVALID_ARGUMENT`
    /// message and a clean sibling aborts. At least one side is `Some`.
    PerOperation {
        start: Option<String>,
        update: Option<String>,
    },
}

/// Translate + validate the full Update-with-Start composition BEFORE any
/// runtime call: the `[Start, Update]` shape gate, per-operation namespace
/// match, the standalone start/update field validations (parity, Req 1.6),
/// the operation-specific prohibitions, and start/update workflow-id
/// consistency (Req 1.1-1.5).
pub fn multi_operation_request_to_edge(
    req: workflowservice::ExecuteMultiOperationRequest,
) -> Result<EdgeExecuteMultiOperationRequest, MultiOperationRequestError> {
    use workflowservice::execute_multi_operation_request::operation::Operation;

    // Shape gate: exactly `[StartWorkflow, UpdateWorkflow]`, in this order
    // (workflow_handler.go:718-726 @ v1.31.0). Matching the oneof
    // exhaustively means any future operation arm falls into the same
    // rejection until it is deliberately mapped.
    let mut operations = req.operations.into_iter();
    let (start_op, update_op) = match (operations.next(), operations.next(), operations.next()) {
        (Some(first), Some(second), None) => match (first.operation, second.operation) {
            (
                Some(Operation::StartWorkflow(start_op)),
                Some(Operation::UpdateWorkflow(update_op)),
            ) => (start_op, update_op),
            _ => return Err(MultiOperationRequestError::Shape),
        },
        _ => return Err(MultiOperationRequestError::Shape),
    };

    let namespace = req.namespace;

    // ── Start leg: namespace gate → standalone start translation → the
    //    three multi-op prohibitions, in the frontend's order
    //    (workflow_handler.go:803-820 @ v1.31.0). ──
    let start_workflow_id = start_op.workflow_id.clone();
    let cron_schedule_set = !start_op.cron_schedule.is_empty();
    let eager_execution_requested = start_op.request_eager_execution;
    // `op.StartWorkflow.WorkflowStartDelay.AsDuration() > 0` — strictly
    // positive; a zero/absent delay passes (workflow_handler.go:817).
    let start_delay_positive = start_op
        .workflow_start_delay
        .as_ref()
        .is_some_and(|delay| delay.seconds > 0 || delay.nanos > 0);

    let mut start_error = None;
    let mut start = None;
    if !start_op.namespace.is_empty() && start_op.namespace != namespace {
        start_error = Some(MULTI_OP_NAMESPACE_MISMATCH.to_owned());
    } else {
        // Standalone `StartWorkflowExecution` translation runs unchanged
        // (field presence, cron-string validation, callback/link limits,
        // conflict/reuse-policy defaulting) so parity is automatic (Req 1.6).
        match start_request_to_edge(start_op) {
            Err(error) => start_error = Some(error.to_string()),
            Ok(mut dto) => {
                if cron_schedule_set {
                    start_error = Some(MULTI_OP_START_CRON.to_owned());
                } else if eager_execution_requested {
                    start_error = Some(MULTI_OP_START_EAGER.to_owned());
                } else if start_delay_positive {
                    start_error = Some(MULTI_OP_START_DELAY.to_owned());
                } else {
                    // The op-level namespace may be empty (only a non-empty
                    // mismatch rejects); the composition always executes in
                    // the request namespace.
                    dto.namespace = namespace.clone();
                    start = Some(dto);
                }
            }
        }
    }

    // ── Update leg: namespace gate → standard update validation (meta/
    //    input/name presence — `errUpdateMetaNotSet`/`errUpdateInputNotSet`/
    //    `errUpdateNameNotSet`, errors.go @ v1.31.0 — plus the ADMITTED
    //    wait-stage rejection standalone update issues) → the two multi-op
    //    prohibitions (workflow_handler.go:833-842) → workflow-id
    //    consistency, reported on the SECOND op
    //    (workflow_handler.go:766-778). ──
    let update_workflow_id = update_op
        .workflow_execution
        .as_ref()
        .map(|execution| execution.workflow_id.clone());
    let update_run_id_set = update_op
        .workflow_execution
        .as_ref()
        .is_some_and(|execution| !execution.run_id.is_empty());
    let first_execution_run_id_set = !update_op.first_execution_run_id.is_empty();
    let update_meta = update_op
        .request
        .as_ref()
        .and_then(|request| request.meta.as_ref());
    let update_identity = update_meta
        .map(|meta| meta.identity.clone())
        .filter(|identity| !identity.is_empty());
    let update_input = update_op
        .request
        .as_ref()
        .and_then(|request| request.input.as_ref());
    let admitted_wait = update_op.wait_policy.as_ref().is_some_and(|policy| {
        policy.lifecycle_stage == enums::UpdateWorkflowExecutionLifecycleStage::Admitted as i32
    });

    let mut update_error = None;
    let mut update = None;
    if !update_op.namespace.is_empty() && update_op.namespace != namespace {
        update_error = Some(MULTI_OP_NAMESPACE_MISMATCH.to_owned());
    } else if update_meta.is_none() {
        update_error = Some(UPDATE_META_NOT_SET.to_owned());
    } else if update_input.is_none() {
        update_error = Some(UPDATE_INPUT_NOT_SET.to_owned());
    } else if update_input.is_some_and(|input| input.name.is_empty()) {
        update_error = Some(UPDATE_NAME_NOT_SET.to_owned());
    } else if admitted_wait {
        // Same rejection standalone update issues for the ADMITTED stage,
        // surfaced pre-mutation here (validate before mutate, Req 2.5).
        update_error =
            Some("UpdateWorkflowExecution does not support waiting for ADMITTED".to_owned());
    } else if first_execution_run_id_set {
        update_error = Some(MULTI_OP_UPDATE_FIRST_EXECUTION_RUN_ID.to_owned());
    } else if update_run_id_set {
        update_error = Some(MULTI_OP_UPDATE_RUN_ID.to_owned());
    } else {
        match update_request_to_edge(update_op) {
            Err(error) => update_error = Some(error.to_string()),
            Ok(mut dto) => {
                if update_workflow_id.as_deref() != Some(start_workflow_id.as_str()) {
                    update_error = Some(MULTI_OP_WORKFLOW_ID_INCONSISTENT.to_owned());
                } else {
                    if dto.update_id.is_empty() {
                        // Mirror standalone update: an empty update id gets a
                        // fresh UUIDv4 at admission.
                        dto.update_id = Uuid::new_v4().to_string();
                    }
                    dto.namespace = namespace.clone();
                    update = Some(dto);
                }
            }
        }
    }

    match (start, update) {
        (Some(start), Some(update)) => Ok(EdgeExecuteMultiOperationRequest {
            namespace,
            start,
            update,
            update_identity,
        }),
        _ => Err(MultiOperationRequestError::PerOperation {
            start: start_error,
            update: update_error,
        }),
    }
}

/// Serialize the ordered `[start_response, update_response]` pair (Req 3;
/// workflow_handler.go:863-895 @ v1.31.0).
pub fn multi_operation_response_to_proto(
    resp: EdgeExecuteMultiOperationResponse,
) -> workflowservice::ExecuteMultiOperationResponse {
    use workflowservice::execute_multi_operation_response::{
        Response, response::Response as ResponseVariant,
    };

    workflowservice::ExecuteMultiOperationResponse {
        responses: vec![
            Response {
                response: Some(ResponseVariant::StartWorkflow(
                    workflowservice::StartWorkflowExecutionResponse {
                        run_id: resp.run_id.0.to_string(),
                        started: resp.started,
                        // `status` reflects the target run's current state —
                        // load-bearing on the dedup/attach/already-completed
                        // paths (proto StartWorkflowExecutionResponse.status
                        // doc; multioperation/api.go @ v1.31.0). No response
                        // link: the multi-op start response carries only
                        // RunId/Started/Status.
                        status: execution_status_to_proto(resp.status),
                        ..Default::default()
                    },
                )),
            },
            Response {
                // The update leg serializes exactly like standalone
                // UpdateWorkflowExecution (outcome mapping incl. the
                // AcceptedRunClosed server-authored failure).
                response: Some(ResponseVariant::UpdateWorkflow(update_response_to_proto(
                    resp.update,
                ))),
            },
        ],
    }
}

/// Serialize a pre-mutation validation failure (Req 1/4): the shape gate is a
/// plain `INVALID_ARGUMENT`; per-operation failures build the structured
/// `MultiOperationExecutionFailure` with the clean sibling aborted.
pub fn multi_operation_request_error_to_status(error: MultiOperationRequestError) -> Status {
    match error {
        MultiOperationRequestError::Shape => {
            Status::invalid_argument(MULTI_OP_NOT_START_AND_UPDATE)
        }
        MultiOperationRequestError::PerOperation { start, update } => {
            let leg = |message: Option<String>| match message {
                Some(message) => MultiOperationLeg::Failed(Status::invalid_argument(message)),
                None => MultiOperationLeg::Aborted,
            };
            multi_operation_execution_failure_status([leg(start), leg(update)])
        }
    }
}

/// Serialize a post-validation leg failure (Req 4): the failing op carries the
/// SAME status its standalone RPC would produce (typed details included); the
/// sibling aborts — except a start leg that already durably applied, which
/// serializes as code OK (`MultiOperationError::UpdateFailed{started}` in the
/// runtime; multioperation/api.go @ v1.31.0).
pub fn multi_operation_failure_to_status(failure: MultiOperationFailure) -> Status {
    match failure {
        MultiOperationFailure::Start(error) => multi_operation_execution_failure_status([
            MultiOperationLeg::Failed(error.into()),
            MultiOperationLeg::Aborted,
        ]),
        MultiOperationFailure::Update { started, error } => {
            let start_leg = if started {
                MultiOperationLeg::Ok
            } else {
                MultiOperationLeg::Aborted
            };
            multi_operation_execution_failure_status([
                start_leg,
                MultiOperationLeg::Failed(error.into()),
            ])
        }
    }
}

/// One leg of a failed composition, pre-serialization. Tracking the sibling
/// abort structurally (rather than by comparing codes) keeps a genuine
/// `Aborted` failure on the failing op distinguishable from the sibling.
enum MultiOperationLeg {
    /// The op that actually failed, carrying its standalone gRPC status.
    Failed(Status),
    /// The non-failing sibling: code `Aborted`, message "Operation was
    /// aborted.", one `MultiOperationExecutionAborted` detail
    /// (proto failure/v1/message.proto; errors.go:83 @ v1.31.0).
    Aborted,
    /// A start leg that durably applied before the update leg failed:
    /// code OK, empty message, no details.
    Ok,
}

/// Assemble the top-level gRPC status: code = the FIRST op that actually
/// failed (`service.proto:116-117 @ v1.31.0`), message "Update-with-Start
/// could not be executed.", and ONE `MultiOperationExecutionFailure` detail
/// carrying one `OperationStatus` per op in request order, riding the encoded
/// `google.rpc.Status` trailer exactly like [`workflow_not_ready_status`]
/// (crate::grpc::errors, errors.rs:146).
fn multi_operation_execution_failure_status(legs: [MultiOperationLeg; 2]) -> Status {
    let top_code = legs
        .iter()
        .find_map(|leg| match leg {
            MultiOperationLeg::Failed(status) => Some(status.code()),
            _ => None,
        })
        // Unreachable by construction (every caller supplies a Failed leg);
        // Internal keeps the fallback honest rather than minting a fake OK.
        .unwrap_or(Code::Internal);

    let statuses = legs
        .iter()
        .map(|leg| match leg {
            MultiOperationLeg::Failed(status) => {
                errordetails_proto::multi_operation_execution_failure::OperationStatus {
                    code: status.code() as i32,
                    message: status.message().to_owned(),
                    // The failing op's own typed details (e.g. the
                    // WorkflowExecutionAlreadyStartedFailure Any) ride inside
                    // its encoded google.rpc.Status trailer; re-parent them
                    // onto the per-operation status (Req 4.2, 4.6).
                    details: RpcStatus::decode(status.details())
                        .map(|rpc_status| rpc_status.details)
                        .unwrap_or_default(),
                }
            }
            MultiOperationLeg::Aborted => {
                errordetails_proto::multi_operation_execution_failure::OperationStatus {
                    code: Code::Aborted as i32,
                    message: MULTI_OP_ABORTED.to_owned(),
                    details: vec![prost_types::Any {
                        type_url:
                            "type.googleapis.com/temporal.api.failure.v1.MultiOperationExecutionAborted"
                                .to_owned(),
                        value: failure_proto::MultiOperationExecutionAborted {}.encode_to_vec(),
                    }],
                }
            }
            MultiOperationLeg::Ok => {
                errordetails_proto::multi_operation_execution_failure::OperationStatus {
                    code: Code::Ok as i32,
                    message: String::new(),
                    details: Vec::new(),
                }
            }
        })
        .collect();

    let failure_any = prost_types::Any {
        type_url: "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure"
            .to_owned(),
        value: errordetails_proto::MultiOperationExecutionFailure { statuses }.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: top_code as i32,
        message: MULTI_OP_COULD_NOT_BE_EXECUTED.to_owned(),
        details: vec![failure_any],
    };
    Status::with_details_and_metadata(
        top_code,
        MULTI_OP_COULD_NOT_BE_EXECUTED,
        rpc_status.encode_to_vec().into(),
        MetadataMap::new(),
    )
}

/// Minimal `google.rpc.Status` mirror for the `grpc-status-details-bin`
/// trailer — same rationale as the mirror in `grpc/errors.rs:146` (no
/// temporal proto pulls `google/rpc/status.proto` into the build), but
/// carrying `prost_types::Any` details directly because per-operation details
/// are re-parented from the failing leg's own encoded status.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        grpc::errors::proto_conversion_status, operator_service::ClusterInfo,
        translate::NamespaceDescription,
    };
    use proptest::prelude::*;
    use tokeira_kernel::state::WorkflowVersioningInfo;
    use tokeira_proto::public::temporal::api::{filter::v1 as filter, taskqueue::v1 as taskqueue};
    use tokeira_types::RunKey;

    #[test]
    fn execution_config_preserves_start_user_metadata() {
        let summary = tokeira_types::Payload {
            data: b"summary".to_vec(),
            metadata: [("encoding".to_owned(), "binary/plain".to_owned())]
                .into_iter()
                .collect(),
            external_payloads: Vec::new(),
        };
        let details = tokeira_types::Payload {
            data: b"details".to_vec(),
            metadata: [("encoding".to_owned(), "binary/plain".to_owned())]
                .into_iter()
                .collect(),
            external_payloads: Vec::new(),
        };
        let config = crate::translate::ExecutionConfigDescription {
            task_queue: "queue".to_owned(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            default_workflow_task_timeout: time::Duration::seconds(10),
            user_metadata: Some(crate::translate::UserMetadata {
                summary: Some(summary.clone()),
                details: Some(details.clone()),
            }),
        };

        let proto = execution_config_to_proto(&config);
        let metadata = proto.user_metadata.expect("metadata must be present");
        assert_eq!(metadata.summary, Some(payload_from_domain(&summary)));
        assert_eq!(metadata.details, Some(payload_from_domain(&details)));
    }

    #[test]
    fn workflow_timeout_zero_maps_to_unlimited() {
        // v1.31.0 treats a zero execution/run timeout as no-timeout (no timer is
        // generated — `service/history/workflow/task_generator.go:153-185 @ v1.31.0`).
        // The SDK encodes an unset timeout as `0s`, so a `Some(0)` proto duration MUST
        // become `None`, or the workflow timeout scanner reaps an idle workflow as an
        // immediately-due deadline. A positive duration is preserved; absent stays None.
        assert_eq!(
            workflow_timeout_to_time(Some(&prost_types::Duration {
                seconds: 0,
                nanos: 0
            })),
            None
        );
        assert_eq!(workflow_timeout_to_time(None), None);
        assert_eq!(
            workflow_timeout_to_time(Some(&prost_types::Duration {
                seconds: 60,
                nanos: 0
            })),
            Some(time::Duration::seconds(60))
        );
    }

    /// A `CompletionCallback` driven past `Standby` (one failed delivery attempt, now
    /// backing off) surfaces its lifecycle on `CallbackInfo`: the `BackingOff` state, the
    /// attempt count, the recorded failure, and the next-retry time the kernel tracks. This
    /// is the read-side of the async-completion callback lifecycle (Wave 5, P7).
    #[test]
    fn backing_off_completion_callback_surfaces_attempt_state_and_failure() {
        let next_attempt =
            time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let callback = KernelCompletionCallback {
            spec: KernelCallbackSpec::Nexus {
                url: "temporal://system".to_string(),
                header: std::collections::BTreeMap::new(),
            },
            links: Vec::new(),
            trigger: KernelCallbackTrigger::WorkflowClosed,
            registration_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            state: KernelCallbackState::BackingOff,
            attempt: 2,
            last_attempt_failure: Some(tokeira_types::Payload {
                data: b"delivery refused".to_vec(),
                metadata: std::collections::BTreeMap::new(),
                external_payloads: Vec::new(),
            }),
            next_attempt_at: Some(next_attempt),
        };

        let info = workflow_callback_info_to_proto(&callback);

        assert_eq!(info.state, enums::CallbackState::BackingOff as i32);
        assert_eq!(info.attempt, 2);
        assert!(
            info.last_attempt_failure.is_some(),
            "a recorded delivery failure is surfaced"
        );
        assert!(
            info.next_attempt_schedule_time.is_some(),
            "a backing-off callback surfaces its next retry time"
        );
    }

    /// A succeeded callback surfaces the terminal state and carries no failure / no pending
    /// retry, even though the attempt counter is non-zero.
    #[test]
    fn succeeded_completion_callback_surfaces_terminal_state() {
        let callback = KernelCompletionCallback {
            spec: KernelCallbackSpec::Nexus {
                url: "temporal://system".to_string(),
                header: std::collections::BTreeMap::new(),
            },
            links: Vec::new(),
            trigger: KernelCallbackTrigger::WorkflowClosed,
            registration_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            state: KernelCallbackState::Succeeded,
            attempt: 1,
            last_attempt_failure: None,
            next_attempt_at: None,
        };

        let info = workflow_callback_info_to_proto(&callback);

        assert_eq!(info.state, enums::CallbackState::Succeeded as i32);
        assert_eq!(info.attempt, 1);
        assert!(info.last_attempt_failure.is_none());
        assert!(info.next_attempt_schedule_time.is_none());
    }

    /// The six kernel callback states map 1:1 to the wire enum, in the v1.31.0 order
    /// (`enums::CallbackState`): Standby=1, Scheduled=2, BackingOff=3, Failed=4,
    /// Succeeded=5, Blocked=6.
    #[test]
    fn callback_state_maps_one_to_one() {
        use enums::CallbackState as P;
        let cases = [
            (KernelCallbackState::Standby, P::Standby),
            (KernelCallbackState::Scheduled, P::Scheduled),
            (KernelCallbackState::BackingOff, P::BackingOff),
            (KernelCallbackState::Failed, P::Failed),
            (KernelCallbackState::Succeeded, P::Succeeded),
            (KernelCallbackState::Blocked, P::Blocked),
        ];
        for (kernel, proto) in cases {
            assert_eq!(kernel_callback_state_to_proto(&kernel), proto);
        }
    }

    /// api-conformance-task-queue Property 3: `ListTaskQueuePartitions` validates the
    /// namespace, task queue, and kind enum before any runtime lookup; all failures are
    /// `INVALID_ARGUMENT` (the conversion error → [`proto_conversion_status`]).
    #[test]
    fn list_task_queue_partitions_validates_before_lookup() {
        fn req(
            namespace: &str,
            task_queue: Option<(&str, i32)>,
        ) -> workflowservice::ListTaskQueuePartitionsRequest {
            workflowservice::ListTaskQueuePartitionsRequest {
                namespace: namespace.to_string(),
                task_queue: task_queue.map(|(name, kind)| taskqueue_proto::TaskQueue {
                    name: name.to_string(),
                    kind,
                    ..Default::default()
                }),
            }
        }

        let normal = enums::TaskQueueKind::Normal as i32;
        // Empty namespace, absent task queue, empty name, and an unrecognized kind all reject.
        assert!(list_task_queue_partitions_request_to_edge(req("", Some(("q", normal)))).is_err());
        assert!(list_task_queue_partitions_request_to_edge(req("ns", None)).is_err());
        assert!(list_task_queue_partitions_request_to_edge(req("ns", Some(("", normal)))).is_err());
        assert!(
            list_task_queue_partitions_request_to_edge(req("ns", Some(("q", 9_999)))).is_err(),
            "an unrecognized task-queue kind enum is rejected"
        );

        // NORMAL and STICKY are both accepted; the queue name is carried through.
        let ok = list_task_queue_partitions_request_to_edge(req("ns", Some(("q", normal))))
            .expect("a valid request converts");
        assert_eq!(ok.namespace, "ns");
        assert_eq!(ok.task_queue, "q");
        assert!(
            list_task_queue_partitions_request_to_edge(req(
                "ns",
                Some(("q", enums::TaskQueueKind::Sticky as i32))
            ))
            .is_ok()
        );
    }

    /// api-conformance-workflow-options: the `update_mask` is validated + reduced to the
    /// supported `versioning_override` change (Property 1 / 3). Empty mask, unsupported
    /// option (`priority`), a half-masked deprecated sub-field, and a missing execution all
    /// reject; a masked Pinned override is `Set`, a masked-but-absent override is `Clear`.
    #[test]
    fn update_workflow_execution_options_request_validation() {
        use tokeira_proto::public::temporal::api::{
            common::v1 as common, deployment::v1 as deployment, workflow::v1 as workflow,
        };
        fn req(
            options: Option<workflow::WorkflowExecutionOptions>,
            paths: &[&str],
            workflow_id: &str,
        ) -> workflowservice::UpdateWorkflowExecutionOptionsRequest {
            workflowservice::UpdateWorkflowExecutionOptionsRequest {
                namespace: "ns".to_string(),
                workflow_execution: (!workflow_id.is_empty()).then(|| common::WorkflowExecution {
                    workflow_id: workflow_id.to_string(),
                    run_id: String::new(),
                }),
                workflow_execution_options: options,
                update_mask: Some(prost_types::FieldMask {
                    paths: paths.iter().map(|p| p.to_string()).collect(),
                }),
                identity: "id".to_string(),
            }
        }

        // Missing execution, empty mask, unsupported option, half-masked sub-field → reject.
        assert!(
            update_workflow_execution_options_request_to_edge(req(
                None,
                &["versioning_override"],
                ""
            ))
            .is_err()
        );
        assert!(
            update_workflow_execution_options_request_to_edge(
                workflowservice::UpdateWorkflowExecutionOptionsRequest {
                    namespace: "ns".to_string(),
                    workflow_execution: Some(common::WorkflowExecution {
                        workflow_id: "w".to_string(),
                        run_id: String::new(),
                    }),
                    workflow_execution_options: None,
                    update_mask: None,
                    identity: String::new(),
                }
            )
            .is_err(),
            "empty mask is rejected"
        );
        assert!(
            update_workflow_execution_options_request_to_edge(req(None, &["priority"], "w"))
                .is_err(),
            "an unsupported option field is rejected"
        );
        assert!(
            update_workflow_execution_options_request_to_edge(req(
                None,
                &["versioning_override.behavior"],
                "w"
            ))
            .is_err(),
            "deprecated versioning_override sub-fields must be masked together"
        );

        // A masked Pinned override → Set; a masked-but-absent override → Clear.
        let pinned = workflow::WorkflowExecutionOptions {
            versioning_override: Some(workflow::VersioningOverride {
                behavior: enums::VersioningBehavior::Pinned as i32,
                deployment: Some(deployment::Deployment {
                    series_name: "series".to_string(),
                    build_id: "build".to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let edge = update_workflow_execution_options_request_to_edge(req(
            Some(pinned),
            &["versioning_override"],
            "w",
        ))
        .expect("valid Pinned set");
        assert_eq!(
            edge.versioning_override,
            VersioningOverrideChange::Set(VersioningOverride::Pinned {
                deployment_series: "series".to_string(),
                build_id: "build".to_string(),
            })
        );
        let edge = update_workflow_execution_options_request_to_edge(req(
            None,
            &["versioning_override"],
            "w",
        ))
        .expect("valid clear");
        assert_eq!(edge.versioning_override, VersioningOverrideChange::Clear);

        // Response projection echoes the post-update override.
        let proto = update_workflow_execution_options_response_to_proto(
            EdgeUpdateWorkflowExecutionOptionsResponse {
                versioning_override: Some(VersioningOverride::AutoUpgrade),
            },
        );
        assert!(
            proto
                .workflow_execution_options
                .and_then(|options| options.versioning_override)
                .is_some()
        );
    }

    #[test]
    fn activity_status_keyword_maps_to_wire_enum() {
        use enums::ActivityExecutionStatus as P;
        // Collapsed non-terminal -> RUNNING; terminals 1:1; unknown -> UNSPECIFIED.
        assert_eq!(activity_status_to_proto("Running"), P::Running as i32);
        assert_eq!(activity_status_to_proto("Completed"), P::Completed as i32);
        assert_eq!(activity_status_to_proto("Failed"), P::Failed as i32);
        assert_eq!(activity_status_to_proto("Canceled"), P::Canceled as i32);
        assert_eq!(activity_status_to_proto("Terminated"), P::Terminated as i32);
        assert_eq!(activity_status_to_proto("TimedOut"), P::TimedOut as i32);
        assert_eq!(activity_status_to_proto("nonsense"), P::Unspecified as i32);
    }

    #[test]
    fn activity_summary_translates_to_list_info() {
        let schedule = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let close = time::OffsetDateTime::from_unix_timestamp(160).unwrap();
        let summary = ActivityExecutionSummary {
            namespace: "ns".to_string(),
            activity_id: "act-1".to_string(),
            run_id: RunId(Uuid::from_u128(9)),
            activity_type: "MyActivity".to_string(),
            task_queue: "tq".to_string(),
            status_keyword: "Completed".to_string(),
            schedule_time: Some(schedule),
            close_time: Some(close),
            state_transition_count: 7,
            state_size_bytes: 0,
            execution_duration: None,
            search_attributes: Default::default(),
        };
        let info = activity_execution_list_info_from_summary(summary);
        assert_eq!(info.activity_id, "act-1");
        assert_eq!(info.run_id, Uuid::from_u128(9).to_string());
        assert_eq!(info.activity_type.unwrap().name, "MyActivity");
        assert_eq!(info.task_queue, "tq");
        assert_eq!(info.state_transition_count, 7); // generic transition_count (Req 10.14)
        assert_eq!(
            info.status,
            enums::ActivityExecutionStatus::Completed as i32
        );
        // execution_duration is derived as close - schedule (60s), only when closed.
        assert_eq!(info.execution_duration.unwrap().seconds, 60);
        assert!(info.schedule_time.is_some() && info.close_time.is_some());
    }

    #[test]
    fn validate_client_cron_schedule_matches_v131_messages() {
        // Descriptors the cron suite relies on must be accepted (tests/cron_test.go
        // uses "@every 5s"/"@every 3s"); "@midnight" is a robfig `ParseStandard`
        // alias; a plain 5-field spec is the standard case.
        for ok in ["@every 5s", "@midnight", "0 * * * *"] {
            validate_client_cron_schedule(Some(ok))
                .unwrap_or_else(|err| panic!("expected {ok:?} to be accepted, got {err:?}"));
        }
        // No cron requested is accepted.
        validate_client_cron_schedule(None).unwrap();

        // An unparseable cron is rejected with the verbatim v1.31.0 message and
        // gRPC InvalidArgument — not the old "missing required field" masking
        // (`backoff.ValidateSchedule @ v1.31.0`).
        let err = validate_client_cron_schedule(Some("not-a-cron"))
            .expect_err("invalid cron should be rejected");
        let ProtoConversionError::InvalidArgument(ref message) = err else {
            panic!("expected InvalidArgument, got {err:?}");
        };
        assert_eq!(message, "invalid CronSchedule.");
        let status = proto_conversion_status(err);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "invalid CronSchedule.");

        // A parseable-but-unsatisfiable cron carries the longer v1.31.0 message.
        let err = validate_client_cron_schedule(Some("0 0 31 2 *"))
            .expect_err("unsatisfiable cron should be rejected");
        let ProtoConversionError::InvalidArgument(message) = err else {
            panic!("expected InvalidArgument, got {err:?}");
        };
        assert_eq!(
            message,
            "invalid CronSchedule, no time can be found to satisfy the schedule"
        );
    }

    #[test]
    fn start_request_validates_links() {
        fn workflow_event_link(ns: &str, wid: &str, rid: &str) -> proto_common::Link {
            proto_common::Link {
                variant: Some(proto_common::link::Variant::WorkflowEvent(
                    proto_common::link::WorkflowEvent {
                        namespace: ns.to_string(),
                        workflow_id: wid.to_string(),
                        run_id: rid.to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }
        }

        // A well-formed WorkflowEvent link is accepted.
        let mut req = minimal_start_proto();
        req.links = vec![workflow_event_link("ns", "wid", "rid")];
        start_request_to_edge(req).expect("a valid workflow-event link is accepted");

        // Count cap (10): the 11th link is rejected with the verbatim message.
        let mut req = minimal_start_proto();
        req.links = vec![workflow_event_link("ns", "wid", "rid"); MAX_LINKS_PER_REQUEST + 1];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "cannot attach more than 10 links per request, got 11"
        );

        // WorkflowEvent identity fields are required.
        let mut req = minimal_start_proto();
        req.links = vec![workflow_event_link("ns", "wid", "")];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(
            status.message(),
            "workflow event link must not have an empty run ID field"
        );

        // BatchJob job ID is required.
        let mut req = minimal_start_proto();
        req.links = vec![proto_common::Link {
            variant: Some(proto_common::link::Variant::BatchJob(
                proto_common::link::BatchJob::default(),
            )),
            ..Default::default()
        }];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(
            status.message(),
            "batch job link must not have an empty job ID"
        );

        // Activity links are an unsupported variant on the start path (v1.31.0
        // admits only WorkflowEvent and BatchJob).
        let mut req = minimal_start_proto();
        req.links = vec![proto_common::Link {
            variant: Some(proto_common::link::Variant::Activity(
                proto_common::link::Activity::default(),
            )),
            ..Default::default()
        }];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.message(), "unsupported link variant");

        // An event ref that names an id but not a type is rejected.
        let mut req = minimal_start_proto();
        req.links = vec![proto_common::Link {
            variant: Some(proto_common::link::Variant::WorkflowEvent(
                proto_common::link::WorkflowEvent {
                    namespace: "ns".to_string(),
                    workflow_id: "wid".to_string(),
                    run_id: "rid".to_string(),
                    reference: Some(proto_common::link::workflow_event::Reference::EventRef(
                        proto_common::link::workflow_event::EventReference {
                            event_id: 5,
                            event_type: 0,
                            ..Default::default()
                        },
                    )),
                },
            )),
            ..Default::default()
        }];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(
            status.message(),
            "workflow event link ref cannot have an unspecified event type and a non-zero event ID"
        );
    }

    #[test]
    fn signal_cancel_terminate_paths_validate_links() {
        // The same v1.31.0 link admission runs on Signal / RequestCancel /
        // Terminate / SignalWithStart (`workflow_handler.go:2183,2228,2356,2433 @
        // v1.31.0`), each over the request's own links (no callback combination).
        let unsupported = || proto_common::Link {
            variant: Some(proto_common::link::Variant::Activity(
                proto_common::link::Activity::default(),
            )),
            ..Default::default()
        };
        let execution = || {
            Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wid".to_string(),
                run_id: String::new(),
            })
        };

        let signal = workflowservice::SignalWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: execution(),
            signal_name: "sig".to_string(),
            links: vec![unsupported()],
            ..Default::default()
        };
        assert_eq!(
            proto_conversion_status(signal_request_to_edge(signal).unwrap_err()).message(),
            "unsupported link variant"
        );

        let cancel = workflowservice::RequestCancelWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: execution(),
            links: vec![unsupported()],
            ..Default::default()
        };
        assert_eq!(
            proto_conversion_status(cancel_request_to_edge(cancel).unwrap_err()).message(),
            "unsupported link variant"
        );

        let terminate = workflowservice::TerminateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: execution(),
            links: vec![unsupported()],
            ..Default::default()
        };
        assert_eq!(
            proto_conversion_status(terminate_request_to_edge(terminate).unwrap_err()).message(),
            "unsupported link variant"
        );

        let signal_with_start = workflowservice::SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "wid".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "wt".to_string(),
            }),
            task_queue: Some(taskqueue::TaskQueue {
                name: "q".to_string(),
                ..Default::default()
            }),
            signal_name: "sig".to_string(),
            links: vec![unsupported()],
            ..Default::default()
        };
        assert_eq!(
            proto_conversion_status(
                signal_with_start_request_to_edge(signal_with_start).unwrap_err()
            )
            .message(),
            "unsupported link variant"
        );
    }

    #[test]
    fn validate_deployment_name_matches_v131_messages_and_order() {
        // Messages and order ground-truthed to v1.31.0: empty (bespoke,
        // service/frontend/workflow_handler.go:4154) precedes the shared field
        // validator's length / '.' / ':' / '__' checks
        // (common/worker_versioning/worker_versioning.go:555). The corpus asserts
        // on these strings via Contains
        // (tests/worker_deployment_test.go TestCreateWorkerDeployment_InvalidDeploymentName).
        let cases = [
            ("", "deployment name cannot be empty"),
            ("a.b", "worker deployment name cannot contain '.'"),
            ("a:b", "worker deployment name cannot contain ':'"),
            ("__reserved", "WorkerDeploymentName cannot start with '__'"),
        ];
        for (name, expected) in cases {
            let err = validate_deployment_name(name).expect_err("name should be rejected");
            let ProtoConversionError::InvalidArgument(message) = err else {
                panic!("expected InvalidArgument, got {err:?}");
            };
            assert_eq!(message, expected, "message mismatch for {name:?}");
        }

        let too_long = "a".repeat(WORKER_DEPLOYMENT_NAME_MAX_LEN + 1);
        let err = validate_deployment_name(&too_long).expect_err("over-long name should reject");
        let ProtoConversionError::InvalidArgument(message) = err else {
            panic!("expected InvalidArgument, got {err:?}");
        };
        assert_eq!(
            message,
            "size of WorkerDeploymentName larger than the maximum allowed"
        );

        validate_deployment_name(&"a".repeat(WORKER_DEPLOYMENT_NAME_MAX_LEN))
            .expect("max-length name is accepted");
        validate_deployment_name("prod-deployment").expect("ordinary name is accepted");
    }

    #[test]
    fn start_request_preserves_eager_worker_deployment_options() {
        let edge = start_request_to_edge(workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "workflow-type".to_string(),
            }),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            request_eager_execution: true,
            eager_worker_deployment_options: Some(deployment_proto::WorkerDeploymentOptions {
                deployment_name: "eager-deployment".to_string(),
                build_id: "eager-build".to_string(),
                worker_versioning_mode: enums::WorkerVersioningMode::Versioned as i32,
            }),
            ..Default::default()
        })
        .expect("start request should convert");

        assert_eq!(
            edge.eager_worker_deployment_options,
            Some(WorkerDeploymentVersionRef {
                deployment_name: "eager-deployment".to_string(),
                build_id: "eager-build".to_string(),
            })
        );
    }

    fn minimal_start_proto() -> workflowservice::StartWorkflowExecutionRequest {
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "workflow-type".to_string(),
            }),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn nexus_callback(url: &str, header: &[(&str, &str)]) -> proto_common::Callback {
        proto_common::Callback {
            variant: Some(proto_common::callback::Variant::Nexus(
                proto_common::callback::Nexus {
                    url: url.to_string(),
                    header: header
                        .iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                },
            )),
            links: Vec::new(),
        }
    }

    fn minimal_signal_with_start_proto() -> workflowservice::SignalWithStartWorkflowExecutionRequest
    {
        workflowservice::SignalWithStartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: "workflow-a".to_string(),
            workflow_type: Some(tokeira_proto::common::WorkflowType {
                name: "workflow-type".to_string(),
            }),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            signal_name: "signal".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn start_request_rejects_internal_only_and_test_server_fields() {
        let mut req = minimal_start_proto();
        req.continued_failure = Some(failure_proto::Failure::default());
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_start_proto();
        req.last_completion_result = Some(proto_common::Payloads::default());
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_start_proto();
        req.time_skipping_config = Some(workflow::TimeSkippingConfig {
            enabled: true,
            ..Default::default()
        });
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn start_request_validates_delay_and_on_conflict_options() {
        let mut req = minimal_start_proto();
        req.cron_schedule = "*/5 * * * *".to_string();
        req.workflow_start_delay = Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        });
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_start_proto();
        req.workflow_start_delay = Some(prost_types::Duration {
            seconds: -1,
            nanos: 0,
        });
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_start_proto();
        req.cron_schedule = "invalid-cron-spec".to_string();
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_start_proto();
        req.cron_schedule = "@every 5s".to_string();
        assert!(start_request_to_edge(req).is_ok());

        let mut req = minimal_start_proto();
        req.on_conflict_options = Some(workflow::OnConflictOptions {
            attach_request_id: false,
            attach_completion_callbacks: true,
            attach_links: false,
        });
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn start_request_validates_completion_callbacks() {
        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![nexus_callback("ftp://callback.example/run", &[])];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "invalid url: unknown scheme: ftp://callback.example/run"
        );

        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![nexus_callback("https://", &[])];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "invalid url: missing host");

        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![nexus_callback(
            &format!("https://{}.example", "a".repeat(CALLBACK_URL_MAX_LENGTH)),
            &[],
        )];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "invalid url: url length longer than max length allowed of 1000"
        );

        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![nexus_callback(
            "https://callback.example/run",
            &[("x", &"v".repeat(CALLBACK_HEADER_MAX_SIZE))],
        )];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "invalid header: header size longer than max allowed size of 8192"
        );

        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![
            nexus_callback("https://callback.example/run", &[]);
            MAX_CALLBACKS_PER_WORKFLOW + 1
        ];
        let status = proto_conversion_status(start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "cannot attach more than 32 callbacks to a workflow"
        );
    }

    #[test]
    fn start_request_lowercases_completion_callback_headers() {
        let mut req = minimal_start_proto();
        req.completion_callbacks = vec![nexus_callback(
            "https://callback.example/run",
            &[("X-Tokeira", "value")],
        )];

        let edge = start_request_to_edge(req).expect("callback should convert");

        assert_eq!(
            edge.completion_callbacks[0].header.get("x-tokeira"),
            Some(&"value".to_string())
        );
        assert!(
            !edge.completion_callbacks[0]
                .header
                .contains_key("X-Tokeira")
        );
    }

    #[test]
    fn legacy_open_visibility_translates_to_running_query() {
        let req = workflowservice::ListOpenWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            maximum_page_size: 25,
            next_page_token: b"page".to_vec(),
            start_time_filter: Some(filter::StartTimeFilter {
                earliest_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                latest_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_060,
                    nanos: 0,
                }),
            }),
            filters: Some(
                workflowservice::list_open_workflow_executions_request::Filters::TypeFilter(
                    filter::WorkflowTypeFilter {
                        name: "ExampleWorkflow".to_string(),
                    },
                ),
            ),
        };

        let edge = list_open_request_to_edge(req).expect("legacy open list");

        assert_eq!(edge.namespace, "default");
        assert_eq!(edge.page_size, 25);
        assert_eq!(edge.next_page_token.as_deref(), Some("page"));
        let query = edge.query.expect("query");
        assert!(query.contains("ExecutionStatus = 'Running'"));
        assert!(query.contains("StartTime BETWEEN"));
        assert!(query.contains("WorkflowType = 'ExampleWorkflow'"));
    }

    #[test]
    fn legacy_closed_visibility_rejects_running_status_filter() {
        let req = workflowservice::ListClosedWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            filters: Some(
                workflowservice::list_closed_workflow_executions_request::Filters::StatusFilter(
                    filter::StatusFilter {
                        status: enums::WorkflowExecutionStatus::Running as i32,
                    },
                ),
            ),
            ..Default::default()
        };

        let status = proto_conversion_status(list_closed_request_to_edge(req).unwrap_err());

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "StatusFilter must be specified and must be not Running."
        );
    }

    #[test]
    fn legacy_closed_visibility_maps_status_filter_to_closed_query() {
        let req = workflowservice::ListClosedWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            filters: Some(
                workflowservice::list_closed_workflow_executions_request::Filters::StatusFilter(
                    filter::StatusFilter {
                        status: enums::WorkflowExecutionStatus::Canceled as i32,
                    },
                ),
            ),
            ..Default::default()
        };

        let edge = list_closed_request_to_edge(req).expect("legacy closed list");

        let query = edge.query.expect("query");
        assert!(query.contains("ExecutionStatus != 'Running'"));
        assert!(query.contains("ExecutionStatus = 'Cancelled'"));
    }

    #[test]
    fn archived_and_scan_visibility_wrap_modern_queries() {
        let archived =
            list_archived_request_to_edge(workflowservice::ListArchivedWorkflowExecutionsRequest {
                namespace: "default".to_string(),
                page_size: 12,
                next_page_token: b"archived-page".to_vec(),
                query: "WorkflowType = 'A'".to_string(),
            })
            .expect("archived wrapper");
        let scanned = scan_request_to_edge(workflowservice::ScanWorkflowExecutionsRequest {
            namespace: "default".to_string(),
            page_size: 13,
            next_page_token: b"scan-page".to_vec(),
            query: "WorkflowType = 'B'".to_string(),
        })
        .expect("scan wrapper");

        assert_eq!(archived.query.as_deref(), Some("WorkflowType = 'A'"));
        assert_eq!(archived.next_page_token.as_deref(), Some("archived-page"));
        assert_eq!(scanned.query.as_deref(), Some("WorkflowType = 'B'"));
        assert_eq!(scanned.next_page_token.as_deref(), Some("scan-page"));
    }

    #[test]
    fn signal_with_start_rejects_fail_conflict_policy_and_time_skipping() {
        let mut req = minimal_signal_with_start_proto();
        req.workflow_id_conflict_policy = enums::WorkflowIdConflictPolicy::Fail as i32;
        let status = proto_conversion_status(signal_with_start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_signal_with_start_proto();
        req.time_skipping_config = Some(workflow::TimeSkippingConfig {
            enabled: true,
            ..Default::default()
        });
        let status = proto_conversion_status(signal_with_start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_signal_with_start_proto();
        req.cron_schedule = "invalid-cron-spec".to_string();
        let status = proto_conversion_status(signal_with_start_request_to_edge(req).unwrap_err());
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut req = minimal_signal_with_start_proto();
        req.cron_schedule = "@every 3s".to_string();
        assert!(signal_with_start_request_to_edge(req).is_ok());
    }

    #[test]
    fn poll_request_applies_default_timeout_and_sticky_ttl() {
        let req = workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        };

        let edge = poll_request_to_edge(req).expect("poll request should convert");
        assert_eq!(edge.timeout, Duration::from_secs(60));
        assert_eq!(edge.sticky_ttl, Duration::from_secs(30));
    }

    #[test]
    fn poll_request_maps_empty_version_fields_to_none() {
        let edge = poll_request_to_edge(workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        })
        .expect("poll request should convert");

        assert_eq!(edge.deployment, None);
        assert_eq!(edge.build_id, None);
    }

    #[test]
    fn poll_request_preserves_worker_version_capabilities() {
        let edge = poll_request_to_edge(workflowservice::PollWorkflowTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            worker_version_capabilities: Some(
                tokeira_proto::public::temporal::api::common::v1::WorkerVersionCapabilities {
                    build_id: "build-a".to_string(),
                    use_versioning: true,
                    deployment_series_name: "deploy-a".to_string(),
                },
            ),
            ..Default::default()
        })
        .expect("poll request should convert");

        assert_eq!(edge.deployment, Some(DeploymentId("deploy-a".to_string())));
        assert_eq!(edge.build_id, Some(BuildId("build-a".to_string())));
    }

    #[test]
    fn respond_completed_request_decodes_speculative_capability() {
        let edge = respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                capabilities: Some(
                    workflowservice::respond_workflow_task_completed_request::Capabilities {
                        discard_speculative_workflow_task_with_events: true,
                    },
                ),
                ..Default::default()
            },
        )
        .expect("respond completed request should convert");

        assert!(edge.client_discards_speculative_with_events);
    }

    #[test]
    fn workflow_task_upserts_preserve_null_as_per_key_clear() {
        let mut metadata = BTreeMap::new();
        metadata.insert("encoding".to_string(), b"json/plain".to_vec());
        let command = command::Command {
            attributes: Some(
                command::command::Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                    command::UpsertWorkflowSearchAttributesCommandAttributes {
                        search_attributes: Some(proto_common::SearchAttributes {
                            indexed_fields: BTreeMap::from([
                                (
                                    "remove".to_string(),
                                    proto_common::Payload {
                                        metadata: metadata.clone(),
                                        data: b"null".to_vec(),
                                        external_payloads: Vec::new(),
                                    },
                                ),
                                (
                                    "keep".to_string(),
                                    proto_common::Payload {
                                        metadata,
                                        data: br#""value""#.to_vec(),
                                        external_payloads: Vec::new(),
                                    },
                                ),
                            ]),
                        }),
                    },
                ),
            ),
            ..Default::default()
        };

        let converted = proto_command_to_workflow_command(command, "default").unwrap();
        let WorkflowCommand::UpsertSearchAttributesPatch(patch) = converted else {
            panic!("expected search-attribute patch");
        };
        assert_eq!(patch.0.get("remove"), Some(&FieldChange::Clear));
        assert_eq!(
            patch.0.get("keep"),
            Some(&FieldChange::Set(tokeira_types::SearchAttrValue::Keyword(
                "value".into()
            )))
        );
    }

    #[test]
    fn respond_completed_request_preserves_metering_sticky_and_worker_envelope() {
        let metering = proto_common::MeteringMetadata {
            nonfirst_local_activity_execution_attempts: 3,
        };
        let edge = respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                identity: "worker-a".to_string(),
                sdk_metadata: Some(
                    tokeira_proto::public::temporal::api::sdk::v1::WorkflowTaskCompletedMetadata {
                        core_used_flags: vec![1, 2],
                        lang_used_flags: vec![3],
                        sdk_name: "sdk".to_string(),
                        sdk_version: "1.0".to_string(),
                    },
                ),
                metering_metadata: Some(metering.clone()),
                sticky_attributes: Some(taskqueue::StickyExecutionAttributes {
                    worker_task_queue: Some(taskqueue::TaskQueue {
                        name: "sticky-queue".to_string(),
                        ..Default::default()
                    }),
                    schedule_to_start_timeout: Some(prost_types::Duration {
                        seconds: 17,
                        nanos: 0,
                    }),
                }),
                resource_id: "workflow-a".to_string(),
                worker_instance_key: "worker-instance-a".to_string(),
                worker_control_task_queue: "worker-control-a".to_string(),
                ..Default::default()
            },
        )
        .expect("respond completed request should convert");

        assert_eq!(edge.metering_metadata, Some(metering.encode_to_vec()),);
        let sticky = edge.sticky.expect("sticky attributes should convert");
        assert_eq!(
            sticky.schedule_to_start_timeout,
            time::Duration::seconds(17)
        );
        assert!(!sticky.queue.0.is_empty());
        assert_eq!(edge.resource_id, "workflow-a");
        assert_eq!(edge.worker_instance_key, "worker-instance-a");
        assert_eq!(edge.worker_control_task_queue, "worker-control-a");
    }

    #[test]
    fn respond_completed_request_rejects_unknown_versioning_behavior() {
        let status = proto_conversion_status(
            respond_completed_request_to_edge(
                workflowservice::RespondWorkflowTaskCompletedRequest {
                    versioning_behavior: 99,
                    ..Default::default()
                },
            )
            .unwrap_err(),
        );

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn respond_completed_request_rejects_versioned_deployment_options_without_identity() {
        let status = proto_conversion_status(
            respond_completed_request_to_edge(
                workflowservice::RespondWorkflowTaskCompletedRequest {
                    deployment_options: Some(deployment_proto::WorkerDeploymentOptions {
                        worker_versioning_mode: enums::WorkerVersioningMode::Versioned as i32,
                        deployment_name: "deployment-a".to_string(),
                        build_id: String::new(),
                    }),
                    ..Default::default()
                },
            )
            .unwrap_err(),
        );

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn respond_completed_request_clears_sticky_when_worker_task_queue_absent() {
        // v1.31.0 treats a nil worker_task_queue as "clear stickiness", not an
        // error (respondworkflowtaskcompleted/api.go:324-340 @ v1.31.0): the
        // completion succeeds with no sticky spec. A worker returning a
        // speculative follow-up task inline may omit the sticky queue
        // (`WorkerSkippedProcessing_RejectByServer`).
        let edge = respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                sticky_attributes: Some(taskqueue::StickyExecutionAttributes {
                    worker_task_queue: None,
                    schedule_to_start_timeout: Some(prost_types::Duration {
                        seconds: 1,
                        nanos: 0,
                    }),
                }),
                ..Default::default()
            },
        )
        .expect("nil worker_task_queue clears stickiness rather than failing");
        assert!(edge.sticky.is_none());
    }

    #[test]
    #[allow(deprecated)]
    fn respond_completed_request_accepts_deprecated_versioning_fields() {
        let edge = respond_completed_request_to_edge(
            workflowservice::RespondWorkflowTaskCompletedRequest {
                worker_version_stamp: Some(proto_common::WorkerVersionStamp {
                    build_id: "legacy-build".to_string(),
                    use_versioning: true,
                }),
                deployment: Some(deployment_proto::Deployment {
                    series_name: "legacy-deployment".to_string(),
                    build_id: "legacy-build".to_string(),
                }),
                ..Default::default()
            },
        )
        .expect("deprecated fields should convert for back-compat");

        assert_eq!(
            edge.worker_version
                .as_ref()
                .and_then(|version| version.stamp.as_ref())
                .map(|stamp| (stamp.build_id.as_str(), stamp.use_versioning)),
            Some(("legacy-build", false))
        );
        assert_eq!(
            edge.deployment_version
                .as_ref()
                .map(|version| (version.deployment_name.as_str(), version.build_id.as_str())),
            Some(("legacy-deployment", "legacy-build"))
        );
        assert_eq!(
            edge.worker_deployment_name.as_deref(),
            Some("legacy-deployment")
        );
    }

    #[test]
    fn modern_pinned_versioning_override_is_preserved() {
        let converted = versioning_override_to_edge(Some(workflow::VersioningOverride {
            r#override: Some(workflow::versioning_override::Override::Pinned(
                workflow::versioning_override::PinnedOverride {
                    behavior: workflow::versioning_override::PinnedOverrideBehavior::Pinned as i32,
                    version: Some(deployment_proto::WorkerDeploymentVersion {
                        deployment_name: "deployment".to_owned(),
                        build_id: "build-id".to_owned(),
                    }),
                },
            )),
            ..Default::default()
        }))
        .expect("modern pinned override should convert");

        assert_eq!(
            converted,
            Some(VersioningOverride::Pinned {
                deployment_series: "deployment".to_owned(),
                build_id: "build-id".to_owned(),
            })
        );
    }

    #[test]
    fn namespace_archival_disabled() {
        let proto = namespace_to_proto(
            NamespaceDescription {
                name: "default".to_string(),
                namespace_id: Some("ns-1".to_string()),
                is_global: false,
                visibility_enabled: true,
                deleted: false,
                description: String::new(),
                owner_email: String::new(),
                cluster_name: "local".to_string(),
                custom_search_attribute_aliases: std::collections::BTreeMap::new(),
                capabilities: crate::translate::NamespaceCapabilities {
                    worker_heartbeats: true,
                    reported_problems_search_attribute: false,
                },
                retention: time::Duration::hours(24),
            },
            false,
        );

        let config = proto.config.expect("config");
        // Retention is echoed (regression: a `None` here renders as `undefined`
        // and crashes the Temporal UI's `.toString()` on the field).
        assert_eq!(
            config.workflow_execution_retention_ttl,
            Some(prost_types::Duration {
                seconds: 24 * 60 * 60,
                nanos: 0,
            })
        );
        assert_eq!(
            config.history_archival_state,
            enums::ArchivalState::Disabled as i32
        );
        assert_eq!(
            config.visibility_archival_state,
            enums::ArchivalState::Disabled as i32
        );
    }

    #[test]
    fn namespace_clusters_populated() {
        let proto = namespace_to_proto(
            NamespaceDescription {
                name: "default".to_string(),
                namespace_id: Some("ns-1".to_string()),
                is_global: false,
                visibility_enabled: true,
                deleted: false,
                description: String::new(),
                owner_email: String::new(),
                cluster_name: "local".to_string(),
                custom_search_attribute_aliases: std::collections::BTreeMap::new(),
                capabilities: crate::translate::NamespaceCapabilities {
                    worker_heartbeats: true,
                    reported_problems_search_attribute: false,
                },
                retention: time::Duration::hours(24),
            },
            false,
        );

        let replication = proto.replication_config.expect("replication");
        assert_eq!(replication.active_cluster_name, "local");
        assert_eq!(replication.clusters.len(), 1);
        assert_eq!(replication.clusters[0].cluster_name, "local");
    }

    #[test]
    fn standalone_activities_capability_reflects_flag() {
        let describe = |standalone: bool| {
            namespace_to_proto(
                NamespaceDescription {
                    name: "default".to_string(),
                    namespace_id: Some("ns-1".to_string()),
                    is_global: false,
                    visibility_enabled: true,
                    deleted: false,
                    description: String::new(),
                    owner_email: String::new(),
                    cluster_name: "local".to_string(),
                    custom_search_attribute_aliases: std::collections::BTreeMap::new(),
                    capabilities: crate::translate::NamespaceCapabilities {
                        worker_heartbeats: true,
                        reported_problems_search_attribute: false,
                    },
                    retention: time::Duration::hours(24),
                },
                standalone,
            )
            .namespace_info
            .unwrap()
            .capabilities
            .unwrap()
        };
        // The capability tracks the server-uniform flag (Req 13.4), not a constant.
        let enabled = describe(true);
        assert!(enabled.standalone_activities);
        assert!(enabled.eager_workflow_start);
        let disabled = describe(false);
        assert!(!disabled.standalone_activities);
        assert!(disabled.eager_workflow_start);
    }

    #[test]
    fn cluster_info_populated() {
        let proto = cluster_info_to_proto(ClusterInfo {
            cluster_name: "tokeira-local".to_string(),
            version: "0.1.0-dev".to_string(),
            notes: vec!["in-memory operator api".to_string()],
            shard_count: 1,
            supported_clients: std::collections::BTreeMap::from([(
                "temporal-go".to_string(),
                ">=1.26.0".to_string(),
            )]),
        });

        assert!(!proto.supported_clients.is_empty());
        assert!(proto.version_info.is_some());
        assert!(proto.history_shard_count >= 1);
    }

    #[test]
    fn system_info_proto_uses_only_upstream_wire_fields() {
        const REQUEST_RESPONSE_PROTO: &str = include_str!(
            "../../../../proto/upstream/temporal/api/workflowservice/v1/request_response.proto"
        );
        const EXPECTED_CAPABILITY_FIELDS: &[&str] = &[
            "signal_and_query_header",
            "internal_error_differentiation",
            "activity_failure_include_heartbeat",
            "supports_schedules",
            "encoded_failure_attributes",
            "build_id_based_versioning",
            "upsert_memo",
            "eager_workflow_start",
            "sdk_metadata",
            "count_group_by_execution_status",
            "nexus",
            "server_scaled_deployments",
        ];

        let proto = system_info_to_proto(SystemInfo {
            server_version: "1.27.0".to_string(),
            capabilities: crate::translate::SystemCapabilities {
                signal_and_query_header: true,
                internal_error_differentiation: true,
                activity_failure_include_heartbeat: false,
                supports_schedules: false,
                encoded_failure_attributes: true,
                build_id_based_versioning: true,
                upsert_memo: false,
                eager_workflow_start: false,
                sdk_metadata: false,
                count_group_by_execution_status: true,
                nexus: true,
                server_scaled_deployments: false,
                worker_heartbeats: true,
            },
        });

        assert_eq!(proto.server_version, "1.27.0");
        assert!(proto.capabilities.is_some());
        assert_eq!(
            get_system_info_capability_fields(REQUEST_RESPONSE_PROTO),
            EXPECTED_CAPABILITY_FIELDS
        );
        assert!(
            !REQUEST_RESPONSE_PROTO
                .split("message GetSystemInfoResponse")
                .nth(1)
                .expect("GetSystemInfoResponse")
                .contains("tokeira"),
            "upstream GetSystemInfoResponse must not carry Tokeira-specific fields"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: worker-deployments, Property 14: versioning-info projection fidelity
        // **Validates: Requirements 10.1, 10.2, 10.3, 10.4**
        #[test]
        fn versioning_info_projection_fidelity(case in arb_versioning_projection_case()) {
            let proto = describe_response_to_proto(description_with_versioning(&case));
            let info = proto
                .workflow_execution_info
                .expect("describe should include workflow execution info");

            assert_deprecated_versioning_defaults(&info);

            match &case.versioning_info {
                None => {
                    prop_assert!(info.versioning_info.is_none());
                    prop_assert_eq!(info.worker_deployment_name, "");
                }
                Some(expected) => {
                    let actual = info.versioning_info.as_ref().expect("versioning info");
                    prop_assert_eq!(actual.behavior, versioning_behavior_to_proto(expected.behavior));
                    prop_assert_eq!(
                        actual.continue_as_new_initial_versioning_behavior,
                        continue_as_new_behavior_to_proto(
                            expected.continue_as_new_initial_versioning_behavior
                        )
                    );
                    prop_assert_eq!(actual.revision_number, expected.revision_number);
                    assert_optional_deployment_version(
                        actual.deployment_version.as_ref(),
                        expected.deployment_version.as_ref(),
                    )?;
                    assert_optional_deployment_transition(
                        actual.version_transition.as_ref(),
                        expected.version_transition.as_ref(),
                    )?;
                    assert_versioning_override(
                        actual.versioning_override.as_ref(),
                        expected.versioning_override.as_ref(),
                    )?;
                    prop_assert_eq!(
                        info.worker_deployment_name,
                        case.worker_deployment_name.clone().unwrap_or_default()
                    );
                }
            }
        }
    }

    fn get_system_info_capability_fields(proto: &str) -> Vec<&str> {
        let response = proto
            .split("message GetSystemInfoResponse")
            .nth(1)
            .expect("GetSystemInfoResponse message");
        let capabilities = response
            .split("message Capabilities")
            .nth(1)
            .expect("GetSystemInfoResponse.Capabilities message");

        capabilities
            .lines()
            .map(str::trim)
            .take_while(|line| *line != "}")
            .filter_map(|line| line.strip_prefix("bool "))
            .filter_map(|line| line.split_once(' '))
            .map(|(name, _)| name)
            .collect()
    }

    #[derive(Clone, Debug)]
    struct VersioningProjectionCase {
        versioning_info: Option<WorkflowVersioningInfo>,
        worker_deployment_name: Option<String>,
    }

    fn arb_versioning_projection_case() -> impl Strategy<Value = VersioningProjectionCase> {
        prop_oneof![
            Just(VersioningProjectionCase {
                versioning_info: None,
                worker_deployment_name: None,
            }),
            (
                arb_workflow_versioning_info(),
                proptest::option::of("[a-z][a-z0-9-]{0,12}")
            )
                .prop_map(|(versioning_info, worker_deployment_name)| {
                    VersioningProjectionCase {
                        versioning_info: Some(versioning_info),
                        worker_deployment_name,
                    }
                }),
        ]
    }

    fn arb_workflow_versioning_info() -> impl Strategy<Value = WorkflowVersioningInfo> {
        (
            arb_versioning_behavior(),
            proptest::option::of(arb_worker_deployment_version_ref()),
            arb_versioning_override(),
            proptest::option::of(arb_worker_deployment_version_ref()),
            0i64..1_000_000,
            arb_continue_as_new_behavior(),
        )
            .prop_map(
                |(
                    behavior,
                    deployment_version,
                    versioning_override,
                    version_transition,
                    revision_number,
                    continue_as_new_initial_versioning_behavior,
                )| {
                    WorkflowVersioningInfo {
                        behavior,
                        deployment_version,
                        versioning_override,
                        version_transition,
                        revision_number,
                        continue_as_new_initial_versioning_behavior,
                        ..WorkflowVersioningInfo::default()
                    }
                },
            )
    }

    fn arb_worker_deployment_version_ref() -> impl Strategy<Value = WorkerDeploymentVersionRef> {
        ("[a-z][a-z0-9-]{0,12}", "[a-z][a-z0-9-]{0,12}").prop_map(|(deployment_name, build_id)| {
            WorkerDeploymentVersionRef {
                deployment_name,
                build_id,
            }
        })
    }

    fn arb_versioning_behavior() -> impl Strategy<Value = VersioningBehavior> {
        prop_oneof![
            Just(VersioningBehavior::Unspecified),
            Just(VersioningBehavior::Pinned),
            Just(VersioningBehavior::AutoUpgrade),
        ]
    }

    fn arb_continue_as_new_behavior() -> impl Strategy<Value = ContinueAsNewVersioningBehavior> {
        prop_oneof![
            Just(ContinueAsNewVersioningBehavior::Unspecified),
            Just(ContinueAsNewVersioningBehavior::AutoUpgrade),
            Just(ContinueAsNewVersioningBehavior::UseRampingVersion),
        ]
    }

    fn arb_versioning_override() -> impl Strategy<Value = Option<KernelVersioningOverride>> {
        prop_oneof![
            Just(None),
            Just(Some(KernelVersioningOverride::AutoUpgrade)),
            arb_worker_deployment_version_ref()
                .prop_map(|version| Some(KernelVersioningOverride::Pinned { version })),
        ]
    }

    fn description_with_versioning(
        case: &VersioningProjectionCase,
    ) -> WorkflowExecutionDescription {
        let run_id = RunId(Uuid::from_u128(0x11111111111111111111111111111111));
        WorkflowExecutionDescription {
            namespace: "default".to_string(),
            workflow_id: "workflow".to_string(),
            run_key: RunKey(Uuid::from_u128(0x22222222222222222222222222222222)),
            run_id,
            workflow_type: "workflow-type".to_string(),
            task_queue: "queue".to_string(),
            status: ExecutionStatus::Running,
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            close_time: None,
            execution_time: OffsetDateTime::UNIX_EPOCH,
            execution_config: crate::translate::ExecutionConfigDescription {
                task_queue: "queue".to_string(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                default_workflow_task_timeout: time::Duration::seconds(10),
                user_metadata: None,
            },
            history_length: 1,
            history_size_bytes: 128,
            state_transition_count: 1,
            parent_namespace_id: None,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: None,
            root_run_id: None,
            first_run_id: Some(run_id),
            memo: tokeira_types::Memo(Default::default()),
            search_attributes: tokeira_types::SearchAttributes(Default::default()),
            pending_activities: Vec::new(),
            pending_children: Vec::new(),
            pending_workflow_task: None,
            callbacks: Vec::new(),
            pending_nexus_operations: Vec::new(),
            pause_info: None,
            execution_expiration_time: None,
            run_expiration_time: None,
            cancel_requested: false,
            original_start_time: OffsetDateTime::UNIX_EPOCH,
            versioning_info: case.versioning_info.clone(),
            worker_deployment_name: case.worker_deployment_name.clone(),
            most_recent_worker_version_stamp: None,
            request_id_infos: std::collections::BTreeMap::new(),
            external_payload_count: 0,
            external_payload_size_bytes: 0,
        }
    }

    #[allow(deprecated)]
    fn assert_deprecated_versioning_defaults(info: &workflow::WorkflowExecutionInfo) {
        assert_eq!(info.assigned_build_id, "");
        assert_eq!(info.inherited_build_id, "");
        assert!(info.most_recent_worker_version_stamp.is_none());
    }

    fn assert_optional_deployment_version(
        actual: Option<&deployment_proto::WorkerDeploymentVersion>,
        expected: Option<&WorkerDeploymentVersionRef>,
    ) -> Result<(), TestCaseError> {
        match (actual, expected) {
            (None, None) => Ok(()),
            (Some(actual), Some(expected)) => assert_deployment_version(actual, expected),
            (actual, expected) => Err(TestCaseError::fail(format!(
                "deployment version mismatch: actual={actual:?} expected={expected:?}"
            ))),
        }
    }

    fn assert_optional_deployment_transition(
        actual: Option<&workflow::DeploymentVersionTransition>,
        expected: Option<&WorkerDeploymentVersionRef>,
    ) -> Result<(), TestCaseError> {
        match (actual, expected) {
            (None, None) => Ok(()),
            (Some(actual), Some(expected)) => assert_optional_deployment_version(
                actual.deployment_version.as_ref(),
                Some(expected),
            ),
            (actual, expected) => Err(TestCaseError::fail(format!(
                "deployment transition mismatch: actual={actual:?} expected={expected:?}"
            ))),
        }
    }

    fn assert_versioning_override(
        actual: Option<&workflow::VersioningOverride>,
        expected: Option<&KernelVersioningOverride>,
    ) -> Result<(), TestCaseError> {
        match (actual.and_then(|value| value.r#override.as_ref()), expected) {
            (None, None) => Ok(()),
            (
                Some(workflow::versioning_override::Override::AutoUpgrade(actual)),
                Some(KernelVersioningOverride::AutoUpgrade),
            ) => {
                prop_assert!(*actual);
                Ok(())
            }
            (
                Some(workflow::versioning_override::Override::Pinned(actual)),
                Some(KernelVersioningOverride::Pinned { version }),
            ) => {
                prop_assert_eq!(
                    actual.behavior,
                    workflow::versioning_override::PinnedOverrideBehavior::Pinned as i32
                );
                assert_optional_deployment_version(actual.version.as_ref(), Some(version))
            }
            (actual, expected) => Err(TestCaseError::fail(format!(
                "versioning override mismatch: actual={actual:?} expected={expected:?}"
            ))),
        }
    }

    fn assert_deployment_version(
        actual: &deployment_proto::WorkerDeploymentVersion,
        expected: &WorkerDeploymentVersionRef,
    ) -> Result<(), TestCaseError> {
        prop_assert_eq!(&actual.deployment_name, &expected.deployment_name);
        prop_assert_eq!(&actual.build_id, &expected.build_id);
        Ok(())
    }

    #[test]
    fn command_without_attributes_returns_missing_field() {
        let err = proto_command_to_workflow_command(
            command::Command {
                attributes: None,
                ..Default::default()
            },
            "",
        )
        .expect_err("missing attributes should fail");

        match err {
            ProtoConversionError::MissingField(field) => {
                assert_eq!(field, "Command.attributes");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn empty_activity_poll_returns_default_response() {
        let default = workflowservice::PollActivityTaskQueueResponse::default();
        assert!(default.task_token.is_empty());
        assert!(default.activity_id.is_empty());
        assert_eq!(default.attempt, 0);
    }

    #[test]
    fn invalid_task_token_returns_error() {
        let err =
            deserialize_activity_token(b"not-json").expect_err("should fail on invalid bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn delete_request_accepts_omitted_run_id_as_current_execution() {
        let request = workflowservice::DeleteWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow".to_string(),
                run_id: String::new(),
            }),
        };

        let translated = delete_request_to_edge(request).unwrap();
        assert_eq!(translated.workflow_id, "workflow");
        assert_eq!(translated.run_id, None);
    }

    // Feature: temporal-ui-support, Property 12: rejected deletion preserves state
    // Delete admission is a pure wire conversion. Every invalid execution shape
    // is rejected before the edge can resolve or mutate authoritative/visibility
    // state; the gRPC integration test separately verifies a seeded run survives.
    // **Validates: Requirements 9.6, 9.7**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn rejected_delete_admission_is_pure(
            invalid_kind in 0u8..3,
            suffix in "[a-z0-9]{1,24}",
        ) {
            let execution = match invalid_kind {
                0 => None,
                1 => Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: String::new(),
                    run_id: String::new(),
                }),
                _ => Some(tokeira_proto::common::WorkflowExecution {
                    workflow_id: format!("workflow-{suffix}"),
                    run_id: format!("not-a-run-id-{suffix}"),
                }),
            };
            let request = workflowservice::DeleteWorkflowExecutionRequest {
                namespace: format!("namespace-{suffix}"),
                workflow_execution: execution,
            };
            let before = request.clone();

            let error = delete_request_to_edge(request.clone()).unwrap_err();

            let ProtoConversionError::InvalidArgument(message) = error else {
                return Err(TestCaseError::fail(format!(
                    "invalid deletion should be InvalidArgument, got {error:?}"
                )));
            };
            let expected = match invalid_kind {
                0 => "Execution is not set on request.",
                1 => "WorkflowId is not set on request.",
                _ => "Invalid RunId.",
            };
            prop_assert_eq!(message, expected);
            prop_assert_eq!(request, before);
        }
    }

    #[test]
    fn empty_task_token_returns_error() {
        let err = deserialize_activity_token(b"").expect_err("should fail on empty bytes");
        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => {
                panic!("unexpected error: {other:?}")
            }
        }
    }

    #[test]
    fn heartbeat_cancel_requested_propagation() {
        let resp = crate::translate::RecordActivityTaskHeartbeatResponse {
            cancel_requested: true,
        };
        let proto = record_heartbeat_to_proto(resp);
        assert!(proto.cancel_requested);

        let resp = crate::translate::RecordActivityTaskHeartbeatResponse {
            cancel_requested: false,
        };
        let proto = record_heartbeat_to_proto(resp);
        assert!(!proto.cancel_requested);
    }

    #[test]
    fn activity_heartbeat_translators_preserve_details() {
        use tokeira_proto::conversions::common::payloads_from_domain;
        let details = Payloads(vec![tokeira_types::Payload {
            data: b"progress".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }]);
        let token = ActivityTaskToken {
            run_key: RunKey(Uuid::nil()),
            activity_id: "activity-1".to_string(),
            schedule_event_id: 7,
            attempt: 2,
            shard_epoch: tokeira_types::ShardEpoch(3),
        };

        let token_edge =
            record_heartbeat_to_edge(workflowservice::RecordActivityTaskHeartbeatRequest {
                task_token: serialize_activity_token(&token),
                details: Some(payloads_from_domain(&details)),
                identity: "worker".to_string(),
                ..Default::default()
            })
            .expect("token heartbeat should translate");
        assert_eq!(token_edge.details, Some(details.clone()));

        let by_id_edge = record_activity_heartbeat_by_id_to_edge(
            workflowservice::RecordActivityTaskHeartbeatByIdRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                activity_id: "activity-1".to_string(),
                details: Some(payloads_from_domain(&details)),
                identity: "worker".to_string(),
                ..Default::default()
            },
        )
        .expect("by-id heartbeat should translate");
        assert_eq!(by_id_edge.details, Some(details));
    }

    #[test]
    fn activity_poll_response_projects_heartbeat_and_timing_fields() {
        use tokeira_proto::conversions::common::payloads_from_domain;

        let details = Payloads(vec![tokeira_types::Payload {
            data: b"checkpoint".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }]);
        let scheduled = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let current_attempt = OffsetDateTime::from_unix_timestamp(200).unwrap();
        let started = OffsetDateTime::from_unix_timestamp(250).unwrap();

        let proto =
            poll_activity_response_to_proto(crate::translate::PollActivityTaskQueueResponse {
                task_token: b"token".to_vec(),
                activity_id: "activity-1".to_string(),
                run_id: RunId(Uuid::from_u128(9)),
                activity_type: "activity-type".to_string(),
                input: Payloads::default(),
                attempt: 2,
                workflow_id: "workflow".to_string(),
                workflow_type: "workflow-type".to_string(),
                workflow_namespace: "default".to_string(),
                run_key: RunKey(Uuid::from_u128(1)),
                header: None,
                retry_policy: None,
                heartbeat_details: Some(details.clone()),
                scheduled_time: Some(scheduled),
                current_attempt_scheduled_time: Some(current_attempt),
                started_time: Some(started),
                schedule_to_close_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                poller_scaling_decision: Some(1),
            });

        assert_eq!(
            proto.heartbeat_details,
            Some(payloads_from_domain(&details))
        );
        assert_eq!(proto.scheduled_time, Some(to_proto_timestamp(scheduled)));
        assert_eq!(
            proto.current_attempt_scheduled_time,
            Some(to_proto_timestamp(current_attempt))
        );
        assert_eq!(proto.started_time, Some(to_proto_timestamp(started)));
        assert_eq!(
            proto
                .poller_scaling_decision
                .expect("scaling decision should be projected")
                .poll_request_delta_suggestion,
            1
        );
    }

    #[test]
    fn task_queue_config_projects_metadata_for_an_explicit_unset() {
        let updated_at = OffsetDateTime::from_unix_timestamp(123).unwrap();
        let proto = task_queue_config_to_proto(crate::translate::TaskQueueConfig {
            queue_rate_limit: None,
            queue_rate_limit_metadata: Some(crate::translate::TaskQueueConfigMetadata {
                reason: "remove queue ceiling".to_string(),
                update_identity: "operator".to_string(),
                update_time: updated_at,
            }),
            fairness_key_rate_limit_default: None,
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: Default::default(),
        });

        let rate = proto
            .queue_rate_limit
            .expect("an explicit unset retains its audit envelope");
        assert!(rate.rate_limit.is_none());
        let metadata = rate.metadata.expect("unset metadata should project");
        assert_eq!(metadata.reason, "remove queue ceiling");
        assert_eq!(metadata.update_identity, "operator");
        assert_eq!(metadata.update_time, Some(to_proto_timestamp(updated_at)));
    }

    #[test]
    fn workflow_poll_response_projects_legacy_query_field() {
        let payloads = Payloads(vec![tokeira_types::Payload {
            data: b"input".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }]);
        let proto = poll_response_to_proto(crate::translate::PollWorkflowTaskQueueResponse {
            task_token: b"query-token".to_vec(),
            started_event_id: 0,
            previous_started_event_id: 12,
            attempt: 1,
            scheduled_time: None,
            started_time: None,
            payload: crate::translate::WorkflowTaskPayloadDto {
                workflow_id: "workflow-a".to_string(),
                run_key: RunKey(Uuid::from_u128(1)),
                run_id: RunId(Uuid::from_u128(1)),
                task_queue: "main".to_string(),
                history: Vec::new(),
            },
            query: Some(crate::translate::WorkflowQueryDto {
                query_type: "state".to_string(),
                query_args: payloads.clone(),
            }),
            queries: Default::default(),
            messages: Vec::new(),
            poller_scaling_decision: None,
        });

        let query = proto.query.expect("legacy query field should be set");
        assert_eq!(proto.task_token, b"query-token".to_vec());
        assert_eq!(proto.started_event_id, 0);
        assert_eq!(query.query_type, "state");
        assert_eq!(
            query.query_args,
            Some(tokeira_proto::conversions::common::payloads_from_domain(
                &payloads
            ))
        );
        assert!(proto.queries.is_empty());
    }

    #[test]
    fn activity_by_id_translators_preserve_run_identity_and_payloads() {
        use tokeira_proto::conversions::common::payloads_from_domain;
        let payloads = Payloads(vec![tokeira_types::Payload {
            data: b"payload".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }]);

        let completed = respond_activity_completed_by_id_to_edge(
            workflowservice::RespondActivityTaskCompletedByIdRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                run_id: "11111111-1111-1111-1111-111111111111".to_string(),
                activity_id: "activity-1".to_string(),
                result: Some(payloads_from_domain(&payloads)),
                identity: "worker-a".to_string(),
                ..Default::default()
            },
        )
        .expect("completed by-id request should translate");
        assert_eq!(
            completed.run_id.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        assert_eq!(completed.result, payloads);
        assert_eq!(completed.identity, "worker-a");

        let failed = respond_activity_failed_by_id_to_edge(
            workflowservice::RespondActivityTaskFailedByIdRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                run_id: String::new(),
                activity_id: "activity-1".to_string(),
                failure: Some(failure_proto::Failure {
                    message: "boom".to_string(),
                    ..Default::default()
                }),
                identity: "worker-b".to_string(),
                ..Default::default()
            },
        )
        .expect("failed by-id request should translate");
        assert_eq!(failed.run_id, None);
        assert_eq!(failed.identity, "worker-b");
        assert!(
            failed
                .failure
                .data
                .windows(4)
                .any(|window| window == b"boom")
        );

        let canceled = respond_activity_canceled_by_id_to_edge(
            workflowservice::RespondActivityTaskCanceledByIdRequest {
                namespace: "default".to_string(),
                workflow_id: "workflow".to_string(),
                activity_id: "activity-1".to_string(),
                details: Some(payloads_from_domain(&payloads)),
                identity: "worker-c".to_string(),
                ..Default::default()
            },
        )
        .expect("canceled by-id request should translate");
        assert_eq!(canceled.details, Some(payloads));
        assert_eq!(canceled.identity, "worker-c");
    }

    #[test]
    fn activity_token_translators_reject_malformed_tokens() {
        let err = respond_activity_canceled_to_edge(
            workflowservice::RespondActivityTaskCanceledRequest {
                task_token: b"not-json".to_vec(),
                ..Default::default()
            },
        )
        .expect_err("malformed activity token should fail translation");

        match err {
            ProtoConversionError::InvalidTaskToken(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn update_activity_options_translation_preserves_target_variants() {
        use workflowservice::update_activity_options_request::Activity;

        let base = workflowservice::UpdateActivityOptionsRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow".to_string(),
                run_id: "11111111-1111-1111-1111-111111111111".to_string(),
                ..Default::default()
            }),
            identity: "operator".to_string(),
            activity_options: Some(activity_proto::ActivityOptions {
                task_queue: Some(taskqueue::TaskQueue {
                    name: "queue-b".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            update_mask: Some(prost_types::FieldMask {
                paths: vec!["task_queue".to_string()],
            }),
            ..Default::default()
        };

        let id_edge =
            update_activity_options_to_edge(workflowservice::UpdateActivityOptionsRequest {
                activity: Some(Activity::Id("activity-1".to_string())),
                ..base.clone()
            })
            .expect("id target should translate");
        assert_eq!(
            id_edge.target,
            crate::translate::ActivityTarget::Id("activity-1".to_string())
        );

        let type_edge =
            update_activity_options_to_edge(workflowservice::UpdateActivityOptionsRequest {
                activity: Some(Activity::Type("ActivityType".to_string())),
                ..base
            })
            .expect("type target should translate for handler rejection");
        assert_eq!(
            type_edge.target,
            crate::translate::ActivityTarget::Type("ActivityType".to_string())
        );
    }

    #[test]
    fn update_activity_options_translation_requires_execution_and_target() {
        let err = update_activity_options_to_edge(workflowservice::UpdateActivityOptionsRequest {
            activity: Some(
                workflowservice::update_activity_options_request::Activity::Id(
                    "activity-1".to_string(),
                ),
            ),
            ..Default::default()
        })
        .expect_err("missing execution should fail");
        assert!(matches!(
            err,
            ProtoConversionError::MissingField("UpdateActivityOptionsRequest.execution")
        ));

        let err = update_activity_options_to_edge(workflowservice::UpdateActivityOptionsRequest {
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "workflow".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect_err("missing target should fail");
        assert!(matches!(
            err,
            ProtoConversionError::MissingField("UpdateActivityOptionsRequest.activity")
        ));
    }

    #[test]
    fn activity_poll_default_timeout() {
        let req = workflowservice::PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            identity: "worker-1".to_string(),
            ..Default::default()
        };
        let edge = poll_activity_request_to_edge(req).unwrap();
        assert_eq!(edge.timeout, Duration::from_secs(60));
    }

    #[test]
    fn terminate_with_details_translates() {
        use tokeira_proto::conversions::common::payloads_from_domain;
        let details = Payloads(vec![tokeira_types::Payload {
            data: b"stack-trace".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        }]);
        let req = workflowservice::TerminateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            reason: "test".to_string(),
            details: Some(payloads_from_domain(&details)),
            identity: "admin".to_string(),
            ..Default::default()
        };
        let edge = terminate_request_to_edge(req).unwrap();
        assert!(edge.details.is_some());
        assert_eq!(edge.details.unwrap().0.len(), 1);
    }

    #[test]
    fn cancel_with_empty_reason() {
        let req = workflowservice::RequestCancelWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            reason: String::new(),
            identity: "admin".to_string(),
            ..Default::default()
        };
        let edge = cancel_request_to_edge(req).unwrap();
        assert_eq!(edge.reason, "");
    }

    #[test]
    fn update_wait_policy_mapping() {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        let wf_exec = || tokeira_proto::common::WorkflowExecution {
            workflow_id: "wf".to_string(),
            run_id: String::new(),
            ..Default::default()
        };
        let update_request = |name: &str, id: &str| update::Request {
            meta: Some(update::Meta {
                update_id: id.to_string(),
                identity: String::new(),
            }),
            input: Some(update::Input {
                name: name.to_string(),
                ..Default::default()
            }),
        };

        // lifecycle_stage 3 → Completed (COMPLETED = 3 in the proto enum)
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(wf_exec()),
            request: Some(update_request("handler", "u1")),
            wait_policy: Some(update::WaitPolicy { lifecycle_stage: 3 }),
            ..Default::default()
        };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Completed
        );

        // lifecycle_stage 1 → Admitted (ADMITTED = 1 in the proto enum)
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(wf_exec()),
            request: Some(update_request("handler", "u2")),
            wait_policy: Some(update::WaitPolicy { lifecycle_stage: 1 }),
            ..Default::default()
        };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Admitted
        );

        // lifecycle_stage 2 → Accepted (ACCEPTED = 2 in the proto enum)
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(wf_exec()),
            request: Some(update_request("handler", "u3")),
            wait_policy: Some(update::WaitPolicy { lifecycle_stage: 2 }),
            ..Default::default()
        };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(
            edge.wait_policy,
            crate::translate::UpdateWaitPolicyDto::Accepted
        );
    }

    #[test]
    fn query_default_timeout() {
        use tokeira_proto::public::temporal::api::query::v1 as query;
        let req = workflowservice::QueryWorkflowRequest {
            namespace: "ns".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            query: Some(query::WorkflowQuery {
                query_type: "check".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let edge = query_request_to_edge(req).unwrap();
        assert_eq!(edge.timeout, Duration::from_secs(10));
    }

    #[test]
    fn update_default_timeout() {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        let req = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "ns".to_string(),
            workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: String::new(),
                ..Default::default()
            }),
            request: Some(update::Request {
                meta: Some(update::Meta {
                    update_id: "u1".to_string(),
                    identity: String::new(),
                }),
                input: Some(update::Input {
                    name: "handler".to_string(),
                    ..Default::default()
                }),
            }),
            wait_policy: None,
            ..Default::default()
        };
        let edge = update_request_to_edge(req).unwrap();
        assert_eq!(edge.timeout, Duration::from_secs(30));
    }

    // The upstream RespondWorkflowTaskCompletedResponse no longer has
    // workflow_completed/new_run_id fields, so the old property test
    // and related tests are removed.

    #[test]
    fn fail_workflow_command_produces_failure_proto_encoding() {
        let failure = failure_proto::Failure {
            message: "app error".to_string(),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "AppError".to_string(),
                    non_retryable: false,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let proto_cmd = command::Command {
            attributes: Some(
                command::command::Attributes::FailWorkflowExecutionCommandAttributes(
                    command::FailWorkflowExecutionCommandAttributes {
                        failure: Some(failure),
                    },
                ),
            ),
            ..Default::default()
        };
        let edge = proto_command_to_workflow_command(proto_cmd, "").unwrap();
        match edge {
            WorkflowCommand::FailWorkflow { failure } => {
                assert_eq!(
                    failure.metadata.get("encoding").map(|s| s.as_str()),
                    Some("temporal/failure+proto")
                );
                let decoded = payload_to_failure(&failure);
                assert_eq!(decoded.message, "app error");
                match decoded.failure_info.unwrap() {
                    failure_proto::failure::FailureInfo::ApplicationFailureInfo(info) => {
                        assert_eq!(info.r#type, "AppError");
                    }
                    other => panic!("unexpected failure_info: {other:?}"),
                }
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn respond_activity_failed_extracts_type_from_application_failure_info() {
        let failure = failure_proto::Failure {
            message: "activity error".to_string(),
            source: "GoSDK".to_string(),
            failure_info: Some(failure_proto::failure::FailureInfo::ApplicationFailureInfo(
                failure_proto::ApplicationFailureInfo {
                    r#type: "CustomActivityError".to_string(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let token = tokeira_types::ActivityTaskToken {
            run_key: tokeira_types::RunKey::new(),
            activity_id: "act-1".to_string(),
            schedule_event_id: 5,
            attempt: 1,
            shard_epoch: tokeira_types::ShardEpoch::ZERO,
        };
        let token_bytes = serialize_activity_token(&token);
        let req = workflowservice::RespondActivityTaskFailedRequest {
            task_token: token_bytes,
            failure: Some(failure),
            identity: "worker".to_string(),
            ..Default::default()
        };
        let edge = respond_activity_failed_to_edge(req).unwrap();
        assert_eq!(
            edge.failure.metadata.get("encoding").map(|s| s.as_str()),
            Some("temporal/failure+proto")
        );
        assert_eq!(
            edge.failure_error_type.as_deref(),
            Some("CustomActivityError")
        );
        assert!(edge.is_non_retryable);
    }

    #[test]
    fn corrupted_payload_produces_fallback_failure() {
        let corrupted = tokeira_types::Payload {
            data: b"garbage bytes".to_vec(),
            metadata: Default::default(),
            external_payloads: Vec::new(),
        };
        let decoded = payload_to_failure(&corrupted);
        assert_eq!(decoded.message, "garbage bytes");
        assert!(decoded.failure_info.is_none());
    }

    // ── ExecuteMultiOperation (Update-with-Start) ──

    fn multi_op_start(workflow_id: &str) -> workflowservice::StartWorkflowExecutionRequest {
        workflowservice::StartWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_id: workflow_id.to_string(),
            workflow_type: Some(proto_common::WorkflowType {
                name: "wf-type".to_string(),
            }),
            task_queue: Some(taskqueue::TaskQueue {
                name: "queue".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn multi_op_update(workflow_id: &str) -> workflowservice::UpdateWorkflowExecutionRequest {
        use tokeira_proto::public::temporal::api::update::v1 as update;
        workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "default".to_string(),
            workflow_execution: Some(proto_common::WorkflowExecution {
                workflow_id: workflow_id.to_string(),
                run_id: String::new(),
            }),
            request: Some(update::Request {
                meta: Some(update::Meta {
                    update_id: String::new(),
                    identity: "update-client".to_string(),
                }),
                input: Some(update::Input {
                    header: None,
                    name: "my-update".to_string(),
                    args: None,
                }),
            }),
            ..Default::default()
        }
    }

    fn multi_op_request(
        start: Option<workflowservice::StartWorkflowExecutionRequest>,
        update: Option<workflowservice::UpdateWorkflowExecutionRequest>,
    ) -> workflowservice::ExecuteMultiOperationRequest {
        use workflowservice::execute_multi_operation_request::{Operation, operation};
        let mut operations = Vec::new();
        if let Some(start) = start {
            operations.push(Operation {
                operation: Some(operation::Operation::StartWorkflow(start)),
            });
        }
        if let Some(update) = update {
            operations.push(Operation {
                operation: Some(operation::Operation::UpdateWorkflow(update)),
            });
        }
        workflowservice::ExecuteMultiOperationRequest {
            namespace: "default".to_string(),
            operations,
            ..Default::default()
        }
    }

    /// Req 1.1: anything other than exactly `[Start, Update]` (in order) hits
    /// the shape gate — a PLAIN INVALID_ARGUMENT with the exact frontend
    /// message and NO multi-operation detail (workflow_handler.go:718-726).
    #[test]
    fn multi_operation_shape_gate_rejects_non_start_update_pairs() {
        // Empty, start-only, and reversed-order compositions all reject.
        let cases = vec![
            multi_op_request(None, None),
            multi_op_request(Some(multi_op_start("wf")), None),
            multi_op_request(None, Some(multi_op_update("wf"))),
            {
                use workflowservice::execute_multi_operation_request::{Operation, operation};
                workflowservice::ExecuteMultiOperationRequest {
                    namespace: "default".to_string(),
                    operations: vec![
                        Operation {
                            operation: Some(operation::Operation::UpdateWorkflow(multi_op_update(
                                "wf",
                            ))),
                        },
                        Operation {
                            operation: Some(operation::Operation::StartWorkflow(multi_op_start(
                                "wf",
                            ))),
                        },
                    ],
                    ..Default::default()
                }
            },
        ];
        for request in cases {
            assert_eq!(
                multi_operation_request_to_edge(request).unwrap_err(),
                MultiOperationRequestError::Shape
            );
        }

        let status = multi_operation_request_error_to_status(MultiOperationRequestError::Shape);
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "Operations have to be exactly [Start, Update]."
        );
        assert!(
            status.details().is_empty(),
            "the shape gate carries no MultiOperationExecutionFailure detail"
        );
    }

    /// Req 1.2/1.4: each prohibited start field and a start-op namespace
    /// mismatch produce that op's own message; the update sibling stays clean.
    #[test]
    fn multi_operation_start_restrictions_produce_per_op_errors() {
        let mut cron = multi_op_start("wf");
        cron.cron_schedule = "* * * * *".to_string();
        let mut eager = multi_op_start("wf");
        eager.request_eager_execution = true;
        let mut delayed = multi_op_start("wf");
        delayed.workflow_start_delay = Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        });
        let mut foreign_namespace = multi_op_start("wf");
        foreign_namespace.namespace = "other".to_string();

        let cases = vec![
            (cron, "CronSchedule is not allowed."),
            (eager, "RequestEagerExecution is not supported."),
            (delayed, "WorkflowStartDelay is not supported."),
            (
                foreign_namespace,
                "Operation namespace did not match request's namespace.",
            ),
        ];
        for (start, expected) in cases {
            let error = multi_operation_request_to_edge(multi_op_request(
                Some(start),
                Some(multi_op_update("wf")),
            ))
            .unwrap_err();
            assert_eq!(
                error,
                MultiOperationRequestError::PerOperation {
                    start: Some(expected.to_string()),
                    update: None,
                }
            );
        }
    }

    /// Req 1.3/1.4/1.5: prohibited update fields, missing meta/input/name,
    /// the ADMITTED wait-stage rejection, and start/update workflow-id
    /// inconsistency all error the UPDATE op with the sibling clean.
    #[test]
    fn multi_operation_update_restrictions_produce_per_op_errors() {
        use tokeira_proto::public::temporal::api::update::v1 as update;

        let mut no_meta = multi_op_update("wf");
        no_meta.request.as_mut().unwrap().meta = None;
        let mut no_input = multi_op_update("wf");
        no_input.request.as_mut().unwrap().input = None;
        let mut unnamed = multi_op_update("wf");
        unnamed
            .request
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .name = String::new();
        let mut admitted = multi_op_update("wf");
        admitted.wait_policy = Some(update::WaitPolicy {
            lifecycle_stage: enums::UpdateWorkflowExecutionLifecycleStage::Admitted as i32,
        });
        let mut first_run = multi_op_update("wf");
        first_run.first_execution_run_id = "b4b3e1f0-0000-0000-0000-000000000000".to_string();
        let mut pinned_run = multi_op_update("wf");
        pinned_run.workflow_execution.as_mut().unwrap().run_id =
            "b4b3e1f0-0000-0000-0000-000000000000".to_string();
        let mut foreign_namespace = multi_op_update("wf");
        foreign_namespace.namespace = "other".to_string();

        let cases = vec![
            (no_meta, "Update meta is not set on request."),
            (no_input, "Update input is not set on request."),
            (unnamed, "Update name is not set on request."),
            (
                admitted,
                "UpdateWorkflowExecution does not support waiting for ADMITTED",
            ),
            (first_run, "FirstExecutionRunId is not allowed."),
            (pinned_run, "RunId is not allowed."),
            (
                foreign_namespace,
                "Operation namespace did not match request's namespace.",
            ),
            // Workflow-id mismatch is reported on the SECOND op; the start
            // op aborts as the sibling (workflow_handler.go:766-778).
            (
                multi_op_update("other-wf"),
                "WorkflowId is not consistent with previous operation(s).",
            ),
        ];
        for (update_op, expected) in cases {
            let error = multi_operation_request_to_edge(multi_op_request(
                Some(multi_op_start("wf")),
                Some(update_op),
            ))
            .unwrap_err();
            assert_eq!(
                error,
                MultiOperationRequestError::PerOperation {
                    start: None,
                    update: Some(expected.to_string()),
                }
            );
        }
    }

    /// Happy path: the composition translates, an empty update id defaults to
    /// a fresh UUIDv4 (standalone-update parity), the update Meta.identity is
    /// captured, and both legs execute in the request namespace.
    #[test]
    fn multi_operation_happy_path_translates_and_defaults_update_id() {
        let edge = multi_operation_request_to_edge(multi_op_request(
            Some(multi_op_start("wf")),
            Some(multi_op_update("wf")),
        ))
        .expect("valid composition");

        assert_eq!(edge.namespace, "default");
        assert_eq!(edge.start.workflow_id, "wf");
        assert_eq!(edge.start.namespace, "default");
        assert_eq!(edge.update.workflow_id, "wf");
        assert_eq!(edge.update.namespace, "default");
        assert_eq!(edge.update.update_name, "my-update");
        assert_eq!(edge.update_identity.as_deref(), Some("update-client"));
        Uuid::parse_str(&edge.update.update_id).expect("empty update id defaults to a UUIDv4");
    }

    /// Req 4.1-4.5 for the validation class: the structured failure carries
    /// one per-op status in request order, the clean sibling aborts with the
    /// MultiOperationExecutionAborted detail, top-level code = the failing
    /// op's, message "Update-with-Start could not be executed.".
    #[test]
    fn multi_operation_per_op_validation_error_serializes_structured_failure() {
        let status =
            multi_operation_request_error_to_status(MultiOperationRequestError::PerOperation {
                start: Some("CronSchedule is not allowed.".to_string()),
                update: None,
            });
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), "Update-with-Start could not be executed.");

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        assert_eq!(rpc_status.code, Code::InvalidArgument as i32);
        let detail = &rpc_status.details[0];
        assert_eq!(
            detail.type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.MultiOperationExecutionFailure"
        );
        let failure =
            errordetails_proto::MultiOperationExecutionFailure::decode(detail.value.as_slice())
                .expect("decode MultiOperationExecutionFailure");
        assert_eq!(failure.statuses.len(), 2);
        assert_eq!(failure.statuses[0].code, Code::InvalidArgument as i32);
        assert_eq!(failure.statuses[0].message, "CronSchedule is not allowed.");
        assert!(failure.statuses[0].details.is_empty());
        assert_eq!(failure.statuses[1].code, Code::Aborted as i32);
        assert_eq!(failure.statuses[1].message, "Operation was aborted.");
        assert_eq!(
            failure.statuses[1].details[0].type_url,
            "type.googleapis.com/temporal.api.failure.v1.MultiOperationExecutionAborted"
        );
    }

    /// Req 4.2/4.4/4.6: a start-conflict failure keeps the standalone
    /// AlreadyExists code AND its typed WorkflowExecutionAlreadyStartedFailure
    /// detail on op0, aborts op1, and surfaces ALREADY_EXISTS top-level.
    #[test]
    fn multi_operation_start_conflict_keeps_already_exists_and_typed_detail() {
        use tokeira_proto::public::temporal::api::errordetails::v1::WorkflowExecutionAlreadyStartedFailure;

        let status = multi_operation_failure_to_status(MultiOperationFailure::Start(
            crate::errors::EdgeError::WorkflowStartRejected {
                message: "Workflow execution is already running. WorkflowId: wf, RunId: r."
                    .to_string(),
                run_id: "run-123".to_string(),
            },
        ));
        assert_eq!(status.code(), Code::AlreadyExists);
        assert_eq!(status.message(), "Update-with-Start could not be executed.");

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        let failure = errordetails_proto::MultiOperationExecutionFailure::decode(
            rpc_status.details[0].value.as_slice(),
        )
        .expect("decode MultiOperationExecutionFailure");
        assert_eq!(failure.statuses[0].code, Code::AlreadyExists as i32);
        let start_detail = &failure.statuses[0].details[0];
        assert_eq!(
            start_detail.type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure"
        );
        let already_started =
            WorkflowExecutionAlreadyStartedFailure::decode(start_detail.value.as_slice())
                .expect("decode WorkflowExecutionAlreadyStartedFailure");
        assert_eq!(already_started.run_id, "run-123");
        assert_eq!(failure.statuses[1].code, Code::Aborted as i32);
    }

    /// `UpdateFailed{started: true}`: the start leg durably applied, so op0
    /// serializes as code OK (empty message, no details) while op1 carries
    /// the update's own error and drives the top-level code.
    #[test]
    fn multi_operation_update_failure_after_started_start_serializes_ok_sibling() {
        let status = multi_operation_failure_to_status(MultiOperationFailure::Update {
            started: true,
            error: crate::errors::EdgeError::NotFound(
                "workflow update was aborted by closing workflow".to_string(),
            ),
        });
        assert_eq!(status.code(), Code::NotFound);

        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        let failure = errordetails_proto::MultiOperationExecutionFailure::decode(
            rpc_status.details[0].value.as_slice(),
        )
        .expect("decode MultiOperationExecutionFailure");
        assert_eq!(failure.statuses[0].code, Code::Ok as i32);
        assert!(failure.statuses[0].message.is_empty());
        assert!(failure.statuses[0].details.is_empty());
        assert_eq!(failure.statuses[1].code, Code::NotFound as i32);
        assert_eq!(
            failure.statuses[1].message,
            "workflow update was aborted by closing workflow"
        );

        // started: false — the start leg never applied, so it aborts and the
        // top-level code is still the update's.
        let status = multi_operation_failure_to_status(MultiOperationFailure::Update {
            started: false,
            error: crate::errors::EdgeError::WorkflowClosing,
        });
        assert_eq!(status.code(), Code::ResourceExhausted);
        let rpc_status = RpcStatus::decode(status.details()).expect("decode google.rpc.Status");
        let failure = errordetails_proto::MultiOperationExecutionFailure::decode(
            rpc_status.details[0].value.as_slice(),
        )
        .expect("decode MultiOperationExecutionFailure");
        assert_eq!(failure.statuses[0].code, Code::Aborted as i32);
        assert_eq!(failure.statuses[1].code, Code::ResourceExhausted as i32);
        // The typed ResourceExhaustedFailure detail survives re-parenting.
        assert_eq!(
            failure.statuses[1].details[0].type_url,
            "type.googleapis.com/temporal.api.errordetails.v1.ResourceExhaustedFailure"
        );
    }

    /// Req 3: the success response is exactly `[start, update]` in order,
    /// with `started`/`status` from the path taken.
    #[test]
    fn multi_operation_response_serializes_ordered_start_update_pair() {
        use workflowservice::execute_multi_operation_response::response::Response as ResponseVariant;

        let run_id = tokeira_types::RunId::new();
        let resp =
            multi_operation_response_to_proto(crate::translate::ExecuteMultiOperationResponse {
                run_id,
                started: false,
                status: ExecutionStatus::Completed,
                update: crate::translate::UpdateWorkflowExecutionResponse {
                    update_ref: crate::translate::UpdateRefDto {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                        update_id: "upd-1".to_string(),
                    },
                    stage: crate::translate::UpdateLifecycleStageDto::Completed,
                    outcome: Some(crate::translate::UpdateOutcomeDto::Completed {
                        accepted_event_id: 5,
                        result: Payloads::default(),
                    }),
                },
            });

        assert_eq!(resp.responses.len(), 2);
        match resp.responses[0].response.as_ref().expect("start response") {
            ResponseVariant::StartWorkflow(start) => {
                assert_eq!(start.run_id, run_id.0.to_string());
                assert!(!start.started);
                assert_eq!(
                    start.status,
                    enums::WorkflowExecutionStatus::Completed as i32
                );
            }
            other => panic!("responses[0] must be the start response, got {other:?}"),
        }
        match resp.responses[1]
            .response
            .as_ref()
            .expect("update response")
        {
            ResponseVariant::UpdateWorkflow(update) => {
                assert_eq!(
                    update.stage,
                    enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32
                );
                assert_eq!(update.update_ref.as_ref().unwrap().update_id, "upd-1");
            }
            other => panic!("responses[1] must be the update response, got {other:?}"),
        }
    }
}
