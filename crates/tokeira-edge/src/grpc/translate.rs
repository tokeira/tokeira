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
    WorkflowCommand,
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
            failure_to_payload, headers_from_domain, headers_to_domain, memo_from_domain,
            memo_to_domain, payload_from_domain, payload_to_domain, payload_to_failure,
            payloads_from_domain, payloads_to_domain, search_attributes_from_domain,
            search_attributes_to_domain, task_queue_from_domain, task_queue_to_domain,
            to_proto_duration, to_proto_timestamp, workflow_execution_from_ids,
        },
    },
    enums,
    public::temporal::api::{
        activity::v1 as activity_proto, command::v1 as command, common::v1 as proto_common,
        compute::v1 as compute_proto, deployment::v1 as deployment_proto,
        failure::v1 as failure_proto, namespace::v1 as namespace_proto,
        replication::v1 as replication_proto, taskqueue::v1 as taskqueue_proto,
        version::v1 as version_proto, workflow::v1 as workflow,
    },
    workflowservice,
};
use tokeira_runtime::{
    AssignmentRule, ComputeConfigScalingGroupUpdate, CreateDeployment, CreateVersion,
    DeleteDeployment, DeleteVersion, DeploymentPage, DeploymentView, DescribeVersion,
    ListDeployments, NewManagerIdentity, RedirectRule, ScheduleError, SetCurrent,
    SetCurrentOutcome, SetManager, SetManagerOutcome, SetRamping, SetRampingOutcome,
    TaskReachabilityType, UpdateComputeConfig, UpdateMetadata, ValidateComputeConfig,
    VersionMetadataView, VersionView, VersioningMutation, VersioningRules, cron_initial_backoff,
};
use tokeira_storage::{
    BuildId as DeploymentBuildId, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider,
    ComputeScaler, ConflictToken, DeploymentKey, DeploymentName, DeploymentTaskQueueType,
    DrainageInfo, RoutingConfigUpdateState, StoredRoutingConfig, VersionDrainageStatus,
    VersionMetadata, WorkerDeploymentVersionKey, WorkerDeploymentVersionStatus,
};
use tokeira_types::{
    ActivityTaskToken, BuildId, DeploymentId, ExecutionStatus, NamespaceId, Payloads, RetryPolicy,
    RunId, RunKey, TaskKind, TaskQueueName, WorkflowId, WorkflowType,
};
use uuid::Uuid;

use crate::translate::{
    ActivityExecutionSummary, CompletionCallback as EdgeCompletionCallback,
    CountActivityExecutionsRequest, CountActivityExecutionsResponse,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse,
    DeleteWorkflowExecutionRequest as EdgeDeleteWorkflowExecutionRequest,
    DescribeTaskQueueRequest as EdgeDescribeTaskQueueRequest,
    DescribeTaskQueueResponse as EdgeDescribeTaskQueueResponse, DescribeWorkflowExecutionRequest,
    Link as EdgeLink, LinkWorkflowEventReference, ListActivityExecutionsRequest,
    ListActivityExecutionsResponse, ListNamespacesResponse as EdgeListNamespacesResponse,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, NamespaceDescription,
    NamespaceStateUpdate, OnConflictOptions as EdgeOnConflictOptions, PollWorkflowTaskQueueRequest,
    PollWorkflowTaskQueueResponse, Priority as EdgePriority, ProtocolMessageDto, QueryResultDto,
    RegisterNamespaceRequest as EdgeRegisterNamespaceRequest,
    ResetWorkflowExecutionRequest as EdgeResetWorkflowExecutionRequest,
    ResetWorkflowExecutionResponse as EdgeResetWorkflowExecutionResponse,
    RespondWorkflowTaskCompletedRequest, RespondWorkflowTaskCompletedResponse,
    SignalWithStartWorkflowExecutionRequest as EdgeSignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse as EdgeSignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionRequest, SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse, SystemInfo, TaskQueueConfig,
    UpdateNamespaceRequest as EdgeUpdateNamespaceRequest, UserMetadata, VersioningOverride,
    WorkflowExecutionDescription, WorkflowExecutionSummary, to_internal::namespace_id_for,
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

pub struct ParsedVersioningMutation {
    pub mutation: VersioningMutation,
    pub commit_build_id: Option<String>,
    pub commit_force: bool,
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
            tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate
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

// v1.62-sync: reads deprecated `VersioningOverride.behavior` and `.deployment`
// fields for wire-compat with v0.4-era SDK clients. v1.62 replaces the flat
// shape with a `override` oneof; migration to the new shape is tracked under
// `runtime-worker-versioning`.
#[allow(deprecated)]
fn versioning_override_to_edge(
    override_: Option<workflow::VersioningOverride>,
) -> Result<Option<VersioningOverride>, ProtoConversionError> {
    let Some(override_) = override_ else {
        return Ok(None);
    };
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
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
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

fn sticky_ttl_from_attributes(
    attrs: Option<&taskqueue_proto::StickyExecutionAttributes>,
) -> Result<Option<time::Duration>, ProtoConversionError> {
    let Some(attrs) = attrs else {
        return Ok(None);
    };
    let Some(queue) = attrs.worker_task_queue.as_ref() else {
        return Err(ProtoConversionError::MissingField(
            "RespondWorkflowTaskCompletedRequest.sticky_attributes.worker_task_queue",
        ));
    };
    if queue.name.is_empty() {
        return Err(ProtoConversionError::MissingField(
            "RespondWorkflowTaskCompletedRequest.sticky_attributes.worker_task_queue.name",
        ));
    }
    valid_non_negative_duration(
        attrs.schedule_to_start_timeout.as_ref(),
        "RespondWorkflowTaskCompletedRequest.sticky_attributes.schedule_to_start_timeout",
    )
    .map(|ttl| ttl.or(Some(time::Duration::seconds(30))))
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
        stats: None,
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
    // look up their corresponding message body.
    let mut messages_by_id: std::collections::HashMap<String, _> = req
        .messages
        .into_iter()
        .map(|m| (m.id.clone(), m))
        .collect();

    let mut commands = Vec::new();
    for cmd in req.commands {
        match proto_command_to_workflow_command(cmd) {
            Ok(WorkflowCommand::ProtocolMessage { message_id, .. }) => {
                // Resolve the body from the messages index.
                if let Some(msg) = messages_by_id.remove(&message_id) {
                    let body = msg
                        .body
                        .map(|body| body.encode_to_vec())
                        .unwrap_or_default();
                    commands.push(WorkflowCommand::ProtocolMessage {
                        message_id,
                        body: resolve_protocol_message_body(&body, msg.protocol_instance_id)?,
                    });
                }
            }
            Ok(cmd) => commands.push(cmd),
            Err(e) => return Err(e),
        }
    }

    // Remaining messages not referenced by commands go into
    // the DTO's messages field for edge-layer processing.
    let remaining_messages = messages_by_id
        .into_values()
        .map(|message| {
            let body = message
                .body
                .map(|body| body.encode_to_vec())
                .unwrap_or_default();
            Ok(ProtocolMessageDto {
                id: message.id,
                protocol_instance_id: message.protocol_instance_id,
                body,
                sequencing_event_id: match message.sequencing_id {
                    Some(
                        tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::EventId(event_id),
                    ) => Some(event_id),
                    Some(
                        tokeira_proto::public::temporal::api::protocol::v1::message::SequencingId::CommandIndex(command_index),
                    ) => Some(command_index),
                    None => None,
                },
            })
        })
        .collect::<Result<Vec<_>, ProtoConversionError>>()?;
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
    let sticky_ttl = sticky_ttl_from_attributes(req.sticky_attributes.as_ref())?;

    Ok(RespondWorkflowTaskCompletedRequest {
        task_token: req.task_token,
        identity: req.identity,
        sdk_metadata: req.sdk_metadata.map(|metadata| metadata.encode_to_vec()),
        metering_metadata: req
            .metering_metadata
            .map(|metadata| metadata.encode_to_vec()),
        worker_version: req.worker_version_stamp.map(|stamp| stamp.build_id),
        versioning_behavior: versioning_behavior_from_proto(req.versioning_behavior)?,
        deployment_version,
        worker_deployment_name,
        sticky_ttl,
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
/// `Failure`; a `Failure` outcome is mapped to `Rejected` because from the
/// kernel's perspective both represent terminal negative outcomes.
fn resolve_protocol_message_body(
    body_bytes: &[u8],
    protocol_instance_id: String,
) -> Result<tokeira_kernel::UpdateProtocolBody, ProtoConversionError> {
    use prost::Message as _;
    let any = prost_types::Any::decode(body_bytes)
        .map_err(|_| ProtoConversionError::MissingField("ProtocolMessage body decode failed"))?;
    match any.type_url.as_str() {
        url if url.ends_with("update.v1.Acceptance") => {
            Ok(tokeira_kernel::UpdateProtocolBody::Accepted {
                update_id: protocol_instance_id,
                update_name: String::new(),
                input: Payloads::default(),
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
                }),
                Some(
                    tokeira_proto::public::temporal::api::update::v1::outcome::Value::Failure(
                        failure,
                    ),
                ) => Ok(tokeira_kernel::UpdateProtocolBody::Rejected {
                    update_id: protocol_instance_id,
                    failure: failure_to_payload(&failure),
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
    Ok(CountWorkflowExecutionsRequest {
        namespace: req.namespace,
        query: non_empty(req.query),
        group_by: None,
    })
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
) -> workflowservice::StartWorkflowExecutionResponse {
    workflowservice::StartWorkflowExecutionResponse {
        run_id: resp.run_id.0.to_string(),
        started: resp.started,
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
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.payload.workflow_id),
        run_id_from_run_key(resp.payload.run_key),
    ));

    let history_bytes =
        crate::translate::history_serializer::serialize_history(&resp.payload.history);
    let history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    // Extract workflow_type from the first history event (WorkflowExecutionStarted)
    let workflow_type_name = resp
        .payload
        .history
        .first()
        .and_then(|ev| {
            if let tokeira_kernel::event::HistoryEventKind::WorkflowExecutionStarted {
                ref workflow_type,
                ..
            } = ev.kind
            {
                Some(workflow_type.0.clone())
            } else {
                None
            }
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
        next_attempt_schedule_time: None,
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
        // Start user metadata is not retained yet, so the DTO keeps it optional
        // and describe leaves the proto field default until that start state exists.
        user_metadata: None,
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
    workflow::PendingNexusOperationInfo {
        endpoint: op.endpoint.clone(),
        service: op.service.clone(),
        operation: op.operation.clone(),
        operation_id: operation_token.clone(),
        schedule_to_close_timeout: op.schedule_to_close_timeout.map(to_proto_duration),
        scheduled_time: Some(to_proto_timestamp(op.scheduled_time)),
        state: if op.started {
            enums::PendingNexusOperationState::Started as i32
        } else {
            enums::PendingNexusOperationState::Scheduled as i32
        },
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
                group_values: vec![tokeira_proto::common::Payload {
                    data: group.value.into_bytes(),
                    ..Default::default()
                }],
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
                eager_workflow_start: false,
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
) -> workflowservice::GetWorkflowExecutionHistoryResponse {
    use prost::Message;
    let history_bytes = crate::translate::history_serializer::serialize_history(&resp.history);
    let mut history = tokeira_proto::history::History::decode(&history_bytes[..]).ok();

    // HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT = 2
    if filter_type == 2
        && let Some(ref mut h) = history
    {
        h.events.retain(|event| is_close_event(event.event_type));
    }

    workflowservice::GetWorkflowExecutionHistoryResponse {
        history,
        // Only set next_page_token when there are genuinely more events to
        // paginate. For close-event filtered responses or complete histories,
        // an empty token tells the SDK "you have everything."
        next_page_token: if filter_type == 2 {
            // Close-event filter: the SDK only needs the close event(s).
            // Never paginate — return empty token so the SDK stops.
            vec![]
        } else {
            resp.next_page_token
        },
        ..Default::default()
    }
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
    })
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
        stats: None,
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
        versions_info: Default::default(),
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
    TaskQueueConfig {
        queue_rate_limit: req
            .update_queue_rate_limit
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        fairness_key_rate_limit_default: req
            .update_fairness_key_rate_limit_default
            .as_ref()
            .and_then(|update| update.rate_limit.as_ref())
            .map(|rate_limit| rate_limit.requests_per_second),
        fairness_weight_overrides: req.set_fairness_weight_overrides.clone(),
    }
}

pub fn task_queue_config_to_proto(config: TaskQueueConfig) -> taskqueue_proto::TaskQueueConfig {
    taskqueue_proto::TaskQueueConfig {
        queue_rate_limit: config.queue_rate_limit.map(rate_limit_config_to_proto),
        fairness_keys_rate_limit_default: config
            .fairness_key_rate_limit_default
            .map(rate_limit_config_to_proto),
        fairness_weight_overrides: config.fairness_weight_overrides,
    }
}

fn rate_limit_config_to_proto(requests_per_second: f32) -> taskqueue_proto::RateLimitConfig {
    taskqueue_proto::RateLimitConfig {
        rate_limit: Some(taskqueue_proto::RateLimit {
            requests_per_second,
        }),
        metadata: None,
    }
}

pub fn delete_request_to_edge(
    req: workflowservice::DeleteWorkflowExecutionRequest,
) -> Result<EdgeDeleteWorkflowExecutionRequest, ProtoConversionError> {
    let execution = req
        .workflow_execution
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "DeleteWorkflowExecutionRequest.workflow_execution",
        ))?;

    Ok(EdgeDeleteWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
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

    Ok(EdgeResetWorkflowExecutionRequest {
        namespace: req.namespace,
        workflow_id: execution.workflow_id.clone(),
        run_id: non_empty(execution.run_id.clone()),
        reason: req.reason,
        workflow_task_finish_event_id: req.workflow_task_finish_event_id,
        request_id: non_empty(req.request_id),
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
        workflow_task_timeout: proto_duration_to_time(req.workflow_task_timeout.as_ref()),
        retry_policy: req.retry_policy.as_ref().map(retry_policy_to_domain),
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

pub fn versioning_mutation_from_proto(
    operation: Option<workflowservice::update_worker_versioning_rules_request::Operation>,
    now: OffsetDateTime,
) -> Result<ParsedVersioningMutation, ProtoConversionError> {
    use workflowservice::update_worker_versioning_rules_request::Operation;

    let operation = operation.ok_or(ProtoConversionError::MissingField(
        "UpdateWorkerVersioningRulesRequest.operation",
    ))?;
    let parsed = match operation {
        Operation::InsertAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::InsertAssignmentRule {
                rule: assignment_rule_from_proto(op.rule, now)?,
                index: op.rule_index.max(0) as usize,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::ReplaceAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::ReplaceAssignmentRule {
                rule: assignment_rule_from_proto(op.rule, now)?,
                index: op.rule_index.max(0) as usize,
                force: op.force,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::DeleteAssignmentRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::DeleteAssignmentRule {
                index: op.rule_index.max(0) as usize,
                force: op.force,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::AddCompatibleRedirectRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::AddRedirectRule {
                rule: redirect_rule_from_proto(op.rule, now)?,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::ReplaceCompatibleRedirectRule(op) => {
            let rule = redirect_rule_from_proto(op.rule, now)?;
            ParsedVersioningMutation {
                mutation: VersioningMutation::ReplaceRedirectRule {
                    source_build_id: rule.source_build_id.clone(),
                    rule,
                },
                commit_build_id: None,
                commit_force: false,
            }
        }
        Operation::DeleteCompatibleRedirectRule(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::DeleteRedirectRule {
                source_build_id: op.source_build_id,
            },
            commit_build_id: None,
            commit_force: false,
        },
        Operation::CommitBuildId(op) => ParsedVersioningMutation {
            mutation: VersioningMutation::CommitBuildId {
                build_id: op.target_build_id.clone(),
            },
            commit_build_id: Some(op.target_build_id),
            commit_force: op.force,
        },
    };
    Ok(parsed)
}

fn assignment_rule_from_proto(
    rule: Option<taskqueue_proto::BuildIdAssignmentRule>,
    now: OffsetDateTime,
) -> Result<AssignmentRule, ProtoConversionError> {
    let rule = rule.ok_or(ProtoConversionError::MissingField(
        "BuildIdAssignmentRule.rule",
    ))?;
    let percentage_ramp = rule.ramp.map(|ramp| match ramp {
        taskqueue_proto::build_id_assignment_rule::Ramp::PercentageRamp(ramp) => {
            ramp.ramp_percentage
        }
    });
    Ok(AssignmentRule {
        target_build_id: rule.target_build_id,
        percentage_ramp,
        create_time: now,
    })
}

fn redirect_rule_from_proto(
    rule: Option<taskqueue_proto::CompatibleBuildIdRedirectRule>,
    now: OffsetDateTime,
) -> Result<RedirectRule, ProtoConversionError> {
    let rule = rule.ok_or(ProtoConversionError::MissingField(
        "CompatibleBuildIdRedirectRule.rule",
    ))?;
    Ok(RedirectRule {
        source_build_id: rule.source_build_id,
        target_build_id: rule.target_build_id,
        create_time: now,
    })
}

pub fn versioning_rules_to_update_proto(
    rules: VersioningRules,
) -> workflowservice::UpdateWorkerVersioningRulesResponse {
    workflowservice::UpdateWorkerVersioningRulesResponse {
        assignment_rules: assignment_rules_to_proto(&rules.assignment_rules),
        compatible_redirect_rules: redirect_rules_to_proto(&rules.redirect_rules),
        conflict_token: rules.conflict_token,
    }
}

pub fn versioning_rules_to_get_proto(
    rules: VersioningRules,
) -> workflowservice::GetWorkerVersioningRulesResponse {
    workflowservice::GetWorkerVersioningRulesResponse {
        assignment_rules: assignment_rules_to_proto(&rules.assignment_rules),
        compatible_redirect_rules: redirect_rules_to_proto(&rules.redirect_rules),
        conflict_token: rules.conflict_token,
    }
}

fn assignment_rules_to_proto(
    rules: &[AssignmentRule],
) -> Vec<taskqueue_proto::TimestampedBuildIdAssignmentRule> {
    rules
        .iter()
        .map(|rule| taskqueue_proto::TimestampedBuildIdAssignmentRule {
            rule: Some(taskqueue_proto::BuildIdAssignmentRule {
                target_build_id: rule.target_build_id.clone(),
                ramp: rule.percentage_ramp.map(|percentage| {
                    taskqueue_proto::build_id_assignment_rule::Ramp::PercentageRamp(
                        taskqueue_proto::RampByPercentage {
                            ramp_percentage: percentage,
                        },
                    )
                }),
            }),
            create_time: Some(to_proto_timestamp(rule.create_time)),
        })
        .collect()
}

fn redirect_rules_to_proto(
    rules: &[RedirectRule],
) -> Vec<taskqueue_proto::TimestampedCompatibleBuildIdRedirectRule> {
    rules
        .iter()
        .map(
            |rule| taskqueue_proto::TimestampedCompatibleBuildIdRedirectRule {
                rule: Some(taskqueue_proto::CompatibleBuildIdRedirectRule {
                    source_build_id: rule.source_build_id.clone(),
                    target_build_id: rule.target_build_id.clone(),
                }),
                create_time: Some(to_proto_timestamp(rule.create_time)),
            },
        )
        .collect()
}

pub fn reachability_to_proto(
    results: Vec<tokeira_runtime::BuildIdReachabilityResult>,
) -> workflowservice::GetWorkerTaskReachabilityResponse {
    workflowservice::GetWorkerTaskReachabilityResponse {
        build_id_reachability: results
            .into_iter()
            .map(|result| taskqueue_proto::BuildIdReachability {
                build_id: result.build_id,
                task_queue_reachability: result
                    .task_queue_reachability
                    .into_iter()
                    .map(|queue| taskqueue_proto::TaskQueueReachability {
                        task_queue: queue.task_queue.0,
                        reachability: queue
                            .reachability
                            .into_iter()
                            .map(|reachability| match reachability {
                                TaskReachabilityType::NewWorkflows => {
                                    enums::TaskReachability::NewWorkflows as i32
                                }
                                TaskReachabilityType::ExistingWorkflows => {
                                    enums::TaskReachability::ExistingWorkflows as i32
                                }
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
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
pub fn proto_command_to_workflow_command(
    cmd: command::Command,
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
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: proto_duration_to_time(
                    attrs.schedule_to_close_timeout.as_ref(),
                ),
                schedule_to_start_timeout: proto_duration_to_time(
                    attrs.schedule_to_start_timeout.as_ref(),
                ),
                start_to_close_timeout: proto_duration_to_time(
                    attrs.start_to_close_timeout.as_ref(),
                ),
                heartbeat_timeout: proto_duration_to_time(attrs.heartbeat_timeout.as_ref()),
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
            Ok(WorkflowCommand::UpsertSearchAttributes(
                attrs
                    .search_attributes
                    .as_ref()
                    .map(search_attributes_to_domain)
                    .transpose()?
                    .unwrap_or_default(),
            ))
        }
        Some(Attributes::ModifyWorkflowPropertiesCommandAttributes(attrs)) => {
            Ok(WorkflowCommand::UpsertMemo(
                attrs
                    .upserted_memo
                    .as_ref()
                    .map(memo_to_domain)
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
            Ok(WorkflowCommand::RequestCancelActivity {
                activity_id: attrs.scheduled_event_id.to_string(),
            })
        }
        Some(Attributes::CancelTimerCommandAttributes(attrs)) => Ok(WorkflowCommand::CancelTimer {
            timer_id: attrs.timer_id,
        }),
        Some(Attributes::CancelWorkflowExecutionCommandAttributes(_attrs)) => {
            Ok(WorkflowCommand::CancelWorkflow)
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
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "ContinueAsNewWorkflowExecutionCommandAttributes.task_queue",
                    ))?;
            Ok(WorkflowCommand::ContinueAsNew {
                new_run_id: RunId::new(),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue: task_queue_to_domain(task_queue),
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
                workflow_task_timeout: proto_duration_to_time(attrs.workflow_task_timeout.as_ref())
                    .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
            })
        }
        Some(Attributes::StartChildWorkflowExecutionCommandAttributes(attrs)) => {
            let task_queue =
                attrs
                    .task_queue
                    .as_ref()
                    .ok_or(ProtoConversionError::MissingField(
                        "StartChildWorkflowExecutionCommandAttributes.task_queue",
                    ))?;
            Ok(WorkflowCommand::StartChildWorkflow {
                child_workflow_id: WorkflowId(attrs.workflow_id),
                namespace_id: namespace_name_to_domain(&attrs.namespace),
                namespace: non_empty(attrs.namespace),
                workflow_type: WorkflowType(
                    attrs
                        .workflow_type
                        .as_ref()
                        .map(|workflow_type| workflow_type.name.clone())
                        .unwrap_or_default(),
                ),
                task_queue: task_queue_to_domain(task_queue),
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
                workflow_task_timeout: proto_duration_to_time(attrs.workflow_task_timeout.as_ref())
                    .unwrap_or(time::Duration::seconds(10)),
                retry_policy: attrs.retry_policy.as_ref().map(retry_policy_to_domain),
                cron_schedule: non_empty(attrs.cron_schedule),
                parent_close_policy: parent_close_policy_to_domain(attrs.parent_close_policy),
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
        WorkflowCommand::RequestCancelActivity { activity_id } => {
            Some(Attributes::RequestCancelActivityTaskCommandAttributes(
                command::RequestCancelActivityTaskCommandAttributes {
                    scheduled_event_id: activity_id.parse::<i64>().unwrap_or_default(),
                },
            ))
        }
        WorkflowCommand::CancelTimer { timer_id } => Some(
            Attributes::CancelTimerCommandAttributes(command::CancelTimerCommandAttributes {
                timer_id: timer_id.clone(),
            }),
        ),
        WorkflowCommand::CancelWorkflow => {
            Some(Attributes::CancelWorkflowExecutionCommandAttributes(
                command::CancelWorkflowExecutionCommandAttributes::default(),
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
        | WorkflowCommand::RequestNewWorkflowTask => {
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
        versioning_info: value
            .versioning_info
            .as_ref()
            .map(workflow_versioning_info_from_edge),
        worker_deployment_name: value.worker_deployment_name.clone().unwrap_or_default(),
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
        execution_time: None,
        close_time: value.close_time.map(to_proto_timestamp),
        history_length: value.history_length,
        state_transition_count: value.state_transition_count,
        memo: Some(memo_from_domain(&value.memo)),
        search_attributes: Some(search_attributes_from_domain(&value.search_attributes)),
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

fn execution_status_to_proto(value: ExecutionStatus) -> i32 {
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

fn run_id_from_run_key(run_key: RunKey) -> RunId {
    RunId(run_key.0)
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

pub fn poll_activity_request_to_edge(
    req: workflowservice::PollActivityTaskQueueRequest,
) -> Result<crate::translate::PollActivityTaskQueueRequest, ProtoConversionError> {
    let task_queue = req
        .task_queue
        .as_ref()
        .ok_or(ProtoConversionError::MissingField(
            "PollActivityTaskQueueRequest.task_queue",
        ))?;

    Ok(crate::translate::PollActivityTaskQueueRequest {
        namespace: req.namespace,
        task_queue: task_queue.name.clone(),
        worker_identity: req.identity,
        timeout: DEFAULT_POLL_TIMEOUT,
    })
}

pub fn poll_activity_response_to_proto(
    resp: crate::translate::PollActivityTaskQueueResponse,
) -> workflowservice::PollActivityTaskQueueResponse {
    let workflow_execution = Some(workflow_execution_from_ids(
        &WorkflowId(resp.workflow_id),
        run_id_from_run_key(resp.run_key),
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
    use tokeira_runtime::{RedirectRule, VersioningRules};

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
        assert_eq!(edge.sticky_ttl, Some(time::Duration::seconds(17)));
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
    fn respond_completed_request_rejects_sticky_attributes_without_task_queue() {
        let status = proto_conversion_status(
            respond_completed_request_to_edge(
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
            .unwrap_err(),
        );

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
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

        assert_eq!(edge.worker_version.as_deref(), Some("legacy-build"));
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
    fn versioning_rules_proto_emits_redirect_create_time() {
        let create_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(42);
        let proto = versioning_rules_to_get_proto(VersioningRules {
            assignment_rules: Vec::new(),
            redirect_rules: vec![RedirectRule {
                source_build_id: "old".to_string(),
                target_build_id: "new".to_string(),
                create_time,
            }],
            conflict_token: 7_u64.to_be_bytes().to_vec(),
        });

        assert_eq!(proto.compatible_redirect_rules.len(), 1);
        assert_eq!(
            proto.compatible_redirect_rules[0].create_time,
            Some(to_proto_timestamp(create_time))
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
            .standalone_activities
        };
        // The capability tracks the server-uniform flag (Req 13.4), not a constant.
        assert!(describe(true));
        assert!(!describe(false));
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
            request_id_infos: std::collections::BTreeMap::new(),
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
        let err = proto_command_to_workflow_command(command::Command {
            attributes: None,
            ..Default::default()
        })
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
        }]);
        let scheduled = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let current_attempt = OffsetDateTime::from_unix_timestamp(200).unwrap();
        let started = OffsetDateTime::from_unix_timestamp(250).unwrap();

        let proto =
            poll_activity_response_to_proto(crate::translate::PollActivityTaskQueueResponse {
                task_token: b"token".to_vec(),
                activity_id: "activity-1".to_string(),
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
    }

    #[test]
    fn workflow_poll_response_projects_legacy_query_field() {
        let payloads = Payloads(vec![tokeira_types::Payload {
            data: b"input".to_vec(),
            metadata: Default::default(),
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
                task_queue: "main".to_string(),
                history: Vec::new(),
            },
            query: Some(crate::translate::WorkflowQueryDto {
                query_type: "state".to_string(),
                query_args: payloads.clone(),
            }),
            queries: Default::default(),
            messages: Vec::new(),
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
        let edge = proto_command_to_workflow_command(proto_cmd).unwrap();
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
        };
        let decoded = payload_to_failure(&corrupted);
        assert_eq!(decoded.message, "garbage bytes");
        assert!(decoded.failure_info.is_none());
    }
}
