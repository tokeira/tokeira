//! gRPC transport adapter for the Temporal `WorkflowService` API.
//!
//! This module is a thin tonic shim: it deserialises proto requests into
//! edge-layer DTOs, delegates to [`WorkflowService`], and serialises the
//! response back to proto. No business logic lives here — the translate
//! layer owns field mapping and the edge `WorkflowService` owns
//! orchestration.

use std::{collections::BTreeMap, sync::Arc};

use prost::Message as _;
use tonic::{Request, Response, Status, codec::CompressionEncoding};
use tracing::debug;

use time::OffsetDateTime;
use tokeira_projection::{STANDARD_SEARCH_ATTRIBUTES, SearchAttrType};
use tokeira_proto::{
    enums::IndexedValueType,
    workflowservice::{
        self,
        workflow_service_server::{
            WorkflowService as WorkflowServiceGrpcApi, WorkflowServiceServer,
        },
    },
};
use tokeira_runtime::{
    ScheduleError, TaskQueueConfigEntry, compute_matching_times, compute_next_times,
};
use tokeira_types::{NamespaceId, TaskQueueName, WorkerIdentity};

use tokeira_chasm_activity::ActivityStatus;
use tokeira_proto::public::temporal::api::{activity::v1 as activity_v1, worker::v1 as worker_v1};

use crate::{
    Action,
    grpc::{
        errors::{
            proto_conversion_status, worker_versioning_v1_disabled_status,
            worker_versioning_v2_disabled_status, workflow_already_started_status,
        },
        metadata::metadata_to_header_map,
        translate,
    },
    translate::{batch, nexus, schedule, to_internal, worker_heartbeat},
    workflow_service::{
        WorkflowService, task_queue_config_metadata_to_edge, task_queue_config_metadata_to_runtime,
    },
};

const DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED: &str =
    "Deployments are deprecated and no longer supported, use Worker Deployments instead";

fn standard_search_attributes() -> BTreeMap<String, i32> {
    // Temporal returns `NameTypeMap.All()` from WorkflowService
    // `GetSearchAttributes`, which includes `sadefs.System()` and
    // `sadefs.Predefined()` before custom attributes
    // (`service/frontend/workflow_handler.go:2874` and
    // `common/searchattribute/name_type_map.go @ v1.31.0`).
    STANDARD_SEARCH_ATTRIBUTES
        .iter()
        .map(|attr| {
            (
                attr.name.to_owned(),
                indexed_value_type_from_projection(attr.attr_type) as i32,
            )
        })
        .collect()
}

fn indexed_value_type_from_projection(value: SearchAttrType) -> IndexedValueType {
    match value {
        SearchAttrType::Keyword => IndexedValueType::Keyword,
        SearchAttrType::KeywordList => IndexedValueType::KeywordList,
        SearchAttrType::Int => IndexedValueType::Int,
        SearchAttrType::Bool => IndexedValueType::Bool,
        SearchAttrType::Double => IndexedValueType::Double,
        SearchAttrType::Datetime => IndexedValueType::Datetime,
        SearchAttrType::Text => IndexedValueType::Text,
    }
}

fn indexed_value_type_from_edge(value: &str) -> Result<IndexedValueType, Status> {
    match value {
        "keyword" | "INDEXED_VALUE_TYPE_KEYWORD" => Ok(IndexedValueType::Keyword),
        "keyword_list" | "INDEXED_VALUE_TYPE_KEYWORD_LIST" => Ok(IndexedValueType::KeywordList),
        "int" | "INDEXED_VALUE_TYPE_INT" => Ok(IndexedValueType::Int),
        "bool" | "INDEXED_VALUE_TYPE_BOOL" => Ok(IndexedValueType::Bool),
        "double" | "INDEXED_VALUE_TYPE_DOUBLE" => Ok(IndexedValueType::Double),
        "datetime" | "INDEXED_VALUE_TYPE_DATETIME" => Ok(IndexedValueType::Datetime),
        "text" | "INDEXED_VALUE_TYPE_TEXT" => Ok(IndexedValueType::Text),
        other => Err(Status::internal(format!(
            "unsupported search attribute type `{other}`"
        ))),
    }
}

/// Tonic service implementation that bridges proto ↔ edge DTOs.
///
/// Each handler follows the same pattern: extract headers, translate the
/// request, delegate to `WorkflowService`, translate the response. Keeping
/// this layer mechanical makes it easy to audit proto field coverage.
#[derive(Clone, Debug)]
pub struct WorkflowServiceGrpc {
    inner: WorkflowService,
    /// Optional standalone-activity bridge. Present only once `tokeirad` has
    /// constructed a CHASM engine and attached it via [`Self::with_chasm_activity`].
    /// When absent, the `*ActivityExecution` RPCs answer `UNIMPLEMENTED` (deferred),
    /// distinct from the per-namespace enable gate the bridge itself enforces.
    chasm_activity: Option<Arc<crate::chasm_activity::ActivityBridge>>,
}

impl WorkflowServiceGrpc {
    pub fn new(inner: WorkflowService) -> Self {
        Self {
            inner,
            chasm_activity: None,
        }
    }

    /// Attach the standalone-activity bridge. Builder form so `tokeirad` wires the
    /// CHASM engine in at bootstrap without threading it through every call site.
    pub fn with_chasm_activity(
        mut self,
        bridge: Arc<crate::chasm_activity::ActivityBridge>,
    ) -> Self {
        self.chasm_activity = Some(bridge);
        self
    }

    pub fn into_service(self) -> WorkflowServiceServer<Self> {
        // Accept (and send) gzip: the Temporal SDKs compress requests by default —
        // the Python SDK defaults to GrpcCompression.GZIP — so a server that does not
        // negotiate gzip rejects unmodified SDK traffic with "Content is compressed
        // with 'gzip' which isn't supported". `send_compressed` only compresses a
        // response when the caller advertises `grpc-accept-encoding`, matching
        // Temporal's behaviour. Identity (uncompressed) callers are unaffected.
        WorkflowServiceServer::new(self)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    }

    /// Whether standalone activities are available on this server (the
    /// server-uniform `standalone_activities` namespace capability, Req 13.4): the
    /// bridge is wired and its `enableStandalone` gate is on.
    fn standalone_activities_enabled(&self) -> bool {
        self.chasm_activity
            .as_ref()
            .is_some_and(|bridge| bridge.is_enabled())
    }

    async fn resolve_namespace_id(&self, namespace: &str) -> Result<NamespaceId, Status> {
        // An empty namespace is rejected InvalidArgument "Namespace is empty." before
        // any lookup, matching v1.31.0's namespace registry
        // (`common/namespace/nsregistry/registry.go:313 @ v1.31.0`;
        // `standalone_activity_test.go:3964`). Without this the empty name falls
        // through to a NotFound, the wrong status class.
        if namespace.is_empty() {
            return Err(Status::invalid_argument("Namespace is empty."));
        }
        self.inner
            .resolve_namespace_id(namespace)
            .await
            .map_err(namespace_resolution_status)
    }

    /// Build a CHASM [`tokeira_chasm::ExecutionKey`] for a standalone-activity
    /// cancel/terminate/delete operation.
    ///
    /// MVP deviation: the public proto allows an empty `run_id` to mean "the
    /// latest run", but the CHASM bridge addresses an execution by its exact key
    /// and no latest-run index exists yet. An empty `run_id` is therefore
    /// rejected with `INVALID_ARGUMENT` rather than silently targeting the wrong
    /// run; clients pass the `run_id` returned by `StartActivityExecution`.
    async fn activity_execution_key(
        &self,
        bridge: &crate::chasm_activity::ActivityBridge,
        namespace: &str,
        activity_id: String,
        run_id: String,
    ) -> Result<tokeira_chasm::ExecutionKey, Status> {
        let namespace_id = self.resolve_namespace_id(namespace).await?;
        let resolved_run_id = if run_id.is_empty() {
            // Bare-id request: resolve the current run via the authoritative
            // current-run pointer (`activity-executions-first-class` Req 1). No current
            // run names the activity id in a NotFound, mirroring v1.31.0's
            // `frontend.go` (the message already lands via `map_activity_not_found`).
            match bridge
                .current_run(&namespace_id.0.to_string(), &activity_id)
                .await?
            {
                Some(run) => run,
                None => {
                    return Err(Status::not_found(format!(
                        "activity not found for ID: {activity_id}"
                    )));
                }
            }
        } else {
            run_id
        };
        Ok(tokeira_chasm::ExecutionKey::new(
            namespace_id.0.to_string(),
            activity_id,
            resolved_run_id,
        ))
    }
}

fn schedule_error_status(error: ScheduleError) -> Status {
    match error {
        ScheduleError::AlreadyExists => Status::already_exists(error.to_string()),
        ScheduleError::NotFound => Status::not_found(error.to_string()),
        ScheduleError::StaleConflictToken => Status::failed_precondition(error.to_string()),
        ScheduleError::InvalidArgument(message) => Status::invalid_argument(message),
    }
}

fn batch_translate_error_status(error: batch::BatchTranslateError) -> Status {
    match error {
        batch::BatchTranslateError::MissingField(_)
        | batch::BatchTranslateError::InvalidArgument(_) => {
            Status::invalid_argument(error.to_string())
        }
        batch::BatchTranslateError::Unsupported(_) => Status::invalid_argument(error.to_string()),
    }
}

fn nexus_translate_error_status(error: nexus::NexusTranslateError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn namespace_resolution_status(error: crate::errors::EdgeError) -> Status {
    match error {
        // Mirror v1.31.0's `serviceerror.NamespaceNotFound`: "Namespace %s is not
        // found." carrying the requested name (`go.temporal.io/api/serviceerror/
        // namespace_not_found.go @ v1.31.0`; `standalone_activity_test.go:3882`).
        crate::errors::EdgeError::NamespaceNotFound(name) => {
            crate::grpc::errors::namespace_not_found_status(name)
        }
        crate::errors::EdgeError::NamespaceDeleted(_) => {
            Status::failed_precondition("namespace is deleted")
        }
        other => Status::from(other),
    }
}

fn proto_timestamp_to_time(value: &prost_types::Timestamp) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp(value.seconds)
        .ok()
        .map(|time| time + time::Duration::nanoseconds(i64::from(value.nanos)))
}

/// Convert an optional proto `Duration` to whole nanoseconds, treating an absent
/// value as `0` (the bridge's "unset" sentinel; normalization applies the real
/// defaults). Saturating arithmetic keeps a hostile or overflowing duration from
/// panicking the handler.
fn proto_duration_to_nanos(value: Option<&prost_types::Duration>) -> i64 {
    match value {
        Some(duration) => duration
            .seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(duration.nanos)),
        None => 0,
    }
}

/// Return the normalized standalone-activity retry policy that v1.31.0 persists and
/// exposes through Describe. Tokeira uses the release defaults as constants rather
/// than dynamic configuration (`chasm/lib/activity/frontend.go:362-419 @ v1.31.0`).
fn defaulted_retry_policy(
    policy: Option<&tokeira_proto::common::RetryPolicy>,
) -> tokeira_proto::common::RetryPolicy {
    // EnsureDefaults: InitialInterval 1s, BackoffCoefficient 2.0, MaximumInterval
    // 100 × InitialInterval, MaximumAttempts 0 (unlimited).
    let mut normalized = policy.cloned().unwrap_or_default();
    if proto_duration_to_nanos(normalized.initial_interval.as_ref()) == 0 {
        normalized.initial_interval = Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        });
    }
    if normalized.backoff_coefficient == 0.0 {
        normalized.backoff_coefficient = 2.0;
    }
    if proto_duration_to_nanos(normalized.maximum_interval.as_ref()) == 0 {
        // DefaultDefaultRetrySettings.MaximumIntervalCoefficient = 100.
        let maximum =
            proto_duration_to_nanos(normalized.initial_interval.as_ref()).saturating_mul(100);
        normalized.maximum_interval = nanos_to_proto_duration(maximum);
    }
    normalized
}

/// Build a `PollActivityTaskQueueResponse` for a standalone-activity task served
/// from the CHASM bridge. Only the fields meaningful for a standalone activity are
/// populated; `activity_run_id` carries the run id (field 20, "only set for
/// standalone activities"), and the stored input is decoded back into the
/// `Payloads` envelope the start request carried.
fn chasm_activity_poll_response(
    namespace: &str,
    task: crate::chasm_activity::PolledActivityTask,
) -> workflowservice::PollActivityTaskQueueResponse {
    workflowservice::PollActivityTaskQueueResponse {
        task_token: task.task_token,
        workflow_namespace: namespace.to_owned(),
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: task.activity_type,
        }),
        activity_id: task.activity_id,
        input: tokeira_proto::common::Payloads::decode(task.input.as_slice()).ok(),
        attempt: task.attempt,
        activity_run_id: task.run_id,
        // Timeouts / priority / header / times the worker needs to honor the task,
        // echoed verbatim from the started activity (`standalone_activity_test.go:322-329`).
        schedule_to_close_timeout: nanos_to_proto_duration(task.schedule_to_close_nanos),
        start_to_close_timeout: nanos_to_proto_duration(task.start_to_close_nanos),
        heartbeat_timeout: nanos_to_proto_duration(task.heartbeat_nanos),
        scheduled_time: nanos_to_proto_timestamp(task.scheduled_time_nanos),
        started_time: nanos_to_proto_timestamp(task.started_time_nanos),
        // Retry attempts are scheduled from the prior completion plus backoff, while
        // `scheduled_time` remains the original execution schedule.
        current_attempt_scheduled_time: nanos_to_proto_timestamp(task.attempt_scheduled_time_nanos),
        priority: decode_echo(&task.priority),
        header: decode_echo(&task.header),
        heartbeat_details: (!task.heartbeat_details.is_empty())
            .then(|| {
                tokeira_proto::common::Payloads::decode(task.heartbeat_details.as_slice()).ok()
            })
            .flatten(),
        ..Default::default()
    }
}

/// Map an internal activity status to the public `ActivityExecutionStatus`
/// (`enums/v1/activity.proto @ v1.31.0`): the three running sub-states collapse to
/// `RUNNING` (their breakdown rides `run_state`), and each terminal maps to its
/// matching status.
fn activity_execution_status(
    status: ActivityStatus,
) -> tokeira_proto::enums::ActivityExecutionStatus {
    use tokeira_proto::enums::ActivityExecutionStatus as Status;
    match status {
        ActivityStatus::Unspecified => Status::Unspecified,
        ActivityStatus::Scheduled | ActivityStatus::Started | ActivityStatus::CancelRequested => {
            Status::Running
        }
        ActivityStatus::Completed => Status::Completed,
        ActivityStatus::Failed => Status::Failed,
        ActivityStatus::Canceled => Status::Canceled,
        ActivityStatus::Terminated => Status::Terminated,
        ActivityStatus::TimedOut => Status::TimedOut,
    }
}

/// The `RUNNING` breakdown (`PendingActivityState`) for a non-terminal activity;
/// `Unspecified` for the terminal and pre-scheduled states.
fn pending_activity_state(status: ActivityStatus) -> tokeira_proto::enums::PendingActivityState {
    use tokeira_proto::enums::PendingActivityState as State;
    match status {
        ActivityStatus::Scheduled => State::Scheduled,
        ActivityStatus::Started => State::Started,
        ActivityStatus::CancelRequested => State::CancelRequested,
        _ => State::Unspecified,
    }
}

/// Convert whole nanoseconds to an optional proto `Duration`; `None` for unset
/// (`<= 0`).
fn nanos_to_proto_duration(nanos: i64) -> Option<prost_types::Duration> {
    (nanos > 0).then_some(prost_types::Duration {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

/// Convert a normalized timeout to a proto duration while retaining an explicit
/// zero. Standalone Describe always populates its timeout messages, including
/// unset values normalized to zero (`chasm/lib/activity/activity.go @ v1.31.0`).
fn timeout_to_proto_duration(nanos: i64) -> Option<prost_types::Duration> {
    let nanos = nanos.max(0);
    Some(prost_types::Duration {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

/// Convert whole nanoseconds to an optional proto `Timestamp`; `None` for unset
/// (`<= 0`).
fn nanos_to_proto_timestamp(nanos: i64) -> Option<prost_types::Timestamp> {
    (nanos > 0).then_some(prost_types::Timestamp {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

/// Decode an optional describe-echo proto from the bytes the activity component
/// stored (empty → `None`). The stored bytes were encoded from the same proto type
/// the `ActivityExecutionInfo` field expects, so a non-empty buffer round-trips; a
/// corrupt buffer degrades to `None` rather than failing the read (the field is
/// observational, not on the correctness path).
fn decode_echo<T: prost::Message + Default>(bytes: &[u8]) -> Option<T> {
    if bytes.is_empty() {
        return None;
    }
    T::decode(bytes).ok()
}

/// The terminal outcome (`ActivityExecutionOutcome`) for a closed activity, or
/// `None` while it is still running. Mirrors the v1.31.0 outcomes
/// (`chasm/lib/activity/statemachine.go @ v1.31.0`):
/// - `Completed` → the result `Payloads`.
/// - `Failed` → the worker's full `Failure` (round-tripped from the stored payload;
///   falls back to a message-only failure if none was captured).
/// - `Terminated` → a `Failure` carrying `TerminatedFailureInfo` (`TransitionTerminated`).
/// - `Canceled` → a `Failure` carrying `CanceledFailureInfo` (`TransitionCanceled`).
/// - `TimedOut` → a `Failure` carrying `TimeoutFailureInfo` with the fired timeout
///   type (built when the timeout fired; `chasm-activity-timeouts-and-retry`).
fn chasm_activity_outcome(
    description: &crate::chasm_activity::ActivityDescription,
) -> Option<activity_v1::ActivityExecutionOutcome> {
    use activity_v1::activity_execution_outcome::Value;
    use tokeira_proto::failure::{
        CanceledFailureInfo, Failure, TerminatedFailureInfo, failure::FailureInfo,
    };
    let value = match description.status {
        ActivityStatus::Completed => Value::Result(
            tokeira_proto::common::Payloads::decode(description.result.as_slice())
                .unwrap_or_default(),
        ),
        // The worker reported a structured Failure; round-trip it verbatim. An empty
        // or corrupt payload degrades to a message-only failure.
        ActivityStatus::Failed => {
            let failure = (!description.failure_payload.is_empty())
                .then(|| Failure::decode(description.failure_payload.as_slice()).ok())
                .flatten()
                .unwrap_or_else(|| Failure {
                    message: description.failure.clone(),
                    ..Default::default()
                });
            Value::Failure(failure)
        }
        ActivityStatus::Terminated => Value::Failure(Failure {
            message: description.failure.clone(),
            failure_info: Some(FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo {
                identity: description.terminate_identity.clone(),
            })),
            ..Default::default()
        }),
        ActivityStatus::Canceled => Value::Failure(Failure {
            message: description.failure.clone(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: decode_echo(&description.canceled_details),
                identity: description.cancel_identity.clone(),
            })),
            ..Default::default()
        }),
        // A timeout records a structured `Failure` (with `TimeoutFailureInfo`) as its
        // failure_payload, built when the timeout fired; round-trip it so the outcome
        // carries the timeout type (`standalone_activity_test.go:4509`). An empty or
        // corrupt payload degrades to a message-only failure.
        ActivityStatus::TimedOut => {
            let failure = (!description.failure_payload.is_empty())
                .then(|| Failure::decode(description.failure_payload.as_slice()).ok())
                .flatten()
                .unwrap_or_else(|| Failure {
                    message: description.failure.clone(),
                    ..Default::default()
                });
            Value::Failure(failure)
        }
        _ => return None,
    };
    Some(activity_v1::ActivityExecutionOutcome { value: Some(value) })
}

/// The `info.last_failure` for a closed-with-failure activity: the worker's full
/// `Failure` when one was captured, else a message-only failure, else `None` for an
/// activity that has not recorded a failure. Keeps `last_failure` consistent with
/// the outcome's failure (Req 5).
fn chasm_last_failure(
    description: &crate::chasm_activity::ActivityDescription,
) -> Option<tokeira_proto::failure::Failure> {
    // Termination/cancellation have terminal outcome failures but are not failed
    // attempts, so v1.31.0 leaves `info.last_failure` unset.
    if matches!(
        description.status,
        ActivityStatus::Terminated | ActivityStatus::Canceled
    ) {
        return None;
    }
    if !description.failure_payload.is_empty()
        && let Ok(failure) =
            tokeira_proto::failure::Failure::decode(description.failure_payload.as_slice())
    {
        return Some(failure);
    }
    (!description.failure.is_empty()).then(|| tokeira_proto::failure::Failure {
        message: description.failure.clone(),
        ..Default::default()
    })
}

/// Build the `ActivityExecutionInfo` projection of an activity description.
fn chasm_activity_info(
    activity_id: String,
    run_id: String,
    description: &crate::chasm_activity::ActivityDescription,
) -> activity_v1::ActivityExecutionInfo {
    activity_v1::ActivityExecutionInfo {
        activity_id,
        run_id,
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: description.activity_type.clone(),
        }),
        status: activity_execution_status(description.status) as i32,
        run_state: pending_activity_state(description.status) as i32,
        task_queue: description.task_queue.clone(),
        schedule_to_close_timeout: timeout_to_proto_duration(description.schedule_to_close_nanos),
        schedule_to_start_timeout: timeout_to_proto_duration(description.schedule_to_start_nanos),
        start_to_close_timeout: timeout_to_proto_duration(description.start_to_close_nanos),
        heartbeat_timeout: timeout_to_proto_duration(description.heartbeat_nanos),
        attempt: description.attempt,
        schedule_time: nanos_to_proto_timestamp(description.scheduled_time_nanos),
        // schedule_time + schedule_to_close_timeout, populated only when that timeout
        // is set (`chasm/lib/activity/activity.go:656-658 @ v1.31.0`; FullResponse
        // asserts it, `standalone_activity_test.go:2846`).
        expiration_time: (description.schedule_to_close_nanos > 0)
            .then(|| {
                nanos_to_proto_timestamp(
                    description.scheduled_time_nanos + description.schedule_to_close_nanos,
                )
            })
            .flatten(),
        last_started_time: nanos_to_proto_timestamp(description.started_time_nanos),
        close_time: nanos_to_proto_timestamp(description.close_time_nanos),
        // Populated only when the activity is closed: close − schedule, including all
        // attempts and backoff (`chasm/lib/activity/activity.go:649-652 @ v1.31.0` —
        // `CloseTime.Sub(ScheduleTime)`, set only when LifecycleState != Running).
        // While running, close_time_nanos is 0, so the saturating subtraction yields
        // 0 and `nanos_to_proto_duration` returns None — matching the "closed only"
        // proto contract (activity/v1/message.proto field 16).
        execution_duration: nanos_to_proto_duration(
            description
                .close_time_nanos
                .saturating_sub(description.scheduled_time_nanos),
        ),
        last_failure: chasm_last_failure(description),
        // The worker's last heartbeat details, captured on a fail request and echoed
        // verbatim (`activity.go:215 @ v1.31.0`; `standalone_activity_test.go:4908`).
        heartbeat_details: decode_echo(&description.heartbeat_details),
        // The reason recorded on the cancel request (`ActivityCancelState.reason`),
        // echoed on a CANCEL_REQUESTED describe (`standalone_activity_test.go:1313`).
        canceled_reason: description.cancel_reason.clone(),
        state_transition_count: description.execution_vt.transition_count,
        last_worker_identity: description.worker_identity.clone(),
        current_retry_interval: nanos_to_proto_duration(description.current_retry_interval_nanos),
        last_attempt_complete_time: nanos_to_proto_timestamp(
            description.last_attempt_complete_time_nanos,
        ),
        next_attempt_schedule_time: (description.status == ActivityStatus::Scheduled
            && description.attempt > 1)
            .then(|| nanos_to_proto_timestamp(description.attempt_scheduled_time_nanos))
            .flatten(),
        // Describe-echo fields stored opaque at Start and returned verbatim (Req 5).
        retry_policy: decode_echo(&description.retry_policy)
            .or_else(|| Some(defaulted_retry_policy(None))),
        priority: decode_echo(&description.priority),
        // v1.31.0 always allocates SearchAttributes for the visibility projection,
        // even when its indexed-fields map is empty (`activity.go @ v1.31.0`).
        search_attributes: decode_echo(&description.search_attributes)
            .or_else(|| Some(tokeira_proto::common::SearchAttributes::default())),
        header: decode_echo(&description.header),
        user_metadata: decode_echo(&description.user_metadata),
        ..Default::default()
    }
}

/// Validate the activity id on a standalone-activity request, mirroring v1.31.0's
/// per-RPC admission validators (`chasm/lib/activity/validator.go @ v1.31.0`). The
/// Describe/Poll/Delete/Cancel/Terminate paths use the spaced "activity ID" message
/// form (the Start path uses a distinct "activityId" message and is validated on its
/// own path). Length is compared in bytes, matching the Go `len(string)` check.
fn validate_sa_activity_id(activity_id: &str, max_id_length: usize) -> Result<(), Status> {
    if activity_id.is_empty() {
        return Err(Status::invalid_argument("activity ID is required"));
    }
    if activity_id.len() > max_id_length {
        return Err(Status::invalid_argument(format!(
            "activity ID exceeds length limit. Length={} Limit={}",
            activity_id.len(),
            max_id_length
        )));
    }
    Ok(())
}

/// Validate an optional run id on a standalone-activity request: empty is allowed
/// (the server resolves the activity's current run), but a non-empty run id must be
/// a valid UUID (`chasm/lib/activity/validator.go @ v1.31.0`).
fn validate_sa_run_id(run_id: &str) -> Result<(), Status> {
    if !run_id.is_empty() && uuid::Uuid::parse_str(run_id).is_err() {
        return Err(Status::invalid_argument(
            "invalid run id: must be a valid UUID",
        ));
    }
    Ok(())
}

/// Validate the optional `request_id` and `identity` fields on a standalone-activity
/// mutating request (cancel/terminate) against the id-length limit, with the
/// verbatim v1.31.0 messages (`chasm/lib/activity/frontend.go @ v1.31.0`). Length is
/// compared in bytes (Go `len(string)`). Skipped when standalone activities are
/// disabled — the RPC then returns `Unimplemented` downstream. (`reason` is bound by
/// `BlobSizeLimitError`, validated separately where that limit is available.)
fn validate_sa_request_metadata(
    enabled: bool,
    max_id_length: usize,
    request_id: &str,
    identity: &str,
) -> Result<(), Status> {
    if !enabled {
        return Ok(());
    }
    if request_id.len() > max_id_length {
        return Err(Status::invalid_argument(format!(
            "request ID exceeds length limit. Length={} Limit={}",
            request_id.len(),
            max_id_length
        )));
    }
    if identity.len() > max_id_length {
        return Err(Status::invalid_argument(format!(
            "identity exceeds length limit. Length={} Limit={}",
            identity.len(),
            max_id_length
        )));
    }
    Ok(())
}

/// Run the shared activity-id + run-id validators for a standalone-activity request,
/// but only when the feature is enabled. A present-but-disabled bridge must answer
/// `UNIMPLEMENTED` (the v1.31.0 baseline) regardless of request shape, so admission
/// validation deliberately does not run ahead of the enable gate.
fn validate_sa_ids(
    enabled: bool,
    max_id_length: usize,
    activity_id: &str,
    run_id: &str,
) -> Result<(), Status> {
    if enabled {
        validate_sa_activity_id(activity_id, max_id_length)?;
        validate_sa_run_id(run_id)?;
    }
    Ok(())
}

/// Parse the caller's `grpc-timeout` header into a wait budget, if present. gRPC wire
/// format is `<value><unit>` with unit ∈ {H,M,S,m,u,n}
/// (grpc HTTP/2 protocol: hours/minutes/seconds/millis/micros/nanos). Absent or
/// malformed → `None` (the long-poll then uses the full server timeout).
fn parse_grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<std::time::Duration> {
    let raw = metadata.get("grpc-timeout")?.to_str().ok()?;
    let split = raw.len().checked_sub(1)?;
    let (digits, unit) = raw.split_at(split);
    let value: u64 = digits.parse().ok()?;
    let nanos = match unit {
        "H" => value.checked_mul(3_600_000_000_000)?,
        "M" => value.checked_mul(60_000_000_000)?,
        "S" => value.checked_mul(1_000_000_000)?,
        "m" => value.checked_mul(1_000_000)?,
        "u" => value.checked_mul(1_000)?,
        "n" => value,
        _ => return None,
    };
    Some(std::time::Duration::from_nanos(nanos))
}

/// Effective describe long-poll wait: `Min(caller_deadline - buffer, long_poll_timeout)`
/// (`chasm/lib/activity/handler.go` → `contextutil.WithDeadlineBuffer` @ v1.31.0). With no
/// caller deadline, the full server timeout. A non-positive result means the caller's
/// deadline is within the buffer, so the wait returns empty immediately.
fn describe_long_poll_budget(
    caller: Option<std::time::Duration>,
    timeout: std::time::Duration,
    buffer: std::time::Duration,
) -> std::time::Duration {
    match caller {
        Some(caller) => caller.saturating_sub(buffer).min(timeout),
        None => timeout,
    }
}

/// Build a `DescribeActivityExecutionResponse`. `long_poll_token` is the caller-
/// supplied serialized `ComponentRef` (execution key + VT) the follow-on long-poll
/// resumes from; it is always present, even for a terminal activity (v1.31.0 sets
/// `ctx.Ref(a)` unconditionally — `chasm/lib/activity/activity.go:723`; the suite
/// asserts a non-nil token on a completed describe, `standalone_activity_test.go:4918`).
/// `run_id` is the resolved run (a bare-id describe echoes the current run id).
/// `input`/`outcome` are populated only when the request asked for them.
fn chasm_describe_response(
    activity_id: String,
    run_id: String,
    include_input: bool,
    include_outcome: bool,
    description: crate::chasm_activity::ActivityDescription,
    long_poll_token: Vec<u8>,
) -> workflowservice::DescribeActivityExecutionResponse {
    let input = include_input
        .then(|| tokeira_proto::common::Payloads::decode(description.input.as_slice()).ok())
        .flatten();
    let outcome = if include_outcome {
        chasm_activity_outcome(&description)
    } else {
        None
    };
    let info = chasm_activity_info(activity_id, run_id.clone(), &description);
    workflowservice::DescribeActivityExecutionResponse {
        run_id,
        info: Some(info),
        input,
        outcome,
        long_poll_token,
        callbacks: Vec::new(),
    }
}

/// Build a `PollActivityExecutionResponse` from a (typically terminal) description.
fn chasm_poll_response(
    run_id: String,
    description: crate::chasm_activity::ActivityDescription,
) -> workflowservice::PollActivityExecutionResponse {
    workflowservice::PollActivityExecutionResponse {
        run_id,
        outcome: chasm_activity_outcome(&description),
    }
}

macro_rules! deferred_unary {
    ($name:ident, $request:ident, $response:ident, $spec:literal) => {
        fn $name<'life0, 'async_trait>(
            &'life0 self,
            _request: Request<workflowservice::$request>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Response<workflowservice::$response>, Status>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                debug!(rpc = stringify!($name), spec = $spec, "deferred rpc");
                Err(Status::unimplemented(format!(
                    "{} is not implemented; tracked in spec {}",
                    stringify!($name),
                    $spec
                )))
            })
        }
    };
}

#[tonic::async_trait]
impl WorkflowServiceGrpcApi for WorkflowServiceGrpc {
    async fn start_workflow_execution(
        &self,
        request: Request<workflowservice::StartWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::StartWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::start_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(workflow_id = %edge_req.workflow_id, workflow_type = %edge_req.workflow_type, "start_workflow_execution");
        // Retained for the response self-link (the request is consumed by the call below).
        let namespace = edge_req.namespace.clone();
        let workflow_id = edge_req.workflow_id.clone();
        let edge_resp = self
            .inner
            .start_workflow_execution(&headers, edge_req)
            .await?;
        debug!(run_id = ?edge_resp.run_id, "start_workflow_execution success");
        Ok(Response::new(translate::start_response_to_proto(
            edge_resp,
            namespace,
            workflow_id,
        )))
    }

    async fn signal_workflow_execution(
        &self,
        request: Request<workflowservice::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::SignalWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::signal_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .signal_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::signal_response_to_proto(
            edge_resp,
        )))
    }

    async fn poll_workflow_task_queue(
        &self,
        request: Request<workflowservice::PollWorkflowTaskQueueRequest>,
    ) -> Result<Response<workflowservice::PollWorkflowTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::poll_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(task_queue = %edge_req.task_queue, "poll_workflow_task_queue");
        let edge_resp = self
            .inner
            .poll_workflow_task_queue(&headers, edge_req)
            .await?;

        let has_task = edge_resp.is_some();
        let num_queries = edge_resp.as_ref().map(|r| r.queries.len()).unwrap_or(0);
        let num_messages = edge_resp.as_ref().map(|r| r.messages.len()).unwrap_or(0);
        debug!(
            has_task,
            num_queries, num_messages, "poll_workflow_task_queue response"
        );
        Ok(Response::new(match edge_resp {
            Some(resp) => translate::poll_response_to_proto(resp),
            None => workflowservice::PollWorkflowTaskQueueResponse::default(),
        }))
    }

    async fn respond_workflow_task_completed(
        &self,
        request: Request<workflowservice::RespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<workflowservice::RespondWorkflowTaskCompletedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::respond_completed_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(
            num_commands = edge_req.commands.len(),
            num_query_results = edge_req.query_results.len(),
            num_messages = edge_req.messages.len(),
            "respond_workflow_task_completed"
        );
        let edge_resp = self
            .inner
            .respond_workflow_task_completed(&headers, edge_req)
            .await?;
        debug!("respond_workflow_task_completed success");
        Ok(Response::new(translate::completed_response_to_proto(
            edge_resp,
        )))
    }

    async fn describe_workflow_execution(
        &self,
        request: Request<workflowservice::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::describe_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(workflow_id = %edge_req.workflow_id, "describe_workflow_execution");
        let edge_resp = self
            .inner
            .describe_workflow_execution(&headers, edge_req)
            .await;
        match edge_resp {
            Ok(resp) => {
                debug!("describe_workflow_execution success");
                Ok(Response::new(translate::describe_response_to_proto(resp)))
            }
            Err(e) => {
                debug!(error = %e, "describe_workflow_execution failed");
                Err(e.into())
            }
        }
    }

    async fn list_workflow_executions(
        &self,
        request: Request<workflowservice::ListWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::list_response_to_proto(edge_resp)))
    }

    async fn count_workflow_executions(
        &self,
        request: Request<workflowservice::CountWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::CountWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::count_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .count_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::count_response_to_proto(edge_resp)))
    }

    async fn poll_activity_task_queue(
        &self,
        request: Request<workflowservice::PollActivityTaskQueueRequest>,
    ) -> Result<Response<workflowservice::PollActivityTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // Translate before the standalone-activity fast path so shared Temporal
        // poll validation (notably versioned sticky NormalName) runs before any
        // task can be claimed (`validateVersioningInfo`, workflow_handler.go
        // @ v1.31.0).
        let edge_req = translate::poll_activity_request_to_edge(req.clone())
            .map_err(proto_conversion_status)?;
        // CHASM-first: serve a queued standalone-activity task if one is waiting on
        // this task queue, before falling through to the workflow-activity path
        // (the two share this RPC).
        if let Some(bridge) = &self.chasm_activity
            && bridge.is_enabled()
        {
            let task_queue = req
                .task_queue
                .as_ref()
                .map(|q| q.name.clone())
                .unwrap_or_default();
            if let Some(task) = bridge
                .poll_activity_task_waiting(&task_queue, &req.identity)
                .await?
            {
                debug!(%task_queue, "poll_activity_task_queue served standalone activity");
                return Ok(Response::new(chasm_activity_poll_response(
                    &req.namespace,
                    task,
                )));
            }
        }
        debug!(task_queue = %edge_req.task_queue, "poll_activity_task_queue");
        let edge_resp = self
            .inner
            .poll_activity_task_queue(&headers, edge_req)
            .await?;

        let has_task = edge_resp.is_some();
        debug!(has_task, "poll_activity_task_queue response");
        Ok(Response::new(match edge_resp {
            Some(resp) => translate::poll_activity_response_to_proto(resp),
            None => workflowservice::PollActivityTaskQueueResponse::default(),
        }))
    }

    async fn respond_activity_task_completed(
        &self,
        request: Request<workflowservice::RespondActivityTaskCompletedRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskCompletedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // Route to the CHASM path only when the token is one the bridge issued; a
        // workflow-activity token falls through unchanged.
        if let Some(bridge) = &self.chasm_activity
            && bridge.owns_task_token(&req.task_token)
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let result = req.result.map(|p| p.encode_to_vec()).unwrap_or_default();
            bridge
                .respond_activity_task_completed(
                    &req.task_token,
                    &namespace_id.0.to_string(),
                    result,
                    req.identity,
                )
                .await?;
            debug!("respond_activity_task_completed (standalone) success");
            return Ok(Response::new(
                translate::respond_activity_completed_to_proto(),
            ));
        }
        let edge_req =
            translate::respond_activity_completed_to_edge(req).map_err(proto_conversion_status)?;
        debug!("respond_activity_task_completed");
        let _edge_resp = self
            .inner
            .respond_activity_task_completed(&headers, edge_req)
            .await?;
        debug!("respond_activity_task_completed success");
        Ok(Response::new(
            translate::respond_activity_completed_to_proto(),
        ))
    }

    async fn respond_activity_task_failed(
        &self,
        request: Request<workflowservice::RespondActivityTaskFailedRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskFailedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if let Some(bridge) = &self.chasm_activity
            && bridge.owns_task_token(&req.task_token)
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let failure = req
                .failure
                .as_ref()
                .map(|f| f.message.clone())
                .unwrap_or_default();
            // Carry the full structured Failure (e.g. ApplicationFailureInfo) so the
            // describe outcome round-trips it, not just the message (Req 5).
            let failure_payload = req
                .failure
                .as_ref()
                .map(|f| f.encode_to_vec())
                .unwrap_or_default();
            let heartbeat_details = req
                .last_heartbeat_details
                .map(|p| p.encode_to_vec())
                .unwrap_or_default();
            bridge
                .respond_activity_task_failed(
                    &req.task_token,
                    &namespace_id.0.to_string(),
                    failure,
                    failure_payload,
                    heartbeat_details,
                    req.identity,
                )
                .await?;
            return Ok(Response::new(translate::respond_activity_failed_to_proto()));
        }
        let edge_req =
            translate::respond_activity_failed_to_edge(req).map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_failed(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::respond_activity_failed_to_proto()))
    }

    async fn record_activity_task_heartbeat(
        &self,
        request: Request<workflowservice::RecordActivityTaskHeartbeatRequest>,
    ) -> Result<Response<workflowservice::RecordActivityTaskHeartbeatResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // Standalone-activity heartbeat: validate the worker token at the frontend
        // boundary (Temporal's generic task-token errors) and route to the bridge.
        // Deviation: on a standalone-enabled server a non-empty token that is not a
        // standalone token is reported as a deserialize failure rather than handed to
        // the inner workflow-activity heartbeat path — the conformance target serves
        // only standalone activities, and v1.31.0 uses one task-token serializer for
        // both (`standalone_activity_test.go:4126,4131`).
        if let Some(bridge) = &self.chasm_activity
            && bridge.is_enabled()
        {
            if req.task_token.is_empty() {
                return Err(Status::invalid_argument("Task token not set on request"));
            }
            if bridge.owns_task_token(&req.task_token) {
                let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
                let details = req.details.map(|p| p.encode_to_vec()).unwrap_or_default();
                let cancel_requested = bridge
                    .record_heartbeat(&req.task_token, &namespace_id.0.to_string(), details)
                    .await?;
                return Ok(Response::new(
                    workflowservice::RecordActivityTaskHeartbeatResponse {
                        cancel_requested,
                        ..Default::default()
                    },
                ));
            }
            return Err(Status::invalid_argument("Error deserializing task token"));
        }
        let edge_req = translate::record_heartbeat_to_edge(req).map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .record_activity_task_heartbeat(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::record_heartbeat_to_proto(
            edge_resp,
        )))
    }

    async fn terminate_workflow_execution(
        &self,
        request: Request<workflowservice::TerminateWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::TerminateWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::terminate_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .terminate_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::terminate_response_to_proto()))
    }

    async fn request_cancel_workflow_execution(
        &self,
        request: Request<workflowservice::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::RequestCancelWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::cancel_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .request_cancel_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::cancel_response_to_proto()))
    }

    async fn query_workflow(
        &self,
        request: Request<workflowservice::QueryWorkflowRequest>,
    ) -> Result<Response<workflowservice::QueryWorkflowResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        // The caller's gRPC deadline drives query semantics — the
        // backoff-vs-deadline guard and the long-poll park both use it
        // (v1.31.0 threads ctx straight through, queryworkflow/api.go). The
        // translate-layer default applies only when no deadline was sent.
        let caller_timeout = parse_grpc_timeout(request.metadata());
        let mut edge_req = translate::query_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        if let Some(timeout) = caller_timeout {
            edge_req.timeout = timeout;
        }
        let edge_resp = self.inner.query_workflow(&headers, edge_req).await?;
        Ok(Response::new(translate::query_response_to_proto(edge_resp)))
    }

    async fn update_workflow_execution(
        &self,
        request: Request<workflowservice::UpdateWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::update_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(
            update_id = %edge_req.update_id,
            update_name = %edge_req.update_name,
            "update_workflow_execution"
        );
        let edge_resp = self
            .inner
            .update_workflow_execution(&headers, edge_req)
            .await?;
        debug!(
            stage = ?edge_resp.stage,
            has_outcome = edge_resp.outcome.is_some(),
            "update_workflow_execution success"
        );
        Ok(Response::new(translate::update_response_to_proto(
            edge_resp,
        )))
    }

    async fn get_workflow_execution_history(
        &self,
        request: Request<workflowservice::GetWorkflowExecutionHistoryRequest>,
    ) -> Result<Response<workflowservice::GetWorkflowExecutionHistoryResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::get_history_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let filter_type = edge_req.history_event_filter_type;
        debug!(
            workflow_id = %edge_req.workflow_id,
            filter_type,
            wait_new_event = edge_req.wait_new_event,
            "get_workflow_execution_history"
        );
        let edge_resp = self
            .inner
            .get_workflow_execution_history(&headers, edge_req)
            .await?;
        let num_events = edge_resp.history.len();
        // `ClientSupportsFeature(FeatureFollowsNextRunID)` is purely header-driven:
        // the comma-delimited `supported-features` metadata must contain
        // "follows-next-run-id" (`version_checker.go:152` + headers.go:17 @ v1.31.0).
        // Legacy clients lacking it get the FixFollowEvents close-event rewrite.
        let client_follows_next_run_id = headers
            .get("supported-features")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|features| features.split(',').any(|f| f == "follows-next-run-id"));
        let resp = translate::get_history_response_to_proto(
            edge_resp,
            filter_type,
            client_follows_next_run_id,
        );
        let filtered_events = resp.history.as_ref().map(|h| h.events.len()).unwrap_or(0);
        debug!(
            num_events,
            filtered_events, "get_workflow_execution_history response"
        );
        Ok(Response::new(resp))
    }

    async fn register_namespace(
        &self,
        request: Request<workflowservice::RegisterNamespaceRequest>,
    ) -> Result<Response<workflowservice::RegisterNamespaceResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::register_namespace_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.register_namespace(&headers, edge_req).await?;
        Ok(Response::new(workflowservice::RegisterNamespaceResponse {}))
    }
    async fn describe_namespace(
        &self,
        request: Request<workflowservice::DescribeNamespaceRequest>,
    ) -> Result<Response<workflowservice::DescribeNamespaceResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let edge_resp = if !req.namespace.is_empty() {
            self.inner
                .describe_namespace(&headers, &req.namespace)
                .await?
        } else if !req.id.is_empty() {
            self.inner
                .describe_namespace_by_id(&headers, &req.id)
                .await?
        } else {
            return Err(Status::invalid_argument("namespace or id is required"));
        };
        Ok(Response::new(translate::namespace_to_proto(
            edge_resp,
            self.standalone_activities_enabled(),
        )))
    }
    async fn list_namespaces(
        &self,
        request: Request<workflowservice::ListNamespacesRequest>,
    ) -> Result<Response<workflowservice::ListNamespacesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.list_namespaces(&headers).await?;
        Ok(Response::new(translate::list_namespaces_to_proto(
            edge_resp,
            self.standalone_activities_enabled(),
        )))
    }
    async fn update_namespace(
        &self,
        request: Request<workflowservice::UpdateNamespaceRequest>,
    ) -> Result<Response<workflowservice::UpdateNamespaceResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::update_namespace_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self.inner.update_namespace(&headers, edge_req).await?;
        Ok(Response::new(
            translate::update_namespace_response_to_proto(
                edge_resp,
                self.standalone_activities_enabled(),
            ),
        ))
    }
    async fn deprecate_namespace(
        &self,
        _request: Request<workflowservice::DeprecateNamespaceRequest>,
    ) -> Result<Response<workflowservice::DeprecateNamespaceResponse>, Status> {
        Err(Status::unimplemented("deprecate_namespace"))
    }
    async fn execute_multi_operation(
        &self,
        request: Request<workflowservice::ExecuteMultiOperationRequest>,
    ) -> Result<Response<workflowservice::ExecuteMultiOperationResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        // Validate the whole composition BEFORE any runtime call (Req 1 /
        // Property 1): the `[Start, Update]` shape gate returns a plain
        // INVALID_ARGUMENT, per-operation failures return the structured
        // `MultiOperationExecutionFailure` (workflow_handler.go:718-726,
        // 766-785 @ v1.31.0).
        let edge_req = translate::multi_operation_request_to_edge(request.into_inner())
            .map_err(translate::multi_operation_request_error_to_status)?;
        debug!(
            workflow_id = %edge_req.start.workflow_id,
            update_id = %edge_req.update.update_id,
            update_name = %edge_req.update.update_name,
            "execute_multi_operation"
        );
        match self
            .inner
            .execute_multi_operation(&headers, edge_req)
            .await?
        {
            crate::translate::ExecuteMultiOperationOutcome::Completed(edge_resp) => {
                debug!(
                    run_id = %edge_resp.run_id.0,
                    started = edge_resp.started,
                    "execute_multi_operation success"
                );
                Ok(Response::new(translate::multi_operation_response_to_proto(
                    edge_resp,
                )))
            }
            // A post-validation leg failure: top-level code = the failing
            // op's code, message "Update-with-Start could not be executed.",
            // per-op statuses (failing op's own error + Aborted/OK sibling)
            // in the MultiOperationExecutionFailure detail (Req 4).
            crate::translate::ExecuteMultiOperationOutcome::Failed(failure) => {
                Err(translate::multi_operation_failure_to_status(failure))
            }
        }
    }
    async fn get_workflow_execution_history_reverse(
        &self,
        request: Request<workflowservice::GetWorkflowExecutionHistoryReverseRequest>,
    ) -> Result<Response<workflowservice::GetWorkflowExecutionHistoryReverseResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::get_history_reverse_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .get_workflow_execution_history_reverse(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::get_history_reverse_response_to_proto(edge_resp),
        ))
    }
    async fn respond_workflow_task_failed(
        &self,
        request: Request<workflowservice::RespondWorkflowTaskFailedRequest>,
    ) -> Result<Response<workflowservice::RespondWorkflowTaskFailedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let failure_cause = translate::wft_failed_cause_from_proto(req.cause);
        let failure_details = req
            .failure
            .as_ref()
            .map(tokeira_proto::conversions::common::failure_to_payload);
        debug!(cause = req.cause, "respond_workflow_task_failed");
        self.inner
            .respond_workflow_task_failed(
                &headers,
                req.namespace,
                req.task_token,
                failure_cause,
                failure_details,
                req.identity,
            )
            .await?;
        Ok(Response::new(
            workflowservice::RespondWorkflowTaskFailedResponse {},
        ))
    }
    async fn record_activity_task_heartbeat_by_id(
        &self,
        request: Request<workflowservice::RecordActivityTaskHeartbeatByIdRequest>,
    ) -> Result<Response<workflowservice::RecordActivityTaskHeartbeatByIdResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // Empty workflow id discriminates a standalone activity, as on the other
        // by-id RPCs (`workflow_handler.go:1671 @ v1.31.0`).
        if let Some(bridge) = &self.chasm_activity
            && req.workflow_id.is_empty()
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let details = req.details.map(|p| p.encode_to_vec()).unwrap_or_default();
            let cancel_requested = bridge
                .heartbeat_by_id(
                    &namespace_id.0.to_string(),
                    &req.activity_id,
                    &req.run_id,
                    details,
                )
                .await?;
            return Ok(Response::new(
                workflowservice::RecordActivityTaskHeartbeatByIdResponse {
                    cancel_requested,
                    ..Default::default()
                },
            ));
        }
        let edge_req = translate::record_activity_heartbeat_by_id_to_edge(req)
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .record_activity_task_heartbeat_by_id(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::record_activity_heartbeat_by_id_to_proto(edge_resp),
        ))
    }
    async fn respond_activity_task_completed_by_id(
        &self,
        request: Request<workflowservice::RespondActivityTaskCompletedByIdRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskCompletedByIdResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // An empty workflow id on a by-id respond is v1.31.0's discriminator for a
        // standalone activity (`workflow_handler.go:1671 @ v1.31.0` — empty
        // workflow_id ⇒ build a component-ref token). A present workflow id is a
        // workflow activity and falls through to the inner path unchanged.
        if let Some(bridge) = &self.chasm_activity
            && req.workflow_id.is_empty()
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let result = req.result.map(|p| p.encode_to_vec()).unwrap_or_default();
            bridge
                .complete_by_id(
                    &namespace_id.0.to_string(),
                    &req.activity_id,
                    &req.run_id,
                    result,
                    req.identity,
                )
                .await?;
            return Ok(Response::new(
                translate::respond_activity_completed_by_id_to_proto(),
            ));
        }
        let edge_req = translate::respond_activity_completed_by_id_to_edge(req)
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_completed_by_id(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::respond_activity_completed_by_id_to_proto(),
        ))
    }
    async fn respond_activity_task_failed_by_id(
        &self,
        request: Request<workflowservice::RespondActivityTaskFailedByIdRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskFailedByIdResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if let Some(bridge) = &self.chasm_activity
            && req.workflow_id.is_empty()
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let failure = req
                .failure
                .as_ref()
                .map(|f| f.message.clone())
                .unwrap_or_default();
            // Carry the full structured Failure so the describe outcome round-trips
            // it, not just the message (Req 5), matching the by-token path.
            let failure_payload = req
                .failure
                .as_ref()
                .map(|f| f.encode_to_vec())
                .unwrap_or_default();
            let heartbeat_details = req
                .last_heartbeat_details
                .map(|p| p.encode_to_vec())
                .unwrap_or_default();
            bridge
                .fail_by_id(
                    &namespace_id.0.to_string(),
                    &req.activity_id,
                    &req.run_id,
                    failure,
                    failure_payload,
                    heartbeat_details,
                    req.identity,
                )
                .await?;
            return Ok(Response::new(
                translate::respond_activity_failed_by_id_to_proto(),
            ));
        }
        let edge_req = translate::respond_activity_failed_by_id_to_edge(req)
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_failed_by_id(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::respond_activity_failed_by_id_to_proto(),
        ))
    }
    async fn respond_activity_task_canceled(
        &self,
        request: Request<workflowservice::RespondActivityTaskCanceledRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskCanceledResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // Standalone-activity token: route to the CHASM bridge for token validation
        // and the CANCEL_REQUESTED → CANCELED transition; a workflow-activity token
        // falls through unchanged (the two share this RPC).
        if let Some(bridge) = &self.chasm_activity
            && bridge.owns_task_token(&req.task_token)
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let details = req.details.map(|p| p.encode_to_vec()).unwrap_or_default();
            bridge
                .respond_activity_task_canceled(
                    &req.task_token,
                    &namespace_id.0.to_string(),
                    details,
                )
                .await?;
            return Ok(Response::new(
                translate::respond_activity_canceled_to_proto(),
            ));
        }
        let edge_req =
            translate::respond_activity_canceled_to_edge(req).map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_canceled(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::respond_activity_canceled_to_proto(),
        ))
    }
    async fn respond_activity_task_canceled_by_id(
        &self,
        request: Request<workflowservice::RespondActivityTaskCanceledByIdRequest>,
    ) -> Result<Response<workflowservice::RespondActivityTaskCanceledByIdResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if let Some(bridge) = &self.chasm_activity
            && req.workflow_id.is_empty()
        {
            let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
            let details = req.details.map(|p| p.encode_to_vec()).unwrap_or_default();
            bridge
                .cancel_by_id(
                    &namespace_id.0.to_string(),
                    &req.activity_id,
                    &req.run_id,
                    details,
                )
                .await?;
            return Ok(Response::new(
                translate::respond_activity_canceled_by_id_to_proto(),
            ));
        }
        let edge_req = translate::respond_activity_canceled_by_id_to_edge(req)
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_canceled_by_id(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::respond_activity_canceled_by_id_to_proto(),
        ))
    }
    async fn signal_with_start_workflow_execution(
        &self,
        request: Request<workflowservice::SignalWithStartWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::SignalWithStartWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::signal_with_start_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .signal_with_start_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::signal_with_start_response_to_proto(edge_resp),
        ))
    }
    async fn reset_workflow_execution(
        &self,
        request: Request<workflowservice::ResetWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::ResetWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::reset_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .reset_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::reset_response_to_proto(edge_resp)))
    }
    async fn delete_workflow_execution(
        &self,
        request: Request<workflowservice::DeleteWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::DeleteWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::delete_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner
            .delete_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::DeleteWorkflowExecutionResponse {},
        ))
    }
    async fn list_open_workflow_executions(
        &self,
        request: Request<workflowservice::ListOpenWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListOpenWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_open_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::list_open_response_to_proto(
            edge_resp,
        )))
    }
    async fn list_closed_workflow_executions(
        &self,
        request: Request<workflowservice::ListClosedWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListClosedWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_closed_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::list_closed_response_to_proto(
            edge_resp,
        )))
    }
    async fn list_archived_workflow_executions(
        &self,
        request: Request<workflowservice::ListArchivedWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListArchivedWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_archived_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::list_archived_response_to_proto(
            edge_resp,
        )))
    }
    async fn scan_workflow_executions(
        &self,
        request: Request<workflowservice::ScanWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ScanWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::scan_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::scan_response_to_proto(edge_resp)))
    }
    async fn get_search_attributes(
        &self,
        request: Request<workflowservice::GetSearchAttributesRequest>,
    ) -> Result<Response<workflowservice::GetSearchAttributesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let attrs = self.inner.get_search_attributes(&headers).await?;
        let mut keys = standard_search_attributes();
        for attr in attrs {
            keys.insert(
                attr.name,
                indexed_value_type_from_edge(&attr.attr_type)? as i32,
            );
        }
        Ok(Response::new(
            workflowservice::GetSearchAttributesResponse { keys },
        ))
    }
    async fn respond_query_task_completed(
        &self,
        _request: Request<workflowservice::RespondQueryTaskCompletedRequest>,
    ) -> Result<Response<workflowservice::RespondQueryTaskCompletedResponse>, Status> {
        let request = _request;
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let result = match tokeira_proto::enums::QueryResultType::try_from(req.completed_type)
            .unwrap_or(tokeira_proto::enums::QueryResultType::Failed)
        {
            tokeira_proto::enums::QueryResultType::Answered => {
                tokeira_runtime::QueryResult::Completed {
                    result: req
                        .query_result
                        .as_ref()
                        .map(tokeira_proto::conversions::common::payloads_to_domain)
                        .unwrap_or_default(),
                }
            }
            tokeira_proto::enums::QueryResultType::Failed
            | tokeira_proto::enums::QueryResultType::Unspecified => {
                tokeira_runtime::QueryResult::Failed {
                    message: req.error_message,
                    failure: req
                        .failure
                        .as_ref()
                        .map(tokeira_proto::conversions::common::failure_to_payload),
                }
            }
        };
        self.inner
            .respond_query_task_completed(&headers, req.namespace, req.task_token, result)
            .await?;
        Ok(Response::new(
            workflowservice::RespondQueryTaskCompletedResponse {},
        ))
    }
    async fn reset_sticky_task_queue(
        &self,
        request: Request<workflowservice::ResetStickyTaskQueueRequest>,
    ) -> Result<Response<workflowservice::ResetStickyTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let execution = req
            .execution
            .ok_or_else(|| Status::invalid_argument("execution is required"))?;
        let run_id = if execution.run_id.is_empty() {
            None
        } else {
            Some(execution.run_id)
        };
        self.inner
            .reset_sticky_task_queue(&headers, req.namespace, execution.workflow_id, run_id)
            .await?;
        Ok(Response::new(
            workflowservice::ResetStickyTaskQueueResponse {},
        ))
    }
    async fn record_worker_heartbeat(
        &self,
        request: Request<workflowservice::RecordWorkerHeartbeatRequest>,
    ) -> Result<Response<workflowservice::RecordWorkerHeartbeatResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if req.namespace.is_empty() {
            tokeira_runtime::metrics::record_worker_heartbeat_rejected("", "invalid_namespace");
            return Err(Status::invalid_argument("namespace is required"));
        }
        let context = self
            .inner
            .admit_request(
                &headers,
                Some(&req.namespace),
                Action::RecordWorkerHeartbeat,
                false,
            )
            .await?;
        let namespace_id = context
            .namespace
            .as_ref()
            .map(|namespace| crate::to_internal::namespace_id_for(&namespace.name))
            .ok_or_else(|| Status::internal("worker heartbeat admitted without namespace"))?;
        let heartbeat_count = req.worker_heartbeat.len();
        for proto in req.worker_heartbeat {
            let heartbeat = worker_heartbeat::worker_heartbeat_from_proto(
                namespace_id,
                proto,
                context.received_at,
            );
            let key = heartbeat.worker_instance_key.clone();
            let active = heartbeat.status.0 != 3;
            self.inner
                .heartbeat_store()
                .insert(heartbeat)
                .map_err(|error| {
                    tokeira_runtime::metrics::record_worker_heartbeat_rejected(
                        &req.namespace,
                        "store_error",
                    );
                    Status::internal(error.to_string())
                })?;
            tokeira_runtime::metrics::record_worker_heartbeat_accepted(namespace_id, &key);
            tokeira_runtime::metrics::record_worker_heartbeat_active(namespace_id, &key, active);
        }
        debug!(
            rpc = "RecordWorkerHeartbeat",
            namespace = %req.namespace,
            heartbeat_count,
            "record_worker_heartbeat"
        );
        Ok(Response::new(
            workflowservice::RecordWorkerHeartbeatResponse {},
        ))
    }
    async fn shutdown_worker(
        &self,
        request: Request<workflowservice::ShutdownWorkerRequest>,
    ) -> Result<Response<workflowservice::ShutdownWorkerResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        // v1.31.0 (`service/frontend/workflow_handler.go:2983 @ v1.31.0`) does NOT pre-validate
        // `sticky_task_queue`: it resolves the namespace and forwards the (possibly empty) sticky
        // queue straight to `ForceUnloadTaskQueuePartition`. A worker that never cached a workflow
        // (activity-only, or shut down before stickiness) sends an empty sticky queue on shutdown, so
        // rejecting it with `InvalidArgument` is an over-rejection (C6-class) that breaks SDK shutdown.
        let context = self
            .inner
            .admit_request(
                &headers,
                Some(&req.namespace),
                Action::ShutdownWorker,
                false,
            )
            .await?;
        let namespace_id = context
            .namespace
            .as_ref()
            .map(|namespace| crate::to_internal::namespace_id_for(&namespace.name))
            .ok_or_else(|| Status::internal("worker shutdown admitted without namespace"))?;
        if let Some(proto) = req.worker_heartbeat {
            let heartbeat = worker_heartbeat::worker_heartbeat_from_proto(
                namespace_id,
                proto,
                context.received_at,
            );
            let key = heartbeat.worker_instance_key.clone();
            let active = heartbeat.status.0 != 3;
            match self.inner.heartbeat_store().insert(heartbeat) {
                Ok(()) => {
                    tokeira_runtime::metrics::record_worker_heartbeat_accepted(namespace_id, &key);
                    tokeira_runtime::metrics::record_worker_heartbeat_active(
                        namespace_id,
                        &key,
                        active,
                    );
                }
                Err(error) => {
                    tracing::warn!(?error, "failed to store shutdown worker heartbeat");
                }
            }
        }
        // An empty sticky queue has nothing to unload (the v1.31.0 unload would target an empty
        // partition — effectively a no-op here), so only deny a worker when a sticky queue is named.
        if !req.sticky_task_queue.is_empty() {
            self.inner
                .broker()
                .deny_worker(
                    namespace_id,
                    TaskQueueName(req.sticky_task_queue),
                    WorkerIdentity(req.identity.clone()),
                )
                .await;
        }
        if !req.task_queue.is_empty() {
            self.inner
                .cancel_outstanding_worker_polls(
                    namespace_id,
                    TaskQueueName(req.task_queue),
                    WorkerIdentity(req.identity),
                )
                .await;
        }
        Ok(Response::new(workflowservice::ShutdownWorkerResponse {}))
    }
    async fn describe_task_queue(
        &self,
        request: Request<workflowservice::DescribeTaskQueueRequest>,
    ) -> Result<Response<workflowservice::DescribeTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::describe_task_queue_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self.inner.describe_task_queue(&headers, edge_req).await?;
        Ok(Response::new(
            translate::describe_task_queue_response_to_proto(edge_resp),
        ))
    }
    async fn get_cluster_info(
        &self,
        request: Request<workflowservice::GetClusterInfoRequest>,
    ) -> Result<Response<workflowservice::GetClusterInfoResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.get_cluster_info(&headers).await?;
        Ok(Response::new(translate::cluster_info_to_proto(edge_resp)))
    }
    async fn get_system_info(
        &self,
        request: Request<workflowservice::GetSystemInfoRequest>,
    ) -> Result<Response<workflowservice::GetSystemInfoResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.get_system_info(&headers).await?;
        Ok(Response::new(translate::system_info_to_proto(edge_resp)))
    }
    async fn list_task_queue_partitions(
        &self,
        request: Request<workflowservice::ListTaskQueuePartitionsRequest>,
    ) -> Result<Response<workflowservice::ListTaskQueuePartitionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_task_queue_partitions_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_task_queue_partitions(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::list_task_queue_partitions_response_to_proto(edge_resp),
        ))
    }
    async fn create_schedule(
        &self,
        request: Request<workflowservice::CreateScheduleRequest>,
    ) -> Result<Response<workflowservice::CreateScheduleResponse>, Status> {
        let store = self.inner.schedule_store();
        let (namespace_id, schedule_id, entry, initial_patch) =
            schedule::create_schedule_request_to_edge(request.into_inner())?;
        let conflict_token = match store.create(entry) {
            Ok(token) => token,
            Err(ScheduleError::AlreadyExists) => {
                // V1 schedules are backed by a scheduler workflow in Temporal,
                // so duplicate creation deliberately preserves the typed
                // WorkflowExecutionAlreadyStarted detail that SDKs translate to
                // ErrScheduleAlreadyRunning (`workflow_handler.go:3625-3631 @
                // v1.31.0`).
                return Err(workflow_already_started_status(
                    format!("schedule {:?} is already registered", schedule_id.0),
                    String::new(),
                ));
            }
            Err(error) => return Err(schedule_error_status(error)),
        };
        if let Some(patch) = initial_patch {
            self.inner
                .apply_schedule_patch(namespace_id, &schedule_id, patch)
                .await?;
        }
        Ok(Response::new(workflowservice::CreateScheduleResponse {
            conflict_token,
        }))
    }

    async fn describe_schedule(
        &self,
        request: Request<workflowservice::DescribeScheduleRequest>,
    ) -> Result<Response<workflowservice::DescribeScheduleResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = to_internal::namespace_id_for(&req.namespace);
        let schedule_id = tokeira_runtime::ScheduleId(req.schedule_id);
        let store = self.inner.schedule_store();
        let mut entry = store
            .describe(namespace_id, &schedule_id)
            .map_err(schedule_error_status)?;
        entry.info.future_action_times =
            compute_next_times(&entry.spec, OffsetDateTime::now_utc(), 10, &schedule_id);
        Ok(Response::new(
            schedule::describe_schedule_response_to_proto(&entry),
        ))
    }

    async fn update_schedule(
        &self,
        request: Request<workflowservice::UpdateScheduleRequest>,
    ) -> Result<Response<workflowservice::UpdateScheduleResponse>, Status> {
        let request = request.into_inner();
        // Workflow-backed schedules reject every memo update, including an
        // explicitly empty memo (`service/frontend/workflow_handler.go:4559-4563
        // @ v1.31.0`). CHASM schedule memo replacement is a different surface.
        if request.memo.is_some() {
            return Err(Status::failed_precondition(
                "memo updates are not supported on workflow-backed schedules",
            ));
        }
        let replace_search_attributes = request.search_attributes.is_some();
        let (namespace_id, schedule_id, token, replacement) =
            schedule::update_schedule_request_to_edge(request)?;
        let store = self.inner.schedule_store();
        store
            .update(namespace_id, &schedule_id, &token, |entry| {
                entry.spec = replacement.spec;
                entry.action = replacement.action;
                entry.policies = replacement.policies;
                entry.state = replacement.state;
                // UpdateSchedule treats absent search attributes as no change;
                // a present empty map explicitly clears them
                // (`service/frontend/workflow_handler.go @ v1.31.0`).
                if replace_search_attributes {
                    entry.search_attributes = replacement.search_attributes;
                }
                entry.info.update_time = OffsetDateTime::now_utc();
            })
            .map_err(schedule_error_status)?;
        Ok(Response::new(workflowservice::UpdateScheduleResponse {}))
    }

    async fn patch_schedule(
        &self,
        request: Request<workflowservice::PatchScheduleRequest>,
    ) -> Result<Response<workflowservice::PatchScheduleResponse>, Status> {
        let (namespace_id, schedule_id, patch) =
            schedule::patch_schedule_request_to_edge(request.into_inner())?;
        self.inner
            .apply_schedule_patch(namespace_id, &schedule_id, patch)
            .await?;
        Ok(Response::new(workflowservice::PatchScheduleResponse {}))
    }

    async fn list_schedule_matching_times(
        &self,
        request: Request<workflowservice::ListScheduleMatchingTimesRequest>,
    ) -> Result<Response<workflowservice::ListScheduleMatchingTimesResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = to_internal::namespace_id_for(&req.namespace);
        let schedule_id = tokeira_runtime::ScheduleId(req.schedule_id);
        let store = self.inner.schedule_store();
        let entry = store
            .describe(namespace_id, &schedule_id)
            .map_err(schedule_error_status)?;
        let start = req
            .start_time
            .as_ref()
            .and_then(proto_timestamp_to_time)
            .unwrap_or_else(OffsetDateTime::now_utc);
        let end = req
            .end_time
            .as_ref()
            .and_then(proto_timestamp_to_time)
            .unwrap_or(start);
        // v1.31.0 repeatedly asks for the next time strictly after StartTime,
        // while accepting a match equal to EndTime
        // (`service/worker/scheduler/workflow.go:1119-1137 @ v1.31.0`).
        let times = compute_matching_times(
            &entry.spec,
            start + time::Duration::nanoseconds(1),
            end,
            &schedule_id,
        );
        Ok(Response::new(schedule::matching_times_response_to_proto(
            times,
        )))
    }

    async fn delete_schedule(
        &self,
        request: Request<workflowservice::DeleteScheduleRequest>,
    ) -> Result<Response<workflowservice::DeleteScheduleResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = to_internal::namespace_id_for(&req.namespace);
        let schedule_id = tokeira_runtime::ScheduleId(req.schedule_id);
        self.inner
            .schedule_store()
            .delete(namespace_id, &schedule_id)
            .map_err(schedule_error_status)?;
        Ok(Response::new(workflowservice::DeleteScheduleResponse {}))
    }

    async fn list_schedules(
        &self,
        request: Request<workflowservice::ListSchedulesRequest>,
    ) -> Result<Response<workflowservice::ListSchedulesResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = to_internal::namespace_id_for(&req.namespace);
        let page_size = if req.maximum_page_size <= 0 {
            100
        } else {
            req.maximum_page_size as usize
        };
        let query = req.query.trim();
        let (mut entries, next_page_token) = self
            .inner
            .schedule_store()
            .list(
                namespace_id,
                page_size,
                &req.next_page_token,
                (!query.is_empty()).then_some(query),
            )
            .map_err(|_| Status::invalid_argument("unsupported schedule query"))?;
        let now = OffsetDateTime::now_utc();
        for entry in &mut entries {
            entry.info.future_action_times =
                compute_next_times(&entry.spec, now, 10, &entry.schedule_id);
        }
        Ok(Response::new(schedule::list_schedules_response_to_proto(
            entries,
            next_page_token,
        )))
    }
    // Worker Versioning V1 (version sets) and V2 (rules) are deprecated in
    // v1.31.0 and their frontend gates (`frontend.workerVersioningDataAPIs`,
    // `frontend.workerVersioningRuleAPIs`) default to off, so a stock server
    // refuses all five RPCs before any namespace or field validation. tokeira
    // has no dynamic config: the gates are permanently off and the rejection
    // is the whole surface (docs/conformance/v1.31.0/worker-versioning.md).
    async fn update_worker_build_id_compatibility(
        &self,
        _request: Request<workflowservice::UpdateWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkerBuildIdCompatibilityResponse>, Status> {
        Err(worker_versioning_v1_disabled_status())
    }
    async fn get_worker_build_id_compatibility(
        &self,
        _request: Request<workflowservice::GetWorkerBuildIdCompatibilityRequest>,
    ) -> Result<Response<workflowservice::GetWorkerBuildIdCompatibilityResponse>, Status> {
        Err(worker_versioning_v1_disabled_status())
    }
    async fn update_worker_versioning_rules(
        &self,
        _request: Request<workflowservice::UpdateWorkerVersioningRulesRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkerVersioningRulesResponse>, Status> {
        Err(worker_versioning_v2_disabled_status())
    }
    async fn get_worker_versioning_rules(
        &self,
        _request: Request<workflowservice::GetWorkerVersioningRulesRequest>,
    ) -> Result<Response<workflowservice::GetWorkerVersioningRulesResponse>, Status> {
        Err(worker_versioning_v2_disabled_status())
    }
    async fn get_worker_task_reachability(
        &self,
        _request: Request<workflowservice::GetWorkerTaskReachabilityRequest>,
    ) -> Result<Response<workflowservice::GetWorkerTaskReachabilityResponse>, Status> {
        // Gated by the v0.1 data flag upstream but surfaces the v0.2 message
        // (workflow_handler.go:5491-5493 @ v1.31.0).
        Err(worker_versioning_v2_disabled_status())
    }
    async fn describe_deployment(
        &self,
        _request: Request<workflowservice::DescribeDeploymentRequest>,
    ) -> Result<Response<workflowservice::DescribeDeploymentResponse>, Status> {
        Err(Status::unimplemented(DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED))
    }
    async fn list_deployments(
        &self,
        _request: Request<workflowservice::ListDeploymentsRequest>,
    ) -> Result<Response<workflowservice::ListDeploymentsResponse>, Status> {
        Err(Status::unimplemented(DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED))
    }
    async fn get_deployment_reachability(
        &self,
        _request: Request<workflowservice::GetDeploymentReachabilityRequest>,
    ) -> Result<Response<workflowservice::GetDeploymentReachabilityResponse>, Status> {
        Err(Status::unimplemented(DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED))
    }
    async fn get_current_deployment(
        &self,
        _request: Request<workflowservice::GetCurrentDeploymentRequest>,
    ) -> Result<Response<workflowservice::GetCurrentDeploymentResponse>, Status> {
        Err(Status::unimplemented(DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED))
    }
    async fn set_current_deployment(
        &self,
        _request: Request<workflowservice::SetCurrentDeploymentRequest>,
    ) -> Result<Response<workflowservice::SetCurrentDeploymentResponse>, Status> {
        Err(Status::unimplemented(DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED))
    }
    async fn poll_workflow_execution_update(
        &self,
        request: Request<workflowservice::PollWorkflowExecutionUpdateRequest>,
    ) -> Result<Response<workflowservice::PollWorkflowExecutionUpdateResponse>, Status> {
        use tokeira_proto::public::temporal::api::update::v1 as update;

        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();

        let update_ref = req
            .update_ref
            .ok_or_else(|| Status::invalid_argument("update_ref is required"))?;
        let execution = update_ref
            .workflow_execution
            .ok_or_else(|| Status::invalid_argument("update_ref.workflow_execution is required"))?;
        let update_id = update_ref.update_id;
        if update_id.is_empty() {
            return Err(Status::invalid_argument("update_ref.update_id is required"));
        }

        let wait_policy = match req
            .wait_policy
            .as_ref()
            .map(|policy| policy.lifecycle_stage)
        {
            None | Some(0) => tokeira_runtime::UpdateWaitPolicy::Unspecified,
            Some(1) => tokeira_runtime::UpdateWaitPolicy::Admitted,
            Some(2) => tokeira_runtime::UpdateWaitPolicy::Accepted,
            Some(3) => tokeira_runtime::UpdateWaitPolicy::Completed,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "invalid update wait lifecycle_stage",
                ));
            }
        };

        let result = self
            .inner
            .poll_workflow_execution_update(
                &headers,
                req.namespace.clone(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
                update_id.clone(),
                wait_policy,
            )
            .await?;

        let (proto_outcome, stage) = match result.outcome {
            Some(tokeira_runtime::UpdateOutcome::Completed { result, .. }) => (
                Some(update::Outcome {
                    value: Some(update::outcome::Value::Success(
                        tokeira_proto::conversions::common::payloads_from_domain(&result),
                    )),
                }),
                tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ),
            Some(tokeira_runtime::UpdateOutcome::Rejected { failure, .. }) => (
                Some(update::Outcome {
                    value: Some(update::outcome::Value::Failure(
                        tokeira_proto::conversions::common::payload_to_failure(&failure),
                    )),
                }),
                tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ),
            Some(tokeira_runtime::UpdateOutcome::AcceptedRunClosed) => (
                Some(update::Outcome {
                    value: Some(update::outcome::Value::Failure(
                        crate::grpc::translate::accepted_update_completed_workflow_failure(),
                    )),
                }),
                tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ),
            Some(tokeira_runtime::UpdateOutcome::RejectedUnprocessed) => (
                Some(update::Outcome {
                    value: Some(update::outcome::Value::Failure(
                        crate::grpc::translate::unprocessed_update_failure(),
                    )),
                }),
                tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32,
            ),
            None => (None, update_lifecycle_stage_to_proto(result.stage)),
        };

        Ok(Response::new(
            workflowservice::PollWorkflowExecutionUpdateResponse {
                outcome: proto_outcome,
                stage,
                update_ref: Some(update::UpdateRef {
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: result.workflow_execution.workflow_id.0,
                        run_id: result
                            .workflow_execution
                            .run_id
                            .map(|run_id| run_id.0.to_string())
                            .unwrap_or_default(),
                    }),
                    update_id: result.update_id,
                }),
            },
        ))
    }
    async fn start_batch_operation(
        &self,
        request: Request<workflowservice::StartBatchOperationRequest>,
    ) -> Result<Response<workflowservice::StartBatchOperationResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = batch::start_batch_request_to_edge(request.into_inner())
            .map_err(batch_translate_error_status)?;
        self.inner.start_batch_operation(&headers, edge_req).await?;
        Ok(Response::new(
            workflowservice::StartBatchOperationResponse::default(),
        ))
    }
    async fn stop_batch_operation(
        &self,
        request: Request<workflowservice::StopBatchOperationRequest>,
    ) -> Result<Response<workflowservice::StopBatchOperationResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = batch::stop_batch_request_to_edge(request.into_inner())
            .map_err(batch_translate_error_status)?;
        self.inner.stop_batch_operation(&headers, edge_req).await?;
        Ok(Response::new(
            workflowservice::StopBatchOperationResponse::default(),
        ))
    }
    async fn describe_batch_operation(
        &self,
        request: Request<workflowservice::DescribeBatchOperationRequest>,
    ) -> Result<Response<workflowservice::DescribeBatchOperationResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = batch::describe_batch_request_to_edge(request.into_inner())
            .map_err(batch_translate_error_status)?;
        let snapshot = self
            .inner
            .describe_batch_operation(&headers, edge_req)
            .await?;
        Ok(Response::new(batch::describe_batch_response_to_proto(
            snapshot,
        )))
    }
    async fn list_batch_operations(
        &self,
        request: Request<workflowservice::ListBatchOperationsRequest>,
    ) -> Result<Response<workflowservice::ListBatchOperationsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = batch::list_batch_request_to_edge(request.into_inner())
            .map_err(batch_translate_error_status)?;
        let (entries, next_page_token) =
            self.inner.list_batch_operations(&headers, edge_req).await?;
        Ok(Response::new(batch::list_batch_response_to_proto(
            entries,
            next_page_token,
        )))
    }
    async fn poll_nexus_task_queue(
        &self,
        request: Request<workflowservice::PollNexusTaskQueueRequest>,
    ) -> Result<Response<workflowservice::PollNexusTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = nexus::poll_request_to_edge(request.into_inner())
            .map_err(nexus_translate_error_status)?;
        let edge_resp = self.inner.poll_nexus_task_queue(&headers, edge_req).await?;
        Ok(Response::new(match edge_resp {
            Some(resp) => {
                nexus::poll_response_to_proto(resp).map_err(nexus_translate_error_status)?
            }
            None => workflowservice::PollNexusTaskQueueResponse::default(),
        }))
    }
    async fn respond_nexus_task_completed(
        &self,
        request: Request<workflowservice::RespondNexusTaskCompletedRequest>,
    ) -> Result<Response<workflowservice::RespondNexusTaskCompletedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = nexus::completed_request_to_edge(request.into_inner())
            .map_err(nexus_translate_error_status)?;
        self.inner
            .respond_nexus_task_completed(&headers, edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::RespondNexusTaskCompletedResponse::default(),
        ))
    }
    async fn respond_nexus_task_failed(
        &self,
        request: Request<workflowservice::RespondNexusTaskFailedRequest>,
    ) -> Result<Response<workflowservice::RespondNexusTaskFailedResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = nexus::failed_request_to_edge(request.into_inner())
            .map_err(nexus_translate_error_status)?;
        self.inner
            .respond_nexus_task_failed(&headers, edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::RespondNexusTaskFailedResponse::default(),
        ))
    }
    async fn count_schedules(
        &self,
        request: Request<workflowservice::CountSchedulesRequest>,
    ) -> Result<Response<workflowservice::CountSchedulesResponse>, Status> {
        let req = request.into_inner();
        if req.namespace.trim().is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        let namespace_id = self
            .inner
            .resolve_namespace_id(&req.namespace)
            .await
            .map_err(namespace_resolution_status)?;
        let query = req.query.trim();
        let count = self
            .inner
            .schedule_store()
            .count_schedules(&namespace_id, (!query.is_empty()).then_some(query))
            .map_err(|_| Status::invalid_argument("unsupported schedule query"))?;
        Ok(Response::new(workflowservice::CountSchedulesResponse {
            count: count as i64,
            groups: Vec::new(),
        }))
    }
    // === Worker Deployments ===
    async fn describe_worker_deployment_version(
        &self,
        request: Request<workflowservice::DescribeWorkerDeploymentVersionRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkerDeploymentVersionResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::describe_worker_deployment_version_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let view = self
            .inner
            .worker_deployment_runtime()?
            .describe_worker_deployment_version(edge_req)
            .await?;
        Ok(Response::new(
            translate::describe_worker_deployment_version_response_from_edge(&view),
        ))
    }

    async fn set_worker_deployment_current_version(
        &self,
        request: Request<workflowservice::SetWorkerDeploymentCurrentVersionRequest>,
    ) -> Result<Response<workflowservice::SetWorkerDeploymentCurrentVersionResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::set_worker_deployment_current_version_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let outcome = self
            .inner
            .worker_deployment_runtime()?
            .set_worker_deployment_current_version(edge_req)
            .await?;
        Ok(Response::new(
            translate::set_worker_deployment_current_version_response_from_edge(&outcome.view),
        ))
    }

    async fn describe_worker_deployment(
        &self,
        request: Request<workflowservice::DescribeWorkerDeploymentRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkerDeploymentResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req =
            translate::describe_worker_deployment_to_edge(req).map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let view = self
            .inner
            .worker_deployment_runtime()?
            .describe_worker_deployment(edge_req)
            .await?;
        Ok(Response::new(
            translate::describe_worker_deployment_response_from_edge(&view),
        ))
    }

    async fn delete_worker_deployment(
        &self,
        request: Request<workflowservice::DeleteWorkerDeploymentRequest>,
    ) -> Result<Response<workflowservice::DeleteWorkerDeploymentResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req =
            translate::delete_worker_deployment_to_edge(req).map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        self.inner
            .worker_deployment_runtime()?
            .delete_worker_deployment(edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::DeleteWorkerDeploymentResponse::default(),
        ))
    }

    async fn delete_worker_deployment_version(
        &self,
        request: Request<workflowservice::DeleteWorkerDeploymentVersionRequest>,
    ) -> Result<Response<workflowservice::DeleteWorkerDeploymentVersionResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::delete_worker_deployment_version_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        self.inner
            .worker_deployment_runtime()?
            .delete_worker_deployment_version(edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::DeleteWorkerDeploymentVersionResponse::default(),
        ))
    }

    async fn set_worker_deployment_ramping_version(
        &self,
        request: Request<workflowservice::SetWorkerDeploymentRampingVersionRequest>,
    ) -> Result<Response<workflowservice::SetWorkerDeploymentRampingVersionResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::set_worker_deployment_ramping_version_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let outcome = self
            .inner
            .worker_deployment_runtime()?
            .set_worker_deployment_ramping_version(edge_req)
            .await?;
        Ok(Response::new(
            translate::set_worker_deployment_ramping_version_response_from_edge(&outcome.view),
        ))
    }

    async fn list_worker_deployments(
        &self,
        request: Request<workflowservice::ListWorkerDeploymentsRequest>,
    ) -> Result<Response<workflowservice::ListWorkerDeploymentsResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req =
            translate::list_worker_deployments_to_edge(req).map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let page = self
            .inner
            .worker_deployment_runtime()?
            .list_worker_deployments(edge_req)
            .await?;
        Ok(Response::new(
            translate::list_worker_deployments_response_from_edge(&page),
        ))
    }

    async fn create_worker_deployment(
        &self,
        request: Request<workflowservice::CreateWorkerDeploymentRequest>,
    ) -> Result<Response<workflowservice::CreateWorkerDeploymentResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req =
            translate::create_worker_deployment_to_edge(req).map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let outcome = self
            .inner
            .worker_deployment_runtime()?
            .create_worker_deployment(edge_req)
            .await?;
        Ok(Response::new(
            translate::create_worker_deployment_response_from_edge(outcome.conflict_token),
        ))
    }

    async fn create_worker_deployment_version(
        &self,
        request: Request<workflowservice::CreateWorkerDeploymentVersionRequest>,
    ) -> Result<Response<workflowservice::CreateWorkerDeploymentVersionResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::create_worker_deployment_version_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        self.inner
            .worker_deployment_runtime()?
            .create_worker_deployment_version(edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::CreateWorkerDeploymentVersionResponse::default(),
        ))
    }

    async fn update_worker_deployment_version_compute_config(
        &self,
        request: Request<workflowservice::UpdateWorkerDeploymentVersionComputeConfigRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkerDeploymentVersionComputeConfigResponse>, Status>
    {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let edge_req = translate::update_worker_deployment_version_compute_config_to_edge(req)
            .map_err(proto_conversion_status)?;
        let mut edge_req = edge_req;
        edge_req.namespace_id = namespace_id;
        self.inner
            .worker_deployment_runtime()?
            .update_worker_deployment_version_compute_config(edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::UpdateWorkerDeploymentVersionComputeConfigResponse::default(),
        ))
    }

    async fn validate_worker_deployment_version_compute_config(
        &self,
        request: Request<workflowservice::ValidateWorkerDeploymentVersionComputeConfigRequest>,
    ) -> Result<
        Response<workflowservice::ValidateWorkerDeploymentVersionComputeConfigResponse>,
        Status,
    > {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let edge_req = translate::validate_worker_deployment_version_compute_config_to_edge(req)
            .map_err(proto_conversion_status)?;
        let mut edge_req = edge_req;
        edge_req.namespace_id = namespace_id;
        self.inner
            .worker_deployment_runtime()?
            .validate_worker_deployment_version_compute_config(edge_req)
            .await?;
        Ok(Response::new(
            workflowservice::ValidateWorkerDeploymentVersionComputeConfigResponse::default(),
        ))
    }

    async fn update_worker_deployment_version_metadata(
        &self,
        request: Request<workflowservice::UpdateWorkerDeploymentVersionMetadataRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkerDeploymentVersionMetadataResponse>, Status>
    {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::update_worker_deployment_version_metadata_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let metadata = self
            .inner
            .worker_deployment_runtime()?
            .update_worker_deployment_version_metadata(edge_req)
            .await?;
        Ok(Response::new(
            translate::update_worker_deployment_version_metadata_response_from_edge(&metadata),
        ))
    }

    async fn set_worker_deployment_manager(
        &self,
        request: Request<workflowservice::SetWorkerDeploymentManagerRequest>,
    ) -> Result<Response<workflowservice::SetWorkerDeploymentManagerResponse>, Status> {
        let req = request.into_inner();
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        let mut edge_req = translate::set_worker_deployment_manager_to_edge(req)
            .map_err(proto_conversion_status)?;
        edge_req.namespace_id = namespace_id;
        let outcome = self
            .inner
            .worker_deployment_runtime()?
            .set_worker_deployment_manager(edge_req)
            .await?;
        Ok(Response::new(
            translate::set_worker_deployment_manager_response_from_edge(&outcome.view),
        ))
    }
    async fn describe_worker(
        &self,
        request: Request<workflowservice::DescribeWorkerRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkerResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let context = self
            .inner
            .admit_request(
                &headers,
                Some(&req.namespace),
                Action::DescribeWorker,
                false,
            )
            .await?;
        let namespace_id = context
            .namespace
            .as_ref()
            .map(|namespace| crate::to_internal::namespace_id_for(&namespace.name))
            .ok_or_else(|| Status::internal("worker describe admitted without namespace"))?;
        let key = tokeira_types::WorkerInstanceKey(req.worker_instance_key);
        let heartbeat = self
            .inner
            .heartbeat_store()
            .get_worker(&namespace_id, &key)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found(format!("Worker {} not found", key.0)))?;
        let heartbeat =
            worker_heartbeat::worker_heartbeat_to_proto(&heartbeat).map_err(|error| {
                Status::internal(format!("stored worker heartbeat is invalid: {error}"))
            })?;
        Ok(Response::new(workflowservice::DescribeWorkerResponse {
            worker_info: Some(worker_v1::WorkerInfo {
                worker_heartbeat: Some(heartbeat),
            }),
        }))
    }

    #[allow(deprecated)]
    async fn list_workers(
        &self,
        request: Request<workflowservice::ListWorkersRequest>,
    ) -> Result<Response<workflowservice::ListWorkersResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let context = self
            .inner
            .admit_request(&headers, Some(&req.namespace), Action::ListWorkers, false)
            .await?;
        let namespace_id = context
            .namespace
            .as_ref()
            .map(|namespace| crate::to_internal::namespace_id_for(&namespace.name))
            .ok_or_else(|| Status::internal("worker list admitted without namespace"))?;
        let heartbeats = self
            .inner
            .heartbeat_store()
            .list_workers(&namespace_id)
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(|heartbeat| {
                worker_heartbeat::worker_heartbeat_to_proto(&heartbeat).map_err(|error| {
                    Status::internal(format!("stored worker heartbeat is invalid: {error}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let heartbeats = crate::worker_inventory::filter_workers(heartbeats, &req.query)?;
        let (heartbeats, next_page_token) = crate::worker_inventory::paginate_workers(
            heartbeats,
            req.page_size,
            &req.next_page_token,
        )?;
        let workers_info = heartbeats
            .iter()
            .cloned()
            .map(|heartbeat| worker_v1::WorkerInfo {
                worker_heartbeat: Some(heartbeat),
            })
            .collect();
        let workers = heartbeats
            .iter()
            .map(crate::worker_inventory::worker_list_info)
            .collect();
        Ok(Response::new(workflowservice::ListWorkersResponse {
            workers_info,
            workers,
            next_page_token,
        }))
    }
    // === End Worker Deployments block ===

    // === Workflow Rules ===
    async fn create_workflow_rule(
        &self,
        request: Request<workflowservice::CreateWorkflowRuleRequest>,
    ) -> Result<Response<workflowservice::CreateWorkflowRuleResponse>, Status> {
        if !crate::workflow_service::workflow_rule_crud_admitted(
            crate::workflow_service::workflow_rules_enabled(),
        ) {
            return Err(Status::unimplemented(
                "method CreateWorkflowRule not supported",
            ));
        }
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("Rule Specification is not set."))?;
        if spec.id.is_empty() {
            return Err(Status::invalid_argument("Workflow Rule ID is not set."));
        }
        // v1.31.0 accepts but does not act on force_scan or request_id in
        // CreateWorkflowRule (`workflow_handler.go:6979-7022 @ v1.31.0`).
        // Preserving that no-op contract is safer than making a future-looking
        // field an admission failure.
        let rule = self
            .inner
            .create_workflow_rule(&headers, req.namespace, spec, req.identity, req.description)
            .await?;
        Ok(Response::new(workflowservice::CreateWorkflowRuleResponse {
            rule: Some(rule),
            job_id: String::new(),
        }))
    }

    async fn describe_workflow_rule(
        &self,
        request: Request<workflowservice::DescribeWorkflowRuleRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkflowRuleResponse>, Status> {
        if !crate::workflow_service::workflow_rule_crud_admitted(
            crate::workflow_service::workflow_rules_enabled(),
        ) {
            return Err(Status::unimplemented(
                "method DescribeWorkflowRule not supported",
            ));
        }
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if req.rule_id.is_empty() {
            return Err(Status::invalid_argument("Workflow Rule ID is not set."));
        }
        let rule = self
            .inner
            .describe_workflow_rule(&headers, req.namespace, req.rule_id)
            .await?;
        Ok(Response::new(
            workflowservice::DescribeWorkflowRuleResponse { rule: Some(rule) },
        ))
    }

    async fn delete_workflow_rule(
        &self,
        request: Request<workflowservice::DeleteWorkflowRuleRequest>,
    ) -> Result<Response<workflowservice::DeleteWorkflowRuleResponse>, Status> {
        if !crate::workflow_service::workflow_rule_crud_admitted(
            crate::workflow_service::workflow_rules_enabled(),
        ) {
            return Err(Status::unimplemented(
                "method DeleteWorkflowRule not supported",
            ));
        }
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        if req.rule_id.is_empty() {
            return Err(Status::invalid_argument("Workflow Rule ID is not set."));
        }
        self.inner
            .delete_workflow_rule(&headers, req.namespace, req.rule_id)
            .await?;
        Ok(Response::new(
            workflowservice::DeleteWorkflowRuleResponse {},
        ))
    }

    async fn list_workflow_rules(
        &self,
        request: Request<workflowservice::ListWorkflowRulesRequest>,
    ) -> Result<Response<workflowservice::ListWorkflowRulesResponse>, Status> {
        if !crate::workflow_service::workflow_rule_crud_admitted(
            crate::workflow_service::workflow_rules_enabled(),
        ) {
            return Err(Status::unimplemented(
                "method ListWorkflowRules not supported",
            ));
        }
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let rules = self
            .inner
            .list_workflow_rules(&headers, req.namespace)
            .await?;
        Ok(Response::new(workflowservice::ListWorkflowRulesResponse {
            rules,
            next_page_token: Vec::new(),
        }))
    }

    async fn trigger_workflow_rule(
        &self,
        _request: Request<workflowservice::TriggerWorkflowRuleRequest>,
    ) -> Result<Response<workflowservice::TriggerWorkflowRuleResponse>, Status> {
        // This is the target behavior, not an implementation backlog: the
        // v1.31.0 frontend handler unconditionally returns this exact error.
        Err(Status::unimplemented(
            "method TriggerWorkflowRule not supported",
        ))
    }
    // === End Workflow Rules block ===

    async fn update_task_queue_config(
        &self,
        request: Request<workflowservice::UpdateTaskQueueConfigRequest>,
    ) -> Result<Response<workflowservice::UpdateTaskQueueConfigResponse>, Status> {
        let req = request.into_inner();
        if req.namespace.trim().is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        if req.task_queue.trim().is_empty() {
            return Err(Status::invalid_argument("task queue is required"));
        }
        let namespace_id = self
            .inner
            .resolve_namespace_id(&req.namespace)
            .await
            .map_err(namespace_resolution_status)?;
        let task_queue = TaskQueueName(req.task_queue.clone());
        let existing = self
            .inner
            .task_queue_config_store()
            .get(&namespace_id, &task_queue);
        let mut config = translate::task_queue_config_from_update_request(&req);
        if let Some(existing) = existing {
            if req.update_queue_rate_limit.is_none() {
                config.queue_rate_limit = existing.queue_rate_limit;
                config.queue_rate_limit_metadata = existing
                    .queue_rate_limit_metadata
                    .map(task_queue_config_metadata_to_edge);
            }
            if req.update_fairness_key_rate_limit_default.is_none() {
                config.fairness_key_rate_limit_default = existing.fairness_key_rate_limit_default;
                config.fairness_key_rate_limit_metadata = existing
                    .fairness_key_rate_limit_metadata
                    .map(task_queue_config_metadata_to_edge);
            }
            let mut merged = existing.fairness_weight_overrides;
            for key in &req.unset_fairness_weight_overrides {
                merged.remove(key);
            }
            merged.extend(req.set_fairness_weight_overrides.clone());
            config.fairness_weight_overrides = merged;
        }
        if config.fairness_weight_overrides.len() > tokeira_runtime::max_fairness_weight_overrides()
        {
            return Err(Status::invalid_argument(
                "fairness weight overrides update rejected: maximum number of overrides exceeded",
            ));
        }
        self.inner
            .task_queue_config_store()
            .set(TaskQueueConfigEntry {
                namespace_id,
                task_queue,
                queue_rate_limit: config.queue_rate_limit,
                queue_rate_limit_metadata: config
                    .queue_rate_limit_metadata
                    .clone()
                    .map(task_queue_config_metadata_to_runtime),
                fairness_key_rate_limit_default: config.fairness_key_rate_limit_default,
                fairness_key_rate_limit_metadata: config
                    .fairness_key_rate_limit_metadata
                    .clone()
                    .map(task_queue_config_metadata_to_runtime),
                fairness_weight_overrides: config.fairness_weight_overrides.clone(),
            });
        Ok(Response::new(
            workflowservice::UpdateTaskQueueConfigResponse {
                config: Some(translate::task_queue_config_to_proto(config)),
            },
        ))
    }
    // === Worker Config — deferred to worker-config-management spec ===
    deferred_unary!(
        fetch_worker_config,
        FetchWorkerConfigRequest,
        FetchWorkerConfigResponse,
        "worker-config-management"
    );
    deferred_unary!(
        update_worker_config,
        UpdateWorkerConfigRequest,
        UpdateWorkerConfigResponse,
        "worker-config-management"
    );
    // === End Worker Config block ===

    // === Pause/Unpause Workflow ===
    async fn pause_workflow_execution(
        &self,
        request: Request<workflowservice::PauseWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::PauseWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::pause_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .pause_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::pause_response_to_proto()))
    }

    async fn unpause_workflow_execution(
        &self,
        request: Request<workflowservice::UnpauseWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::UnpauseWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::unpause_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .unpause_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::unpause_response_to_proto()))
    }
    // === End Pause/Unpause Workflow block ===

    // === Activity Executions (standalone) ===
    //
    // All standalone-activity handlers enter the shared edge admission seam
    // before their feature gate or field validation. This mirrors the frontend
    // authorization interceptor running before handlers in v1.31.0 while keeping
    // CHASM execution in Tokeira's existing component bridge.
    async fn start_activity_execution(
        &self,
        request: Request<workflowservice::StartActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::StartActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::StartActivityExecution,
                false,
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "start_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let req = request.into_inner();
        // Validate request_id / identity length before any start work, so a length
        // violation is InvalidArgument rather than colliding with the id dedup
        // (`standalone_activity_test.go:447,467`). Same messages/limit as the
        // cancel/terminate metadata validator.
        validate_sa_request_metadata(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.request_id,
            &req.identity,
        )?;
        let namespace_id = self.resolve_namespace_id(&req.namespace).await?;
        // Reject unregistered search-attribute keys before the start commits
        // (`standalone_activity_test.go:521`). Keys are read here, before the
        // SearchAttributes are encoded into the StartActivity below.
        if let Some(sa) = req.search_attributes.as_ref() {
            let keys: Vec<String> = sa.indexed_fields.keys().cloned().collect();
            self.inner
                .validate_search_attribute_keys(namespace_id, &keys)
                .await?;
        }
        // Map the id reuse/conflict policy before minting a run id — an unsupported
        // policy is rejected with InvalidArgument, mirroring the chasm activity
        // handler's policy-map lookup (`handler.go:54-61 @ v1.31.0`).
        let policy =
            translate::activity_id_policy_to_chasm(req.id_reuse_policy, req.id_conflict_policy)
                .map_err(proto_conversion_status)?;
        // The run id names this instance; the start request carries none, so the
        // server mints it (UUIDv4), mirroring run-id assignment for workflows.
        let run_id = uuid::Uuid::new_v4().to_string();
        // Apply Temporal's retry-policy defaults once, here at the edge, and fold the
        // result into scalar fields on the activity state (the pure crate is
        // proto-free). Computed before `req.retry_policy` is moved into the opaque
        // describe-echo bytes below.
        let retry_policy = defaulted_retry_policy(req.retry_policy.as_ref());
        let retry_initial = proto_duration_to_nanos(retry_policy.initial_interval.as_ref());
        let retry_coefficient = retry_policy.backoff_coefficient;
        let retry_maximum = proto_duration_to_nanos(retry_policy.maximum_interval.as_ref());
        let retry_max_attempts = retry_policy.maximum_attempts;
        let start = crate::chasm_activity::StartActivity {
            namespace_id: namespace_id.0.to_string(),
            activity_id: req.activity_id,
            run_id,
            activity_type: req.activity_type.map(|t| t.name).unwrap_or_default(),
            task_queue: req.task_queue.map(|q| q.name).unwrap_or_default(),
            // The activity input is opaque to the edge; carry the encoded
            // `Payloads` envelope through to the component verbatim.
            input: req.input.map(|p| p.encode_to_vec()).unwrap_or_default(),
            schedule_to_start_nanos: proto_duration_to_nanos(
                req.schedule_to_start_timeout.as_ref(),
            ),
            schedule_to_close_nanos: proto_duration_to_nanos(
                req.schedule_to_close_timeout.as_ref(),
            ),
            start_to_close_nanos: proto_duration_to_nanos(req.start_to_close_timeout.as_ref()),
            heartbeat_nanos: proto_duration_to_nanos(req.heartbeat_timeout.as_ref()),
            // Standalone activities have no enclosing run; schedule-to-close is the
            // outer cap, applied during normalization.
            run_timeout_nanos: 0,
            request_id: (!req.request_id.is_empty()).then_some(req.request_id),
            policy,
            // Describe-echo fields: stored opaque and returned verbatim by
            // DescribeActivityExecution (Req 5). Encoded here at the edge boundary so
            // the component holds only bytes.
            header: req.header.map(|h| h.encode_to_vec()).unwrap_or_default(),
            retry_policy: retry_policy.encode_to_vec(),
            // Fold the (defaulted) retry policy into scalar fields so the pure retry
            // decision needs no proto. Defaults match v1.31.0's `EnsureDefaults`.
            retry_initial_interval_nanos: retry_initial,
            retry_backoff_coefficient: retry_coefficient,
            retry_maximum_interval_nanos: retry_maximum,
            maximum_attempts: retry_max_attempts,
            priority: req.priority.map(|p| p.encode_to_vec()).unwrap_or_default(),
            search_attributes: req
                .search_attributes
                .map(|s| s.encode_to_vec())
                .unwrap_or_default(),
            user_metadata: req
                .user_metadata
                .map(|u| u.encode_to_vec())
                .unwrap_or_default(),
        };
        let outcome = bridge.start(start).await?;
        Ok(Response::new(
            workflowservice::StartActivityExecutionResponse {
                run_id: outcome.reference.execution_key.run_id,
                started: outcome.started,
                link: None,
            },
        ))
    }
    async fn describe_activity_execution(
        &self,
        request: Request<workflowservice::DescribeActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::DescribeActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::DescribeActivityExecution,
                !request.get_ref().long_poll_token.is_empty(),
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "describe_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let caller_timeout = parse_grpc_timeout(request.metadata());
        let req = request.into_inner();
        if bridge.is_enabled() {
            validate_sa_activity_id(&req.activity_id, bridge.max_id_length())?;
            validate_sa_run_id(&req.run_id)?;
            // A long-poll token needs a concrete run id to anchor the wait
            // (`chasm/lib/activity/validator.go @ v1.31.0`).
            if !req.long_poll_token.is_empty() && req.run_id.is_empty() {
                return Err(Status::invalid_argument(
                    "run id is required when long poll token is provided",
                ));
            }
        }
        let key = self
            .activity_execution_key(
                bridge,
                &req.namespace,
                req.activity_id.clone(),
                req.run_id.clone(),
            )
            .await?;
        // Echo the resolved run id (a bare-id describe resolves to the current run,
        // and the response must carry that concrete run — `standalone_activity_test.go:2826`).
        let resolved_run_id = key.run_id.clone();
        // Absent a token, return the current state immediately. With a token, this is a
        // long-poll for any state change (`frontend.go @ v1.31.0`).
        if req.long_poll_token.is_empty() {
            let description = bridge.describe(key.clone()).await?;
            let token = bridge.encode_describe_token(&key, description.execution_vt);
            return Ok(Response::new(chasm_describe_response(
                req.activity_id,
                resolved_run_id,
                req.include_input,
                req.include_outcome,
                description,
                token,
            )));
        }
        // Decode + validate the caller's token against the requested execution before
        // waiting: a malformed token or one issued for a different execution/namespace
        // is rejected up front (`chasm/lib/activity/handler.go:147-150 @ v1.31.0`).
        // First, though, confirm the requested activity exists — v1.31.0 loads the
        // component via the request's ref (NotFound if absent) inside PollComponent
        // before `ExecutionStateChanged` runs, so a long-poll for a missing activity
        // is NotFound, not a token error (`standalone_activity_test.go:3931`).
        bridge.describe(key.clone()).await?;
        let since = bridge
            .decode_describe_token(&req.long_poll_token, &key)
            .map_err(Status::from)?;
        // Time the wait out at Min(caller_deadline - buffer, long_poll_timeout) and return an
        // empty, non-error response on elapse so the caller resubmits — never letting the
        // caller's gRPC deadline fire (`chasm/lib/activity/handler.go` →
        // `contextutil.WithDeadlineBuffer` @ v1.31.0).
        let budget = describe_long_poll_budget(
            caller_timeout,
            bridge.long_poll_timeout(),
            bridge.long_poll_buffer(),
        );
        let advanced = if budget.is_zero() {
            None
        } else {
            match tokio::time::timeout(budget, bridge.poll(key.clone(), since)).await {
                Ok(result) => result?,
                Err(_elapsed) => None,
            }
        };
        match advanced {
            Some(description) => {
                let token = bridge.encode_describe_token(&key, description.execution_vt);
                Ok(Response::new(chasm_describe_response(
                    req.activity_id,
                    resolved_run_id,
                    req.include_input,
                    req.include_outcome,
                    description,
                    token,
                )))
            }
            // Empty non-error response: an invitation to resubmit the long-poll.
            None => Ok(Response::new(
                workflowservice::DescribeActivityExecutionResponse::default(),
            )),
        }
    }
    async fn poll_activity_execution(
        &self,
        request: Request<workflowservice::PollActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::PollActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::PollActivityExecution,
                true,
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "poll_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let req = request.into_inner();
        validate_sa_ids(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.activity_id,
            &req.run_id,
        )?;
        let key = self
            .activity_execution_key(
                bridge,
                &req.namespace,
                req.activity_id.clone(),
                req.run_id.clone(),
            )
            .await?;
        // Echo the resolved run id (a bare-id poll resolves to the current run —
        // `standalone_activity_test.go:3274`).
        let resolved_run_id = key.run_id.clone();
        let description = bridge.poll_outcome(key).await?;
        Ok(Response::new(chasm_poll_response(
            resolved_run_id,
            description,
        )))
    }
    async fn list_activity_executions(
        &self,
        request: Request<workflowservice::ListActivityExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListActivityExecutionsResponse>, Status> {
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "list_activity_executions is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_activity_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_activity_executions(&headers, bridge.archetype_id(), edge_req)
            .await?;
        Ok(Response::new(translate::list_activity_response_to_proto(
            edge_resp,
        )))
    }

    async fn count_activity_executions(
        &self,
        request: Request<workflowservice::CountActivityExecutionsRequest>,
    ) -> Result<Response<workflowservice::CountActivityExecutionsResponse>, Status> {
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "count_activity_executions is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::count_activity_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .count_activity_executions(&headers, bridge.archetype_id(), edge_req)
            .await?;
        Ok(Response::new(translate::count_activity_response_to_proto(
            edge_resp,
        )))
    }
    async fn request_cancel_activity_execution(
        &self,
        request: Request<workflowservice::RequestCancelActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::RequestCancelActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::RequestCancelActivityExecution,
                false,
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "request_cancel_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let req = request.into_inner();
        validate_sa_ids(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.activity_id,
            &req.run_id,
        )?;
        validate_sa_request_metadata(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.request_id,
            &req.identity,
        )?;
        let key = self
            .activity_execution_key(bridge, &req.namespace, req.activity_id, req.run_id)
            .await?;
        bridge
            .request_cancel(key, req.identity, req.request_id, req.reason)
            .await?;
        Ok(Response::new(
            workflowservice::RequestCancelActivityExecutionResponse {},
        ))
    }
    async fn terminate_activity_execution(
        &self,
        request: Request<workflowservice::TerminateActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::TerminateActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::TerminateActivityExecution,
                false,
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "terminate_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let req = request.into_inner();
        validate_sa_ids(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.activity_id,
            &req.run_id,
        )?;
        validate_sa_request_metadata(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.request_id,
            &req.identity,
        )?;
        let key = self
            .activity_execution_key(bridge, &req.namespace, req.activity_id, req.run_id)
            .await?;
        bridge
            .terminate(key, req.reason, req.request_id, req.identity)
            .await?;
        Ok(Response::new(
            workflowservice::TerminateActivityExecutionResponse {},
        ))
    }
    async fn delete_activity_execution(
        &self,
        request: Request<workflowservice::DeleteActivityExecutionRequest>,
    ) -> Result<Response<workflowservice::DeleteActivityExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        self.inner
            .admit_request(
                &headers,
                Some(&request.get_ref().namespace),
                Action::DeleteActivityExecution,
                false,
            )
            .await?;
        let Some(bridge) = &self.chasm_activity else {
            return Err(Status::unimplemented(
                "delete_activity_execution is not implemented; tracked in spec activity-executions-first-class",
            ));
        };
        let req = request.into_inner();
        validate_sa_ids(
            bridge.is_enabled(),
            bridge.max_id_length(),
            &req.activity_id,
            &req.run_id,
        )?;
        let key = self
            .activity_execution_key(bridge, &req.namespace, req.activity_id, req.run_id)
            .await?;
        bridge.delete(key).await?;
        Ok(Response::new(
            workflowservice::DeleteActivityExecutionResponse {},
        ))
    }
    // === End Activity Executions block ===

    deferred_unary!(
        start_nexus_operation_execution,
        StartNexusOperationExecutionRequest,
        StartNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        describe_nexus_operation_execution,
        DescribeNexusOperationExecutionRequest,
        DescribeNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        poll_nexus_operation_execution,
        PollNexusOperationExecutionRequest,
        PollNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        list_nexus_operation_executions,
        ListNexusOperationExecutionsRequest,
        ListNexusOperationExecutionsResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        count_nexus_operation_executions,
        CountNexusOperationExecutionsRequest,
        CountNexusOperationExecutionsResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        request_cancel_nexus_operation_execution,
        RequestCancelNexusOperationExecutionRequest,
        RequestCancelNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        terminate_nexus_operation_execution,
        TerminateNexusOperationExecutionRequest,
        TerminateNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    deferred_unary!(
        delete_nexus_operation_execution,
        DeleteNexusOperationExecutionRequest,
        DeleteNexusOperationExecutionResponse,
        "edge-nexus-task-transport"
    );
    async fn update_activity_options(
        &self,
        request: Request<workflowservice::UpdateActivityOptionsRequest>,
    ) -> Result<Response<workflowservice::UpdateActivityOptionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::update_activity_options_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .update_activity_options(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::update_activity_options_to_proto(
            edge_resp,
        )))
    }
    async fn update_workflow_execution_options(
        &self,
        request: Request<workflowservice::UpdateWorkflowExecutionOptionsRequest>,
    ) -> Result<Response<workflowservice::UpdateWorkflowExecutionOptionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req =
            translate::update_workflow_execution_options_request_to_edge(request.into_inner())
                .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .update_workflow_execution_options(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::update_workflow_execution_options_response_to_proto(edge_resp),
        ))
    }
    async fn pause_activity(
        &self,
        request: Request<workflowservice::PauseActivityRequest>,
    ) -> Result<Response<workflowservice::PauseActivityResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::pause_activity_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.pause_activity(&headers, edge_req).await?;
        Ok(Response::new(translate::pause_activity_to_proto()))
    }
    async fn unpause_activity(
        &self,
        request: Request<workflowservice::UnpauseActivityRequest>,
    ) -> Result<Response<workflowservice::UnpauseActivityResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::unpause_activity_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.unpause_activity(&headers, edge_req).await?;
        Ok(Response::new(translate::unpause_activity_to_proto()))
    }
    async fn reset_activity(
        &self,
        request: Request<workflowservice::ResetActivityRequest>,
    ) -> Result<Response<workflowservice::ResetActivityResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::reset_activity_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.reset_activity(&headers, edge_req).await?;
        Ok(Response::new(translate::reset_activity_to_proto()))
    }
}

fn update_lifecycle_stage_to_proto(stage: tokeira_runtime::UpdateLifecycleStage) -> i32 {
    match stage {
        tokeira_runtime::UpdateLifecycleStage::Unspecified => {
            tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Unspecified as i32
        }
        tokeira_runtime::UpdateLifecycleStage::Admitted => {
            tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Admitted as i32
        }
        tokeira_runtime::UpdateLifecycleStage::Accepted => {
            tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Accepted as i32
        }
        tokeira_runtime::UpdateLifecycleStage::Completed => {
            tokeira_proto::enums::UpdateWorkflowExecutionLifecycleStage::Completed as i32
        }
    }
}

#[cfg(test)]
mod tests {
    // Several tests construct/inspect proto messages with fields Temporal has
    // deprecated but still ships on the wire (DescribeTaskQueue status,
    // RespondNexusTaskFailed error); exercising them is required for v1.31.0.
    #![allow(deprecated)]

    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use async_trait::async_trait;
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokio::sync::Notify;
    use tonic::Request;
    use uuid::Uuid;

    use super::*;
    use crate::{
        history_wait::{HistoryNotifyingRepository, HistoryWaitRegistry},
        interceptors::EdgeInterceptors,
        long_poll::{LongPollConfig, LongPollGate},
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace},
        operator_service::InMemoryOperatorApi,
        poller_registry::PollerRegistry,
        routing::LocalOnlyRouter,
        to_internal::namespace_id_for,
        workflow_service::{
            EmptyVisibilityApi, ExecutionResolver, WorkflowMutationOutcome, WorkflowRuntimeApi,
        },
    };
    use tokeira_kernel::{
        BasicKernel, Command, Kernel, LoadedRun, NexusResolution, SignalRequest, StartRequest,
    };
    use tokeira_proto::public::temporal::api::{
        deployment::v1::WorkerDeploymentVersion, nexus::v1 as nexus_v1, worker::v1 as worker_v1,
    };
    use tokeira_runtime::{
        NexusHttpTaskRequest, NexusHttpTaskRequestVariant, NexusTask, NexusTaskBroker,
        NexusTaskRequest, NexusTaskToken, WorkerRegistry,
    };
    use tokeira_storage::{CommitResult, DispatchableWorkflowTask, RunRepository};
    use tokeira_types::{
        EventPrincipal, EvictionReport, HeartbeatStore, HeartbeatStoreError, LogicalTaskSeq, Memo,
        Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey, SearchAttributes, ShardEpoch,
        TaskKind, TaskQueueName, WorkerHeartbeat as HeartbeatObservation, WorkerIdentity,
        WorkerInstanceKey, WorkflowId, WorkflowType,
    };

    #[test]
    fn chasm_activity_outcome_terminated_carries_terminated_failure_info() {
        // TransitionTerminated yields a Failure with TerminatedFailureInfo
        // (statemachine.go:307 @ v1.31.0); the describe outcome must carry non-nil
        // TerminatedFailureInfo (standalone_activity_test.go:3064).
        use tokeira_proto::failure::failure::FailureInfo;
        let description = crate::chasm_activity::ActivityDescription {
            status: ActivityStatus::Terminated,
            failure: "test termination".to_owned(),
            terminate_identity: "terminator".to_owned(),
            ..Default::default()
        };
        let outcome = chasm_activity_outcome(&description).expect("terminal outcome");
        match outcome.value {
            Some(activity_v1::activity_execution_outcome::Value::Failure(failure)) => {
                assert_eq!(failure.message, "test termination");
                let Some(FailureInfo::TerminatedFailureInfo(info)) = failure.failure_info else {
                    panic!("expected terminated failure info");
                };
                assert_eq!(info.identity, "terminator");
                assert!(chasm_last_failure(&description).is_none());
            }
            other => panic!("expected failure outcome, got {other:?}"),
        }
    }

    #[test]
    fn chasm_activity_outcome_failed_round_trips_structured_failure() {
        // A worker's full Failure (here ApplicationFailureInfo) round-trips through
        // the stored failure_payload to the describe outcome, not just the message
        // (standalone_activity_test.go:3047 asserts ProtoEqual on the failure).
        use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
        let original = Failure {
            message: "Failed Activity".to_owned(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "Test".to_owned(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let description = crate::chasm_activity::ActivityDescription {
            status: ActivityStatus::Failed,
            failure: original.message.clone(),
            failure_payload: original.encode_to_vec(),
            ..Default::default()
        };
        let outcome = chasm_activity_outcome(&description).expect("terminal outcome");
        match outcome.value {
            Some(activity_v1::activity_execution_outcome::Value::Failure(failure)) => {
                assert_eq!(failure, original);
            }
            other => panic!("expected failure outcome, got {other:?}"),
        }
    }

    #[test]
    fn chasm_describe_response_sets_token_and_execution_duration_when_terminal() {
        // A completed describe must carry a non-nil long-poll token and a positive
        // execution_duration: the suite asserts both in validateCompletion via
        // validateBaseActivityResponse (standalone_activity_test.go:4918 — NotNil
        // token; the completion path also asserts ExecutionDuration > 0). Temporal
        // sets the token unconditionally (ctx.Ref, activity.go:723) and the duration
        // as close − schedule only when closed (activity.go:649-652 @ v1.31.0).
        let scheduled = 1_700_000_000_000_000_000;
        let description = crate::chasm_activity::ActivityDescription {
            status: ActivityStatus::Completed,
            scheduled_time_nanos: scheduled,
            close_time_nanos: scheduled + 250_000_000,
            execution_vt: tokeira_chasm::VersionedTransition::new(1, 5),
            ..Default::default()
        };
        let response = chasm_describe_response(
            "act-1".to_owned(),
            "run-1".to_owned(),
            false,
            false,
            description,
            vec![7, 7, 7],
        );
        assert_eq!(
            response.long_poll_token,
            vec![7, 7, 7],
            "the caller-supplied long-poll token is echoed verbatim"
        );
        let duration = response
            .info
            .expect("info present")
            .execution_duration
            .expect("execution_duration set when closed");
        assert_eq!(duration.seconds, 0);
        assert_eq!(duration.nanos, 250_000_000);
    }

    #[test]
    fn chasm_describe_response_omits_execution_duration_while_running() {
        // While running (close_time_nanos == 0) the field is absent — the proto
        // contract is "populated only if the activity is closed" (activity/v1/
        // message.proto field 16; activity.go:649-652 @ v1.31.0).
        let description = crate::chasm_activity::ActivityDescription {
            status: ActivityStatus::Started,
            scheduled_time_nanos: 1_700_000_000_000_000_000,
            execution_vt: tokeira_chasm::VersionedTransition::new(1, 2),
            ..Default::default()
        };
        let response = chasm_describe_response(
            "act-1".to_owned(),
            "run-1".to_owned(),
            false,
            false,
            description,
            vec![9],
        );
        assert!(
            !response.long_poll_token.is_empty(),
            "running describe carries a long-poll token to block on"
        );
        assert!(
            response
                .info
                .expect("info present")
                .execution_duration
                .is_none(),
            "execution_duration must be unset while running"
        );
    }

    #[test]
    fn chasm_describe_defaults_match_normalized_start_request() {
        // validateAndPopulateStartRequest materializes retry defaults before CHASM
        // persists the request, and buildActivityExecutionInfo returns explicit zero
        // timeout/search-attribute messages (`frontend.go` and `activity.go @ v1.31.0`).
        let info = chasm_activity_info(
            "act-1".to_owned(),
            "run-1".to_owned(),
            &crate::chasm_activity::ActivityDescription::default(),
        );

        for timeout in [
            info.schedule_to_close_timeout,
            info.schedule_to_start_timeout,
            info.start_to_close_timeout,
            info.heartbeat_timeout,
        ] {
            assert_eq!(timeout, Some(prost_types::Duration::default()));
        }
        let retry = info.retry_policy.expect("normalized retry policy");
        assert_eq!(retry.initial_interval.expect("initial").seconds, 1);
        assert_eq!(retry.backoff_coefficient, 2.0);
        assert_eq!(retry.maximum_interval.expect("maximum").seconds, 100);
        assert_eq!(retry.maximum_attempts, 0);
        assert_eq!(
            info.search_attributes,
            Some(tokeira_proto::common::SearchAttributes::default())
        );
    }

    struct PollNoneRuntime;

    struct BlockingPollRuntime {
        ready: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct NexusRecordingRuntime {
        applied: bool,
        resolutions: Mutex<Vec<(RunKey, String, i64, NexusResolution)>>,
    }

    fn test_worker_heartbeat(key: &str) -> worker_v1::WorkerHeartbeat {
        worker_v1::WorkerHeartbeat {
            worker_instance_key: key.to_string(),
            worker_identity: format!("identity-{key}"),
            host_info: None,
            task_queue: "queue".to_string(),
            deployment_version: Some(WorkerDeploymentVersion {
                build_id: "build-a".to_string(),
                deployment_name: "deployment-a".to_string(),
            }),
            sdk_name: String::new(),
            sdk_version: "rust-0.4".to_string(),
            status: 1,
            start_time: None,
            heartbeat_time: None,
            elapsed_since_last_heartbeat: None,
            workflow_task_slots_info: None,
            activity_task_slots_info: None,
            nexus_task_slots_info: None,
            local_activity_slots_info: None,
            workflow_poller_info: None,
            workflow_sticky_poller_info: None,
            activity_poller_info: None,
            nexus_poller_info: None,
            total_sticky_cache_hit: 0,
            total_sticky_cache_miss: 0,
            current_sticky_cache_size: 0,
            plugins: Vec::new(),
            drivers: Vec::new(),
        }
    }

    impl NexusRecordingRuntime {
        fn new(applied: bool) -> Self {
            Self {
                applied,
                resolutions: Mutex::new(Vec::new()),
            }
        }

        fn recorded(&self) -> Vec<(RunKey, String, i64, NexusResolution)> {
            self.resolutions.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WorkflowRuntimeApi for PollNoneRuntime {
        async fn start_workflow(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn start_workflow_with_policy(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<tokeira_runtime::StartWorkflowResult> {
            unreachable!()
        }

        async fn signal_with_start_workflow(
            &self,
            _req: tokeira_kernel::SignalWithStartRequest,
        ) -> Result<tokeira_runtime::SignalWithStartResult> {
            unreachable!()
        }

        async fn signal_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::SignalRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_workflow_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
            Ok(None)
        }

        async fn complete_workflow_task(
            &self,
            _req: tokeira_kernel::WorkflowTaskCompletedRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_activity_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
            Ok(None)
        }

        async fn complete_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _result: tokeira_types::Payloads,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn fail_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _failure: tokeira_types::Payload,
            _failure_error_type: Option<String>,
            _is_non_retryable: bool,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<()> {
            unreachable!()
        }

        async fn record_activity_heartbeat(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _details: Option<tokeira_types::Payloads>,
            _identity: Option<tokeira_types::WorkerIdentity>,
        ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome> {
            unreachable!()
        }

        async fn terminate_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::TerminateRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn cancel_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::CancelRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn reset_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _req: tokeira_kernel::ResetRequest,
        ) -> Result<tokeira_runtime::ResetWorkflowResult> {
            unreachable!()
        }

        async fn query_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _query_type: String,
            _query_args: tokeira_types::Payloads,
            _timeout: std::time::Duration,
        ) -> Result<tokeira_runtime::QueryResult> {
            unreachable!()
        }

        async fn update_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _update_id: String,
            _update_name: String,
            _input: tokeira_types::Payloads,
            _request: tokeira_types::RequestContext,
            _timeout: std::time::Duration,
            _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        ) -> Result<tokeira_runtime::UpdateLifecycleSnapshot> {
            unreachable!()
        }

        async fn pending_update_transports(
            &self,
            _run_key: tokeira_types::RunKey,
            _include_sent: bool,
        ) -> Result<Vec<tokeira_runtime::PendingUpdateTransport>> {
            Ok(Vec::new())
        }

        async fn resolve_update_transport(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
            _resolution: tokeira_runtime::UpdateTransportResolution,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn peek_update_info(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
        ) -> Result<Option<(String, tokeira_types::Payloads)>> {
            Ok(None)
        }

        async fn resolve_nexus_operation(
            &self,
            _run_key: tokeira_types::RunKey,
            _operation_id: String,
            _scheduled_event_id: i64,
            _resolution: NexusResolution,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn record_nexus_cancellation_attempt(
            &self,
            _run_key: tokeira_types::RunKey,
            _operation_id: String,
            _scheduled_event_id: i64,
            _requested_event_id: i64,
            _outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[async_trait]
    impl WorkflowRuntimeApi for BlockingPollRuntime {
        async fn start_workflow(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn start_workflow_with_policy(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<tokeira_runtime::StartWorkflowResult> {
            unreachable!()
        }

        async fn signal_with_start_workflow(
            &self,
            _req: tokeira_kernel::SignalWithStartRequest,
        ) -> Result<tokeira_runtime::SignalWithStartResult> {
            unreachable!()
        }

        async fn signal_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::SignalRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_workflow_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
            self.ready.notify_waiters();
            self.release.notified().await;
            Ok(None)
        }

        async fn complete_workflow_task(
            &self,
            _req: tokeira_kernel::WorkflowTaskCompletedRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_activity_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
            unreachable!()
        }

        async fn complete_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _result: tokeira_types::Payloads,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn fail_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _failure: tokeira_types::Payload,
            _failure_error_type: Option<String>,
            _is_non_retryable: bool,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<()> {
            unreachable!()
        }

        async fn record_activity_heartbeat(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _details: Option<tokeira_types::Payloads>,
            _identity: Option<tokeira_types::WorkerIdentity>,
        ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome> {
            unreachable!()
        }

        async fn terminate_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::TerminateRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn cancel_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::CancelRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn reset_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _req: tokeira_kernel::ResetRequest,
        ) -> Result<tokeira_runtime::ResetWorkflowResult> {
            unreachable!()
        }

        async fn query_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _query_type: String,
            _query_args: tokeira_types::Payloads,
            _timeout: std::time::Duration,
        ) -> Result<tokeira_runtime::QueryResult> {
            unreachable!()
        }

        async fn update_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _update_id: String,
            _update_name: String,
            _input: tokeira_types::Payloads,
            _request: tokeira_types::RequestContext,
            _timeout: std::time::Duration,
            _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        ) -> Result<tokeira_runtime::UpdateLifecycleSnapshot> {
            unreachable!()
        }

        async fn pending_update_transports(
            &self,
            _run_key: tokeira_types::RunKey,
            _include_sent: bool,
        ) -> Result<Vec<tokeira_runtime::PendingUpdateTransport>> {
            Ok(Vec::new())
        }

        async fn resolve_update_transport(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
            _resolution: tokeira_runtime::UpdateTransportResolution,
        ) -> Result<bool> {
            Ok(false)
        }
        async fn peek_update_info(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
        ) -> Result<Option<(String, tokeira_types::Payloads)>> {
            Ok(None)
        }

        async fn resolve_nexus_operation(
            &self,
            _run_key: tokeira_types::RunKey,
            _operation_id: String,
            _scheduled_event_id: i64,
            _resolution: NexusResolution,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn record_nexus_cancellation_attempt(
            &self,
            _run_key: tokeira_types::RunKey,
            _operation_id: String,
            _scheduled_event_id: i64,
            _requested_event_id: i64,
            _outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[async_trait]
    impl WorkflowRuntimeApi for NexusRecordingRuntime {
        async fn start_workflow(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn start_workflow_with_policy(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<tokeira_runtime::StartWorkflowResult> {
            unreachable!()
        }

        async fn signal_with_start_workflow(
            &self,
            _req: tokeira_kernel::SignalWithStartRequest,
        ) -> Result<tokeira_runtime::SignalWithStartResult> {
            unreachable!()
        }

        async fn signal_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::SignalRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_workflow_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
            Ok(None)
        }

        async fn complete_workflow_task(
            &self,
            _req: tokeira_kernel::WorkflowTaskCompletedRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_activity_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
            Ok(None)
        }

        async fn complete_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _result: tokeira_types::Payloads,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn fail_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _failure: tokeira_types::Payload,
            _failure_error_type: Option<String>,
            _is_non_retryable: bool,
            _worker_identity: Option<tokeira_types::WorkerIdentity>,
            _request: tokeira_types::RequestContext,
        ) -> Result<()> {
            unreachable!()
        }

        async fn record_activity_heartbeat(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _details: Option<tokeira_types::Payloads>,
            _identity: Option<tokeira_types::WorkerIdentity>,
        ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome> {
            unreachable!()
        }

        async fn terminate_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::TerminateRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn cancel_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::CancelRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn reset_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _req: tokeira_kernel::ResetRequest,
        ) -> Result<tokeira_runtime::ResetWorkflowResult> {
            unreachable!()
        }

        async fn query_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _query_type: String,
            _query_args: tokeira_types::Payloads,
            _timeout: std::time::Duration,
        ) -> Result<tokeira_runtime::QueryResult> {
            unreachable!()
        }

        async fn update_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _update_id: String,
            _update_name: String,
            _input: tokeira_types::Payloads,
            _request: tokeira_types::RequestContext,
            _timeout: std::time::Duration,
            _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        ) -> Result<tokeira_runtime::UpdateLifecycleSnapshot> {
            unreachable!()
        }

        async fn pending_update_transports(
            &self,
            _run_key: tokeira_types::RunKey,
            _include_sent: bool,
        ) -> Result<Vec<tokeira_runtime::PendingUpdateTransport>> {
            Ok(Vec::new())
        }

        async fn resolve_update_transport(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
            _resolution: tokeira_runtime::UpdateTransportResolution,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn peek_update_info(
            &self,
            _run_key: tokeira_types::RunKey,
            _update_id: String,
        ) -> Result<Option<(String, tokeira_types::Payloads)>> {
            Ok(None)
        }

        async fn resolve_nexus_operation(
            &self,
            run_key: tokeira_types::RunKey,
            operation_id: String,
            scheduled_event_id: i64,
            resolution: NexusResolution,
        ) -> Result<bool> {
            self.resolutions.lock().unwrap().push((
                run_key,
                operation_id,
                scheduled_event_id,
                resolution,
            ));
            Ok(self.applied)
        }

        async fn record_nexus_cancellation_attempt(
            &self,
            _run_key: tokeira_types::RunKey,
            _operation_id: String,
            _scheduled_event_id: i64,
            _requested_event_id: i64,
            _outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[derive(Default)]
    struct NoopResolver;

    #[derive(Default)]
    struct StaticNamespaceCache;

    struct FailingHeartbeatStore;

    impl HeartbeatStore for FailingHeartbeatStore {
        fn insert(&self, _heartbeat: HeartbeatObservation) -> Result<(), HeartbeatStoreError> {
            Err(HeartbeatStoreError::Backend("injected failure".to_owned()))
        }

        fn get_worker(
            &self,
            _namespace: &NamespaceId,
            _worker_instance_key: &WorkerInstanceKey,
        ) -> Result<Option<HeartbeatObservation>, HeartbeatStoreError> {
            Err(HeartbeatStoreError::Backend("injected failure".to_owned()))
        }

        fn list_workers(
            &self,
            _namespace: &NamespaceId,
        ) -> Result<Vec<HeartbeatObservation>, HeartbeatStoreError> {
            Err(HeartbeatStoreError::Backend("injected failure".to_owned()))
        }

        fn maintain(
            &self,
            _now: OffsetDateTime,
            _ttl: time::Duration,
            _min_evict_age: time::Duration,
            _max_entries: usize,
        ) -> Result<EvictionReport, HeartbeatStoreError> {
            Err(HeartbeatStoreError::Backend("injected failure".to_owned()))
        }
    }

    #[async_trait]
    impl ExecutionResolver for NoopResolver {
        async fn current_run_key(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<Option<tokeira_types::RunKey>> {
            Ok(None)
        }

        async fn describe_execution(
            &self,
            _namespace: &str,
            _workflow_id: &str,
            _run_id: Option<tokeira_types::RunId>,
        ) -> Result<Option<crate::WorkflowExecutionDescription>> {
            Ok(None)
        }
    }

    #[async_trait]
    impl NamespaceCache for StaticNamespaceCache {
        async fn get(&self, name: &str) -> Result<Option<ResolvedNamespace>> {
            Ok(Some(ResolvedNamespace::active(name)))
        }

        async fn list_all(&self) -> Result<Vec<ResolvedNamespace>> {
            Ok(vec![ResolvedNamespace::active("default")])
        }

        async fn insert(&self, _ns: ResolvedNamespace) -> Result<()> {
            Ok(())
        }
    }

    fn versioning_test_service() -> (WorkflowServiceGrpc, tokeira_runtime::InMemoryBroker) {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let broker = tokeira_runtime::InMemoryBroker::default();
        let service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::BufferedQueryRegistry::default(),
            broker.clone(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
            HistoryWaitRegistry::default(),
        );
        (WorkflowServiceGrpc::new(service), broker)
    }

    async fn worker_heartbeat_test_service()
    -> (WorkflowServiceGrpc, tokeira_runtime::InMemoryBroker) {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .expect("default namespace should seed");
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let broker = tokeira_runtime::InMemoryBroker::default();
        let service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::BufferedQueryRegistry::default(),
            broker.clone(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
            HistoryWaitRegistry::default(),
        );
        (WorkflowServiceGrpc::new(service), broker)
    }

    fn worker_deployment_test_service() -> WorkflowServiceGrpc {
        let cache: Arc<dyn NamespaceCache> = Arc::new(StaticNamespaceCache);
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let service =
            WorkflowService::new_with_stores_and_buffered_queries_and_history_wait_registry(
                Arc::new(PollNoneRuntime),
                Arc::new(NoopResolver),
                Arc::new(EmptyVisibilityApi),
                Arc::new(tokeira_storage::InMemoryStore::default()),
                operator_api,
                cache.clone(),
                Arc::new(EdgeInterceptors::permissive(cache)),
                PollerRegistry::default(),
                crate::PendingQueryStore::default(),
                tokeira_runtime::BufferedQueryRegistry::default(),
                tokeira_runtime::InMemoryBroker::default(),
                tokeira_runtime::NexusTaskBroker::default(),
                LongPollGate::new(LongPollConfig::default()),
                Arc::new(LocalOnlyRouter),
                HistoryWaitRegistry::default(),
                WorkerRegistry::default(),
                Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
                Arc::new(tokeira_runtime::ScheduleStore::default()),
                Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
                Arc::new(tokeira_runtime::BatchOperationStore::default()),
            );
        WorkflowServiceGrpc::new(service)
    }

    /// api-conformance-task-queue Property 1 (Single-Partition Compatibility): with
    /// tokeira's single-partition model, `ListTaskQueuePartitions` returns exactly one
    /// root partition per task type, keyed by the bare task-queue name — no invented
    /// extra partitions.
    #[tokio::test]
    async fn list_task_queue_partitions_returns_one_root_partition_per_type() {
        let grpc = worker_deployment_test_service();
        let resp = grpc
            .list_task_queue_partitions(Request::new(
                workflowservice::ListTaskQueuePartitionsRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(
                        tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                            name: "queue".to_string(),
                            kind: tokeira_proto::enums::TaskQueueKind::Normal as i32,
                            ..Default::default()
                        },
                    ),
                },
            ))
            .await
            .expect("list partitions should succeed")
            .into_inner();

        assert_eq!(resp.activity_task_queue_partitions.len(), 1);
        assert_eq!(resp.workflow_task_queue_partitions.len(), 1);
        assert_eq!(resp.activity_task_queue_partitions[0].key, "queue");
        assert_eq!(resp.workflow_task_queue_partitions[0].key, "queue");
    }

    /// api-conformance-workflow-options Property 3 (Expected Error Mapping): a malformed
    /// run id is `INVALID_ARGUMENT` and a missing execution is `NOT_FOUND`. An empty
    /// `update_mask` is a no-op only after the target execution has resolved, so an empty
    /// store still returns `NOT_FOUND` (`updateworkflowoptions/api.go @ v1.31.0`).
    #[tokio::test]
    async fn update_workflow_execution_options_maps_expected_errors() {
        use tokeira_proto::public::temporal::api::{
            common::v1 as common, workflow::v1 as workflow,
        };
        let grpc = worker_deployment_test_service();
        fn request(run_id: &str) -> workflowservice::UpdateWorkflowExecutionOptionsRequest {
            workflowservice::UpdateWorkflowExecutionOptionsRequest {
                namespace: "default".to_string(),
                workflow_execution: Some(common::WorkflowExecution {
                    workflow_id: "wf".to_string(),
                    run_id: run_id.to_string(),
                }),
                workflow_execution_options: Some(workflow::WorkflowExecutionOptions::default()),
                update_mask: Some(prost_types::FieldMask {
                    paths: vec!["versioning_override".to_string()],
                }),
                identity: String::new(),
            }
        }

        // Missing execution (empty store) → NOT_FOUND.
        let not_found = grpc
            .update_workflow_execution_options(Request::new(request("")))
            .await
            .unwrap_err();
        assert_eq!(not_found.code(), tonic::Code::NotFound);

        // Malformed run id → INVALID_ARGUMENT (validated before lookup).
        let bad_run = grpc
            .update_workflow_execution_options(Request::new(request("not-a-uuid")))
            .await
            .unwrap_err();
        assert_eq!(bad_run.code(), tonic::Code::InvalidArgument);

        // Empty mask still resolves the execution before returning the unchanged options.
        let empty_mask = grpc
            .update_workflow_execution_options(Request::new(
                workflowservice::UpdateWorkflowExecutionOptionsRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: String::new(),
                    }),
                    workflow_execution_options: None,
                    update_mask: None,
                    identity: String::new(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(empty_mask.code(), tonic::Code::NotFound);
    }

    #[test]
    fn standalone_activity_id_validation_matches_v131_messages() {
        // Mirrors `chasm/lib/activity/validator.go @ v1.31.0` and the conformance
        // `TestStandaloneActivityTestSuite/TestDelete/RequestValidations` expectations.
        let empty = validate_sa_activity_id("", 1000).unwrap_err();
        assert_eq!(empty.code(), tonic::Code::InvalidArgument);
        assert_eq!(empty.message(), "activity ID is required");

        let long = "x".repeat(1001);
        let too_long = validate_sa_activity_id(&long, 1000).unwrap_err();
        assert_eq!(too_long.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            too_long.message(),
            "activity ID exceeds length limit. Length=1001 Limit=1000"
        );
        assert!(validate_sa_activity_id("act-1", 1000).is_ok());

        // Run id: empty is allowed; a non-empty run id must be a valid UUID.
        assert!(validate_sa_run_id("").is_ok());
        assert!(validate_sa_run_id(&Uuid::new_v4().to_string()).is_ok());
        let bad_run = validate_sa_run_id("invalid-run-id").unwrap_err();
        assert_eq!(bad_run.code(), tonic::Code::InvalidArgument);
        assert_eq!(bad_run.message(), "invalid run id: must be a valid UUID");

        // The enable gate: validation is skipped when disabled so a present-but-off
        // bridge still answers UNIMPLEMENTED (baseline) rather than InvalidArgument.
        assert!(validate_sa_ids(false, 1000, "", "invalid-run-id").is_ok());
        assert!(validate_sa_ids(true, 1000, "act-1", "").is_ok());
    }

    #[tokio::test]
    async fn get_search_attributes_exposes_system_visibility_keys() {
        let (grpc, _broker) = versioning_test_service();

        let response = grpc
            .get_search_attributes(Request::new(workflowservice::GetSearchAttributesRequest {}))
            .await
            .expect("get search attributes")
            .into_inner();

        assert_eq!(
            response.keys.get("ExecutionStatus").copied(),
            Some(IndexedValueType::Keyword as i32)
        );
        assert_eq!(
            response.keys.get("ParentWorkflowId").copied(),
            Some(IndexedValueType::Keyword as i32)
        );
        assert_eq!(
            response.keys.get("RootRunId").copied(),
            Some(IndexedValueType::Keyword as i32)
        );
        assert_eq!(
            response.keys.get("ExecutionDuration").copied(),
            Some(IndexedValueType::Int as i32)
        );
        assert_eq!(
            response.keys.get("TemporalExternalPayloadCount").copied(),
            Some(IndexedValueType::Int as i32)
        );
        assert_eq!(
            response
                .keys
                .get("TemporalUsedWorkerDeploymentVersions")
                .copied(),
            Some(IndexedValueType::KeywordList as i32)
        );
    }

    macro_rules! assert_deferred_rpc {
        ($grpc:expr, $method:ident, $request:ident, $spec:literal) => {{
            let status = $grpc
                .$method(Request::new(workflowservice::$request::default()))
                .await
                .expect_err("deferred rpc should return Unimplemented");
            assert_eq!(status.code(), tonic::Code::Unimplemented);
            assert_eq!(
                status.message(),
                format!(
                    "{} is not implemented; tracked in spec {}",
                    stringify!($method),
                    $spec
                )
            );
        }};
    }

    #[tokio::test]
    async fn deferred_handler_blocks_return_tracked_unimplemented_messages() {
        let (grpc, _broker) = versioning_test_service();

        let status = grpc
            .trigger_workflow_rule(Request::new(
                workflowservice::TriggerWorkflowRuleRequest::default(),
            ))
            .await
            .expect_err("v1.31.0 leaves TriggerWorkflowRule unimplemented");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert_eq!(status.message(), "method TriggerWorkflowRule not supported");

        assert_deferred_rpc!(
            grpc,
            fetch_worker_config,
            FetchWorkerConfigRequest,
            "worker-config-management"
        );
        assert_deferred_rpc!(
            grpc,
            update_worker_config,
            UpdateWorkerConfigRequest,
            "worker-config-management"
        );
    }

    fn nexus_test_service(
        runtime: Arc<dyn WorkflowRuntimeApi>,
    ) -> (WorkflowServiceGrpc, NexusTaskBroker) {
        nexus_test_service_with_waiters(
            runtime,
            crate::nexus_http::NexusHttpWaiterRegistry::default(),
        )
    }

    fn nexus_test_service_with_waiters(
        runtime: Arc<dyn WorkflowRuntimeApi>,
        nexus_http_waiters: crate::nexus_http::NexusHttpWaiterRegistry,
    ) -> (WorkflowServiceGrpc, NexusTaskBroker) {
        let cache = Arc::new(StaticNamespaceCache);
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let broker = tokeira_runtime::InMemoryBroker::default();
        let nexus_broker = NexusTaskBroker::default();
        let service =
            WorkflowService::new_with_stores_and_buffered_queries_and_history_wait_registry(
                runtime,
                Arc::new(NoopResolver),
                Arc::new(EmptyVisibilityApi),
                Arc::new(tokeira_storage::InMemoryStore::default()),
                operator_api,
                cache.clone(),
                Arc::new(EdgeInterceptors::permissive(cache)),
                PollerRegistry::default(),
                crate::PendingQueryStore::default(),
                tokeira_runtime::BufferedQueryRegistry::default(),
                broker,
                nexus_broker.clone(),
                LongPollGate::new(LongPollConfig::default()),
                Arc::new(LocalOnlyRouter),
                HistoryWaitRegistry::default(),
                WorkerRegistry::default(),
                Arc::new(tokeira_runtime::InMemoryHeartbeatStore::default()),
                Arc::new(tokeira_runtime::ScheduleStore::default()),
                Arc::new(tokeira_runtime::InMemoryTaskQueueConfigStore::default()),
                Arc::new(tokeira_runtime::BatchOperationStore::default()),
            )
            .with_nexus_http_waiters(nexus_http_waiters);
        (WorkflowServiceGrpc::new(service), nexus_broker)
    }

    /// All five deprecated Worker Versioning RPCs answer exactly like a
    /// default-config v1.31.0 server: PERMISSION_DENIED with the fixed
    /// "disabled on this namespace" messages, emitted before any request-field
    /// validation — the requests below name a registered namespace but omit
    /// the required task queue on purpose; upstream's gate check precedes
    /// field validation inside the handler (workflow_handler.go:5330-5524,
    /// errors.go:131-132 @ v1.31.0). The trailer carries exactly one
    /// `PermissionDeniedFailure{reason:""}` detail inside a `google.rpc.Status`
    /// wrapper, matching `serviceerror.NewPermissionDenied(message, "")`, and
    /// `GetWorkerTaskReachability` surfaces the v0.2 message (the upstream
    /// v0.1-gate/v0.2-message quirk).
    #[tokio::test]
    async fn worker_versioning_v1_v2_rpcs_are_rejected_like_stock_default_config() {
        const V1_MESSAGE: &str =
            "Worker versioning v0.1 (Version Set-based, deprecated) is disabled on this namespace.";
        const V2_MESSAGE: &str =
            "Worker versioning v0.2 (Rules-based, deprecated) is disabled on this namespace.";

        // Local mirrors of `google.rpc.Status` / `google.protobuf.Any` to
        // decode the `grpc-status-details-bin` trailer bytes.
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct RpcStatusMirror {
            #[prost(int32, tag = "1")]
            code: i32,
            #[prost(string, tag = "2")]
            message: String,
            #[prost(message, repeated, tag = "3")]
            details: Vec<ProtoAnyMirror>,
        }
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct ProtoAnyMirror {
            #[prost(string, tag = "1")]
            type_url: String,
            #[prost(bytes = "vec", tag = "2")]
            value: Vec<u8>,
        }

        fn assert_versioning_disabled(status: &Status, expected_message: &str) {
            use prost::Message as _;
            assert_eq!(status.code(), tonic::Code::PermissionDenied);
            assert_eq!(status.message(), expected_message);
            let trailer = RpcStatusMirror::decode(status.details())
                .expect("grpc-status-details-bin must decode as google.rpc.Status");
            assert_eq!(trailer.code, tonic::Code::PermissionDenied as i32);
            assert_eq!(trailer.message, expected_message);
            assert_eq!(trailer.details.len(), 1, "exactly one status detail");
            assert_eq!(
                trailer.details[0].type_url,
                "type.googleapis.com/temporal.api.errordetails.v1.PermissionDeniedFailure"
            );
            assert!(
                trailer.details[0].value.is_empty(),
                "PermissionDeniedFailure.reason must be empty, as stock encodes it"
            );
        }

        let (grpc, _broker) = versioning_test_service();

        let update_v1 = grpc
            .update_worker_build_id_compatibility(Request::new(
                workflowservice::UpdateWorkerBuildIdCompatibilityRequest {
                    namespace: "default".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("v1 update is gated off");
        assert_versioning_disabled(&update_v1, V1_MESSAGE);

        let get_v1 = grpc
            .get_worker_build_id_compatibility(Request::new(
                workflowservice::GetWorkerBuildIdCompatibilityRequest {
                    namespace: "default".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("v1 get is gated off");
        assert_versioning_disabled(&get_v1, V1_MESSAGE);

        let update_v2 = grpc
            .update_worker_versioning_rules(Request::new(
                workflowservice::UpdateWorkerVersioningRulesRequest {
                    namespace: "default".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("v2 update is gated off");
        assert_versioning_disabled(&update_v2, V2_MESSAGE);

        let get_v2 = grpc
            .get_worker_versioning_rules(Request::new(
                workflowservice::GetWorkerVersioningRulesRequest {
                    namespace: "default".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("v2 get is gated off");
        assert_versioning_disabled(&get_v2, V2_MESSAGE);

        let reachability = grpc
            .get_worker_task_reachability(Request::new(
                workflowservice::GetWorkerTaskReachabilityRequest {
                    namespace: "default".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("reachability is gated off");
        assert_versioning_disabled(&reachability, V2_MESSAGE);
    }

    #[tokio::test]
    async fn deployment_handlers_return_unimplemented_messages() {
        let (grpc, _broker) = versioning_test_service();

        let describe = grpc
            .describe_deployment(Request::new(
                workflowservice::DescribeDeploymentRequest::default(),
            ))
            .await
            .expect_err("deployment describe should be unsupported");
        assert_eq!(describe.code(), tonic::Code::Unimplemented);
        assert_eq!(describe.message(), DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED);

        let list = grpc
            .list_deployments(Request::new(
                workflowservice::ListDeploymentsRequest::default(),
            ))
            .await
            .expect_err("deployment list should be unsupported");
        assert_eq!(list.code(), tonic::Code::Unimplemented);
        assert_eq!(list.message(), DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED);

        let reachability = grpc
            .get_deployment_reachability(Request::new(
                workflowservice::GetDeploymentReachabilityRequest::default(),
            ))
            .await
            .expect_err("deployment reachability should be unsupported");
        assert_eq!(reachability.code(), tonic::Code::Unimplemented);
        assert_eq!(reachability.message(), DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED);

        let current = grpc
            .get_current_deployment(Request::new(
                workflowservice::GetCurrentDeploymentRequest::default(),
            ))
            .await
            .expect_err("current deployment should be unsupported");
        assert_eq!(current.code(), tonic::Code::Unimplemented);
        assert_eq!(current.message(), DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED);

        let set = grpc
            .set_current_deployment(Request::new(
                workflowservice::SetCurrentDeploymentRequest::default(),
            ))
            .await
            .expect_err("set current deployment should be unsupported");
        assert_eq!(set.code(), tonic::Code::Unimplemented);
        assert_eq!(set.message(), DEPRECATED_DEPLOYMENTS_UNIMPLEMENTED);
    }

    #[tokio::test]
    async fn worker_deployment_handlers_validate_input_before_registry_access() {
        let grpc = worker_deployment_test_service();

        let create = grpc
            .create_worker_deployment(Request::new(
                workflowservice::CreateWorkerDeploymentRequest {
                    namespace: "default".to_string(),
                    deployment_name: String::new(),
                    identity: "operator".to_string(),
                    request_id: "request-1".to_string(),
                },
            ))
            .await
            .expect_err("empty deployment name should be invalid");
        assert_eq!(create.code(), tonic::Code::InvalidArgument);

        let create_version = grpc
            .create_worker_deployment_version(Request::new(
                workflowservice::CreateWorkerDeploymentVersionRequest {
                    namespace: "default".to_string(),
                    deployment_version: None,
                    compute_config: None,
                    identity: "operator".to_string(),
                    request_id: "request-1".to_string(),
                },
            ))
            .await
            .expect_err("missing deployment version should be invalid");
        assert_eq!(create_version.code(), tonic::Code::InvalidArgument);

        let ramping = grpc
            .set_worker_deployment_ramping_version(Request::new(
                workflowservice::SetWorkerDeploymentRampingVersionRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    build_id: "build-a".to_string(),
                    percentage: 101.0,
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("out-of-range percentage should be invalid");
        assert_eq!(ramping.code(), tonic::Code::InvalidArgument);

        let manager = grpc
            .set_worker_deployment_manager(Request::new(
                workflowservice::SetWorkerDeploymentManagerRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    identity: "operator".to_string(),
                    new_manager_identity: None,
                    conflict_token: Vec::new(),
                },
            ))
            .await
            .expect_err("unset manager identity oneof should be invalid");
        assert_eq!(manager.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn worker_deployment_handlers_are_no_longer_deferred() {
        let grpc = worker_deployment_test_service();
        let version = || WorkerDeploymentVersion {
            deployment_name: "deployment-a".to_string(),
            build_id: "build-a".to_string(),
        };

        assert_worker_deployment_registry_missing(
            grpc.describe_worker_deployment_version(Request::new(
                workflowservice::DescribeWorkerDeploymentVersionRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    report_task_queue_stats: false,
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.set_worker_deployment_current_version(Request::new(
                workflowservice::SetWorkerDeploymentCurrentVersionRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    build_id: "build-a".to_string(),
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.describe_worker_deployment(Request::new(
                workflowservice::DescribeWorkerDeploymentRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.delete_worker_deployment(Request::new(
                workflowservice::DeleteWorkerDeploymentRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    identity: "operator".to_string(),
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.delete_worker_deployment_version(Request::new(
                workflowservice::DeleteWorkerDeploymentVersionRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.set_worker_deployment_ramping_version(Request::new(
                workflowservice::SetWorkerDeploymentRampingVersionRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    build_id: "build-a".to_string(),
                    percentage: 10.0,
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.list_worker_deployments(Request::new(
                workflowservice::ListWorkerDeploymentsRequest {
                    namespace: "default".to_string(),
                    page_size: 10,
                    next_page_token: Vec::new(),
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.create_worker_deployment(Request::new(
                workflowservice::CreateWorkerDeploymentRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    identity: "operator".to_string(),
                    request_id: "request-1".to_string(),
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.create_worker_deployment_version(Request::new(
                workflowservice::CreateWorkerDeploymentVersionRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    compute_config: None,
                    identity: "operator".to_string(),
                    request_id: "request-1".to_string(),
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.update_worker_deployment_version_compute_config(Request::new(
                workflowservice::UpdateWorkerDeploymentVersionComputeConfigRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    identity: "operator".to_string(),
                    request_id: "request-1".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.validate_worker_deployment_version_compute_config(Request::new(
                workflowservice::ValidateWorkerDeploymentVersionComputeConfigRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.update_worker_deployment_version_metadata(Request::new(
                workflowservice::UpdateWorkerDeploymentVersionMetadataRequest {
                    namespace: "default".to_string(),
                    deployment_version: Some(version()),
                    identity: "operator".to_string(),
                    ..Default::default()
                },
            ))
            .await,
        );
        assert_worker_deployment_registry_missing(
            grpc.set_worker_deployment_manager(Request::new(
                workflowservice::SetWorkerDeploymentManagerRequest {
                    namespace: "default".to_string(),
                    deployment_name: "deployment-a".to_string(),
                    identity: "operator".to_string(),
                    new_manager_identity: Some(
                        workflowservice::set_worker_deployment_manager_request::NewManagerIdentity::ManagerIdentity(
                            "manager-a".to_string(),
                        ),
                    ),
                    conflict_token: Vec::new(),
                },
            ))
            .await,
        );
    }

    fn assert_worker_deployment_registry_missing<T>(result: Result<Response<T>, Status>) {
        let status = match result {
            Ok(_) => panic!("service should stop at missing registry"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("worker deployment registry"));
    }

    #[tokio::test]
    async fn shutdown_worker_is_idempotent_and_blocks_workflow_delivery() {
        let (grpc, broker) = worker_heartbeat_test_service().await;
        let namespace_id = namespace_id_for("default");
        let queue = QueueKey {
            namespace_id,
            task_queue: TaskQueueName("sticky-queue".to_string()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };

        for _ in 0..2 {
            grpc.shutdown_worker(Request::new(workflowservice::ShutdownWorkerRequest {
                namespace: "default".to_string(),
                sticky_task_queue: "sticky-queue".to_string(),
                identity: "worker-a".to_string(),
                reason: "test".to_string(),
                worker_heartbeat: None,
                worker_instance_key: String::new(),
                task_queue: "sticky-queue".to_string(),
                task_queue_types: Vec::new(),
            }))
            .await
            .expect("shutdown should be idempotent");
        }

        broker
            .publish_workflow_task(
                DispatchableWorkflowTask {
                    run_key: RunKey::new(),
                    queue: queue.clone(),
                    logical_seq: LogicalTaskSeq(1),
                    sticky_preferred: None,
                    sticky_expires_at: None,
                },
                None,
            )
            .await;

        let denied = broker
            .poll_workflow_task(
                &queue,
                &WorkerIdentity("worker-a".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();
        let allowed = broker
            .poll_workflow_task(
                &queue,
                &WorkerIdentity("worker-b".to_string()),
                std::time::Duration::from_millis(5),
            )
            .await
            .unwrap();

        assert!(denied.is_none());
        assert!(allowed.is_some());
    }

    #[tokio::test]
    async fn record_worker_heartbeat_stores_lossless_heartbeat() {
        let (grpc, _broker) = worker_heartbeat_test_service().await;
        let store = grpc.inner.heartbeat_store();

        grpc.record_worker_heartbeat(Request::new(
            workflowservice::RecordWorkerHeartbeatRequest {
                namespace: "default".to_string(),
                identity: "client".to_string(),
                worker_heartbeat: vec![test_worker_heartbeat("worker-a")],
                resource_id: String::new(),
            },
        ))
        .await
        .expect("heartbeat should be accepted");

        let stored = store
            .get_worker(
                &namespace_id_for("default"),
                &WorkerInstanceKey("worker-a".to_string()),
            )
            .expect("store read should succeed")
            .expect("heartbeat should be stored");
        assert_eq!(stored.build_id.as_deref(), Some("build-a"));
        assert_eq!(stored.deployment_name.as_deref(), Some("deployment-a"));
        assert_eq!(stored.sdk_name, None);
        assert_eq!(stored.sdk_version.as_deref(), Some("rust-0.4"));
        assert_eq!(
            worker_heartbeat::worker_heartbeat_to_proto(&stored)
                .expect("encoded heartbeat should decode"),
            test_worker_heartbeat("worker-a")
        );
    }

    #[tokio::test]
    async fn worker_inventory_round_trips_complete_heartbeats() {
        // Feature: worker-heartbeat-observability, Property 8: lossless
        // observation-backed worker inventory.
        let (grpc, _broker) = worker_heartbeat_test_service().await;
        let mut heartbeat = test_worker_heartbeat("worker-a");
        heartbeat.total_sticky_cache_hit = 41;
        heartbeat.total_sticky_cache_miss = 7;
        heartbeat.current_sticky_cache_size = 3;
        heartbeat.host_info = Some(worker_v1::WorkerHostInfo {
            host_name: "host-a".to_owned(),
            worker_grouping_key: "group-a".to_owned(),
            process_id: "123".to_owned(),
            current_host_cpu_usage: 0.25,
            current_host_mem_usage: 0.5,
        });
        heartbeat.plugins = vec![worker_v1::PluginInfo {
            name: "plugin-a".to_owned(),
            version: "1.2.3".to_owned(),
        }];

        grpc.record_worker_heartbeat(Request::new(
            workflowservice::RecordWorkerHeartbeatRequest {
                namespace: "default".to_owned(),
                identity: "client".to_owned(),
                worker_heartbeat: vec![heartbeat.clone()],
                resource_id: String::new(),
            },
        ))
        .await
        .expect("heartbeat should be admitted");

        let described = grpc
            .describe_worker(Request::new(workflowservice::DescribeWorkerRequest {
                namespace: "default".to_owned(),
                worker_instance_key: "worker-a".to_owned(),
            }))
            .await
            .expect("existing worker should be described")
            .into_inner();
        assert_eq!(
            described.worker_info.and_then(|info| info.worker_heartbeat),
            Some(heartbeat.clone())
        );

        let listed = grpc
            .list_workers(Request::new(workflowservice::ListWorkersRequest {
                namespace: "default".to_owned(),
                query: "WorkerInstanceKey='worker-a'".to_owned(),
                page_size: 1,
                next_page_token: Vec::new(),
            }))
            .await
            .expect("worker query should succeed")
            .into_inner();
        assert_eq!(listed.workers_info.len(), 1);
        assert_eq!(
            listed.workers_info[0].worker_heartbeat,
            Some(heartbeat.clone())
        );
        assert_eq!(listed.workers.len(), 1);
        assert_eq!(listed.workers[0].worker_instance_key, "worker-a");
        assert_eq!(listed.workers[0].host_name, "host-a");
        assert_eq!(listed.workers[0].plugins, heartbeat.plugins);
        assert!(listed.next_page_token.is_empty());

        let missing = grpc
            .describe_worker(Request::new(workflowservice::DescribeWorkerRequest {
                namespace: "default".to_owned(),
                worker_instance_key: "missing".to_owned(),
            }))
            .await
            .expect_err("missing worker should return NotFound");
        assert_eq!(missing.code(), tonic::Code::NotFound);

        let unknown_namespace = grpc
            .describe_worker(Request::new(workflowservice::DescribeWorkerRequest {
                namespace: "unknown".to_owned(),
                worker_instance_key: "worker-a".to_owned(),
            }))
            .await
            .expect_err("unknown namespace should return NamespaceNotFound");
        assert_eq!(unknown_namespace.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn shutdown_worker_removes_final_shutdown_heartbeat() {
        let (grpc, _broker) = worker_heartbeat_test_service().await;
        let store = grpc.inner.heartbeat_store();

        grpc.record_worker_heartbeat(Request::new(
            workflowservice::RecordWorkerHeartbeatRequest {
                namespace: "default".to_string(),
                identity: "client".to_string(),
                worker_heartbeat: vec![test_worker_heartbeat("worker-a")],
                resource_id: String::new(),
            },
        ))
        .await
        .expect("running heartbeat should be accepted");

        let mut final_heartbeat = test_worker_heartbeat("worker-a");
        final_heartbeat.status = 3;

        grpc.shutdown_worker(Request::new(workflowservice::ShutdownWorkerRequest {
            namespace: "default".to_string(),
            sticky_task_queue: "sticky".to_string(),
            identity: "worker-a".to_string(),
            reason: "test".to_string(),
            worker_heartbeat: Some(final_heartbeat),
            worker_instance_key: "worker-a".to_string(),
            task_queue: "queue".to_string(),
            task_queue_types: Vec::new(),
        }))
        .await
        .expect("shutdown should succeed");

        let stored = store
            .get_worker(
                &namespace_id_for("default"),
                &WorkerInstanceKey("worker-a".to_string()),
            )
            .expect("store read should succeed");
        assert!(stored.is_none(), "shutdown is removal, not a tombstone");
    }

    #[tokio::test]
    async fn poll_none_returns_default_proto_response() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let response = grpc
            .poll_workflow_task_queue(Request::new(
                workflowservice::PollWorkflowTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(
                        tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                            name: "queue".to_string(),
                            ..Default::default()
                        },
                    ),
                    identity: "worker".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect("poll should succeed")
            .into_inner();

        assert_eq!(
            response,
            workflowservice::PollWorkflowTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn activity_poll_none_returns_default_response() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let response = grpc
            .poll_activity_task_queue(Request::new(
                workflowservice::PollActivityTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(
                        tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                            name: "queue".to_string(),
                            ..Default::default()
                        },
                    ),
                    identity: "worker".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect("poll should succeed")
            .into_inner();

        assert_eq!(
            response,
            workflowservice::PollActivityTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn activity_poll_shares_long_poll_gate() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        // Create a gate with max_concurrent = 1 and
        // short admission timeout
        let gate = LongPollGate::new(LongPollConfig {
            max_concurrent: 1,
            acquire_timeout: std::time::Duration::from_millis(10),
        });

        // Directly acquire the single permit to
        // simulate a workflow poll holding it
        let permit = gate.acquire().await.unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            gate,
            Arc::new(LocalOnlyRouter),
        );

        // Activity poll should be rejected because
        // the gate is exhausted
        let headers = http::HeaderMap::new();
        let req = crate::translate::PollActivityTaskQueueRequest {
            namespace: "default".to_string(),
            task_queue: "queue".to_string(),
            worker_identity: "w2".to_string(),
            worker_instance_key: String::new(),
            deployment: None,
            build_id: None,
            worker_rate_limit: None,
            timeout: std::time::Duration::from_millis(50),
        };
        let result = service.poll_activity_task_queue(&headers, req).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let status: tonic::Status = err.into();
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);

        // Release the permit
        drop(permit);
    }

    #[tokio::test]
    async fn describe_task_queue_lists_active_pollers() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let ready = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let service = WorkflowService::new(
            Arc::new(BlockingPollRuntime {
                ready: ready.clone(),
                release: release.clone(),
            }),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let poll = tokio::spawn({
            let grpc = grpc.clone();
            async move {
                grpc.poll_workflow_task_queue(Request::new(
                    workflowservice::PollWorkflowTaskQueueRequest {
                        namespace: "default".to_string(),
                        task_queue: Some(
                            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                                name: "queue".to_string(),
                                ..Default::default()
                            },
                        ),
                        identity: "worker-1".to_string(),
                        ..Default::default()
                    },
                ))
                .await
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), ready.notified())
            .await
            .expect("poll should register before describe");

        let describe = grpc
            .describe_task_queue(Request::new(workflowservice::DescribeTaskQueueRequest {
                namespace: "default".to_string(),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "queue".to_string(),
                        ..Default::default()
                    },
                ),
                task_queue_type: tokeira_proto::enums::TaskQueueType::Workflow as i32,
                include_task_queue_status: true,
                ..Default::default()
            }))
            .await
            .expect("describe should succeed")
            .into_inner();

        assert_eq!(describe.pollers.len(), 1);
        assert_eq!(describe.pollers[0].identity, "worker-1");
        assert!(describe.pollers[0].last_access_time.is_some());
        assert_eq!(
            describe
                .task_queue_status
                .as_ref()
                .map(|status| status.backlog_count_hint),
            Some(0)
        );

        release.notify_waiters();
        let response = poll.await.unwrap().unwrap().into_inner();
        assert_eq!(
            response,
            workflowservice::PollWorkflowTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn delete_workflow_execution_missing_returns_not_found() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let error = grpc
            .delete_workflow_execution(Request::new(
                workflowservice::DeleteWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "missing".to_string(),
                        run_id: String::new(),
                    }),
                },
            ))
            .await
            .expect_err("missing workflow should fail");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn rejected_delete_requests_leave_authoritative_run_unchanged() {
        let (grpc, repo, run_key, _run_id) = history_test_service().await;
        let cases = [
            (
                workflowservice::DeleteWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: None,
                },
                "Execution is not set on request.",
            ),
            (
                workflowservice::DeleteWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: String::new(),
                        run_id: String::new(),
                    }),
                },
                "WorkflowId is not set on request.",
            ),
            (
                workflowservice::DeleteWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: "not-a-run-id".to_string(),
                    }),
                },
                "Invalid RunId.",
            ),
        ];

        for (request, expected_message) in cases {
            let error = grpc
                .delete_workflow_execution(Request::new(request))
                .await
                .expect_err("invalid delete request should fail before mutation");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(error.message(), expected_message);
            assert!(matches!(
                repo.load_run(run_key).await.unwrap(),
                LoadedRun::Existing(_)
            ));
        }
    }

    #[tokio::test]
    async fn reset_workflow_execution_invalid_event_returns_invalid_argument() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;

        let error = grpc
            .reset_workflow_execution(Request::new(
                workflowservice::ResetWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                    }),
                    reason: "operator reset".to_string(),
                    workflow_task_finish_event_id: 2,
                    request_id: "reset-1".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("invalid reset target should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn reset_workflow_execution_missing_returns_not_found() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let error = grpc
            .reset_workflow_execution(Request::new(
                workflowservice::ResetWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "missing".to_string(),
                        run_id: String::new(),
                    }),
                    reason: "operator reset".to_string(),
                    workflow_task_finish_event_id: 1,
                    request_id: "reset-missing".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("missing workflow should fail");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn respond_query_task_completed_without_waiter_returns_success() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let namespace_id = crate::translate::to_internal::namespace_id_for("default");
        let response = grpc
            .respond_query_task_completed(Request::new(
                workflowservice::RespondQueryTaskCompletedRequest {
                    task_token: format!("query-task:{}:queue:missing", namespace_id.0).into_bytes(),
                    completed_type: tokeira_proto::enums::QueryResultType::Answered as i32,
                    query_result: Some(tokeira_proto::common::Payloads::default()),
                    error_message: String::new(),
                    namespace: "default".to_string(),
                    failure: None,
                    cause: 0,
                    poller_group_id: String::new(),
                },
            ))
            .await
            .expect("legacy query completion should succeed")
            .into_inner();

        assert_eq!(
            response,
            workflowservice::RespondQueryTaskCompletedResponse {}
        );
    }

    #[tokio::test]
    async fn respond_query_task_completed_routes_legacy_result() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        let namespace_id = crate::translate::to_internal::namespace_id_for("default");
        let task_token = format!("query-task:{}:queue:legacy-query", namespace_id.0).into_bytes();
        service
            .insert_legacy_query_waiter(task_token.clone(), tx)
            .await;
        let grpc = WorkflowServiceGrpc::new(service);

        grpc.respond_query_task_completed(Request::new(
            workflowservice::RespondQueryTaskCompletedRequest {
                task_token,
                completed_type: tokeira_proto::enums::QueryResultType::Answered as i32,
                query_result: Some(tokeira_proto::common::Payloads {
                    payloads: vec![tokeira_proto::common::Payload::default()],
                }),
                error_message: String::new(),
                namespace: "default".to_string(),
                failure: None,
                cause: 0,
                poller_group_id: String::new(),
            },
        ))
        .await
        .expect("legacy query completion should route");

        let result = rx.await.expect("legacy waiter should receive result");
        match result {
            tokeira_runtime::QueryResult::Completed { result } => {
                assert_eq!(result.0.len(), 1);
            }
            other => panic!("unexpected query result: {other:?}"),
        }
    }

    async fn history_test_service() -> (
        WorkflowServiceGrpc,
        Arc<HistoryNotifyingRepository<tokeira_storage::InMemoryStore>>,
        RunKey,
        RunId,
    ) {
        history_test_service_with_principal(None).await
    }

    async fn history_test_service_with_principal(
        principal: Option<EventPrincipal>,
    ) -> (
        WorkflowServiceGrpc,
        Arc<HistoryNotifyingRepository<tokeira_storage::InMemoryStore>>,
        RunKey,
        RunId,
    ) {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let waits = HistoryWaitRegistry::default();
        let store = Arc::new(tokeira_storage::InMemoryStore::default());
        let repo = Arc::new(HistoryNotifyingRepository::new(store, waits.clone()));

        let run_key = RunKey::new();
        let run_id = RunId(Uuid::new_v4());
        seed_started_run(repo.as_ref(), run_key, run_id, principal).await;

        let service = WorkflowService::new_with_history_wait_registry(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            repo.clone(),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
            waits,
        );
        (WorkflowServiceGrpc::new(service), repo, run_key, run_id)
    }

    async fn seed_started_run(
        repo: &HistoryNotifyingRepository<tokeira_storage::InMemoryStore>,
        run_key: RunKey,
        run_id: RunId,
        principal: Option<EventPrincipal>,
    ) {
        let start = StartRequest {
            initiator: None,
            run_key,
            namespace_id: namespace_id_for("default"),
            workflow_id: WorkflowId("wf".to_string()),
            run_id,
            workflow_type: WorkflowType("wf-type".to_string()),
            task_queue: TaskQueueName("q".to_string()),
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
            workflow_task_timeout: time::Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
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
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("seed-start".to_string()),
                caller_identity: None,
                principal,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
            cron_schedule: None,
            reserved_poller_identity: None,
            eager_execution_accepted: false,
            inherited_versioning_info: None,
        };

        let transition = BasicKernel
            .apply(LoadedRun::Absent, Command::Start(start))
            .expect("start transition");
        let result = repo
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .expect("commit start");
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    async fn append_signal_event(
        repo: Arc<HistoryNotifyingRepository<tokeira_storage::InMemoryStore>>,
        run_key: RunKey,
    ) {
        let loaded = repo.load_run(run_key).await.expect("load run");
        let transition = BasicKernel
            .apply(
                loaded,
                Command::Signal(SignalRequest {
                    signal_name: "sig".to_string(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: RequestContext {
                        request_id: RequestId("sig-1".to_string()),
                        caller_identity: None,
                        principal: None,
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .expect("signal transition");
        let result = repo
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .expect("commit signal");
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    fn history_request(
        run_id: RunId,
        wait_new_event: bool,
        next_page_token: Vec<u8>,
    ) -> workflowservice::GetWorkflowExecutionHistoryRequest {
        workflowservice::GetWorkflowExecutionHistoryRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: run_id.0.to_string(),
                ..Default::default()
            }),
            wait_new_event,
            history_event_filter_type: 1,
            next_page_token,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn history_immediate_return_when_wait_disabled() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;

        let response = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("history call should succeed")
            .into_inner();

        let history = response.history.expect("history");
        assert_eq!(history.events.len(), 2);
        // v1.31.0: a non-long-poll read that returns all currently-available events emits an
        // empty next_page_token ("you have everything") so the client's paginate-until-empty
        // loop terminates. (getworkflowexecutionhistory/api.go:488)
        assert!(
            response.next_page_token.is_empty(),
            "snapshot read of a caught-up workflow must return an empty page token"
        );
    }

    #[tokio::test]
    async fn history_rpc_reads_back_half_empty_durable_principal() {
        // Feature: authorization-foundation, Property 5: the public history
        // RPC returns the batch-aligned sidecar verbatim; an authenticated
        // empty subject remains present rather than collapsing to anonymous.
        let expected = tokeira_proto::common::Principal {
            r#type: "jwt".to_owned(),
            name: String::new(),
        };
        let (grpc, _repo, _run_key, run_id) =
            history_test_service_with_principal(Some(EventPrincipal {
                principal_type: expected.r#type.clone(),
                name: expected.name.clone(),
            }))
            .await;

        let response = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("history call should succeed")
            .into_inner();
        let events = response.history.expect("history").events;

        assert!(!events.is_empty());
        let committed_principals = events
            .iter()
            .filter(|event| event.event_id <= 2)
            .map(|event| event.principal.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            committed_principals,
            vec![Some(expected.clone()), Some(expected)]
        );
    }

    #[tokio::test]
    async fn history_long_poll_wakes_when_event_arrives() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;
        // A long-poll (wait=true) read retains a continuation token even when caught up, so the
        // follow client can resume; a non-long-poll baseline would now (correctly) return an empty
        // token. v1.31.0: getworkflowexecutionhistory/api.go:488.
        let baseline = grpc
            .get_workflow_execution_history(Request::new(history_request(run_id, true, Vec::new())))
            .await
            .expect("baseline history call should succeed")
            .into_inner();
        assert_eq!(baseline.next_page_token, 2i64.to_be_bytes());

        let task = tokio::spawn({
            let grpc = grpc.clone();
            let next_page_token = baseline.next_page_token.clone();
            async move {
                grpc.get_workflow_execution_history(Request::new(history_request(
                    run_id,
                    true,
                    next_page_token,
                )))
                .await
                .expect("history call should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        append_signal_event(repo, run_key).await;

        let response = task.await.expect("join");
        let history = response.history.expect("history");
        assert_eq!(history.events.len(), 1);
        assert_eq!(response.next_page_token, 3i64.to_be_bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn history_long_poll_times_out_without_new_event() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;
        // Long-poll baseline (wait=true) yields a retained continuation token; the follow-up call
        // then blocks for new events and times out. v1.31.0: api.go:488.
        let baseline = grpc
            .get_workflow_execution_history(Request::new(history_request(run_id, true, Vec::new())))
            .await
            .expect("baseline history call should succeed")
            .into_inner();

        let task = tokio::spawn({
            let grpc = grpc.clone();
            let next_page_token = baseline.next_page_token.clone();
            async move {
                grpc.get_workflow_execution_history(Request::new(history_request(
                    run_id,
                    true,
                    next_page_token,
                )))
                .await
                .expect("history call should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;

        let response = task.await.expect("join");
        let history = response.history.expect("history");
        assert!(history.events.is_empty());
        assert_eq!(response.next_page_token, baseline.next_page_token);
    }

    #[tokio::test]
    async fn history_next_page_token_tracks_position_across_pages() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;
        append_signal_event(repo, run_key).await; // history now has 3 events (1, 2, 3)

        // Genuine pagination: MaximumPageSize=2 over a 3-event history. A full page means more
        // events remain (non-empty token); a partial final page means the client has everything
        // (empty token), matching v1.31.0's continuation contract.
        let paged = |token: Vec<u8>| {
            let mut req = history_request(run_id, false, token);
            req.maximum_page_size = 2;
            req
        };

        let first = grpc
            .get_workflow_execution_history(Request::new(paged(Vec::new())))
            .await
            .expect("first history call should succeed")
            .into_inner();
        let first_history = first.history.expect("history");
        assert_eq!(first_history.events.len(), 2);
        assert_eq!(first.next_page_token, 2i64.to_be_bytes());

        let second = grpc
            .get_workflow_execution_history(Request::new(paged(first.next_page_token)))
            .await
            .expect("second history call should succeed")
            .into_inner();
        let second_history = second.history.expect("history");
        assert_eq!(second_history.events.len(), 1);
        assert!(
            second.next_page_token.is_empty(),
            "final page must return an empty token so the client stops paginating"
        );
    }

    #[tokio::test]
    async fn reverse_history_paginates_in_descending_event_order() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;
        append_signal_event(repo, run_key).await;

        let first = grpc
            .get_workflow_execution_history_reverse(Request::new(
                workflowservice::GetWorkflowExecutionHistoryReverseRequest {
                    namespace: "default".to_string(),
                    execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                        ..Default::default()
                    }),
                    maximum_page_size: 1,
                    next_page_token: Vec::new(),
                },
            ))
            .await
            .expect("reverse history should succeed")
            .into_inner();
        let first_history = first.history.expect("history");
        assert_eq!(first_history.events.len(), 1);
        let newest_event_id = first_history.events[0].event_id;
        assert_eq!(first.next_page_token, newest_event_id.to_be_bytes());

        let second = grpc
            .get_workflow_execution_history_reverse(Request::new(
                workflowservice::GetWorkflowExecutionHistoryReverseRequest {
                    namespace: "default".to_string(),
                    execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                        ..Default::default()
                    }),
                    maximum_page_size: 10,
                    next_page_token: first.next_page_token,
                },
            ))
            .await
            .expect("reverse history second page should succeed")
            .into_inner();
        let second_history = second.history.expect("history");
        assert!(!second_history.events.is_empty());
        assert!(
            second_history
                .events
                .iter()
                .all(|event| event.event_id < newest_event_id)
        );
        for pair in second_history.events.windows(2) {
            assert!(pair[0].event_id > pair[1].event_id);
        }
    }

    #[tokio::test]
    async fn poll_nexus_task_queue_rejects_empty_namespace() {
        let (grpc, _broker) = nexus_test_service(Arc::new(PollNoneRuntime));

        let error = grpc
            .poll_nexus_task_queue(Request::new(workflowservice::PollNexusTaskQueueRequest {
                namespace: String::new(),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "nexus-q".to_string(),
                        ..Default::default()
                    },
                ),
                identity: "worker".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("empty namespace should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn poll_nexus_task_queue_rejects_empty_task_queue() {
        let (grpc, _broker) = nexus_test_service(Arc::new(PollNoneRuntime));

        let error = grpc
            .poll_nexus_task_queue(Request::new(workflowservice::PollNexusTaskQueueRequest {
                namespace: "default".to_string(),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: String::new(),
                        ..Default::default()
                    },
                ),
                identity: "worker".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("empty task queue should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test(start_paused = true)]
    async fn poll_nexus_task_queue_timeout_returns_default_response() {
        let (grpc, _broker) = nexus_test_service(Arc::new(PollNoneRuntime));

        let task = tokio::spawn({
            let grpc = grpc.clone();
            async move {
                grpc.poll_nexus_task_queue(Request::new(
                    workflowservice::PollNexusTaskQueueRequest {
                        namespace: "default".to_string(),
                        task_queue: Some(
                            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                                name: "nexus-q".to_string(),
                                ..Default::default()
                            },
                        ),
                        identity: "worker".to_string(),
                        ..Default::default()
                    },
                ))
                .await
                .expect("poll should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;

        let response = task.await.expect("join");
        assert!(response.task_token.is_empty());
        assert!(response.request.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn poll_nexus_task_queue_ignores_piggyback_store_failure() {
        // Feature: worker-heartbeat-observability, Property 13: observation
        // failure cannot fail the Nexus delivery long poll.
        let (mut grpc, _broker) = nexus_test_service(Arc::new(PollNoneRuntime));
        grpc.inner = grpc
            .inner
            .clone()
            .with_heartbeat_store(Arc::new(FailingHeartbeatStore));

        let task = tokio::spawn(async move {
            grpc.poll_nexus_task_queue(Request::new(workflowservice::PollNexusTaskQueueRequest {
                namespace: "default".to_owned(),
                task_queue: Some(
                    tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "nexus-q".to_owned(),
                        ..Default::default()
                    },
                ),
                identity: "worker".to_owned(),
                worker_heartbeat: vec![test_worker_heartbeat("nexus-worker")],
                ..Default::default()
            }))
            .await
            .expect("heartbeat-store failure must not fail the poll")
            .into_inner()
        });

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        let response = task.await.expect("poll task should join");
        assert!(response.task_token.is_empty());
        assert!(response.request.is_none());
    }

    #[tokio::test]
    async fn poll_nexus_task_queue_wakes_on_publish() {
        let (grpc, broker) = nexus_test_service(Arc::new(PollNoneRuntime));
        let mut heartbeat = test_worker_heartbeat("nexus-worker");
        heartbeat.total_sticky_cache_hit = 55;

        let task = tokio::spawn({
            let grpc = grpc.clone();
            let heartbeat = heartbeat.clone();
            async move {
                grpc.poll_nexus_task_queue(Request::new(
                    workflowservice::PollNexusTaskQueueRequest {
                        namespace: "default".to_string(),
                        task_queue: Some(
                            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                                name: "nexus-q".to_string(),
                                ..Default::default()
                            },
                        ),
                        identity: "worker".to_string(),
                        worker_heartbeat: vec![heartbeat],
                        ..Default::default()
                    },
                ))
                .await
                .expect("poll should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        broker
            .publish(
                namespace_id_for("default"),
                TaskQueueName("nexus-q".to_string()),
                NexusTask {
                    token: NexusTaskToken {
                        namespace_id: namespace_id_for("default").0.to_string(),
                        task_queue: "nexus-q".to_string(),
                        task_id: Uuid::from_u128(7).to_string(),
                    },
                    request: NexusTaskRequest::StartOperation {
                        service: "svc".to_string(),
                        operation: "op".to_string(),
                        request_id: "req-1".to_string(),
                        payload: None,
                        scheduled_time: Some(OffsetDateTime::UNIX_EPOCH),
                        callback_url: None,
                        callback_token: None,
                    },
                },
            )
            .await;

        let response = task.await.expect("join");
        assert!(!response.task_token.is_empty());
        let request = response.request.expect("request");
        match request.variant.expect("variant") {
            nexus_v1::request::Variant::StartOperation(start) => {
                assert_eq!(start.service, "svc");
                assert_eq!(start.operation, "op");
                assert_eq!(start.request_id, "req-1");
            }
            other => panic!("unexpected nexus request variant: {other:?}"),
        }
        let stored = grpc
            .inner
            .heartbeat_store()
            .get_worker(
                &namespace_id_for("default"),
                &WorkerInstanceKey("nexus-worker".to_owned()),
            )
            .expect("heartbeat store read should succeed")
            .expect("Nexus poll should record its piggyback heartbeat");
        assert_eq!(
            worker_heartbeat::worker_heartbeat_to_proto(&stored)
                .expect("stored Nexus heartbeat should decode")
                .total_sticky_cache_hit,
            55
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_rejects_empty_task_token() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        let error = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "default".to_string(),
                    task_token: Vec::new(),
                    response: Some(nexus_v1::Response {
                        variant: Some(nexus_v1::response::Variant::CancelOperation(
                            nexus_v1::CancelOperationResponse {},
                        )),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("empty token should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), "Task token not set on request.");
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_does_not_invent_missing_response_validation() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        let token = NexusTaskToken {
            namespace_id: namespace_id_for("default").0.to_string(),
            task_queue: "nexus-q".to_string(),
            task_id: Uuid::new_v4().to_string(),
        }
        .encode()
        .expect("token");
        let error = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "default".to_string(),
                    task_token: token,
                    response: None,
                    ..Default::default()
                },
            ))
            .await
            .expect_err("unknown delivery should fail after response admission");
        assert_eq!(error.code(), tonic::Code::NotFound);
        assert_eq!(error.message(), "Nexus task not found or already expired");
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_rejects_malformed_task_token() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        let error = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "default".to_string(),
                    task_token: b"not-json".to_vec(),
                    response: Some(nexus_v1::Response {
                        variant: Some(nexus_v1::response::Variant::CancelOperation(
                            nexus_v1::CancelOperationResponse {},
                        )),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("malformed token should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), "Error deserializing task token.");
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_rejects_invalid_token_fields_exactly() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        for (token, expected) in [
            (
                NexusTaskToken {
                    namespace_id: namespace_id_for("default").0.to_string(),
                    task_queue: String::new(),
                    task_id: Uuid::new_v4().to_string(),
                },
                "Invalid TaskToken.",
            ),
            (
                NexusTaskToken {
                    namespace_id: String::new(),
                    task_queue: "nexus-q".to_string(),
                    task_id: Uuid::new_v4().to_string(),
                },
                "Namespace not set on request.",
            ),
        ] {
            let error = grpc
                .respond_nexus_task_completed(Request::new(
                    workflowservice::RespondNexusTaskCompletedRequest {
                        namespace: "default".to_string(),
                        task_token: token.encode().expect("token"),
                        response: Some(nexus_v1::Response::default()),
                        ..Default::default()
                    },
                ))
                .await
                .expect_err("invalid token fields must fail");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(error.message(), expected);
        }
    }

    #[tokio::test]
    async fn respond_nexus_task_methods_reject_token_namespace_mismatch_before_delivery_lookup() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));
        let token = NexusTaskToken {
            namespace_id: namespace_id_for("default").0.to_string(),
            task_queue: "nexus-q".to_string(),
            task_id: Uuid::new_v4().to_string(),
        }
        .encode()
        .expect("token");

        let completed = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "external".to_string(),
                    task_token: token.clone(),
                    response: Some(nexus_v1::Response::default()),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("token namespace must win");
        assert_eq!(completed.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            completed.message(),
            "Operation requested with a token from a different namespace."
        );

        let failed = grpc
            .respond_nexus_task_failed(Request::new(
                workflowservice::RespondNexusTaskFailedRequest {
                    namespace: "external".to_string(),
                    task_token: token,
                    error: Some(nexus_v1::HandlerError::default()),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("token namespace must win");
        assert_eq!(failed.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            failed.message(),
            "Operation requested with a token from a different namespace."
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_validates_operation_token_before_task_token() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));
        let operation_token = "long".repeat(2_000);
        let error = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "default".to_string(),
                    task_token: b"not-protobuf".to_vec(),
                    response: Some(nexus_v1::Response {
                        variant: Some(nexus_v1::response::Variant::StartOperation(
                            nexus_v1::StartOperationResponse {
                                variant: Some(
                                    nexus_v1::start_operation_response::Variant::AsyncSuccess(
                                        nexus_v1::start_operation_response::Async {
                                            operation_token,
                                            ..Default::default()
                                        },
                                    ),
                                ),
                            },
                        )),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("oversized operation token must be checked first");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "operation token length exceeds allowed limit (8000/4096)"
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_methods_validate_failure_shapes_before_consumption() {
        let (grpc, broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));
        let token = publish_test_workflow_nexus_task(&broker, RunKey::new()).await;
        let decoded = NexusTaskToken::decode(&token).expect("token");

        let completed = grpc
            .respond_nexus_task_completed(Request::new(
                workflowservice::RespondNexusTaskCompletedRequest {
                    namespace: "default".to_string(),
                    task_token: token.clone(),
                    response: Some(nexus_v1::Response {
                        variant: Some(nexus_v1::response::Variant::StartOperation(
                            nexus_v1::StartOperationResponse {
                                variant: Some(
                                    nexus_v1::start_operation_response::Variant::OperationError(
                                        nexus_v1::UnsuccessfulOperationError {
                                            operation_state: "failed".to_string(),
                                            failure: Some(nexus_v1::Failure {
                                                details: b"not valid JSON".to_vec(),
                                                ..Default::default()
                                            }),
                                        },
                                    ),
                                ),
                            },
                        )),
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("invalid operation failure details must fail");
        assert_eq!(completed.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            completed.message(),
            "failure details must be JSON serializable"
        );
        assert!(
            broker.consume(&decoded.task_id).await.is_some(),
            "frontend validation must leave the private delivery route outstanding"
        );

        let failed_token = publish_test_workflow_nexus_task(&broker, RunKey::new()).await;
        let failed = grpc
            .respond_nexus_task_failed(Request::new(
                workflowservice::RespondNexusTaskFailedRequest {
                    namespace: "default".to_string(),
                    task_token: failed_token,
                    error: Some(nexus_v1::HandlerError {
                        failure: Some(nexus_v1::Failure {
                            details: b"not valid JSON".to_vec(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("invalid handler failure details must fail");
        assert_eq!(failed.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            failed.message(),
            "failure details must be JSON serializable"
        );

        let modern_token = publish_test_workflow_nexus_task(&broker, RunKey::new()).await;
        let modern = grpc
            .respond_nexus_task_failed(Request::new(
                workflowservice::RespondNexusTaskFailedRequest {
                    namespace: "default".to_string(),
                    task_token: modern_token,
                    failure: Some(tokeira_proto::failure::Failure::default()),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("modern failures require NexusHandlerFailureInfo");
        assert_eq!(modern.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            modern.message(),
            "request Failure must contain error or failure with NexusHandlerFailureInfo"
        );
    }

    // Feature: edge-nexus-task-transport, Property 5: any invalid JSON failure
    // is rejected before the worker's private delivery correlation is consumed.
    proptest! {
        #[test]
        fn property_nexus_validation_precedes_correlation_consumption(
            details in proptest::collection::vec(any::<u8>(), 1..64)
                .prop_filter("details must not be valid JSON", |value| {
                    serde_json::from_slice::<serde_json::Value>(value).is_err()
                }),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            runtime.block_on(async move {
                let (grpc, broker) =
                    nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));
                let token = publish_test_workflow_nexus_task(&broker, RunKey::new()).await;
                let decoded = NexusTaskToken::decode(&token).expect("token");
                let error = grpc
                    .respond_nexus_task_completed(Request::new(
                        workflowservice::RespondNexusTaskCompletedRequest {
                            namespace: "default".to_string(),
                            task_token: token,
                            response: Some(nexus_v1::Response {
                                variant: Some(nexus_v1::response::Variant::StartOperation(
                                    nexus_v1::StartOperationResponse {
                                        variant: Some(
                                            nexus_v1::start_operation_response::Variant::OperationError(
                                                nexus_v1::UnsuccessfulOperationError {
                                                    operation_state: "failed".to_string(),
                                                    failure: Some(nexus_v1::Failure {
                                                        details,
                                                        ..Default::default()
                                                    }),
                                                },
                                            ),
                                        ),
                                    },
                                )),
                            }),
                            ..Default::default()
                        },
                    ))
                    .await
                    .expect_err("invalid details must fail");
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
                assert_eq!(
                    error.message(),
                    "failure details must be JSON serializable"
                );
                assert!(broker.consume(&decoded.task_id).await.is_some());
            });
        }
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_routes_workflow_correlation_once() {
        let runtime = Arc::new(NexusRecordingRuntime::new(true));
        let (grpc, broker) = nexus_test_service(runtime.clone());
        let run_key = RunKey::new();
        let token = publish_test_workflow_nexus_task(&broker, run_key).await;
        let request = workflowservice::RespondNexusTaskCompletedRequest {
            namespace: "default".to_string(),
            task_token: token.clone(),
            response: Some(nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(nexus_v1::start_operation_response::Variant::SyncSuccess(
                            nexus_v1::start_operation_response::Sync::default(),
                        )),
                    },
                )),
            }),
            ..Default::default()
        };

        grpc.respond_nexus_task_completed(Request::new(request.clone()))
            .await
            .expect("known workflow correlation must route");
        let recorded = runtime.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, run_key);
        assert_eq!(recorded[0].1, "op-1");
        assert_eq!(recorded[0].2, 1);
        assert!(matches!(recorded[0].3, NexusResolution::Completed { .. }));

        let repeated = grpc
            .respond_nexus_task_completed(Request::new(request))
            .await
            .expect_err("delivery correlation is single-use");
        assert_eq!(repeated.code(), tonic::Code::NotFound);
        assert_eq!(
            repeated.message(),
            "Nexus task not found or already expired"
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_routes_http_correlation_only_to_edge_waiter() {
        let waiters = crate::nexus_http::NexusHttpWaiterRegistry::default();
        let mut waiter = waiters.register();
        let runtime = Arc::new(NexusRecordingRuntime::new(true));
        let (grpc, broker) = nexus_test_service_with_waiters(runtime.clone(), waiters);
        let namespace_id = namespace_id_for("default");
        let task_queue = TaskQueueName("nexus-q".to_string());
        let _lease = broker
            .publish_http(
                namespace_id,
                task_queue.clone(),
                waiter.id().to_string(),
                NexusTaskRequest::Http(NexusHttpTaskRequest {
                    header: BTreeMap::new(),
                    scheduled_time: OffsetDateTime::UNIX_EPOCH,
                    temporal_failure_responses: false,
                    endpoint: String::new(),
                    dispatch_deadline: OffsetDateTime::UNIX_EPOCH,
                    variant: NexusHttpTaskRequestVariant::CancelOperation {
                        service: "svc".to_string(),
                        operation: "op".to_string(),
                        operation_id: "worker-token".to_string(),
                        operation_token: "worker-token".to_string(),
                    },
                }),
            )
            .await;
        let task = broker
            .poll(namespace_id, task_queue, std::time::Duration::ZERO)
            .await
            .expect("published HTTP task");
        let response = nexus_v1::Response {
            variant: Some(nexus_v1::response::Variant::CancelOperation(
                nexus_v1::CancelOperationResponse {},
            )),
        };

        grpc.respond_nexus_task_completed(Request::new(
            workflowservice::RespondNexusTaskCompletedRequest {
                namespace: "default".to_string(),
                task_token: task.token.encode().expect("token"),
                response: Some(response.clone()),
                ..Default::default()
            },
        ))
        .await
        .expect("HTTP correlation must be acknowledged");

        assert_eq!(
            waiter.outcome().await.expect("waiter outcome"),
            crate::nexus_http::NexusHttpWorkerOutcome::Completed(response)
        );
        assert!(
            runtime.recorded().is_empty(),
            "caller-facing delivery must not touch workflow state"
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_completed_cancel_ack_does_not_resolve() {
        // A CancelOperation response received on a start-operation delivery does not
        // resolve the parent operation. The workflow cancellation-delivery branch is
        // exercised through the store-backed runtime integration tests.
        let runtime = Arc::new(NexusRecordingRuntime::new(true));
        let (grpc, broker) = nexus_test_service(runtime.clone());

        let run_key = RunKey::new();
        let token = publish_test_workflow_nexus_task(&broker, run_key).await;
        grpc.respond_nexus_task_completed(Request::new(
            workflowservice::RespondNexusTaskCompletedRequest {
                namespace: "default".to_string(),
                task_token: token,
                response: Some(nexus_v1::Response {
                    variant: Some(nexus_v1::response::Variant::CancelOperation(
                        nexus_v1::CancelOperationResponse {},
                    )),
                }),
                ..Default::default()
            },
        ))
        .await
        .expect("cancel-ack is acked");

        assert_eq!(
            runtime.recorded().len(),
            0,
            "cancel-ack must not resolve the operation"
        );
    }

    #[tokio::test]
    async fn respond_nexus_task_failed_rejects_empty_task_token() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        let error = grpc
            .respond_nexus_task_failed(Request::new(
                workflowservice::RespondNexusTaskFailedRequest {
                    namespace: "default".to_string(),
                    task_token: Vec::new(),
                    error: Some(nexus_v1::HandlerError {
                        error_type: "Handler".to_string(),
                        failure: None,
                        retry_behavior: 0,
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("empty token should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error.message(), "Task token not set on request.");
    }

    #[tokio::test]
    async fn respond_nexus_task_failed_rejects_missing_error() {
        let (grpc, _broker) = nexus_test_service(Arc::new(NexusRecordingRuntime::new(true)));

        let token = NexusTaskToken {
            namespace_id: namespace_id_for("default").0.to_string(),
            task_queue: "nexus-q".to_string(),
            task_id: Uuid::new_v4().to_string(),
        }
        .encode()
        .expect("token");
        let error = grpc
            .respond_nexus_task_failed(Request::new(
                workflowservice::RespondNexusTaskFailedRequest {
                    namespace: "default".to_string(),
                    task_token: token,
                    error: None,
                    ..Default::default()
                },
            ))
            .await
            .expect_err("missing error should fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn respond_nexus_task_failed_kernel_rejection_returns_success() {
        let runtime = Arc::new(NexusRecordingRuntime::new(false));
        let (grpc, broker) = nexus_test_service(runtime.clone());

        let token = publish_test_workflow_nexus_task(&broker, RunKey::new()).await;
        grpc.respond_nexus_task_failed(Request::new(
            workflowservice::RespondNexusTaskFailedRequest {
                namespace: "default".to_string(),
                task_token: token,
                error: Some(nexus_v1::HandlerError {
                    error_type: "Handler".to_string(),
                    failure: None,
                    retry_behavior: 0,
                }),
                ..Default::default()
            },
        ))
        .await
        .expect("kernel rejection should be swallowed");

        assert_eq!(runtime.recorded().len(), 1);
    }

    async fn publish_test_workflow_nexus_task(
        broker: &NexusTaskBroker,
        run_key: RunKey,
    ) -> Vec<u8> {
        let namespace_id = namespace_id_for("default");
        let task_queue = TaskQueueName("nexus-q".to_string());
        broker
            .publish_workflow(
                namespace_id,
                task_queue.clone(),
                run_key,
                "op-1".to_string(),
                1,
                NexusTaskRequest::StartOperation {
                    service: "svc".to_string(),
                    operation: "op".to_string(),
                    request_id: "op-1".to_string(),
                    payload: None,
                    scheduled_time: Some(OffsetDateTime::UNIX_EPOCH),
                    callback_url: None,
                    callback_token: None,
                },
            )
            .await;
        broker
            .poll(namespace_id, task_queue, std::time::Duration::ZERO)
            .await
            .expect("published task")
            .token
            .encode()
            .expect("token")
    }
}
