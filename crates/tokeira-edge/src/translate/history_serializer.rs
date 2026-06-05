//! Converts kernel `HistoryEvent` values into proto-encoded bytes for the
//! Temporal wire format.
//!
//! The kernel stores events in its own domain types; this serializer is the
//! single place where those types are mapped to the upstream proto `History`
//! message that SDKs expect. Many proto attribute structs use
//! `..Default::default()` because the kernel does not yet carry the full set
//! of upstream fields (e.g. chained `Failure.cause`, `stack_trace`,
//! `failure_info` variants). See `docs/proto-field-audit.md` §3 for the
//! complete gap inventory.

use prost::Message;
use tokeira_kernel::{
    event::{HistoryEvent, HistoryEventKind},
    state::{VersioningBehavior, WorkerDeploymentVersionRef},
};
use tokeira_proto::{
    conversions::common::{
        headers_from_domain, memo_from_domain, payload_from_domain, payload_to_failure,
        payloads_from_domain, search_attributes_from_domain, task_queue_from_domain,
        to_opt_proto_duration, to_proto_duration, to_proto_timestamp,
    },
    enums, history,
    public::temporal::api::{deployment::v1 as deployment_proto, update::v1 as proto_update},
};

/// Serialize a slice of kernel history events into
/// proto-encoded bytes representing a
/// `temporal.api.history.v1.History` message.
pub fn serialize_history(events: &[HistoryEvent]) -> Vec<u8> {
    let proto = history::History {
        events: events.iter().map(history_event_to_proto).collect(),
    };
    proto.encode_to_vec()
}

/// Convert a single kernel `HistoryEvent` to its proto
/// representation.
pub fn history_event_to_proto(event: &HistoryEvent) -> history::HistoryEvent {
    history::HistoryEvent {
        event_id: event.event_id,
        event_time: Some(to_proto_timestamp(event.happened_at)),
        event_type: event_type_for_kind(&event.kind),
        attributes: Some(attributes_for_kind(event)),
        user_metadata: event_user_metadata(event),
        links: event_links(event),
        ..Default::default()
    }
}

fn opt_run_id(r: &Option<tokeira_types::RunId>) -> String {
    r.as_ref().map(|id| id.0.to_string()).unwrap_or_default()
}

fn opt_string(s: &Option<String>) -> String {
    s.clone().unwrap_or_default()
}

fn event_user_metadata(event: &HistoryEvent) -> Option<proto_sdk::UserMetadata> {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted { user_metadata, .. } => {
            user_metadata.as_ref().map(user_metadata_to_proto)
        }
        _ => None,
    }
}

fn event_links(event: &HistoryEvent) -> Vec<proto_common::Link> {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted { links, .. } => {
            links.iter().map(link_to_proto).collect()
        }
        _ => Vec::new(),
    }
}

fn user_metadata_to_proto(metadata: &UserMetadata) -> proto_sdk::UserMetadata {
    proto_sdk::UserMetadata {
        summary: metadata.summary.as_ref().map(payload_from_domain),
        details: metadata.details.as_ref().map(payload_from_domain),
    }
}

fn link_to_proto(link: &Link) -> proto_common::Link {
    use proto_common::link::Variant;
    match link {
        Link::WorkflowEvent {
            namespace,
            workflow_id,
            run_id,
            reference,
        } => proto_common::Link {
            variant: Some(Variant::WorkflowEvent(proto_common::link::WorkflowEvent {
                namespace: namespace.clone(),
                workflow_id: workflow_id.clone(),
                run_id: run_id.clone(),
                reference: reference.as_ref().map(link_reference_to_proto),
            })),
        },
        Link::BatchJob { job_id } => proto_common::Link {
            variant: Some(Variant::BatchJob(proto_common::link::BatchJob {
                job_id: job_id.clone(),
            })),
        },
        Link::Activity {
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
        Link::NexusOperation {
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

fn link_reference_to_proto(
    reference: &LinkWorkflowEventReference,
) -> proto_common::link::workflow_event::Reference {
    use proto_common::link::workflow_event::{EventReference, Reference, RequestIdReference};
    match reference {
        LinkWorkflowEventReference::Event {
            event_id,
            event_type,
        } => Reference::EventRef(EventReference {
            event_id: *event_id,
            event_type: *event_type,
        }),
        LinkWorkflowEventReference::RequestId {
            request_id,
            event_type,
        } => Reference::RequestIdRef(RequestIdReference {
            request_id: request_id.clone(),
            event_type: *event_type,
        }),
    }
}

fn completion_callback_to_proto(callback: &CompletionCallback) -> proto_common::Callback {
    let variant = match &callback.spec {
        CallbackSpec::Nexus { url, header } => Some(proto_common::callback::Variant::Nexus(
            proto_common::callback::Nexus {
                url: url.clone(),
                header: header.clone(),
            },
        )),
    };
    proto_common::Callback {
        variant,
        links: callback.links.iter().map(link_to_proto).collect(),
    }
}

fn priority_to_proto(priority: &Priority) -> proto_common::Priority {
    proto_common::Priority {
        priority_key: priority.priority_key,
        fairness_key: priority.fairness_key.clone(),
        fairness_weight: priority.fairness_weight,
    }
}

fn marker_detail(value: &str) -> tokeira_types::Payloads {
    tokeira_types::Payloads(vec![tokeira_types::Payload::new(value.as_bytes().to_vec())])
}

fn pause_marker(marker_name: &str, identity: &str, reason: &str, request_id: &str) -> Attributes {
    Attributes::MarkerRecordedEventAttributes(history::MarkerRecordedEventAttributes {
        marker_name: marker_name.to_string(),
        details: [
            ("identity".to_string(), marker_detail(identity)),
            ("reason".to_string(), marker_detail(reason)),
            ("request_id".to_string(), marker_detail(request_id)),
        ]
        .into_iter()
        .map(|(key, value)| (key, payloads_from_domain(&value)))
        .collect(),
        ..Default::default()
    })
}

fn event_type_for_kind(kind: &HistoryEventKind) -> i32 {
    use tokeira_proto::enums::EventType as E;
    let et = match kind {
        HistoryEventKind::WorkflowExecutionStarted { .. } => E::WorkflowExecutionStarted,
        HistoryEventKind::WorkflowExecutionCompleted { .. } => E::WorkflowExecutionCompleted,
        HistoryEventKind::WorkflowExecutionFailed { .. } => E::WorkflowExecutionFailed,
        HistoryEventKind::WorkflowExecutionTimedOut { .. } => E::WorkflowExecutionTimedOut,
        HistoryEventKind::WorkflowExecutionCancelRequested { .. } => {
            E::WorkflowExecutionCancelRequested
        }
        HistoryEventKind::WorkflowExecutionCanceled { .. } => E::WorkflowExecutionCanceled,
        HistoryEventKind::WorkflowExecutionTerminated { .. } => E::WorkflowExecutionTerminated,
        HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => {
            E::WorkflowExecutionContinuedAsNew
        }
        HistoryEventKind::WorkflowExecutionSignaled { .. } => E::WorkflowExecutionSignaled,
        HistoryEventKind::WorkflowExecutionPaused { .. }
        | HistoryEventKind::WorkflowExecutionUnpaused { .. } => E::MarkerRecorded,
        HistoryEventKind::WorkflowTaskScheduled { .. } => E::WorkflowTaskScheduled,
        HistoryEventKind::WorkflowTaskStarted { .. } => E::WorkflowTaskStarted,
        HistoryEventKind::WorkflowTaskCompleted { .. } => E::WorkflowTaskCompleted,
        HistoryEventKind::WorkflowTaskFailed { .. } => E::WorkflowTaskFailed,
        HistoryEventKind::WorkflowTaskTimedOut { .. } => E::WorkflowTaskTimedOut,
        HistoryEventKind::ActivityTaskScheduled { .. } => E::ActivityTaskScheduled,
        HistoryEventKind::ActivityTaskStarted { .. } => E::ActivityTaskStarted,
        HistoryEventKind::ActivityTaskCompleted { .. } => E::ActivityTaskCompleted,
        HistoryEventKind::ActivityTaskFailed { .. } => E::ActivityTaskFailed,
        HistoryEventKind::ActivityTaskTimedOut { .. } => E::ActivityTaskTimedOut,
        HistoryEventKind::ActivityTaskCanceled { .. } => E::ActivityTaskCanceled,
        HistoryEventKind::ActivityTaskCancelRequested { .. } => E::ActivityTaskCancelRequested,
        HistoryEventKind::TimerStarted { .. } => E::TimerStarted,
        HistoryEventKind::TimerFired { .. } => E::TimerFired,
        HistoryEventKind::TimerCanceled { .. } => E::TimerCanceled,
        HistoryEventKind::MarkerRecorded { .. } => E::MarkerRecorded,
        HistoryEventKind::StartChildWorkflowExecutionInitiated { .. } => {
            E::StartChildWorkflowExecutionInitiated
        }
        HistoryEventKind::ChildWorkflowExecutionStarted { .. } => E::ChildWorkflowExecutionStarted,
        HistoryEventKind::StartChildWorkflowExecutionFailed { .. } => {
            E::StartChildWorkflowExecutionFailed
        }
        HistoryEventKind::ChildWorkflowExecutionCompleted { .. } => {
            E::ChildWorkflowExecutionCompleted
        }
        HistoryEventKind::ChildWorkflowExecutionFailed { .. } => E::ChildWorkflowExecutionFailed,
        HistoryEventKind::ChildWorkflowExecutionCanceled { .. } => {
            E::ChildWorkflowExecutionCanceled
        }
        HistoryEventKind::ChildWorkflowExecutionTerminated { .. } => {
            E::ChildWorkflowExecutionTerminated
        }
        HistoryEventKind::ChildWorkflowExecutionTimedOut { .. } => {
            E::ChildWorkflowExecutionTimedOut
        }
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated { .. } => {
            E::SignalExternalWorkflowExecutionInitiated
        }
        HistoryEventKind::ExternalWorkflowExecutionSignaled { .. } => {
            E::ExternalWorkflowExecutionSignaled
        }
        HistoryEventKind::SignalExternalWorkflowExecutionFailed { .. } => {
            E::SignalExternalWorkflowExecutionFailed
        }
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated { .. } => {
            E::RequestCancelExternalWorkflowExecutionInitiated
        }
        HistoryEventKind::ExternalWorkflowExecutionCancelRequested { .. } => {
            E::ExternalWorkflowExecutionCancelRequested
        }
        HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed { .. } => {
            E::RequestCancelExternalWorkflowExecutionFailed
        }
        HistoryEventKind::NexusOperationScheduled { .. } => E::NexusOperationScheduled,
        HistoryEventKind::NexusOperationStarted { .. } => E::NexusOperationStarted,
        HistoryEventKind::NexusOperationCompleted { .. } => E::NexusOperationCompleted,
        HistoryEventKind::NexusOperationFailed { .. } => E::NexusOperationFailed,
        HistoryEventKind::NexusOperationCanceled { .. } => E::NexusOperationCanceled,
        HistoryEventKind::NexusOperationTimedOut { .. } => E::NexusOperationTimedOut,
        HistoryEventKind::NexusOperationCancelRequested { .. } => E::NexusOperationCancelRequested,
        HistoryEventKind::WorkflowExecutionUpdateAccepted { .. } => {
            E::WorkflowExecutionUpdateAccepted
        }
        HistoryEventKind::WorkflowExecutionUpdateCompleted { .. } => {
            E::WorkflowExecutionUpdateCompleted
        }
        HistoryEventKind::WorkflowExecutionUpdateRejected { .. } => {
            E::WorkflowExecutionUpdateRejected
        }
        HistoryEventKind::WorkflowExecutionOptionsUpdated { .. } => {
            E::WorkflowExecutionOptionsUpdated
        }
    };
    et as i32
}

use history::history_event::Attributes;
use tokeira_kernel::{
    command::{
        ContinueAsNewInitiator, RetryState, WorkflowTaskFailedCause, WorkflowTaskTimeoutType,
    },
    state::{
        CallbackSpec, CompletionCallback, Link, LinkWorkflowEventReference, ParentClosePolicy,
        Priority, UserMetadata,
    },
};
use tokeira_proto::public::temporal::api::{
    common::v1 as proto_common, failure::v1 as proto_failure, sdk::v1 as proto_sdk,
};

#[allow(clippy::too_many_lines)]
// v1.62-sync: writes deprecated history event fields for wire-compat with
// v0.4-era SDK history readers. Four deprecations land here:
// - `WorkflowExecutionContinuedAsNewEventAttributes.failure` — v1.62 moves
//   failure to the wrapper event level; v0.4 readers still expect the inner.
// - `SignalExternalWorkflowExecutionInitiatedEventAttributes.control` and
//   `RequestCancelExternalWorkflowExecutionInitiatedEventAttributes.control`
//   — v1.62 replaces `control` with an `input_payload`-based shape; v0.4
//   readers still consume `control`.
// - `NexusOperationStartedEventAttributes.operation_id` — v1.62 renames to
//   `operation_token`; v0.4 readers still expect `operation_id`.
// Serialised history is part of the SDK-visible contract and cannot be
// quietly migrated without a replay-compat spec.
#[allow(deprecated)]
fn attributes_for_kind(event: &HistoryEvent) -> Attributes {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted {
            workflow_type,
            task_queue,
            input,
            header,
            workflow_start_delay,
            completion_callbacks,
            user_metadata: _,
            links: _,
            memo,
            search_attributes,
            request_id: _,
            identity,
            continued_execution_run_id,
            first_execution_run_id,
            retry_policy,
            attempt,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            parent_workflow_id,
            parent_run_id,
            parent_namespace_id,
            parent_initiated_event_id,
            root_workflow_id: _,
            root_run_id: _,
            original_execution_run_id,
            continued_failure,
            last_completion_result,
            cron_schedule,
            versioning_info: _,
            worker_deployment_name: _,
            priority,
        } => Attributes::WorkflowExecutionStartedEventAttributes(
            history::WorkflowExecutionStartedEventAttributes {
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                parent_workflow_namespace_id: parent_namespace_id
                    .map(|id| id.0.to_string())
                    .unwrap_or_default(),
                parent_workflow_execution: parent_workflow_id.as_ref().map(|workflow_id| {
                    proto_common::WorkflowExecution {
                        workflow_id: workflow_id.0.clone(),
                        run_id: opt_run_id(parent_run_id),
                    }
                }),
                parent_initiated_event_id: *parent_initiated_event_id,
                task_queue: Some(task_queue_from_domain(task_queue)),
                input: Some(payloads_from_domain(input)),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                continued_execution_run_id: opt_run_id(continued_execution_run_id),
                continued_failure: continued_failure.as_ref().map(payload_to_failure),
                last_completion_result: last_completion_result.as_ref().map(payloads_from_domain),
                original_execution_run_id: opt_run_id(original_execution_run_id),
                first_execution_run_id: opt_run_id(first_execution_run_id),
                cron_schedule: cron_schedule.clone().unwrap_or_default(),
                retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
                attempt: *attempt as i32,
                workflow_execution_timeout: to_opt_proto_duration(*workflow_execution_timeout),
                workflow_run_timeout: to_opt_proto_duration(*workflow_run_timeout),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                identity: identity.clone(),
                header: header.as_ref().map(headers_from_domain),
                first_workflow_task_backoff: to_opt_proto_duration(*workflow_start_delay),
                completion_callbacks: completion_callbacks
                    .iter()
                    .map(completion_callback_to_proto)
                    .collect(),
                priority: priority.as_ref().map(priority_to_proto),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionCompleted {
            workflow_task_completed_event_id,
            result,
        } => {
            Attributes::WorkflowExecutionCompletedEventAttributes(
                history::WorkflowExecutionCompletedEventAttributes {
                    result: Some(payloads_from_domain(result)),
                    workflow_task_completed_event_id: *workflow_task_completed_event_id,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionFailed {
            workflow_task_completed_event_id,
            failure,
            retry_state,
            attempt: _,
        } => {
            let failure = Some(payload_to_failure(failure));
            Attributes::WorkflowExecutionFailedEventAttributes(
                history::WorkflowExecutionFailedEventAttributes {
                    failure,
                    retry_state: retry_state_i32(retry_state),
                    workflow_task_completed_event_id: *workflow_task_completed_event_id,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: _,
            retry_state,
            new_execution_run_id,
        } => Attributes::WorkflowExecutionTimedOutEventAttributes(
            history::WorkflowExecutionTimedOutEventAttributes {
                retry_state: retry_state_i32(retry_state),
                new_execution_run_id: opt_run_id(new_execution_run_id),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionCancelRequested {
            reason,
            external_workflow_execution,
            external_initiated_event_id,
            identity,
            request_id: _,
        } => {
            let ext_exec =
                external_workflow_execution
                    .as_ref()
                    .map(|e| proto_common::WorkflowExecution {
                        workflow_id: e.workflow_id.0.clone(),
                        run_id: e.run_id.0.to_string(),
                    });
            Attributes::WorkflowExecutionCancelRequestedEventAttributes(
                history::WorkflowExecutionCancelRequestedEventAttributes {
                    cause: reason.clone(),
                    external_initiated_event_id: *external_initiated_event_id,
                    external_workflow_execution: ext_exec,
                    identity: identity.clone(),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionCanceled {
            workflow_task_completed_event_id,
            details,
        } => {
            Attributes::WorkflowExecutionCanceledEventAttributes(
                history::WorkflowExecutionCanceledEventAttributes {
                    workflow_task_completed_event_id: *workflow_task_completed_event_id,
                    details: details.as_ref().map(payloads_from_domain),
                },
            )
        }
        HistoryEventKind::WorkflowExecutionTerminated {
            reason,
            details,
            identity,
        } => Attributes::WorkflowExecutionTerminatedEventAttributes(
            history::WorkflowExecutionTerminatedEventAttributes {
                reason: reason.clone(),
                details: details.as_ref().map(payloads_from_domain),
                identity: identity.clone(),
            },
        ),
        HistoryEventKind::WorkflowExecutionContinuedAsNew {
            new_run_id,
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_execution_timeout: _,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy: _,
            initiator,
            failure,
            last_completion_result,
            backoff_start_interval,
            cron_schedule: _,
            workflow_task_completed_event_id,
        } => Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
            history::WorkflowExecutionContinuedAsNewEventAttributes {
                new_execution_run_id: new_run_id.0.to_string(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(task_queue_from_domain(task_queue)),
                input: Some(payloads_from_domain(input)),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                workflow_run_timeout: to_opt_proto_duration(*workflow_run_timeout),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                initiator: continue_as_new_initiator_i32(initiator),
                failure: failure.as_ref().map(payload_to_failure),
                last_completion_result: last_completion_result.as_ref().map(payloads_from_domain),
                backoff_start_interval: to_opt_proto_duration(*backoff_start_interval),
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionSignaled {
            signal_name,
            input,
            header,
            request_id: _,
            identity,
        } => Attributes::WorkflowExecutionSignaledEventAttributes(
            history::WorkflowExecutionSignaledEventAttributes {
                signal_name: signal_name.clone(),
                input: Some(payloads_from_domain(input)),
                identity: opt_string(identity),
                header: header.as_ref().map(headers_from_domain),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionPaused {
            identity,
            reason,
            request_id,
        } => pause_marker("tokeira:paused", identity, reason, request_id),
        HistoryEventKind::WorkflowExecutionUnpaused {
            identity,
            reason,
            request_id,
        } => pause_marker("tokeira:unpaused", identity, reason, request_id),
        HistoryEventKind::WorkflowTaskScheduled {
            logical_seq: _,
            task_queue,
            workflow_task_timeout,
            attempt,
        } => Attributes::WorkflowTaskScheduledEventAttributes(
            history::WorkflowTaskScheduledEventAttributes {
                task_queue: Some(task_queue_from_domain(task_queue)),
                start_to_close_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                attempt: *attempt as i32,
            },
        ),
        HistoryEventKind::WorkflowTaskStarted {
            logical_seq: _,
            scheduled_event_id,
            attempt: _,
            identity,
            request_id,
            history_size_bytes,
            suggest_continue_as_new,
        } => Attributes::WorkflowTaskStartedEventAttributes(
            history::WorkflowTaskStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                identity: identity.0.clone(),
                request_id: request_id.clone(),
                history_size_bytes: *history_size_bytes,
                suggest_continue_as_new: *suggest_continue_as_new,
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowTaskCompleted {
            logical_seq: _,
            scheduled_event_id,
            started_event_id,
            identity,
            sdk_metadata,
            metering_metadata,
            worker_version,
            versioning_behavior,
            deployment_version,
            worker_deployment_name,
        } => Attributes::WorkflowTaskCompletedEventAttributes(
            history::WorkflowTaskCompletedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                identity: identity.0.clone(),
                sdk_metadata: sdk_metadata.as_ref().and_then(|bytes| {
                    tokeira_proto::public::temporal::api::sdk::v1::WorkflowTaskCompletedMetadata::decode(bytes.as_slice()).ok()
                }),
                metering_metadata: metering_metadata
                    .as_ref()
                    .and_then(|bytes| proto_common::MeteringMetadata::decode(bytes.as_slice()).ok()),
                worker_version: worker_version.as_ref().map(|build_id| {
                    proto_common::WorkerVersionStamp {
                        build_id: build_id.clone(),
                        ..Default::default()
                    }
                }),
                versioning_behavior: versioning_behavior_to_proto(*versioning_behavior),
                deployment_version: deployment_version.as_ref().map(deployment_version_to_proto),
                worker_deployment_name: worker_deployment_name.clone().unwrap_or_default(),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowTaskFailed {
            logical_seq: _,
            scheduled_event_id,
            started_event_id,
            failure_cause,
            failure_details,
            identity,
            base_run_id,
            new_run_id,
            fork_event_version,
            fork_event_id: _,
        } => {
            let failure = failure_details.as_ref().map(payload_to_failure);
            Attributes::WorkflowTaskFailedEventAttributes(
                history::WorkflowTaskFailedEventAttributes {
                    scheduled_event_id: *scheduled_event_id,
                    started_event_id: *started_event_id,
                    cause: wft_failed_cause_i32(failure_cause),
                    failure,
                    identity: identity.0.clone(),
                    base_run_id: opt_run_id(base_run_id),
                    new_run_id: opt_run_id(new_run_id),
                    fork_event_version: fork_event_version.unwrap_or(0),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowTaskTimedOut {
            logical_seq: _,
            scheduled_event_id,
            started_event_id,
            timeout_type,
        } => Attributes::WorkflowTaskTimedOutEventAttributes(
            history::WorkflowTaskTimedOutEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                timeout_type: wft_timeout_i32(timeout_type),
            },
        ),
        HistoryEventKind::ActivityTaskScheduled {
            workflow_task_completed_event_id,
            activity_id,
            activity_type,
            task_queue,
            input,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            header,
            retry_policy,
        } => Attributes::ActivityTaskScheduledEventAttributes(
            history::ActivityTaskScheduledEventAttributes {
                activity_id: activity_id.clone(),
                activity_type: Some(proto_common::ActivityType {
                    name: activity_type.clone(),
                }),
                task_queue: Some(task_queue_from_domain(task_queue)),
                header: header.as_ref().map(headers_from_domain),
                input: Some(payloads_from_domain(input)),
                schedule_to_close_timeout: to_opt_proto_duration(*schedule_to_close_timeout),
                schedule_to_start_timeout: to_opt_proto_duration(*schedule_to_start_timeout),
                start_to_close_timeout: to_opt_proto_duration(*start_to_close_timeout),
                heartbeat_timeout: to_opt_proto_duration(*heartbeat_timeout),
                retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskStarted {
            activity_id: _,
            scheduled_event_id,
            attempt,
            identity,
            request_id,
            last_failure,
        } => Attributes::ActivityTaskStartedEventAttributes(
            history::ActivityTaskStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                identity: identity.0.clone(),
                attempt: *attempt as i32,
                request_id: request_id.clone(),
                last_failure: last_failure.as_ref().map(payload_to_failure),
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCompleted {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            identity,
            result,
        } => Attributes::ActivityTaskCompletedEventAttributes(
            history::ActivityTaskCompletedEventAttributes {
                result: Some(payloads_from_domain(result)),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                identity: identity
                    .as_ref()
                    .map(|worker| worker.0.clone())
                    .unwrap_or_default(),
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskFailed {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            identity,
            retry_state,
            failure,
        } => {
            let failure = Some(payload_to_failure(failure));
            Attributes::ActivityTaskFailedEventAttributes(
                history::ActivityTaskFailedEventAttributes {
                    failure,
                    scheduled_event_id: *scheduled_event_id,
                    started_event_id: *started_event_id,
                    identity: identity
                        .as_ref()
                        .map(|worker| worker.0.clone())
                        .unwrap_or_default(),
                    retry_state: retry_state_i32(retry_state),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::ActivityTaskTimedOut {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            timeout_type,
            retry_state,
        } => Attributes::ActivityTaskTimedOutEventAttributes(
            history::ActivityTaskTimedOutEventAttributes {
                failure: Some(proto_failure::Failure {
                    message: format!("activity timed out: {timeout_type}"),
                    failure_info: Some(proto_failure::failure::FailureInfo::TimeoutFailureInfo(
                        proto_failure::TimeoutFailureInfo {
                            timeout_type: activity_timeout_type_i32(timeout_type),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                retry_state: retry_state_i32(retry_state),
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCanceled {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            identity,
            details,
        } => Attributes::ActivityTaskCanceledEventAttributes(
            history::ActivityTaskCanceledEventAttributes {
                details: details.as_ref().map(payloads_from_domain),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                identity: identity
                    .as_ref()
                    .map(|worker| worker.0.clone())
                    .unwrap_or_default(),
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCancelRequested {
            activity_id,
            scheduled_event_id,
            workflow_task_completed_event_id,
        } => {
            let _ = activity_id;
            Attributes::ActivityTaskCancelRequestedEventAttributes(
                history::ActivityTaskCancelRequestedEventAttributes {
                    scheduled_event_id: *scheduled_event_id,
                    workflow_task_completed_event_id: *workflow_task_completed_event_id,
                },
            )
        }
        HistoryEventKind::TimerStarted {
            workflow_task_completed_event_id,
            timer_id,
            fire_at,
        } => {
            Attributes::TimerStartedEventAttributes(history::TimerStartedEventAttributes {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(to_proto_duration(
                    (*fire_at - event.happened_at).max(time::Duration::ZERO),
                )),
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                ..Default::default()
            })
        }
        HistoryEventKind::TimerFired {
            timer_id,
            started_event_id,
        } => Attributes::TimerFiredEventAttributes(history::TimerFiredEventAttributes {
            timer_id: timer_id.clone(),
            started_event_id: *started_event_id,
        }),
        HistoryEventKind::TimerCanceled {
            workflow_task_completed_event_id,
            timer_id,
            started_event_id,
        } => {
            Attributes::TimerCanceledEventAttributes(history::TimerCanceledEventAttributes {
                timer_id: timer_id.clone(),
                started_event_id: *started_event_id,
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                ..Default::default()
            })
        }
        HistoryEventKind::MarkerRecorded {
            workflow_task_completed_event_id,
            marker_name,
            details,
            failure,
            header,
        } => Attributes::MarkerRecordedEventAttributes(history::MarkerRecordedEventAttributes {
            marker_name: marker_name.clone(),
            details: details
                .iter()
                .map(|(k, v)| (k.clone(), payloads_from_domain(v)))
                .collect(),
            failure: failure.as_ref().map(payload_to_failure),
            header: header.as_ref().map(|h| proto_common::Header {
                fields: h
                    .iter()
                    .map(|(k, v)| (k.clone(), payload_from_domain(v)))
                    .collect(),
            }),
            workflow_task_completed_event_id: *workflow_task_completed_event_id,
            ..Default::default()
        }),
        HistoryEventKind::StartChildWorkflowExecutionInitiated {
            workflow_task_completed_event_id,
            child_workflow_id,
            workflow_type,
            task_queue,
            input,
            namespace_id,
            namespace,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            cron_schedule,
            parent_close_policy,
        } => Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(
            history::StartChildWorkflowExecutionInitiatedEventAttributes {
                namespace: namespace.clone().unwrap_or_default(),
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(task_queue_from_domain(task_queue)),
                input: Some(payloads_from_domain(input)),
                namespace_id: namespace_id.0.to_string(),
                header: header.as_ref().map(headers_from_domain),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                workflow_execution_timeout: to_opt_proto_duration(*workflow_execution_timeout),
                workflow_run_timeout: to_opt_proto_duration(*workflow_run_timeout),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
                cron_schedule: cron_schedule.clone().unwrap_or_default(),
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                parent_close_policy: parent_close_policy_i32(parent_close_policy),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionStarted {
            child_workflow_id,
            child_run_id,
            workflow_type,
            initiated_event_id,
        } => Attributes::ChildWorkflowExecutionStartedEventAttributes(
            history::ChildWorkflowExecutionStartedEventAttributes {
                initiated_event_id: *initiated_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    run_id: child_run_id.0.to_string(),
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::StartChildWorkflowExecutionFailed {
            child_workflow_id,
            initiated_event_id,
            namespace_id,
            namespace,
            workflow_type,
            cause,
        } => Attributes::StartChildWorkflowExecutionFailedEventAttributes(
            history::StartChildWorkflowExecutionFailedEventAttributes {
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                cause: start_child_workflow_failed_cause_i32(cause),
                initiated_event_id: *initiated_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionCompleted {
            child_workflow_id,
            namespace_id,
            namespace,
            child_run_id,
            workflow_type,
            result,
            initiated_event_id,
            started_event_id,
        } => Attributes::ChildWorkflowExecutionCompletedEventAttributes(
            history::ChildWorkflowExecutionCompletedEventAttributes {
                result: Some(payloads_from_domain(result)),
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    run_id: opt_run_id(child_run_id),
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionFailed {
            child_workflow_id,
            namespace_id,
            namespace,
            child_run_id,
            workflow_type,
            retry_state,
            failure,
            initiated_event_id,
            started_event_id,
        } => Attributes::ChildWorkflowExecutionFailedEventAttributes(
            history::ChildWorkflowExecutionFailedEventAttributes {
                failure: Some(payload_to_failure(failure)),
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    run_id: opt_run_id(child_run_id),
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                retry_state: retry_state_i32(retry_state),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionCanceled {
            child_workflow_id,
            namespace_id,
            namespace,
            child_run_id,
            workflow_type,
            details,
            initiated_event_id,
            started_event_id,
        } => Attributes::ChildWorkflowExecutionCanceledEventAttributes(
            history::ChildWorkflowExecutionCanceledEventAttributes {
                details: details.as_ref().map(payloads_from_domain),
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    run_id: opt_run_id(child_run_id),
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionTerminated {
            child_workflow_id,
            namespace_id,
            namespace,
            workflow_type,
            initiated_event_id,
            started_event_id,
        } => Attributes::ChildWorkflowExecutionTerminatedEventAttributes(
            history::ChildWorkflowExecutionTerminatedEventAttributes {
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    ..Default::default()
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionTimedOut {
            child_workflow_id,
            namespace_id,
            namespace,
            workflow_type,
            retry_state,
            initiated_event_id,
            started_event_id,
        } => Attributes::ChildWorkflowExecutionTimedOutEventAttributes(
            history::ChildWorkflowExecutionTimedOutEventAttributes {
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                initiated_event_id: *initiated_event_id,
                started_event_id: *started_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    ..Default::default()
                }),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                retry_state: retry_state_i32(retry_state),
                ..Default::default()
            },
        ),
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
            workflow_task_completed_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
            header,
            control,
        } => Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(
            history::SignalExternalWorkflowExecutionInitiatedEventAttributes {
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                signal_name: signal_name.clone(),
                input: Some(payloads_from_domain(input)),
                header: header.as_ref().map(headers_from_domain),
                control: control.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::ExternalWorkflowExecutionSignaled {
            initiated_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
        } => Attributes::ExternalWorkflowExecutionSignaledEventAttributes(
            history::ExternalWorkflowExecutionSignaledEventAttributes {
                initiated_event_id: *initiated_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::SignalExternalWorkflowExecutionFailed {
            initiated_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
            cause,
        } => Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(
            history::SignalExternalWorkflowExecutionFailedEventAttributes {
                cause: signal_external_workflow_failed_cause_i32(cause),
                initiated_event_id: *initiated_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
            workflow_task_completed_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
            control,
        } => Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
            history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                control: control.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::ExternalWorkflowExecutionCancelRequested {
            initiated_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
        } => Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(
            history::ExternalWorkflowExecutionCancelRequestedEventAttributes {
                initiated_event_id: *initiated_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed {
            initiated_event_id,
            namespace_id,
            namespace,
            target_workflow_id,
            target_run_id,
            cause,
        } => Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(
            history::RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                cause: cancel_external_workflow_failed_cause_i32(cause),
                initiated_event_id: *initiated_event_id,
                namespace: namespace.clone().unwrap_or_default(),
                namespace_id: namespace_id.0.to_string(),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationScheduled {
            workflow_task_completed_event_id,
            operation_id: _,
            endpoint,
            endpoint_id,
            service,
            operation,
            input,
            nexus_header,
            schedule_to_close_timeout,
        } => Attributes::NexusOperationScheduledEventAttributes(
            history::NexusOperationScheduledEventAttributes {
                endpoint: endpoint.clone(),
                endpoint_id: endpoint_id.clone(),
                service: service.clone(),
                operation: operation.clone(),
                input: input.0.first().map(payload_from_domain),
                nexus_header: nexus_header.clone(),
                schedule_to_close_timeout: to_opt_proto_duration(*schedule_to_close_timeout),
                workflow_task_completed_event_id: *workflow_task_completed_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationStarted {
            operation_id,
            scheduled_event_id,
        } => Attributes::NexusOperationStartedEventAttributes(
            history::NexusOperationStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                request_id: operation_id.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationCompleted {
            operation_id,
            scheduled_event_id,
            result,
        } => Attributes::NexusOperationCompletedEventAttributes(
            history::NexusOperationCompletedEventAttributes {
                request_id: operation_id.clone(),
                scheduled_event_id: *scheduled_event_id,
                result: result.0.first().map(payload_from_domain),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationFailed {
            operation_id,
            scheduled_event_id,
            failure,
        } => Attributes::NexusOperationFailedEventAttributes(
            history::NexusOperationFailedEventAttributes {
                request_id: operation_id.clone(),
                scheduled_event_id: *scheduled_event_id,
                failure: Some(payload_to_failure(failure)),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationCanceled {
            operation_id,
            scheduled_event_id,
        } => Attributes::NexusOperationCanceledEventAttributes(
            history::NexusOperationCanceledEventAttributes {
                request_id: operation_id.clone(),
                scheduled_event_id: *scheduled_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationTimedOut {
            operation_id,
            scheduled_event_id,
        } => Attributes::NexusOperationTimedOutEventAttributes(
            history::NexusOperationTimedOutEventAttributes {
                request_id: operation_id.clone(),
                scheduled_event_id: *scheduled_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationCancelRequested { scheduled_event_id } => {
            Attributes::NexusOperationCancelRequestedEventAttributes(
                history::NexusOperationCancelRequestedEventAttributes {
                    scheduled_event_id: *scheduled_event_id,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionUpdateAccepted {
            update_id,
            update_name,
            input,
            accepted_request_sequencing_event_id,
        } => Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
            history::WorkflowExecutionUpdateAcceptedEventAttributes {
                protocol_instance_id: update_id.clone(),
                accepted_request_message_id: update_id.clone(),
                accepted_request_sequencing_event_id: *accepted_request_sequencing_event_id,
                accepted_request: Some(proto_update::Request {
                    meta: Some(proto_update::Meta {
                        update_id: update_id.clone(),
                        identity: String::new(),
                    }),
                    input: Some(proto_update::Input {
                        header: None,
                        name: update_name.clone(),
                        args: Some(payloads_from_domain(input)),
                    }),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionUpdateCompleted {
            update_id,
            result,
            accepted_event_id,
        } => {
            Attributes::WorkflowExecutionUpdateCompletedEventAttributes(
                history::WorkflowExecutionUpdateCompletedEventAttributes {
                    meta: Some(proto_update::Meta {
                        update_id: update_id.clone(),
                        identity: String::new(),
                    }),
                    outcome: Some(proto_update::Outcome {
                        value: Some(proto_update::outcome::Value::Success(payloads_from_domain(
                            result,
                        ))),
                    }),
                    accepted_event_id: *accepted_event_id,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionUpdateRejected {
            update_id,
            failure,
            rejected_request_message_id,
            rejected_request_sequencing_event_id,
        } => {
            Attributes::WorkflowExecutionUpdateRejectedEventAttributes(
                history::WorkflowExecutionUpdateRejectedEventAttributes {
                    protocol_instance_id: update_id.clone(),
                    rejected_request_message_id: rejected_request_message_id.clone(),
                    rejected_request_sequencing_event_id: *rejected_request_sequencing_event_id,
                    failure: Some(payload_to_failure(failure)),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionOptionsUpdated {
            versioning_override,
            completion_callbacks,
            attached_completion_callbacks,
            attached_links,
            attached_request_id,
        } => {
            // The upstream proto only has `versioning_override`. `completion_callbacks`
            // plus on-conflict attachment fields are internal kernel fields with
            // no proto representation. `VersioningOverride` is a placeholder type
            // so we can't populate the proto field yet either.
            let _ = (
                versioning_override,
                completion_callbacks,
                attached_completion_callbacks,
                attached_links,
                attached_request_id,
            );
            Attributes::WorkflowExecutionOptionsUpdatedEventAttributes(
                history::WorkflowExecutionOptionsUpdatedEventAttributes {
                    ..Default::default()
                },
            )
        }
    }
}

fn retry_policy_to_proto(rp: &tokeira_types::RetryPolicy) -> proto_common::RetryPolicy {
    proto_common::RetryPolicy {
        initial_interval: Some(to_proto_duration(rp.initial_interval)),
        backoff_coefficient: rp.backoff_coefficient,
        maximum_interval: to_opt_proto_duration(rp.maximum_interval),
        maximum_attempts: rp.maximum_attempts as i32,
        non_retryable_error_types: rp.non_retryable_error_types.clone(),
    }
}

fn versioning_behavior_to_proto(behavior: VersioningBehavior) -> i32 {
    match behavior {
        VersioningBehavior::Unspecified => enums::VersioningBehavior::Unspecified as i32,
        VersioningBehavior::Pinned => enums::VersioningBehavior::Pinned as i32,
        VersioningBehavior::AutoUpgrade => enums::VersioningBehavior::AutoUpgrade as i32,
    }
}

fn deployment_version_to_proto(
    version: &WorkerDeploymentVersionRef,
) -> deployment_proto::WorkerDeploymentVersion {
    deployment_proto::WorkerDeploymentVersion {
        build_id: version.build_id.clone(),
        deployment_name: version.deployment_name.clone(),
    }
}

fn continue_as_new_initiator_i32(initiator: &ContinueAsNewInitiator) -> i32 {
    use tokeira_proto::enums::ContinueAsNewInitiator as I;
    match initiator {
        ContinueAsNewInitiator::Workflow => I::Workflow as i32,
        ContinueAsNewInitiator::Retry => I::Retry as i32,
        ContinueAsNewInitiator::CronSchedule => I::CronSchedule as i32,
    }
}

fn retry_state_i32(s: &RetryState) -> i32 {
    use tokeira_proto::enums::RetryState as R;
    (match s {
        RetryState::InProgress => R::InProgress,
        RetryState::NonRetryableFailure => R::NonRetryableFailure,
        RetryState::Timeout => R::Timeout,
        RetryState::MaximumAttemptsReached => R::MaximumAttemptsReached,
        RetryState::RetryPolicyNotSet => R::RetryPolicyNotSet,
        RetryState::InternalServerError => R::InternalServerError,
        RetryState::CancelRequested => R::CancelRequested,
    }) as i32
}

fn wft_failed_cause_i32(c: &WorkflowTaskFailedCause) -> i32 {
    use tokeira_proto::enums::WorkflowTaskFailedCause as C;
    (match c {
        WorkflowTaskFailedCause::NonDeterminismError => C::NonDeterministicError,
        WorkflowTaskFailedCause::BadScheduleActivityAttributes => C::BadScheduleActivityAttributes,
        WorkflowTaskFailedCause::BadStartTimerAttributes => C::BadStartTimerAttributes,
        WorkflowTaskFailedCause::UnhandledCommand => C::UnhandledCommand,
        WorkflowTaskFailedCause::BadRequestCancelActivityAttributes => {
            C::BadRequestCancelActivityAttributes
        }
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure => {
            C::WorkflowWorkerUnhandledFailure
        }
        WorkflowTaskFailedCause::BadSignalWorkflowExecutionAttributes => {
            C::BadSignalWorkflowExecutionAttributes
        }
        WorkflowTaskFailedCause::ResetWorkflow => C::ResetWorkflow,
    }) as i32
}

fn wft_timeout_i32(t: &WorkflowTaskTimeoutType) -> i32 {
    use tokeira_proto::enums::TimeoutType as T;
    (match t {
        WorkflowTaskTimeoutType::StartToClose => T::StartToClose,
    }) as i32
}

fn parent_close_policy_i32(p: &ParentClosePolicy) -> i32 {
    use tokeira_proto::enums::ParentClosePolicy as P;
    (match p {
        ParentClosePolicy::Terminate => P::Terminate,
        ParentClosePolicy::RequestCancel => P::RequestCancel,
        ParentClosePolicy::Abandon => P::Abandon,
    }) as i32
}

fn activity_timeout_type_i32(timeout_type: &str) -> i32 {
    use tokeira_proto::enums::TimeoutType as T;
    match timeout_type {
        "START_TO_CLOSE" | "StartToClose" | "start_to_close" => T::StartToClose as i32,
        "SCHEDULE_TO_START" | "ScheduleToStart" | "schedule_to_start" => T::ScheduleToStart as i32,
        "SCHEDULE_TO_CLOSE" | "ScheduleToClose" | "schedule_to_close" => T::ScheduleToClose as i32,
        "HEARTBEAT" | "Heartbeat" | "heartbeat" => T::Heartbeat as i32,
        _ => T::Unspecified as i32,
    }
}

fn start_child_workflow_failed_cause_i32(cause: &str) -> i32 {
    use tokeira_proto::enums::StartChildWorkflowExecutionFailedCause as C;
    match cause {
        "WORKFLOW_ALREADY_EXISTS" | "WorkflowAlreadyExists" => C::WorkflowAlreadyExists as i32,
        "NAMESPACE_NOT_FOUND" | "NamespaceNotFound" => C::NamespaceNotFound as i32,
        _ => C::Unspecified as i32,
    }
}

fn signal_external_workflow_failed_cause_i32(cause: &str) -> i32 {
    use tokeira_proto::enums::SignalExternalWorkflowExecutionFailedCause as C;
    match cause {
        "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND" | "ExternalWorkflowExecutionNotFound" => {
            C::ExternalWorkflowExecutionNotFound as i32
        }
        "NAMESPACE_NOT_FOUND" | "NamespaceNotFound" => C::NamespaceNotFound as i32,
        "SIGNAL_COUNT_LIMIT_EXCEEDED" | "SignalCountLimitExceeded" => {
            C::SignalCountLimitExceeded as i32
        }
        _ => C::Unspecified as i32,
    }
}

fn cancel_external_workflow_failed_cause_i32(cause: &str) -> i32 {
    use tokeira_proto::enums::CancelExternalWorkflowExecutionFailedCause as C;
    match cause {
        "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND" | "ExternalWorkflowExecutionNotFound" => {
            C::ExternalWorkflowExecutionNotFound as i32
        }
        "NAMESPACE_NOT_FOUND" | "NamespaceNotFound" => C::NamespaceNotFound as i32,
        _ => C::Unspecified as i32,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    use time::OffsetDateTime;
    use tokeira_kernel::{
        command::{ContinueAsNewInitiator, RetryState},
        event::{HistoryEvent, HistoryEventKind},
        state::{CallbackState, CallbackTrigger, ParentClosePolicy},
    };
    use tokeira_proto::conversions::common::failure_to_payload;
    use tokeira_types::{
        LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, RetryPolicy, RunId, SearchAttributes,
        TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    fn arb_payloads() -> impl Strategy<Value = Payloads> {
        Just(Payloads::default())
    }

    fn arb_failure_payload() -> impl Strategy<Value = Payload> {
        "[a-z ]{0,20}".prop_map(|msg| {
            let failure = proto_failure::Failure {
                message: msg,
                ..Default::default()
            };
            failure_to_payload(&failure)
        })
    }

    fn arb_run_id() -> impl Strategy<Value = RunId> {
        any::<u128>().prop_map(|v| RunId(uuid::Uuid::from_u128(v)))
    }

    fn arb_opt_run_id() -> impl Strategy<Value = Option<RunId>> {
        proptest::option::of(arb_run_id())
    }

    fn arb_opt_duration() -> impl Strategy<Value = Option<time::Duration>> {
        proptest::option::of((0i64..1_000_000).prop_map(|ms| time::Duration::milliseconds(ms)))
    }

    fn arb_duration() -> impl Strategy<Value = time::Duration> {
        (0i64..1_000_000).prop_map(time::Duration::milliseconds)
    }

    fn arb_timestamp() -> impl Strategy<Value = OffsetDateTime> {
        (1i64..2_000_000_000).prop_map(|s| OffsetDateTime::from_unix_timestamp(s).unwrap())
    }

    fn arb_retry_policy() -> impl Strategy<Value = Option<RetryPolicy>> {
        proptest::option::of(
            (arb_duration(), 1.0f64..5.0, arb_opt_duration(), 0u32..10).prop_map(
                |(init, coeff, max, attempts)| RetryPolicy {
                    initial_interval: init,
                    backoff_coefficient: coeff,
                    maximum_interval: max,
                    maximum_attempts: attempts,
                    non_retryable_error_types: vec![],
                },
            ),
        )
    }

    fn arb_retry_state() -> impl Strategy<Value = RetryState> {
        prop_oneof![
            Just(RetryState::InProgress),
            Just(RetryState::NonRetryableFailure),
            Just(RetryState::Timeout),
            Just(RetryState::MaximumAttemptsReached),
            Just(RetryState::RetryPolicyNotSet),
            Just(RetryState::InternalServerError),
            Just(RetryState::CancelRequested),
        ]
    }

    fn arb_headers() -> impl Strategy<Value = Option<tokeira_types::Headers>> {
        proptest::option::of(Just(tokeira_types::Headers(Default::default())))
    }

    fn arb_history_event_kind() -> impl Strategy<Value = HistoryEventKind> {
        prop_oneof![
            (
                "[a-z]{1,6}",
                "[a-z]{1,6}",
                arb_payloads(),
                "[a-z]{1,8}",
                arb_opt_run_id(),
                arb_opt_run_id(),
                arb_retry_policy(),
                0u32..5,
                arb_opt_duration(),
                arb_opt_duration(),
                arb_duration(),
            )
                .prop_map(
                    |(wt, tq, input, rid, cont, first, rp, att, wet, wrt, wtt)| {
                        HistoryEventKind::WorkflowExecutionStarted {
                            workflow_type: WorkflowType(wt),
                            task_queue: TaskQueueName(tq),
                            input,
                            header: None,
                            workflow_start_delay: None,
                            completion_callbacks: Vec::new(),
                            user_metadata: None,
                            links: Vec::new(),
                            memo: Memo::default(),
                            search_attributes: SearchAttributes::default(),
                            request_id: rid,
                            identity: "client".to_string(),
                            continued_execution_run_id: cont,
                            first_execution_run_id: first,
                            retry_policy: rp,
                            attempt: att,
                            workflow_execution_timeout: wet,
                            workflow_run_timeout: wrt,
                            workflow_task_timeout: wtt,
                            parent_workflow_id: None,
                            parent_run_id: None,
                            parent_namespace_id: None,
                            parent_initiated_event_id: 0,
                            root_workflow_id: None,
                            root_run_id: None,
                            original_execution_run_id: None,
                            continued_failure: None,
                            last_completion_result: None,
                            cron_schedule: None,
                            versioning_info: None,
                            worker_deployment_name: None,
                            priority: None,
                        }
                    }
                ),
            arb_payloads().prop_map(|r| {
                HistoryEventKind::WorkflowExecutionCompleted {
                    workflow_task_completed_event_id: 4,
                    result: r,
                }
            }),
            (arb_failure_payload(), arb_retry_state(), 0u32..5).prop_map(|(failure, rs, att)| {
                HistoryEventKind::WorkflowExecutionFailed {
                    workflow_task_completed_event_id: 4,
                    failure,
                    retry_state: rs,
                    attempt: att,
                }
            },),
            Just(HistoryEventKind::WorkflowExecutionCanceled {
                workflow_task_completed_event_id: 4,
                details: None,
            }),
            "[a-z]{1,8}".prop_map(|r| {
                HistoryEventKind::WorkflowExecutionTerminated {
                    reason: r,
                    details: None,
                    identity: "test".to_string(),
                }
            }),
            ("[a-z]{1,6}", arb_payloads(), "[a-z]{1,8}").prop_map(|(sn, input, rid)| {
                HistoryEventKind::WorkflowExecutionSignaled {
                    signal_name: sn,
                    input,
                    header: None,
                    request_id: rid,
                    identity: None,
                }
            }),
            (1u64..100, "[a-z]{1,6}", 1u32..5).prop_map(|(seq, tq, attempt)| {
                HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq: LogicalTaskSeq(seq),
                    task_queue: TaskQueueName(tq),
                    workflow_task_timeout: time::Duration::seconds(30),
                    attempt,
                }
            }),
            (1u64..100, 1i64..100, 1u32..5).prop_map(|(seq, sched, att)| {
                HistoryEventKind::WorkflowTaskStarted {
                    logical_seq: LogicalTaskSeq(seq),
                    scheduled_event_id: sched,
                    attempt: att,
                    identity: WorkerIdentity("w".to_string()),
                    request_id: "start-req".to_string(),
                    history_size_bytes: 0,
                    suggest_continue_as_new: false,
                }
            }),
            (1u64..100, 1i64..100, 1i64..100).prop_map(|(seq, sched, started)| {
                HistoryEventKind::WorkflowTaskCompleted {
                    logical_seq: LogicalTaskSeq(seq),
                    scheduled_event_id: sched,
                    started_event_id: started,
                    identity: WorkerIdentity("w".to_string()),
                    sdk_metadata: None,
                    metering_metadata: None,
                    worker_version: None,
                    versioning_behavior: VersioningBehavior::Unspecified,
                    deployment_version: None,
                    worker_deployment_name: None,
                }
            }),
            (
                "[a-z]{1,6}",
                "[a-z]{1,8}",
                "[a-z]{1,6}",
                arb_payloads(),
                arb_headers(),
                arb_retry_policy(),
                arb_opt_duration(),
                arb_opt_duration(),
                arb_opt_duration(),
                arb_opt_duration(),
            )
                .prop_map(|(aid, at, tq, input, hdr, rp, s2c, s2s, stc, hb)| {
                    HistoryEventKind::ActivityTaskScheduled {
                        workflow_task_completed_event_id: 4,
                        activity_id: aid,
                        activity_type: at,
                        task_queue: TaskQueueName(tq),
                        input,
                        header: hdr,
                        retry_policy: rp,
                        schedule_to_close_timeout: s2c,
                        schedule_to_start_timeout: s2s,
                        start_to_close_timeout: stc,
                        heartbeat_timeout: hb,
                    }
                },),
            ("[a-z]{1,6}", arb_payloads()).prop_map(|(aid, result)| {
                HistoryEventKind::ActivityTaskCompleted {
                    activity_id: aid,
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: Some(WorkerIdentity("w".to_string())),
                    result,
                }
            }),
            ("[a-z]{1,6}", arb_failure_payload()).prop_map(|(aid, failure)| {
                HistoryEventKind::ActivityTaskFailed {
                    activity_id: aid,
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: Some(WorkerIdentity("w".to_string())),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure,
                }
            }),
            ("[a-z]{1,6}", 1i64..100, 1u32..5).prop_map(|(aid, sched, att)| {
                HistoryEventKind::ActivityTaskStarted {
                    activity_id: aid,
                    scheduled_event_id: sched,
                    attempt: att,
                    identity: WorkerIdentity("w".to_string()),
                    request_id: "activity-start".to_string(),
                    last_failure: None,
                }
            }),
            ("[a-z]{1,6}", arb_timestamp()).prop_map(|(tid, fire_at)| {
                HistoryEventKind::TimerStarted {
                    workflow_task_completed_event_id: 4,
                    timer_id: tid,
                    fire_at,
                }
            }),
            "[a-z]{1,6}".prop_map(|tid| HistoryEventKind::TimerFired {
                timer_id: tid,
                started_event_id: 5,
            }),
            "[a-z]{1,6}".prop_map(|tid| HistoryEventKind::TimerCanceled {
                workflow_task_completed_event_id: 4,
                timer_id: tid,
                started_event_id: 5,
            }),
            "[a-z]{1,6}".prop_map(|aid| {
                HistoryEventKind::ActivityTaskCancelRequested {
                    workflow_task_completed_event_id: 4,
                    activity_id: aid,
                    scheduled_event_id: 5,
                }
            }),
            ("[a-z]{1,6}", arb_run_id(), "[a-z]{1,6}", 1i64..100).prop_map(
                |(cwid, crid, wt, iei)| {
                    HistoryEventKind::ChildWorkflowExecutionStarted {
                        child_workflow_id: WorkflowId(cwid),
                        child_run_id: crid,
                        workflow_type: WorkflowType(wt),
                        initiated_event_id: iei,
                    }
                },
            ),
            ("[a-z]{1,6}", arb_payloads(), 1i64..100, 1i64..100).prop_map(
                |(cwid, result, iei, sei)| {
                    HistoryEventKind::ChildWorkflowExecutionCompleted {
                        child_workflow_id: WorkflowId(cwid),
                        namespace_id: NamespaceId::new(),
                        namespace: Some("default".to_string()),
                        child_run_id: Some(RunId::new()),
                        workflow_type: WorkflowType("child".to_string()),
                        result,
                        initiated_event_id: iei,
                        started_event_id: sei,
                    }
                },
            ),
            ("[a-z]{1,6}", arb_failure_payload(), 1i64..100, 1i64..100).prop_map(
                |(cwid, failure, iei, sei)| {
                    HistoryEventKind::ChildWorkflowExecutionFailed {
                        child_workflow_id: WorkflowId(cwid),
                        namespace_id: NamespaceId::new(),
                        namespace: Some("default".to_string()),
                        child_run_id: Some(RunId::new()),
                        workflow_type: WorkflowType("child".to_string()),
                        retry_state: RetryState::RetryPolicyNotSet,
                        failure,
                        initiated_event_id: iei,
                        started_event_id: sei,
                    }
                },
            ),
            ("[a-z]{1,6}", 1i64..100, 1i64..100).prop_map(|(cwid, iei, sei)| {
                HistoryEventKind::ChildWorkflowExecutionCanceled {
                    child_workflow_id: WorkflowId(cwid),
                    namespace_id: NamespaceId::new(),
                    namespace: Some("default".to_string()),
                    child_run_id: Some(RunId::new()),
                    workflow_type: WorkflowType("child".to_string()),
                    details: None,
                    initiated_event_id: iei,
                    started_event_id: sei,
                }
            }),
            ("[a-z]{1,6}", 1i64..100, 1i64..100).prop_map(|(cwid, iei, sei)| {
                HistoryEventKind::ChildWorkflowExecutionTerminated {
                    child_workflow_id: WorkflowId(cwid),
                    namespace_id: NamespaceId::new(),
                    namespace: Some("default".to_string()),
                    workflow_type: WorkflowType("child".to_string()),
                    initiated_event_id: iei,
                    started_event_id: sei,
                }
            }),
            ("[a-z]{1,6}", 1i64..100, 1i64..100).prop_map(|(cwid, iei, sei)| {
                HistoryEventKind::ChildWorkflowExecutionTimedOut {
                    child_workflow_id: WorkflowId(cwid),
                    namespace_id: NamespaceId::new(),
                    namespace: Some("default".to_string()),
                    workflow_type: WorkflowType("child".to_string()),
                    retry_state: RetryState::Timeout,
                    initiated_event_id: iei,
                    started_event_id: sei,
                }
            }),
            (
                "[a-z]{1,6}",
                "[a-z]{1,6}",
                "[a-z]{1,6}",
                "[a-z]{1,6}",
                arb_payloads(),
                arb_opt_duration(),
            )
                .prop_map(|(oid, ep, svc, op, input, timeout)| {
                    HistoryEventKind::NexusOperationScheduled {
                        workflow_task_completed_event_id: 4,
                        operation_id: oid,
                        endpoint: ep,
                        endpoint_id: "endpoint-id".to_string(),
                        service: svc,
                        operation: op,
                        input,
                        nexus_header: std::collections::BTreeMap::new(),
                        schedule_to_close_timeout: timeout,
                    }
                }),
            ("[a-z]{1,6}", "[a-z]{1,6}", arb_payloads()).prop_map(|(uid, uname, input)| {
                HistoryEventKind::WorkflowExecutionUpdateAccepted {
                    update_id: uid,
                    update_name: uname,
                    input,
                    accepted_request_sequencing_event_id: 3,
                }
            }),
            ("[a-z]{1,6}", arb_payloads()).prop_map(|(uid, result)| {
                HistoryEventKind::WorkflowExecutionUpdateCompleted {
                    update_id: uid,
                    result,
                    accepted_event_id: 2,
                }
            }),
            ("[a-z]{1,6}", arb_failure_payload()).prop_map(|(uid, failure)| {
                HistoryEventKind::WorkflowExecutionUpdateRejected {
                    update_id: uid,
                    failure,
                    rejected_request_message_id: "msg".to_string(),
                    rejected_request_sequencing_event_id: 3,
                }
            }),
        ]
    }

    fn arb_history_event() -> impl Strategy<Value = HistoryEvent> {
        (1i64..10000, arb_timestamp(), arb_history_event_kind()).prop_map(|(eid, ts, kind)| {
            HistoryEvent {
                event_id: eid,
                happened_at: ts,
                kind,
            }
        })
    }

    // Feature: history-delivery, Property 1:
    // History serialization round-trip
    // **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_history_serialization_round_trip(event in arb_history_event()) {
            let proto = history_event_to_proto(&event);
            let bytes = proto.encode_to_vec();
            let decoded = history::HistoryEvent::decode(&bytes[..])
                .expect("decode should succeed");
            prop_assert_eq!(proto.event_id, decoded.event_id);
            prop_assert_eq!(proto.event_time, decoded.event_time);
            prop_assert_eq!(proto.event_type, decoded.event_type);
            prop_assert_eq!(proto.attributes, decoded.attributes);
        }
    }

    #[test]
    fn empty_history_produces_valid_proto() {
        let bytes = serialize_history(&[]);
        let decoded = history::History::decode(&bytes[..]).expect("decode should succeed");
        assert!(decoded.events.is_empty());
    }

    #[test]
    fn workflow_task_completed_serializes_metering_metadata() {
        let metering = proto_common::MeteringMetadata {
            nonfirst_local_activity_execution_attempts: 4,
        };
        let event = HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowTaskCompleted {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 2,
                started_event_id: 3,
                identity: WorkerIdentity("worker".to_string()),
                sdk_metadata: None,
                metering_metadata: Some(metering.encode_to_vec()),
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
            },
        };

        match history_event_to_proto(&event).attributes.unwrap() {
            Attributes::WorkflowTaskCompletedEventAttributes(attrs) => {
                assert_eq!(attrs.metering_metadata, Some(metering));
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn workflow_started_golden_example() {
        let event = HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("MyWorkflow".to_string()),
                task_queue: TaskQueueName("default".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "req-1".to_string(),
                identity: "client".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: None,
                continued_failure: None,
                last_completion_result: None,
                cron_schedule: None,
                versioning_info: None,
                worker_deployment_name: None,
                priority: None,
            },
        };
        let proto = history_event_to_proto(&event);
        assert_eq!(proto.event_id, 1);
        assert_eq!(
            proto.event_type,
            tokeira_proto::enums::EventType::WorkflowExecutionStarted as i32
        );
        match proto.attributes.unwrap() {
            Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                assert_eq!(attrs.workflow_type.unwrap().name, "MyWorkflow");
                let timeout = attrs.workflow_task_timeout.unwrap();
                assert_eq!(timeout.seconds, 10);
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn workflow_started_serializes_start_metadata_fields() {
        let mut callback_header = BTreeMap::new();
        callback_header.insert("x-callback".to_string(), "value".to_string());
        let event = HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("MyWorkflow".to_string()),
                task_queue: TaskQueueName("default".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: Some(time::Duration::seconds(9)),
                completion_callbacks: vec![CompletionCallback {
                    spec: CallbackSpec::Nexus {
                        url: "https://callback.example/run".to_string(),
                        header: callback_header,
                    },
                    links: vec![Link::BatchJob {
                        job_id: "batch-1".to_string(),
                    }],
                    trigger: CallbackTrigger::WorkflowClosed,
                    registration_time: None,
                    state: CallbackState::Standby,
                    attempt: 0,
                    last_attempt_failure: None,
                }],
                user_metadata: Some(UserMetadata {
                    summary: Some(Payload::new(b"summary".to_vec())),
                    details: Some(Payload::new(b"details".to_vec())),
                }),
                links: vec![Link::Activity {
                    namespace: "default".to_string(),
                    activity_id: "activity-1".to_string(),
                    run_id: "run-1".to_string(),
                }],
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "req-1".to_string(),
                identity: "client".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: None,
                continued_failure: None,
                last_completion_result: None,
                cron_schedule: None,
                versioning_info: None,
                worker_deployment_name: None,
                priority: Some(Priority {
                    priority_key: 2,
                    fairness_key: "tenant-a".to_string(),
                    fairness_weight: 1.5,
                }),
            },
        };

        let proto = history_event_to_proto(&event);
        assert_eq!(
            proto
                .user_metadata
                .as_ref()
                .and_then(|metadata| metadata.summary.as_ref())
                .map(|payload| payload.data.as_slice()),
            Some(&b"summary"[..])
        );
        assert_eq!(proto.links.len(), 1);
        match proto.attributes.unwrap() {
            Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                assert_eq!(attrs.first_workflow_task_backoff.unwrap().seconds, 9);
                assert_eq!(attrs.completion_callbacks.len(), 1);
                assert_eq!(
                    attrs.completion_callbacks[0]
                        .links
                        .first()
                        .and_then(|link| link.variant.as_ref())
                        .is_some(),
                    true
                );
                let priority = attrs.priority.unwrap();
                assert_eq!(priority.priority_key, 2);
                assert_eq!(priority.fairness_key, "tenant-a");
                assert_eq!(priority.fairness_weight, 1.5);
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn cron_schedule_field_set() {
        let event = HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("ScheduledWorkflow".to_string()),
                task_queue: TaskQueueName("default".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "req-1".to_string(),
                identity: "client".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: None,
                continued_failure: None,
                last_completion_result: None,
                cron_schedule: Some("schedule-a".to_string()),
                versioning_info: None,
                worker_deployment_name: None,
                priority: None,
            },
        };

        match history_event_to_proto(&event).attributes.unwrap() {
            Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                assert_eq!(attrs.cron_schedule, "schedule-a");
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn non_schedule_start_empty_cron() {
        let event = HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("NormalWorkflow".to_string()),
                task_queue: TaskQueueName("default".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "req-1".to_string(),
                identity: "client".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                parent_workflow_id: None,
                parent_run_id: None,
                parent_namespace_id: None,
                parent_initiated_event_id: 0,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id: None,
                continued_failure: None,
                last_completion_result: None,
                cron_schedule: None,
                versioning_info: None,
                worker_deployment_name: None,
                priority: None,
            },
        };

        match history_event_to_proto(&event).attributes.unwrap() {
            Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                assert!(attrs.cron_schedule.is_empty());
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn activity_scheduled_golden_example() {
        let event = HistoryEvent {
            event_id: 5,
            happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
            kind: HistoryEventKind::ActivityTaskScheduled {
                workflow_task_completed_event_id: 4,
                activity_id: "act-1".to_string(),
                activity_type: "activity-type".to_string(),
                task_queue: TaskQueueName("q".to_string()),
                input: Payloads::default(),
                header: None,
                retry_policy: None,
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            },
        };
        let proto = history_event_to_proto(&event);
        assert_eq!(
            proto.event_type,
            tokeira_proto::enums::EventType::ActivityTaskScheduled as i32
        );
        match proto.attributes.unwrap() {
            Attributes::ActivityTaskScheduledEventAttributes(attrs) => {
                assert_eq!(attrs.activity_id, "act-1");
                let timeout = attrs.schedule_to_close_timeout.unwrap();
                assert_eq!(timeout.seconds, 30);
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn timer_started_golden_example() {
        let fire_at = OffsetDateTime::from_unix_timestamp(5000).unwrap();
        let event = HistoryEvent {
            event_id: 10,
            happened_at: OffsetDateTime::from_unix_timestamp(4000).unwrap(),
            kind: HistoryEventKind::TimerStarted {
                workflow_task_completed_event_id: 4,
                timer_id: "t1".to_string(),
                fire_at,
            },
        };
        let proto = history_event_to_proto(&event);
        assert_eq!(
            proto.event_type,
            tokeira_proto::enums::EventType::TimerStarted as i32
        );
        match proto.attributes.unwrap() {
            Attributes::TimerStartedEventAttributes(attrs) => {
                assert_eq!(attrs.timer_id, "t1");
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn child_workflow_golden_example() {
        let event = HistoryEvent {
            event_id: 20,
            happened_at: OffsetDateTime::from_unix_timestamp(3000).unwrap(),
            kind: HistoryEventKind::StartChildWorkflowExecutionInitiated {
                workflow_task_completed_event_id: 4,
                child_workflow_id: WorkflowId("child-1".to_string()),
                workflow_type: WorkflowType("ChildWf".to_string()),
                task_queue: TaskQueueName("child-q".to_string()),
                input: Payloads::default(),
                namespace_id: NamespaceId(uuid::Uuid::nil()),
                namespace: None,
                header: None,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                retry_policy: None,
                cron_schedule: None,
                parent_close_policy: ParentClosePolicy::Terminate,
            },
        };
        let proto = history_event_to_proto(&event);
        assert_eq!(
            proto.event_type,
            tokeira_proto::enums::EventType::StartChildWorkflowExecutionInitiated as i32
        );
        match proto.attributes.unwrap() {
            Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(attrs) => {
                assert_eq!(attrs.workflow_id, "child-1");
                assert_eq!(attrs.workflow_type.unwrap().name, "ChildWf");
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    fn make_failure_payload(failure: &proto_failure::Failure) -> Payload {
        failure_to_payload(failure)
    }

    fn arb_proto_failure() -> impl Strategy<Value = proto_failure::Failure> {
        (
            "[a-z ]{0,20}",
            "[a-z]{0,10}",
            "[a-z\n]{0,30}",
            prop_oneof![
                Just(None),
                "[a-z]{0,10}".prop_map(|t| Some(
                    proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                        proto_failure::ApplicationFailureInfo {
                            r#type: t,
                            non_retryable: false,
                            ..Default::default()
                        },
                    )
                )),
            ],
        )
            .prop_map(
                |(msg, source, stack, failure_info)| proto_failure::Failure {
                    message: msg,
                    source,
                    stack_trace: stack,
                    failure_info,
                    ..Default::default()
                },
            )
    }

    // ── Property 2: WorkflowExecutionFailed preserves failure ──
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_workflow_execution_failed_preserves_failure(
            failure in arb_proto_failure(),
            retry_state in arb_retry_state(),
            attempt in 0u32..5,
        ) {
            let payload = make_failure_payload(&failure);
            let event = HistoryEvent {
                event_id: 1,
                happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
                kind: HistoryEventKind::WorkflowExecutionFailed {
                    workflow_task_completed_event_id: 4,
                    failure: payload,
                    retry_state,
                    attempt,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionFailedEventAttributes(attrs) => {
                    let proto_failure = attrs.failure.unwrap();
                    prop_assert_eq!(proto_failure.message, failure.message);
                    prop_assert_eq!(
                        proto_failure.failure_info.is_some(),
                        failure.failure_info.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        // ── Property 3: ActivityTaskFailed preserves failure ──
        #[test]
        fn prop_activity_task_failed_preserves_failure(
            failure in arb_proto_failure(),
        ) {
            let payload = make_failure_payload(&failure);
            let event = HistoryEvent {
                event_id: 7,
                happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
                kind: HistoryEventKind::ActivityTaskFailed {
                    activity_id: "act-1".to_string(),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: Some(WorkerIdentity("worker".to_string())),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure: payload,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::ActivityTaskFailedEventAttributes(attrs) => {
                    let proto_failure = attrs.failure.unwrap();
                    prop_assert_eq!(proto_failure.message, failure.message);
                    prop_assert_eq!(
                        proto_failure.failure_info.is_some(),
                        failure.failure_info.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        // ── Property 4: ChildWorkflowExecutionFailed preserves failure ──
        #[test]
        fn prop_child_workflow_failed_preserves_failure(
            failure in arb_proto_failure(),
        ) {
            let payload = make_failure_payload(&failure);
            let event = HistoryEvent {
                event_id: 15,
                happened_at: OffsetDateTime::from_unix_timestamp(3000).unwrap(),
                kind: HistoryEventKind::ChildWorkflowExecutionFailed {
                    child_workflow_id: WorkflowId("child-1".to_string()),
                    namespace_id: NamespaceId(uuid::Uuid::nil()),
                    namespace: None,
                    child_run_id: Some(RunId::new()),
                    workflow_type: WorkflowType("ChildWf".to_string()),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure: payload,
                    initiated_event_id: 10,
                    started_event_id: 11,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::ChildWorkflowExecutionFailedEventAttributes(attrs) => {
                    let proto_failure = attrs.failure.unwrap();
                    prop_assert_eq!(proto_failure.message, failure.message);
                    prop_assert_eq!(
                        proto_failure.failure_info.is_some(),
                        failure.failure_info.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        // ── Property 5: WorkflowTaskFailed preserves failure ──
        #[test]
        fn prop_workflow_task_failed_preserves_failure(
            failure in arb_proto_failure(),
        ) {
            let payload = make_failure_payload(&failure);
            let event = HistoryEvent {
                event_id: 8,
                happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
                kind: HistoryEventKind::WorkflowTaskFailed {
                    logical_seq: LogicalTaskSeq(1),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    failure_cause: tokeira_kernel::command::WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                    failure_details: Some(payload),
                    identity: WorkerIdentity("w".to_string()),
                    base_run_id: None,
                    new_run_id: None,
                    fork_event_version: None,
                    fork_event_id: None,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowTaskFailedEventAttributes(attrs) => {
                    let proto_failure = attrs.failure.unwrap();
                    prop_assert_eq!(proto_failure.message, failure.message);
                    prop_assert_eq!(
                        proto_failure.failure_info.is_some(),
                        failure.failure_info.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        // ── Property 6: All failure-bearing events preserve failure_info ──
        #[test]
        fn prop_all_failure_events_preserve_failure_info(
            failure in arb_proto_failure(),
            variant in 0u8..5,
        ) {
            let payload = make_failure_payload(&failure);
            let kind = match variant {
                0 => HistoryEventKind::WorkflowExecutionFailed {
                    workflow_task_completed_event_id: 4,
                    failure: payload,
                    retry_state: RetryState::InProgress,
                    attempt: 1,
                },
                1 => HistoryEventKind::ActivityTaskFailed {
                    activity_id: "act-1".to_string(),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: Some(WorkerIdentity("worker".to_string())),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure: payload,
                },
                2 => HistoryEventKind::ChildWorkflowExecutionFailed {
                    child_workflow_id: WorkflowId("child-1".to_string()),
                    namespace_id: NamespaceId(uuid::Uuid::nil()),
                    namespace: None,
                    child_run_id: Some(RunId::new()),
                    workflow_type: WorkflowType("ChildWf".to_string()),
                    retry_state: RetryState::RetryPolicyNotSet,
                    failure: payload,
                    initiated_event_id: 10,
                    started_event_id: 11,
                },
                3 => HistoryEventKind::NexusOperationFailed {
                    operation_id: "op-1".to_string(),
                    scheduled_event_id: 10,
                    failure: payload,
                },
                _ => HistoryEventKind::WorkflowExecutionUpdateRejected {
                    update_id: "update-1".to_string(),
                    failure: payload,
                    rejected_request_message_id: "msg".to_string(),
                    rejected_request_sequencing_event_id: 3,
                },
            };
            let event = HistoryEvent {
                event_id: 1,
                happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
                kind,
            };
            let proto = history_event_to_proto(&event);
            let has_failure_info = failure.failure_info.is_some();
            let proto_has_failure_info = match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionFailedEventAttributes(a) => {
                    a.failure.unwrap().failure_info.is_some()
                }
                Attributes::ActivityTaskFailedEventAttributes(a) => {
                    a.failure.unwrap().failure_info.is_some()
                }
                Attributes::ChildWorkflowExecutionFailedEventAttributes(a) => {
                    a.failure.unwrap().failure_info.is_some()
                }
                Attributes::NexusOperationFailedEventAttributes(a) => {
                    a.failure.unwrap().failure_info.is_some()
                }
                Attributes::WorkflowExecutionUpdateRejectedEventAttributes(a) => {
                    a.failure.unwrap().failure_info.is_some()
                }
                other => panic!("unexpected attributes: {other:?}"),
            };
            prop_assert_eq!(has_failure_info, proto_has_failure_info);
        }
    }

    // ── Task 10: Golden unit tests ──

    #[test]
    fn golden_workflow_execution_failed_with_application_failure_info() {
        let failure = proto_failure::Failure {
            message: "something went wrong".to_string(),
            stack_trace: "at main.rs:42".to_string(),
            failure_info: Some(proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                proto_failure::ApplicationFailureInfo {
                    r#type: "MyAppError".to_string(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let event = HistoryEvent {
            event_id: 5,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionFailed {
                workflow_task_completed_event_id: 4,
                failure: make_failure_payload(&failure),
                retry_state: RetryState::NonRetryableFailure,
                attempt: 1,
            },
        };
        let proto = history_event_to_proto(&event);
        match proto.attributes.unwrap() {
            Attributes::WorkflowExecutionFailedEventAttributes(attrs) => {
                let f = attrs.failure.unwrap();
                assert_eq!(f.message, "something went wrong");
                assert_eq!(f.stack_trace, "at main.rs:42");
                match f.failure_info.unwrap() {
                    proto_failure::failure::FailureInfo::ApplicationFailureInfo(info) => {
                        assert_eq!(info.r#type, "MyAppError");
                        assert!(info.non_retryable);
                    }
                    other => panic!("unexpected failure_info: {other:?}"),
                }
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn golden_activity_task_failed_with_cause_chain() {
        let cause = proto_failure::Failure {
            message: "root cause".to_string(),
            failure_info: Some(proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                proto_failure::ApplicationFailureInfo {
                    r#type: "RootError".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let failure = proto_failure::Failure {
            message: "activity failed".to_string(),
            cause: Some(Box::new(cause)),
            failure_info: Some(proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                proto_failure::ApplicationFailureInfo {
                    r#type: "ActivityError".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let event = HistoryEvent {
            event_id: 7,
            happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
            kind: HistoryEventKind::ActivityTaskFailed {
                activity_id: "act-1".to_string(),
                scheduled_event_id: 5,
                started_event_id: 6,
                identity: Some(WorkerIdentity("worker".to_string())),
                retry_state: RetryState::RetryPolicyNotSet,
                failure: make_failure_payload(&failure),
            },
        };
        let proto = history_event_to_proto(&event);
        match proto.attributes.unwrap() {
            Attributes::ActivityTaskFailedEventAttributes(attrs) => {
                let f = attrs.failure.unwrap();
                assert_eq!(f.message, "activity failed");
                assert!(f.failure_info.is_some());
                let c = f.cause.unwrap();
                assert_eq!(c.message, "root cause");
                assert!(c.failure_info.is_some());
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn golden_workflow_task_failed_with_failure_details() {
        let failure = proto_failure::Failure {
            message: "non-determinism detected".to_string(),
            failure_info: Some(proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                proto_failure::ApplicationFailureInfo {
                    r#type: "NonDeterminismError".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let event = HistoryEvent {
            event_id: 8,
            happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
            kind: HistoryEventKind::WorkflowTaskFailed {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 5,
                started_event_id: 6,
                failure_cause:
                    tokeira_kernel::command::WorkflowTaskFailedCause::NonDeterminismError,
                failure_details: Some(make_failure_payload(&failure)),
                identity: WorkerIdentity("w".to_string()),
                base_run_id: None,
                new_run_id: None,
                fork_event_version: None,
                fork_event_id: None,
            },
        };
        let proto = history_event_to_proto(&event);
        match proto.attributes.unwrap() {
            Attributes::WorkflowTaskFailedEventAttributes(attrs) => {
                let f = attrs.failure.unwrap();
                assert_eq!(f.message, "non-determinism detected");
                assert!(f.failure_info.is_some());
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn golden_marker_recorded_with_failure() {
        let failure = proto_failure::Failure {
            message: "marker failure".to_string(),
            failure_info: Some(proto_failure::failure::FailureInfo::ApplicationFailureInfo(
                proto_failure::ApplicationFailureInfo {
                    r#type: "MarkerError".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let event = HistoryEvent {
            event_id: 9,
            happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
            kind: HistoryEventKind::MarkerRecorded {
                workflow_task_completed_event_id: 4,
                marker_name: "test-marker".to_string(),
                details: Default::default(),
                failure: Some(make_failure_payload(&failure)),
                header: None,
            },
        };
        let proto = history_event_to_proto(&event);
        match proto.attributes.unwrap() {
            Attributes::MarkerRecordedEventAttributes(attrs) => {
                let f = attrs.failure.unwrap();
                assert_eq!(f.message, "marker failure");
                assert!(f.failure_info.is_some());
            }
            other => panic!("unexpected attributes: {other:?}"),
        }
    }

    #[test]
    fn golden_corrupted_payload_produces_fallback_failure() {
        let corrupted = Payload {
            data: b"not valid proto bytes".to_vec(),
            metadata: Default::default(),
        };
        let decoded = payload_to_failure(&corrupted);
        assert_eq!(decoded.message, "not valid proto bytes");
        assert!(decoded.failure_info.is_none());
    }

    fn arb_namespace_id() -> impl Strategy<Value = NamespaceId> {
        any::<u128>().prop_map(|v| NamespaceId(Uuid::from_u128(v)))
    }

    fn arb_workflow_id() -> impl Strategy<Value = WorkflowId> {
        "[a-z]{1,10}".prop_map(WorkflowId)
    }

    fn arb_opt_failure_payload() -> impl Strategy<Value = Option<Payload>> {
        proptest::option::of(arb_failure_payload())
    }

    fn arb_opt_payloads() -> impl Strategy<Value = Option<Payloads>> {
        proptest::option::of(arb_payloads())
    }

    fn make_started_event(
        parent_workflow_id: Option<WorkflowId>,
        parent_run_id: Option<RunId>,
        parent_namespace_id: Option<NamespaceId>,
        parent_initiated_event_id: i64,
        original_execution_run_id: Option<RunId>,
        continued_failure: Option<Payload>,
        last_completion_result: Option<Payloads>,
    ) -> HistoryEvent {
        HistoryEvent {
            event_id: 1,
            happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
            kind: HistoryEventKind::WorkflowExecutionStarted {
                workflow_type: WorkflowType("W".to_string()),
                task_queue: TaskQueueName("q".to_string()),
                input: Payloads::default(),
                header: None,
                workflow_start_delay: None,
                completion_callbacks: Vec::new(),
                user_metadata: None,
                links: Vec::new(),
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "r".to_string(),
                identity: "client".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
                parent_workflow_id,
                parent_run_id,
                parent_namespace_id,
                parent_initiated_event_id,
                root_workflow_id: None,
                root_run_id: None,
                original_execution_run_id,
                continued_failure,
                last_completion_result,
                cron_schedule: None,
                versioning_info: None,
                worker_deployment_name: None,
                priority: None,
            },
        }
    }

    // ── 7.1: WorkflowExecutionStarted parent metadata serialization (Property 1, 2) ──
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Property 1: when parent fields are present, proto reflects them
        #[test]
        fn prop_started_parent_metadata_present(
            wid in arb_workflow_id(),
            rid in arb_run_id(),
            nsid in arb_namespace_id(),
            initiated_eid in 1i64..1000,
        ) {
            let event = make_started_event(
                Some(wid.clone()), Some(rid), Some(nsid), initiated_eid,
                None, None, None,
            );
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                    let exec = attrs.parent_workflow_execution.as_ref().unwrap();
                    prop_assert_eq!(&exec.workflow_id, &wid.0);
                    prop_assert_eq!(&exec.run_id, &rid.0.to_string());
                    prop_assert_eq!(
                        &attrs.parent_workflow_namespace_id,
                        &nsid.0.to_string()
                    );
                    prop_assert_eq!(attrs.parent_initiated_event_id, initiated_eid);
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        // Property 2: when parent_workflow_id is None, proto has no parent
        #[test]
        fn prop_started_parent_metadata_absent(
            orig_rid in arb_opt_run_id(),
        ) {
            let event = make_started_event(
                None, None, None, 0,
                orig_rid, None, None,
            );
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                    prop_assert!(attrs.parent_workflow_execution.is_none());
                    prop_assert!(attrs.parent_workflow_namespace_id.is_empty());
                    prop_assert_eq!(attrs.parent_initiated_event_id, 0);
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }
    }

    // ── 7.2: WorkflowExecutionStarted chain fields serialization (Property 3, 4, 5) ──
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_started_chain_fields_serialization(
            orig_rid in arb_opt_run_id(),
            cont_failure in arb_opt_failure_payload(),
            last_result in arb_opt_payloads(),
        ) {
            let event = make_started_event(
                None, None, None, 0,
                orig_rid.clone(), cont_failure.clone(), last_result.clone(),
            );
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionStartedEventAttributes(attrs) => {
                    if let Some(ref rid) = orig_rid {
                        prop_assert_eq!(
                            &attrs.original_execution_run_id,
                            &rid.0.to_string()
                        );
                    } else {
                        prop_assert!(attrs.original_execution_run_id.is_empty());
                    }
                    prop_assert_eq!(
                        attrs.continued_failure.is_some(),
                        cont_failure.is_some()
                    );
                    prop_assert_eq!(
                        attrs.last_completion_result.is_some(),
                        last_result.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }
    }

    // ── 7.3: WorkflowExecutionContinuedAsNew enriched fields serialization (Property 6) ──
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_continued_as_new_enriched_fields(
            failure in arb_opt_failure_payload(),
            last_result in arb_opt_payloads(),
        ) {
            let event = HistoryEvent {
                event_id: 1,
                happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
                kind: HistoryEventKind::WorkflowExecutionContinuedAsNew {
                    workflow_task_completed_event_id: 4,
                    new_run_id: RunId(Uuid::from_u128(42)),
                    workflow_type: WorkflowType("W".to_string()),
                    task_queue: TaskQueueName("q".to_string()),
                    input: Payloads::default(),
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: time::Duration::seconds(10),
                    retry_policy: None,
                    initiator: ContinueAsNewInitiator::Workflow,
                    failure: failure.clone(),
                    last_completion_result: last_result.clone(),
                    backoff_start_interval: None,
                    cron_schedule: None,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs) => {
                    // Workflow variant maps to proto value 1
                    prop_assert!(attrs.initiator != 0,
                        "initiator should be non-zero for Workflow variant");
                    prop_assert_eq!(
                        attrs.failure.is_some(),
                        failure.is_some()
                    );
                    prop_assert_eq!(
                        attrs.last_completion_result.is_some(),
                        last_result.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }
    }

    // ── 7.4: Signal/cancel-external control field serialization (Property 7) ──
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_signal_external_control_field(
            control in "[a-z0-9]{0,20}",
            wid in arb_workflow_id(),
            rid in arb_opt_run_id(),
        ) {
            let event = HistoryEvent {
                event_id: 1,
                happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
                kind: HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
                    workflow_task_completed_event_id: 4,
                    namespace_id: NamespaceId(uuid::Uuid::nil()),
                    namespace: None,
                    target_workflow_id: wid,
                    target_run_id: rid,
                    signal_name: "sig".to_string(),
                    input: Payloads::default(),
                    header: None,
                    control: control.clone(),
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(
                    attrs,
                ) => {
                    prop_assert_eq!(&attrs.control, &control);
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }

        #[test]
        fn prop_cancel_external_control_field(
            control in "[a-z0-9]{0,20}",
            wid in arb_workflow_id(),
            rid in arb_opt_run_id(),
        ) {
            let event = HistoryEvent {
                event_id: 1,
                happened_at: OffsetDateTime::from_unix_timestamp(1000).unwrap(),
                kind: HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
                    workflow_task_completed_event_id: 4,
                    namespace_id: NamespaceId(uuid::Uuid::nil()),
                    namespace: None,
                    target_workflow_id: wid,
                    target_run_id: rid,
                    control: control.clone(),
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
                    attrs,
                ) => {
                    prop_assert_eq!(&attrs.control, &control);
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }
    }

    // ── 7.5: ActivityTaskScheduled timeout completeness (Property 8) ──
    // The existing `prop_history_serialization_round_trip` generates
    // `ActivityTaskScheduled` events via `arb_history_event_kind` which
    // already produces all four timeout fields (schedule_to_close,
    // schedule_to_start, start_to_close, heartbeat) through
    // `arb_opt_duration()`. The round-trip test encodes and decodes the
    // proto, asserting full attribute equality. This covers Property 8.
    //
    // The following test adds an explicit assertion that all four timeout
    // fields survive serialization.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_activity_scheduled_timeout_completeness(
            s2c in arb_opt_duration(),
            s2s in arb_opt_duration(),
            stc in arb_opt_duration(),
            hb in arb_opt_duration(),
        ) {
            let event = HistoryEvent {
                event_id: 5,
                happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
                kind: HistoryEventKind::ActivityTaskScheduled {
                    workflow_task_completed_event_id: 4,
                    activity_id: "act-1".to_string(),
                    activity_type: "at".to_string(),
                    task_queue: TaskQueueName("q".to_string()),
                    input: Payloads::default(),
                    header: None,
                    retry_policy: None,
                    schedule_to_close_timeout: s2c,
                    schedule_to_start_timeout: s2s,
                    start_to_close_timeout: stc,
                    heartbeat_timeout: hb,
                },
            };
            let proto = history_event_to_proto(&event);
            match proto.attributes.unwrap() {
                Attributes::ActivityTaskScheduledEventAttributes(attrs) => {
                    prop_assert_eq!(
                        attrs.schedule_to_close_timeout.is_some(),
                        s2c.is_some()
                    );
                    prop_assert_eq!(
                        attrs.schedule_to_start_timeout.is_some(),
                        s2s.is_some()
                    );
                    prop_assert_eq!(
                        attrs.start_to_close_timeout.is_some(),
                        stc.is_some()
                    );
                    prop_assert_eq!(
                        attrs.heartbeat_timeout.is_some(),
                        hb.is_some()
                    );
                }
                other => panic!("unexpected attributes: {other:?}"),
            }
        }
    }
}
