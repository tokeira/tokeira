//! Converts kernel `HistoryEvent` values into
//! proto-encoded bytes for the Temporal wire format.

use prost::Message;
use tokeira_kernel::event::{HistoryEvent, HistoryEventKind};
use tokeira_proto::public::temporal::api::update::v1 as proto_update;
use tokeira_proto::{
    conversions::common::{
        headers_from_domain, memo_from_domain, payload_from_domain, payloads_from_domain,
        search_attributes_from_domain, task_queue_from_domain, to_opt_proto_duration,
        to_proto_duration, to_proto_timestamp,
    },
    history,
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
        ..Default::default()
    }
}

fn opt_run_id(r: &Option<tokeira_types::RunId>) -> String {
    r.as_ref().map(|id| id.0.to_string()).unwrap_or_default()
}

fn opt_string(s: &Option<String>) -> String {
    s.clone().unwrap_or_default()
}

fn event_type_for_kind(kind: &HistoryEventKind) -> i32 {
    use tokeira_proto::enums::EventType as E;
    let et = match kind {
        HistoryEventKind::WorkflowExecutionStarted { .. } => E::WorkflowExecutionStarted,
        HistoryEventKind::WorkflowExecutionCompleted { .. } => {
            E::WorkflowExecutionCompleted
        }
        HistoryEventKind::WorkflowExecutionFailed { .. } => E::WorkflowExecutionFailed,
        HistoryEventKind::WorkflowExecutionTimedOut { .. } => {
            E::WorkflowExecutionTimedOut
        }
        HistoryEventKind::WorkflowExecutionCancelRequested { .. } => {
            E::WorkflowExecutionCancelRequested
        }
        HistoryEventKind::WorkflowExecutionCanceled => E::WorkflowExecutionCanceled,
        HistoryEventKind::WorkflowExecutionTerminated { .. } => {
            E::WorkflowExecutionTerminated
        }
        HistoryEventKind::WorkflowExecutionContinuedAsNew { .. } => {
            E::WorkflowExecutionContinuedAsNew
        }
        HistoryEventKind::WorkflowExecutionSignaled { .. } => {
            E::WorkflowExecutionSignaled
        }
        // Paused/Unpaused are Tokeira-specific; no upstream EventType yet.
        HistoryEventKind::WorkflowExecutionPaused { .. } => E::Unspecified,
        HistoryEventKind::WorkflowExecutionUnpaused { .. } => E::Unspecified,
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
        HistoryEventKind::ActivityTaskCancelRequested { .. } => {
            E::ActivityTaskCancelRequested
        }
        HistoryEventKind::TimerStarted { .. } => E::TimerStarted,
        HistoryEventKind::TimerFired { .. } => E::TimerFired,
        HistoryEventKind::TimerCanceled { .. } => E::TimerCanceled,
        HistoryEventKind::MarkerRecorded { .. } => E::MarkerRecorded,
        HistoryEventKind::StartChildWorkflowExecutionInitiated { .. } => {
            E::StartChildWorkflowExecutionInitiated
        }
        HistoryEventKind::ChildWorkflowExecutionStarted { .. } => {
            E::ChildWorkflowExecutionStarted
        }
        HistoryEventKind::StartChildWorkflowExecutionFailed { .. } => {
            E::StartChildWorkflowExecutionFailed
        }
        HistoryEventKind::ChildWorkflowExecutionCompleted { .. } => {
            E::ChildWorkflowExecutionCompleted
        }
        HistoryEventKind::ChildWorkflowExecutionFailed { .. } => {
            E::ChildWorkflowExecutionFailed
        }
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
        HistoryEventKind::NexusOperationCancelRequested { .. } => {
            E::NexusOperationCancelRequested
        }
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
use tokeira_kernel::command::{
    RetryState, WorkflowTaskFailedCause, WorkflowTaskTimeoutType,
};
use tokeira_kernel::state::ParentClosePolicy;
use tokeira_proto::public::temporal::api::common::v1 as proto_common;
use tokeira_proto::public::temporal::api::failure::v1 as proto_failure;

#[allow(clippy::too_many_lines)]
fn attributes_for_kind(event: &HistoryEvent) -> Attributes {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted {
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            request_id,
            continued_execution_run_id,
            first_execution_run_id,
            retry_policy,
            attempt,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
        } => Attributes::WorkflowExecutionStartedEventAttributes(
            history::WorkflowExecutionStartedEventAttributes {
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(task_queue_from_domain(task_queue)),
                input: Some(payloads_from_domain(input)),
                memo: Some(memo_from_domain(memo)),
                search_attributes: Some(search_attributes_from_domain(search_attributes)),
                continued_execution_run_id: opt_run_id(continued_execution_run_id),
                first_execution_run_id: opt_run_id(first_execution_run_id),
                retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
                attempt: *attempt as i32,
                workflow_execution_timeout: to_opt_proto_duration(
                    *workflow_execution_timeout,
                ),
                workflow_run_timeout: to_opt_proto_duration(*workflow_run_timeout),
                workflow_task_timeout: Some(to_proto_duration(*workflow_task_timeout)),
                identity: request_id.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionCompleted { result } => {
            Attributes::WorkflowExecutionCompletedEventAttributes(
                history::WorkflowExecutionCompletedEventAttributes {
                    result: Some(payloads_from_domain(result)),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionFailed {
            message,
            details,
            retry_state,
            attempt: _,
        } => {
            let failure = Some(proto_failure::Failure {
                message: message.clone(),
                encoded_attributes: details.as_ref().map(payload_from_domain),
                ..Default::default()
            });
            Attributes::WorkflowExecutionFailedEventAttributes(
                history::WorkflowExecutionFailedEventAttributes {
                    failure,
                    retry_state: retry_state_i32(retry_state),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionTimedOut {
            timeout_type: _,
            retry_state,
        } => Attributes::WorkflowExecutionTimedOutEventAttributes(
            history::WorkflowExecutionTimedOutEventAttributes {
                retry_state: retry_state_i32(retry_state),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionCancelRequested {
            reason,
            external_workflow_execution,
            request_id: _,
        } => {
            let ext_exec = external_workflow_execution.as_ref().map(|e| {
                proto_common::WorkflowExecution {
                    workflow_id: e.workflow_id.0.clone(),
                    run_id: e.run_id.0.to_string(),
                }
            });
            Attributes::WorkflowExecutionCancelRequestedEventAttributes(
                history::WorkflowExecutionCancelRequestedEventAttributes {
                    cause: reason.clone(),
                    external_workflow_execution: ext_exec,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionCanceled => {
            Attributes::WorkflowExecutionCanceledEventAttributes(
                history::WorkflowExecutionCanceledEventAttributes {
                    ..Default::default()
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
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowExecutionSignaled {
            signal_name,
            input,
            request_id: _,
            identity,
        } => Attributes::WorkflowExecutionSignaledEventAttributes(
            history::WorkflowExecutionSignaledEventAttributes {
                signal_name: signal_name.clone(),
                input: Some(payloads_from_domain(input)),
                identity: opt_string(identity),
                ..Default::default()
            },
        ),
        // Paused/Unpaused are Tokeira-specific; no upstream proto type yet.
        // Map to WorkflowExecutionCanceledEventAttributes as a placeholder.
        HistoryEventKind::WorkflowExecutionPaused { .. } => {
            Attributes::WorkflowExecutionCanceledEventAttributes(
                history::WorkflowExecutionCanceledEventAttributes {
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionUnpaused { .. } => {
            Attributes::WorkflowExecutionCanceledEventAttributes(
                history::WorkflowExecutionCanceledEventAttributes {
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowTaskScheduled { logical_seq: _ } => {
            Attributes::WorkflowTaskScheduledEventAttributes(
                history::WorkflowTaskScheduledEventAttributes {
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowTaskStarted {
            logical_seq: _,
            scheduled_event_id,
            attempt: _,
            identity,
        } => Attributes::WorkflowTaskStartedEventAttributes(
            history::WorkflowTaskStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                identity: identity.0.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::WorkflowTaskCompleted {
            logical_seq: _,
            scheduled_event_id,
            started_event_id,
            identity,
        } => Attributes::WorkflowTaskCompletedEventAttributes(
            history::WorkflowTaskCompletedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                identity: identity.0.clone(),
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
            let failure = failure_details.as_ref().map(|_p| proto_failure::Failure {
                message: String::new(),
                ..Default::default()
            });
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
                schedule_to_close_timeout: to_opt_proto_duration(
                    *schedule_to_close_timeout,
                ),
                schedule_to_start_timeout: to_opt_proto_duration(
                    *schedule_to_start_timeout,
                ),
                start_to_close_timeout: to_opt_proto_duration(*start_to_close_timeout),
                heartbeat_timeout: to_opt_proto_duration(*heartbeat_timeout),
                retry_policy: retry_policy.as_ref().map(retry_policy_to_proto),
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskStarted {
            activity_id: _,
            scheduled_event_id,
            attempt,
            identity,
        } => Attributes::ActivityTaskStartedEventAttributes(
            history::ActivityTaskStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                identity: identity.0.clone(),
                attempt: *attempt as i32,
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCompleted {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            result,
        } => Attributes::ActivityTaskCompletedEventAttributes(
            history::ActivityTaskCompletedEventAttributes {
                result: Some(payloads_from_domain(result)),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskFailed {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            message,
        } => {
            let failure = Some(proto_failure::Failure {
                message: message.clone(),
                ..Default::default()
            });
            Attributes::ActivityTaskFailedEventAttributes(
                history::ActivityTaskFailedEventAttributes {
                    failure,
                    scheduled_event_id: *scheduled_event_id,
                    started_event_id: *started_event_id,
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::ActivityTaskTimedOut {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            timeout_type,
        } => Attributes::ActivityTaskTimedOutEventAttributes(
            history::ActivityTaskTimedOutEventAttributes {
                failure: Some(proto_failure::Failure {
                    message: format!("activity timed out: {timeout_type}"),
                    failure_info: Some(
                        proto_failure::failure::FailureInfo::TimeoutFailureInfo(
                            proto_failure::TimeoutFailureInfo {
                                timeout_type: activity_timeout_type_i32(timeout_type),
                                ..Default::default()
                            },
                        ),
                    ),
                    ..Default::default()
                }),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCanceled {
            activity_id: _,
            scheduled_event_id,
            started_event_id,
            details,
        } => Attributes::ActivityTaskCanceledEventAttributes(
            history::ActivityTaskCanceledEventAttributes {
                details: details.as_ref().map(payloads_from_domain),
                scheduled_event_id: *scheduled_event_id,
                started_event_id: *started_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::ActivityTaskCancelRequested { activity_id } => {
            let _ = activity_id;
            Attributes::ActivityTaskCancelRequestedEventAttributes(
                history::ActivityTaskCancelRequestedEventAttributes {
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::TimerStarted { timer_id, fire_at } => {
            Attributes::TimerStartedEventAttributes(
                history::TimerStartedEventAttributes {
                    timer_id: timer_id.clone(),
                    start_to_fire_timeout: Some(to_proto_duration(
                        (*fire_at - event.happened_at).max(time::Duration::ZERO),
                    )),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::TimerFired {
            timer_id,
            started_event_id,
        } => Attributes::TimerFiredEventAttributes(history::TimerFiredEventAttributes {
            timer_id: timer_id.clone(),
            started_event_id: *started_event_id,
        }),
        HistoryEventKind::TimerCanceled { timer_id } => {
            Attributes::TimerCanceledEventAttributes(
                history::TimerCanceledEventAttributes {
                    timer_id: timer_id.clone(),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::MarkerRecorded {
            marker_name,
            details,
            failure,
            header,
        } => Attributes::MarkerRecordedEventAttributes(
            history::MarkerRecordedEventAttributes {
                marker_name: marker_name.clone(),
                details: details
                    .iter()
                    .map(|(k, v)| (k.clone(), payloads_from_domain(v)))
                    .collect(),
                failure: failure.as_ref().map(|_p| proto_failure::Failure {
                    message: String::new(),
                    ..Default::default()
                }),
                header: header.as_ref().map(|h| proto_common::Header {
                    fields: h
                        .iter()
                        .map(|(k, v)| (k.clone(), payload_from_domain(v)))
                        .collect(),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::StartChildWorkflowExecutionInitiated {
            child_workflow_id,
            workflow_type,
            task_queue,
            input,
            namespace_id,
            parent_close_policy,
        } => Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(
            history::StartChildWorkflowExecutionInitiatedEventAttributes {
                workflow_id: child_workflow_id.0.clone(),
                workflow_type: Some(proto_common::WorkflowType {
                    name: workflow_type.0.clone(),
                }),
                task_queue: Some(task_queue_from_domain(task_queue)),
                input: Some(payloads_from_domain(input)),
                namespace_id: namespace_id.0.to_string(),
                parent_close_policy: parent_close_policy_i32(parent_close_policy),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionStarted {
            child_workflow_id,
            child_run_id,
            workflow_type,
        } => Attributes::ChildWorkflowExecutionStartedEventAttributes(
            history::ChildWorkflowExecutionStartedEventAttributes {
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
            cause,
        } => Attributes::StartChildWorkflowExecutionFailedEventAttributes(
            history::StartChildWorkflowExecutionFailedEventAttributes {
                workflow_id: child_workflow_id.0.clone(),
                cause: start_child_workflow_failed_cause_i32(cause),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionCompleted {
            child_workflow_id,
            result,
        } => Attributes::ChildWorkflowExecutionCompletedEventAttributes(
            history::ChildWorkflowExecutionCompletedEventAttributes {
                result: Some(payloads_from_domain(result)),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionFailed {
            child_workflow_id,
            failure,
        } => Attributes::ChildWorkflowExecutionFailedEventAttributes(
            history::ChildWorkflowExecutionFailedEventAttributes {
                failure: Some(proto_failure::Failure {
                    message: failure.clone(),
                    ..Default::default()
                }),
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: child_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ChildWorkflowExecutionCanceled { child_workflow_id } => {
            Attributes::ChildWorkflowExecutionCanceledEventAttributes(
                history::ChildWorkflowExecutionCanceledEventAttributes {
                    workflow_execution: Some(proto_common::WorkflowExecution {
                        workflow_id: child_workflow_id.0.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::ChildWorkflowExecutionTerminated { child_workflow_id } => {
            Attributes::ChildWorkflowExecutionTerminatedEventAttributes(
                history::ChildWorkflowExecutionTerminatedEventAttributes {
                    workflow_execution: Some(proto_common::WorkflowExecution {
                        workflow_id: child_workflow_id.0.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::ChildWorkflowExecutionTimedOut { child_workflow_id } => {
            Attributes::ChildWorkflowExecutionTimedOutEventAttributes(
                history::ChildWorkflowExecutionTimedOutEventAttributes {
                    workflow_execution: Some(proto_common::WorkflowExecution {
                        workflow_id: child_workflow_id.0.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
            target_workflow_id,
            target_run_id,
            signal_name,
            input,
        } => Attributes::SignalExternalWorkflowExecutionInitiatedEventAttributes(
            history::SignalExternalWorkflowExecutionInitiatedEventAttributes {
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                signal_name: signal_name.clone(),
                input: Some(payloads_from_domain(input)),
                ..Default::default()
            },
        ),
        HistoryEventKind::ExternalWorkflowExecutionSignaled {
            initiated_event_id,
            target_workflow_id,
        } => Attributes::ExternalWorkflowExecutionSignaledEventAttributes(
            history::ExternalWorkflowExecutionSignaledEventAttributes {
                initiated_event_id: *initiated_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::SignalExternalWorkflowExecutionFailed {
            initiated_event_id,
            target_workflow_id,
            cause,
        } => Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(
            history::SignalExternalWorkflowExecutionFailedEventAttributes {
                cause: signal_external_workflow_failed_cause_i32(cause),
                initiated_event_id: *initiated_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::RequestCancelExternalWorkflowExecutionInitiated {
            target_workflow_id,
            target_run_id,
        } => Attributes::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes(
            history::RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    run_id: opt_run_id(target_run_id),
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::ExternalWorkflowExecutionCancelRequested {
            initiated_event_id,
            target_workflow_id,
        } => Attributes::ExternalWorkflowExecutionCancelRequestedEventAttributes(
            history::ExternalWorkflowExecutionCancelRequestedEventAttributes {
                initiated_event_id: *initiated_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::RequestCancelExternalWorkflowExecutionFailed {
            initiated_event_id,
            target_workflow_id,
            cause,
        } => Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(
            history::RequestCancelExternalWorkflowExecutionFailedEventAttributes {
                cause: cancel_external_workflow_failed_cause_i32(cause),
                initiated_event_id: *initiated_event_id,
                workflow_execution: Some(proto_common::WorkflowExecution {
                    workflow_id: target_workflow_id.0.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationScheduled {
            operation_id: _,
            endpoint,
            service,
            operation,
            input,
            schedule_to_close_timeout,
        } => Attributes::NexusOperationScheduledEventAttributes(
            history::NexusOperationScheduledEventAttributes {
                endpoint: endpoint.clone(),
                service: service.clone(),
                operation: operation.clone(),
                input: input.0.first().map(payload_from_domain),
                schedule_to_close_timeout: to_opt_proto_duration(
                    *schedule_to_close_timeout,
                ),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationStarted {
            operation_id,
            scheduled_event_id,
        } => Attributes::NexusOperationStartedEventAttributes(
            history::NexusOperationStartedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                operation_id: operation_id.clone(),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationCompleted {
            operation_id: _,
            scheduled_event_id,
            result,
        } => Attributes::NexusOperationCompletedEventAttributes(
            history::NexusOperationCompletedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                result: result.0.first().map(payload_from_domain),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationFailed {
            operation_id: _,
            scheduled_event_id,
            failure,
        } => Attributes::NexusOperationFailedEventAttributes(
            history::NexusOperationFailedEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                failure: Some(proto_failure::Failure {
                    message: failure.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationCanceled {
            operation_id: _,
            scheduled_event_id,
        } => Attributes::NexusOperationCanceledEventAttributes(
            history::NexusOperationCanceledEventAttributes {
                scheduled_event_id: *scheduled_event_id,
                ..Default::default()
            },
        ),
        HistoryEventKind::NexusOperationTimedOut {
            operation_id: _,
            scheduled_event_id,
        } => Attributes::NexusOperationTimedOutEventAttributes(
            history::NexusOperationTimedOutEventAttributes {
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
        } => Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
            history::WorkflowExecutionUpdateAcceptedEventAttributes {
                protocol_instance_id: update_id.clone(),
                accepted_request_message_id: update_id.clone(),
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
        HistoryEventKind::WorkflowExecutionUpdateCompleted { update_id, result } => {
            Attributes::WorkflowExecutionUpdateCompletedEventAttributes(
                history::WorkflowExecutionUpdateCompletedEventAttributes {
                    meta: Some(proto_update::Meta {
                        update_id: update_id.clone(),
                        identity: String::new(),
                    }),
                    outcome: Some(proto_update::Outcome {
                        value: Some(proto_update::outcome::Value::Success(
                            payloads_from_domain(result),
                        )),
                    }),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionUpdateRejected { update_id, failure } => {
            Attributes::WorkflowExecutionUpdateRejectedEventAttributes(
                history::WorkflowExecutionUpdateRejectedEventAttributes {
                    protocol_instance_id: update_id.clone(),
                    failure: Some(proto_failure::Failure {
                        message: failure.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        }
        HistoryEventKind::WorkflowExecutionOptionsUpdated {
            versioning_override,
            completion_callbacks,
            attached_request_id,
        } => {
            // The upstream proto only has `versioning_override`. `completion_callbacks`
            // and `attached_request_id` are internal kernel fields with no proto
            // representation. `VersioningOverride` is a placeholder type so we can't
            // populate the proto field yet either.
            let _ = (
                versioning_override,
                completion_callbacks,
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
        WorkflowTaskFailedCause::BadScheduleActivityAttributes => {
            C::BadScheduleActivityAttributes
        }
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
        "SCHEDULE_TO_START" | "ScheduleToStart" | "schedule_to_start" => {
            T::ScheduleToStart as i32
        }
        "SCHEDULE_TO_CLOSE" | "ScheduleToClose" | "schedule_to_close" => {
            T::ScheduleToClose as i32
        }
        "HEARTBEAT" | "Heartbeat" | "heartbeat" => T::Heartbeat as i32,
        _ => T::Unspecified as i32,
    }
}

fn start_child_workflow_failed_cause_i32(cause: &str) -> i32 {
    use tokeira_proto::enums::StartChildWorkflowExecutionFailedCause as C;
    match cause {
        "WORKFLOW_ALREADY_EXISTS" | "WorkflowAlreadyExists" => {
            C::WorkflowAlreadyExists as i32
        }
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
    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    use time::OffsetDateTime;
    use tokeira_kernel::command::RetryState;
    use tokeira_kernel::event::{HistoryEvent, HistoryEventKind};
    use tokeira_kernel::state::ParentClosePolicy;
    use tokeira_types::{
        LogicalTaskSeq, Memo, NamespaceId, Payloads, RetryPolicy, RunId,
        SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
    };

    fn arb_payloads() -> impl Strategy<Value = Payloads> {
        Just(Payloads::default())
    }

    fn arb_run_id() -> impl Strategy<Value = RunId> {
        any::<u128>().prop_map(|v| RunId(uuid::Uuid::from_u128(v)))
    }

    fn arb_opt_run_id() -> impl Strategy<Value = Option<RunId>> {
        proptest::option::of(arb_run_id())
    }

    fn arb_opt_duration() -> impl Strategy<Value = Option<time::Duration>> {
        proptest::option::of(
            (0i64..1_000_000).prop_map(|ms| time::Duration::milliseconds(ms)),
        )
    }

    fn arb_duration() -> impl Strategy<Value = time::Duration> {
        (0i64..1_000_000).prop_map(time::Duration::milliseconds)
    }

    fn arb_timestamp() -> impl Strategy<Value = OffsetDateTime> {
        (1i64..2_000_000_000)
            .prop_map(|s| OffsetDateTime::from_unix_timestamp(s).unwrap())
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
                            memo: Memo::default(),
                            search_attributes: SearchAttributes::default(),
                            request_id: rid,
                            continued_execution_run_id: cont,
                            first_execution_run_id: first,
                            retry_policy: rp,
                            attempt: att,
                            workflow_execution_timeout: wet,
                            workflow_run_timeout: wrt,
                            workflow_task_timeout: wtt,
                        }
                    }
                ),
            arb_payloads().prop_map(|r| {
                HistoryEventKind::WorkflowExecutionCompleted { result: r }
            }),
            ("[a-z]{1,8}", arb_retry_state(), 0u32..5).prop_map(|(msg, rs, att)| {
                HistoryEventKind::WorkflowExecutionFailed {
                    message: msg,
                    details: Some(tokeira_types::Payload {
                        data: b"err-detail".to_vec(),
                        metadata: Default::default(),
                    }),
                    retry_state: rs,
                    attempt: att,
                }
            }),
            Just(HistoryEventKind::WorkflowExecutionCanceled),
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
                    request_id: rid,
                    identity: None,
                }
            }),
            (1u64..100).prop_map(|seq| {
                HistoryEventKind::WorkflowTaskScheduled {
                    logical_seq: LogicalTaskSeq(seq),
                }
            }),
            (1u64..100, 1i64..100, 1u32..5).prop_map(|(seq, sched, att)| {
                HistoryEventKind::WorkflowTaskStarted {
                    logical_seq: LogicalTaskSeq(seq),
                    scheduled_event_id: sched,
                    attempt: att,
                    identity: WorkerIdentity("w".to_string()),
                }
            }),
            (1u64..100, 1i64..100, 1i64..100).prop_map(|(seq, sched, started)| {
                HistoryEventKind::WorkflowTaskCompleted {
                    logical_seq: LogicalTaskSeq(seq),
                    scheduled_event_id: sched,
                    started_event_id: started,
                    identity: WorkerIdentity("w".to_string()),
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
                .prop_map(
                    |(aid, at, tq, input, hdr, rp, s2c, s2s, stc, hb)| {
                        HistoryEventKind::ActivityTaskScheduled {
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
                    },
                ),
            ("[a-z]{1,6}", arb_payloads()).prop_map(|(aid, result)| {
                HistoryEventKind::ActivityTaskCompleted {
                    activity_id: aid,
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    result,
                }
            }),
            ("[a-z]{1,6}", "[a-z]{1,8}").prop_map(|(aid, msg)| {
                HistoryEventKind::ActivityTaskFailed {
                    activity_id: aid,
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    message: msg,
                }
            }),
            ("[a-z]{1,6}", 1i64..100, 1u32..5).prop_map(|(aid, sched, att)| {
                HistoryEventKind::ActivityTaskStarted {
                    activity_id: aid,
                    scheduled_event_id: sched,
                    attempt: att,
                    identity: WorkerIdentity("w".to_string()),
                }
            }),
            ("[a-z]{1,6}", arb_timestamp()).prop_map(|(tid, fire_at)| {
                HistoryEventKind::TimerStarted {
                    timer_id: tid,
                    fire_at,
                }
            }),
            "[a-z]{1,6}".prop_map(|tid| HistoryEventKind::TimerFired {
                timer_id: tid,
                started_event_id: 5,
            }),
            "[a-z]{1,6}"
                .prop_map(|tid| HistoryEventKind::TimerCanceled { timer_id: tid }),
            "[a-z]{1,6}".prop_map(|aid| {
                HistoryEventKind::ActivityTaskCancelRequested { activity_id: aid }
            }),
            ("[a-z]{1,6}", arb_run_id(), "[a-z]{1,6}").prop_map(|(cwid, crid, wt)| {
                HistoryEventKind::ChildWorkflowExecutionStarted {
                    child_workflow_id: WorkflowId(cwid),
                    child_run_id: crid,
                    workflow_type: WorkflowType(wt),
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
                        operation_id: oid,
                        endpoint: ep,
                        service: svc,
                        operation: op,
                        input,
                        schedule_to_close_timeout: timeout,
                    }
                }),
            ("[a-z]{1,6}", "[a-z]{1,6}", arb_payloads()).prop_map(
                |(uid, uname, input)| {
                    HistoryEventKind::WorkflowExecutionUpdateAccepted {
                        update_id: uid,
                        update_name: uname,
                        input,
                    }
                }
            ),
            ("[a-z]{1,6}", arb_payloads()).prop_map(|(uid, result)| {
                HistoryEventKind::WorkflowExecutionUpdateCompleted {
                    update_id: uid,
                    result,
                }
            }),
            ("[a-z]{1,6}", "[a-z]{1,8}").prop_map(|(uid, failure)| {
                HistoryEventKind::WorkflowExecutionUpdateRejected {
                    update_id: uid,
                    failure,
                }
            }),
        ]
    }

    fn arb_history_event() -> impl Strategy<Value = HistoryEvent> {
        (1i64..10000, arb_timestamp(), arb_history_event_kind()).prop_map(
            |(eid, ts, kind)| HistoryEvent {
                event_id: eid,
                happened_at: ts,
                kind,
            },
        )
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
        let decoded =
            history::History::decode(&bytes[..]).expect("decode should succeed");
        assert!(decoded.events.is_empty());
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
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
                request_id: "req-1".to_string(),
                continued_execution_run_id: None,
                first_execution_run_id: None,
                retry_policy: None,
                attempt: 1,
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: time::Duration::seconds(10),
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
    fn activity_scheduled_golden_example() {
        let event = HistoryEvent {
            event_id: 5,
            happened_at: OffsetDateTime::from_unix_timestamp(2000).unwrap(),
            kind: HistoryEventKind::ActivityTaskScheduled {
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
                child_workflow_id: WorkflowId("child-1".to_string()),
                workflow_type: WorkflowType("ChildWf".to_string()),
                task_queue: TaskQueueName("child-q".to_string()),
                input: Payloads::default(),
                namespace_id: NamespaceId(uuid::Uuid::nil()),
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
}
