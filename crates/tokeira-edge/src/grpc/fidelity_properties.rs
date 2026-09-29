//! Wire-level properties for execution links and update validation.

use proptest::prelude::*;
use tokeira_proto::public::temporal::api::update::v1 as update;

use super::*;
use crate::translate::{
    ExecuteMultiOperationResponse, SignalWithStartWorkflowExecutionResponse,
    SignalWorkflowExecutionResponse, UpdateLifecycleStageDto, UpdateOutcomeDto, UpdateRefDto,
    UpdateWorkflowExecutionResponse,
};

fn update_response(run: RunId, workflow_id: &str, outcome: u8) -> UpdateWorkflowExecutionResponse {
    UpdateWorkflowExecutionResponse {
        update_ref: UpdateRefDto {
            workflow_id: workflow_id.into(),
            run_id: run.0.to_string(),
            update_id: "update".into(),
        },
        stage: if outcome == 0 {
            UpdateLifecycleStageDto::Accepted
        } else {
            UpdateLifecycleStageDto::Completed
        },
        outcome: match outcome {
            0 => None,
            1 => Some(UpdateOutcomeDto::Completed {
                accepted_event_id: 5,
                result: Payloads::default(),
            }),
            2 => Some(UpdateOutcomeDto::Rejected {
                accepted_event_id: 0,
                failure: tokeira_types::Payload::new(b"failure".to_vec()),
            }),
            3 => Some(UpdateOutcomeDto::AcceptedRunClosed),
            _ => Some(UpdateOutcomeDto::RejectedUnprocessed),
        },
    }
}

fn assert_request_link(
    link: proto_common::Link,
    namespace: &str,
    workflow_id: &str,
    run: RunId,
    request_id: &str,
    event_type: i32,
) {
    let Some(proto_common::link::Variant::WorkflowEvent(event)) = link.variant else {
        panic!("workflow event link")
    };
    assert_eq!(event.namespace, namespace);
    assert_eq!(event.workflow_id, workflow_id);
    assert_eq!(event.run_id, run.0.to_string());
    let Some(proto_common::link::workflow_event::Reference::RequestIdRef(reference)) =
        event.reference
    else {
        panic!("request id reference")
    };
    assert_eq!(reference.request_id, request_id);
    assert_eq!(reference.event_type, event_type);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: v132-lifecycle-fidelity, Property 3: total-updates limit at admission
    // The complete interpolated limit error survives multi-operation wrapping
    // (service/history/workflow/update/registry.go:438-449 @ v1.32.0).
    #[test]
    fn update_limit_failure_retains_operation_shape(limit in -1i64..2001) {
        let exceeded = tokeira_runtime::UpdateLimitExceeded { limit };
        let status = multi_operation_failure_to_status(crate::translate::MultiOperationFailure::Update {
            started: false, error: crate::errors::EdgeError::from(anyhow::Error::new(exceeded)),
        });
        prop_assert_eq!(status.code(), Code::FailedPrecondition);
        prop_assert_eq!(status.message(), "Update-with-Start could not be executed.");
        let details = RpcStatus::decode(status.details()).unwrap();
        let failure = errordetails_proto::MultiOperationExecutionFailure::decode(details.details[0].value.as_slice()).unwrap();
        prop_assert_eq!(failure.statuses.len(), 2);
        prop_assert_eq!(failure.statuses[0].code, Code::Aborted as i32);
        prop_assert_eq!(&failure.statuses[0].message, "Operation was aborted.");
        prop_assert_eq!(failure.statuses[1].code, Code::FailedPrecondition as i32);
        prop_assert_eq!(&failure.statuses[1].message, &exceeded.to_string());
    }

    // Feature: v132-lifecycle-fidelity, Property 20: eager dispatch guards
    // Keep the wire compatibility flag through translation without adding it to
    // the kernel command (service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:528-549 @ v1.32.0).
    #[test]
    fn eager_build_compatibility_flag_reaches_delivery(flags in prop::collection::vec(any::<bool>(), 0..16)) {
        let mut expected = std::collections::HashSet::new();
        let commands = flags.into_iter().enumerate().map(|(index, compatible)| {
            let activity_id = format!("activity-{index}");
            if compatible { expected.insert(activity_id.clone()); }
            command::Command {
                command_type: enums::CommandType::ScheduleActivityTask as i32,
                attributes: Some(command::command::Attributes::ScheduleActivityTaskCommandAttributes(command::ScheduleActivityTaskCommandAttributes {
                    activity_id, activity_type: Some(proto_common::ActivityType { name: "activity".into() }),
                    task_queue: Some(taskqueue_proto::TaskQueue { name: "queue".into(), ..Default::default() }),
                    request_eager_execution: true, use_workflow_build_id: compatible,
                    schedule_to_close_timeout: Some(prost_types::Duration { seconds: 30, nanos: 0 }), ..Default::default()
                })), ..Default::default()
            }
        }).collect();
        let result = respond_completed_request_to_edge(workflowservice::RespondWorkflowTaskCompletedRequest { namespace: "default".into(), commands, ..Default::default() }).unwrap();
        prop_assert_eq!(result.eager_use_workflow_build_id, expected);
    }

    // Feature: v132-lifecycle-fidelity, Property 5: running-workflow start leg
    // Resolved runtime outcomes retain the running leg and event 1 link
    // (service/history/api/multioperation/api.go:334-339; service/history/api/link_util.go:12-31 @ v1.32.0).
    #[test]
    fn running_start_leg_keeps_lineage_and_event_reference(run in any::<u128>(), chain in any::<u128>(), namespace in "[a-z]{1,20}", workflow_id in "[a-z]{1,20}") {
        let run = RunId(Uuid::from_u128(run));
        let chain = RunId(Uuid::from_u128(chain));
        let response = multi_operation_response_to_proto(ExecuteMultiOperationResponse {
            run_id: run, first_execution_run_id: chain, started: false, status: ExecutionStatus::Running,
            update: update_response(run, &workflow_id, 0),
        }, &namespace, "request");
        let Some(workflowservice::execute_multi_operation_response::response::Response::StartWorkflow(start)) = response.responses[0].response.as_ref() else { panic!("start leg") };
        prop_assert!(!start.started);
        prop_assert_eq!(start.status, enums::WorkflowExecutionStatus::Running as i32);
        prop_assert_eq!(&start.run_id, &run.0.to_string());
        prop_assert_eq!(&start.first_execution_run_id, &chain.0.to_string());
        let Some(proto_common::link::Variant::WorkflowEvent(event)) = start.link.as_ref().and_then(|link| link.variant.as_ref()) else { panic!("start link") };
        prop_assert_eq!(&event.namespace, &namespace);
        prop_assert_eq!(&event.workflow_id, &workflow_id);
        prop_assert_eq!(&event.run_id, &run.0.to_string());
        let Some(proto_common::link::workflow_event::Reference::EventRef(reference)) = &event.reference else { panic!("event reference") };
        prop_assert_eq!(reference.event_id, 1);
        prop_assert_eq!(reference.event_type, enums::EventType::WorkflowExecutionStarted as i32);
    }

    // Feature: v132-lifecycle-fidelity, Property 6: update response link by outcome
    // Completed failures name the workflow; accepted/success outcomes name the
    // request's acceptance (service/history/api/updateworkflow/api.go:277-303 @ v1.32.0).
    #[test]
    fn update_link_matches_outcome(outcome in 0u8..5, run in any::<u128>(), namespace in "[a-z]{1,20}", workflow_id in "[a-z]{1,20}", id in "[a-z]{0,32}") {
        let run = RunId(Uuid::from_u128(run));
        let response = update_response_to_proto(update_response(run, &workflow_id, outcome), &namespace, &id);
        let link = response.link.unwrap();
        if outcome >= 2 {
            let Some(proto_common::link::Variant::Workflow(workflow)) = link.variant else { panic!("workflow link") };
            prop_assert_eq!(workflow.namespace, namespace);
            prop_assert_eq!(workflow.workflow_id, workflow_id);
            prop_assert_eq!(workflow.run_id, run.0.to_string());
            prop_assert_eq!(workflow.reason, "Update rejected");
        } else {
            assert_request_link(link, &namespace, &workflow_id, run, &id, enums::EventType::WorkflowExecutionUpdateAccepted as i32);
        }
    }

    // Feature: v132-lifecycle-fidelity, Property 7: signal links are unconditional and idempotent
    // No config read governs response links (service/history/api/signalworkflow/api.go:115-122;
    // service/frontend/workflow_handler.go:2386 @ v1.32.0).
    #[test]
    fn both_signal_responses_name_the_resolved_run(run in any::<u128>(), namespace in "[a-z]{1,20}", workflow_id in "[a-z]{1,20}", id in "[a-z]{1,32}", started in any::<bool>()) {
        let run = RunId(Uuid::from_u128(run));
        let signal = SignalWorkflowExecutionResponse { accepted: true, transition_seq: 1, last_event_id: 3, run_id: Some(run), request_id: id.clone() };
        let response = signal_response_to_proto(signal.clone(), namespace.clone(), workflow_id.clone());
        let retried = signal_response_to_proto(signal, namespace.clone(), workflow_id.clone());
        prop_assert_eq!(&response, &retried);
        let sws = signal_with_start_response_to_proto(SignalWithStartWorkflowExecutionResponse {
            run_id: run, first_execution_run_id: run, started, request_id: id.clone(),
        }, namespace.clone(), workflow_id.clone());
        prop_assert_eq!(&response.link, &sws.signal_link);
        assert_request_link(response.link.unwrap(), &namespace, &workflow_id, run, &id, enums::EventType::WorkflowExecutionSignaled as i32);
    }

    // Feature: v132-lifecycle-fidelity, Property 11: callback precondition
    // Compare the complete error; accepted callback requests leave no callback
    // registration in the edge command (service/history/workflow/update/update.go:390-395 @ v1.32.0).
    #[test]
    fn callbacks_require_request_id(callbacks in 0usize..5, id in "[a-z]{0,24}") {
        let request = workflowservice::UpdateWorkflowExecutionRequest {
            namespace: "default".into(), workflow_execution: Some(proto_common::WorkflowExecution { workflow_id: "workflow".into(), run_id: String::new() }),
            request: Some(update::Request {
                request_id: id.clone(), completion_callbacks: vec![proto_common::Callback::default(); callbacks],
                meta: Some(update::Meta { update_id: "update".into(), identity: "client".into() }),
                input: Some(update::Input { name: "handler".into(), ..Default::default() }), ..Default::default()
            }), ..Default::default()
        };
        let result = update_request_to_edge(request.clone());
        if callbacks > 0 && id.is_empty() {
            let status = crate::grpc::errors::proto_conversion_status(result.unwrap_err());
            prop_assert_eq!(status.code(), Code::InvalidArgument);
            prop_assert_eq!(status.message(), "invalid *update.Request: request_id is required when completion_callbacks are set");
        } else {
            let mut without_callbacks = request;
            without_callbacks.request.as_mut().unwrap().completion_callbacks.clear();
            prop_assert_eq!(result.unwrap(), update_request_to_edge(without_callbacks).unwrap());
        }
    }
}
