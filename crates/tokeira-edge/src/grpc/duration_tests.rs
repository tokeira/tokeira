//! Duration admission, recorded values and proto-only failure preservation.

use super::*;
use command::command::Attributes;
use proptest::prelude::*;
use tokeira_kernel::{
    BasicKernel, Command, Kernel, LoadedRun, Reject, StartWorkflowTaskRequest, WorkflowState,
    WorkflowTaskCompletedRequest, WorkflowTaskFailedCause as Cause,
};
use tokeira_proto::public::temporal::api::{
    history::v1::history_event::Attributes as HistoryAttributes, schedule::v1 as schedule,
    update::v1 as update,
};
use tokeira_types::{ShardEpoch, WorkerIdentity, WorkflowTaskToken};
use workflowservice::execute_multi_operation_request::{Operation, operation};

const CAP: time::Duration = time::Duration::seconds(3_153_600_000);

fn wire(seconds: i64, nanos: i32) -> prost_types::Duration {
    prost_types::Duration { seconds, nanos }
}

fn start() -> workflowservice::StartWorkflowExecutionRequest {
    workflowservice::StartWorkflowExecutionRequest {
        namespace: "default".into(),
        workflow_id: "workflow".into(),
        workflow_type: Some(proto_common::WorkflowType { name: "wf".into() }),
        task_queue: Some(taskqueue_proto::TaskQueue {
            name: "queue".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn start_and_signal_with_start_validate_and_record_each_duration() {
    for field in 0..4 {
        for (value, error) in [
            (wire(-1, 0), Some("negative duration")),
            (
                wire(1, -1),
                Some("duration has seconds and nanos with different signs"),
            ),
            (wire(i64::MAX, i32::MAX), None),
        ] {
            let mut request = start();
            let (slot, name) = match field {
                0 => (
                    &mut request.workflow_execution_timeout,
                    "WorkflowExecutionTimeout",
                ),
                1 => (&mut request.workflow_run_timeout, "WorkflowRunTimeout"),
                2 => (&mut request.workflow_task_timeout, "WorkflowTaskTimeout"),
                _ => (&mut request.workflow_start_delay, "WorkflowStartDelay"),
            };
            *slot = Some(value);
            let signal = workflowservice::SignalWithStartWorkflowExecutionRequest {
                namespace: request.namespace.clone(),
                workflow_id: request.workflow_id.clone(),
                workflow_type: request.workflow_type.clone(),
                task_queue: request.task_queue.clone(),
                workflow_execution_timeout: request.workflow_execution_timeout,
                workflow_run_timeout: request.workflow_run_timeout,
                workflow_task_timeout: request.workflow_task_timeout,
                workflow_start_delay: request.workflow_start_delay,
                signal_name: "signal".into(),
                ..Default::default()
            };
            let started = start_request_to_edge(request);
            let signaled = signal_with_start_request_to_edge(signal);
            if let Some(error) = error {
                let expected =
                    format!("An invalid {name}Seconds is set on request. cause: {error}");
                assert_eq!(started.unwrap_err().to_string(), expected);
                assert_eq!(signaled.unwrap_err().to_string(), expected);
            } else {
                let started = started.unwrap();
                let signaled = signaled.unwrap();
                assert_eq!(
                    [
                        started.workflow_execution_timeout,
                        started.workflow_run_timeout,
                        started.workflow_task_timeout,
                        started.workflow_start_delay
                    ][field],
                    Some(CAP)
                );
                assert_eq!(
                    [
                        signaled.workflow_execution_timeout,
                        signaled.workflow_run_timeout,
                        signaled.workflow_task_timeout,
                        signaled.workflow_start_delay
                    ][field],
                    Some(CAP)
                );
            }
        }
    }
}

#[test]
fn capped_start_durations_are_recorded_in_history() {
    let mut request = start();
    request.workflow_execution_timeout = Some(wire(i64::MAX, i32::MAX));
    request.workflow_run_timeout = request.workflow_execution_timeout;
    request.workflow_task_timeout = request.workflow_execution_timeout;
    request.workflow_start_delay = request.workflow_execution_timeout;
    let request = start_request_to_edge(request).unwrap();
    let context = crate::interceptors::EdgeContext {
        request_id: crate::request_id::RequestId::new("request"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: OffsetDateTime::UNIX_EPOCH,
        is_long_poll: false,
    };
    let internal = crate::translate::to_internal::start_request(
        request,
        &context,
        time::OffsetDateTime::now_utc(),
    );
    let transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(internal))
        .unwrap();
    let event =
        crate::translate::history_serializer::history_event_to_proto(&transition.history_events[0]);
    let Some(HistoryAttributes::WorkflowExecutionStartedEventAttributes(attrs)) = event.attributes
    else {
        panic!("missing start event")
    };
    for duration in [
        attrs.workflow_execution_timeout,
        attrs.workflow_run_timeout,
        attrs.workflow_task_timeout,
        attrs.first_workflow_task_backoff,
    ] {
        assert_eq!(duration, Some(wire(3_153_600_000, 0)));
    }
}

fn command_cases(value: prost_types::Duration) -> Vec<(Attributes, Cause, String)> {
    let mut cases = vec![(
        Attributes::StartTimerCommandAttributes(command::StartTimerCommandAttributes {
            timer_id: "timer".into(),
            start_to_fire_timeout: Some(value),
        }),
        Cause::BadStartTimerAttributes,
        "An invalid StartToFireTimeout is set on StartTimerCommand: ERROR. TimerId=timer".into(),
    )];
    for field in 0..4 {
        let mut attrs = command::ScheduleActivityTaskCommandAttributes {
            activity_id: "activity".into(),
            ..Default::default()
        };
        let (slot, name) = match field {
            0 => (
                &mut attrs.schedule_to_close_timeout,
                "ScheduleToCloseTimeout",
            ),
            1 => (
                &mut attrs.schedule_to_start_timeout,
                "ScheduleToStartTimeout",
            ),
            2 => (&mut attrs.start_to_close_timeout, "StartToCloseTimeout"),
            _ => (&mut attrs.heartbeat_timeout, "HeartbeatTimeout"),
        };
        *slot = Some(value);
        cases.push((
            Attributes::ScheduleActivityTaskCommandAttributes(attrs),
            Cause::BadScheduleActivityAttributes,
            format!("invalid {name}: ERROR"),
        ));
    }
    for field in 0..3 {
        let mut attrs = command::StartChildWorkflowExecutionCommandAttributes {
            workflow_id: "child".into(),
            workflow_type: Some(proto_common::WorkflowType { name: "wf".into() }),
            namespace: "default".into(),
            ..Default::default()
        };
        let (slot, name) = match field {
            0 => (
                &mut attrs.workflow_execution_timeout,
                "WorkflowExecutionTimeout",
            ),
            1 => (&mut attrs.workflow_run_timeout, "WorkflowRunTimeout"),
            _ => (&mut attrs.workflow_task_timeout, "WorkflowTaskTimeout"),
        };
        *slot = Some(value);
        cases.push((
            Attributes::StartChildWorkflowExecutionCommandAttributes(attrs),
            Cause::BadStartChildExecutionAttributes,
            format!(
                "Invalid {name} on StartChildWorkflowExecutionCommand: ERROR. \
                 WorkflowId=child WorkflowType=wf Namespace=default"
            ),
        ));
        let mut attrs = command::ContinueAsNewWorkflowExecutionCommandAttributes::default();
        let (slot, name) = match field {
            0 => (&mut attrs.workflow_run_timeout, "WorkflowRunTimeout"),
            1 => (&mut attrs.workflow_task_timeout, "WorkflowTaskTimeout"),
            _ => (&mut attrs.backoff_start_interval, "BackoffStartInterval"),
        };
        *slot = Some(value);
        cases.push((
            Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs),
            Cause::BadContinueAsNewAttributes,
            format!("Invalid {name} on ContinueAsNewWorkflowExecutionCommand: ERROR"),
        ));
        let mut attrs = command::ScheduleNexusOperationCommandAttributes::default();
        let (slot, name) = match field {
            0 => (
                &mut attrs.schedule_to_close_timeout,
                "ScheduleToCloseTimeout",
            ),
            1 => (
                &mut attrs.schedule_to_start_timeout,
                "ScheduleToStartTimeout",
            ),
            _ => (&mut attrs.start_to_close_timeout, "StartToCloseTimeout"),
        };
        *slot = Some(value);
        cases.push((
            Attributes::ScheduleNexusOperationCommandAttributes(attrs),
            Cause::BadScheduleNexusOperationAttributes,
            format!("ScheduleNexusOperationCommandAttributes.{name} is invalid: ERROR"),
        ));
    }
    cases
}

fn completion(command: WorkflowCommand) -> (WorkflowState, WorkflowTaskCompletedRequest) {
    let context = crate::interceptors::EdgeContext {
        request_id: crate::request_id::RequestId::new("request"),
        claims: None,
        auth_principal: None,
        namespace: None,
        received_at: OffsetDateTime::UNIX_EPOCH,
        is_long_poll: false,
    };
    let request = crate::translate::to_internal::start_request(
        start_request_to_edge(start()).unwrap(),
        &context,
        time::OffsetDateTime::now_utc(),
    );
    let started = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request.clone()))
        .unwrap();
    let pending = started.next_state.pending_workflow_task.as_ref().unwrap();
    let task = StartWorkflowTaskRequest {
        logical_seq: pending.logical_seq,
        worker_identity: WorkerIdentity("worker".into()),
        request_id: "task".into(),
        history_size_bytes: 0,
        advice_policy: request.advice_policy,
        deployment_transition: None,
        deployment_transition_revision_number: None,
        polled_task_queue: request.task_queue.clone(),
        now: request.now,
        target_version_changed_enabled: false,
        target_deployment_version: None,
    };
    let state = BasicKernel
        .apply(
            LoadedRun::Existing(started.next_state),
            Command::WorkflowTaskStarted(task),
        )
        .unwrap()
        .next_state;
    let pending = state.pending_workflow_task.as_ref().unwrap();
    let completed = WorkflowTaskCompletedRequest {
        token: WorkflowTaskToken {
            run_key: request.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        client_discards_speculative_with_events: false,
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: Default::default(),
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands: vec![command],
        command_sizes: vec![],
        eager_activities: Default::default(),
        inline_workflow_task: None,
        heartbeat: Default::default(),
        force_new_workflow_task: false,
        limits: Default::default(),
        delivered_update_ids: vec![],
        request: request.request,
        now: OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND,
    };
    (state, completed)
}

#[test]
fn command_durations_preserve_failure_cause_and_message() {
    for (value, error) in [
        (wire(-1, 0), "negative duration"),
        (
            wire(-1, 1),
            "duration has seconds and nanos with different signs",
        ),
    ] {
        for (attributes, expected_cause, expected_message) in command_cases(value) {
            let command = proto_command_to_workflow_command(
                command::Command {
                    attributes: Some(attributes),
                    ..Default::default()
                },
                "default",
            )
            .unwrap();
            let WorkflowCommand::InvalidDuration { cause, message, .. } = &command else {
                panic!("expected invalid duration command")
            };
            assert_eq!(cause, &expected_cause);
            assert_eq!(message, &expected_message.replace("ERROR", error));
            let mut expected = expected_message.replace("ERROR", error);
            if expected_cause == Cause::BadContinueAsNewAttributes {
                expected.push_str(
                    ". WorkflowType=wf TaskQueue=name:\"queue\" kind:TASK_QUEUE_KIND_NORMAL",
                );
            }
            let (state, request) = completion(command);
            assert_eq!(
                BasicKernel
                    .apply(
                        LoadedRun::Existing(state),
                        Command::WorkflowTaskCompleted(request)
                    )
                    .unwrap_err(),
                Reject::InvalidCommandAttributes {
                    cause: expected_cause,
                    message: Some(expected)
                }
            );
        }
    }
}

#[test]
fn command_durations_record_capped_values() {
    for (attributes, _, _) in command_cases(wire(i64::MAX, i32::MAX)) {
        let command = proto_command_to_workflow_command(
            command::Command {
                attributes: Some(attributes),
                ..Default::default()
            },
            "default",
        )
        .unwrap();
        let recorded = workflow_command_to_proto(&command)
            .unwrap()
            .attributes
            .unwrap();
        let durations = match recorded {
            Attributes::StartTimerCommandAttributes(attrs) => {
                let duration =
                    proto_duration_to_time(attrs.start_to_fire_timeout.as_ref()).unwrap();
                assert_eq!(duration, CAP);
                continue;
            }
            Attributes::ScheduleActivityTaskCommandAttributes(attrs) => vec![
                attrs.schedule_to_close_timeout,
                attrs.schedule_to_start_timeout,
                attrs.start_to_close_timeout,
                attrs.heartbeat_timeout,
            ],
            Attributes::StartChildWorkflowExecutionCommandAttributes(attrs) => vec![
                attrs.workflow_execution_timeout,
                attrs.workflow_run_timeout,
                attrs.workflow_task_timeout,
            ],
            Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(attrs) => vec![
                attrs.workflow_run_timeout,
                attrs.workflow_task_timeout,
                attrs.backoff_start_interval,
            ],
            Attributes::ScheduleNexusOperationCommandAttributes(attrs) => vec![
                attrs.schedule_to_close_timeout,
                attrs.schedule_to_start_timeout,
                attrs.start_to_close_timeout,
            ],
            _ => panic!("unexpected command"),
        };
        assert!(
            durations
                .into_iter()
                .flatten()
                .any(|value| value == wire(3_153_600_000, 0))
        );
    }
}

#[test]
fn capped_command_durations_are_recorded_in_history() {
    for (mut attributes, _, _) in command_cases(wire(i64::MAX, i32::MAX)) {
        if let Attributes::ScheduleActivityTaskCommandAttributes(attrs) = &mut attributes {
            attrs.activity_type = Some(proto_common::ActivityType {
                name: "activity".into(),
            });
            attrs
                .start_to_close_timeout
                .get_or_insert(wire(3_153_600_000, 0));
        }
        let command = proto_command_to_workflow_command(
            command::Command {
                attributes: Some(attributes),
                ..Default::default()
            },
            "default",
        )
        .unwrap();
        let (state, request) = completion(command);
        let transition = BasicKernel
            .apply(
                LoadedRun::Existing(state),
                Command::WorkflowTaskCompleted(request),
            )
            .unwrap();
        let recorded: Vec<_> = transition
            .history_events
            .iter()
            .filter_map(|event| {
                crate::translate::history_serializer::history_event_to_proto(event).attributes
            })
            .flat_map(|attributes| match attributes {
                HistoryAttributes::TimerStartedEventAttributes(a) => vec![a.start_to_fire_timeout],
                HistoryAttributes::ActivityTaskScheduledEventAttributes(a) => vec![
                    a.schedule_to_close_timeout,
                    a.schedule_to_start_timeout,
                    a.start_to_close_timeout,
                    a.heartbeat_timeout,
                ],
                HistoryAttributes::StartChildWorkflowExecutionInitiatedEventAttributes(a) => vec![
                    a.workflow_execution_timeout,
                    a.workflow_run_timeout,
                    a.workflow_task_timeout,
                ],
                HistoryAttributes::WorkflowExecutionContinuedAsNewEventAttributes(a) => vec![
                    a.workflow_run_timeout,
                    a.workflow_task_timeout,
                    a.backoff_start_interval,
                ],
                HistoryAttributes::NexusOperationScheduledEventAttributes(a) => vec![
                    a.schedule_to_close_timeout,
                    a.schedule_to_start_timeout,
                    a.start_to_close_timeout,
                ],
                _ => Vec::new(),
            })
            .flatten()
            .collect();
        assert!(recorded.contains(&wire(3_153_600_000, 0)), "{recorded:?}");
    }
}

#[test]
fn retry_durations_validate_and_cap_each_interval() {
    for initial in [true, false] {
        for (value, error) in [
            (wire(-1, 0), Some("negative duration")),
            (
                wire(1, -1),
                Some("duration has seconds and nanos with different signs"),
            ),
            (wire(i64::MAX, i32::MAX), None),
        ] {
            let mut policy = proto_common::RetryPolicy::default();
            let name = if initial {
                policy.initial_interval = Some(value);
                "InitialInterval"
            } else {
                policy.maximum_interval = Some(value);
                "MaximumInterval"
            };
            let mut request = start();
            request.retry_policy = Some(policy.clone());
            if let Some(error) = error {
                assert_eq!(
                    start_request_to_edge(request).unwrap_err().to_string(),
                    format!("invalid {name} set on retry policy: {error}")
                );
            } else {
                let policy = start_request_to_edge(request)
                    .unwrap()
                    .retry_policy
                    .unwrap();
                assert_eq!(
                    if initial {
                        policy.initial_interval
                    } else {
                        policy.maximum_interval.unwrap()
                    },
                    CAP
                );
            }
            policy.maximum_attempts = 1;
            assert!(validate_retry_durations(Some(&policy)).is_ok());
        }
    }
}

#[test]
fn retry_command_and_standalone_durations_validate_and_cap() {
    for initial in [true, false] {
        for (value, error) in [
            (wire(-1, 0), Some("negative duration")),
            (
                wire(1, -1),
                Some("duration has seconds and nanos with different signs"),
            ),
            (wire(i64::MAX, i32::MAX), None),
        ] {
            let mut policy = proto_common::RetryPolicy::default();
            let name = if initial {
                policy.initial_interval = Some(value);
                "InitialInterval"
            } else {
                policy.maximum_interval = Some(value);
                "MaximumInterval"
            };
            let standalone =
                standalone_activity_durations(&workflowservice::StartActivityExecutionRequest {
                    retry_policy: Some(policy.clone()),
                    ..Default::default()
                });
            if let Some(error) = error {
                assert_eq!(
                    standalone.unwrap_err().to_string(),
                    format!("invalid {name} set on retry policy: {error}")
                );
            } else {
                standalone.unwrap();
                let recorded = crate::chasm_activity::defaulted_retry_policy(Some(&policy));
                assert_eq!(
                    if initial {
                        recorded.initial_interval
                    } else {
                        recorded.maximum_interval
                    },
                    Some(wire(3_153_600_000, 0))
                );
            }
            for (mut attributes, cause, _) in command_cases(wire(1, 0)) {
                let prefix = match &mut attributes {
                    Attributes::ScheduleActivityTaskCommandAttributes(a) => {
                        a.retry_policy = Some(policy.clone());
                        ""
                    }
                    Attributes::StartChildWorkflowExecutionCommandAttributes(a) => {
                        a.retry_policy = Some(policy.clone());
                        "invalid WorkflowRetryPolicy on StartChildWorkflowExecutionCommand: "
                    }
                    Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(a) => {
                        a.retry_policy = Some(policy.clone());
                        "invalid WorkflowRetryPolicy on ContinueAsNewWorkflowExecutionCommand: "
                    }
                    _ => continue,
                };
                let command = proto_command_to_workflow_command(
                    command::Command {
                        attributes: Some(attributes),
                        ..Default::default()
                    },
                    "default",
                )
                .unwrap();
                if let Some(error) = error {
                    let (state, request) = completion(command);
                    let actual = BasicKernel
                        .apply(
                            LoadedRun::Existing(state),
                            Command::WorkflowTaskCompleted(request),
                        )
                        .unwrap_err();
                    let context = match cause {
                        Cause::BadStartChildExecutionAttributes => {
                            ". WorkflowId=child WorkflowType=wf Namespace=default"
                        }
                        Cause::BadContinueAsNewAttributes => {
                            ". WorkflowType=wf TaskQueue=name:\"queue\" kind:TASK_QUEUE_KIND_NORMAL"
                        }
                        _ => "",
                    };
                    assert_eq!(
                        actual,
                        Reject::InvalidCommandAttributes {
                            cause,
                            message: Some(format!(
                                "{prefix}invalid {name} set on retry policy: {error}{context}"
                            ))
                        }
                    );
                } else {
                    let policy = match command.semantic_command() {
                        WorkflowCommand::ScheduleActivity { retry_policy, .. }
                        | WorkflowCommand::StartChildWorkflow { retry_policy, .. }
                        | WorkflowCommand::ContinueAsNew { retry_policy, .. } => {
                            retry_policy.as_ref().unwrap()
                        }
                        _ => panic!("unexpected command"),
                    };
                    assert_eq!(
                        if initial {
                            policy.initial_interval
                        } else {
                            policy.maximum_interval.unwrap()
                        },
                        CAP
                    );
                }
            }
        }
    }
}

#[test]
fn namespace_retention_validates_and_caps() {
    for (value, invalid) in [
        (wire(-1, 0), true),
        (wire(-1, 1), true),
        (wire(i64::MAX, i32::MAX), false),
    ] {
        let result =
            register_namespace_request_to_edge(workflowservice::RegisterNamespaceRequest {
                workflow_execution_retention_period: Some(value),
                ..Default::default()
            });
        if invalid {
            assert_eq!(
                result.unwrap_err().to_string(),
                "A valid retention period is not set on request."
            );
        } else {
            assert_eq!(result.unwrap().retention, CAP);
        }
    }
}

#[test]
fn standalone_and_activity_option_durations_validate_and_cap() {
    for field in 0..4 {
        for (value, error) in [
            (wire(-1, 0), Some("negative duration")),
            (
                wire(1, -1),
                Some("duration has seconds and nanos with different signs"),
            ),
            (wire(i64::MAX, i32::MAX), None),
        ] {
            let mut request = workflowservice::StartActivityExecutionRequest::default();
            let (slot, name) = match field {
                0 => (
                    &mut request.schedule_to_close_timeout,
                    "ScheduleToCloseTimeout",
                ),
                1 => (
                    &mut request.schedule_to_start_timeout,
                    "ScheduleToStartTimeout",
                ),
                2 => (&mut request.start_to_close_timeout, "StartToCloseTimeout"),
                _ => (&mut request.heartbeat_timeout, "HeartbeatTimeout"),
            };
            *slot = Some(value);
            let options = activity_options_to_edge(&activity_proto::ActivityOptions {
                schedule_to_close_timeout: request.schedule_to_close_timeout,
                schedule_to_start_timeout: request.schedule_to_start_timeout,
                start_to_close_timeout: request.start_to_close_timeout,
                heartbeat_timeout: request.heartbeat_timeout,
                ..Default::default()
            });
            let standalone = standalone_activity_durations(&request);
            if let Some(error) = error {
                let expected = format!("invalid {name}: {error}");
                assert_eq!(standalone.unwrap_err().to_string(), expected);
                assert_eq!(options.unwrap_err().to_string(), expected);
            } else {
                assert_eq!(standalone.unwrap()[field], 3_153_600_000_000_000_000);
                let options = options.unwrap();
                assert_eq!(
                    [
                        options.schedule_to_close_timeout,
                        options.schedule_to_start_timeout,
                        options.start_to_close_timeout,
                        options.heartbeat_timeout
                    ][field],
                    Some(CAP)
                );
            }
        }
    }
}

#[test]
fn schedule_and_multi_operation_starts_validate_and_cap() {
    for field in 0..3 {
        for (value, error) in [
            (wire(-1, 0), Some("negative duration")),
            (
                wire(-1, 1),
                Some("duration has seconds and nanos with different signs"),
            ),
            (wire(i64::MAX, i32::MAX), None),
        ] {
            let mut request = start();
            let (slot, name) = match field {
                0 => (
                    &mut request.workflow_execution_timeout,
                    "WorkflowExecutionTimeout",
                ),
                1 => (&mut request.workflow_run_timeout, "WorkflowRunTimeout"),
                _ => (&mut request.workflow_task_timeout, "WorkflowTaskTimeout"),
            };
            *slot = Some(value);
            let action =
                crate::translate::schedule::schedule_action_to_domain(schedule::ScheduleAction {
                    action: Some(schedule::schedule_action::Action::StartWorkflow(
                        workflow::NewWorkflowExecutionInfo {
                            workflow_id: request.workflow_id.clone(),
                            workflow_type: request.workflow_type.clone(),
                            task_queue: request.task_queue.clone(),
                            workflow_execution_timeout: request.workflow_execution_timeout,
                            workflow_run_timeout: request.workflow_run_timeout,
                            workflow_task_timeout: request.workflow_task_timeout,
                            ..Default::default()
                        },
                    )),
                });
            let multi =
                multi_operation_request_to_edge(workflowservice::ExecuteMultiOperationRequest {
                    namespace: "default".into(),
                    operations: vec![
                        Operation {
                            operation: Some(operation::Operation::StartWorkflow(request)),
                        },
                        Operation {
                            operation: Some(operation::Operation::UpdateWorkflow(
                                workflowservice::UpdateWorkflowExecutionRequest {
                                    namespace: "default".into(),
                                    workflow_execution: Some(proto_common::WorkflowExecution {
                                        workflow_id: "workflow".into(),
                                        run_id: String::new(),
                                    }),
                                    request: Some(update::Request {
                                        meta: Some(update::Meta {
                                            update_id: "update".into(),
                                            identity: "worker".into(),
                                        }),
                                        input: Some(update::Input {
                                            name: "update".into(),
                                            ..Default::default()
                                        }),
                                    }),
                                    ..Default::default()
                                },
                            )),
                        },
                    ],
                    ..Default::default()
                });
            if let Some(error) = error {
                let expected =
                    format!("An invalid {name}Seconds is set on request. cause: {error}");
                assert_eq!(action.unwrap_err().message(), expected);
                assert_eq!(
                    multi.unwrap_err(),
                    MultiOperationRequestError::PerOperation {
                        start: Some(expected),
                        update: None
                    }
                );
            } else {
                let action = action.unwrap().start_workflow;
                let multi = multi.unwrap().start;
                assert_eq!(
                    [
                        action.workflow_execution_timeout,
                        action.workflow_run_timeout,
                        action.workflow_task_timeout
                    ][field],
                    Some(CAP)
                );
                assert_eq!(
                    [
                        multi.workflow_execution_timeout,
                        multi.workflow_run_timeout,
                        multi.workflow_task_timeout
                    ][field],
                    Some(CAP)
                );
            }
        }
    }
}

proptest! {
    // Every wire pair is either rejected or bounded, without normalization overflow.
    #[test]
    fn duration_conversions_are_total(seconds in any::<i64>(), nanos in any::<i32>()) {
        let value = wire(seconds, nanos);
        let _ = valid_non_negative_duration(Some(&value), "duration");
        let _ = proto_duration_value(Some(&value));
        let _ = standalone_activity_durations(&workflowservice::StartActivityExecutionRequest {
            schedule_to_close_timeout: Some(value),
            schedule_to_start_timeout: Some(value),
            start_to_close_timeout: Some(value),
            heartbeat_timeout: Some(value),
            ..Default::default()
        });
        let converted = proto_duration_to_time(Some(&value));
        if let Some(duration) = converted {
            prop_assert!(duration >= time::Duration::ZERO && duration <= CAP);
        }
        for (attributes, _, _) in command_cases(value) {
            let _ = proto_command_to_workflow_command(
                command::Command {
                    attributes: Some(attributes),
                    ..Default::default()
                },
                "default",
            );
        }
    }
}

#[test]
fn activity_patch_duration_validation_respects_the_mask_and_restore() {
    let options = activity_proto::ActivityOptions {
        heartbeat_timeout: Some(wire(1, -1)),
        retry_policy: Some(proto_common::RetryPolicy {
            initial_interval: Some(wire(-1, 0)),
            ..Default::default()
        }),
        ..Default::default()
    };
    for mask in [vec![], vec!["taskQueue.name".to_owned()]] {
        assert!(activity_options_patch_to_edge(&options, &mask, false).is_ok());
    }
    assert!(activity_options_patch_to_edge(&options, &[], true).is_ok());
    assert_eq!(
        activity_options_patch_to_edge(&options, &["heartbeat_timeout".into()], false)
            .unwrap_err()
            .to_string(),
        "invalid HeartbeatTimeout: duration has seconds and nanos with different signs"
    );
    assert_eq!(
        activity_options_patch_to_edge(&options, &["retry_policy.initial_interval".into()], false)
            .unwrap_err()
            .to_string(),
        "invalid InitialInterval set on retry policy: negative duration"
    );
}
