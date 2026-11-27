//! A worker awaiting cancellation observes the terminal activity event before completing.

use super::*;
use tokeira_proto::{
    public::temporal::api::{
        command::v1 as command, failure::v1 as failure, history::v1::history_event::Attributes,
    },
    workflowservice::{
        PollWorkflowTaskQueueResponse, RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedRequest,
    },
};

fn queue() -> Option<tokeira_proto::taskqueue::TaskQueue> {
    Some(tokeira_proto::taskqueue::TaskQueue {
        name: "cancel-retry".into(),
        ..Default::default()
    })
}

async fn poll(grpc: &WorkflowServiceGrpc) -> PollWorkflowTaskQueueResponse {
    let task = grpc
        .poll_workflow_task_queue(Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".into(),
            task_queue: queue(),
            identity: "worker".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!task.task_token.is_empty());
    task
}

async fn complete(
    grpc: &WorkflowServiceGrpc,
    task: PollWorkflowTaskQueueResponse,
    command: command::Command,
) {
    grpc.respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
        namespace: "default".into(),
        task_token: task.task_token,
        identity: "worker".into(),
        commands: vec![command],
        ..Default::default()
    }))
    .await
    .unwrap();
}

async fn cancellation_waits_for_failed_activity(by_id: bool) {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;
    let run_id = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".into(),
                workflow_id: "cancel-retry".into(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "cancel-retry".into(),
                }),
                task_queue: queue(),
                request_id: "start".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .run_id;
    complete(
        &grpc,
        poll(&grpc).await,
        command::Command {
            command_type: enums::CommandType::ScheduleActivityTask as i32,
            attributes: Some(
                command::command::Attributes::ScheduleActivityTaskCommandAttributes(
                    command::ScheduleActivityTaskCommandAttributes {
                        activity_id: "activity".into(),
                        activity_type: Some(tokeira_proto::common::ActivityType {
                            name: "activity".into(),
                        }),
                        task_queue: queue(),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 60,
                            nanos: 0,
                        }),
                        retry_policy: Some(tokeira_proto::common::RetryPolicy {
                            initial_interval: Some(prost_types::Duration {
                                seconds: 1,
                                nanos: 0,
                            }),
                            maximum_attempts: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        },
    )
    .await;
    let activity = grpc
        .poll_activity_task_queue(Request::new(
            workflowservice::PollActivityTaskQueueRequest {
                namespace: "default".into(),
                task_queue: queue(),
                identity: "worker".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(activity.attempt, 1);
    assert!(!activity.task_token.is_empty());
    let execution = Some(tokeira_proto::common::WorkflowExecution {
        workflow_id: "cancel-retry".into(),
        run_id: run_id.clone(),
    });
    grpc.signal_workflow_execution(Request::new(
        workflowservice::SignalWorkflowExecutionRequest {
            namespace: "default".into(),
            workflow_execution: execution.clone(),
            signal_name: "cancel".into(),
            request_id: "cancel".into(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();
    let cancellation = poll(&grpc).await;
    let scheduled_event_id = cancellation
        .history
        .as_ref()
        .unwrap()
        .events
        .iter()
        .find(|event| {
            matches!(
                event.attributes,
                Some(Attributes::ActivityTaskScheduledEventAttributes(_))
            )
        })
        .unwrap()
        .event_id;
    complete(
        &grpc,
        cancellation,
        command::Command {
            command_type: enums::CommandType::RequestCancelActivityTask as i32,
            attributes: Some(
                command::command::Attributes::RequestCancelActivityTaskCommandAttributes(
                    command::RequestCancelActivityTaskCommandAttributes { scheduled_event_id },
                ),
            ),
            ..Default::default()
        },
    )
    .await;
    let run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id: namespace_id_for("default"),
            workflow_id: WorkflowId("cancel-retry".into()),
            run_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    let LoadedRun::Existing(waiting) = store.load_run(run_key).await.unwrap() else {
        panic!("run")
    };
    assert!(waiting.activities["activity"].cancel_requested);
    assert!(waiting.pending_workflow_task.is_none());
    let failure = failure::Failure {
        message: "retryable failure".into(),
        failure_info: Some(failure::failure::FailureInfo::ApplicationFailureInfo(
            failure::ApplicationFailureInfo {
                r#type: "retryable".into(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    if by_id {
        grpc.respond_activity_task_failed_by_id(Request::new(
            RespondActivityTaskFailedByIdRequest {
                namespace: "default".into(),
                workflow_id: "cancel-retry".into(),
                run_id,
                activity_id: "activity".into(),
                failure: Some(failure.clone()),
                identity: "worker".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    } else {
        grpc.respond_activity_task_failed(Request::new(RespondActivityTaskFailedRequest {
            namespace: "default".into(),
            task_token: activity.task_token,
            failure: Some(failure.clone()),
            identity: "worker".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    }
    let LoadedRun::Existing(resolved) = store.load_run(run_key).await.unwrap() else {
        panic!("run")
    };
    assert!(
        resolved.activities.is_empty(),
        "waiting for cancellation must finish after the worker failure"
    );
    assert!(resolved.pending_workflow_task.is_some());
    let next = poll(&grpc).await;
    let history = next.history.as_ref().unwrap();
    let failed = history
        .events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(Attributes::ActivityTaskFailedEventAttributes(attrs)) => {
                Some((event.event_id, attrs))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].1.retry_state,
        enums::RetryState::CancelRequested as i32
    );
    assert_eq!(failed[0].1.failure, Some(failure));
    assert!(
        history
            .events
            .iter()
            .any(|event| event.event_id > failed[0].0
                && matches!(
                    event.attributes,
                    Some(Attributes::WorkflowTaskScheduledEventAttributes(_))
                ))
    );
    complete(
        &grpc,
        next,
        command::Command {
            command_type: enums::CommandType::CompleteWorkflowExecution as i32,
            attributes: Some(
                command::command::Attributes::CompleteWorkflowExecutionCommandAttributes(
                    command::CompleteWorkflowExecutionCommandAttributes::default(),
                ),
            ),
            ..Default::default()
        },
    )
    .await;
    let LoadedRun::Existing(completed) = store.load_run(run_key).await.unwrap() else {
        panic!("run")
    };
    assert_eq!(completed.status, tokeira_types::ExecutionStatus::Completed);
}

#[tokio::test]
async fn cancel_requested_worker_failure_by_token_releases_waiting_workflow() {
    cancellation_waits_for_failed_activity(false).await;
}

#[tokio::test]
async fn cancel_requested_worker_failure_by_id_releases_waiting_workflow() {
    cancellation_waits_for_failed_activity(true).await;
}

async fn started_activity() -> (
    Arc<InMemoryStore>,
    WorkflowServiceGrpc,
    String,
    Vec<u8>,
    tokeira_types::RunKey,
) {
    let store = Arc::new(InMemoryStore::default());
    let grpc = build_grpc(store.clone()).await;
    let run_id = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".into(),
                workflow_id: "cancel-retry".into(),
                workflow_type: Some(tokeira_proto::common::WorkflowType {
                    name: "cancel-retry".into(),
                }),
                task_queue: queue(),
                request_id: "start".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .run_id;
    complete(
        &grpc,
        poll(&grpc).await,
        command::Command {
            command_type: enums::CommandType::ScheduleActivityTask as i32,
            attributes: Some(
                command::command::Attributes::ScheduleActivityTaskCommandAttributes(
                    command::ScheduleActivityTaskCommandAttributes {
                        activity_id: "activity".into(),
                        activity_type: Some(tokeira_proto::common::ActivityType {
                            name: "activity".into(),
                        }),
                        task_queue: queue(),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 60,
                            nanos: 0,
                        }),
                        retry_policy: Some(tokeira_proto::common::RetryPolicy {
                            initial_interval: Some(prost_types::Duration {
                                seconds: 1,
                                nanos: 0,
                            }),
                            maximum_interval: Some(prost_types::Duration {
                                seconds: 2,
                                nanos: 0,
                            }),
                            maximum_attempts: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        },
    )
    .await;
    let activity = grpc
        .poll_activity_task_queue(Request::new(
            workflowservice::PollActivityTaskQueueRequest {
                namespace: "default".into(),
                task_queue: queue(),
                identity: "worker".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(activity.attempt, 1);
    assert!(!activity.task_token.is_empty());
    let run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id: namespace_id_for("default"),
            workflow_id: WorkflowId("cancel-retry".into()),
            run_id: None,
        })
        .await
        .unwrap()
        .unwrap();
    (store, grpc, run_id, activity.task_token, run_key)
}

#[tokio::test]
async fn model_requested_retry_delay_reaches_runtime_by_token_and_id() {
    // The outer application's delay replaces the policy interval AND maximum:
    // service/history/workflow/retry.go:56-68,96-112 and
    // service/history/workflow/mutable_state_impl.go:6300-6317 @ v1.31.0.
    for by_id in [false, true] {
        for delay in [
            prost_types::Duration {
                seconds: 3,
                nanos: 17,
            },
            prost_types::Duration {
                seconds: 0,
                nanos: -1,
            },
        ] {
            let (store, grpc, run_id, token, run_key) = started_activity().await;
            let failure = failure::Failure {
                failure_info: Some(failure::failure::FailureInfo::ApplicationFailureInfo(
                    failure::ApplicationFailureInfo {
                        r#type: "retryable".into(),
                        next_retry_delay: Some(delay),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            if by_id {
                grpc.respond_activity_task_failed_by_id(Request::new(
                    RespondActivityTaskFailedByIdRequest {
                        namespace: "default".into(),
                        workflow_id: "cancel-retry".into(),
                        run_id,
                        activity_id: "activity".into(),
                        failure: Some(failure),
                        identity: "worker".into(),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();
            } else {
                grpc.respond_activity_task_failed(Request::new(RespondActivityTaskFailedRequest {
                    namespace: "default".into(),
                    task_token: token,
                    failure: Some(failure),
                    identity: "worker".into(),
                    ..Default::default()
                }))
                .await
                .unwrap();
            }
            let LoadedRun::Existing(state) = store.load_run(run_key).await.unwrap() else {
                panic!("run")
            };
            let activity = &state.activities["activity"];
            assert_eq!(activity.attempt, 2);
            assert_eq!(
                activity.current_attempt_scheduled_at.unwrap()
                    - activity.last_attempt_complete_time.unwrap(),
                time::Duration::seconds(delay.seconds)
                    + time::Duration::nanoseconds(i64::from(delay.nanos))
            );
        }
    }
}

#[tokio::test]
async fn model_unsolicited_cancel_by_token_and_id_leaves_activity_open() {
    // Token validation precedes the durable cancel-bit check, which changes no state:
    // service/history/api/respondactivitytaskcanceled/api.go:81-95 and
    // service/history/consts/const.go:48-49 @ v1.31.0.
    for by_id in [false, true] {
        let (store, grpc, run_id, token, run_key) = started_activity().await;
        let before = store.load_run(run_key).await.unwrap();
        let history = store.read_history_to_end(run_key, 0).await.unwrap();
        let error = if by_id {
            grpc.respond_activity_task_canceled_by_id(Request::new(
                workflowservice::RespondActivityTaskCanceledByIdRequest {
                    namespace: "default".into(),
                    workflow_id: "cancel-retry".into(),
                    run_id,
                    activity_id: "activity".into(),
                    identity: "worker".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err()
        } else {
            grpc.respond_activity_task_canceled(Request::new(
                workflowservice::RespondActivityTaskCanceledRequest {
                    namespace: "default".into(),
                    task_token: token,
                    identity: "worker".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err()
        };
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "unable to mark activity as canceled without activity being request canceled first"
        );
        let after = store.load_run(run_key).await.unwrap();
        assert_eq!(after, before);
        assert_eq!(
            store.read_history_to_end(run_key, 0).await.unwrap(),
            history
        );
        let LoadedRun::Existing(state) = after else {
            panic!("run")
        };
        assert!(state.activities.contains_key("activity"));
    }
}
