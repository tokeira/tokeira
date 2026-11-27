//! Retry-policy admission observed through workflow-task completion and history.

use super::*;
use tokeira_proto::{
    common::{ActivityType, RetryPolicy, WorkflowExecution, WorkflowType},
    history::{HistoryEvent, history_event::Attributes},
    public::temporal::api::command::v1::{Command, ScheduleActivityTaskCommandAttributes, command},
    taskqueue::TaskQueue,
};

async fn schedule(initial_years: i64, attempts: i32) -> (Option<tonic::Status>, Vec<HistoryEvent>) {
    let grpc = build_grpc(Arc::new(InMemoryStore::default())).await;
    let queue = Some(TaskQueue {
        name: "policy".into(),
        ..Default::default()
    });
    let run = grpc
        .start_workflow_execution(Request::new(
            workflowservice::StartWorkflowExecutionRequest {
                namespace: "default".into(),
                workflow_id: "policy".into(),
                workflow_type: Some(WorkflowType {
                    name: "policy".into(),
                }),
                task_queue: queue.clone(),
                request_id: "start".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    let task = grpc
        .poll_workflow_task_queue(Request::new(PollWorkflowTaskQueueRequest {
            namespace: "default".into(),
            task_queue: queue.clone(),
            identity: "worker".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!task.task_token.is_empty());
    let result = grpc
        .respond_workflow_task_completed(Request::new(RespondWorkflowTaskCompletedRequest {
            namespace: "default".into(),
            task_token: task.task_token,
            identity: "worker".into(),
            commands: vec![Command {
                command_type: enums::CommandType::ScheduleActivityTask as i32,
                attributes: Some(command::Attributes::ScheduleActivityTaskCommandAttributes(
                    ScheduleActivityTaskCommandAttributes {
                        activity_id: "activity".into(),
                        activity_type: Some(ActivityType {
                            name: "activity".into(),
                        }),
                        task_queue: queue,
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: 60,
                            nanos: 0,
                        }),
                        retry_policy: Some(RetryPolicy {
                            initial_interval: Some(prost_types::Duration {
                                seconds: initial_years * 365 * 24 * 60 * 60,
                                nanos: 0,
                            }),
                            maximum_attempts: attempts,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await;
    let history = grpc
        .get_workflow_execution_history(Request::new(
            workflowservice::GetWorkflowExecutionHistoryRequest {
                namespace: "default".into(),
                execution: Some(WorkflowExecution {
                    workflow_id: "policy".into(),
                    run_id: run.run_id,
                }),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(history.next_page_token.is_empty());
    (result.err(), history.history.unwrap().events)
}

#[tokio::test]
async fn wrapped_activity_maximum_fails_workflow_task() {
    // Default-before-Validate produces a negative maximum and the command cause
    // (common/retrypolicy/retry_policy.go:49-50,76-77;
    // service/history/api/command_attr_validator.go:81,123-125 @ v1.31.0).
    let (error, events) = schedule(3, 0).await;
    let message = "BadScheduleActivityAttributes: invalid MaximumInterval set on retry policy: negative duration";
    let error = error.unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(error.message(), message);
    let failed: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(Attributes::WorkflowTaskFailedEventAttributes(attrs)) => Some(attrs),
            _ => None,
        })
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].cause,
        enums::WorkflowTaskFailedCause::BadScheduleActivityAttributes as i32
    );
    assert_eq!(failed[0].failure.as_ref().unwrap().message, message);
    assert!(!events.iter().any(|event| matches!(
        &event.attributes,
        Some(Attributes::ActivityTaskScheduledEventAttributes(_))
    )));
}

#[tokio::test]
async fn one_attempt_records_two_hundred_year_maximum() {
    // Validate's one-attempt exception bypasses the cap, and history records the
    // defaulted policy (common/retrypolicy/retry_policy.go:65-69;
    // service/history/historybuilder/event_factory.go:245 @ v1.31.0).
    let (error, events) = schedule(2, 1).await;
    assert!(error.is_none(), "{error:?}");
    let scheduled: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.attributes {
            Some(Attributes::ActivityTaskScheduledEventAttributes(attrs)) => Some(attrs),
            _ => None,
        })
        .collect();
    assert_eq!(scheduled.len(), 1);
    let policy = scheduled[0].retry_policy.as_ref().unwrap();
    assert_eq!(
        policy.maximum_interval,
        Some(prost_types::Duration {
            seconds: 6_307_200_000,
            nanos: 0
        })
    );
    assert_eq!(policy.maximum_attempts, 1);
}
