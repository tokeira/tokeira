use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{
    HistoryEventKind, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{LaneConfig, TokeiraRuntime};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId, RetryPolicy,
    SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn schedule_poll_complete_activity_produces_completed_history() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(store.clone(), 2, LaneConfig::default());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("activity-complete".to_string());
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        workflow_id.clone(),
        "activity-1",
        None,
    )
    .await?;

    let started = runtime
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("activity should be pollable");
    assert_eq!(started.activity_id, "activity-1");

    let _ = runtime
        .complete_activity_task(started.token, payloads("done"))
        .await?;

    let history = store.read_history(run_key, 0, 64).await?;
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCompleted { activity_id, .. } if activity_id == "activity-1"
    )));
    Ok(())
}

#[tokio::test]
async fn retryable_activity_failure_redispatches_next_attempt() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(store.clone(), 2, LaneConfig::default());
    let namespace_id = NamespaceId::new();

    let policy = RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: vec!["fatal".to_string()],
    };
    let _ = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-retry".to_string()),
        "activity-1",
        Some(policy),
    )
    .await?;

    let first = runtime
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("first attempt should be pollable");
    assert_eq!(first.attempt, 1);

    runtime
        .fail_activity_task(
            first.token,
            "boom".to_string(),
            Some("retryable".to_string()),
        )
        .await?;

    let second = runtime
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("retried activity should be pollable");
    assert_eq!(second.attempt, 2);
    assert_eq!(second.activity_id, "activity-1");
    Ok(())
}

#[tokio::test]
async fn non_retryable_activity_failure_submits_failed_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(store.clone(), 2, LaneConfig::default());
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-fail".to_string()),
        "activity-1",
        Some(RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 5,
            non_retryable_error_types: vec!["fatal".to_string()],
        }),
    )
    .await?;

    let started = runtime
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("activity should be pollable");

    runtime
        .fail_activity_task(started.token, "boom".to_string(), Some("fatal".to_string()))
        .await?;

    let history = store.read_history(run_key, 0, 64).await?;
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskFailed { activity_id, message } if activity_id == "activity-1" && message == "boom"
    )));
    Ok(())
}

#[tokio::test]
async fn republish_activity_queue_after_restart_restores_pollability() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();

    let runtime = TokeiraRuntime::new(store.clone(), 2, LaneConfig::default());
    let _ = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-republish".to_string()),
        "activity-1",
        None,
    )
    .await?;

    let restarted = TokeiraRuntime::new(store.clone(), 2, LaneConfig::default());
    let count = restarted
        .republish_activity_queue(activity_queue(namespace_id), 10)
        .await?;
    assert!(count >= 1);

    let started = restarted
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?;
    assert!(started.is_some());
    Ok(())
}

async fn start_and_schedule_activity(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    activity_id: &str,
    retry_policy: Option<RetryPolicy>,
) -> Result<tokeira_types::RunKey> {
    let start = runtime
        .start_workflow(start_request(
            namespace_id,
            workflow_id.clone(),
            "req-start",
        ))
        .await?;
    let run_key = match start {
        CommitResult::Applied { new_state } => new_state.run_key,
        other => panic!("unexpected start result: {other:?}"),
    };

    let workflow_task = runtime
        .poll_workflow_task(
            workflow_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("workflow task should be pollable");

    let _ = runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: workflow_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            commands: vec![WorkflowCommand::ScheduleActivity {
                activity_id: activity_id.to_string(),
                task_queue: TaskQueueName("activity-q".to_string()),
                input: payloads("input"),
                retry_policy,
                schedule_to_close_timeout: Some(Duration::minutes(5)),
                schedule_to_start_timeout: Some(Duration::seconds(30)),
                start_to_close_timeout: Some(Duration::minutes(1)),
                heartbeat_timeout: Some(Duration::seconds(20)),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    Ok(run_key)
}

fn start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    request_id: &str,
) -> StartRequest {
    StartRequest {
        run_key: tokeira_types::RunKey::new(),
        namespace_id,
        workflow_id,
        run_id: tokeira_types::RunId::new(),
        workflow_type: WorkflowType("example".to_string()),
        task_queue: TaskQueueName("workflow-q".to_string()),
        input: Payloads::default(),
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: None,
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
    }
}

fn workflow_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("workflow-q".to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn activity_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("activity-q".to_string()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload {
        metadata: std::collections::BTreeMap::new(),
        data: value.as_bytes().to_vec(),
    }])
}
