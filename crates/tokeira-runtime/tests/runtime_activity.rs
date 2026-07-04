use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{
    Command, FieldChange, HistoryEventKind, LoadedRun, StartRequest, UpdateActivityOptionsRequest,
    WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTokenResolutionError, BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    BuildId, DeploymentId, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, RetryPolicy, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId,
    WorkflowType,
};

#[tokio::test]
async fn schedule_poll_complete_activity_produces_completed_history() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
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
        .complete_activity_task(started.token, payloads("done"), None)
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
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
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
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
        )
        .await?;

    // The retried attempt is published only after the 1s retry backoff
    // (v1.31.0 dispatches retries on a retry timer, activity.go:74 @ v1.31.0),
    // so the poll window must cover it.
    let second = runtime
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .expect("retried activity should be pollable");
    assert_eq!(second.attempt, 2);
    assert_eq!(second.activity_id, "activity-1");
    Ok(())
}

#[tokio::test]
async fn retryable_activity_failure_preserves_versioned_queue() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let deployment = DeploymentId("deploy-a".to_string());
    let build_id = BuildId("build-a".to_string());

    let policy = RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: vec![],
    };
    let _ = start_and_schedule_activity_with_version(
        &runtime,
        namespace_id,
        WorkflowId("activity-versioned-retry".to_string()),
        "activity-1",
        Some(policy),
        Some(deployment.clone()),
        Some(build_id.clone()),
    )
    .await?;

    assert!(
        runtime
            .poll_activity_task(
                activity_queue(namespace_id),
                WorkerIdentity("worker-a".to_string()),
                tokio::time::Duration::from_millis(5),
            )
            .await?
            .is_none()
    );

    let first = runtime
        .poll_activity_task(
            activity_queue_with_version(
                namespace_id,
                Some(deployment.clone()),
                Some(build_id.clone()),
            ),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("versioned first attempt should be pollable");
    assert_eq!(first.attempt, 1);

    runtime
        .fail_activity_task(
            first.token,
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
        )
        .await?;

    assert!(
        runtime
            .poll_activity_task(
                activity_queue(namespace_id),
                WorkerIdentity("worker-a".to_string()),
                tokio::time::Duration::from_millis(5),
            )
            .await?
            .is_none()
    );

    // The retried attempt is published only after the 1s retry backoff
    // (activity.go:74 @ v1.31.0), so the poll window must cover it.
    let second = runtime
        .poll_activity_task(
            activity_queue_with_version(namespace_id, Some(deployment), Some(build_id)),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .expect("versioned retry should be pollable");
    assert_eq!(second.attempt, 2);
    Ok(())
}

#[tokio::test]
async fn non_retryable_activity_failure_submits_failed_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
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
        .fail_activity_task(
            started.token,
            Payload::new(b"boom".to_vec()),
            Some("fatal".to_string()),
            false,
            None,
        )
        .await?;

    let history = store.read_history(run_key, 0, 64).await?;
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskFailed { activity_id, failure, .. }
            if activity_id == "activity-1" && failure.data == b"boom"
    )));
    Ok(())
}

#[tokio::test]
async fn republish_activity_queue_after_restart_restores_pollability() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();

    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let _ = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-republish".to_string()),
        "activity-1",
        None,
    )
    .await?;

    let restarted = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
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

#[tokio::test]
async fn resolve_activity_token_matches_started_activity() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-by-id-token".to_string()),
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

    let resolved = runtime
        .resolve_activity_token(run_key, "activity-1")
        .await
        .expect("started activity should resolve to token");

    assert_eq!(resolved, started.token);
    Ok(())
}

#[tokio::test]
async fn resolve_activity_token_distinguishes_missing_and_not_started() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-by-id-errors".to_string()),
        "activity-1",
        None,
    )
    .await?;

    let not_started = runtime
        .resolve_activity_token(run_key, "activity-1")
        .await
        .expect_err("scheduled activity should not produce a completion token");
    assert!(matches!(
        not_started,
        ActivityTokenResolutionError::ActivityNotStarted { .. }
    ));

    let missing = runtime
        .resolve_activity_token(run_key, "missing-activity")
        .await
        .expect_err("missing activity should not resolve");
    assert!(matches!(
        missing,
        ActivityTokenResolutionError::ActivityNotFound { .. }
    ));

    let missing_run = runtime
        .resolve_activity_token(tokeira_types::RunKey::new(), "activity-1")
        .await
        .expect_err("missing run should not resolve");
    assert!(matches!(
        missing_run,
        ActivityTokenResolutionError::RunNotFound { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn cancel_activity_task_emits_canceled_history_with_worker_identity() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-cancel".to_string()),
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
    let identity = WorkerIdentity("worker-a".to_string());
    let details = payloads("cancel-details");

    runtime
        .cancel_activity_task(started.token, Some(details.clone()), Some(identity.clone()))
        .await?;

    let history = store.read_history(run_key, 0, 64).await?;
    assert!(history.iter().any(|event| matches!(
        &event.kind,
        HistoryEventKind::ActivityTaskCanceled {
            activity_id,
            identity: event_identity,
            details: event_details,
            ..
        } if activity_id == "activity-1"
            && event_identity.as_ref() == Some(&identity)
            && event_details.as_ref() == Some(&details)
    )));
    Ok(())
}

#[tokio::test]
async fn update_activity_options_applies_field_changes_to_scheduled_activity() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("activity-options".to_string()),
        "activity-1",
        None,
    )
    .await?;

    runtime
        .submit(
            run_key,
            Command::UpdateActivityOptions(UpdateActivityOptionsRequest {
                activity_id: "activity-1".to_string(),
                task_queue: FieldChange::Set(TaskQueueName("activity-q-b".to_string())),
                schedule_to_close_timeout: FieldChange::Set(Some(Duration::minutes(9))),
                schedule_to_start_timeout: FieldChange::Unchanged,
                start_to_close_timeout: FieldChange::Set(Some(Duration::minutes(2))),
                heartbeat_timeout: FieldChange::Clear,
                request: RequestContext {
                    request_id: RequestId("req-update-activity-options".to_string()),
                    caller_identity: Some("operator".to_string()),
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
            }),
        )
        .await?;

    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        panic!("run should exist after update");
    };
    let activity = state
        .activities
        .get("activity-1")
        .expect("activity should still be tracked");

    assert_eq!(
        activity.task_queue,
        TaskQueueName("activity-q-b".to_string())
    );
    assert_eq!(
        activity.schedule_to_close_timeout,
        Some(Duration::minutes(9))
    );
    assert_eq!(activity.start_to_close_timeout, Some(Duration::minutes(2)));
    assert_eq!(activity.heartbeat_timeout, None);
    Ok(())
}

async fn start_and_schedule_activity(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    activity_id: &str,
    retry_policy: Option<RetryPolicy>,
) -> Result<tokeira_types::RunKey> {
    start_and_schedule_activity_with_version(
        runtime,
        namespace_id,
        workflow_id,
        activity_id,
        retry_policy,
        None,
        None,
    )
    .await
}

async fn start_and_schedule_activity_with_version(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    activity_id: &str,
    retry_policy: Option<RetryPolicy>,
    deployment: Option<DeploymentId>,
    build_id: Option<BuildId>,
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
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleActivity {
                activity_id: activity_id.to_string(),
                activity_type: "activity-type".to_string(),
                task_queue: TaskQueueName("activity-q".to_string()),
                input: payloads("input"),
                header: None,
                request_eager_execution: false,
                retry_policy,
                deployment,
                build_id,
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
    let run_id = tokeira_types::RunId::new();
    StartRequest {
        run_key: tokeira_types::RunKey::new(),
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType("example".to_string()),
        task_queue: TaskQueueName("workflow-q".to_string()),
        input: Payloads::default(),
        header: None,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
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
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: None,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        reserved_poller_identity: None,
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
    activity_queue_with_version(namespace_id, None, None)
}

fn activity_queue_with_version(
    namespace_id: NamespaceId,
    deployment: Option<DeploymentId>,
    build_id: Option<BuildId>,
) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("activity-q".to_string()),
        task_kind: TaskKind::Activity,
        deployment,
        build_id,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload {
        metadata: std::collections::BTreeMap::new(),
        data: value.as_bytes().to_vec(),
    }])
}
