use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{
    ActivityPriorityPatch, Command, FieldChange, HistoryEventKind, LoadedRun, StartRequest,
    UpdateActivityOptionsRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTokenResolutionError, BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    BuildId, DeploymentId, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, RetryPolicy, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity,
    WorkerTaskClass, WorkflowId, WorkflowType,
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
        .complete_activity_task(
            started.token,
            payloads("done"),
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
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
    assert!(
        store
            .read_history(first.run_key, 0, 64)
            .await?
            .iter()
            .all(|event| !matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. })),
        "a retry-policy activity start remains transient until terminal resolution"
    );

    runtime
        .fail_activity_task(
            first.token,
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
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
    assert!(
        store
            .read_history(second.run_key, 0, 64)
            .await?
            .iter()
            .all(|event| !matches!(
                event.kind,
                HistoryEventKind::ActivityTaskStarted { .. }
                    | HistoryEventKind::ActivityTaskFailed { .. }
            )),
        "a retryable attempt writes neither Started nor Failed history"
    );
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
    assert_eq!(first.origin.namespace_id, namespace_id);
    assert_eq!(first.origin.normal_task_queue.0, "activity-q");
    assert_eq!(first.origin.task_class, WorkerTaskClass::Activity);
    assert_eq!(first.origin.deployment, deployment);
    assert_eq!(first.origin.build_id, build_id);

    runtime
        .fail_activity_task(
            first.token,
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
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
            activity_queue_with_version(
                namespace_id,
                Some(deployment.clone()),
                Some(build_id.clone()),
            ),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .expect("versioned retry should be pollable");
    assert_eq!(second.attempt, 2);
    assert_eq!(second.origin.normal_task_queue.0, "activity-q");
    assert_eq!(second.origin.task_class, WorkerTaskClass::Activity);
    assert_eq!(second.origin.deployment, deployment);
    assert_eq!(second.origin.build_id, build_id);
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
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
        .await?;

    let history = store.read_history(run_key, 0, 64).await?;
    let terminal = history
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                HistoryEventKind::ActivityTaskStarted { .. }
                    | HistoryEventKind::ActivityTaskFailed { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal.len(), 2);
    let HistoryEventKind::ActivityTaskStarted {
        activity_id,
        identity,
        ..
    } = &terminal[0].kind
    else {
        panic!("transient start must materialize before terminal failure");
    };
    assert_eq!(activity_id, "activity-1");
    assert_eq!(identity.0, "worker-a");
    let HistoryEventKind::ActivityTaskFailed {
        activity_id,
        started_event_id,
        failure,
        ..
    } = &terminal[1].kind
    else {
        panic!("terminal failure must follow the materialized start");
    };
    assert_eq!(activity_id, "activity-1");
    assert_eq!(*started_event_id, terminal[0].event_id);
    assert_eq!(failure.data, b"boom");
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
        .cancel_activity_task(
            started.token,
            Some(details.clone()),
            Some(identity.clone()),
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
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
                target: tokeira_kernel::ActivityControlTarget::Id("activity-1".to_string()),
                task_queue: FieldChange::Set(TaskQueueName("activity-q-b".to_string())),
                schedule_to_close_timeout: FieldChange::Set(Some(Duration::minutes(9))),
                schedule_to_start_timeout: FieldChange::Unchanged,
                start_to_close_timeout: FieldChange::Set(Some(Duration::minutes(2))),
                heartbeat_timeout: FieldChange::Clear,
                retry_policy: tokeira_kernel::ActivityRetryPolicyPatch::default(),
                priority: ActivityPriorityPatch::Unchanged,
                original_options: std::collections::BTreeMap::new(),
                restore_original_options: false,
                reschedule_at: std::collections::BTreeMap::new(),
                request: RequestContext {
                    request_id: RequestId("req-update-activity-options".to_string()),
                    caller_identity: Some("operator".to_string()),
                    principal: None,
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
            client_discards_speculative_with_events: false,
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
                priority: None,
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
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
        initiator: None,
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
        parent_namespace_name: None,
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
            principal: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
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
        external_payloads: Vec::new(),
    }])
}

/// Runtime with an aggressive activity scanner so durable-dispatch
/// reconciliation is observable within test time, and a reconciliation
/// budget the starvation test can pin to one row per pass.
fn reconciling_runtime(
    store: Arc<InMemoryStore>,
    max_dispatch_reconciliations_per_scan: usize,
) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new_with_nexus(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        tokeira_runtime::ActivityTimeoutScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(50),
            max_timeouts_per_scan: 100,
            max_dispatch_reconciliations_per_scan,
        },
        tokeira_runtime::NexusTimeoutScannerConfig::default(),
        tokeira_runtime::NexusEndpointRegistry::default(),
        Arc::new(tokeira_runtime::NoopNexusHttpClient),
        tokeira_runtime::NexusCompletionDeps::default(),
    )
}

/// Poll until an activity task arrives; scanner-observable synchronization
/// with a bounded ceiling that protects the suite, not the ordering.
async fn poll_activity_until_delivered(
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: &QueueKey,
    ceiling: tokio::time::Duration,
) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
    let deadline = tokio::time::Instant::now() + ceiling;
    while tokio::time::Instant::now() < deadline {
        if let Some(started) = runtime
            .poll_activity_task(
                queue.clone(),
                WorkerIdentity("worker-b".to_string()),
                tokio::time::Duration::from_millis(50),
            )
            .await?
        {
            return Ok(Some(started));
        }
    }
    Ok(None)
}

fn activity_pause_rule(id: &str, predicate: &str) -> tokeira_types::WorkflowRuleRecord {
    tokeira_types::WorkflowRuleRecord {
        id: id.to_string(),
        create_time: OffsetDateTime::UNIX_EPOCH,
        created_by_identity: "rule-owner".to_string(),
        description: "policy pause".to_string(),
        trigger: tokeira_types::WorkflowRuleTrigger::ActivityStart {
            predicate: predicate.to_string(),
        },
        visibility_query: String::new(),
        actions: vec![tokeira_types::WorkflowRuleAction::ActivityPause],
        expiration_time: None,
    }
}

/// Feature: durable-activity-dispatch — the focused loss regression. An offer
/// removed from the broker whose caller vanishes before `Started` commits must
/// be rediscovered from the durable dispatch row by the live scanner, without
/// shard takeover or a manual republish. Fails on the pre-fix engine: nothing
/// republishes and the poll ceiling below expires empty.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_offer_is_rediscovered_by_durable_dispatch_reconciliation() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = reconciling_runtime(store.clone(), 100);
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("dropped-offer".to_string()),
        "activity-1",
        None,
    )
    .await?;

    let offer = runtime
        .poll_activity_task_offer(
            activity_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("scheduled activity must be offerable");
    let offered_attempt = offer.task.attempt;
    // The caller disappears between broker take and durable start.
    drop(offer);

    let started = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_secs(10),
    )
    .await?
    .expect("durable dispatch row must be republished by the live scanner");
    // Same attempt, and the Started commit's stamp fence accepted it — the
    // republished offer carried the identical `(attempt, stamp)` identity.
    assert_eq!(started.run_key, run_key);
    assert_eq!(started.attempt, offered_attempt);

    runtime
        .complete_activity_task(
            started.token,
            payloads("done"),
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
        .await?;

    // Workflow progress: completion schedules the next workflow task.
    let next_wft = runtime
        .poll_workflow_task(
            workflow_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?;
    assert!(
        next_wft.is_some(),
        "completion must schedule a workflow task"
    );

    // Exactly one Started transition materialized.
    let history = store.read_history(run_key, 0, 128).await?;
    let started_events = history
        .iter()
        .filter(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. }))
        .count();
    assert_eq!(started_events, 1, "recovered offer must start exactly once");

    // No later usable duplicate: the scanner keeps ticking and must not
    // republish the completed attempt.
    let duplicate = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_millis(500),
    )
    .await?;
    assert!(duplicate.is_none(), "no duplicate offer may remain usable");
    Ok(())
}

/// A retry inside its backoff window is durably present but not deliverable:
/// neither the reconciler nor a shard-recovery sweep publishes it early, and
/// once due it arrives through the live scanner with no takeover involved.
#[tokio::test(flavor = "multi_thread")]
async fn retry_backoff_is_respected_by_reconciliation_and_recovery() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = reconciling_runtime(store.clone(), 100);
    let namespace_id = NamespaceId::new();
    let policy = RetryPolicy {
        initial_interval: Duration::seconds(2),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: vec![],
    };
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("backoff-respected".to_string()),
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
    runtime
        .fail_activity_task(
            first.token,
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
        .await?;

    // The durable row exists immediately (inspection view) but delivery
    // withholds it: several scanner ticks pass within this window and none
    // may surface the future retry.
    assert_eq!(
        store
            .list_all_dispatchable_activity_tasks(&activity_queue(namespace_id), 10)
            .await?
            .len(),
        1
    );
    let early = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_millis(600),
    )
    .await?;
    assert!(
        early.is_none(),
        "a retry must not be delivered inside its backoff"
    );

    // A shard-recovery sweep during the window must not publish it either.
    let recovered = TokeiraRuntime::new_with_nexus_and_shards(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        tokeira_runtime::ActivityTimeoutScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(50),
            max_timeouts_per_scan: 100,
            max_dispatch_reconciliations_per_scan: 100,
        },
        tokeira_runtime::NexusTimeoutScannerConfig::default(),
        tokeira_runtime::NexusEndpointRegistry::default(),
        Arc::new(tokeira_runtime::NoopNexusHttpClient),
        tokeira_runtime::NexusCompletionDeps::default(),
        1,
        "backoff-restart-owner".to_string(),
        false,
    );
    recovered.acquire_shard(tokeira_types::ShardId(0)).await?;
    let immediately_after_recovery = recovered
        .poll_activity_task(
            activity_queue(namespace_id),
            WorkerIdentity("worker-b".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?;
    assert!(
        immediately_after_recovery.is_none(),
        "recovery must not publish a retry before its backoff elapses"
    );

    // Once due, the recovered node's LIVE scanner delivers it — no second
    // takeover, no manual republish.
    let second = poll_activity_until_delivered(
        &recovered,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_secs(10),
    )
    .await?
    .expect("due retry must be delivered by the live scanner");
    assert_eq!(second.run_key, run_key);
    assert_eq!(second.attempt, 2);
    Ok(())
}

/// Repeated reconciliation while an offer is parked in the broker is
/// suppressed by delivery dedupe: after several scanner passes there is
/// exactly one usable offer, and starting it exactly one Started transition.
#[tokio::test(flavor = "multi_thread")]
async fn parked_offer_is_not_duplicated_by_reconciliation() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = reconciling_runtime(store.clone(), 100);
    let namespace_id = NamespaceId::new();
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("parked-dedupe".to_string()),
        "activity-1",
        None,
    )
    .await?;

    // Let several reconciliation passes run against the parked offer before
    // any poller shows up (scanner ticks every 50ms).
    tokio::time::timeout(
        tokio::time::Duration::from_millis(400),
        std::future::pending::<()>(),
    )
    .await
    .ok();

    let started = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_secs(5),
    )
    .await?
    .expect("parked offer must be deliverable");
    assert_eq!(started.run_key, run_key);
    let duplicate = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_millis(500),
    )
    .await?;
    assert!(
        duplicate.is_none(),
        "reconciliation of a parked offer must dedupe, not duplicate"
    );
    let history = store.read_history(run_key, 0, 128).await?;
    let started_events = history
        .iter()
        .filter(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. }))
        .count();
    assert_eq!(started_events, 1);
    Ok(())
}

/// A `BackoffInterval` workflow rule installed AFTER the retry publication but
/// BEFORE start is still enforced at the start gate, evaluated from the
/// durable `dispatch_at - last_attempt_complete_time` interval.
#[tokio::test(flavor = "multi_thread")]
async fn backoff_interval_rule_between_publication_and_start_pauses_durably() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = reconciling_runtime(store.clone(), 100);
    let namespace_id = NamespaceId::new();
    let policy = RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 5,
        non_retryable_error_types: vec![],
    };
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("rule-before-start".to_string()),
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
        .expect("first attempt");
    runtime
        .fail_activity_task(
            first.token,
            Payload::new(b"boom".to_vec()),
            Some("retryable".to_string()),
            false,
            None,
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
        .await?;

    // Take the published attempt-2 offer WITHOUT starting it, then install
    // the rule — the exact window between publication and Started.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    let offer = loop {
        if let Some(offer) = runtime
            .poll_activity_task_offer(
                activity_queue(namespace_id),
                WorkerIdentity("worker-a".to_string()),
                tokio::time::Duration::from_millis(50),
            )
            .await?
        {
            break offer;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "attempt 2 offer must publish after its backoff"
        );
    };
    assert_eq!(offer.task.attempt, 2);
    store
        .create_workflow_rule(
            namespace_id,
            activity_pause_rule("backoff-gate", "BackoffInterval >= 1"),
            10,
        )
        .await?;

    let started = runtime
        .start_activity_task_offer(offer, WorkerIdentity("worker-a".to_string()))
        .await?;
    assert!(
        started.is_none(),
        "the start gate must enforce the rule installed after publication"
    );
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        panic!("run must exist");
    };
    let activity = &state.activities["activity-1"];
    assert_eq!(
        activity
            .pause_info
            .as_ref()
            .and_then(|pause| pause.rule_id.as_deref()),
        Some("backoff-gate")
    );
    // The stamp advanced, fencing the taken offer's identity.
    assert!(activity.stamp > 0);
    assert!(
        store
            .list_all_dispatchable_activity_tasks(&activity_queue(namespace_id), 10)
            .await?
            .is_empty(),
        "a durably paused activity leaves no dispatch row"
    );
    let after = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_millis(500),
    )
    .await?;
    assert!(
        after.is_none(),
        "no usable offer may remain after the pause"
    );
    Ok(())
}

/// Pinned v1.31.0 first-attempt semantics: every `BackoffInterval` predicate —
/// including equality with zero — is a clean non-match on attempt one, so the
/// first attempt starts normally under such a rule.
#[tokio::test(flavor = "multi_thread")]
async fn backoff_interval_rule_is_clean_non_match_for_first_attempt() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = reconciling_runtime(store.clone(), 100);
    let namespace_id = NamespaceId::new();
    store
        .create_workflow_rule(
            namespace_id,
            activity_pause_rule("zero-backoff", "BackoffInterval = 0"),
            10,
        )
        .await?;
    let run_key = start_and_schedule_activity(
        &runtime,
        namespace_id,
        WorkflowId("first-attempt-non-match".to_string()),
        "activity-1",
        None,
    )
    .await?;
    let started = poll_activity_until_delivered(
        &runtime,
        &activity_queue(namespace_id),
        tokio::time::Duration::from_secs(5),
    )
    .await?
    .expect("attempt one must start: BackoffInterval is absent, not zero");
    assert_eq!(started.run_key, run_key);
    assert_eq!(started.attempt, 1);
    Ok(())
}
