use std::{sync::Arc, time::Instant};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, SearchAttributes,
    TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn external_signal_delivery_signals_target_and_resolves_originator() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let originator_id = WorkflowId("originator-signal".into());
    let target_id = WorkflowId("target-signal".into());

    let target_run_key = start_workflow(
        &runtime,
        namespace_id,
        target_id.clone(),
        "target-q",
        "req-target",
    )
    .await?;
    let originator_run_key = start_workflow(
        &runtime,
        namespace_id,
        originator_id.clone(),
        "originator-q",
        "req-originator",
    )
    .await?;

    let originator_task = poll_wft(&runtime, workflow_queue(namespace_id, "originator-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: originator_task.token,
            identity: WorkerIdentity("worker-originator".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: namespace_id,
                target_namespace: None,
                target_workflow_id: target_id.clone(),
                target_run_id: None,
                signal_name: "poke".into(),
                input: payloads("signal-input"),
                header: None,
                control: "ctl".into(),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, target_run_key, |history| {
        history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } if signal_name == "poke"
        ))
    })
    .await?;

    wait_for_history(&store, originator_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::ExternalWorkflowExecutionSignaled { target_workflow_id, .. }
                    if target_workflow_id == &target_id
            )
        })
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn external_cancel_delivery_requests_cancel_on_target_and_resolves_originator() -> Result<()>
{
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let originator_id = WorkflowId("originator-cancel".into());
    let target_id = WorkflowId("target-cancel".into());

    let target_run_key = start_workflow(
        &runtime,
        namespace_id,
        target_id.clone(),
        "target-q",
        "req-target",
    )
    .await?;
    let originator_run_key = start_workflow(
        &runtime,
        namespace_id,
        originator_id.clone(),
        "originator-q",
        "req-originator",
    )
    .await?;

    let originator_task = poll_wft(&runtime, workflow_queue(namespace_id, "originator-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: originator_task.token,
            identity: WorkerIdentity("worker-originator".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::RequestCancelExternalWorkflowExecution {
                target_namespace_id: namespace_id,
                target_namespace: None,
                target_workflow_id: target_id.clone(),
                target_run_id: None,
                control: "ctl".into(),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, target_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::WorkflowExecutionCancelRequested {
                    external_workflow_execution: Some(external),
                    ..
                } if external.workflow_id == originator_id
            )
        })
    })
    .await?;

    wait_for_history(&store, originator_run_key, |history| {
        history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::ExternalWorkflowExecutionCancelRequested { target_workflow_id, .. }
                if target_workflow_id == &target_id
        ))
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn external_signal_cross_namespace_uses_target_namespace() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let originator_ns = NamespaceId::new();
    let target_ns = NamespaceId::new();
    let originator_id = WorkflowId("originator-cross".into());
    let target_id = WorkflowId("target-cross".into());

    let target_run_key = start_workflow(
        &runtime,
        target_ns,
        target_id.clone(),
        "target-q",
        "req-target",
    )
    .await?;
    let originator_run_key = start_workflow(
        &runtime,
        originator_ns,
        originator_id,
        "originator-q",
        "req-originator",
    )
    .await?;

    let originator_task = poll_wft(&runtime, workflow_queue(originator_ns, "originator-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: originator_task.token,
            identity: WorkerIdentity("worker-originator".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: target_ns,
                target_namespace: None,
                target_workflow_id: target_id.clone(),
                target_run_id: None,
                signal_name: "cross".into(),
                input: payloads("cross-input"),
                header: None,
                control: "ctl".into(),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, target_run_key, |history| {
        history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } if signal_name == "cross"
        ))
    }).await?;

    wait_for_history(&store, originator_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::ExternalWorkflowExecutionSignaled { target_workflow_id, .. }
                    if target_workflow_id == &target_id
            )
        })
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn external_signal_not_found_delivers_failed_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let originator_id = WorkflowId("originator-missing".into());
    let missing_target = WorkflowId("missing-target".into());

    let originator_run_key = start_workflow(
        &runtime,
        namespace_id,
        originator_id,
        "originator-q",
        "req-originator",
    )
    .await?;

    let originator_task = poll_wft(&runtime, workflow_queue(namespace_id, "originator-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: originator_task.token,
            identity: WorkerIdentity("worker-originator".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::SignalExternalWorkflowExecution {
                target_namespace_id: namespace_id,
                target_namespace: None,
                target_workflow_id: missing_target.clone(),
                target_run_id: None,
                signal_name: "missing".into(),
                input: payloads("missing-input"),
                header: None,
                control: "ctl".into(),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, originator_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::SignalExternalWorkflowExecutionFailed {
                    target_workflow_id,
                    cause,
                    ..
                } if target_workflow_id == &missing_target && cause.contains("not found")
            )
        })
    })
    .await?;

    Ok(())
}

fn runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

async fn start_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    task_queue: &str,
    request_id: &str,
) -> Result<tokeira_types::RunKey> {
    let result = runtime
        .start_workflow(start_request(
            namespace_id,
            workflow_id,
            task_queue,
            request_id,
        ))
        .await?;
    Ok(applied_state(&result).run_key)
}

async fn poll_wft(
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: QueueKey,
) -> Result<tokeira_runtime::StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".into()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected workflow task"))
}

async fn wait_for_history<F>(
    store: &InMemoryStore,
    run_key: tokeira_types::RunKey,
    predicate: F,
) -> Result<()>
where
    F: Fn(&[HistoryEvent]) -> bool,
{
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for external-workflow history condition");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn applied_state(result: &CommitResult) -> tokeira_kernel::WorkflowState {
    match result {
        CommitResult::Applied { new_state } => new_state.clone(),
        other => panic!("expected applied result, got {other:?}"),
    }
}

fn start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    task_queue: &str,
    request_id: &str,
) -> StartRequest {
    let run_id = tokeira_types::RunId::new();
    StartRequest {
        run_key: tokeira_types::RunKey::new(),
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType("example".into()),
        task_queue: TaskQueueName(task_queue.into()),
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
            request_id: RequestId(request_id.into()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}

fn workflow_queue(namespace_id: NamespaceId, name: &str) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
        external_payloads: Vec::new(),
    }])
}
