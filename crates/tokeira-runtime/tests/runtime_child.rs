use std::{sync::Arc, time::Instant};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEventKind, ParentClosePolicy, StartRequest, WorkflowCommand,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId,
    SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn child_workflow_happy_path_delivers_start_and_completion_back_to_parent()
-> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let parent_workflow_id = WorkflowId("parent-happy".to_string());
    let child_workflow_id = WorkflowId("child-happy".to_string());

    let parent_run_key = applied_state(
        &runtime
            .start_workflow(start_request(
                namespace_id,
                parent_workflow_id.clone(),
                "req-parent-start",
                "parent-q",
            ))
            .await?,
    )
    .run_key;
    let parent_task =
        poll_wft(&runtime, workflow_queue(namespace_id, "parent-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: parent_task.token,
            identity: WorkerIdentity("worker-parent".to_string()),
            commands: vec![WorkflowCommand::StartChildWorkflow {
                child_workflow_id: child_workflow_id.clone(),
                namespace_id,
                workflow_type: WorkflowType("child-example".to_string()),
                task_queue: TaskQueueName("child-q".to_string()),
                input: payloads("child-input"),
                parent_close_policy: ParentClosePolicy::Terminate,
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    wait_for_history(&*store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                event.kind,
                HistoryEventKind::ChildWorkflowExecutionStarted { .. }
            )
        })
    })
    .await?;

    let child_run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: None,
        })
        .await?
        .expect("child run should exist");

    let child_task = poll_wft(&runtime, workflow_queue(namespace_id, "child-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: child_task.token,
            identity: WorkerIdentity("worker-child".to_string()),
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: payloads("child-result"),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    wait_for_history(&*store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::ChildWorkflowExecutionCompleted {
                    child_workflow_id: id,
                    ..
                } if id == &child_workflow_id
            )
        })
    })
    .await?;

    let child_history = store.read_history(child_run_key, 0, 64).await?;
    assert!(child_history.iter().any(|event| {
        matches!(
            event.kind,
            HistoryEventKind::WorkflowExecutionCompleted { .. }
        )
    }));

    Ok(())
}

#[tokio::test]
async fn parent_close_policy_terminate_closes_started_child() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let parent_workflow_id = WorkflowId("parent-terminate".to_string());
    let child_workflow_id = WorkflowId("child-terminate".to_string());

    let _parent_run_key = start_parent_with_child(
        &runtime,
        &*store,
        namespace_id,
        &parent_workflow_id,
        &child_workflow_id,
        ParentClosePolicy::Terminate,
    )
    .await?;

    let child_run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: None,
        })
        .await?
        .expect("child run should exist");

    let parent_task =
        poll_wft(&runtime, workflow_queue(namespace_id, "parent-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: parent_task.token,
            identity: WorkerIdentity("worker-parent".to_string()),
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: payloads("parent-done"),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    wait_for_history(&*store, child_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowExecutionTerminated { .. }
            )
        })
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn parent_close_policy_request_cancel_requests_cancel_on_child() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let parent_workflow_id = WorkflowId("parent-cancel".to_string());
    let child_workflow_id = WorkflowId("child-cancel".to_string());

    start_parent_with_child(
        &runtime,
        &*store,
        namespace_id,
        &parent_workflow_id,
        &child_workflow_id,
        ParentClosePolicy::RequestCancel,
    )
    .await?;

    let child_run_key = store
        .resolve_execution(&ExecutionRef {
            namespace_id,
            workflow_id: child_workflow_id.clone(),
            run_id: None,
        })
        .await?
        .expect("child run should exist");

    let parent_task =
        poll_wft(&runtime, workflow_queue(namespace_id, "parent-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: parent_task.token,
            identity: WorkerIdentity("worker-parent".to_string()),
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: payloads("parent-done"),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    wait_for_history(&*store, child_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowExecutionCancelRequested { .. }
            )
        })
    })
    .await?;

    Ok(())
}

#[tokio::test]
async fn duplicate_child_start_delivers_failed_confirmation_to_parent() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();
    let parent_workflow_id = WorkflowId("parent-duplicate".to_string());
    let child_workflow_id = WorkflowId("child-duplicate".to_string());

    runtime
        .start_workflow(start_request(
            namespace_id,
            child_workflow_id.clone(),
            "req-existing-child",
            "child-q",
        ))
        .await?;

    let parent_run_key = applied_state(
        &runtime
            .start_workflow(start_request(
                namespace_id,
                parent_workflow_id.clone(),
                "req-parent-start",
                "parent-q",
            ))
            .await?,
    )
    .run_key;

    let parent_task =
        poll_wft(&runtime, workflow_queue(namespace_id, "parent-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: parent_task.token,
            identity: WorkerIdentity("worker-parent".to_string()),
            commands: vec![WorkflowCommand::StartChildWorkflow {
                child_workflow_id: child_workflow_id.clone(),
                namespace_id,
                workflow_type: WorkflowType("child-example".to_string()),
                task_queue: TaskQueueName("child-q".to_string()),
                input: payloads("child-input"),
                parent_close_policy: ParentClosePolicy::Terminate,
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    wait_for_history(&*store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::StartChildWorkflowExecutionFailed {
                    child_workflow_id: id,
                    ..
                } if id == &child_workflow_id
            )
        })
    })
    .await?;

    Ok(())
}

async fn start_parent_with_child(
    runtime: &TokeiraRuntime<InMemoryStore>,
    store: &InMemoryStore,
    namespace_id: NamespaceId,
    parent_workflow_id: &WorkflowId,
    child_workflow_id: &WorkflowId,
    parent_close_policy: ParentClosePolicy,
) -> Result<tokeira_types::RunKey> {
    let parent_run_key = applied_state(
        &runtime
            .start_workflow(start_request(
                namespace_id,
                parent_workflow_id.clone(),
                "req-parent-start",
                "parent-q",
            ))
            .await?,
    )
    .run_key;

    let parent_task = poll_wft(runtime, workflow_queue(namespace_id, "parent-q")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: parent_task.token,
            identity: WorkerIdentity("worker-parent".to_string()),
            commands: vec![WorkflowCommand::StartChildWorkflow {
                child_workflow_id: child_workflow_id.clone(),
                namespace_id,
                workflow_type: WorkflowType("child-example".to_string()),
                task_queue: TaskQueueName("child-q".to_string()),
                input: payloads("child-input"),
                parent_close_policy,
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::ChildWorkflowExecutionStarted {
                    child_workflow_id: id,
                    ..
                } if id == child_workflow_id
            )
        })
    })
    .await?;

    Ok(parent_run_key)
}

async fn poll_wft(
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: QueueKey,
) -> Result<tokeira_runtime::StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
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
    F: Fn(&[tokeira_kernel::HistoryEvent]) -> bool,
{
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for child-workflow history condition");
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
    request_id: &str,
    task_queue: &str,
) -> StartRequest {
    StartRequest {
        run_key: tokeira_types::RunKey::new(),
        namespace_id,
        workflow_id,
        run_id: tokeira_types::RunId::new(),
        workflow_type: WorkflowType("example".to_string()),
        task_queue: TaskQueueName(task_queue.to_string()),
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
        parent_run_key: None,
        parent_workflow_id: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
    }
}

fn workflow_queue(namespace_id: NamespaceId, name: &str) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
    }])
}
