use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokio::time::Instant;

use tokeira_kernel::{
    HistoryEventKind, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId, SearchAttributes,
    TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn timer_scanner_fires_due_timer_end_to_end() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(10),
            max_timers_per_scan: 100,
        },
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();

    let run_key =
        start_workflow(&runtime, namespace_id, WorkflowId("timer-fire".into())).await?;
    complete_with_commands(
        &runtime,
        namespace_id,
        vec![WorkflowCommand::StartTimer {
            timer_id: "timer-1".to_string(),
            fire_at: OffsetDateTime::now_utc() - Duration::seconds(1),
        }],
    )
    .await?;

    wait_for_history(&store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::TimerFired { timer_id } if timer_id == "timer-1"
            )
        })
    })
    .await?;

    runtime.shutdown_timer_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn canceled_timer_is_harmlessly_ignored_by_scanner() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(10),
            max_timers_per_scan: 100,
        },
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();

    let run_key =
        start_workflow(&runtime, namespace_id, WorkflowId("timer-cancel".into())).await?;
    complete_with_commands(
        &runtime,
        namespace_id,
        vec![
            WorkflowCommand::StartTimer {
                timer_id: "timer-1".to_string(),
                fire_at: OffsetDateTime::now_utc() + Duration::seconds(1),
            },
            WorkflowCommand::RequestNewWorkflowTask,
        ],
    )
    .await?;
    complete_with_commands(
        &runtime,
        namespace_id,
        vec![WorkflowCommand::CancelTimer {
            timer_id: "timer-1".to_string(),
        }],
    )
    .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    let history = store.read_history(run_key, 0, 128).await?;
    assert!(!history.iter().any(|event| {
        matches!(
            &event.kind,
            HistoryEventKind::TimerFired { timer_id } if timer_id == "timer-1"
        )
    }));

    runtime.shutdown_timer_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn multiple_due_timers_all_fire() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(10),
            max_timers_per_scan: 100,
        },
        WorkflowTimeoutScannerConfig::default(),
    );
    let namespace_id = NamespaceId::new();

    let run_key =
        start_workflow(&runtime, namespace_id, WorkflowId("timer-multi".into())).await?;
    complete_with_commands(
        &runtime,
        namespace_id,
        vec![
            WorkflowCommand::StartTimer {
                timer_id: "timer-1".to_string(),
                fire_at: OffsetDateTime::now_utc() - Duration::seconds(3),
            },
            WorkflowCommand::StartTimer {
                timer_id: "timer-2".to_string(),
                fire_at: OffsetDateTime::now_utc() - Duration::seconds(2),
            },
            WorkflowCommand::StartTimer {
                timer_id: "timer-3".to_string(),
                fire_at: OffsetDateTime::now_utc() - Duration::seconds(1),
            },
        ],
    )
    .await?;

    wait_for_history(&store, run_key, |history| {
        let fired = history
            .iter()
            .filter(|event| matches!(event.kind, HistoryEventKind::TimerFired { .. }))
            .count();
        fired == 3
    })
    .await?;

    runtime.shutdown_timer_scanner().await?;
    Ok(())
}

async fn start_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
) -> Result<tokeira_types::RunKey> {
    let result = runtime
        .start_workflow(start_request(namespace_id, workflow_id, "req-start"))
        .await?;
    Ok(match result {
        CommitResult::Applied { new_state } => new_state.run_key,
        other => panic!("unexpected start result: {other:?}"),
    })
}

async fn complete_with_commands(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    commands: Vec<WorkflowCommand>,
) -> Result<()> {
    let workflow_task = runtime
        .poll_workflow_task(
            workflow_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .expect("workflow task should be pollable");

    let _ = runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: workflow_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            commands,
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    Ok(())
}

async fn wait_for_history(
    store: &Arc<InMemoryStore>,
    run_key: tokeira_types::RunKey,
    predicate: impl Fn(&[tokeira_kernel::HistoryEvent]) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + tokio::time::Duration::from_secs(2);
    loop {
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for timer scanner history condition");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
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

fn workflow_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("workflow-q".to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}
