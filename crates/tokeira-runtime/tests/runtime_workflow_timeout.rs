use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokio::time::Instant;

use tokeira_kernel::{HistoryEventKind, StartRequest, TerminateRequest};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, Memo, NamespaceId, Payloads, RequestContext, RequestId,
    SearchAttributes, TaskQueueName, WorkflowId, WorkflowType,
};

#[tokio::test]
async fn execution_timeout_fires_end_to_end() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();

    let run_key = start_workflow(
        &runtime,
        start_request(
            namespace_id,
            WorkflowId("wf-exec-timeout".into()),
            Some(Duration::milliseconds(1)),
            None,
        ),
    )
    .await?;

    wait_for_run(&store, run_key, |state, history| {
        state.status == ExecutionStatus::TimedOut
            && history.iter().any(|event| {
                matches!(
                    &event.kind,
                    HistoryEventKind::WorkflowExecutionTimedOut { timeout_type, .. }
                        if *timeout_type == tokeira_kernel::WorkflowTimeoutType::ExecutionTimeout
                )
            })
    })
    .await?;

    shutdown(&mut runtime).await
}

#[tokio::test]
async fn run_timeout_fires_end_to_end() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();

    let run_key = start_workflow(
        &runtime,
        start_request(
            namespace_id,
            WorkflowId("wf-run-timeout".into()),
            None,
            Some(Duration::milliseconds(1)),
        ),
    )
    .await?;

    wait_for_run(&store, run_key, |state, history| {
        state.status == ExecutionStatus::TimedOut
            && history.iter().any(|event| {
                matches!(
                    &event.kind,
                    HistoryEventKind::WorkflowExecutionTimedOut { timeout_type, .. }
                        if *timeout_type == tokeira_kernel::WorkflowTimeoutType::RunTimeout
                )
            })
    })
    .await?;

    shutdown(&mut runtime).await
}

#[tokio::test]
async fn execution_timeout_takes_precedence_when_both_expire() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();

    let run_key = start_workflow(
        &runtime,
        start_request(
            namespace_id,
            WorkflowId("wf-both-timeout".into()),
            Some(Duration::milliseconds(1)),
            Some(Duration::milliseconds(1)),
        ),
    )
    .await?;

    wait_for_run(&store, run_key, |_state, history| {
        let timeout_events: Vec<_> = history
            .iter()
            .filter_map(|event| match &event.kind {
                HistoryEventKind::WorkflowExecutionTimedOut { timeout_type, .. } => {
                    Some(timeout_type.clone())
                }
                _ => None,
            })
            .collect();
        timeout_events.len() == 1
            && timeout_events[0] == tokeira_kernel::WorkflowTimeoutType::ExecutionTimeout
    })
    .await?;

    shutdown(&mut runtime).await
}

#[tokio::test]
async fn no_timeout_configuration_produces_no_timeout_event() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();

    let run_key = start_workflow(
        &runtime,
        start_request(namespace_id, WorkflowId("wf-no-timeout".into()), None, None),
    )
    .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    let history = store.read_history(run_key, 0, 128).await?;
    assert!(!history.iter().any(|event| {
        matches!(
            event.kind,
            HistoryEventKind::WorkflowExecutionTimedOut { .. }
        )
    }));

    shutdown(&mut runtime).await
}

#[tokio::test]
async fn manually_terminated_workflow_does_not_timeout() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = make_runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("wf-manual-terminate".into());

    let run_key = start_workflow(
        &runtime,
        start_request(
            namespace_id,
            workflow_id.clone(),
            Some(Duration::seconds(1)),
            Some(Duration::seconds(1)),
        ),
    )
    .await?;

    let _ = runtime
        .terminate_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            TerminateRequest {
                reason: "manual".to_string(),
                details: None,
                identity: "tester".to_string(),
                request: RequestContext {
                    request_id: RequestId("req-term".to_string()),
                    caller_identity: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    let history = store.read_history(run_key, 0, 128).await?;
    assert!(!history.iter().any(|event| {
        matches!(
            event.kind,
            HistoryEventKind::WorkflowExecutionTimedOut { .. }
        )
    }));

    shutdown(&mut runtime).await
}

fn make_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(10),
            max_timeouts_per_scan: 100,
        },
        BacklogConfig::default(),
    )
}

async fn start_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    request: StartRequest,
) -> Result<tokeira_types::RunKey> {
    let result = runtime.start_workflow(request).await?;
    Ok(match result {
        CommitResult::Applied { new_state } => new_state.run_key,
        other => panic!("unexpected start result: {other:?}"),
    })
}

async fn wait_for_run(
    store: &Arc<InMemoryStore>,
    run_key: tokeira_types::RunKey,
    predicate: impl Fn(&tokeira_kernel::WorkflowState, &[tokeira_kernel::HistoryEvent]) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + tokio::time::Duration::from_secs(2);
    loop {
        let loaded = store.load_run(run_key).await?;
        let state = match loaded {
            tokeira_kernel::LoadedRun::Existing(state) => state,
            tokeira_kernel::LoadedRun::Absent => anyhow::bail!("run disappeared"),
        };
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&state, &history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for workflow-timeout condition");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

async fn shutdown(runtime: &mut TokeiraRuntime<InMemoryStore>) -> Result<()> {
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    Ok(())
}

fn start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
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
        workflow_execution_timeout,
        workflow_run_timeout,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
        versioning_override: None,
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
            request_id: RequestId("req-start".to_string()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}
