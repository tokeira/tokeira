use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, SignalRequest, StartRequest, WorkflowCommand,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig,
    NexusEndpointConfig, NexusEndpointRegistry, NexusHttpClient,
    NexusStartResult, NexusTimeoutScannerConfig, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

#[derive(Clone)]
struct MockNexusClient {
    state: Arc<Mutex<MockNexusClientState>>,
}

struct MockNexusClientState {
    start_result: NexusStartResult,
    cancel_ok: bool,
    start_calls: Vec<(
        String,
        String,
        String,
        String,
        Payloads,
        Option<time::Duration>,
    )>,
    cancel_calls: Vec<(String, String, String)>,
}

impl MockNexusClient {
    fn new(start_result: NexusStartResult, cancel_ok: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockNexusClientState {
                start_result,
                cancel_ok,
                start_calls: Vec::new(),
                cancel_calls: Vec::new(),
            })),
        }
    }

    fn snapshot(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.start_calls.len(), state.cancel_calls.len())
    }
}

#[async_trait]
impl NexusHttpClient for MockNexusClient {
    async fn start_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        operation: &str,
        input: &Payloads,
        schedule_to_close_timeout: Option<time::Duration>,
    ) -> Result<NexusStartResult> {
        let mut state = self.state.lock().unwrap();
        state.start_calls.push((
            address.to_string(),
            operation_id.to_string(),
            service.to_string(),
            operation.to_string(),
            input.clone(),
            schedule_to_close_timeout,
        ));
        Ok(state.start_result.clone())
    }

    async fn cancel_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.cancel_calls.push((
            address.to_string(),
            operation_id.to_string(),
            service.to_string(),
        ));
        if state.cancel_ok {
            Ok(())
        } else {
            Err(anyhow!("cancel failed"))
        }
    }
}

#[tokio::test]
async fn nexus_schedule_sync_complete_delivers_completed_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::SyncCompleted {
            result: payloads("nexus-result"),
        },
        true,
    ));
    let mut runtime = runtime_with_nexus(store.clone(), client.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("nexus-complete".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(
                namespace_id,
                workflow_id.clone(),
                "req-start",
            ))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&*store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationCompleted { operation_id, .. }
                if operation_id == "op-1"
            )
        })
    })
    .await?;

    assert_eq!(client.snapshot(), (1, 0));
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn nexus_async_started_times_out_via_scanner() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
    let mut runtime = runtime_with_nexus(store.clone(), client);
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("nexus-timeout".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(namespace_id, workflow_id, "req-start"))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::ZERO),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&*store, run_key, |history| {
        let started = history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationStarted { operation_id, .. }
                if operation_id == "op-1"
            )
        });
        let timed_out = history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationTimedOut { operation_id, .. }
                if operation_id == "op-1"
            )
        });
        started && timed_out
    })
    .await?;

    assert!(runtime.nexus_timeout_tracking().snapshot().is_empty());
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn nexus_cancel_success_delivers_canceled_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
    let mut runtime = runtime_with_nexus(store.clone(), client.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("nexus-cancel".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(
                namespace_id,
                workflow_id.clone(),
                "req-start",
            ))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&*store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationStarted { operation_id, .. }
                if operation_id == "op-1"
            )
        })
    })
    .await?;

    let scheduled_event_id = store
        .read_history(run_key, 0, 64)
        .await?
        .into_iter()
        .find_map(|event| match event.kind {
            HistoryEventKind::NexusOperationScheduled { operation_id, .. }
                if operation_id == "op-1" =>
            {
                Some(event.event_id)
            }
            _ => None,
        })
        .expect("scheduled event should exist");

    runtime
        .signal_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            SignalRequest {
                signal_name: "poke".to_string(),
                input: Payloads::default(),
                request: RequestContext {
                    request_id: RequestId("req-signal".to_string()),
                    caller_identity: None,
                    received_at: OffsetDateTime::now_utc(),
                },
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;

    let cancel_task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: cancel_task.token,
            identity: WorkerIdentity("worker".to_string()),
            commands: vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&*store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationCanceled { operation_id, .. }
                if operation_id == "op-1"
            )
        })
    })
    .await?;

    assert_eq!(client.snapshot(), (1, 1));
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

fn runtime_with_nexus(
    store: Arc<InMemoryStore>,
    client: Arc<dyn NexusHttpClient>,
) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new_with_nexus(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig {
            scan_interval: tokio::time::Duration::from_millis(10),
            max_timeouts_per_scan: 100,
        },
        NexusEndpointRegistry::new(HashMap::from([(
            "payments".to_string(),
            NexusEndpointConfig {
                address: "http://payments".to_string(),
            },
        )])),
        client,
    )
}

async fn poll_wft(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    task_queue: &str,
) -> Result<tokeira_runtime::StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            tokeira_types::QueueKey {
                namespace_id,
                task_queue: TaskQueueName(task_queue.to_string()),
                task_kind: tokeira_types::TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .ok_or_else(|| anyhow!("expected workflow task"))
}

async fn wait_for_history<F>(
    store: &InMemoryStore,
    run_key: RunKey,
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
            anyhow::bail!("timed out waiting for nexus history condition");
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
) -> StartRequest {
    StartRequest {
        run_key: RunKey::new(),
        namespace_id,
        workflow_id,
        run_id: RunId::new(),
        workflow_type: WorkflowType("example".to_string()),
        task_queue: TaskQueueName("workflow-q".to_string()),
        input: Payloads::default(),
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: time::Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
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

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload::new(value.as_bytes().to_vec())])
}
