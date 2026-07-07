use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{
    CallbackSpec, CallbackState, CallbackTrigger, CompletionCallback, HistoryEvent,
    HistoryEventKind, SignalRequest, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, COMPLETION_TOKEN_VERSION,
    CompletionCallbackScannerConfig, CompletionDeliveryOutcome, EndpointTarget,
    InMemoryNexusEndpointStore, LaneConfig, NexusCompletion, NexusCompletionClient,
    NexusCompletionDeps, NexusCompletionRuntimeConfig, NexusCompletionToken, NexusEndpointRegistry,
    NexusEndpointSpec, NexusEndpointSpecTarget, NexusEndpointStore, NexusHttpClient,
    NexusStartResult, NexusTaskRequest, NexusTimeoutScannerConfig, SYSTEM_CALLBACK_URL,
    TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

/// Build a store-backed [`NexusEndpointRegistry`] seeded with `(name, target)` pairs.
/// Mirrors how the live endpoint admin populates the shared store, so dispatch tests
/// resolve endpoints exactly as production does. The Worker target's namespace name
/// is irrelevant to dispatch (only the id + task queue route), so it is left empty.
fn seed_registry(endpoints: Vec<(&str, EndpointTarget)>) -> NexusEndpointRegistry {
    let store = Arc::new(InMemoryNexusEndpointStore::new());
    for (name, target) in endpoints {
        let spec_target = match target {
            EndpointTarget::External { address } => {
                NexusEndpointSpecTarget::External { url: address }
            }
            EndpointTarget::Worker {
                namespace_id,
                task_queue,
            } => NexusEndpointSpecTarget::Worker {
                namespace_name: String::new(),
                namespace_id: namespace_id.0.to_string(),
                task_queue: task_queue.0,
            },
        };
        store
            .create(
                NexusEndpointSpec {
                    name: name.to_string(),
                    description: Vec::new(),
                    target: spec_target,
                },
                0,
            )
            .expect("seed nexus endpoint");
    }
    NexusEndpointRegistry::new(store)
}

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
        _trace_headers: &[KeyValue],
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
        service: &str,
        operation: &str,
        operation_token: &str,
        _trace_headers: &[KeyValue],
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let _ = service;
        state.cancel_calls.push((
            address.to_string(),
            operation.to_string(),
            operation_token.to_string(),
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
            links: Vec::new(),
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
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, run_key, |history| {
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
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
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
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                // A non-zero schedule-to-close that elapses almost immediately:
                // v1.31.0 treats a zero timeout as "no deadline" (it only emits the
                // timeout task when the duration is non-zero), so the deadline must
                // be a real, small interval for the scanner to fire.
                schedule_to_close_timeout: Some(time::Duration::milliseconds(1)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, run_key, |history| {
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
async fn nexus_cancel_requests_without_resolving() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
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
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, run_key, |history| {
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
                header: None,
                links: Vec::new(),
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
            client_discards_speculative_with_events: false,
            token: cancel_task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    // v1.31.0 decouples cancel-ack from operation resolution: CancelNexusOperation
    // records a NexusOperationCancelRequested event and dispatches the cancel to the
    // handler, but does NOT resolve the operation. The operation resolves solely from
    // its completion when the backing workflow closes
    // (components/nexusoperations/statemachine.go:424,668 @ v1.31.0; Gap C of commit
    // 31b50953 "decouple cancel from resolution"). So we assert the cancel-request is
    // recorded and the cancel is dispatched exactly once — never a Canceled resolution.
    wait_for_history(&store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationCancelRequested { scheduled_event_id: id }
                if *id == scheduled_event_id
            )
        })
    })
    .await?;

    // The cancel is delivered to the handler exactly once (1 start, 1 cancel). Dispatch
    // runs after the WFT commit, so wait for it rather than assume synchronous delivery.
    wait_for_client(&client, (1, 1)).await?;

    // The cancel-ack must not resolve the operation: no NexusOperationCanceled is
    // emitted, and the operation stays pending awaiting its completion.
    let history = store.read_history(run_key, 0, 256).await?;
    assert!(
        !history
            .iter()
            .any(|event| matches!(&event.kind, HistoryEventKind::NexusOperationCanceled { .. })),
        "cancel-ack must not emit NexusOperationCanceled (v1.31.0 decoupling)"
    );
    let state = match store.load_run(run_key).await? {
        tokeira_kernel::LoadedRun::Existing(state) => state,
        tokeira_kernel::LoadedRun::Absent => panic!("run should still exist"),
    };
    assert!(
        state
            .pending_nexus_operations
            .values()
            .any(|pending| pending.scheduled_event_id == scheduled_event_id),
        "operation stays pending after cancel-ack"
    );

    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn nexus_schedule_to_start_times_out_via_scanner() -> Result<()> {
    // A Worker-target operation that no worker ever polls never starts, so the
    // schedule-to-start deadline (scheduled time + timeout) must fire on its own
    // and produce a NexusOperationTimedOut event — mirroring v1.31.0's
    // ScheduleToStartTimeoutTask (components/nexusoperations/statemachine.go @ v1.31.0).
    // Isolates schedule-to-start firing from the gRPC long-poll path.
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
    let namespace_id = NamespaceId::new();
    let registry = seed_registry(vec![(
        "payments",
        EndpointTarget::Worker {
            namespace_id,
            task_queue: TaskQueueName("unreachable-nexus-q".to_string()),
        },
    )]);
    let mut runtime = runtime_with_registry(store.clone(), client, registry);
    let workflow_id = WorkflowId("nexus-s2s-timeout".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(namespace_id, workflow_id, "req-start"))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: Some(time::Duration::milliseconds(20)),
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationTimedOut { operation_id, .. }
                if operation_id == "op-1"
            )
        })
    })
    .await?;

    assert!(runtime.nexus_timeout_tracking().snapshot().is_empty());
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

fn runtime_with_nexus(
    store: Arc<InMemoryStore>,
    client: Arc<dyn NexusHttpClient>,
) -> TokeiraRuntime<InMemoryStore> {
    runtime_with_registry(
        store,
        client,
        seed_registry(vec![(
            "payments",
            EndpointTarget::External {
                address: "http://payments".to_string(),
            },
        )]),
    )
}

fn runtime_with_registry(
    store: Arc<InMemoryStore>,
    client: Arc<dyn NexusHttpClient>,
    registry: NexusEndpointRegistry,
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
        registry,
        client,
        NexusCompletionDeps::default(),
    )
}

#[tokio::test]
async fn worker_targeted_nexus_schedule_publishes_to_broker() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
    let namespace_id = NamespaceId::new();
    let registry = seed_registry(vec![(
        "payments",
        EndpointTarget::Worker {
            namespace_id,
            task_queue: TaskQueueName("nexus-q".to_string()),
        },
    )]);
    let mut runtime = runtime_with_registry(store.clone(), client.clone(), registry);
    let workflow_id = WorkflowId("nexus-worker-schedule".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(namespace_id, workflow_id, "req-start"))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let broker_task = runtime
        .nexus_task_broker()
        .poll(
            namespace_id,
            TaskQueueName("nexus-q".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await
        .expect("worker-targeted nexus task should publish");
    assert_eq!(broker_task.token.run_key, run_key);
    assert_eq!(broker_task.token.operation_id, "op-1");
    match broker_task.request {
        NexusTaskRequest::StartOperation {
            service,
            operation,
            request_id,
            payload,
            ..
        } => {
            assert_eq!(service, "charge");
            assert_eq!(operation, "authorize");
            assert_eq!(request_id, "op-1");
            assert_eq!(payload, Some(payloads("input").0[0].clone()));
        }
        other => panic!("expected start operation task, got {other:?}"),
    }
    assert_eq!(client.snapshot(), (0, 0));
    assert_eq!(runtime.nexus_timeout_tracking().snapshot().len(), 1);
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

#[tokio::test]
async fn worker_targeted_nexus_cancel_publishes_to_broker() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
    let namespace_id = NamespaceId::new();
    let registry = seed_registry(vec![(
        "payments",
        EndpointTarget::Worker {
            namespace_id,
            task_queue: TaskQueueName("nexus-q".to_string()),
        },
    )]);
    let mut runtime = runtime_with_registry(store.clone(), client.clone(), registry);
    let workflow_id = WorkflowId("nexus-worker-cancel".to_string());

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
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "payments".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    let _ = runtime
        .nexus_task_broker()
        .poll(
            namespace_id,
            TaskQueueName("nexus-q".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await;

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
                header: None,
                links: Vec::new(),
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
            client_discards_speculative_with_events: false,
            token: cancel_task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let broker_task = runtime
        .nexus_task_broker()
        .poll(
            namespace_id,
            TaskQueueName("nexus-q".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await
        .expect("worker-targeted nexus cancel should publish");
    assert_eq!(broker_task.token.run_key, run_key);
    match broker_task.request {
        NexusTaskRequest::CancelOperation {
            service,
            operation,
            operation_id,
            ..
        } => {
            assert_eq!(service, "charge");
            assert_eq!(operation, "authorize");
            assert_eq!(operation_id, "op-1");
        }
        other => panic!("expected cancel operation task, got {other:?}"),
    }
    assert_eq!(client.snapshot(), (0, 0));
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

// Feature: edge-nexus-task-transport, Property 5: Dispatch-to-broker field preservation
proptest! {
    #![proptest_config(ProptestConfig { cases: 16, .. ProptestConfig::default() })]
    #[test]
    fn property_dispatch_to_broker_field_preservation(
        namespace_seed in any::<u128>(),
        service in "[a-z]{1,10}",
        operation in "[a-z]{1,10}",
        operation_id in "[a-z0-9_-]{1,16}",
        input_bytes in proptest::collection::vec(any::<u8>(), 0..24),
    ) {
        let rt = Runtime::new().expect("runtime");
        rt.block_on(async move {
            let store = Arc::new(InMemoryStore::default());
            let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted { operation_token: "async-op-token".to_string(), links: Vec::new() }, true));
            let namespace_id = NamespaceId(Uuid::from_u128(namespace_seed));
            let registry = seed_registry(vec![(
                "payments",
                EndpointTarget::Worker {
                    namespace_id,
                    task_queue: TaskQueueName("nexus-q".to_string()),
                },
            )]);
            let mut runtime = runtime_with_registry(store.clone(), client, registry);
            let workflow_id = WorkflowId(format!("wf-{operation_id}"));
            let input = Payloads(vec![Payload::new(input_bytes.clone())]);
            let expected_service = service.clone();
            let expected_operation = operation.clone();
            let expected_operation_id = operation_id.clone();

            let run_key = applied_state(
                &runtime
                    .start_workflow(start_request(namespace_id, workflow_id.clone(), "req-start"))
                    .await
                    .expect("start workflow"),
            )
            .run_key;
            let task = poll_wft(&runtime, namespace_id, "workflow-q")
                .await
                .expect("workflow task");
            runtime
                .complete_workflow_task(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: task.token,
                    identity: WorkerIdentity("worker".to_string()),
                    sdk_metadata: None,
                metering_metadata: None,
                    worker_version: None,
                versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                    commands: vec![WorkflowCommand::ScheduleNexusOperation {
                        operation_id: operation_id.clone(),
                        endpoint: "payments".to_string(),
                        service: service.clone(),
                        operation: operation.clone(),
                        input: input.clone(),
                        schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                        schedule_to_start_timeout: None,
                        start_to_close_timeout: None,
                    }],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now: OffsetDateTime::now_utc(),
                })
                .await
                .expect("schedule nexus op");

            let start_task = runtime
                .nexus_task_broker()
                .poll(
                    namespace_id,
                    TaskQueueName("nexus-q".to_string()),
                    tokio::time::Duration::from_millis(50),
                )
                .await
                .expect("start task");
            prop_assert_eq!(start_task.token.run_key, run_key);
            prop_assert_eq!(start_task.token.operation_id, expected_operation_id.clone());
            // The task token's scheduled_event_id is the op's fencing key; the
            // completion token must carry the same value (Property 1).
            let task_scheduled_event_id = start_task.token.scheduled_event_id;
            match start_task.request {
                NexusTaskRequest::StartOperation {
                    service: actual_service,
                    operation: actual_operation,
                    request_id,
                    payload,
                    scheduled_time: _,
                    callback_url,
                    callback_token,
                } => {
                    prop_assert_eq!(actual_service, expected_service.clone());
                    prop_assert_eq!(actual_operation, expected_operation.clone());
                    prop_assert_eq!(request_id, expected_operation_id.clone());
                    prop_assert_eq!(payload, Some(Payload::new(input_bytes.clone())));
                    // Property 1: a Worker-target dispatch carries the resolved HTTP
                    // completion callback URL (the handler POSTs its async outcome here)
                    // + a decodable, version-checked completion token whose routing keys
                    // equal the dispatched operation's. The URL is the configured listener
                    // base joined with the callback path — never the `temporal://system`
                    // sentinel, which a Worker SDK cannot POST to.
                    prop_assert_eq!(
                        callback_url,
                        Some(format!(
                            "{}{}",
                            NexusCompletionRuntimeConfig::default().system_callback_url,
                            tokeira_runtime::NEXUS_CALLBACK_PATH
                        ))
                    );
                    let encoded = callback_token.expect("completion token attached");
                    let token = NexusCompletionToken::decode(&encoded)
                        .expect("completion token decodes + version-checks");
                    prop_assert_eq!(token.originator_run_key, run_key);
                    prop_assert_eq!(token.operation_id, expected_operation_id.clone());
                    prop_assert_eq!(token.scheduled_event_id, task_scheduled_event_id);
                }
                other => panic!("unexpected start request: {other:?}"),
            }

            let scheduled_event_id = store
                .read_history(run_key, 0, 64)
                .await
                .expect("history")
                .into_iter()
                .find_map(|event| match event.kind {
                    HistoryEventKind::NexusOperationScheduled { operation_id: scheduled_operation_id, .. }
                        if scheduled_operation_id == expected_operation_id =>
                    {
                        Some(event.event_id)
                    }
                    _ => None,
                })
                .expect("scheduled event");

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
                        header: None,
                        links: Vec::new(),
                        request: RequestContext {
                            request_id: RequestId("req-signal".to_string()),
                            caller_identity: None,
                            received_at: OffsetDateTime::now_utc(),
                        },
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await
                .expect("signal workflow");

            let cancel_task = poll_wft(&runtime, namespace_id, "workflow-q")
                .await
                .expect("cancel wft");
            runtime
                .complete_workflow_task(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false,
                    token: cancel_task.token,
                    identity: WorkerIdentity("worker".to_string()),
                    sdk_metadata: None,
                metering_metadata: None,
                    worker_version: None,
                versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                    commands: vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
                    force_new_workflow_task: false,
                    delivered_update_ids: Vec::new(),
                    now: OffsetDateTime::now_utc(),
                })
                .await
                .expect("cancel nexus op");

            let cancel_task = runtime
                .nexus_task_broker()
                .poll(
                    namespace_id,
                    TaskQueueName("nexus-q".to_string()),
                    tokio::time::Duration::from_millis(50),
                )
                .await
                .expect("cancel task");
            prop_assert_eq!(cancel_task.token.run_key, run_key);
            prop_assert_eq!(cancel_task.token.operation_id, expected_operation_id.clone());
            match cancel_task.request {
                NexusTaskRequest::CancelOperation {
                    service: actual_service,
                    operation: actual_operation,
                    operation_id: actual_operation_id,
                    ..
                } => {
                    prop_assert_eq!(actual_service, expected_service);
                    prop_assert_eq!(actual_operation, expected_operation);
                    prop_assert_eq!(actual_operation_id, expected_operation_id);
                }
                other => panic!("unexpected cancel request: {other:?}"),
            }

            runtime.shutdown_timer_scanner().await.expect("shutdown timer");
            runtime
                .shutdown_workflow_timeout_scanner()
                .await
                .expect("shutdown workflow timeout");
            runtime
                .shutdown_nexus_timeout_scanner()
                .await
                .expect("shutdown nexus timeout");
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn nexus_unknown_endpoint_delivers_failed_resolution() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
    let mut runtime = runtime_with_registry(
        store.clone(),
        client.clone(),
        NexusEndpointRegistry::default(),
    );
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("nexus-missing-endpoint".to_string());

    let run_key = applied_state(
        &runtime
            .start_workflow(start_request(namespace_id, workflow_id, "req-start"))
            .await?,
    )
    .run_key;
    let task = poll_wft(&runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "missing".to_string(),
                service: "charge".to_string(),
                operation: "authorize".to_string(),
                input: payloads("input"),
                schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationFailed { operation_id, .. }
                if operation_id == "op-1"
            )
        })
    })
    .await?;

    assert_eq!(client.snapshot(), (0, 0));
    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

/// Feature: agentic-conductor (tokeira-odori task 0.1) — conformance proof for
/// async, intra-cluster, **cross-namespace** Nexus. A parent workflow in the
/// `control` namespace schedules an async Nexus operation on a worker-targeted
/// endpoint in a **different** (`agents`) namespace; a handler polling the
/// agents-namespace broker responds `Started`, then later `Completed`, and the
/// completion routes back to the originator in the `control` namespace. Also
/// asserts a long schedule-to-close timeout (>= the 3600s agent wall budget) is
/// honored, so a long agent turn is never killed by a short default.
///
/// This exercises the path Odori's `RunWorkflow` (control) → `AgentWorkflow`
/// (agents) bridge depends on; the existing worker-targeted tests prove dispatch
/// but never resolve an async op back across the namespace boundary.
#[tokio::test]
async fn cross_namespace_async_nexus_completes_back_to_originator() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(
        NexusStartResult::AsyncAccepted {
            operation_token: "async-op-token".to_string(),
            links: Vec::new(),
        },
        true,
    ));
    let ns_control = NamespaceId::new();
    let ns_agents = NamespaceId::new();
    assert!(
        ns_control != ns_agents,
        "control and agents namespaces must differ for a cross-namespace test"
    );

    // Endpoint lives in `control` but targets a worker task queue in `agents`.
    let registry = seed_registry(vec![(
        "odori-agent",
        EndpointTarget::Worker {
            namespace_id: ns_agents,
            task_queue: TaskQueueName("odori-agent-nexus-q".to_string()),
        },
    )]);
    let mut runtime = runtime_with_registry(store.clone(), client.clone(), registry);

    // Parent ("RunWorkflow") in the control namespace.
    let workflow_id = WorkflowId("odori-run-xns".to_string());
    let parent_run_key = applied_state(
        &runtime
            .start_workflow(start_request(ns_control, workflow_id, "req-start"))
            .await?,
    )
    .run_key;

    // Schedule the async Nexus op with a 1-hour schedule-to-close timeout
    // (>= max_wall_seconds = 3600s); a short default would kill a long agent turn.
    let task = poll_wft(&runtime, ns_control, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("control-worker".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-agent".to_string(),
                endpoint: "odori-agent".to_string(),
                service: "odori.agent".to_string(),
                operation: "run_agent_turn".to_string(),
                input: payloads("agent-request"),
                schedule_to_close_timeout: Some(time::Duration::seconds(3600)),
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
            }],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    // The task is dispatched to the AGENTS-namespace broker (cross-namespace),
    // while the token still carries the CONTROL-namespace originator run_key.
    let broker_task = runtime
        .nexus_task_broker()
        .poll(
            ns_agents,
            TaskQueueName("odori-agent-nexus-q".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await
        .expect("worker-targeted nexus task should publish to the agents-namespace broker");
    assert_eq!(
        broker_task.token.run_key, parent_run_key,
        "the task token must carry the control-namespace originator run_key"
    );
    let scheduled_event_id = broker_task.token.scheduled_event_id;
    match &broker_task.request {
        NexusTaskRequest::StartOperation {
            service,
            operation,
            request_id,
            ..
        } => {
            assert_eq!(service, "odori.agent");
            assert_eq!(operation, "run_agent_turn");
            assert_eq!(request_id, "op-agent");
        }
        other => panic!("expected start operation task, got {other:?}"),
    }
    // A worker-targeted endpoint never calls the external HTTP client.
    assert_eq!(client.snapshot(), (0, 0));
    // The op is tracked for timeout, but its 1-hour deadline is far away.
    assert_eq!(runtime.nexus_timeout_tracking().snapshot().len(), 1);

    // Handler (in `agents`) accepts async: `Started` is non-terminal.
    assert!(
        runtime
            .resolve_nexus_operation(
                parent_run_key,
                "op-agent".to_string(),
                scheduled_event_id,
                tokeira_kernel::NexusResolution::Started {
                    operation_token: String::new(),
                    links: Vec::new()
                },
            )
            .await?,
        "Started resolution should apply"
    );
    wait_for_history(&store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationStarted { operation_id, .. }
                if operation_id == "op-agent"
            )
        })
    })
    .await?;
    assert!(
        !nexus_history_has_timeout(&store, parent_run_key, "op-agent").await?,
        "long schedule-to-close must not time out a pending op"
    );

    // Later, the agent turn finishes: completion routes back to the control parent.
    assert!(
        runtime
            .resolve_nexus_operation(
                parent_run_key,
                "op-agent".to_string(),
                scheduled_event_id,
                tokeira_kernel::NexusResolution::Completed {
                    result: payloads("agent-turn-result"),
                    links: Vec::new(),
                },
            )
            .await?,
        "Completed resolution should apply"
    );
    wait_for_history(&store, parent_run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationCompleted { operation_id, .. }
                if operation_id == "op-agent"
            )
        })
    })
    .await?;

    // The control-namespace parent is woken with a fresh workflow task, and the
    // op never timed out (1-hour schedule-to-close honored).
    assert!(
        poll_wft(&runtime, ns_control, "workflow-q").await.is_ok(),
        "completion should schedule a workflow task back in the control namespace"
    );
    assert!(
        !nexus_history_has_timeout(&store, parent_run_key, "op-agent").await?,
        "resolved op must show completion, never a timeout"
    );
    assert!(
        runtime.nexus_timeout_tracking().snapshot().is_empty(),
        "a terminally-resolved op should no longer be tracked for timeout"
    );

    runtime.shutdown_timer_scanner().await?;
    runtime.shutdown_workflow_timeout_scanner().await?;
    runtime.shutdown_nexus_timeout_scanner().await?;
    Ok(())
}

async fn nexus_history_has_timeout(
    store: &InMemoryStore,
    run_key: RunKey,
    operation_id: &str,
) -> Result<bool> {
    Ok(store
        .read_history(run_key, 0, 256)
        .await?
        .iter()
        .any(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::NexusOperationTimedOut { operation_id: oid, .. }
                if oid == operation_id
            )
        }))
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

async fn wait_for_history<F>(store: &InMemoryStore, run_key: RunKey, predicate: F) -> Result<()>
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

/// Poll the mock Nexus client until its `(start_calls, cancel_calls)` snapshot reaches
/// `want` (bounded), mirroring [`wait_for_history`]. A `DispatchOp::CancelNexusOperation`
/// is delivered after the WFT commit, so callers wait for the dispatch to land rather
/// than assume it is synchronous with `complete_workflow_task`.
async fn wait_for_client(client: &MockNexusClient, want: (usize, usize)) -> Result<()> {
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if client.snapshot() == want {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for nexus client snapshot {want:?}");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    request_id: &str,
) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        run_key: RunKey::new(),
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
        workflow_task_timeout: time::Duration::seconds(10),
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
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        reserved_poller_identity: None,
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload::new(value.as_bytes().to_vec())])
}

// Feature: nexus-async-completion, Property 5: completion token round-trip.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// For any `NexusCompletionToken`, `decode(encode(t)) == t`, and the encoding is the
    /// versioned `{v,d}` envelope. **Validates: Requirements 1.4, 1.5**
    #[test]
    fn property_p5_completion_token_round_trip(
        uuid_bytes in any::<[u8; 16]>(),
        operation_id in ".{0,64}",
        scheduled_event_id in any::<i64>(),
        request_id in ".{0,64}",
    ) {
        let token = NexusCompletionToken {
            originator_run_key: RunKey(Uuid::from_bytes(uuid_bytes)),
            operation_id,
            scheduled_event_id,
            request_id,
            endpoint: String::new(),
            service: String::new(),
            operation: String::new(),
        };
        let encoded = token.encode().expect("encode succeeds");
        // The wire envelope is a `{v,d}` JSON object (mirrors callback_token.go @ v1.31.0).
        prop_assert!(encoded.contains("\"v\":"), "envelope carries a version field: {encoded}");
        prop_assert!(encoded.contains("\"d\":"), "envelope carries a data field: {encoded}");
        let decoded = NexusCompletionToken::decode(&encoded).expect("decode succeeds");
        prop_assert_eq!(decoded, token);
    }
}

/// A `{v,d}` envelope whose version != `COMPLETION_TOKEN_VERSION` is rejected, mirroring
/// `DecodeCallbackToken`'s `codes.InvalidArgument "unsupported token version"`
/// (`common/nexus/callback_token.go @ v1.31.0`).
#[test]
fn completion_token_decode_rejects_wrong_version() {
    use base64::Engine as _;
    let token = NexusCompletionToken {
        originator_run_key: RunKey(Uuid::from_u128(7)),
        operation_id: "op".into(),
        scheduled_event_id: 12,
        request_id: "req".into(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    };
    // Hand-roll an envelope with a bumped version but valid data.
    let inner = serde_json::to_vec(&token).expect("serialize inner");
    let bad = format!(
        "{{\"v\":{},\"d\":\"{}\"}}",
        COMPLETION_TOKEN_VERSION + 1,
        base64::engine::general_purpose::URL_SAFE.encode(inner),
    );
    let err = NexusCompletionToken::decode(&bad).expect_err("wrong version is rejected");
    assert!(
        err.to_string()
            .contains("unsupported nexus completion token version"),
        "unexpected error: {err}"
    );
}

/// A non-envelope string and a `{v,d}` envelope with undecodable data both fail to
/// decode (the inbound handler maps these to a `BadRequest`, Wave 5).
#[test]
fn completion_token_decode_rejects_malformed() {
    assert!(NexusCompletionToken::decode("not a token").is_err());
    assert!(
        NexusCompletionToken::decode("{\"v\":1,\"d\":\"!!! not base64 !!!\"}").is_err(),
        "undecodable base64 data is rejected"
    );
}

/// A correct `{v=1, d=valid-base64}` envelope whose decoded bytes are not a valid token
/// JSON is rejected at the inner-payload step (distinct from the version/base64 paths).
#[test]
fn completion_token_decode_rejects_garbage_inner_payload() {
    use base64::Engine as _;
    let bad = format!(
        "{{\"v\":{},\"d\":\"{}\"}}",
        COMPLETION_TOKEN_VERSION,
        base64::engine::general_purpose::URL_SAFE.encode(b"not a token json"),
    );
    let err = NexusCompletionToken::decode(&bad).expect_err("garbage inner payload is rejected");
    assert!(
        err.to_string()
            .contains("invalid nexus completion token payload"),
        "unexpected error: {err}"
    );
}

// ---- nexus-async-completion Wave 4: completion-callback firing + scanner ----

/// Records each `complete_operation` call and returns scripted outcomes (the last one
/// repeats once the script is exhausted), so a test can assert the firing handler built
/// the right request and drove the right lifecycle transition.
#[derive(Clone)]
struct RecordingCompletionClient {
    calls: Arc<Mutex<Vec<RecordedCompletion>>>,
    outcomes: Arc<Mutex<std::collections::VecDeque<CompletionDeliveryOutcome>>>,
    default_outcome: CompletionDeliveryOutcome,
}

#[derive(Clone, Debug)]
struct RecordedCompletion {
    url: String,
    token: String,
    state: String,
}

impl RecordingCompletionClient {
    fn new(
        script: Vec<CompletionDeliveryOutcome>,
        default_outcome: CompletionDeliveryOutcome,
    ) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            outcomes: Arc::new(Mutex::new(script.into())),
            default_outcome,
        }
    }

    fn calls(&self) -> Vec<RecordedCompletion> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl NexusCompletionClient for RecordingCompletionClient {
    async fn complete_operation(
        &self,
        url: &str,
        token: &str,
        completion: NexusCompletion,
        _links: &[tokeira_kernel::Link],
    ) -> Result<CompletionDeliveryOutcome> {
        self.calls.lock().unwrap().push(RecordedCompletion {
            url: url.to_string(),
            token: token.to_string(),
            state: completion.operation_state().to_string(),
        });
        Ok(self
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.default_outcome.clone()))
    }
}

/// Build a runtime whose completion client is the recording mock and whose scanner ticks
/// fast (so a re-fire is observable without a long wait).
fn runtime_with_completion_client(
    store: Arc<InMemoryStore>,
    registry: NexusEndpointRegistry,
    completion_client: Arc<RecordingCompletionClient>,
) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new_with_nexus(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        registry,
        Arc::new(MockNexusClient::new(
            NexusStartResult::AsyncAccepted {
                operation_token: "tok".to_string(),
                links: Vec::new(),
            },
            true,
        )),
        NexusCompletionDeps {
            client: completion_client,
            config: NexusCompletionRuntimeConfig::default(),
            scanner: CompletionCallbackScannerConfig {
                scan_interval: tokio::time::Duration::from_millis(10),
                max_per_scan: 100,
            },
        },
    )
}

/// A `temporal://system` Nexus completion callback carrying `token` in its
/// `Temporal-Callback-Token` header — the shape tokeira attaches to a handler workflow.
fn system_completion_callback(token: &str) -> CompletionCallback {
    CompletionCallback {
        spec: CallbackSpec::Nexus {
            url: SYSTEM_CALLBACK_URL.to_string(),
            header: std::collections::BTreeMap::from([(
                "Temporal-Callback-Token".to_string(),
                token.to_string(),
            )]),
        },
        links: Vec::new(),
        trigger: CallbackTrigger::WorkflowClosed,
        registration_time: None,
        state: CallbackState::Standby,
        attempt: 0,
        last_attempt_failure: None,
        next_attempt_at: None,
    }
}

/// Poll the run state until `predicate` holds (bounded), mirroring `wait_for_history`.
async fn wait_for_run<F>(
    store: &InMemoryStore,
    run_key: RunKey,
    predicate: F,
) -> Result<tokeira_kernel::WorkflowState>
where
    F: Fn(&tokeira_kernel::WorkflowState) -> bool,
{
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let tokeira_kernel::LoadedRun::Existing(state) = store.load_run(run_key).await?
            && predicate(&state)
        {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for run condition");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
}

/// Start a workflow carrying `callback`, run one WFT, and close it with `close`.
async fn close_workflow_with_callback(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    callback: CompletionCallback,
    close: WorkflowCommand,
) -> Result<RunKey> {
    let mut start = start_request(namespace_id, workflow_id, "req-start");
    start.completion_callbacks = vec![callback];
    let run_key = applied_state(&runtime.start_workflow(start).await?).run_key;
    let task = poll_wft(runtime, namespace_id, "workflow-q").await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![close],
            force_new_workflow_task: false,
            delivered_update_ids: Vec::new(),
            now: OffsetDateTime::now_utc(),
        })
        .await?;
    Ok(run_key)
}

#[tokio::test]
async fn completion_callback_delivered_on_close_succeeds() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let registry = seed_registry(vec![]);
    let client = Arc::new(RecordingCompletionClient::new(
        Vec::new(),
        CompletionDeliveryOutcome::Delivered,
    ));
    let runtime = runtime_with_completion_client(store.clone(), registry, client.clone());

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(42)),
        operation_id: "op-async".to_string(),
        scheduled_event_id: 7,
        request_id: "op-async".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::CompleteWorkflow {
            result: payloads("agent-result"),
        },
    )
    .await?;

    // The callback reaches `Succeeded` once the (Noop-Delivered) POST is acknowledged.
    let closed = wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Succeeded)
    })
    .await?;
    assert_eq!(closed.completion_callbacks.len(), 1);

    // The firing handler resolved `temporal://system` to the configured listener and sent
    // the original token + a `succeeded` state.
    let calls = client.calls();
    assert_eq!(calls.len(), 1, "exactly one delivery attempt");
    assert_eq!(
        calls[0].url,
        format!(
            "{}{}",
            NexusCompletionRuntimeConfig::default().system_callback_url,
            tokeira_runtime::NEXUS_CALLBACK_PATH
        )
    );
    assert_eq!(calls[0].state, "succeeded");
    let decoded = NexusCompletionToken::decode(&calls[0].token).expect("token round-trips");
    assert_eq!(decoded.operation_id, "op-async");
    assert_eq!(decoded.scheduled_event_id, 7);
    Ok(())
}

/// Regression: a callback attached via the real `StartWorkflowExecution` path is stored
/// with a **lowercased** header key (`callback_to_edge` lowercases keys to mirror
/// v1.31.0's `nexus.Header`). The firing path must still find the token — a case-sensitive
/// lookup would miss it, fire with an empty token, and the inbound listener would 400, so
/// the caller's async op would never resolve (the multiple-callers conformance timeout).
#[tokio::test]
async fn completion_callback_fires_with_lowercased_token_header() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let registry = seed_registry(vec![]);
    let client = Arc::new(RecordingCompletionClient::new(
        Vec::new(),
        CompletionDeliveryOutcome::Delivered,
    ));
    let runtime = runtime_with_completion_client(store.clone(), registry, client.clone());

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(99)),
        operation_id: "op-lower".to_string(),
        scheduled_event_id: 11,
        request_id: "op-lower".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    // The key as the inbound StartWorkflowExecution path stores it: lowercase.
    let callback = CompletionCallback {
        spec: CallbackSpec::Nexus {
            url: SYSTEM_CALLBACK_URL.to_string(),
            header: std::collections::BTreeMap::from([(
                "temporal-callback-token".to_string(),
                token.clone(),
            )]),
        },
        links: Vec::new(),
        trigger: CallbackTrigger::WorkflowClosed,
        registration_time: None,
        state: CallbackState::Standby,
        attempt: 0,
        last_attempt_failure: None,
        next_attempt_at: None,
    };

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf-lower".to_string()),
        callback,
        WorkflowCommand::CompleteWorkflow {
            result: payloads("agent-result"),
        },
    )
    .await?;

    wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Succeeded)
    })
    .await?;

    let calls = client.calls();
    assert_eq!(calls.len(), 1, "exactly one delivery attempt");
    assert!(
        !calls[0].token.is_empty(),
        "the lowercased token header must still be found and forwarded"
    );
    let decoded = NexusCompletionToken::decode(&calls[0].token)
        .expect("the forwarded token decodes (it was not dropped)");
    assert_eq!(decoded.operation_id, "op-lower");
    assert_eq!(decoded.scheduled_event_id, 11);
    Ok(())
}

#[tokio::test]
async fn completion_callback_failed_close_delivers_failed_state() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let client = Arc::new(RecordingCompletionClient::new(
        Vec::new(),
        CompletionDeliveryOutcome::Delivered,
    ));
    let runtime =
        runtime_with_completion_client(store.clone(), seed_registry(vec![]), client.clone());

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(43)),
        operation_id: "op-fail".to_string(),
        scheduled_event_id: 9,
        request_id: "op-fail".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf-fail".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::FailWorkflow {
            failure: Payload::new(b"boom".to_vec()),
        },
    )
    .await?;

    wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Succeeded)
    })
    .await?;
    let calls = client.calls();
    assert_eq!(calls.len(), 1);
    // A failed handler workflow delivers state=failed (GetNexusCompletion failed arm).
    assert_eq!(calls[0].state, "failed");
    Ok(())
}

#[tokio::test]
async fn completion_callback_retryable_backs_off_then_scanner_refires() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    // First attempt is retryable, then delivered — exercising BackingOff → scanner re-fire
    // → Succeeded.
    let client = Arc::new(RecordingCompletionClient::new(
        vec![CompletionDeliveryOutcome::RetryableError {
            detail: "503".to_string(),
        }],
        CompletionDeliveryOutcome::Delivered,
    ));
    let runtime =
        runtime_with_completion_client(store.clone(), seed_registry(vec![]), client.clone());

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(44)),
        operation_id: "op-retry".to_string(),
        scheduled_event_id: 5,
        request_id: "op-retry".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf-retry".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::CompleteWorkflow {
            result: payloads("ok"),
        },
    )
    .await?;

    // After the retryable first attempt the callback backs off with a future
    // next_attempt_at and an incremented attempt count.
    wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::BackingOff)
    })
    .await?;

    // The scanner (fast tick) re-fires the BackingOff callback once next_attempt_at passes;
    // the second attempt is Delivered, so it terminally succeeds.
    let resolved = wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Succeeded)
    })
    .await?;
    assert_eq!(resolved.completion_callbacks[0].attempt, 1);
    assert!(
        client.calls().len() >= 2,
        "the scanner re-fired the backing-off callback"
    );
    Ok(())
}

#[tokio::test]
async fn sweep_rebuilds_backing_off_completion_callbacks() -> Result<()> {
    // The shard-takeover rebuild query surfaces exactly the runs whose completion callbacks
    // are BackingOff (the volatile scanner index is reconstructed from this on takeover).
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    // Always-retryable client keeps the callback in BackingOff so we can query for it.
    let client = Arc::new(RecordingCompletionClient::new(
        Vec::new(),
        CompletionDeliveryOutcome::RetryableError {
            detail: "stuck".to_string(),
        },
    ));
    let runtime = runtime_with_completion_client(store.clone(), seed_registry(vec![]), client);

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(99)),
        operation_id: "op-sweep".to_string(),
        scheduled_event_id: 3,
        request_id: "op-sweep".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("swept".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::CompleteWorkflow {
            result: payloads("ok"),
        },
    )
    .await?;

    wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::BackingOff)
    })
    .await?;

    // The default single-shard store homes every run on shard 0; the rebuild query finds
    // the backing-off callback there.
    let entries = store
        .list_runs_with_pending_completion_callbacks_for_shard(
            tokeira_types::ShardId(0),
            usize::MAX,
        )
        .await?;
    assert!(
        entries
            .iter()
            .any(|entry| entry.run_key == run_key && entry.callback_index == 0),
        "backing-off callback must be swept, got {entries:?}"
    );
    Ok(())
}

#[tokio::test]
async fn completion_callback_non_retryable_delivery_fails_terminally() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let client = Arc::new(RecordingCompletionClient::new(
        vec![CompletionDeliveryOutcome::NonRetryableError {
            detail: "400 bad token".to_string(),
        }],
        CompletionDeliveryOutcome::Delivered,
    ));
    let runtime =
        runtime_with_completion_client(store.clone(), seed_registry(vec![]), client.clone());

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(77)),
        operation_id: "op-nonretry".to_string(),
        scheduled_event_id: 4,
        request_id: "op-nonretry".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf-nonretry".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::CompleteWorkflow {
            result: payloads("ok"),
        },
    )
    .await?;

    // A non-retryable delivery error terminally fails the callback (no backoff, no retry).
    wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Failed)
    })
    .await?;
    assert_eq!(
        client.calls().len(),
        1,
        "no retry after a non-retryable error"
    );
    Ok(())
}

#[tokio::test]
async fn completion_callback_exhausts_max_attempts_to_failed() -> Result<()> {
    // retry_max_attempts = 1 means a single delivery attempt: the first retryable failure
    // exhausts the budget and the callback fails terminally rather than backing off.
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let client = Arc::new(RecordingCompletionClient::new(
        Vec::new(),
        CompletionDeliveryOutcome::RetryableError {
            detail: "503".to_string(),
        },
    ));
    let runtime = TokeiraRuntime::new_with_nexus(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        seed_registry(vec![]),
        Arc::new(MockNexusClient::new(
            NexusStartResult::AsyncAccepted {
                operation_token: "tok".to_string(),
                links: Vec::new(),
            },
            true,
        )),
        NexusCompletionDeps {
            client: client.clone(),
            config: NexusCompletionRuntimeConfig {
                retry_max_attempts: 1,
                ..NexusCompletionRuntimeConfig::default()
            },
            scanner: CompletionCallbackScannerConfig {
                scan_interval: tokio::time::Duration::from_millis(10),
                max_per_scan: 100,
            },
        },
    );

    let token = NexusCompletionToken {
        originator_run_key: RunKey(uuid::Uuid::from_u128(88)),
        operation_id: "op-exhaust".to_string(),
        scheduled_event_id: 6,
        request_id: "op-exhaust".to_string(),
        endpoint: String::new(),
        service: String::new(),
        operation: String::new(),
    }
    .encode()
    .expect("encode token");

    let run_key = close_workflow_with_callback(
        &runtime,
        namespace_id,
        WorkflowId("handler-wf-exhaust".to_string()),
        system_completion_callback(&token),
        WorkflowCommand::CompleteWorkflow {
            result: payloads("ok"),
        },
    )
    .await?;

    let failed = wait_for_run(&store, run_key, |state| {
        state
            .completion_callbacks
            .first()
            .map(|cb| cb.state.clone())
            == Some(CallbackState::Failed)
    })
    .await?;
    assert_eq!(failed.completion_callbacks[0].state, CallbackState::Failed);
    Ok(())
}
