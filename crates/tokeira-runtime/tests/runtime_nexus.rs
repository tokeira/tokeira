use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, SignalRequest, StartRequest, WorkflowCommand,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, EndpointTarget, LaneConfig,
    NexusEndpointConfig, NexusEndpointRegistry, NexusHttpClient, NexusStartResult,
    NexusTaskRequest, NexusTimeoutScannerConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunId,
    RunKey, SearchAttributes, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

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
        operation_id: &str,
        service: &str,
        _trace_headers: &[KeyValue],
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
    runtime_with_registry(
        store,
        client,
        NexusEndpointRegistry::new(HashMap::from([(
            "payments".to_string(),
            NexusEndpointConfig {
                target: EndpointTarget::External {
                    address: "http://payments".to_string(),
                },
            },
        )])),
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
    )
}

#[tokio::test]
async fn worker_targeted_nexus_schedule_publishes_to_broker() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
    let namespace_id = NamespaceId::new();
    let registry = NexusEndpointRegistry::new(HashMap::from([(
        "payments".to_string(),
        NexusEndpointConfig {
            target: EndpointTarget::Worker {
                namespace_id,
                task_queue: TaskQueueName("nexus-q".to_string()),
            },
        },
    )]));
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
    let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
    let namespace_id = NamespaceId::new();
    let registry = NexusEndpointRegistry::new(HashMap::from([(
        "payments".to_string(),
        NexusEndpointConfig {
            target: EndpointTarget::Worker {
                namespace_id,
                task_queue: TaskQueueName("nexus-q".to_string()),
            },
        },
    )]));
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
            let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
            let namespace_id = NamespaceId(Uuid::from_u128(namespace_seed));
            let registry = NexusEndpointRegistry::new(HashMap::from([(
                "payments".to_string(),
                NexusEndpointConfig {
                    target: EndpointTarget::Worker {
                        namespace_id,
                        task_queue: TaskQueueName("nexus-q".to_string()),
                    },
                },
            )]));
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
                    token: task.token,
                    identity: WorkerIdentity("worker".to_string()),
                    commands: vec![WorkflowCommand::ScheduleNexusOperation {
                        operation_id: operation_id.clone(),
                        endpoint: "payments".to_string(),
                        service: service.clone(),
                        operation: operation.clone(),
                        input: input.clone(),
                        schedule_to_close_timeout: Some(time::Duration::seconds(30)),
                    }],
                    force_new_workflow_task: false,
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
            match start_task.request {
                NexusTaskRequest::StartOperation {
                    service: actual_service,
                    operation: actual_operation,
                    request_id,
                    payload,
                    ..
                } => {
                    prop_assert_eq!(actual_service, expected_service.clone());
                    prop_assert_eq!(actual_operation, expected_operation.clone());
                    prop_assert_eq!(request_id, expected_operation_id.clone());
                    prop_assert_eq!(payload, Some(Payload::new(input_bytes.clone())));
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
                    token: cancel_task.token,
                    identity: WorkerIdentity("worker".to_string()),
                    commands: vec![WorkflowCommand::CancelNexusOperation { scheduled_event_id }],
                    force_new_workflow_task: false,
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
    let client = Arc::new(MockNexusClient::new(NexusStartResult::AsyncAccepted, true));
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
            token: task.token,
            identity: WorkerIdentity("worker".to_string()),
            commands: vec![WorkflowCommand::ScheduleNexusOperation {
                operation_id: "op-1".to_string(),
                endpoint: "missing".to_string(),
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
    let run_id = RunId::new();
    StartRequest {
        run_key: RunKey::new(),
        namespace_id,
        workflow_id,
        run_id,
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
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
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
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![tokeira_types::Payload::new(value.as_bytes().to_vec())])
}
