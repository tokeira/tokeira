use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{StartRequest, WorkflowTaskCompletedRequest};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, LogicalTaskSeq, Memo, NamespaceId, Payloads, QueueKey, RequestContext,
    RequestId, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId,
    WorkflowType,
};

#[tokio::test]
async fn start_and_signal_publish_workflow_tasks() -> Result<()> {
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
    let workflow_id = WorkflowId("workflow-1".to_string());
    let queue = queue(namespace_id, "queue-a");

    let start = runtime
        .start_workflow(start_request(
            namespace_id,
            workflow_id.clone(),
            "req-start",
        ))
        .await?;
    let started_state = applied_state(&start);
    let first_task = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("start should publish a workflow task");
    assert_eq!(first_task.run_key, started_state.run_key);
    assert_eq!(first_task.token.logical_seq, LogicalTaskSeq::ONE);

    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            commands: Vec::new(),
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    runtime
        .signal_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            signal_request("req-signal"),
        )
        .await?;

    let signaled_task = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("signal should publish a new workflow task");
    assert_eq!(signaled_task.run_key, started_state.run_key);
    assert_eq!(signaled_task.token.logical_seq, LogicalTaskSeq(2));

    Ok(())
}

#[tokio::test]
async fn occ_conflicts_are_retried_for_signals() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig {
            max_occ_retries: 5,
            max_drain_per_activation: 16,
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("workflow-conflict".to_string());

    let start = runtime
        .start_workflow(start_request(
            namespace_id,
            workflow_id.clone(),
            "req-start",
        ))
        .await?;
    let run_key = applied_state(&start).run_key;
    store.inject_conflict(run_key, 2).await;

    let first = runtime.signal_workflow(
        ExecutionRef {
            namespace_id,
            workflow_id: workflow_id.clone(),
            run_id: None,
        },
        signal_request("req-signal-1"),
    );
    let second = runtime.signal_workflow(
        ExecutionRef {
            namespace_id,
            workflow_id,
            run_id: None,
        },
        signal_request("req-signal-2"),
    );

    let (first, second) = tokio::join!(first, second);
    let first = first?;
    let second = second?;
    assert!(matches!(first, CommitResult::Applied { .. }));
    assert!(matches!(second, CommitResult::Applied { .. }));

    Ok(())
}

#[tokio::test]
async fn burst_signals_produce_complete_history() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("workflow-burst".to_string());
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

    for index in 0..5 {
        runtime
            .signal_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id.clone(),
                    run_id: None,
                },
                signal_request(&format!("req-signal-{index}")),
            )
            .await?;
    }

    let history = store.read_history(run_key, 0, 64).await?;
    let signal_events = history
        .into_iter()
        .filter(|event| {
            matches!(
                event.kind,
                tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled { .. }
            )
        })
        .count();
    assert_eq!(signal_events, 5);

    Ok(())
}

fn applied_state(result: &CommitResult) -> tokeira_kernel::WorkflowState {
    match result {
        CommitResult::Applied { new_state } => new_state.clone(),
        other => panic!("expected applied result, got {other:?}"),
    }
}

fn queue(namespace_id: NamespaceId, name: &str) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
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
        task_queue: TaskQueueName("queue-a".to_string()),
        input: Payloads::default(),
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
    }
}

fn signal_request(request_id: &str) -> tokeira_kernel::SignalRequest {
    tokeira_kernel::SignalRequest {
        signal_name: "sig".to_string(),
        input: Payloads::default(),
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
    }
}
