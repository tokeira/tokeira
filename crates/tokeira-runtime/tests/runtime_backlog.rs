use std::sync::Arc;

use time::{Duration, OffsetDateTime};

use tokeira_kernel::StartRequest;
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::InMemoryStore;
use tokeira_types::{
    Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

fn sample_start_request(
    namespace_id: NamespaceId,
    workflow_id: &str,
    task_queue: &str,
) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        initiator: None,
        run_key: RunKey::new(),
        namespace_id,
        workflow_id: WorkflowId(workflow_id.into()),
        run_id,
        workflow_type: WorkflowType("wf".into()),
        task_queue: TaskQueueName(task_queue.into()),
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
        versioning_override: None,
        workflow_start_delay: None,
        client_cron_schedule: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        attempt: 1,
        header: None,
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
            request_id: RequestId(format!("req-{workflow_id}")),
            caller_identity: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
    }
}

/// Full lifecycle: publish workflow task with no poller →
/// grace window expires → grace scanner persists to
/// backlog → drain loop retrieves → re-publishes to
/// broker → poller arrives and receives the task.
#[tokio::test]
async fn backlog_full_lifecycle_publish_grace_drain_poll() {
    let store = Arc::new(InMemoryStore::default());
    let backlog_config = BacklogConfig {
        workflow_grace_window: std::time::Duration::from_millis(10),
        activity_grace_window: std::time::Duration::from_millis(10),
        grace_scan_interval: std::time::Duration::from_millis(5),
        drain_interval: std::time::Duration::from_millis(5),
        drain_batch_limit: 100,
    };
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        backlog_config,
    );

    let ns = NamespaceId::new();
    let request = sample_start_request(ns, "wf-backlog", "q");
    let result = runtime.start_workflow(request.clone()).await;
    assert!(result.is_ok());

    // No poller yet — the workflow task sits in
    // live-ready. Wait for grace window + scanner cycle
    // to persist it to backlog.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // Now poll — the drain loop should have retrieved
    // the task from backlog and re-published it.
    let queue = QueueKey {
        namespace_id: ns,
        task_queue: TaskQueueName("q".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let worker = WorkerIdentity("worker-1".into());
    let task = runtime
        .poll_workflow_task(queue, worker, std::time::Duration::from_millis(200))
        .await
        .unwrap();

    assert!(task.is_some(), "expected task after backlog lifecycle");

    runtime.shutdown_grace_scanner().await.unwrap();
    runtime.shutdown_drain_loop().await.unwrap();
    runtime.shutdown_timer_scanner().await.unwrap();
    runtime.shutdown_workflow_timeout_scanner().await.unwrap();
}

/// Graceful shutdown: cancel the grace scanner and drain
/// loop, verify they exit cleanly.
#[tokio::test]
async fn backlog_graceful_shutdown() {
    let store = Arc::new(InMemoryStore::default());
    let mut runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );

    // Shutdown immediately — should complete without
    // hanging.
    runtime.shutdown_grace_scanner().await.unwrap();
    runtime.shutdown_drain_loop().await.unwrap();
    runtime.shutdown_timer_scanner().await.unwrap();
    runtime.shutdown_workflow_timeout_scanner().await.unwrap();
}
