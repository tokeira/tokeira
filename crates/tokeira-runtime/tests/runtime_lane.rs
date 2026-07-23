use std::sync::Arc;

use anyhow::Result;
use std::collections::BTreeMap;
use time::{Duration, OffsetDateTime};

use tokeira_kernel::{
    CallbackSpec, CallbackState, Command, CompletionCallback, HistoryEventKind, LoadedRun,
    SignalWithStartRequest, StartRequest, VersioningBehavior, VersioningOverride,
    WORKFLOW_START_DELAY_TIMER_ID, WorkerDeploymentVersionRef, WorkflowCommand,
    WorkflowStartDelayElapsedRequest, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionRef, Headers, LogicalTaskSeq, Memo, NamespaceId, Payload,
    Payloads, QueueKey, RequestContext, RequestId, RunKey, SearchAttributes, ShardId, TaskKind,
    TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
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
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: Vec::new(),
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
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
async fn signal_with_start_existing_run_preserves_signal_metadata() -> Result<()> {
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
    let workflow_id = WorkflowId("workflow-signal-with-start-existing".to_string());
    let started = runtime
        .start_workflow(start_request(
            namespace_id,
            workflow_id.clone(),
            "req-start",
        ))
        .await?;
    let run_key = applied_state(&started).run_key;
    let mut header = BTreeMap::new();
    header.insert("x-signal".to_string(), Payload::new(b"metadata".to_vec()));
    let links = vec![tokeira_kernel::state::Link::BatchJob {
        job_id: "batch-1".to_string(),
    }];
    let mut request = signal_with_start_request(namespace_id, workflow_id, "req-signal-with-start");
    request.conflict_policy = tokeira_kernel::WorkflowIdConflictPolicy::UseExisting;
    request.header = Some(Headers(header.clone()));
    request.links = links.clone();

    let result = runtime.signal_with_start_workflow(request).await?;
    assert!(matches!(
        result,
        tokeira_runtime::SignalWithStartResult::Signaled { .. }
    ));
    let history = store.read_history(run_key, 0, 64).await?;
    let signaled = history
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionSignaled { header, links, .. } => {
                Some((header, links))
            }
            _ => None,
        })
        .expect("signal-with-start existing path should append a signal event");
    assert_eq!(signaled.0, &Some(Headers(header)));
    assert_eq!(signaled.1, &links);

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
            ..LaneConfig::default()
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
async fn delayed_start_persists_due_timer_without_publishing_wft() -> Result<()> {
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
    let workflow_id = WorkflowId("delayed-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");
    let mut request = start_request(namespace_id, workflow_id, "req-delayed-start");
    let start_time = request.now;
    request.workflow_start_delay = Some(Duration::seconds(30));

    let result = runtime.start_workflow(request).await?;
    let state = applied_state(&result);

    let immediate = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(immediate.is_none());

    let due_before = store
        .list_due_timers(start_time + Duration::seconds(29), 10)
        .await?;
    assert!(due_before.is_empty());
    let due_after = store
        .list_due_timers(start_time + Duration::seconds(30), 10)
        .await?;
    assert_eq!(due_after.len(), 1);
    assert_eq!(due_after[0].run_key, state.run_key);
    assert_eq!(due_after[0].timer_id, WORKFLOW_START_DELAY_TIMER_ID);

    Ok(())
}

#[tokio::test]
async fn client_cron_start_uses_durable_first_wft_backoff() -> Result<()> {
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
    let workflow_id = WorkflowId("cron-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");
    let start_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut request = start_request(namespace_id, workflow_id, "req-cron-start");
    request.now = start_time;
    request.request.received_at = start_time;
    request.client_cron_schedule = Some("* * * * *".to_string());
    request.cron_schedule = Some("* * * * *".to_string());

    let result = runtime.start_workflow(request).await?;
    let state = applied_state(&result);

    let immediate = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(immediate.is_none());

    let due_before = store
        .list_due_timers(start_time + Duration::seconds(39), 10)
        .await?;
    assert!(due_before.is_empty());
    let due_after = store
        .list_due_timers(start_time + Duration::seconds(40), 10)
        .await?;
    assert_eq!(due_after.len(), 1);
    assert_eq!(due_after[0].run_key, state.run_key);
    assert_eq!(due_after[0].timer_id, WORKFLOW_START_DELAY_TIMER_ID);

    Ok(())
}

#[tokio::test]
async fn cron_terminal_completion_authors_delayed_successor_run() -> Result<()> {
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
    let workflow_id = WorkflowId("cron-successor-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");
    let start_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut request = start_request(namespace_id, workflow_id.clone(), "req-cron-successor");
    request.now = start_time;
    request.request.received_at = start_time;
    request.cron_schedule = Some("* * * * *".to_string());

    let start = runtime.start_workflow(request).await?;
    let predecessor = applied_state(&start);
    let first_task = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("cron run should publish its first WFT when no first-WFT backoff is set");

    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: start_time,
        })
        .await?;

    // v1.31.0 cron closes the run with its real outcome — `WorkflowExecutionCompleted`
    // carrying `new_execution_run_id` — not a `WorkflowExecutionContinuedAsNew`. The
    // delayed cron backoff now lives on the successor's start event, not the close.
    let history = store.read_history(predecessor.run_key, 0, 16).await?;
    let successor_run_id = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionCompleted {
                new_execution_run_id: Some(new_run_id),
                ..
            } => Some(*new_run_id),
            _ => None,
        })
        .expect("cron completion should close as Completed naming the successor run");
    let successor_key = RunKey::derive(namespace_id, &workflow_id, successor_run_id);

    let successor = wait_for_existing_run(&store, successor_key).await?;
    assert_eq!(successor.workflow_start_delay, Some(Duration::seconds(40)));
    assert!(successor.timers.contains_key(WORKFLOW_START_DELAY_TIMER_ID));
    let successor_history = store.read_history(successor_key, 0, 1).await?;
    assert!(matches!(
        successor_history.first().map(|event| &event.kind),
        Some(HistoryEventKind::WorkflowExecutionStartedV2 {
            continued_execution_run_id,
            cron_schedule,
            workflow_start_delay,
            eager_execution_accepted: false,
            ..
        }) if *continued_execution_run_id == Some(predecessor.run_id)
            && cron_schedule.as_deref() == Some("* * * * *")
            && *workflow_start_delay == Some(Duration::seconds(40))
    ));

    let immediate = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(immediate.is_none());

    Ok(())
}

#[tokio::test]
async fn restart_preserves_delayed_start_callbacks_and_versioning_route() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("restart-delayed-workflow".to_string());
    let start_time = OffsetDateTime::now_utc();
    let deployment = "payments-v2".to_string();
    let build_id = "build-2026-06".to_string();
    let mut request = start_request(namespace_id, workflow_id, "req-restart-delayed");
    request.now = start_time;
    request.request.received_at = start_time;
    request.workflow_start_delay = Some(Duration::seconds(30));
    request.completion_callbacks = vec![completion_callback("https://callback.example/closed")];
    request.deployment = Some(DeploymentId(deployment.clone()));
    request.build_id = Some(BuildId(build_id.clone()));
    request.versioning_override = Some(VersioningOverride::Pinned {
        version: WorkerDeploymentVersionRef {
            deployment_name: deployment.clone(),
            build_id: build_id.clone(),
        },
    });

    let runtime_before_restart = runtime_with_store(store.clone());
    let result = runtime_before_restart.start_workflow(request).await?;
    let started = applied_state(&result);

    let reloaded = wait_for_existing_run(&store, started.run_key).await?;
    assert_eq!(reloaded.workflow_start_delay, Some(Duration::seconds(30)));
    assert_eq!(reloaded.completion_callbacks.len(), 1);
    assert_eq!(
        reloaded.completion_callbacks[0].state,
        CallbackState::Standby
    );
    assert_eq!(
        reloaded.versioning_override(),
        Some(&VersioningOverride::Pinned {
            version: WorkerDeploymentVersionRef {
                deployment_name: deployment.clone(),
                build_id: build_id.clone(),
            },
        })
    );
    assert!(
        store
            .list_due_timers(start_time + Duration::seconds(30), 10)
            .await?
            .iter()
            .any(|timer| timer.run_key == started.run_key
                && timer.timer_id == WORKFLOW_START_DELAY_TIMER_ID)
    );

    let runtime_after_restart = runtime_with_store(store.clone());
    let unversioned = runtime_after_restart
        .poll_workflow_task(
            queue(namespace_id, "queue-a"),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(unversioned.is_none());

    runtime_after_restart
        .submit(
            started.run_key,
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest {
                fired_at: start_time + Duration::seconds(30),
            }),
        )
        .await?;

    let still_not_unversioned = runtime_after_restart
        .poll_workflow_task(
            queue(namespace_id, "queue-a"),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(still_not_unversioned.is_none());

    let versioned_task = runtime_after_restart
        .poll_workflow_task(
            versioned_queue(namespace_id, "queue-a", &deployment, &build_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("delayed start should route to the pinned worker deployment after reload");

    runtime_after_restart
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: versioned_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: start_time + Duration::seconds(31),
        })
        .await?;

    let closed = wait_for_existing_run(&store, started.run_key).await?;
    // The registered completion callback is preserved across restart and fired on close.
    // Its post-close *delivery* state (now driven by the Wave 4 firing path) is exercised
    // deterministically by the completion-delivery tests in `runtime_nexus.rs`; this test
    // only asserts the callback survived the reload (it must not be dropped), since the
    // async delivery makes the exact lifecycle state racy here.
    assert_eq!(closed.completion_callbacks.len(), 1);

    Ok(())
}

#[tokio::test]
async fn restart_preserves_cron_state_before_terminal_successor() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("restart-cron-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");
    let start_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut request = start_request(namespace_id, workflow_id.clone(), "req-restart-cron");
    request.now = start_time;
    request.request.received_at = start_time;
    request.cron_schedule = Some("* * * * *".to_string());

    let runtime_before_restart = runtime_with_store(store.clone());
    let start = runtime_before_restart.start_workflow(request).await?;
    let predecessor = applied_state(&start);

    let runtime_after_restart = recovering_runtime_with_store(store.clone());
    runtime_after_restart.acquire_shard(ShardId(0)).await?;
    let first_task = runtime_after_restart
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("cron run should still have its first WFT after runtime reload");

    runtime_after_restart
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: start_time,
        })
        .await?;

    // The reloaded runtime must close the cron run with its real outcome
    // (`WorkflowExecutionCompleted` naming the successor), matching v1.31.0's
    // model rather than authoring a `WorkflowExecutionContinuedAsNew`.
    let history = store.read_history(predecessor.run_key, 0, 16).await?;
    let successor_run_id = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionCompleted {
                new_execution_run_id: Some(new_run_id),
                ..
            } => Some(*new_run_id),
            _ => None,
        })
        .expect("cron completion should author a successor after runtime reload");
    let successor_key = RunKey::derive(namespace_id, &workflow_id, successor_run_id);
    let successor = wait_for_existing_run(&store, successor_key).await?;
    assert_eq!(successor.workflow_start_delay, Some(Duration::seconds(40)));
    assert!(successor.timers.contains_key(WORKFLOW_START_DELAY_TIMER_ID));

    let successor_history = store.read_history(successor_key, 0, 1).await?;
    assert!(matches!(
        successor_history.first().map(|event| &event.kind),
        Some(HistoryEventKind::WorkflowExecutionStartedV2 {
            continued_execution_run_id,
            cron_schedule,
            workflow_start_delay,
            eager_execution_accepted: false,
            ..
        }) if *continued_execution_run_id == Some(predecessor.run_id)
            && cron_schedule.as_deref() == Some("* * * * *")
            && *workflow_start_delay == Some(Duration::seconds(40))
    ));

    let immediate = runtime_after_restart
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?;
    assert!(immediate.is_none());

    Ok(())
}

#[tokio::test]
async fn restart_preserves_wft_completion_routing_metadata() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("restart-completion-routing".to_string());
    let deployment = "payments-v3".to_string();
    let build_id = "build-2026-07".to_string();
    let queue_name = "queue-a";
    let start_time = OffsetDateTime::now_utc();
    let runtime_before_restart = runtime_with_store(store.clone());

    let start = runtime_before_restart
        .start_workflow(start_request(
            namespace_id,
            workflow_id,
            "req-restart-completion-routing",
        ))
        .await?;
    let started = applied_state(&start);
    let first_task = runtime_before_restart
        .poll_workflow_task(
            queue(namespace_id, queue_name),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("start should publish the first WFT");

    runtime_before_restart
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: Some(b"metering-after-complete".to_vec()),
            worker_version: None,
            versioning_behavior: VersioningBehavior::Pinned,
            deployment_version: Some(WorkerDeploymentVersionRef {
                deployment_name: deployment.clone(),
                build_id: build_id.clone(),
            }),
            worker_deployment_name: Some(deployment.clone()),
            sticky: Some(tokeira_kernel::StickySpec {
                queue: tokeira_types::TaskQueueName("sticky-worker-a".to_owned()),
                schedule_to_start_timeout: Duration::seconds(60),
            }),
            commands: Vec::new(),
            force_new_workflow_task: true,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: start_time,
        })
        .await?;

    let completed = wait_for_existing_run(&store, started.run_key).await?;
    assert_eq!(
        completed
            .sticky
            .as_ref()
            .map(|sticky| &sticky.worker_identity),
        Some(&WorkerIdentity("worker-a".to_string()))
    );
    assert_eq!(
        completed.worker_deployment_name.as_deref(),
        Some("payments-v3")
    );
    assert_eq!(
        completed.effective_deployment(),
        Some(&WorkerDeploymentVersionRef {
            deployment_name: deployment.clone(),
            build_id: build_id.clone(),
        })
    );

    let runtime_after_restart = recovering_runtime_with_store(store.clone());
    runtime_after_restart.acquire_shard(ShardId(0)).await?;

    let recovered_task = runtime_after_restart
        .poll_workflow_task(
            queue(namespace_id, queue_name),
            WorkerIdentity("worker-b".to_string()),
            tokio::time::Duration::ZERO,
        )
        .await?
        .expect("recovered pending WFT should retain durable affinity and versioned routing");
    assert_eq!(recovered_task.run_key, started.run_key);
    // Broker liveness is intentionally volatile. After restart there is no
    // observation for the durable sticky queue, so recovery falls back to the
    // normal versioned queue without clearing affinity. Partial-history attach
    // requires actual sticky-queue dispatch (`setHistoryForRecordWfTaskStartedResp`,
    // recordworkflowtaskstarted/api.go:272-278 @ v1.31.0).
    assert!(!recovered_task.is_sticky_match);

    Ok(())
}

// A WFT published with NO poller parked ages past the grace window into the
// durable backlog; a poller arriving later must still receive it via the
// demand-driven drain loop (TestGetWorkflowExecutionHistory_All starts the
// workflow 8s before its first poll and hung forever on this path).
#[tokio::test]
async fn late_poller_receives_workflow_task_from_backlog() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig {
            workflow_grace_window: tokio::time::Duration::from_millis(100),
            activity_grace_window: tokio::time::Duration::from_millis(100),
            grace_scan_interval: tokio::time::Duration::from_millis(25),
            drain_interval: tokio::time::Duration::from_millis(50),
            drain_batch_limit: 100,
        },
    );
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("workflow-late-poller".to_string());

    let start = runtime
        .start_workflow(start_request(namespace_id, workflow_id, "req-late-poller"))
        .await?;
    let started_state = applied_state(&start);

    // Let the unclaimed task expire out of the in-memory broker into the
    // durable backlog before the first poller ever shows up.
    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;

    let task = runtime
        .poll_workflow_task(
            queue(namespace_id, "queue-a"),
            WorkerIdentity("late-worker".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .expect("late poller must receive the backlogged workflow task");
    assert_eq!(task.run_key, started_state.run_key);
    assert_eq!(task.token.logical_seq, LogicalTaskSeq::ONE);

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

#[tokio::test]
async fn retryable_failure_starts_attempt_two_successor() -> Result<()> {
    // Feature: workflow-retry-chain — a retry-eligible FailWorkflow closes the run
    // Failed with new_execution_run_id and starts a backoff-delayed attempt-2
    // successor chained to the predecessor (Req 1.2, 2.2, 4.2).
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
    let workflow_id = WorkflowId("retry-successor-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");

    let mut request = start_request(namespace_id, workflow_id.clone(), "req-retry-successor");
    // Coefficient 1.0 → a flat 1s backoff; three attempts allowed.
    request.retry_policy = Some(tokeira_types::RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 1.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: Vec::new(),
    });

    let start = runtime.start_workflow(request).await?;
    let predecessor = applied_state(&start);
    let first_task = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("first WFT should be published");

    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::FailWorkflow {
                failure: retryable_app_failure("BoomError"),
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    // The predecessor closed Failed with retry_state InProgress and a successor id.
    let history = store.read_history(predecessor.run_key, 0, 16).await?;
    let successor_run_id = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionFailed {
                new_execution_run_id,
                retry_state,
                ..
            } => {
                assert_eq!(*retry_state, tokeira_kernel::RetryState::InProgress);
                *new_execution_run_id
            }
            _ => None,
        })
        .expect("failed event should carry a retry successor run id");
    let successor_key = RunKey::derive(namespace_id, &workflow_id, successor_run_id);

    // The attempt-2 successor started, chained and backoff-delayed by 1s.
    let successor = wait_for_existing_run(&store, successor_key).await?;
    assert_eq!(successor.attempt, 2);
    assert_eq!(successor.first_execution_run_id, Some(predecessor.run_id));
    assert_eq!(successor.workflow_start_delay, Some(Duration::seconds(1)));
    let successor_history = store.read_history(successor_key, 0, 1).await?;
    assert!(matches!(
        successor_history.first().map(|event| &event.kind),
        Some(HistoryEventKind::WorkflowExecutionStartedV2 {
            continued_execution_run_id,
            attempt,
            eager_execution_accepted: false,
            ..
        }) if *continued_execution_run_id == Some(predecessor.run_id) && *attempt == 2
    ));

    Ok(())
}

#[tokio::test]
async fn non_retryable_failure_is_terminal_without_successor() -> Result<()> {
    // Feature: workflow-retry-chain — a failure whose type is in the policy's
    // non_retryable_error_types closes the run terminally (NonRetryableFailure),
    // with no successor run id and no successor started (Req 4.1).
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
    let workflow_id = WorkflowId("retry-terminal-workflow".to_string());
    let queue = queue(namespace_id, "queue-a");

    let mut request = start_request(namespace_id, workflow_id.clone(), "req-retry-terminal");
    request.retry_policy = Some(tokeira_types::RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 2.0,
        maximum_interval: Some(Duration::seconds(10)),
        maximum_attempts: 3,
        non_retryable_error_types: vec!["FatalError".to_string()],
    });

    let start = runtime.start_workflow(request).await?;
    let predecessor = applied_state(&start);
    let first_task = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("first WFT should be published");

    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: first_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::FailWorkflow {
                failure: retryable_app_failure("FatalError"),
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: tokeira_types::RequestContext::unattributed(time::OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let history = store.read_history(predecessor.run_key, 0, 16).await?;
    let outcome = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionFailed {
                new_execution_run_id,
                retry_state,
                ..
            } => Some((retry_state.clone(), *new_execution_run_id)),
            _ => None,
        })
        .expect("failed event should be present");
    assert_eq!(
        outcome,
        (tokeira_kernel::RetryState::NonRetryableFailure, None)
    );

    Ok(())
}

/// Encode a retryable application `Failure` payload (an `ApplicationFailureInfo`
/// of the given type, not flagged non-retryable) for a FailWorkflow command.
fn retryable_app_failure(error_type: &str) -> Payload {
    use prost::Message as _;
    use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
    let failure = Failure {
        message: "boom".to_string(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: error_type.to_string(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    Payload::new(failure.encode_to_vec())
}

fn applied_state(result: &CommitResult) -> tokeira_kernel::WorkflowState {
    match result {
        CommitResult::Applied { new_state } => new_state.clone(),
        other => panic!("expected applied result, got {other:?}"),
    }
}

fn runtime_with_store(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

fn recovering_runtime_with_store(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new_with_nexus_and_shards(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        "restart-test-owner".to_string(),
        false,
    )
}

async fn wait_for_existing_run(
    store: &Arc<InMemoryStore>,
    run_key: RunKey,
) -> Result<tokeira_kernel::WorkflowState> {
    for _ in 0..100 {
        if let LoadedRun::Existing(state) = store.load_run(run_key).await? {
            return Ok(state);
        }
        tokio::task::yield_now().await;
    }
    anyhow::bail!("successor run was not materialized");
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

fn versioned_queue(
    namespace_id: NamespaceId,
    name: &str,
    deployment: &str,
    build_id: &str,
) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.to_string()),
        task_kind: TaskKind::Workflow,
        deployment: Some(DeploymentId(deployment.to_string())),
        build_id: Some(BuildId(build_id.to_string())),
    }
}

fn completion_callback(url: &str) -> CompletionCallback {
    CompletionCallback {
        spec: CallbackSpec::Nexus {
            url: url.to_string(),
            header: BTreeMap::new(),
        },
        links: Vec::new(),
        trigger: tokeira_kernel::CallbackTrigger::WorkflowClosed,
        registration_time: None,
        state: CallbackState::Standby,
        attempt: 0,
        last_attempt_failure: None,
        last_attempt_complete_time: None,
        next_attempt_at: None,
    }
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
        task_queue: TaskQueueName("queue-a".to_string()),
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
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn signal_with_start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    request_id: &str,
) -> SignalWithStartRequest {
    let start = start_request(namespace_id, workflow_id, request_id);
    SignalWithStartRequest {
        initiator: None,
        run_key: start.run_key,
        namespace_id: start.namespace_id,
        workflow_id: start.workflow_id,
        run_id: start.run_id,
        workflow_type: start.workflow_type,
        task_queue: start.task_queue,
        input: start.input,
        memo: start.memo,
        search_attributes: start.search_attributes,
        workflow_execution_timeout: start.workflow_execution_timeout,
        workflow_run_timeout: start.workflow_run_timeout,
        workflow_task_timeout: start.workflow_task_timeout,
        retry_policy: start.retry_policy,
        conflict_policy: start.conflict_policy,
        reuse_policy: start.reuse_policy,
        header: start.header,
        deployment: start.deployment,
        build_id: start.build_id,
        versioning_override: start.versioning_override,
        workflow_start_delay: start.workflow_start_delay,
        user_metadata: start.user_metadata,
        links: start.links,
        priority: start.priority,
        cron_schedule: start.cron_schedule,
        attempt: start.attempt,
        continued_execution_run_id: start.continued_execution_run_id,
        first_execution_run_id: start.first_execution_run_id,
        parent_run_key: start.parent_run_key,
        parent_workflow_id: start.parent_workflow_id,
        parent_run_id: start.parent_run_id,
        parent_namespace_id: start.parent_namespace_id,
        parent_namespace_name: None,
        parent_initiated_event_id: start.parent_initiated_event_id,
        root_workflow_id: start.root_workflow_id,
        root_run_id: start.root_run_id,
        original_execution_run_id: start.original_execution_run_id,
        continued_failure: start.continued_failure,
        last_completion_result: start.last_completion_result,
        first_run_started_at: start.first_run_started_at,
        request: start.request,
        now: start.now,
        client_cron_schedule: start.client_cron_schedule,
        signal_name: "sig".to_string(),
        signal_input: Payloads::default(),
    }
}

fn signal_request(request_id: &str) -> tokeira_kernel::SignalRequest {
    tokeira_kernel::SignalRequest {
        signal_name: "sig".to_string(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            principal: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
    }
}
