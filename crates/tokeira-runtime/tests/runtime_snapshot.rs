//! Restore-then-recovery equivalence for `InMemoryStore` snapshots.
//!
//! Proves the spec's governing claim (inmemory-store-snapshots, Requirement 4):
//! a store constructed by `InMemoryStore::from_snapshot` and taken through the
//! normal recovery path (`acquire_shard` → sweep) is indistinguishable from a
//! process restart against the original store. Every case runs the SAME
//! recovery against both stores and compares the observable results; nothing
//! here exercises a snapshot-specific fixup path, because none exists.

use std::sync::Arc;

use anyhow::Result;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, LogicalTaskSeq, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, RunKey, SearchAttributes, ShardId, TaskKind, TaskQueueName, WorkerIdentity,
    WorkflowId, WorkflowType,
};

/// A start whose delayed-start timer (if any) fired long before "now": the
/// restore path must treat it as due immediately (absolute-time durable
/// semantics), exactly like a restart after downtime.
const PAST_NOW: i64 = 1_700_000_000;

/// The runtime that seeds workloads. Its timer scanner is effectively inert so
/// that only the restarted runtimes' recovery (the thing under test) fires
/// past-due timers — otherwise the seeder could race the comparison.
fn seeding_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig {
            scan_interval: tokio::time::Duration::from_secs(3600),
            max_timers_per_scan: 100,
        },
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

/// A restarted node: fresh brokers and trackers, recovery sweep on
/// `acquire_shard`. Mirrors `recovering_runtime_with_store` in
/// `runtime_lane.rs`.
fn recovering_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
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
        "snapshot-restart-owner".to_string(),
        false,
    )
}

fn workflow_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("queue-a".to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn activity_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("activity-q".to_string()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

fn start_request(
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    request_id: &str,
    now: OffsetDateTime,
    start_delay: Option<Duration>,
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
        workflow_start_delay: start_delay,
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
            received_at: now,
        },
        now,
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn signal_request(request_id: &str, now: OffsetDateTime) -> tokeira_kernel::SignalRequest {
    tokeira_kernel::SignalRequest {
        signal_name: "sig".to_string(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request: RequestContext {
            request_id: RequestId(request_id.to_string()),
            caller_identity: None,
            principal: None,
            received_at: now,
        },
        now,
    }
}

/// Poll until `expected` workflow tasks are collected (recovery publishes
/// asynchronously after the sweep), then verify no extras follow. Sorted so
/// delivery order does not leak into the comparison.
async fn drain_workflow_tasks(
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: &QueueKey,
    expected: usize,
) -> Result<Vec<(RunKey, LogicalTaskSeq)>> {
    let mut collected = Vec::new();
    for _ in 0..500 {
        if collected.len() == expected {
            break;
        }
        let task = runtime
            .poll_workflow_task(
                queue.clone(),
                WorkerIdentity("worker-a".to_string()),
                tokio::time::Duration::from_millis(5),
            )
            .await?;
        match task {
            Some(task) => collected.push((task.run_key, task.token.logical_seq)),
            None => tokio::task::yield_now().await,
        }
    }
    anyhow::ensure!(
        collected.len() == expected,
        "expected {expected} recovered workflow tasks, drained {}",
        collected.len()
    );
    let extra = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?;
    anyhow::ensure!(extra.is_none(), "recovered more workflow tasks than seeded");
    collected.sort();
    Ok(collected)
}

/// One seeded workflow in a generated workload.
#[derive(Clone, Copy, Debug)]
enum WorkloadKind {
    /// Plain start: first WFT immediately pollable after recovery.
    Immediate,
    /// Start plus a buffered signal against the pending first WFT.
    Signaled,
    /// Delayed start whose timer fire time is deep in the past: recovery must
    /// fire it and surface the first WFT.
    DelayPast,
    /// Delayed start an hour in the future: recovery must NOT surface a WFT.
    DelayFuture,
}

fn workload_kind() -> impl Strategy<Value = WorkloadKind> {
    prop_oneof![
        Just(WorkloadKind::Immediate),
        Just(WorkloadKind::Signaled),
        Just(WorkloadKind::DelayPast),
        Just(WorkloadKind::DelayFuture),
    ]
}

async fn seed_workload(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workload: &[WorkloadKind],
) -> Result<()> {
    for (index, kind) in workload.iter().enumerate() {
        let workflow_id = WorkflowId(format!("wf-{index}"));
        let request_id = format!("req-{index}");
        let (now, delay) = match kind {
            WorkloadKind::Immediate | WorkloadKind::Signaled => (OffsetDateTime::now_utc(), None),
            WorkloadKind::DelayPast => (
                OffsetDateTime::from_unix_timestamp(PAST_NOW)?,
                Some(Duration::seconds(40)),
            ),
            WorkloadKind::DelayFuture => (OffsetDateTime::now_utc(), Some(Duration::hours(1))),
        };
        let result = runtime
            .start_workflow(start_request(
                namespace_id,
                workflow_id.clone(),
                &request_id,
                now,
                delay,
            ))
            .await?;
        anyhow::ensure!(
            matches!(result, CommitResult::Applied { .. }),
            "seed start must apply"
        );
        if matches!(kind, WorkloadKind::Signaled) {
            runtime
                .signal_workflow(
                    ExecutionRef {
                        namespace_id,
                        workflow_id,
                        run_id: None,
                    },
                    signal_request(&format!("req-signal-{index}"), OffsetDateTime::now_utc()),
                )
                .await?;
        }
    }
    Ok(())
}

/// Recover both stores — the original (a plain process restart) and a
/// snapshot-restored copy — and compare every observable: repository reads
/// and the exact set of workflow tasks recovery surfaces.
async fn assert_recovery_equivalence(
    store: Arc<InMemoryStore>,
    namespace_id: NamespaceId,
    expected_tasks: usize,
) -> Result<()> {
    let snapshot = store.snapshot().await?;
    let restored = Arc::new(InMemoryStore::from_snapshot(&snapshot)?);

    let original_runs = store.list_runs_for_namespace(namespace_id).await?;
    let restored_runs = restored.list_runs_for_namespace(namespace_id).await?;
    assert_eq!(original_runs, restored_runs);
    for run_key in &original_runs {
        let original_history = store.read_history(*run_key, 0, 128).await?;
        let restored_history = restored.read_history(*run_key, 0, 128).await?;
        assert_eq!(original_history, restored_history);
    }

    let restart_original = recovering_runtime(store);
    restart_original.acquire_shard(ShardId(0)).await?;
    let restart_restored = recovering_runtime(restored);
    restart_restored.acquire_shard(ShardId(0)).await?;

    let queue = workflow_queue(namespace_id);
    let from_original = drain_workflow_tasks(&restart_original, &queue, expected_tasks).await?;
    let from_restored = drain_workflow_tasks(&restart_restored, &queue, expected_tasks).await?;
    assert_eq!(from_original, from_restored);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Feature: inmemory-store-snapshots, Property 4: restore-then-recovery equivalence
    #[test]
    fn property_restore_then_recovery_equals_restart(
        workload in prop::collection::vec(workload_kind(), 1..4),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let store = Arc::new(InMemoryStore::default());
            let namespace_id = NamespaceId::new();
            let seeder = seeding_runtime(store.clone());
            seed_workload(&seeder, namespace_id, &workload).await.unwrap();
            // Everything except a future-dated delayed start surfaces exactly
            // one recovered first workflow task.
            let expected_tasks = workload
                .iter()
                .filter(|kind| !matches!(kind, WorkloadKind::DelayFuture))
                .count();
            assert_recovery_equivalence(store, namespace_id, expected_tasks)
                .await
                .unwrap();
            Ok::<(), TestCaseError>(())
        })?;
    }
}

/// Requirement 4.2 pinned deterministically: a timer already past due when the
/// snapshot is restored fires immediately through normal recovery — the
/// restored store needs no special handling, matching a delayed restart
/// against durable storage.
#[tokio::test]
async fn past_due_start_delay_fires_immediately_after_restore() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let seeder = seeding_runtime(store.clone());
    let result = seeder
        .start_workflow(start_request(
            namespace_id,
            WorkflowId("delayed".to_string()),
            "req-delayed",
            OffsetDateTime::from_unix_timestamp(PAST_NOW)?,
            Some(Duration::seconds(40)),
        ))
        .await?;
    let CommitResult::Applied { new_state } = result else {
        panic!("start should apply, got {result:?}");
    };

    let snapshot = store.snapshot().await?;
    let restored = Arc::new(InMemoryStore::from_snapshot(&snapshot)?);
    let restarted = recovering_runtime(restored);
    restarted.acquire_shard(ShardId(0)).await?;

    let tasks = drain_workflow_tasks(&restarted, &workflow_queue(namespace_id), 1).await?;
    assert_eq!(tasks, vec![(new_state.run_key, LogicalTaskSeq(1))]);
    Ok(())
}

/// A dispatchable activity survives the snapshot and is recovered exactly like
/// a restart against the original store (`runtime_activity.rs`'s restart
/// pattern with a snapshot-restored store).
#[tokio::test]
async fn scheduled_activity_recovers_identically_from_restored_store() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let seeder = seeding_runtime(store.clone());
    let now = OffsetDateTime::now_utc();

    let start = seeder
        .start_workflow(start_request(
            namespace_id,
            WorkflowId("with-activity".to_string()),
            "req-activity",
            now,
            None,
        ))
        .await?;
    let CommitResult::Applied { new_state } = start else {
        panic!("start should apply, got {start:?}");
    };
    let workflow_task = seeder
        .poll_workflow_task(
            workflow_queue(namespace_id),
            WorkerIdentity("worker-a".to_string()),
            tokio::time::Duration::from_millis(5),
        )
        .await?
        .expect("first workflow task should be pollable");
    seeder
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: workflow_task.token,
            identity: WorkerIdentity("worker-a".to_string()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ScheduleActivity {
                activity_id: "activity-1".to_string(),
                activity_type: "activity-type".to_string(),
                task_queue: TaskQueueName("activity-q".to_string()),
                input: Payloads(vec![Payload::new(b"input".to_vec())]),
                header: None,
                request_eager_execution: false,
                retry_policy: None,
                deployment: None,
                build_id: None,
                schedule_to_close_timeout: Some(Duration::minutes(5)),
                schedule_to_start_timeout: Some(Duration::seconds(30)),
                start_to_close_timeout: Some(Duration::minutes(1)),
                heartbeat_timeout: Some(Duration::seconds(20)),
                priority: None,
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            now,
        })
        .await?;

    let snapshot = store.snapshot().await?;
    let restored = Arc::new(InMemoryStore::from_snapshot(&snapshot)?);

    let mut recovered = Vec::new();
    for candidate in [store, restored] {
        let restarted = recovering_runtime(candidate);
        restarted.acquire_shard(ShardId(0)).await?;
        let mut started = None;
        for _ in 0..500 {
            started = restarted
                .poll_activity_task(
                    activity_queue(namespace_id),
                    WorkerIdentity("worker-a".to_string()),
                    tokio::time::Duration::from_millis(5),
                )
                .await?;
            if started.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let started = started.expect("recovery should republish the scheduled activity");
        recovered.push((started.run_key, started.activity_id.clone()));
    }
    assert_eq!(recovered[0], recovered[1]);
    assert_eq!(recovered[0], (new_state.run_key, "activity-1".to_string()));
    Ok(())
}
