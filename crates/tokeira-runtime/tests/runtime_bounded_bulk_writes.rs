//! `bounded-bulk-writes` through the runtime's reset on the in-memory store.
//!
//! A reset materializes its successor in several transactions behind a
//! bulk-write record. A materialization that stops part way leaves no visible
//! successor, and the runtime removes the record and the rows it wrote; a node
//! that takes the shard over switches a materialization another node left
//! before the shard admits commands. A reset of a closed workflow names the
//! closed run as the one its successor replaces, so it succeeds.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{LoadedRun, ResetRequest, StartRequest, WorkflowCommand};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, ResetWorkflowResult,
    StartedWorkflowTask, TimerScannerConfig, TokeiraRuntime, WorkflowActivation,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{BulkWritePhase, CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, ShardId, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

const QUEUE: &str = "bulk-queue";
const WORKFLOW: &str = "reset-in-pages";

fn runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

/// A node taking over: a new runtime over the same store that acquires the
/// shard, recovering it before it admits commands.
async fn restarted(store: Arc<InMemoryStore>) -> Result<TokeiraRuntime<InMemoryStore>> {
    let runtime = TokeiraRuntime::new_with_nexus_and_shards(
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
        "taking-over".to_string(),
        false,
    );
    runtime.acquire_shard(ShardId(0)).await?;
    Ok(runtime)
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.to_string()),
        caller_identity: Some("tester".to_string()),
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn start_request(namespace_id: NamespaceId) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::new(),
        namespace_id,
        workflow_id: WorkflowId(WORKFLOW.to_string()),
        run_id,
        workflow_type: WorkflowType("bulk".to_string()),
        task_queue: TaskQueueName(QUEUE.to_string()),
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
        first_execution_run_id: Some(run_id),
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
        request: context("start"),
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

/// Start the workflow and answer its first workflow task with `commands`.
/// Returns the run's key and id.
async fn start_and_answer(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    commands: Vec<WorkflowCommand>,
) -> Result<(RunKey, RunId)> {
    let request = start_request(namespace_id);
    let (run_key, run_id) = (request.run_key, request.run_id);
    let CommitResult::Applied { .. } = runtime.start_workflow(request).await? else {
        anyhow::bail!("start not applied");
    };
    let task = poll(runtime, namespace_id).await?;
    runtime
        .complete_workflow_task(completion(task, commands))
        .await?;
    Ok((run_key, run_id))
}

/// The phase of `run_key`'s bulk-write record, if it has one.
async fn phase_of(store: &InMemoryStore, run_key: RunKey) -> Result<Option<BulkWritePhase>> {
    Ok(store
        .list_run_bulk_writes(ShardId(0), None, 100)
        .await?
        .into_iter()
        .find(|record| record.run_key == run_key)
        .map(|record| record.phase))
}

async fn poll(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<StartedWorkflowTask> {
    let activation = runtime
        .poll_workflow_activation(
            QueueKey {
                namespace_id,
                task_queue: TaskQueueName(QUEUE.to_string()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            None,
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?;
    match activation {
        Some(WorkflowActivation::WorkflowTask(task)) => Ok(task),
        other => anyhow::bail!("expected the first workflow task, got {other:?}"),
    }
}

fn completion(
    task: StartedWorkflowTask,
    commands: Vec<WorkflowCommand>,
) -> tokeira_kernel::WorkflowTaskCompletedRequest {
    tokeira_kernel::WorkflowTaskCompletedRequest {
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
        commands,
        force_new_workflow_task: false,
        limits: Default::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: OffsetDateTime::now_utc(),
        command_sizes: Vec::new(),
    }
}

/// Reset the run `base_run_id` to its first task's WorkflowTaskCompleted.
async fn reset(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    base_run_id: RunId,
    new_run_id: RunId,
) -> Result<ResetWorkflowResult> {
    runtime
        .reset_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId(WORKFLOW.to_string()),
                run_id: Some(base_run_id),
            },
            ResetRequest {
                fork_event_id: 4,
                new_run_id,
                reapply_exclude_signal: false,
                reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(),
                // The runtime reads it at admission.
                expected_current_run_key: None,
                reason: "reset".to_string(),
                request: context("reset"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await
}

/// A reset whose materialization stops after it has copied the history leaves
/// no successor, no record and none of the rows it wrote (criteria 2.8, 2.9).
#[tokio::test]
async fn a_failed_materialization_leaves_nothing_behind() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let (_, base_run_id) = start_and_answer(&runtime, namespace_id, Vec::new()).await?;

    // The record's transaction and the copy's commit; the final one fails.
    store.fail_bulk_write_after(2).await;
    let new_run_id = RunId::new();
    let refused = reset(&runtime, namespace_id, base_run_id, new_run_id).await;
    assert!(refused.is_err(), "the materialization was stopped");

    let successor = RunKey::derive(namespace_id, &WorkflowId(WORKFLOW.to_string()), new_run_id);
    assert!(matches!(
        store.load_run(successor).await?,
        LoadedRun::Absent
    ));
    assert!(store.read_history_to_end(successor, 0).await?.is_empty());
    assert!(
        store
            .list_run_bulk_writes(ShardId(0), None, 10)
            .await?
            .is_empty()
    );
    Ok(())
}

/// A reset of a closed workflow replaces the closed run, which the pointer
/// still names (criteria 2.13, 3.5).
#[tokio::test]
async fn a_reset_of_a_closed_workflow_succeeds() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let (_, base_run_id) = start_and_answer(
        &runtime,
        namespace_id,
        vec![WorkflowCommand::CompleteWorkflow {
            result: Payloads::default(),
        }],
    )
    .await?;

    let reset = reset(&runtime, namespace_id, base_run_id, RunId::new()).await?;
    assert_eq!(
        store
            .find_latest_run(namespace_id, &WorkflowId(WORKFLOW.to_string()))
            .await?,
        Some(reset.successor_run_key)
    );
    let LoadedRun::Existing(successor) = store.load_run(reset.successor_run_key).await? else {
        anyhow::bail!("the successor is stored");
    };
    assert!(successor.status.is_open());
    Ok(())
}

/// A node that takes a shard over switches the materializations another node
/// left recorded on it before the shard admits commands, so none of them can
/// make its successor visible afterwards (criterion 2.9).
#[tokio::test]
async fn a_takeover_abandons_a_stopped_materialization() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    let (base_run_key, _) = start_and_answer(&before, namespace_id, Vec::new()).await?;
    drop(before);

    // The record commits and the copy stops, as a node that stops there
    // leaves its materialization.
    store.fail_bulk_write_after(1).await;
    let successor_run_id = RunId::new();
    store
        .materialize_reset_successor(base_run_key, 4, successor_run_id, Some(base_run_key))
        .await
        .expect_err("the copy stops");
    let successor = RunKey::derive(
        namespace_id,
        &WorkflowId(WORKFLOW.to_string()),
        successor_run_id,
    );
    assert_eq!(
        phase_of(&store, successor).await?,
        Some(BulkWritePhase::Materializing)
    );

    let _after = restarted(store.clone()).await?;
    assert_ne!(
        phase_of(&store, successor).await?,
        Some(BulkWritePhase::Materializing)
    );
    Ok(())
}
