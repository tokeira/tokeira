#![cfg(feature = "dsql-integration")]
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

//! `activity-inputs-from-history` through the runtime on an operator-owned
//! ephemeral Aurora DSQL cluster. No connection occurs unless
//! `TOKEIRA_DSQL_TEST_DATABASE_URL` is set. The storage live suite applies the
//! schema first, as for the runtime's other live checks; run it serially
//! (`docs/testing/dsql-live-suites.md`).

use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEventKind, ResetRequest, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, StartedActivityTask, StartedWorkflowTask, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{
    CommitResult, RunRepository, codec,
    dsql::{
        DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig,
        run_repository::DsqlRunRepository,
    },
    write_budget::MAX_RESET_BATCH_BYTES,
};
use tokeira_types::{
    ExecutionRef, Headers, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, RunId, RunKey, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity,
    WorkflowId, WorkflowType,
};

const WORKFLOW_QUEUE: &str = "workflow-q";
const ACTIVITY_QUEUE: &str = "activity-q";

/// The cluster's runtime and its repository, or none when the live suite isn't
/// configured.
async fn dsql_runtime()
-> Result<Option<(TokeiraRuntime<DsqlRunRepository>, Arc<DsqlRunRepository>)>> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(None);
    };
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 4,
                inflight_limit: 2,
                ..ReservoirConfig::default()
            },
            shard_count: 1,
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    let (_, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);
    Ok(Some((
        TokeiraRuntime::new(
            repo.clone(),
            2,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
            BacklogConfig::default(),
        ),
        repo,
    )))
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.to_string()),
        caller_identity: None,
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn start_request(namespace_id: NamespaceId, workflow_id: &str) -> StartRequest {
    let run_id = tokeira_types::RunId::new();
    // The key a start derives, which a lookup of the run by its id resolves.
    let workflow_id = WorkflowId(workflow_id.to_string());
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::derive(namespace_id, &workflow_id, run_id),
        namespace_id,
        workflow_id,
        run_id,
        workflow_type: WorkflowType("inputs".to_string()),
        task_queue: TaskQueueName(WORKFLOW_QUEUE.to_string()),
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
        client_cron_schedule: None,
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
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn queue(namespace_id: NamespaceId, name: &str, task_kind: TaskKind) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.to_string()),
        task_kind,
        deployment: None,
        build_id: None,
    }
}

fn input(index: usize, len: usize) -> Payloads {
    Payloads(vec![Payload::new(vec![
        u8::try_from(index % 251).unwrap();
        len
    ])])
}

fn header(index: usize) -> Headers {
    Headers(BTreeMap::from([(
        "trace".to_string(),
        Payload::new(format!("header-{index}").into_bytes()),
    )]))
}

fn schedule(index: usize, input_bytes: usize) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".to_string(),
        task_queue: TaskQueueName(ACTIVITY_QUEUE.to_string()),
        input: input(index, input_bytes),
        header: Some(header(index)),
        request_eager_execution: false,
        retry_policy: None,
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::minutes(1)),
        heartbeat_timeout: None,
        priority: None,
    }
}

/// Six workflow tasks each schedule an activity with an input of about 200 KB,
/// asking for the next task, so six inputs are pending at once: 1.2 MB, past
/// DSQL's value limit had the run's state held them. Every completion commits,
/// and each activity is delivered with its own input and header (bugfix 1.1,
/// 2.1, 2.2).
#[tokio::test]
async fn dsql_inputs_six_pending_activities_with_large_inputs() -> Result<()> {
    let Some((runtime, _repo)) = dsql_runtime().await? else {
        return Ok(());
    };
    let namespace_id = NamespaceId::new();
    let CommitResult::Applied { .. } = runtime
        .start_workflow(start_request(namespace_id, "six-large-inputs"))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    for index in 0..6 {
        let task = poll_workflow_task(&runtime, namespace_id).await?;
        complete(&runtime, task, vec![schedule(index, 200_000)], true)
            .await
            .map_err(|error| error.context(format!("completing workflow task {index}")))?;
    }

    let delivered = poll_started(&runtime, namespace_id, 6)
        .await?
        .into_iter()
        .map(|task| (task.activity_id.clone(), (task.input, task.header)))
        .collect::<BTreeMap<_, _>>();
    for index in 0..6 {
        assert_eq!(
            delivered.get(&format!("activity-{index}")),
            Some(&(input(index, 200_000), Some(header(index)))),
        );
    }
    Ok(())
}

async fn poll_workflow_task(
    runtime: &TokeiraRuntime<DsqlRunRepository>,
    namespace_id: NamespaceId,
) -> Result<StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            queue(namespace_id, WORKFLOW_QUEUE, TaskKind::Workflow),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_secs(5),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("a workflow task"))
}

async fn complete(
    runtime: &TokeiraRuntime<DsqlRunRepository>,
    task: StartedWorkflowTask,
    commands: Vec<WorkflowCommand>,
    force_new_workflow_task: bool,
) -> Result<()> {
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
            commands,
            force_new_workflow_task,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
            command_sizes: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Poll until `count` activity tasks start; an offer a reset left stale answers
/// nothing, and polling goes on.
async fn poll_started(
    runtime: &TokeiraRuntime<DsqlRunRepository>,
    namespace_id: NamespaceId,
    count: usize,
) -> Result<Vec<StartedActivityTask>> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(60);
    let mut started = Vec::new();
    while started.len() < count {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "{} of {count} activity tasks started",
            started.len()
        );
        if let Some(task) = runtime
            .poll_activity_task(
                queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity),
                WorkerIdentity("worker".to_string()),
                tokio::time::Duration::from_secs(1),
            )
            .await?
        {
            started.push(task);
        }
    }
    Ok(started)
}

/// One workflow task schedules three activities, so their scheduled events share
/// one history batch; each is delivered with its own input and header, the
/// second from the middle of the batch.
#[tokio::test]
async fn dsql_inputs_three_activities_scheduled_in_one_batch() -> Result<()> {
    let Some((runtime, _repo)) = dsql_runtime().await? else {
        return Ok(());
    };
    let namespace_id = NamespaceId::new();
    let CommitResult::Applied { .. } = runtime
        .start_workflow(start_request(namespace_id, "one-batch"))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    let task = poll_workflow_task(&runtime, namespace_id).await?;
    let commands = (0..3)
        .map(|index| schedule(index, 50_000 + index))
        .collect();
    complete(&runtime, task, commands, false).await?;

    let delivered = poll_started(&runtime, namespace_id, 3).await?;
    let delivered = delivered
        .into_iter()
        .map(|task| (task.activity_id.clone(), (task.input, task.header)))
        .collect::<BTreeMap<_, _>>();
    for index in 0..3 {
        assert_eq!(
            delivered.get(&format!("activity-{index}")),
            Some(&(input(index, 50_000 + index), Some(header(index)))),
        );
    }
    Ok(())
}

/// Eight workflow tasks each schedule an activity with an input of about 100 KB,
/// and the workflow is reset after the last. The successor's copied history is
/// cut into batches of up to 512 KiB that each hold several scheduled events, and
/// each activity the successor re-dispatches is delivered with its input and
/// header, read from those batches.
#[tokio::test]
async fn dsql_inputs_from_a_reset_successors_copied_batches() -> Result<()> {
    let Some((runtime, repo)) = dsql_runtime().await? else {
        return Ok(());
    };
    let namespace_id = NamespaceId::new();
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, "reset-successor"))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    for index in 0..8 {
        let task = poll_workflow_task(&runtime, namespace_id).await?;
        complete(&runtime, task, vec![schedule(index, 100_000)], true).await?;
    }
    let task = poll_workflow_task(&runtime, namespace_id).await?;
    complete(&runtime, task, Vec::new(), false).await?;
    let fork_event_id = repo
        .read_history_to_end(new_state.run_key, 0)
        .await?
        .iter()
        .rev()
        .find(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. }))
        .map(|event| event.event_id)
        .ok_or_else(|| anyhow::anyhow!("a completed workflow task"))?;
    let successor_run_id = RunId::new();
    runtime
        .reset_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId("reset-successor".to_string()),
                run_id: Some(new_state.run_id),
            },
            ResetRequest {
                fork_event_id,
                new_run_id: successor_run_id,
                reapply_exclude_signal: false,
                reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: None,
                reason: "deliver from the copied history".to_string(),
                request: context("reset"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    let successor = RunKey::derive(
        namespace_id,
        &WorkflowId("reset-successor".to_string()),
        successor_run_id,
    );

    // The successor's copied batches, cut as the store cuts them.
    let copied = repo
        .read_history_to_end(successor, 0)
        .await?
        .into_iter()
        .filter(|event| event.event_id < fork_event_id)
        .collect::<Vec<_>>();
    let batches =
        codec::reset_history_batches(&copied, &vec![None; copied.len()], MAX_RESET_BATCH_BYTES)?;
    let batch_openers = batches
        .iter()
        .map(|batch| copied[batch.start].event_id)
        .collect::<Vec<_>>();

    let delivered = poll_started(&runtime, namespace_id, 8).await?;
    let mut inside = 0;
    for task in &delivered {
        assert_eq!(task.run_key, successor);
        let index = task
            .activity_id
            .strip_prefix("activity-")
            .and_then(|index| index.parse::<usize>().ok())
            .expect("a scheduled activity");
        assert_eq!(
            (&task.input, &task.header),
            (&input(index, 100_000), &Some(header(index)))
        );
        if !batch_openers.contains(&task.token.schedule_event_id) {
            inside += 1;
        }
    }
    assert!(
        batches.len() > 1 && inside > 0,
        "{} batches; {inside} scheduled events inside one",
        batches.len()
    );
    Ok(())
}
