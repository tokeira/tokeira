//! `activity-inputs-from-history` through the runtime on the in-memory store.
//!
//! An activity's input and header live only in its ActivityTaskScheduled event.
//! The run's state, the activity's dispatch row and a backlog entry for its task
//! carry no copy, and a delivered task carries the event's input and header on
//! every path that delivers it.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    ActivityControlTarget, ActivityPriorityPatch, ActivityRetryPolicyPatch, FieldChange,
    HistoryEvent, HistoryEventKind, LoadedRun, PauseActivityRequest, ResetRequest, StartRequest,
    WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, ResetActivitiesRequest,
    StartedActivityTask, StartedWorkflowTask, TimerScannerConfig, TokeiraRuntime,
    UnpauseActivitiesRequest, UpdateActivitiesOptionsRequest, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository, codec};
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, Headers, Memo, NamespaceId, Payload, Payloads, QueueKey,
    RequestContext, RequestId, RetryPolicy, RunId, RunKey, SearchAttributes, ShardId, TaskKind,
    TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

const WORKFLOW_QUEUE: &str = "workflow-q";
const ACTIVITY_QUEUE: &str = "activity-q";
/// An input larger than everything else a run's state, a dispatch row or a
/// backlog entry holds, so any copy of it shows in their sizes.
const INPUT_BYTES: usize = 100_000;

fn runtime(store: Arc<InMemoryStore>, backlog: BacklogConfig) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        backlog,
    )
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

/// `len` bytes that differ from activity to activity.
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

/// Start a workflow and complete its first workflow task with `commands`.
async fn start_and_schedule(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: &str,
    commands: Vec<WorkflowCommand>,
) -> Result<RunKey> {
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, workflow_id))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    let task = runtime
        .poll_workflow_task(
            queue(namespace_id, WORKFLOW_QUEUE, TaskKind::Workflow),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?
        .expect("the first workflow task");
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
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
            command_sizes: Vec::new(),
        })
        .await?;
    Ok(new_state.run_key)
}

async fn poll_activity(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<StartedActivityTask> {
    runtime
        .poll_activity_task(
            queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("an activity task"))
}

/// The run's state holds none of its pending activities' inputs: it encodes
/// smaller than any one of them (bugfix 1.1, 1.2, 2.1).
#[tokio::test]
async fn a_runs_state_holds_none_of_its_pending_inputs() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone(), BacklogConfig::default());
    let namespace_id = NamespaceId::new();
    let commands = (0..3).map(|index| schedule(index, INPUT_BYTES)).collect();
    let run_key = start_and_schedule(&runtime, namespace_id, "state", commands).await?;

    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        anyhow::bail!("the run is stored");
    };
    assert_eq!(state.activities.len(), 3);
    let encoded = codec::encode_workflow_state(&state)?.len();
    assert!(
        encoded < INPUT_BYTES,
        "the state encodes to {encoded} bytes with three pending inputs of {INPUT_BYTES}"
    );
    Ok(())
}

/// A pending activity's dispatch row carries no copy of its input (bugfix 1.3,
/// 2.1).
#[tokio::test]
async fn a_dispatch_row_holds_no_input() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone(), BacklogConfig::default());
    let namespace_id = NamespaceId::new();
    start_and_schedule(
        &runtime,
        namespace_id,
        "row",
        vec![schedule(0, INPUT_BYTES)],
    )
    .await?;

    let rows = store
        .list_all_dispatchable_activity_tasks(
            &queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity),
            10,
        )
        .await?;
    assert_eq!(rows.len(), 1);
    let size = serde_json::to_vec(&rows[0])?.len();
    assert!(
        size < INPUT_BYTES,
        "the dispatch row serializes to {size} bytes for an input of {INPUT_BYTES}"
    );
    Ok(())
}

/// A task spilled to the durable backlog carries no copy of its input (bugfix
/// 1.4, 2.1).
#[tokio::test]
async fn a_spilled_backlog_entry_holds_no_input() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(
        store.clone(),
        BacklogConfig {
            activity_grace_window: tokio::time::Duration::ZERO,
            grace_scan_interval: tokio::time::Duration::from_millis(10),
            ..BacklogConfig::default()
        },
    );
    let namespace_id = NamespaceId::new();
    start_and_schedule(
        &runtime,
        namespace_id,
        "spill",
        vec![schedule(0, INPUT_BYTES)],
    )
    .await?;

    // No worker polls, so the grace scanner spills the task and nothing drains
    // it; the ceiling only bounds the suite.
    let activity_queue = queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity);
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    let entry = loop {
        if let Some(entry) = store.drain_backlog(&activity_queue, 10).await?.pop() {
            break entry;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "the task was never spilled"
        );
        tokio::task::yield_now().await;
    };
    let size = codec::encode_backlog_payload(&entry.payload, entry.priority.as_ref())?.len();
    assert!(
        size < INPUT_BYTES,
        "the backlog entry encodes to {size} bytes for an input of {INPUT_BYTES}"
    );
    Ok(())
}

/// A polled task carries the input and header the workflow scheduled the
/// activity with (bugfix 2.2, 3.1).
#[tokio::test]
async fn a_polled_task_carries_its_scheduled_input_and_header() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone(), BacklogConfig::default());
    let namespace_id = NamespaceId::new();
    let commands = (0..3).map(|index| schedule(index, 1_000 + index)).collect();
    start_and_schedule(&runtime, namespace_id, "delivery", commands).await?;

    let mut delivered = BTreeMap::new();
    for _ in 0..3 {
        let task = poll_activity(&runtime, namespace_id).await?;
        delivered.insert(task.activity_id.clone(), (task.input, task.header));
    }
    for index in 0..3 {
        assert_eq!(
            delivered.get(&format!("activity-{index}")),
            Some(&(input(index, 1_000 + index), Some(header(index)))),
        );
    }
    Ok(())
}

// ---- Properties over generated inputs, headers and delivery paths ----

/// A node after a restart: a new runtime over the same store that takes its
/// shard and recovers it, republishing the runs' pending tasks.
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
        "restarted-owner".to_string(),
        false,
    );
    runtime.acquire_shard(ShardId(0)).await?;
    Ok(runtime)
}

/// Bytes no input or header of another activity, nor anything else a run holds,
/// repeats: a SplitMix64 stream from `seed`.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        bytes.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

/// An activity's input and header, as the workflow schedules it.
#[derive(Clone, Debug, PartialEq)]
struct Scheduled {
    input: Payloads,
    header: Option<Headers>,
}

fn arb_scheduled() -> impl Strategy<Value = Scheduled> {
    (32usize..512, any::<u64>(), prop::option::of(any::<u64>())).prop_map(|(len, seed, header)| {
        Scheduled {
            input: Payloads(vec![Payload::new(noise(len, seed))]),
            header: header.map(|seed| {
                Headers(BTreeMap::from([(
                    "trace".to_string(),
                    Payload::new(noise(32, seed)),
                )]))
            }),
        }
    })
}

/// Schedule `activity-{index}` with `scheduled`'s input and header. Retries are
/// due at once, so a retried attempt is delivered without waiting.
fn schedule_generated(index: usize, scheduled: &Scheduled) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".to_string(),
        task_queue: TaskQueueName(ACTIVITY_QUEUE.to_string()),
        input: scheduled.input.clone(),
        header: scheduled.header.clone(),
        request_eager_execution: false,
        retry_policy: Some(RetryPolicy {
            initial_interval: Duration::ZERO,
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        }),
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::minutes(1)),
        heartbeat_timeout: None,
        priority: None,
    }
}

async fn poll_workflow_task(
    runtime: &TokeiraRuntime<InMemoryStore>,
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
    runtime: &TokeiraRuntime<InMemoryStore>,
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

/// Poll until `count` activity tasks start. A poll that finds an offer an
/// options update, a pause or a reset made stale answers nothing, and polling
/// goes on.
async fn poll_started(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    count: usize,
) -> Result<Vec<StartedActivityTask>> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
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
                tokio::time::Duration::from_millis(200),
            )
            .await?
        {
            started.push(task);
        }
    }
    Ok(started)
}

/// The input and header of `task`'s ActivityTaskScheduled event in its run's
/// history.
async fn scheduled_event(store: &InMemoryStore, task: &StartedActivityTask) -> Result<Scheduled> {
    match store
        .read_history_event(task.run_key, task.token.schedule_event_id)
        .await?
    {
        Some(HistoryEvent {
            kind: HistoryEventKind::ActivityTaskScheduled { input, header, .. },
            ..
        }) => Ok(Scheduled { input, header }),
        other => anyhow::bail!("not a scheduled event: {other:?}"),
    }
}

fn operator_context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.to_string()),
        caller_identity: Some("operator".to_string()),
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn every_activity() -> ActivityControlTarget {
    ActivityControlTarget::Type("work".to_string())
}

/// A path that delivers an activity's task to a worker.
#[derive(Clone, Copy, Debug)]
enum Path {
    Poll,
    Eager,
    Retry,
    OptionsUpdate,
    PauseUnpause,
    ResetActivity,
    Recovery,
    BacklogDrain,
    ResetSuccessor,
}

fn arb_path() -> impl Strategy<Value = Path> {
    prop_oneof![
        Just(Path::Poll),
        Just(Path::Eager),
        Just(Path::Retry),
        Just(Path::OptionsUpdate),
        Just(Path::PauseUnpause),
        Just(Path::ResetActivity),
        Just(Path::Recovery),
        Just(Path::BacklogDrain),
        Just(Path::ResetSuccessor),
    ]
}

/// Schedule `activities` in one workflow task, deliver their tasks along `path`,
/// and answer what each delivered task carried, by activity id.
async fn deliver(
    path: Path,
    activities: &[Scheduled],
) -> Result<BTreeMap<String, (StartedActivityTask, Scheduled)>> {
    let store = Arc::new(InMemoryStore::default());
    let backlog = match path {
        Path::BacklogDrain => BacklogConfig {
            activity_grace_window: tokio::time::Duration::ZERO,
            grace_scan_interval: tokio::time::Duration::from_millis(10),
            ..BacklogConfig::default()
        },
        _ => BacklogConfig::default(),
    };
    let mut runtime = runtime(store.clone(), backlog);
    let namespace_id = NamespaceId::new();
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, "delivery-paths"))
        .await?
    else {
        anyhow::bail!("start not applied");
    };
    let run_key = new_state.run_key;
    let commands = activities
        .iter()
        .enumerate()
        .map(|(index, scheduled)| schedule_generated(index, scheduled))
        .collect();
    let task = poll_workflow_task(&runtime, namespace_id).await?;
    complete(
        &runtime,
        task,
        commands,
        matches!(path, Path::ResetSuccessor),
    )
    .await?;
    let activity_queue = queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity);

    let mut started = Vec::new();
    match path {
        Path::Poll => {}
        Path::Eager => {
            // The completion's own answer claims the task with the command's
            // input and header, as the edge's eager path does.
            started.push(
                runtime
                    .try_claim_activity_task(
                        activity_queue.clone(),
                        run_key,
                        "activity-0".to_string(),
                        WorkerIdentity("worker".to_string()),
                        activities[0].input.clone(),
                        activities[0].header.clone(),
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("an eager claim"))?,
            );
        }
        Path::Retry => {
            for task in poll_started(&runtime, namespace_id, activities.len()).await? {
                runtime
                    .fail_activity_task(
                        task.token,
                        Payload::new(b"retryable".to_vec()),
                        None,
                        false,
                        None,
                        None,
                        RequestContext::unattributed(OffsetDateTime::now_utc()),
                    )
                    .await?;
            }
        }
        Path::OptionsUpdate => {
            runtime
                .update_activity_options(
                    run_key,
                    UpdateActivitiesOptionsRequest {
                        target: every_activity(),
                        task_queue: FieldChange::Unchanged,
                        schedule_to_close_timeout: FieldChange::Unchanged,
                        schedule_to_start_timeout: FieldChange::Unchanged,
                        start_to_close_timeout: FieldChange::Unchanged,
                        heartbeat_timeout: FieldChange::Set(Some(Duration::seconds(30))),
                        retry_policy: ActivityRetryPolicyPatch::default(),
                        priority: ActivityPriorityPatch::default(),
                        restore_original_options: false,
                        request: operator_context("options"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
        }
        Path::PauseUnpause => {
            runtime
                .pause_activities(
                    run_key,
                    PauseActivityRequest {
                        target: every_activity(),
                        identity: "operator".to_string(),
                        reason: "pause".to_string(),
                        rule_id: None,
                        request: operator_context("pause"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
            runtime
                .unpause_activities(
                    run_key,
                    UnpauseActivitiesRequest {
                        target: every_activity(),
                        reset_attempts: false,
                        reset_heartbeat: false,
                        jitter: None,
                        request: operator_context("unpause"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
        }
        Path::ResetActivity => {
            // Restoring the original options reads each scheduled event too.
            runtime
                .reset_activities(
                    run_key,
                    ResetActivitiesRequest {
                        target: every_activity(),
                        reset_heartbeat: false,
                        keep_paused: false,
                        jitter: None,
                        restore_original_options: true,
                        request: operator_context("reset-activity"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
        }
        Path::Recovery => {
            drop(runtime);
            runtime = restarted(store.clone()).await?;
        }
        Path::BacklogDrain => {
            // Every task spills, unclaimed, and a poll drains it back.
            let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
            let mut spilled = Vec::new();
            while spilled.len() < activities.len() {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the tasks were never spilled"
                );
                spilled.extend(store.drain_backlog(&activity_queue, 10).await?);
                tokio::task::yield_now().await;
            }
            store.persist_to_backlog(spilled).await?;
        }
        Path::ResetSuccessor => {
            let task = poll_workflow_task(&runtime, namespace_id).await?;
            complete(&runtime, task, Vec::new(), false).await?;
            let fork_event_id = store
                .read_history_to_end(run_key, 0)
                .await?
                .iter()
                .rev()
                .find(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. }))
                .map(|event| event.event_id)
                .ok_or_else(|| anyhow::anyhow!("a completed workflow task"))?;
            runtime
                .reset_workflow(
                    ExecutionRef {
                        namespace_id,
                        workflow_id: WorkflowId("delivery-paths".to_string()),
                        run_id: Some(new_state.run_id),
                    },
                    ResetRequest {
                        fork_event_id,
                        new_run_id: RunId::new(),
                        reapply_exclude_signal: false,
                        reapply_exclude_update: false,
                        post_reset_versioning_overrides: Vec::new(),
                        expected_current_run_key: None,
                        reason: "deliver from the copied history".to_string(),
                        request: operator_context("reset"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
        }
    }
    let remaining = activities.len() - started.len();
    started.extend(poll_started(&runtime, namespace_id, remaining).await?);

    let mut delivered = BTreeMap::new();
    for task in started {
        if matches!(path, Path::ResetSuccessor) {
            anyhow::ensure!(task.run_key != run_key, "delivered from the successor");
        }
        let event = scheduled_event(&store, &task).await?;
        delivered.insert(task.activity_id.clone(), (task, event));
    }
    Ok(delivered)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    // Feature: activity-inputs-from-history, Property 2: A delivered task carries
    // its scheduled event's input and header
    #[test]
    fn property_a_delivered_task_carries_its_scheduled_events_input_and_header(
        path in arb_path(),
        activities in prop::collection::vec(arb_scheduled(), 1..4),
    ) {
        let delivered = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(deliver(path, &activities))
            .unwrap();
        prop_assert_eq!(delivered.len(), activities.len());
        for (index, scheduled) in activities.iter().enumerate() {
            let (task, event) = &delivered[&format!("activity-{index}")];
            prop_assert_eq!(event, scheduled);
            prop_assert_eq!(&task.input, &scheduled.input);
            prop_assert_eq!(&task.header, &scheduled.header);
        }
    }
}

/// A step a run's activities take while pending or running.
#[derive(Clone, Debug)]
enum Step {
    Start,
    Heartbeat,
    Fail,
    Pause(usize),
    Unpause(usize),
    Reset(usize),
    UpdateOptions(usize),
}

fn arb_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        Just(Step::Start),
        Just(Step::Heartbeat),
        Just(Step::Fail),
        (0usize..3).prop_map(Step::Pause),
        (0usize..3).prop_map(Step::Unpause),
        (0usize..3).prop_map(Step::Reset),
        (0usize..3).prop_map(Step::UpdateOptions),
    ]
}

/// Whether `needle` appears anywhere in `haystack`.
fn holds(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Fail if the run's state, a dispatch row or a backlog entry of `queue` holds
/// any of `inputs`' bytes.
async fn assert_no_copies(
    store: &InMemoryStore,
    run_key: RunKey,
    queue: &QueueKey,
    inputs: &[Vec<u8>],
) -> Result<()> {
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        anyhow::bail!("the run is stored");
    };
    let mut stored = vec![("state", codec::encode_workflow_state(&state)?)];
    for row in store
        .list_all_dispatchable_activity_tasks(queue, 100)
        .await?
    {
        stored.push(("dispatch row", codec::encode(&row)?));
    }
    // Draining shows the spilled entries; they go back as they were.
    let entries = store.drain_backlog(queue, 100).await?;
    for entry in &entries {
        stored.push((
            "backlog entry",
            codec::encode_backlog_payload(&entry.payload, entry.priority.as_ref())?,
        ));
    }
    store.persist_to_backlog(entries).await?;
    for (what, bytes) in &stored {
        for input in inputs {
            anyhow::ensure!(!holds(bytes, input), "the {what} holds an activity's input");
        }
    }
    Ok(())
}

async fn take_steps(activities: &[Scheduled], steps: &[Step]) -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(
        store.clone(),
        BacklogConfig {
            activity_grace_window: tokio::time::Duration::ZERO,
            grace_scan_interval: tokio::time::Duration::from_millis(10),
            ..BacklogConfig::default()
        },
    );
    let namespace_id = NamespaceId::new();
    let commands = activities
        .iter()
        .enumerate()
        .map(|(index, scheduled)| schedule_generated(index, scheduled))
        .collect();
    let run_key = start_and_schedule(&runtime, namespace_id, "no-copies", commands).await?;
    let activity_queue = queue(namespace_id, ACTIVITY_QUEUE, TaskKind::Activity);
    let mut inputs = Vec::new();
    for scheduled in activities {
        inputs.extend(scheduled.input.0.iter().map(|payload| payload.data.clone()));
        if let Some(header) = &scheduled.header {
            inputs.extend(header.0.values().map(|payload| payload.data.clone()));
        }
    }
    assert_no_copies(&store, run_key, &activity_queue, &inputs).await?;

    let target =
        |index: usize| ActivityControlTarget::Id(format!("activity-{}", index % activities.len()));
    let mut running: Vec<ActivityTaskToken> = Vec::new();
    for (number, step) in steps.iter().enumerate() {
        let request = operator_context(&format!("step-{number}"));
        let now = OffsetDateTime::now_utc();
        // A step that doesn't apply, such as unpausing an activity that isn't
        // paused, is refused and changes nothing; the copies are checked anyway.
        match step {
            Step::Start => {
                if let Some(task) = runtime
                    .poll_activity_task(
                        activity_queue.clone(),
                        WorkerIdentity("worker".to_string()),
                        tokio::time::Duration::from_millis(50),
                    )
                    .await?
                {
                    running.push(task.token);
                }
            }
            Step::Heartbeat => {
                if let Some(token) = running.last() {
                    let _ = runtime
                        .record_activity_heartbeat(
                            token.clone(),
                            Some(Payloads(vec![Payload::new(b"progress".to_vec())])),
                            None,
                        )
                        .await;
                }
            }
            Step::Fail => {
                if let Some(token) = running.pop() {
                    let _ = runtime
                        .fail_activity_task(
                            token,
                            Payload::new(b"retryable".to_vec()),
                            None,
                            false,
                            None,
                            None,
                            request,
                        )
                        .await;
                }
            }
            Step::Pause(index) => {
                let _ = runtime
                    .pause_activities(
                        run_key,
                        PauseActivityRequest {
                            target: target(*index),
                            identity: "operator".to_string(),
                            reason: "pause".to_string(),
                            rule_id: None,
                            request,
                            now,
                        },
                    )
                    .await;
            }
            Step::Unpause(index) => {
                let _ = runtime
                    .unpause_activities(
                        run_key,
                        UnpauseActivitiesRequest {
                            target: target(*index),
                            reset_attempts: false,
                            reset_heartbeat: false,
                            jitter: None,
                            request,
                            now,
                        },
                    )
                    .await;
            }
            Step::Reset(index) => {
                let _ = runtime
                    .reset_activities(
                        run_key,
                        ResetActivitiesRequest {
                            target: target(*index),
                            reset_heartbeat: false,
                            keep_paused: false,
                            jitter: None,
                            restore_original_options: true,
                            request,
                            now,
                        },
                    )
                    .await;
            }
            Step::UpdateOptions(index) => {
                let _ = runtime
                    .update_activity_options(
                        run_key,
                        UpdateActivitiesOptionsRequest {
                            target: target(*index),
                            task_queue: FieldChange::Unchanged,
                            schedule_to_close_timeout: FieldChange::Unchanged,
                            schedule_to_start_timeout: FieldChange::Unchanged,
                            start_to_close_timeout: FieldChange::Unchanged,
                            heartbeat_timeout: FieldChange::Set(Some(Duration::seconds(30))),
                            retry_policy: ActivityRetryPolicyPatch::default(),
                            priority: ActivityPriorityPatch::default(),
                            restore_original_options: false,
                            request,
                            now,
                        },
                    )
                    .await;
            }
        }
        assert_no_copies(&store, run_key, &activity_queue, &inputs)
            .await
            .map_err(|error| error.context(format!("after step {number}: {step:?}")))?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Feature: activity-inputs-from-history, Property 1: No copies of an
    // activity's input or header
    #[test]
    fn property_no_copies_of_an_activitys_input_or_header(
        activities in prop::collection::vec(arb_scheduled(), 1..4),
        steps in prop::collection::vec(arb_step(), 0..10),
    ) {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(take_steps(&activities, &steps))
            .unwrap();
    }
}
