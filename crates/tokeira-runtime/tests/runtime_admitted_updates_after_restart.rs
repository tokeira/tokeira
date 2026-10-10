//! `admitted-updates-after-restart` through the runtime on the in-memory store.
//!
//! A restart is a new runtime over the same store: it holds none of the update
//! requests the old one held, as a node holds none after it restarts or takes
//! a run over. A run then keeps only the admitted updates it can deliver, so a
//! client's retry of a lost update is admitted anew and gets its outcome, no
//! empty speculative task follows a lost update, and the continue-as-new advice
//! leaves lost updates out. The updates a reset reapplied stay in flight.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::Result;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    LoadedRun, Reject, ResetRequest, StartRequest, Transition, UpdateProtocolBody, WorkflowCommand,
    WorkflowState, WorkflowTaskCompletedRequest,
    limits::{UpdateLimit, UpdateLimitExceeded},
};
use tokeira_runtime::{
    ActivityTimeoutScannerConfig, BacklogConfig, KernelRejected, LaneConfig, NexusCompletionDeps,
    NexusEndpointRegistry, NexusTimeoutScannerConfig, NoopNexusHttpClient, StartedWorkflowTask,
    TimerScannerConfig, TokeiraRuntime, UpdateLifecycleStage, UpdateOutcome, UpdateWaitPolicy,
    WorkflowActivation, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, RunId,
    RunKey, SearchAttributes, ShardEpoch, ShardId, TaskKind, TaskQueueName, WorkerIdentity,
    WorkflowId, WorkflowType,
};

const QUEUE: &str = "restart-queue";
const WORKFLOW: &str = "restarted";

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

/// The node after a restart: a new runtime over the same store that takes its
/// shard and recovers it, republishing the runs' pending workflow tasks.
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
        workflow_type: WorkflowType("restarted".to_string()),
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

fn execution(namespace_id: NamespaceId) -> ExecutionRef {
    ExecutionRef {
        namespace_id,
        workflow_id: WorkflowId(WORKFLOW.to_string()),
        run_id: None,
    }
}

async fn start(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<RunKey> {
    match runtime.start_workflow(start_request(namespace_id)).await? {
        CommitResult::Applied { new_state } => Ok(new_state.run_key),
        other => anyhow::bail!("start not applied: {other:?}"),
    }
}

/// One poll through the public poll path, answered `None` when no task arrives
/// within `wait_ms`. That path discards a task whose start the run refuses as
/// stale, as a worker's PollWorkflowTaskQueue does.
async fn poll(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    wait_ms: u64,
) -> Result<Option<StartedWorkflowTask>> {
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
            tokio::time::Duration::from_millis(wait_ms),
        )
        .await?;
    match activation {
        Some(WorkflowActivation::WorkflowTask(task)) => Ok(Some(task)),
        Some(WorkflowActivation::QueryTask(_)) => anyhow::bail!("no query was sent"),
        None => Ok(None),
    }
}

fn completion(
    task: StartedWorkflowTask,
    commands: Vec<WorkflowCommand>,
) -> WorkflowTaskCompletedRequest {
    WorkflowTaskCompletedRequest {
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

fn accept(update_id: &str) -> WorkflowCommand {
    WorkflowCommand::ProtocolMessage {
        message_id: format!("{update_id}/accept"),
        body: UpdateProtocolBody::Accepted {
            update_id: update_id.to_string(),
            update_name: "handler".to_string(),
            input: Payloads::default(),
            sequencing_event_id: 1,
        },
    }
}

fn result_of(update_id: &str) -> Payloads {
    Payloads(vec![Payload::new(update_id.as_bytes())])
}

fn complete(update_id: &str) -> WorkflowCommand {
    WorkflowCommand::ProtocolMessage {
        message_id: format!("{update_id}/complete"),
        body: UpdateProtocolBody::Completed {
            update_id: update_id.to_string(),
            result: result_of(update_id),
            failure: None,
        },
    }
}

/// Admit `update_id` and return once it is admitted.
async fn admit(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    update_id: &str,
) -> Result<()> {
    runtime
        .update_workflow(
            execution(namespace_id),
            update_id.to_string(),
            "handler".to_string(),
            Payloads::default(),
            context(update_id),
            Duration::milliseconds(200),
            UpdateWaitPolicy::Admitted,
            100,
        )
        .await?;
    Ok(())
}

/// Rewrite the run's stored state with `seed`, outside the lane, as a run that
/// reached it by ordinary traffic would hold it.
async fn seed(
    store: &InMemoryStore,
    run_key: RunKey,
    seed: impl FnOnce(&mut WorkflowState),
) -> Result<()> {
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        anyhow::bail!("run stored");
    };
    let mut seeded = state.clone();
    seeded.transition_seq = state.transition_seq.next();
    seed(&mut seeded);
    store
        .commit_transition(
            run_key,
            Transition {
                expected_seq: state.transition_seq,
                next_state: seeded,
                history_events: Default::default(),
                event_principals: Default::default(),
                request_dedupe_ops: Default::default(),
                activity_ops: Default::default(),
                timer_ops: Default::default(),
                dispatch_ops: Default::default(),
                events_numbered_at_close: 0,
                growth_limits: None,
            },
            ShardEpoch::ZERO,
        )
        .await?;
    Ok(())
}

async fn stored(store: &InMemoryStore, run_key: RunKey) -> Result<WorkflowState> {
    match store.load_run(run_key).await? {
        LoadedRun::Existing(state) => Ok(state),
        LoadedRun::Absent => anyhow::bail!("run stored"),
    }
}

fn update_refusal(error: &anyhow::Error) -> Option<UpdateLimit> {
    match error.downcast_ref::<KernelRejected>()? {
        KernelRejected(Reject::UpdateLimitExceeded(UpdateLimitExceeded { limit, .. })) => {
            Some(*limit)
        }
        _ => None,
    }
}

/// A worker that answers each workflow task by accepting and completing every
/// update it carries, until `done` is set. Returns how many tasks it was given.
async fn work_updates(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    done: &AtomicBool,
) -> Result<usize> {
    let mut tasks = 0;
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    while !done.load(Ordering::SeqCst) && Instant::now() < deadline {
        let Some(task) = poll(runtime, namespace_id, 100).await? else {
            continue;
        };
        tasks += 1;
        let updates = runtime.pending_update_transports(task.run_key, false);
        let commands = updates
            .iter()
            .flat_map(|update| [accept(&update.update_id), complete(&update.update_id)])
            .collect::<Vec<_>>();
        runtime
            .complete_workflow_task(completion(task, commands))
            .await?;
    }
    Ok(tasks)
}

/// Retry `update_id` until it completes, while a worker answers the run's
/// tasks; the retry's answer.
async fn retry_while_working(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    update_id: &str,
) -> Result<tokeira_runtime::UpdateLifecycleSnapshot> {
    let done = AtomicBool::new(false);
    let retry = async {
        let answer = runtime
            .update_workflow(
                execution(namespace_id),
                update_id.to_string(),
                "handler".to_string(),
                Payloads::default(),
                context(&format!("{update_id}-retry")),
                Duration::seconds(3),
                UpdateWaitPolicy::Completed,
                100,
            )
            .await;
        done.store(true, Ordering::SeqCst);
        answer
    };
    let (answer, worked) = tokio::join!(retry, work_updates(runtime, namespace_id, &done));
    worked?;
    answer
}

fn completed_with_its_result(answer: &tokeira_runtime::UpdateLifecycleSnapshot) -> bool {
    answer.stage == UpdateLifecycleStage::Completed
        && matches!(
            &answer.outcome,
            Some(UpdateOutcome::Completed { result, .. }) if *result == result_of(&answer.update_id)
        )
}

/// A client's retry of an update whose request a restart lost is admitted
/// anew, delivered, and answered with its outcome (bugfix 1.1, 2.2).
#[tokio::test]
async fn a_lost_updates_retry_gets_its_outcome() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    start(&before, namespace_id).await?;
    admit(&before, namespace_id, "lost").await?;
    drop(before);

    let after = restarted(store.clone()).await?;
    let answer = retry_while_working(&after, namespace_id, "lost").await?;
    assert!(completed_with_its_result(&answer), "{answer:?}");
    Ok(())
}

/// After a restart, the speculative task scheduled for a lost update is not
/// worked over and over: a worker polling for it gets none, or none after the
/// first (bugfix 1.2, 2.3). The forget drops the stored task when the poll
/// tries to start it, and the poll discards the refused start.
#[tokio::test]
async fn no_empty_speculative_task_follows_a_lost_update() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    start(&before, namespace_id).await?;
    let first = poll(&before, namespace_id, 200)
        .await?
        .expect("the first workflow task");
    before
        .complete_workflow_task(completion(first, Vec::new()))
        .await?;
    admit(&before, namespace_id, "lost").await?;
    drop(before);

    let after = restarted(store.clone()).await?;
    if let Some(stray) = poll(&after, namespace_id, 300).await? {
        assert!(
            after
                .pending_update_transports(stray.run_key, false)
                .is_empty(),
            "the restart lost the update's request"
        );
        after
            .complete_workflow_task(completion(stray, Vec::new()))
            .await?;
    }
    let next = poll(&after, namespace_id, 300).await?;
    assert!(
        next.is_none(),
        "a workflow task followed the empty one: {:?}",
        next.map(|task| task.token)
    );
    Ok(())
}

/// The continue-as-new advice counts in flight only the updates a run can
/// deliver, so lost updates don't bring it forward (bugfix 1.3, 2.5).
#[tokio::test]
async fn the_advice_leaves_out_lost_updates() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    let run_key = start(&before, namespace_id).await?;
    // 1,795 completed and ten in flight would reach v1.31.0's 1,800 (90% of
    // the 2,000 total).
    seed(&store, run_key, |state| {
        state.completed_update_count = 1_795
    })
    .await?;
    for index in 0..10 {
        admit(&before, namespace_id, &format!("lost-{index}")).await?;
    }
    drop(before);

    let after = restarted(store.clone()).await?;
    let task = poll(&after, namespace_id, 300)
        .await?
        .expect("the first workflow task survives the restart");
    assert!(
        !task.advice.suggest_continue_as_new,
        "the advice counted lost updates: {:?}",
        task.advice
    );
    Ok(())
}

/// The updates a reset reapplied count in flight, though the runtime holds no
/// request for them: with ten reapplied, an eleventh is refused (bugfix 1.4,
/// 2.5, 2.6).
#[tokio::test]
async fn a_resets_reapplied_updates_count_in_flight() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let runtime = runtime(store.clone());
    let run_key = start(&runtime, namespace_id).await?;
    let first = poll(&runtime, namespace_id, 200)
        .await?
        .expect("the first workflow task");
    runtime
        .complete_workflow_task(completion(first, Vec::new()))
        .await?;
    let ids = (0..10)
        .map(|index| format!("reapplied-{index}"))
        .collect::<Vec<_>>();
    for id in &ids {
        admit(&runtime, namespace_id, id).await?;
    }
    let task = poll(&runtime, namespace_id, 200)
        .await?
        .expect("the task that carries the updates");
    let delivered = runtime.pending_update_transports(task.run_key, false);
    assert_eq!(delivered.len(), ids.len());
    runtime
        .complete_workflow_task(completion(task, ids.iter().map(|id| accept(id)).collect()))
        .await?;

    let base = stored(&store, run_key).await?;
    let new_run_id = RunId::new();
    runtime
        .reset_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId(WORKFLOW.to_string()),
                run_id: Some(base.run_id),
            },
            ResetRequest {
                // The first task's WorkflowTaskCompleted, before every acceptance.
                fork_event_id: 4,
                new_run_id,
                reapply_exclude_signal: false,
                reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: None,
                reason: "reapply the accepted updates".to_string(),
                request: context("reset"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    let successor = stored(
        &store,
        RunKey::derive(namespace_id, &WorkflowId(WORKFLOW.to_string()), new_run_id),
    )
    .await?;
    assert_eq!(successor.admitted_updates.len(), ids.len());

    let refused = admit(&runtime, namespace_id, "eleventh")
        .await
        .expect_err("ten reapplied updates fill the in-flight limit");
    assert_eq!(update_refusal(&refused), Some(UpdateLimit::InFlight));
    Ok(())
}

/// A lost update's retry is admitted under the update limits: once the updates
/// admitted after the restart fill the in-flight limit, it is refused (bugfix
/// 2.2, 2.5).
#[tokio::test]
async fn a_lost_updates_retry_meets_the_update_limits() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    start(&before, namespace_id).await?;
    admit(&before, namespace_id, "lost").await?;
    drop(before);

    let after = restarted(store.clone()).await?;
    // The first admission forgets the lost update, so ten new ones fit.
    for index in 0..10 {
        admit(&after, namespace_id, &format!("held-{index}")).await?;
    }
    let refused = admit(&after, namespace_id, "lost")
        .await
        .expect_err("the held updates fill the in-flight limit");
    assert_eq!(update_refusal(&refused), Some(UpdateLimit::InFlight));
    Ok(())
}

/// The updates a reset reapplied survive a restart: they still count in
/// flight, and a client's update with one of their ids joins it rather than
/// admitting another (bugfix 2.6, 3.3).
#[tokio::test]
async fn a_resets_reapplied_updates_survive_a_restart() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let namespace_id = NamespaceId::new();
    let before = runtime(store.clone());
    let run_key = start(&before, namespace_id).await?;
    let first = poll(&before, namespace_id, 200)
        .await?
        .expect("the first workflow task");
    before
        .complete_workflow_task(completion(first, Vec::new()))
        .await?;
    let ids = (0..10)
        .map(|index| format!("reapplied-{index}"))
        .collect::<Vec<_>>();
    for id in &ids {
        admit(&before, namespace_id, id).await?;
    }
    let task = poll(&before, namespace_id, 200)
        .await?
        .expect("the task that carries the updates");
    assert_eq!(
        before.pending_update_transports(task.run_key, false).len(),
        ids.len()
    );
    before
        .complete_workflow_task(completion(task, ids.iter().map(|id| accept(id)).collect()))
        .await?;
    let base = stored(&store, run_key).await?;
    let new_run_id = RunId::new();
    before
        .reset_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: WorkflowId(WORKFLOW.to_string()),
                run_id: Some(base.run_id),
            },
            ResetRequest {
                fork_event_id: 4,
                new_run_id,
                reapply_exclude_signal: false,
                reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: None,
                reason: "reapply the accepted updates".to_string(),
                request: context("reset"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    drop(before);

    let after = restarted(store.clone()).await?;
    let joined = after
        .update_workflow(
            execution(namespace_id),
            ids[0].clone(),
            "handler".to_string(),
            Payloads::default(),
            context("rejoin"),
            Duration::milliseconds(200),
            UpdateWaitPolicy::Admitted,
            100,
        )
        .await?;
    assert_eq!(joined.stage, UpdateLifecycleStage::Admitted);
    let refused = admit(&after, namespace_id, "eleventh")
        .await
        .expect_err("the reapplied updates still fill the in-flight limit");
    assert_eq!(update_refusal(&refused), Some(UpdateLimit::InFlight));
    let successor = stored(
        &store,
        RunKey::derive(namespace_id, &WorkflowId(WORKFLOW.to_string()), new_run_id),
    )
    .await?;
    assert_eq!(successor.history_admitted_updates.len(), ids.len());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    // Feature: admitted-updates-after-restart, Property 2: A lost update's retry is a new update
    #[test]
    fn a_lost_updates_retry_is_a_new_update(
        lost in 1usize..4,
        held in 0usize..4,
        first_task_done in any::<bool>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = Arc::new(InMemoryStore::default());
            let namespace_id = NamespaceId::new();
            let before = runtime(store.clone());
            start(&before, namespace_id).await.unwrap();
            // The lost updates ride the run's first task, or get a speculative
            // task of their own once it is done.
            if first_task_done {
                let first = poll(&before, namespace_id, 200).await.unwrap().unwrap();
                before.complete_workflow_task(completion(first, Vec::new())).await.unwrap();
            }
            for index in 0..lost {
                admit(&before, namespace_id, &format!("lost-{index}")).await.unwrap();
            }
            drop(before);

            let after = restarted(store.clone()).await.unwrap();
            for index in 0..held {
                admit(&after, namespace_id, &format!("held-{index}")).await.unwrap();
            }
            for index in 0..lost {
                let answer = retry_while_working(&after, namespace_id, &format!("lost-{index}"))
                    .await
                    .unwrap();
                assert!(completed_with_its_result(&answer), "{answer:?}");
            }
        });
    }
}
