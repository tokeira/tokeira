//! `run-growth-limits` through the runtime's lane: a commit the store refuses
//! for a growth limit terminates the run in the same activation, and the
//! caller is answered with the breach.

use std::sync::Arc;

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, LoadedRun, SignalRequest, StartRequest, Transition,
    WorkflowCommand, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
    limits::{
        HISTORY_COUNT_LIMIT_ERROR, HISTORY_SIZE_LIMIT_ERROR, MUTABLE_STATE_SIZE_LIMIT_ERROR,
        RunLimit, RunLimitExceeded,
    },
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, ExecutionStatus, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, RetryPolicy, RunKey, SearchAttributes, ShardEpoch, TaskKind, TaskQueueName,
    WorkerIdentity, WorkflowId, WorkflowType,
};

fn start_request(namespace_id: NamespaceId, workflow_id: WorkflowId) -> StartRequest {
    let run_id = tokeira_types::RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
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
            request_id: RequestId("start".to_string()),
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

fn signal_request(request_id: &str) -> SignalRequest {
    SignalRequest {
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

/// With a history of exactly the count limit and a scheduled workflow task, a
/// signal succeeds, since v1.31.0 numbers it only when it finishes the write;
/// the next signal finds one event too many and terminates the run, as in
/// v1.31.0's `TestTerminateWorkflowCausedByHistoryCountLimit`
/// (`tests/sizelimit_test.go:40-224 @ v1.31.0`).
#[tokio::test]
async fn the_history_count_terminates_the_run_one_write_after_the_limit() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("count-limit".to_string());
    let started = runtime
        .start_workflow(start_request(namespace_id, workflow_id.clone()))
        .await?;
    let CommitResult::Applied { new_state } = started else {
        panic!("start applied");
    };
    let run_key = new_state.run_key;
    assert!(new_state.pending_workflow_task.is_some());

    // Seed the run at the limit: the events in between don't matter here.
    let limit = i64::try_from(HISTORY_COUNT_LIMIT_ERROR)?;
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        panic!("run stored");
    };
    let mut seeded = state.clone();
    seeded.transition_seq = state.transition_seq.next();
    seeded.last_event_id = limit;
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

    let execution = ExecutionRef {
        namespace_id,
        workflow_id,
        run_id: None,
    };
    runtime
        .signal_workflow(execution.clone(), signal_request("signal-1"))
        .await?;
    let error = runtime
        .signal_workflow(execution, signal_request("signal-2"))
        .await
        .expect_err("the second signal is refused");
    assert_eq!(
        error
            .downcast_ref::<RunLimitExceeded>()
            .map(|breach| breach.limit),
        Some(RunLimit::HistoryCount)
    );

    let LoadedRun::Existing(closed) = store.load_run(run_key).await? else {
        panic!("run stored");
    };
    assert_eq!(closed.status, ExecutionStatus::Terminated);
    let events = store.read_history(run_key, limit, 8).await?;
    let ids_and_kinds: Vec<(i64, &HistoryEventKind)> = events
        .iter()
        .map(|event| (event.event_id, &event.kind))
        .collect();
    assert_eq!(ids_and_kinds.len(), 2, "{ids_and_kinds:?}");
    assert!(matches!(
        ids_and_kinds[0],
        (id, HistoryEventKind::WorkflowExecutionSignaled { .. }) if id == limit + 1
    ));
    match ids_and_kinds[1] {
        (
            id,
            HistoryEventKind::WorkflowExecutionTerminated {
                reason,
                details,
                identity,
                ..
            },
        ) => {
            assert_eq!(id, limit + 2);
            assert_eq!(reason, "Workflow history count exceeds limit.");
            assert_eq!(details, &None);
            assert_eq!(identity, "history-service");
        }
        other => panic!("expected the termination, got {other:?}"),
    }
    Ok(())
}

// Feature: projection-accumulator, Property 8: extension growth follows the existing runtime breach policy.
#[tokio::test]
async fn projection_accumulator_growth_breach_terminates_through_the_lane() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("accumulator-growth".into());
    let CommitResult::Applied { new_state: state } = runtime
        .start_workflow(start_request(namespace_id, workflow_id.clone()))
        .await?
    else {
        panic!("start")
    };
    let mut next = state.clone();
    next.transition_seq = state.transition_seq.next();
    next.used_worker_deployment_versions =
        Some(vec!["v".repeat(MUTABLE_STATE_SIZE_LIMIT_ERROR + 1)]);
    store
        .commit_transition(
            state.run_key,
            Transition {
                expected_seq: state.transition_seq,
                next_state: next,
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
    let error = runtime
        .signal_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id,
                run_id: None,
            },
            signal_request("growth"),
        )
        .await
        .expect_err("state size refused");
    assert_eq!(
        error.downcast_ref::<RunLimitExceeded>().unwrap().limit,
        RunLimit::StateSize
    );
    let LoadedRun::Existing(closed) = store.load_run(state.run_key).await? else {
        panic!("run")
    };
    assert_eq!(closed.status, ExecutionStatus::Terminated);
    assert_eq!(
        closed.used_worker_deployment_versions.unwrap()[0].len(),
        MUTABLE_STATE_SIZE_LIMIT_ERROR + 1
    );
    let history = store
        .read_history(state.run_key, state.last_event_id, 10)
        .await?;
    assert!(history.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionTerminated { .. }
    )));
    assert!(!history.iter().any(|event| matches!(
        event.kind,
        HistoryEventKind::WorkflowExecutionSignaled { .. }
    )));
    Ok(())
}

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

fn schedule_activity(index: usize, input_size: usize) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".into(),
        task_queue: TaskQueueName(String::new()),
        input: Payloads(vec![Payload::new(vec![7; input_size])]),
        header: None,
        request_eager_execution: false,
        retry_policy: None,
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::seconds(10)),
        heartbeat_timeout: None,
        priority: None,
    }
}

/// A workflow task's completion whose history batch passes 4 MiB on its own
/// terminates the run: the completing task is force-closed and the run
/// terminated with the error as details
/// (`respondworkflowtaskcompleted/api.go:645-674 @ v1.31.0`).
#[tokio::test]
async fn a_completion_over_the_batch_limit_terminates_the_run() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(
            namespace_id,
            WorkflowId("batch-limit".to_string()),
        ))
        .await?
    else {
        panic!("start applied");
    };
    let run_key = new_state.run_key;
    let task = runtime
        .poll_workflow_task(
            QueueKey {
                namespace_id,
                task_queue: TaskQueueName("queue-a".to_string()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .expect("the first workflow task");
    let started_event_id = task.token.started_event_id;

    let error = runtime
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
            commands: (0..3)
                .map(|index| schedule_activity(index, 3 * 1024 * 1024 / 2))
                .collect(),
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
            now: OffsetDateTime::now_utc(),
            command_sizes: Vec::new(),
        })
        .await
        .expect_err("the completion is refused");
    let breach = error
        .downcast_ref::<RunLimitExceeded>()
        .expect("a growth limit breach")
        .clone();
    assert_eq!(breach.limit, RunLimit::TransactionSize);
    assert!(
        breach.message.starts_with("transaction size of ")
            && breach
                .message
                .ends_with(" bytes exceeds limit of 4194304 bytes"),
        "{}",
        breach.message
    );

    let events = store.read_history(run_key, started_event_id, 8).await?;
    assert_eq!(events.len(), 2, "{events:?}");
    match &events[0].kind {
        HistoryEventKind::WorkflowTaskFailed {
            failure_cause,
            identity,
            ..
        } => {
            assert_eq!(failure_cause, &WorkflowTaskFailedCause::ForceCloseCommand);
            assert_eq!(identity.0, "history-service");
        }
        other => panic!("expected the force-close, got {other:?}"),
    }
    match &events[1].kind {
        HistoryEventKind::WorkflowExecutionTerminated {
            reason,
            details,
            identity,
            ..
        } => {
            assert_eq!(reason, "Transaction size exceeds limit.");
            assert_eq!(identity, "history-service");
            let details = details.as_ref().expect("details");
            assert_eq!(details.0.len(), 1);
            assert_eq!(details.0[0].data, serde_json::to_vec(&breach.message)?);
        }
        other => panic!("expected the termination, got {other:?}"),
    }
    Ok(())
}

fn queue(namespace_id: NamespaceId, task_kind: TaskKind) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("queue-a".to_string()),
        task_kind,
        deployment: None,
        build_id: None,
    }
}

/// A run whose first workflow task scheduled one activity, `retried` with a
/// retry policy.
async fn run_with_activity(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    name: &str,
    retried: bool,
) -> Result<RunKey> {
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, WorkflowId(name.to_string())))
        .await?
    else {
        panic!("start applied");
    };
    let task = runtime
        .poll_workflow_task(
            queue(namespace_id, TaskKind::Workflow),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .expect("the first workflow task");
    let mut command = schedule_activity(0, 8);
    if retried && let WorkflowCommand::ScheduleActivity { retry_policy, .. } = &mut command {
        *retry_policy = Some(RetryPolicy {
            initial_interval: Duration::seconds(60),
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 3,
            non_retryable_error_types: Vec::new(),
        });
    }
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
            commands: vec![command],
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

/// Take the run's History Size past the 50 MiB limit with one batch written
/// straight to the store, which checks no limits on a commit without them.
async fn seed_history_size_over_the_limit(store: &InMemoryStore, run_key: RunKey) -> Result<()> {
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        panic!("run stored");
    };
    let mut next = state.clone();
    next.transition_seq = state.transition_seq.next();
    next.last_event_id = state.last_event_id + 1;
    let event = HistoryEvent {
        event_id: next.last_event_id,
        happened_at: OffsetDateTime::now_utc(),
        kind: HistoryEventKind::WorkflowExecutionSignaled {
            signal_name: "seed".to_string(),
            input: Payloads(vec![Payload::new(vec![7; HISTORY_SIZE_LIMIT_ERROR + 1])]),
            header: None,
            links: Vec::new(),
            request_id: "seed".to_string(),
            identity: None,
        },
    };
    store
        .commit_transition(
            run_key,
            Transition {
                expected_seq: state.transition_seq,
                next_state: next,
                history_events: vec![event].into(),
                event_principals: vec![None].into(),
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

async fn assert_terminated_for_history_size(store: &InMemoryStore, run_key: RunKey) -> Result<()> {
    let LoadedRun::Existing(state) = store.load_run(run_key).await? else {
        panic!("run stored");
    };
    assert_eq!(state.status, ExecutionStatus::Terminated);
    let events = store
        .read_history(run_key, state.last_event_id - 1, 1)
        .await?;
    match &events[0].kind {
        HistoryEventKind::WorkflowExecutionTerminated {
            reason, identity, ..
        } => {
            assert_eq!(reason, "Workflow history size exceeds limit.");
            assert_eq!(identity, "history-service");
        }
        other => panic!("expected the termination, got {other:?}"),
    }
    Ok(())
}

/// An activity start refused for a growth limit terminates the run through its
/// lane, and the poll goes on without the task, as v1.31.0's matching does
/// (`service/matching/matching_engine.go:982, 1060-1069 @ v1.31.0`).
#[tokio::test]
async fn an_activity_start_over_a_limit_terminates_the_run_and_the_poll_goes_on() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = run_with_activity(&runtime, namespace_id, "start-limit", false).await?;
    seed_history_size_over_the_limit(&store, run_key).await?;

    let task = runtime
        .poll_activity_task(
            queue(namespace_id, TaskKind::Activity),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?;
    assert!(task.is_none(), "the refused start hands out no task");
    assert_terminated_for_history_size(&store, run_key).await
}

/// A retry refused for a growth limit terminates the run through its lane, and
/// the worker's failure call is answered with the breach.
#[tokio::test]
async fn an_activity_retry_over_a_limit_terminates_the_run() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = run_with_activity(&runtime, namespace_id, "retry-limit", true).await?;
    let task = runtime
        .poll_activity_task(
            queue(namespace_id, TaskKind::Activity),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?
        .expect("the activity task");
    seed_history_size_over_the_limit(&store, run_key).await?;

    let error = runtime
        .fail_activity_task(
            task.token,
            tokeira_proto::conversions::common::failure_to_payload(
                &tokeira_proto::failure::Failure {
                    message: "boom".to_string(),
                    ..Default::default()
                },
            ),
            Some("Retryable".to_string()),
            false,
            None,
            Some(WorkerIdentity("worker".to_string())),
            RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        )
        .await
        .expect_err("the retry is refused");
    assert_eq!(
        error
            .downcast_ref::<RunLimitExceeded>()
            .map(|breach| breach.limit),
        Some(RunLimit::HistorySize)
    );
    assert_terminated_for_history_size(&store, run_key).await
}
