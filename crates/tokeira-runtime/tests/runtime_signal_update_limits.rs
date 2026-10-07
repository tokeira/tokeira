//! `signal-update-limits` through the runtime on the in-memory store.
//!
//! A refused signal is answered as Temporal v1.31.0 answers it: a repeat of a
//! request the run already applied succeeds, whether the run is closed,
//! closing or at the signal limit, except SignalWithStart's, whose count and
//! closing come first. A signal to another workflow resolves its sender with
//! v1.31.0's cause. Updates count in flight only while the runtime holds their
//! requests, and the total limit also refuses a worker's re-admission of an
//! update the run doesn't hold.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::{sync::Arc, time::Instant};

use anyhow::Result;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, LoadedRun, Reject, SignalRequest, SignalWithStartRequest,
    StartRequest, TerminateRequest, Transition, UpdateProtocolBody, WorkflowCommand, WorkflowState,
    WorkflowTaskCompletedRequest,
    limits::{MAXIMUM_SIGNALS_PER_EXECUTION, UpdateLimit, UpdateLimitExceeded},
};
use tokeira_runtime::{
    BacklogConfig, KernelRejected, LaneConfig, SignalWithStartResult, StartedWorkflowTask,
    TimerScannerConfig, TokeiraRuntime, UpdateWaitPolicy, WorkflowClosing,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payloads, QueueKey, RequestContext, RequestId, RunId, RunKey,
    SearchAttributes, ShardEpoch, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId,
    WorkflowType,
};

const QUEUE: &str = "limits-queue";

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

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.to_string()),
        caller_identity: Some("tester".to_string()),
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn start_request(namespace_id: NamespaceId, workflow_id: &str, task_queue: &str) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::new(),
        namespace_id,
        workflow_id: WorkflowId(workflow_id.to_string()),
        run_id,
        workflow_type: WorkflowType("limits".to_string()),
        task_queue: TaskQueueName(task_queue.to_string()),
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
        request: context(&format!("start-{workflow_id}")),
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

/// A signal-with-start to `workflow_id` that signals a running run.
fn signal_with_start(
    namespace_id: NamespaceId,
    workflow_id: &str,
    request_id: &str,
) -> SignalWithStartRequest {
    let start = start_request(namespace_id, workflow_id, QUEUE);
    SignalWithStartRequest {
        advice_policy: start.advice_policy,
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
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: start.workflow_task_timeout,
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::UseExisting,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        header: None,
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        user_metadata: None,
        links: Vec::new(),
        priority: None,
        cron_schedule: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: start.first_execution_run_id,
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: start.original_execution_run_id,
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: context(request_id),
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        signal_name: "sig".to_string(),
        signal_input: Payloads::default(),
    }
}

fn signal(request_id: &str) -> SignalRequest {
    SignalRequest {
        signal_name: "sig".to_string(),
        input: Payloads::default(),
        header: None,
        links: Vec::new(),
        request: context(request_id),
        now: OffsetDateTime::now_utc(),
    }
}

fn execution(namespace_id: NamespaceId, workflow_id: &str) -> ExecutionRef {
    ExecutionRef {
        namespace_id,
        workflow_id: WorkflowId(workflow_id.to_string()),
        run_id: None,
    }
}

fn queue(namespace_id: NamespaceId, task_queue: &str) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(task_queue.to_string()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

async fn start(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: &str,
    task_queue: &str,
) -> Result<RunKey> {
    Ok(start_run(runtime, namespace_id, workflow_id, task_queue)
        .await?
        .0)
}

async fn start_run(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: &str,
    task_queue: &str,
) -> Result<(RunKey, RunId)> {
    match runtime
        .start_workflow(start_request(namespace_id, workflow_id, task_queue))
        .await?
    {
        CommitResult::Applied { new_state } => Ok((new_state.run_key, new_state.run_id)),
        other => anyhow::bail!("start not applied: {other:?}"),
    }
}

async fn poll(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    task_queue: &str,
) -> Result<StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            queue(namespace_id, task_queue),
            WorkerIdentity("worker".to_string()),
            tokio::time::Duration::from_millis(200),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected a workflow task"))
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
        held_updates: 0,
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: OffsetDateTime::now_utc(),
        command_sizes: Vec::new(),
    }
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

async fn terminate(runtime: &TokeiraRuntime<InMemoryStore>, execution: ExecutionRef) -> Result<()> {
    runtime
        .terminate_workflow(
            execution,
            TerminateRequest {
                reason: "done".to_string(),
                details: None,
                identity: "tester".to_string(),
                links: Vec::new(),
                request: context("terminate"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    Ok(())
}

fn rejected(error: &anyhow::Error) -> Option<&Reject> {
    error
        .downcast_ref::<KernelRejected>()
        .map(|KernelRejected(reject)| reject)
}

fn update_refusal(error: &anyhow::Error) -> Option<UpdateLimit> {
    match rejected(error)? {
        Reject::UpdateLimitExceeded(UpdateLimitExceeded { limit, .. }) => Some(*limit),
        _ => None,
    }
}

async fn wait_for_history(
    store: &InMemoryStore,
    run_key: RunKey,
    predicate: impl Fn(&[HistoryEvent]) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the history condition: {history:?}");
        }
        tokio::task::yield_now().await;
    }
}

/// What a client's signal gets, as the property models it.
#[derive(Debug, PartialEq)]
enum Answer {
    Recorded,
    Duplicate,
    LimitRefused,
    Closed,
}

fn answer(result: Result<CommitResult>) -> Answer {
    match result {
        Ok(CommitResult::Applied { .. }) => Answer::Recorded,
        Ok(CommitResult::Duplicate) => Answer::Duplicate,
        Ok(other) => panic!("unexpected commit result {other:?}"),
        Err(error) => match rejected(&error) {
            Some(Reject::SignalLimitExceeded) => Answer::LimitRefused,
            Some(Reject::RunClosed(_)) => Answer::Closed,
            _ => panic!("unexpected refusal {error:?}"),
        },
    }
}

fn sws_answer(result: Result<SignalWithStartResult>) -> Answer {
    match result {
        Ok(SignalWithStartResult::Signaled { .. }) => Answer::Recorded,
        Ok(other) => panic!("unexpected signal-with-start result {other:?}"),
        Err(error) => match rejected(&error) {
            Some(Reject::SignalLimitExceeded) => Answer::LimitRefused,
            _ => panic!("unexpected refusal {error:?}"),
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, .. ProptestConfig::default() })]

    // Feature: signal-update-limits, Property 2: A client's signal is answered as v1.31.0 answers it
    #[test]
    fn a_clients_signal_is_answered_as_v1_31_0_answers_it(
        at_limit in any::<bool>(),
        repeat in any::<bool>(),
        with_start in any::<bool>(),
        closed in any::<bool>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = Arc::new(InMemoryStore::default());
            let runtime = runtime(store.clone());
            let namespace_id = NamespaceId::new();
            let (run_key, run_id) =
                start_run(&runtime, namespace_id, "signals", QUEUE).await.unwrap();
            // Named by run id, as the edge names a closed run it resolved.
            let target = ExecutionRef {
                run_id: Some(run_id),
                ..execution(namespace_id, "signals")
            };
            // The repeat's first delivery, below the limit.
            let first = runtime.signal_workflow(target.clone(), signal("repeated")).await;
            assert_eq!(answer(first), Answer::Recorded);
            let count = if at_limit {
                MAXIMUM_SIGNALS_PER_EXECUTION
            } else {
                MAXIMUM_SIGNALS_PER_EXECUTION - 1
            };
            seed(&store, run_key, |state| state.signal_count = count).await.unwrap();
            if closed {
                terminate(&runtime, target.clone()).await.unwrap();
            }
            let request_id = if repeat { "repeated" } else { "fresh" };
            // SignalWithStart to a closed run starts a new one; only its
            // signal to a running run is in question here.
            let with_start = with_start && !closed;
            let got = if with_start {
                sws_answer(
                    runtime
                        .signal_with_start_workflow(signal_with_start(namespace_id, "signals", request_id))
                        .await,
                )
            } else {
                answer(runtime.signal_workflow(target, signal(request_id)).await)
            };
            let expected = match (closed, repeat, with_start, at_limit) {
                // SignalWorkflowExecution checks the request id first.
                (_, true, false, _) => Answer::Duplicate,
                (true, false, _, _) => Answer::Closed,
                // SignalWithStart checks the count first.
                (false, _, _, true) => Answer::LimitRefused,
                // Below the limit a SignalWithStart repeat is the store's
                // duplicate, which signal-with-start reports as signaled.
                (false, _, true, false) => Answer::Recorded,
                (false, false, false, false) => Answer::Recorded,
                (true, true, true, _) => unreachable!("with_start is false for a closed run"),
            };
            assert_eq!(got, expected);
        });
    }

    // Feature: signal-update-limits, Property 7: A signal to another workflow resolves as v1.31.0 resolves it
    #[test]
    fn a_signal_to_another_workflow_resolves_as_v1_31_0_resolves_it(
        target in 0u8..5,
        already_applied in any::<bool>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let store = Arc::new(InMemoryStore::default());
            let runtime = runtime(store.clone());
            let namespace_id = NamespaceId::new();
            let sender = start(&runtime, namespace_id, "sender", "sender-queue").await.unwrap();
            // 0: open; 1: at the limit; 2: closed; 3: missing; 4: the sender itself.
            let target_id = match target {
                3 => "missing",
                4 => "sender",
                _ => "target",
            };
            let target_run = if target < 3 {
                Some(start(&runtime, namespace_id, "target", QUEUE).await.unwrap())
            } else {
                None
            };
            // The signal's initiated event follows the sender's first task:
            // Started(1), Scheduled(2), Started(3), Completed(4), Initiated(5).
            let initiated_event_id = 5;
            let delivery_request_id = format!("ext-signal-{sender:?}-{initiated_event_id}");
            let applied = already_applied && target_run.is_some();
            if let Some(target_run) = target_run {
                if applied {
                    // An earlier delivery of this very signal reached the target.
                    let result = runtime
                        .signal_workflow(execution(namespace_id, "target"), signal(&delivery_request_id))
                        .await;
                    assert_eq!(answer(result), Answer::Recorded);
                }
                if target == 1 {
                    seed(&store, target_run, |state| {
                        state.signal_count = MAXIMUM_SIGNALS_PER_EXECUTION;
                    })
                    .await
                    .unwrap();
                }
                if target == 2 {
                    terminate(&runtime, execution(namespace_id, "target")).await.unwrap();
                }
            }
            let task = poll(&runtime, namespace_id, "sender-queue").await.unwrap();
            runtime
                .complete_workflow_task(completion(
                    task,
                    vec![WorkflowCommand::SignalExternalWorkflowExecution {
                        target_namespace_id: namespace_id,
                        target_namespace: None,
                        target_workflow_id: WorkflowId(target_id.to_string()),
                        target_run_id: None,
                        signal_name: "poke".to_string(),
                        input: Payloads::default(),
                        header: None,
                        control: String::new(),
                    }],
                ))
                .await
                .unwrap();
            let expected_failure = match target {
                0 => None,
                1 if applied => None,
                1 => Some("SIGNAL_COUNT_LIMIT_EXCEEDED"),
                2 if applied => None,
                _ => Some("EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND"),
            };
            wait_for_history(&store, sender, |history| {
                history.iter().any(|event| match (&event.kind, expected_failure) {
                    (HistoryEventKind::ExternalWorkflowExecutionSignaled { initiated_event_id: id, .. }, None) => {
                        *id == initiated_event_id
                    }
                    (
                        HistoryEventKind::SignalExternalWorkflowExecutionFailed { initiated_event_id: id, cause, .. },
                        Some(expected),
                    ) => *id == initiated_event_id && cause == expected,
                    _ => false,
                })
            })
            .await
            .unwrap();
        });
    }
}

/// A run whose close bounced off a buffered signal is closing while its retry
/// task is started. A repeat of an applied signal still succeeds, a new one is
/// refused as closing, and at the limit the count's refusal comes first; a
/// SignalWithStart repeat is refused as closing (`signalworkflow/api.go:40-66`;
/// `signal_workflow_util.go:53-70`; `signal_with_start_workflow.go:273-300 @
/// v1.31.0`).
#[tokio::test]
async fn a_closing_run_answers_repeats_before_closing_and_the_count_before_both() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id, "closing", QUEUE).await?;
    let target = execution(namespace_id, "closing");
    let task = poll(&runtime, namespace_id, QUEUE).await?;
    runtime
        .signal_workflow(target.clone(), signal("buffered"))
        .await?;
    runtime
        .complete_workflow_task(completion(
            task,
            vec![WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            }],
        ))
        .await
        .expect_err("the close bounces off the buffered signal");
    let _retry = poll(&runtime, namespace_id, QUEUE).await?;

    assert_eq!(
        answer(
            runtime
                .signal_workflow(target.clone(), signal("buffered"))
                .await
        ),
        Answer::Duplicate
    );
    let closing = runtime
        .signal_workflow(target.clone(), signal("fresh"))
        .await
        .expect_err("a new signal is refused while closing");
    assert!(
        closing.downcast_ref::<WorkflowClosing>().is_some(),
        "{closing:?}"
    );
    let repeat_with_start = runtime
        .signal_with_start_workflow(signal_with_start(namespace_id, "closing", "buffered"))
        .await
        .expect_err("a signal-with-start repeat is refused while closing");
    assert!(
        repeat_with_start
            .downcast_ref::<WorkflowClosing>()
            .is_some(),
        "{repeat_with_start:?}"
    );

    seed(&store, run_key, |state| {
        state.signal_count = MAXIMUM_SIGNALS_PER_EXECUTION;
    })
    .await?;
    assert_eq!(
        answer(
            runtime
                .signal_workflow(target.clone(), signal("fresh"))
                .await
        ),
        Answer::LimitRefused
    );
    assert_eq!(
        answer(runtime.signal_workflow(target, signal("buffered")).await),
        Answer::Duplicate
    );
    Ok(())
}

async fn admit(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    update_id: &str,
    request_bytes: u64,
) -> Result<()> {
    runtime
        .update_workflow(
            execution(namespace_id, "updates"),
            update_id.to_string(),
            "handler".to_string(),
            Payloads::default(),
            context(update_id),
            Duration::milliseconds(50),
            UpdateWaitPolicy::Admitted,
            request_bytes,
        )
        .await?;
    Ok(())
}

/// Ten admitted updates fill the in-flight limit. After a restart has lost
/// their requests they no longer count, as v1.31.0 forgets admitted updates on
/// reload, so the run takes a new update (criterion 2.9).
#[tokio::test]
async fn updates_whose_requests_a_restart_lost_free_their_slots() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let first = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&first, namespace_id, "updates", QUEUE).await?;
    for index in 0..10 {
        admit(&first, namespace_id, &format!("update-{index}"), 100).await?;
    }
    let refused = admit(&first, namespace_id, "update-10", 100)
        .await
        .expect_err("an eleventh update is refused");
    assert_eq!(update_refusal(&refused), Some(UpdateLimit::InFlight));
    assert!(
        refused
            .to_string()
            .contains("limit on number of concurrent in-flight updates has been reached (10)"),
        "{refused}"
    );

    // A new runtime over the same store holds none of the ten requests.
    drop(first);
    let restarted = runtime(store.clone());
    admit(&restarted, namespace_id, "update-after-restart", 100).await?;
    Ok(())
}

/// The held updates' requests, with a new one, reach 20 MiB at the fourth of
/// four 5 MiB requests, and one byte less fits. An accepted update's request no
/// longer counts (criterion 2.11).
#[tokio::test]
async fn the_in_flight_payload_limit_counts_held_requests() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&runtime, namespace_id, "updates", QUEUE).await?;
    admit(&runtime, namespace_id, "accepted", 100).await?;
    let task = poll(&runtime, namespace_id, QUEUE).await?;
    runtime
        .complete_workflow_task(completion(
            task,
            vec![WorkflowCommand::ProtocolMessage {
                message_id: "accepted/accept".to_string(),
                body: UpdateProtocolBody::Accepted {
                    update_id: "accepted".to_string(),
                    update_name: "handler".to_string(),
                    input: Payloads::default(),
                    sequencing_event_id: 1,
                },
            }],
        ))
        .await?;
    let five_mib = 5 * 1024 * 1024;
    for index in 0..3 {
        admit(&runtime, namespace_id, &format!("update-{index}"), five_mib).await?;
    }
    let refused = admit(&runtime, namespace_id, "update-3", five_mib)
        .await
        .expect_err("the fourth reaches the limit");
    assert_eq!(
        update_refusal(&refused),
        Some(UpdateLimit::InFlightPayloads)
    );
    admit(&runtime, namespace_id, "update-3", five_mib - 1).await?;
    Ok(())
}

/// A run with 1,999 completed updates takes one more, then refuses the next for
/// the total, and refuses a worker's acceptance of an update it doesn't hold,
/// recording nothing (criteria 2.10, 2.13).
#[tokio::test]
async fn the_total_limit_refuses_admission_and_resurrection() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id, "updates", QUEUE).await?;
    seed(&store, run_key, |state| {
        state.completed_update_count = 1_999
    })
    .await?;
    admit(&runtime, namespace_id, "last", 100).await?;
    let refused = admit(&runtime, namespace_id, "over", 100)
        .await
        .expect_err("the total is reached");
    assert_eq!(update_refusal(&refused), Some(UpdateLimit::Total));

    let task = poll(&runtime, namespace_id, QUEUE).await?;
    let before = store.read_history(run_key, 0, 256).await?;
    let error = runtime
        .complete_workflow_task(completion(
            task,
            vec![WorkflowCommand::ProtocolMessage {
                message_id: "ghost/accept".to_string(),
                body: UpdateProtocolBody::Accepted {
                    update_id: "ghost".to_string(),
                    update_name: "handler".to_string(),
                    input: Payloads::default(),
                    sequencing_event_id: 1,
                },
            }],
        ))
        .await
        .expect_err("re-admitting an update at the total is refused");
    assert_eq!(update_refusal(&error), Some(UpdateLimit::Total));
    assert_eq!(store.read_history(run_key, 0, 256).await?, before);
    Ok(())
}
