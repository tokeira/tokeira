//! `update-refusals` through the runtime on the in-memory store.
//!
//! An update request to a running workflow is refused, before its id is looked
//! up, when the workflow is paused, when its workflow task keeps failing, and
//! when it is closing, in that order (`updateworkflow/api.go:118-155 @
//! v1.31.0`). A workflow task's completion that carries an acceptance, a
//! rejection or a response for an update that has completed fails the task
//! with an invalid state transition (`update/update.go:470, 562, 609, 648-656;
//! workflow_task_completed_handler.go:383-386 @ v1.31.0`).
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use anyhow::Result;
use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEventKind, PauseWorkflowRequest, SignalRequest, StartRequest, UpdateProtocolBody,
    WorkflowCommand, WorkflowTaskCompletedRequest, WorkflowTaskFailedCause,
};
use tokeira_runtime::{
    BacklogConfig, InvalidWorkflowCommand, KernelRejected, LaneConfig, StartedWorkflowTask,
    TimerScannerConfig, TokeiraRuntime, UpdateLifecycleSnapshot, UpdatePaused, UpdateWaitPolicy,
    WorkflowClosing, WorkflowNotReady, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, RunId,
    RunKey, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};

const QUEUE: &str = "refusals-queue";
const WORKFLOW_ID: &str = "refusals";
const UPDATE_ID: &str = "update-1";
const UPDATE_NAME: &str = "handler";

/// v1.31.0's answer to an update request for a paused workflow
/// (`updateworkflow/api.go:128-130 @ v1.31.0`).
const PAUSED: &str = "Workflow is paused. Cannot update the workflow.";

/// v1.31.0's answer to an update request for a workflow whose workflow task
/// keeps failing (`updateworkflow/api.go:132-143 @ v1.31.0`).
const FAILING: &str =
    "Unable to perform workflow execution update due to Workflow Task in failed state.";

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

fn start_request(namespace_id: NamespaceId) -> StartRequest {
    let run_id = RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::new(),
        namespace_id,
        workflow_id: WorkflowId(WORKFLOW_ID.to_string()),
        run_id,
        workflow_type: WorkflowType("refusals".to_string()),
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
        workflow_id: WorkflowId(WORKFLOW_ID.to_string()),
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

async fn poll(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            QueueKey {
                namespace_id,
                task_queue: TaskQueueName(QUEUE.to_string()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
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
        request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
        now: OffsetDateTime::now_utc(),
        command_sizes: Vec::new(),
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

fn payloads(value: &str) -> Payloads {
    Payloads(vec![Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
        external_payloads: Vec::new(),
    }])
}

/// An update request that returns once the update is admitted.
async fn request_update(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    request_id: &str,
) -> Result<UpdateLifecycleSnapshot> {
    runtime
        .update_workflow(
            execution(namespace_id),
            UPDATE_ID.to_string(),
            UPDATE_NAME.to_string(),
            payloads("input"),
            context(request_id),
            Duration::milliseconds(50),
            UpdateWaitPolicy::Admitted,
            0,
        )
        .await
}

/// Fail the run's workflow task `times` times, so the next task's attempt is
/// `times + 1`.
async fn fail_tasks(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    times: usize,
) -> Result<()> {
    for _ in 0..times {
        let task = poll(runtime, namespace_id).await?;
        runtime
            .fail_workflow_task(
                task.token,
                WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
                None,
                WorkerIdentity("worker".to_string()),
                context("fail"),
                OffsetDateTime::now_utc(),
            )
            .await?;
    }
    Ok(())
}

async fn pause(runtime: &TokeiraRuntime<InMemoryStore>, namespace_id: NamespaceId) -> Result<()> {
    runtime
        .pause_workflow(
            execution(namespace_id),
            PauseWorkflowRequest {
                identity: "tester".to_string(),
                reason: "pause".to_string(),
                request: context("pause"),
                now: OffsetDateTime::now_utc(),
            },
        )
        .await?;
    Ok(())
}

/// A workflow task whose workflow is failing: its two earlier tasks failed, so
/// its attempt is 3. An update request is refused with `WorkflowNotReady`.
#[tokio::test]
async fn a_failing_workflow_refuses_an_update() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&runtime, namespace_id).await?;
    fail_tasks(&runtime, namespace_id, 2).await?;
    match request_update(&runtime, namespace_id, "update").await {
        Err(error) => {
            assert_eq!(
                error
                    .downcast_ref::<WorkflowNotReady>()
                    .map(|not_ready| not_ready.message),
                Some(FAILING),
                "{error:#}"
            );
        }
        Ok(snapshot) => panic!("the update was taken at stage {:?}", snapshot.stage),
    }
    Ok(())
}

/// A workflow whose close bounced off a buffered signal is closing while its
/// retry task is started. An update request is refused as closing.
#[tokio::test]
async fn a_closing_workflow_refuses_an_update() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&runtime, namespace_id).await?;
    let task = poll(&runtime, namespace_id).await?;
    runtime
        .signal_workflow(execution(namespace_id), signal("buffered"))
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
    let _retry = poll(&runtime, namespace_id).await?;
    match request_update(&runtime, namespace_id, "update").await {
        Err(error) => assert!(
            error.downcast_ref::<WorkflowClosing>().is_some(),
            "{error:#}"
        ),
        Ok(snapshot) => panic!("the update was taken at stage {:?}", snapshot.stage),
    }
    Ok(())
}

/// An update request to a paused workflow is refused with v1.31.0's answer.
#[tokio::test]
async fn a_paused_workflow_refuses_an_update_with_its_answer() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&runtime, namespace_id).await?;
    pause(&runtime, namespace_id).await?;
    match request_update(&runtime, namespace_id, "update").await {
        Err(error) => assert_eq!(error.to_string(), PAUSED, "{error:#}"),
        Ok(snapshot) => panic!("the update was taken at stage {:?}", snapshot.stage),
    }
    Ok(())
}

/// An update is admitted, then the workflow is paused. A request repeating the
/// update's id is refused as paused, as a new id is, rather than attached to
/// the update.
#[tokio::test]
async fn a_repeat_of_an_admitted_update_is_refused_while_paused() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    start(&runtime, namespace_id).await?;
    request_update(&runtime, namespace_id, "first").await?;
    pause(&runtime, namespace_id).await?;
    match request_update(&runtime, namespace_id, "repeat").await {
        Err(error) => assert_eq!(error.to_string(), PAUSED, "{error:#}"),
        Ok(snapshot) => panic!("the repeat attached at stage {:?}", snapshot.stage),
    }
    Ok(())
}

/// Admit and complete the update in one workflow task, then signal the run so
/// that it has a new task, and return that task started.
async fn completed_update(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
) -> Result<StartedWorkflowTask> {
    let first = poll(runtime, namespace_id).await?;
    runtime
        .complete_workflow_task(completion(first, Vec::new()))
        .await?;
    request_update(runtime, namespace_id, "update").await?;
    let task = poll(runtime, namespace_id).await?;
    runtime
        .complete_workflow_task(completion(
            task,
            vec![
                WorkflowCommand::ProtocolMessage {
                    message_id: "accept".to_string(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: UPDATE_ID.to_string(),
                        update_name: UPDATE_NAME.to_string(),
                        input: payloads("input"),
                        sequencing_event_id: 1,
                    },
                },
                WorkflowCommand::ProtocolMessage {
                    message_id: "respond".to_string(),
                    body: UpdateProtocolBody::Completed {
                        update_id: UPDATE_ID.to_string(),
                        result: payloads("done"),
                        failure: None,
                    },
                },
            ],
        ))
        .await?;
    runtime
        .signal_workflow(execution(namespace_id), signal("wake"))
        .await?;
    poll(runtime, namespace_id).await
}

/// Complete `task` with one update message for the completed update, and check
/// that the task fails with v1.31.0's invalid state transition for `kind`,
/// writing no event for the message.
async fn assert_message_fails_the_task(
    runtime: &TokeiraRuntime<InMemoryStore>,
    store: &InMemoryStore,
    run_key: RunKey,
    task: StartedWorkflowTask,
    kind: &str,
    body: UpdateProtocolBody,
) -> Result<()> {
    let result = runtime
        .complete_workflow_task(completion(
            task,
            vec![WorkflowCommand::ProtocolMessage {
                message_id: "late".to_string(),
                body,
            }],
        ))
        .await;
    let expected = format!(
        "invalid state transition attempted for Update {UPDATE_ID}: received *update.{kind} \
         message while in state Completed"
    );
    match result {
        Err(error) => assert_eq!(
            error
                .downcast_ref::<InvalidWorkflowCommand>()
                .map(|invalid| invalid.message.as_str()),
            Some(expected.as_str()),
            "{error:#}"
        ),
        Ok(result) => panic!("the task completed: {result:?}"),
    }
    let history = store.read_history(run_key, 0, 256).await?;
    let accepted = history
        .iter()
        .filter(|event| {
            matches!(
                &event.kind,
                HistoryEventKind::WorkflowExecutionUpdateAccepted { update_id, .. }
                    if update_id == UPDATE_ID
            )
        })
        .count();
    assert_eq!(accepted, 1, "the update was accepted {accepted} times");
    assert!(
        history.iter().any(|event| matches!(
            &event.kind,
            HistoryEventKind::WorkflowTaskFailed {
                failure_cause: WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage,
                ..
            }
        )),
        "the task failed with BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE"
    );
    Ok(())
}

/// A worker replays an acceptance, with its request, for an update that has
/// completed. The task fails, and the update isn't accepted a second time.
#[tokio::test]
async fn an_acceptance_for_a_completed_update_fails_the_task() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id).await?;
    let task = completed_update(&runtime, namespace_id).await?;
    assert_message_fails_the_task(
        &runtime,
        &store,
        run_key,
        task,
        "Acceptance",
        UpdateProtocolBody::Accepted {
            update_id: UPDATE_ID.to_string(),
            update_name: UPDATE_NAME.to_string(),
            input: payloads("input"),
            sequencing_event_id: 1,
        },
    )
    .await
}

/// A rejection for an update that has completed fails the task.
#[tokio::test]
async fn a_rejection_for_a_completed_update_fails_the_task() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id).await?;
    let task = completed_update(&runtime, namespace_id).await?;
    assert_message_fails_the_task(
        &runtime,
        &store,
        run_key,
        task,
        "Rejection",
        UpdateProtocolBody::Rejected {
            update_id: UPDATE_ID.to_string(),
            failure: payloads("rejected").0.remove(0),
        },
    )
    .await
}

/// A response for an update that has completed fails the task with the invalid
/// state transition, not `NotFound`.
#[tokio::test]
async fn a_response_for_a_completed_update_fails_the_task() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id).await?;
    let task = completed_update(&runtime, namespace_id).await?;
    assert_message_fails_the_task(
        &runtime,
        &store,
        run_key,
        task,
        "Response",
        UpdateProtocolBody::Completed {
            update_id: UPDATE_ID.to_string(),
            result: payloads("again"),
            failure: None,
        },
    )
    .await
}

/// The answer an update request got, as Property 1 compares it.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Taken,
    Paused,
    Failing,
    Closing,
    Other(String),
}

fn answer(result: Result<UpdateLifecycleSnapshot>) -> Answer {
    match result {
        Ok(_) => Answer::Taken,
        Err(error) if error.downcast_ref::<UpdatePaused>().is_some() => Answer::Paused,
        Err(error)
            if error
                .downcast_ref::<WorkflowNotReady>()
                .map(|not_ready| not_ready.message)
                == Some(FAILING) =>
        {
            Answer::Failing
        }
        Err(error) if error.downcast_ref::<WorkflowClosing>().is_some() => Answer::Closing,
        Err(error) => Answer::Other(format!("{error:#}")),
    }
}

/// Bring a fresh run to the generated combination: `known` admits the update
/// first, then `failures` tasks fail, then `closing` bounces a close off a
/// buffered signal and starts its retry, then `paused` pauses the run.
async fn prepare(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    known: bool,
    failures: usize,
    closing: bool,
    paused: bool,
) -> Result<RunKey> {
    let run_key = start(runtime, namespace_id).await?;
    if known {
        request_update(runtime, namespace_id, "known").await?;
    }
    fail_tasks(runtime, namespace_id, failures).await?;
    if closing {
        let task = poll(runtime, namespace_id).await?;
        runtime
            .signal_workflow(execution(namespace_id), signal("buffered"))
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
        poll(runtime, namespace_id).await?;
    }
    if paused {
        pause(runtime, namespace_id).await?;
    }
    Ok(run_key)
}

// Feature: update-refusals, Property 1: The refusals come first, in order
// Feature: update-refusals, Property 2: A refused request leaves no trace
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Whatever the run's combination of failing tasks, a close attempt and a
    /// pause, and whether the request repeats an update the run holds, the first
    /// of v1.31.0's refusals that holds answers, paused then failing then
    /// closing, and a run none holds for takes the request. A refused request
    /// changes nothing: the run's state, its history and its updates.
    #[test]
    fn the_refusals_come_first_in_order_and_leave_no_trace(
        known in any::<bool>(),
        failures in 0usize..4,
        closing in any::<bool>(),
        paused in any::<bool>(),
    ) {
        let tokio = tokio::runtime::Runtime::new().unwrap();
        tokio.block_on(async {
            let store = Arc::new(InMemoryStore::default());
            let runtime = runtime(store.clone());
            let namespace_id = NamespaceId::new();
            let run_key = prepare(&runtime, namespace_id, known, failures, closing, paused)
                .await
                .unwrap();
            let tokeira_kernel::LoadedRun::Existing(before) = store.load_run(run_key).await.unwrap()
            else {
                panic!("the run is stored");
            };
            let history_before = store.read_history_to_end(run_key, 0).await.unwrap();
            let expected = if paused {
                Answer::Paused
            } else if before.workflow_task_attempt >= 3 {
                Answer::Failing
            } else if closing {
                Answer::Closing
            } else {
                Answer::Taken
            };
            let update_id = if known { UPDATE_ID } else { "new-update" };
            let answered = answer(
                runtime
                    .update_workflow(
                        execution(namespace_id),
                        update_id.to_string(),
                        UPDATE_NAME.to_string(),
                        payloads("input"),
                        context("request"),
                        Duration::milliseconds(50),
                        UpdateWaitPolicy::Admitted,
                        0,
                    )
                    .await,
            );
            prop_assert_eq!(&answered, &expected);
            if expected != Answer::Taken {
                let tokeira_kernel::LoadedRun::Existing(after) =
                    store.load_run(run_key).await.unwrap()
                else {
                    panic!("the run is stored");
                };
                prop_assert_eq!(&after, &before);
                prop_assert_eq!(
                    store.read_history_to_end(run_key, 0).await.unwrap(),
                    history_before
                );
                if !known {
                    let poll = runtime
                        .poll_workflow_update(
                            execution(namespace_id),
                            update_id.to_string(),
                            UpdateWaitPolicy::Admitted,
                            Duration::milliseconds(50),
                        )
                        .await;
                    prop_assert!(poll.is_err(), "the refused update is known: {:?}", poll);
                }
            }
            Ok(())
        })?;
    }
}

/// The run's task keeps failing once the lane applies the request, though the
/// request was read before: the kernel refuses the update itself.
#[tokio::test]
async fn the_lane_refuses_an_update_to_a_workflow_whose_task_keeps_failing() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = runtime(store.clone());
    let namespace_id = NamespaceId::new();
    let run_key = start(&runtime, namespace_id).await?;
    fail_tasks(&runtime, namespace_id, 2).await?;
    let refused = runtime
        .submit(
            run_key,
            tokeira_kernel::Command::Update(tokeira_kernel::UpdateRequest {
                update_id: UPDATE_ID.to_string(),
                update_name: UPDATE_NAME.to_string(),
                input: payloads("input"),
                request: context("raced"),
                now: OffsetDateTime::now_utc(),
                limits: Default::default(),
                request_bytes: 0,
                in_flight_request_bytes: 0,
                readmit: false,
            }),
        )
        .await
        .expect_err("the kernel refuses the update");
    assert!(
        matches!(
            refused.downcast_ref::<KernelRejected>(),
            Some(KernelRejected(
                tokeira_kernel::Reject::UpdateWorkflowTaskFailing
            ))
        ),
        "{refused:#}"
    );
    Ok(())
}
