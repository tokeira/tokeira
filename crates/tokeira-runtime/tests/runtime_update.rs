use std::{sync::Arc, time::Instant};

use anyhow::Result;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    StartRequest, UpdateProtocolBody, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, UpdateOutcome,
    UpdateWaitPolicy, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{CommitResult, InMemoryStore, RunRepository};
use tokeira_types::{
    ExecutionRef, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext,
    RequestId, SearchAttributes, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId,
    WorkflowType,
};

#[tokio::test]
async fn update_completed_notifies_waiting_caller() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("update-complete".into());
    let run_key =
        start_workflow(&runtime, namespace_id, workflow_id.clone(), "queue-a").await?;

    let runtime_for_update = runtime.clone();
    let workflow_id_for_update = workflow_id.clone();
    let caller = tokio::spawn(async move {
        runtime_for_update
            .update_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id_for_update,
                    run_id: None,
                },
                "update-1".into(),
                "set-value".into(),
                payloads("input"),
                request_context("update-1"),
                Duration::milliseconds(200),
                UpdateWaitPolicy::Completed,
            )
            .await
    });

    wait_for_pending_update(&*store, run_key, "update-1").await?;
    let task = poll_wft(&runtime, workflow_queue(namespace_id, "queue-a")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker-a".into()),
            commands: vec![
                WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept-update-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "set-value".into(),
                        input: payloads("input"),
                    },
                },
                WorkflowCommand::UpdateCompleted {
                    update_id: "update-1".into(),
                    result: payloads("done"),
                },
            ],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let outcome = caller.await.unwrap()?;
    assert_eq!(
        outcome,
        UpdateOutcome::Completed {
            accepted_event_id: 0,
            result: payloads("done"),
        }
    );
    Ok(())
}

#[tokio::test]
async fn update_rejected_notifies_waiting_caller() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("update-reject".into());
    let run_key =
        start_workflow(&runtime, namespace_id, workflow_id.clone(), "queue-a").await?;

    let runtime_for_update = runtime.clone();
    let workflow_id_for_update = workflow_id.clone();
    let caller = tokio::spawn(async move {
        runtime_for_update
            .update_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id_for_update,
                    run_id: None,
                },
                "update-1".into(),
                "reject-me".into(),
                payloads("input"),
                request_context("update-1"),
                Duration::milliseconds(200),
                UpdateWaitPolicy::Completed,
            )
            .await
    });

    wait_for_pending_update(&*store, run_key, "update-1").await?;
    let task = poll_wft(&runtime, workflow_queue(namespace_id, "queue-a")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker-a".into()),
            commands: vec![
                WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept-update-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "reject-me".into(),
                        input: payloads("input"),
                    },
                },
                WorkflowCommand::UpdateRejected {
                    update_id: "update-1".into(),
                    failure: payload("nope"),
                },
            ],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let outcome = caller.await.unwrap()?;
    assert_eq!(
        outcome,
        UpdateOutcome::Rejected {
            accepted_event_id: 0,
            failure: payload("nope"),
        }
    );
    Ok(())
}

#[tokio::test]
async fn update_timeout_does_not_block_late_completion_commit() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("update-timeout".into());
    let run_key =
        start_workflow(&runtime, namespace_id, workflow_id.clone(), "queue-a").await?;

    let error = runtime
        .update_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: workflow_id.clone(),
                run_id: None,
            },
            "update-1".into(),
            "slow".into(),
            payloads("input"),
            request_context("update-1"),
            Duration::milliseconds(20),
            UpdateWaitPolicy::Completed,
        )
        .await
        .expect_err("update should time out");
    assert!(error.to_string().contains("timed out"));

    wait_for_pending_update(&*store, run_key, "update-1").await?;
    let task = poll_wft(&runtime, workflow_queue(namespace_id, "queue-a")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker-a".into()),
            commands: vec![
                WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept-update-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "slow".into(),
                        input: payloads("input"),
                    },
                },
                WorkflowCommand::UpdateCompleted {
                    update_id: "update-1".into(),
                    result: payloads("late"),
                },
            ],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    wait_for_history(&*store, run_key, |history| {
        history.iter().any(|event| {
            matches!(
                &event.kind,
                tokeira_kernel::HistoryEventKind::WorkflowExecutionUpdateCompleted {
                    update_id,
                    result
                } if update_id == "update-1" && result == &payloads("late")
            )
        })
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn run_close_notifies_waiting_update_callers() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("update-run-close".into());
    let run_key =
        start_workflow(&runtime, namespace_id, workflow_id.clone(), "queue-a").await?;

    let runtime_for_update = runtime.clone();
    let workflow_id_for_update = workflow_id.clone();
    let caller = tokio::spawn(async move {
        runtime_for_update
            .update_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id_for_update,
                    run_id: None,
                },
                "update-1".into(),
                "close-me".into(),
                payloads("input"),
                request_context("update-1"),
                Duration::milliseconds(200),
                UpdateWaitPolicy::Completed,
            )
            .await
    });

    wait_for_pending_update(&*store, run_key, "update-1").await?;
    let task = poll_wft(&runtime, workflow_queue(namespace_id, "queue-a")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker-a".into()),
            commands: vec![WorkflowCommand::CompleteWorkflow {
                result: payloads("closed"),
            }],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let error = caller
        .await
        .unwrap()
        .expect_err("run close should fail caller");
    assert!(error.to_string().contains("run closed"));
    Ok(())
}

#[tokio::test]
async fn multiple_updates_resolved_in_single_wft() -> Result<()> {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(make_runtime(store.clone()));
    let namespace_id = NamespaceId::new();
    let workflow_id = WorkflowId("update-multi".into());
    let run_key =
        start_workflow(&runtime, namespace_id, workflow_id.clone(), "queue-a").await?;

    // Submit two updates concurrently.
    let r1 = runtime.clone();
    let wf1 = workflow_id.clone();
    let caller1 = tokio::spawn(async move {
        r1.update_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: wf1,
                run_id: None,
            },
            "update-1".into(),
            "handler-a".into(),
            payloads("input-1"),
            request_context("update-1"),
            Duration::milliseconds(500),
            UpdateWaitPolicy::Completed,
        )
        .await
    });

    let r2 = runtime.clone();
    let wf2 = workflow_id.clone();
    let caller2 = tokio::spawn(async move {
        r2.update_workflow(
            ExecutionRef {
                namespace_id,
                workflow_id: wf2,
                run_id: None,
            },
            "update-2".into(),
            "handler-b".into(),
            payloads("input-2"),
            request_context("update-2"),
            Duration::milliseconds(500),
            UpdateWaitPolicy::Completed,
        )
        .await
    });

    // Wait for both updates to be pending.
    wait_for_pending_update(&*store, run_key, "update-1").await?;
    wait_for_pending_update(&*store, run_key, "update-2").await?;

    // Poll the WFT (may need to poll twice if the
    // kernel scheduled separate WFTs for each update).
    // Complete both updates in a single WFT completion.
    let task = poll_wft(&runtime, workflow_queue(namespace_id, "queue-a")).await?;
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            token: task.token,
            identity: WorkerIdentity("worker-a".into()),
            commands: vec![
                WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept-update-1".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-1".into(),
                        update_name: "handler-a".into(),
                        input: payloads("input-1"),
                    },
                },
                WorkflowCommand::UpdateCompleted {
                    update_id: "update-1".into(),
                    result: payloads("result-1"),
                },
                WorkflowCommand::ProtocolMessage {
                    message_id: "msg-accept-update-2".into(),
                    body: UpdateProtocolBody::Accepted {
                        update_id: "update-2".into(),
                        update_name: "handler-b".into(),
                        input: payloads("input-2"),
                    },
                },
                WorkflowCommand::UpdateCompleted {
                    update_id: "update-2".into(),
                    result: payloads("result-2"),
                },
            ],
            force_new_workflow_task: false,
            now: OffsetDateTime::now_utc(),
        })
        .await?;

    let outcome1 = caller1.await.unwrap()?;
    let outcome2 = caller2.await.unwrap()?;

    // Both callers should receive their respective
    // results independently.
    match outcome1 {
        UpdateOutcome::Completed { result, .. } => {
            assert_eq!(result, payloads("result-1"));
        }
        other => panic!(
            "expected Completed for update-1: \
             {other:?}"
        ),
    }
    match outcome2 {
        UpdateOutcome::Completed { result, .. } => {
            assert_eq!(result, payloads("result-2"));
        }
        other => panic!(
            "expected Completed for update-2: \
             {other:?}"
        ),
    }
    Ok(())
}

fn make_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
    TokeiraRuntime::new(
        store,
        2,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    )
}

async fn start_workflow(
    runtime: &TokeiraRuntime<InMemoryStore>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    task_queue: &str,
) -> Result<tokeira_types::RunKey> {
    let run_id = tokeira_types::RunId::new();
    let result = runtime
        .start_workflow(StartRequest {
            run_key: tokeira_types::RunKey::new(),
            namespace_id,
            workflow_id,
            run_id,
            workflow_type: WorkflowType("update-workflow".into()),
            task_queue: TaskQueueName(task_queue.into()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            continued_execution_run_id: None,
            attempt: 1,
            first_execution_run_id: None,
            first_run_started_at: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            original_execution_run_id: Some(run_id),
            continued_failure: None,
            last_completion_result: None,
            request: request_context("start-1"),
            now: OffsetDateTime::now_utc(),
            cron_schedule: None,
        })
        .await?;
    Ok(match result {
        CommitResult::Applied { new_state } => new_state.run_key,
        other => panic!("unexpected start result: {other:?}"),
    })
}

async fn poll_wft(
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: QueueKey,
) -> Result<tokeira_runtime::StartedWorkflowTask> {
    runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker-a".into()),
            tokio::time::Duration::from_millis(50),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected workflow task"))
}

async fn wait_for_pending_update(
    store: &InMemoryStore,
    run_key: tokeira_types::RunKey,
    update_id: &str,
) -> Result<()> {
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match store.load_run(run_key).await? {
            tokeira_kernel::LoadedRun::Existing(state)
                if state.pending_updates.contains_key(update_id)
                    || state.admitted_updates.contains(update_id) =>
            {
                return Ok(());
            }
            _ => {}
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for pending update");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_history<F>(
    store: &InMemoryStore,
    run_key: tokeira_types::RunKey,
    predicate: F,
) -> Result<()>
where
    F: Fn(&[tokeira_kernel::HistoryEvent]) -> bool,
{
    let deadline = Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let history = store.read_history(run_key, 0, 256).await?;
        if predicate(&history) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for history condition");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn workflow_queue(namespace_id: NamespaceId, task_queue: &str) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(task_queue.into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn request_context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.into()),
        caller_identity: Some("tester".into()),
        received_at: OffsetDateTime::now_utc(),
    }
}

fn payloads(value: &str) -> Payloads {
    Payloads(vec![Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
    }])
}

fn payload(value: &str) -> Payload {
    Payload {
        data: value.as_bytes().to_vec(),
        metadata: Default::default(),
    }
}
