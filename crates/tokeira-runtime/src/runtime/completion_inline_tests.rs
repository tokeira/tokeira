//! Completion-owned task contracts through real persistence and timeout recovery.

use super::{discovery_tests::stop, *};
use tokeira_kernel::{
    CompletionWorkflowTaskStart, StickySpec, UpdateProtocolBody, WorkflowCommand, WorkflowTaskType,
};
use tokeira_storage::{InMemoryStore, WorkflowDiscoveryRange, WorkflowDispatchRouting};
use tokeira_types::TaskQueueName;

pub(super) async fn fixture() -> (
    Arc<InMemoryStore>,
    TokeiraRuntime<InMemoryStore>,
    StartedWorkflowTask,
    QueueKey,
) {
    fixture_with_start(tests::sample_start_request(None, None)).await
}

pub(super) async fn fixture_with_start(
    request: StartRequest,
) -> (
    Arc<InMemoryStore>,
    TokeiraRuntime<InMemoryStore>,
    StartedWorkflowTask,
    QueueKey,
) {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    runtime.start_workflow(request).await.unwrap();
    let first = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("a".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    (store, runtime, first, queue)
}

pub(super) fn completion(
    task: &StartedWorkflowTask,
    return_new: bool,
) -> WorkflowTaskCompletedRequest {
    WorkflowTaskCompletedRequest {
        token: task.token.clone(),
        client_discards_speculative_with_events: false,
        identity: task.worker_identity.clone(),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands: Vec::new(),
        command_sizes: Vec::new(),
        heartbeat: Default::default(),
        force_new_workflow_task: false,
        eager_activities: Default::default(),
        inline_workflow_task: return_new.then_some(CompletionWorkflowTaskStart {
            scope: None,
            history_size_bytes: 0,
            advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        }),
        limits: Default::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(OffsetDateTime::now_utc()),
        now: OffsetDateTime::now_utc(),
    }
}

async fn admit(runtime: &TokeiraRuntime<InMemoryStore>, task: &StartedWorkflowTask, id: &str) {
    runtime
        .update_workflow(
            ExecutionRef {
                namespace_id: task.origin.namespace_id,
                workflow_id: task.workflow_id.clone(),
                run_id: Some(task.run_id),
            },
            id.into(),
            "handler".into(),
            Payloads::default(),
            RequestContext {
                request_id: RequestId(format!("admit-{id}")),
                ..RequestContext::unattributed(OffsetDateTime::now_utc())
            },
            Duration::seconds(1),
            UpdateWaitPolicy::Admitted,
            0,
        )
        .await
        .unwrap();
}

async fn no_delivery(
    store: &InMemoryStore,
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: &QueueKey,
    task: &StartedWorkflowTask,
) {
    let page = store
        .list_workflow_dispatch_page(
            &WorkflowDiscoveryRange {
                namespace_id: queue.namespace_id,
                queue_name: queue.task_queue.clone(),
                routing: WorkflowDispatchRouting::Live,
            },
            None,
            std::num::NonZeroU32::new(100).unwrap(),
        )
        .await
        .unwrap();
    assert!(page.candidates.is_empty());
    assert!(
        store
            .list_sticky_workflow_dispatch(
                ShardId(0),
                None,
                std::num::NonZeroU32::new(100).unwrap()
            )
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    assert!(
        runtime
            .broker
            .try_claim_workflow_task(queue, task.run_key)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn update_only_inline_is_virtual_and_rejection_discards_it() {
    let (store, runtime, first, queue) = fixture().await;
    admit(&runtime, &first, "new").await;
    let outcome = runtime
        .complete_workflow_task_with_result(completion(&first, true))
        .await
        .unwrap();
    let inline = outcome.workflow_task.unwrap();
    let CommitResult::Applied { new_state } = outcome.commit else {
        panic!("commit");
    };
    assert_eq!(
        new_state.pending_workflow_task.as_ref().unwrap().task_type,
        WorkflowTaskType::Speculative
    );
    assert_eq!(inline.token.started_event_id, new_state.last_event_id + 2);
    assert_eq!(
        inline.previous_started_event_id,
        first.token.started_event_id
    );
    assert!(inline.is_sticky_match);
    no_delivery(&store, &runtime, &queue, &inline).await;
    assert_eq!(
        runtime
            .pending_update_transports(inline.run_key, false)
            .len(),
        1
    );
    let mut reject = completion(&inline, true);
    reject.commands.push(WorkflowCommand::ProtocolMessage {
        message_id: "reject".into(),
        body: UpdateProtocolBody::Rejected {
            update_id: "new".into(),
            failure: tokeira_types::Payload::new(Vec::new()),
        },
    });
    let dropped = runtime
        .complete_workflow_task_with_result(reject)
        .await
        .unwrap();
    assert!(dropped.workflow_task.is_none());
    let CommitResult::Applied {
        new_state: dropped_state,
    } = dropped.commit
    else {
        panic!("commit");
    };
    assert_eq!(dropped_state.last_event_id, new_state.last_event_id);
    assert_eq!(
        dropped_state.previous_started_event_id,
        first.token.started_event_id
    );
    assert!(dropped_state.pending_workflow_task.is_none());
    stop(&runtime).await;
}

#[tokio::test]
async fn inline_speculative_materializes_on_completion_and_timeout() {
    for timeout in [false, true] {
        let (store, runtime, first, queue) = fixture().await;
        admit(&runtime, &first, "new").await;
        let mut request = completion(&first, true);
        request.sticky = Some(StickySpec {
            queue: TaskQueueName("inline-cache".into()),
            schedule_to_start_timeout: Duration::seconds(5),
        });
        let inline = runtime
            .complete_workflow_task_with_result(request)
            .await
            .unwrap()
            .workflow_task
            .unwrap();
        let before = store.read_history_to_end(first.run_key, 0).await.unwrap();
        assert!(
            before
                .iter()
                .all(|event| event.event_id < (inline.token.started_event_id - 1))
        );
        no_delivery(&store, &runtime, &queue, &inline).await;
        if timeout {
            let entry = runtime
                .wft_timeout_tracking
                .snapshot()
                .into_iter()
                .find(|entry| entry.run_key == inline.run_key)
                .unwrap();
            assert_eq!(entry.kind, crate::wft_timeout::WftTimeoutKind::StartToClose);
            runtime
                .submit_for_owned_shard(
                    inline.run_key,
                    Command::WorkflowTaskTimedOut(tokeira_kernel::WorkflowTaskTimedOutRequest {
                        logical_seq: inline.token.logical_seq,
                        started_event_id: inline.token.started_event_id,
                        timeout_type: tokeira_kernel::WorkflowTaskTimeoutType::StartToClose,
                        now: inline.started_time + inline.workflow_task_timeout,
                    }),
                )
                .await
                .unwrap();
        } else {
            let mut request = completion(&inline, false);
            request.sticky = Some(StickySpec {
                queue: TaskQueueName("inline-cache".into()),
                schedule_to_start_timeout: Duration::seconds(5),
            });
            request.commands.push(WorkflowCommand::CompleteWorkflow {
                result: Payloads::default(),
            });
            runtime.complete_workflow_task(request).await.unwrap();
        }
        let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
        assert_eq!(
            history
                .iter()
                .filter(|event| event.event_id == (inline.token.started_event_id - 1))
                .count(),
            1
        );
        let scheduled = history
            .iter()
            .find(|event| event.event_id == inline.token.started_event_id - 1)
            .unwrap();
        let tokeira_kernel::HistoryEventKind::WorkflowTaskScheduled { task_queue, .. } =
            &scheduled.kind
        else {
            panic!("materialized schedule");
        };
        assert_eq!(task_queue.0, "inline-cache");
        let started = history
            .iter()
            .find(|event| event.event_id == inline.token.started_event_id)
            .unwrap();
        let tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { identity, .. } = &started.kind
        else {
            panic!("materialized start");
        };
        // v1.31.0 materializes a timed-out speculative start with an empty identity.
        assert_eq!(identity.0, if timeout { "" } else { "a" });
        assert_eq!(
            history
                .iter()
                .filter(|event| matches!(&event.kind,
            tokeira_kernel::HistoryEventKind::WorkflowTaskStarted { scheduled_event_id, .. }
                if *scheduled_event_id == (inline.token.started_event_id - 1)))
                .count(),
            1
        );
        if timeout {
            let polled = runtime
                .poll_workflow_task(
                    queue,
                    WorkerIdentity("b".into()),
                    std::time::Duration::from_secs(1),
                )
                .await
                .unwrap()
                .unwrap();
            assert_ne!(polled.token.logical_seq, inline.token.logical_seq);
        }
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn update_only_without_return_is_normal_and_pollable() {
    let (store, runtime, first, queue) = fixture().await;
    admit(&runtime, &first, "new").await;
    let outcome = runtime
        .complete_workflow_task_with_result(completion(&first, false))
        .await
        .unwrap();
    assert!(outcome.workflow_task.is_none());
    let CommitResult::Applied { new_state } = outcome.commit else {
        panic!("commit");
    };
    assert_eq!(
        new_state.pending_workflow_task.as_ref().unwrap().task_type,
        WorkflowTaskType::Normal
    );
    let polled = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("b".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(polled.token.started_event_id > new_state.last_event_id);
    assert!(matches!(
        store.load_run(first.run_key).await.unwrap(),
        LoadedRun::Existing(_)
    ));
    stop(&runtime).await;
}

#[tokio::test]
async fn heartbeat_preserves_sent_updates_and_attaches_new_admissions() {
    let (_store, runtime, first, _queue) = fixture().await;
    admit(&runtime, &first, "sent").await;
    assert_eq!(
        runtime
            .pending_update_transports(first.run_key, false)
            .len(),
        1
    );
    admit(&runtime, &first, "during-completion").await;
    let mut request = completion(&first, true);
    request.force_new_workflow_task = true;
    let inline = runtime
        .complete_workflow_task_with_result(request)
        .await
        .unwrap()
        .workflow_task
        .unwrap();
    let messages = runtime.pending_update_transports(inline.run_key, inline.token.attempt > 1);
    assert_eq!(
        messages
            .iter()
            .map(|message| message.update_id.as_str())
            .collect::<Vec<_>>(),
        ["during-completion"]
    );
    assert_eq!(
        runtime.update_registry.sent_update_ids(first.run_key).len(),
        2
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn lost_inline_reply_reloads_started_and_times_out_before_poll_delivery() {
    let (store, runtime, first, queue) = fixture().await;
    let mut request = completion(&first, true);
    request.force_new_workflow_task = true;
    request.sticky = Some(StickySpec {
        queue: TaskQueueName("sticky-a".into()),
        schedule_to_start_timeout: Duration::seconds(5),
    });
    let outcome = runtime
        .complete_workflow_task_with_result(request)
        .await
        .unwrap();
    let inline = outcome.workflow_task.unwrap();
    let entry = runtime
        .wft_timeout_tracking
        .snapshot()
        .into_iter()
        .find(|entry| entry.run_key == inline.run_key)
        .unwrap();
    assert_eq!(entry.started_event_id, inline.token.started_event_id);
    assert_eq!(entry.kind, crate::wft_timeout::WftTimeoutKind::StartToClose);
    no_delivery(&store, &runtime, &queue, &inline).await;
    stop(&runtime).await;
    let restored =
        Arc::new(InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap());
    tokeira_storage::prepare_execution_placement(restored.as_ref())
        .await
        .unwrap();
    restored.ensure_outbound_prepared().await.unwrap();
    let reloaded = TokeiraRuntime::new(
        restored.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    reloaded.ready().await.unwrap();
    no_delivery(&restored, &reloaded, &queue, &inline).await;
    let entries = reloaded.wft_timeout_tracking.snapshot();
    assert!(entries.iter().any(
        |entry| entry.started_event_id == inline.token.started_event_id
            && entry.kind == crate::wft_timeout::WftTimeoutKind::StartToClose
    ));
    let at_deadline = inline.started_time + inline.workflow_task_timeout + Duration::nanoseconds(1);
    assert!(crate::wft_timeout::evaluate_wft_timeout(
        &entry,
        at_deadline
    ));
    reloaded
        .submit_for_owned_shard(
            inline.run_key,
            Command::WorkflowTaskTimedOut(tokeira_kernel::WorkflowTaskTimedOutRequest {
                logical_seq: inline.token.logical_seq,
                started_event_id: inline.token.started_event_id,
                timeout_type: tokeira_kernel::WorkflowTaskTimeoutType::StartToClose,
                now: at_deadline,
            }),
        )
        .await
        .unwrap();
    let retried = reloaded
        .poll_workflow_task(
            queue,
            WorkerIdentity("b".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.token.attempt, inline.token.attempt);
    assert_ne!(retried.token.logical_seq, inline.token.logical_seq);
    stop(&reloaded).await;
}

#[tokio::test]
async fn failed_completion_never_returns_inline_work() {
    let (store, runtime, first, queue) = fixture().await;
    let mut request = completion(&first, true);
    request.force_new_workflow_task = true;
    request
        .commands
        .push(WorkflowCommand::RequestCancelActivity {
            scheduled_event_id: 999,
        });
    assert!(
        runtime
            .complete_workflow_task_with_result(request)
            .await
            .is_err()
    );
    let LoadedRun::Existing(state) = store.load_run(first.run_key).await.unwrap() else {
        panic!("run");
    };
    assert!(
        state
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .started_event_id
            .is_none()
    );
    assert!(
        runtime
            .poll_workflow_task(
                queue,
                WorkerIdentity("b".into()),
                std::time::Duration::from_secs(1)
            )
            .await
            .unwrap()
            .is_some()
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn completion_inline_scope_denial_leaves_task_for_authorized_poll() {
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
    );
    let mut start = tests::sample_start_request(None, None);
    start.deployment = Some(tokeira_types::DeploymentId("workers".into()));
    start.build_id = Some(tokeira_types::BuildId("build".into()));
    let queue = QueueKey {
        namespace_id: start.namespace_id,
        task_queue: start.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: start.deployment.clone(),
        build_id: start.build_id.clone(),
    };
    runtime.start_workflow(start).await.unwrap();
    let first = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("a".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut request = completion(&first, true);
    request.force_new_workflow_task = true;
    request.versioning_behavior = tokeira_kernel::VersioningBehavior::AutoUpgrade;
    request.deployment_version = Some(tokeira_kernel::WorkerDeploymentVersionRef {
        deployment_name: "workers".into(),
        build_id: "build".into(),
    });
    request.inline_workflow_task.as_mut().unwrap().scope =
        Some(tokeira_kernel::CompletionWorkflowTaskScope {
            namespace_id: queue.namespace_id,
            normal_task_queues: vec![TaskQueueName("other".into())],
            deployment: queue.deployment.clone().unwrap(),
            build_id: queue.build_id.clone().unwrap(),
        });
    let denied = runtime
        .complete_workflow_task_with_result(request)
        .await
        .unwrap();
    assert!(denied.workflow_task.is_none());
    let polled = runtime
        .poll_workflow_task(
            queue.clone(),
            WorkerIdentity("b".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let mut request = completion(&polled, true);
    request.force_new_workflow_task = true;
    request.versioning_behavior = tokeira_kernel::VersioningBehavior::AutoUpgrade;
    request.deployment_version = Some(tokeira_kernel::WorkerDeploymentVersionRef {
        deployment_name: "workers".into(),
        build_id: "build".into(),
    });
    request.sticky = Some(StickySpec {
        queue: TaskQueueName("b-cache".into()),
        schedule_to_start_timeout: Duration::seconds(5),
    });
    request.inline_workflow_task.as_mut().unwrap().scope =
        Some(tokeira_kernel::CompletionWorkflowTaskScope {
            namespace_id: queue.namespace_id,
            normal_task_queues: vec![queue.task_queue.clone()],
            deployment: queue.deployment.unwrap(),
            build_id: queue.build_id.unwrap(),
        });
    let allowed = runtime
        .complete_workflow_task_with_result(request)
        .await
        .unwrap();
    assert!(allowed.workflow_task.is_some());
    let CommitResult::Applied { new_state } = allowed.commit else {
        panic!("commit");
    };
    assert_eq!(new_state.sticky.unwrap().sticky_queue.0, "b-cache");
    stop(&runtime).await;
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(100))]
    // Feature: api-conformance-wft-completion, Property 3
    // Polls and reloads cannot add a second start to a completion-owned incarnation.
    #[test]
    fn completion_inline_generated_commits_offers_and_reload(
        return_new in proptest::bool::ANY, sticky in proptest::bool::ANY,
        update in proptest::bool::ANY, failed in proptest::bool::ANY, reload in proptest::bool::ANY,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (store, runtime, first, queue) = fixture().await;
            if update { admit(&runtime, &first, "mid-task").await; }
            let LoadedRun::Existing(before) = store.load_run(first.run_key).await.unwrap() else { panic!("run"); };
            let mut request = completion(&first, return_new);
            request.force_new_workflow_task = !update;
            request.sticky = sticky.then(|| StickySpec { queue: TaskQueueName("cache".into()), schedule_to_start_timeout: Duration::seconds(5) });
            if failed { request.commands.push(WorkflowCommand::RequestCancelActivity { scheduled_event_id: i64::MAX }); }
            let outcome = runtime.complete_workflow_task_with_result(request).await;
            let LoadedRun::Existing(after) = store.load_run(first.run_key).await.unwrap() else { panic!("run"); };
            assert_eq!(after.transition_seq.0, before.transition_seq.0 + 1);
            assert_eq!(outcome.is_err(), failed);
            let should_start = return_new && !failed;
            assert_eq!(after.pending_workflow_task.as_ref().unwrap().started_event_id.is_some(), should_start);
            if should_start {
                let inline = outcome.unwrap().workflow_task.unwrap();
                no_delivery(&store, &runtime, &queue, &inline).await;
                stop(&runtime).await;
                if reload {
                    let restored = Arc::new(InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap());
                    tokeira_storage::prepare_execution_placement(restored.as_ref()).await.unwrap();
                    restored.ensure_outbound_prepared().await.unwrap();
                    let next = TokeiraRuntime::new(restored.clone(), 1, LaneConfig::default(), TimerScannerConfig::default(), WorkflowTimeoutScannerConfig::default());
                    next.ready().await.unwrap();
                    no_delivery(&restored, &next, &queue, &inline).await;
                    let LoadedRun::Existing(reloaded) = restored.load_run(first.run_key).await.unwrap() else { panic!("run"); };
                    assert_eq!(reloaded.transition_seq, after.transition_seq);
                    assert_eq!(reloaded.pending_workflow_task.unwrap().started_event_id, Some(inline.token.started_event_id));
                    stop(&next).await;
                }
            } else {
                let polled = runtime.poll_workflow_task(queue, WorkerIdentity("b".into()), std::time::Duration::from_secs(1)).await.unwrap().unwrap();
                assert_eq!(polled.token.logical_seq, after.pending_workflow_task.unwrap().logical_seq);
                let LoadedRun::Existing(polled_state) = store.load_run(first.run_key).await.unwrap() else { panic!("run"); };
                assert_eq!(polled_state.transition_seq.0, after.transition_seq.0 + 1);
                stop(&runtime).await;
            }
        });
    }
}
