//! Completion-owned activity starts, reply loss, transient history and rule gates.
use super::{
    completion_inline_tests::{completion, fixture, fixture_with_start},
    discovery_tests::stop,
    *,
};
use proptest::prelude::*;
use tokeira_storage::{ActivityDiscoveryRange, InMemoryStore};
use tokeira_types::{WorkflowRuleAction, WorkflowRuleRecord, WorkflowRuleTrigger};

pub(super) fn schedule(id: &str, retry: bool) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: id.into(),
        activity_type: "eager-type".into(),
        task_queue: TaskQueueName("activities".into()),
        input: Payloads(vec![Payload::new(id.as_bytes().to_vec())]),
        header: Some(Headers::default()),
        request_eager_execution: true,
        use_workflow_build_id: false,
        retry_policy: retry.then(|| RetryPolicy {
            initial_interval: Duration::ZERO,
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 3,
            non_retryable_error_types: Vec::new(),
        }),
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: Some(Duration::minutes(10)),
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::minutes(1)),
        heartbeat_timeout: Some(Duration::seconds(10)),
        priority: None,
    }
}

async fn assert_no_offer(
    store: &InMemoryStore,
    runtime: &TokeiraRuntime<InMemoryStore>,
    queue: &QueueKey,
    run: RunKey,
    id: &str,
) {
    let page = store
        .list_activity_dispatch_page(
            &ActivityDiscoveryRange {
                namespace_id: queue.namespace_id,
                queue_name: queue.task_queue.clone(),
            },
            OffsetDateTime::now_utc() + Duration::days(1),
            None,
            std::num::NonZeroU32::new(100).unwrap(),
        )
        .await
        .unwrap();
    assert!(page.candidates.is_empty());
    assert!(
        runtime
            .activity_broker
            .try_claim_activity_task(queue, run, id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn eager_default_off_and_five_starts_without_inline_workflow() {
    for enabled in [false, true] {
        let (store, runtime, first, mut queue) = fixture().await;
        let runtime = runtime.with_activity_eager_execution(enabled);
        let mut req = completion(&first, false);
        req.commands = (0..5).map(|i| schedule(&format!("a-{i}"), true)).collect();
        let result = runtime
            .complete_workflow_task_with_result(req)
            .await
            .unwrap();
        assert!(result.workflow_task.is_none());
        assert_eq!(result.activity_tasks.len(), if enabled { 5 } else { 0 });
        queue.task_kind = TaskKind::Activity;
        queue.task_queue = TaskQueueName("activities".into());
        if enabled {
            assert_no_offer(&store, &runtime, &queue, first.run_key, "a-0").await;
            for task in result.activity_tasks {
                assert_eq!(
                    task.input,
                    Payloads(vec![Payload::new(task.activity_id.as_bytes().to_vec())])
                );
                assert_eq!(task.header, Some(Headers::default()));
                assert_eq!(
                    task.current_attempt_scheduled_time,
                    Some(task.scheduled_time)
                );
                assert_eq!(task.attempt, 1);
            }
        } else {
            for _ in 0..5 {
                assert!(
                    runtime
                        .poll_activity_task(
                            queue.clone(),
                            WorkerIdentity("b".into()),
                            std::time::Duration::from_secs(1)
                        )
                        .await
                        .unwrap()
                        .is_some()
                );
            }
        }
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn eager_transient_start_materializes_original_request_identity_and_time() {
    let (store, runtime, first, _) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut req = completion(&first, false);
    req.commands = vec![schedule("a", true)];
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    let task = outcome.activity_tasks.into_iter().next().unwrap();
    let CommitResult::Applied { new_state } = outcome.commit else {
        panic!("commit")
    };
    let receipt = new_state.activity_start_receipts["a"].clone();
    let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(&e.kind, HistoryEventKind::ActivityTaskStarted { .. }))
    );
    runtime
        .complete_activity_task(
            task.token,
            Payloads::default(),
            Some(WorkerIdentity("a".into())),
            RequestContext::unattributed(OffsetDateTime::now_utc()),
        )
        .await
        .unwrap();
    let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
    let event = history
        .iter()
        .find(|e| matches!(&e.kind, HistoryEventKind::ActivityTaskStarted { .. }))
        .unwrap();
    assert_eq!(event.happened_at, task.started_time);
    assert!(
        matches!(&event.kind, HistoryEventKind::ActivityTaskStarted { request_id, identity, .. } if *request_id == receipt.request_id.to_string() && *identity == receipt.worker_identity)
    );
    stop(&runtime).await;
}

async fn rules_case(retry: bool, eager: bool) {
    let (store, runtime, first, mut queue) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(eager);
    store
        .create_workflow_rule(
            first.origin.namespace_id,
            WorkflowRuleRecord {
                id: "pause-eager-type".into(),
                create_time: OffsetDateTime::now_utc(),
                created_by_identity: "test".into(),
                description: String::new(),
                trigger: WorkflowRuleTrigger::ActivityStart {
                    predicate: "ActivityType = 'eager-type'".into(),
                },
                visibility_query: String::new(),
                actions: vec![WorkflowRuleAction::ActivityPause],
                expiration_time: None,
            },
            10,
        )
        .await
        .unwrap();
    let mut req = completion(&first, false);
    req.commands = vec![schedule("a", retry)];
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    assert_eq!(outcome.activity_tasks.len(), usize::from(eager));
    if eager {
        assert_no_offer(&store, &runtime, &queue, first.run_key, "a").await;
        if retry {
            runtime
                .fail_activity_task(
                    outcome.activity_tasks[0].token.clone(),
                    Payload::new(b"failure".to_vec()),
                    None,
                    false,
                    None,
                    None,
                    RequestContext::unattributed(OffsetDateTime::now_utc()),
                )
                .await
                .unwrap();
        }
    }
    if !eager || retry {
        assert!(
            runtime
                .poll_activity_task(
                    queue,
                    WorkerIdentity("poller".into()),
                    std::time::Duration::from_millis(10)
                )
                .await
                .unwrap()
                .is_none()
        );
        let LoadedRun::Existing(state) = store.load_run(first.run_key).await.unwrap() else {
            panic!("run")
        };
        assert!(state.activities["a"].pause_info.is_some());
    }
    stop(&runtime).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-dispatch, Property 10
    // Feature: workflow-rules, Property 8
    // Eager first starts bypass rules; ordinary starts and retries apply them.
    #[test]
    fn eager_first_start_rule_bypass(retry in any::<bool>(), eager in any::<bool>()) {
        tokio::runtime::Runtime::new().unwrap().block_on(rules_case(retry, eager));
    }
}

#[tokio::test]
async fn eager_scope_denial_retains_ordinary_delivery() {
    let (_, runtime, first, mut queue) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut req = completion(&first, false);
    req.eager_activities.scope = Some(tokeira_kernel::CompletionWorkflowTaskScope {
        namespace_id: first.origin.namespace_id,
        normal_task_queues: vec![TaskQueueName("different".into())],
        deployment: tokeira_types::DeploymentId(String::new()),
        build_id: tokeira_types::BuildId(String::new()),
    });
    req.commands = vec![schedule("a", false)];
    assert!(
        runtime
            .complete_workflow_task_with_result(req)
            .await
            .unwrap()
            .activity_tasks
            .is_empty()
    );
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    assert_eq!(
        runtime
            .poll_activity_task(
                queue,
                WorkerIdentity("authorized-poller".into()),
                std::time::Duration::from_secs(1)
            )
            .await
            .unwrap()
            .unwrap()
            .activity_id,
        "a"
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn eager_paused_workflow_unpause_delivers_pending_activity() {
    let (store, runtime, first, mut queue) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let now = OffsetDateTime::now_utc();
    runtime
        .submit_for_owned_shard(
            first.run_key,
            Command::PauseWorkflow(tokeira_kernel::PauseWorkflowRequest {
                identity: "operator".into(),
                reason: "pause".into(),
                request: RequestContext {
                    request_id: RequestId("pause".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
    let mut req = completion(&first, false);
    req.commands = vec![schedule("a", false)];
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    assert!(outcome.activity_tasks.is_empty());
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    assert_no_offer(&store, &runtime, &queue, first.run_key, "a").await;
    runtime
        .submit_for_owned_shard(
            first.run_key,
            Command::UnpauseWorkflow(tokeira_kernel::UnpauseWorkflowRequest {
                identity: "operator".into(),
                reason: String::new(),
                request: RequestContext {
                    request_id: RequestId("unpause".into()),
                    ..RequestContext::unattributed(now)
                },
                now,
            }),
        )
        .await
        .unwrap();
    assert!(
        runtime
            .poll_activity_task(
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
async fn eager_reply_loss_reload_and_both_timeouts_retry_normally() {
    for heartbeat in [false, true] {
        let (store, runtime, first, mut queue) = fixture().await;
        let runtime = runtime.with_activity_eager_execution(true);
        let mut req = completion(&first, false);
        req.commands = vec![schedule("a", true)];
        if let WorkflowCommand::ScheduleActivity {
            heartbeat_timeout, ..
        } = &mut req.commands[0]
        {
            *heartbeat_timeout = heartbeat.then_some(Duration::seconds(10));
        }
        let outcome = runtime
            .complete_workflow_task_with_result(req)
            .await
            .unwrap();
        let task = outcome.activity_tasks[0].clone();
        let entry = runtime
            .activity_tracking
            .snapshot()
            .into_iter()
            .find(|e| e.activity_id == "a")
            .unwrap();
        assert_eq!(entry.started_at, Some(task.started_time));
        queue.task_kind = TaskKind::Activity;
        queue.task_queue = TaskQueueName("activities".into());
        assert_no_offer(&store, &runtime, &queue, first.run_key, "a").await;
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
        let entry = reloaded
            .activity_tracking
            .snapshot()
            .into_iter()
            .find(|e| e.activity_id == "a")
            .unwrap();
        assert_eq!(entry.started_at, Some(task.started_time));
        assert_no_offer(&restored, &reloaded, &queue, first.run_key, "a").await;
        let LoadedRun::Existing(state) = restored.load_run(first.run_key).await.unwrap() else {
            panic!("run")
        };
        let activity = state.activities["a"].clone();
        let now = task.started_time + Duration::seconds(if heartbeat { 11 } else { 61 });
        assert_eq!(
            crate::activity_timeout::evaluate_activity_timeout(&entry, &activity, now),
            Some(if heartbeat {
                crate::activity_timeout::TimeoutViolation::Heartbeat
            } else {
                crate::activity_timeout::TimeoutViolation::StartToClose
            })
        );
        let deps = reloaded.activity_retry_deps();
        deps.tracking
            .insert(crate::activity_timeout::ActivityTrackingEntry {
                started_at: Some(OffsetDateTime::now_utc() - Duration::minutes(2)),
                ..entry
            });
        crate::activity_timeout::scan_activity_timeouts_once(
            &deps,
            None,
            &reloaded.lanes,
            reloaded.lanes.len(),
            &ActivityTimeoutScannerConfig::default(),
        )
        .await;
        let retried = reloaded
            .poll_activity_task(
                queue,
                WorkerIdentity("b".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.attempt, 2);
        stop(&reloaded).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-dispatch, Property 12
    // Feature: edge-eager-dispatch, Property 9
    // Feature: edge-eager-dispatch, Property 11
    // Eager responses preserve payloads and normalized timeouts, independently of WFT return.
    #[test]
    fn eager_generated_response_and_lost_reply(
        data in prop::collection::vec(any::<u8>(), 0..128),
        count in 1usize..8, enabled in any::<bool>(), return_new in any::<bool>(),
        retry in any::<bool>(), reload in any::<bool>(), explicit_close in any::<bool>(),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (store, runtime, first, mut queue) = fixture().await;
            let runtime = runtime.with_activity_eager_execution(enabled);
            let input = Payloads(vec![Payload::new(data.clone())]);
            let header = Some(Headers(std::collections::BTreeMap::from([("trace".into(), Payload::new(data.clone()))])));
            let mut req = completion(&first, return_new);
            req.commands = (0..count).map(|i| {
                let mut command = schedule(&format!("a-{i}"), retry);
                if let WorkflowCommand::ScheduleActivity { input: target, header: target_header, schedule_to_close_timeout, .. } = &mut command {
                    *target = input.clone(); *target_header = header.clone();
                    *schedule_to_close_timeout = explicit_close.then_some(Duration::minutes(10));
                }
                command
            }).collect();
            let result = runtime.complete_workflow_task_with_result(req).await.unwrap();
            assert_eq!(result.activity_tasks.len(), if enabled { count } else { 0 });
            for (i, task) in result.activity_tasks.iter().enumerate() {
                assert_eq!(task.activity_id, format!("a-{i}"));
                assert_eq!(task.input, input); assert_eq!(task.header, header);
                assert_eq!(task.schedule_to_close_timeout, explicit_close.then_some(Duration::minutes(10)));
                assert_eq!(task.start_to_close_timeout, Some(Duration::minutes(1)));
                assert_eq!(task.heartbeat_timeout, Some(Duration::seconds(10)));
                assert_eq!(task.current_attempt_scheduled_time, Some(task.scheduled_time));
                assert_eq!(task.started_time, task.scheduled_time);
                assert_eq!(task.workflow_type, "example");
            }
            queue.task_kind = TaskKind::Activity; queue.task_queue = TaskQueueName("activities".into());
            if enabled { assert_no_offer(&store, &runtime, &queue, first.run_key, "a-0").await; }
            if reload {
                let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
                let LoadedRun::Existing(state) = restored.load_run(first.run_key).await.unwrap() else { panic!("run") };
                assert_eq!(state.activities.values().filter(|a| a.started_at.is_some()).count(), if enabled { count } else { 0 });
                assert_eq!(state.activity_start_receipts.len(), if enabled { count } else { 0 });
            }
            stop(&runtime).await;
        });
    }
}

#[tokio::test]
async fn failed_completion_never_starts_eager_activity() {
    let (store, runtime, first, _) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut req = completion(&first, false);
    req.commands = vec![schedule("duplicate", true), schedule("duplicate", true)];
    assert!(
        runtime
            .complete_workflow_task_with_result(req)
            .await
            .is_err()
    );
    let LoadedRun::Existing(state) = store.load_run(first.run_key).await.unwrap() else {
        panic!("run")
    };
    assert!(state.activities.is_empty());
    assert!(state.activity_start_receipts.is_empty());
    stop(&runtime).await;
}

async fn normalized_timeout_case(close: Option<Duration>, start: Duration, heartbeat: Duration) {
    let (store, runtime, first, _) = fixture_with_start(tests::sample_start_request(
        None,
        Some(Duration::minutes(5)),
    ))
    .await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut command = schedule("normalized", true);
    if let WorkflowCommand::ScheduleActivity {
        schedule_to_close_timeout,
        start_to_close_timeout,
        heartbeat_timeout,
        ..
    } = &mut command
    {
        *schedule_to_close_timeout = close;
        *start_to_close_timeout = Some(start);
        *heartbeat_timeout = Some(heartbeat);
    }
    let mut req = completion(&first, false);
    req.commands = vec![command];
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    let task = &outcome.activity_tasks[0];
    let history = store.read_history_to_end(first.run_key, 0).await.unwrap();
    let scheduled = history
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::ActivityTaskScheduled {
                schedule_to_close_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
                ..
            } => Some((
                *schedule_to_close_timeout,
                *start_to_close_timeout,
                *heartbeat_timeout,
            )),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        (
            task.schedule_to_close_timeout,
            task.start_to_close_timeout,
            task.heartbeat_timeout
        ),
        scheduled,
        "eager timeouts must match the normalized scheduled event",
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn eager_response_normalizes_omitted_close_timeout() {
    normalized_timeout_case(None, Duration::minutes(1), Duration::seconds(10)).await;
}

#[tokio::test]
async fn eager_response_normalizes_timeouts_above_run_timeout() {
    normalized_timeout_case(
        Some(Duration::minutes(10)),
        Duration::minutes(8),
        Duration::minutes(6),
    )
    .await;
}

#[tokio::test]
async fn eager_response_normalizes_heartbeat_above_start_timeout() {
    normalized_timeout_case(
        Some(Duration::minutes(3)),
        Duration::minutes(1),
        Duration::minutes(2),
    )
    .await;
}

async fn own_priority_case(activity_priority: Option<tokeira_kernel::Priority>) {
    let workflow_priority = tokeira_kernel::Priority {
        priority_key: 3,
        fairness_key: "workflow".into(),
        fairness_weight: 2.0,
    };
    let mut start = tests::sample_start_request(None, None);
    start.priority = Some(workflow_priority.clone());
    let (_, runtime, first, mut queue) = fixture_with_start(start).await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut req = completion(&first, false);
    req.commands = [true, false]
        .into_iter()
        .map(|eager| {
            let mut command = schedule(if eager { "eager" } else { "ordinary" }, true);
            if let WorkflowCommand::ScheduleActivity {
                priority,
                request_eager_execution,
                ..
            } = &mut command
            {
                *priority = activity_priority.clone();
                *request_eager_execution = eager;
            }
            command
        })
        .collect();
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    queue.task_kind = TaskKind::Activity;
    queue.task_queue = TaskQueueName("activities".into());
    let ordinary = runtime
        .poll_activity_task(
            queue,
            WorkerIdentity("b".into()),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    let expected_merge = tokeira_kernel::Priority {
        priority_key: activity_priority.as_ref().map_or(3, |p| p.priority_key),
        fairness_key: "workflow".into(),
        fairness_weight: 2.0,
    };
    assert_eq!(ordinary.activity_id, "ordinary");
    assert_eq!(ordinary.priority, Some(expected_merge));
    assert_eq!(
        outcome.activity_tasks[0].priority, activity_priority,
        "eager priority must not inherit workflow fields"
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn eager_response_keeps_absent_activity_priority() {
    own_priority_case(None).await;
}

#[tokio::test]
async fn eager_response_keeps_partial_activity_priority() {
    own_priority_case(Some(tokeira_kernel::Priority {
        priority_key: 1,
        fairness_key: String::new(),
        fairness_weight: 0.0,
    }))
    .await;
}

#[tokio::test]
async fn eager_tracking_does_not_refresh_existing_started_activity() {
    let (_, runtime, first, _) = fixture().await;
    let runtime = runtime.with_activity_eager_execution(true);
    let mut req = completion(&first, true);
    req.force_new_workflow_task = true;
    req.commands = vec![schedule("existing", true)];
    let outcome = runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    let heartbeat = outcome.activity_tasks[0].started_time + Duration::seconds(1);
    runtime
        .activity_tracking
        .record_heartbeat(first.run_key, "existing", heartbeat);
    let next = outcome.workflow_task.unwrap();
    let mut req = completion(&next, false);
    req.commands = vec![
        WorkflowCommand::RequestCancelActivity {
            scheduled_event_id: outcome.activity_tasks[0].token.schedule_event_id,
        },
        schedule("new", true),
    ];
    runtime
        .complete_workflow_task_with_result(req)
        .await
        .unwrap();
    let entries = runtime.activity_tracking.snapshot();
    let existing = entries
        .iter()
        .find(|e| e.activity_id == "existing")
        .unwrap();
    assert_eq!(existing.last_heartbeat_at, Some(heartbeat));
    assert!(existing.cancel_requested);
    assert!(
        entries
            .iter()
            .any(|e| e.activity_id == "new" && e.started_at.is_some())
    );
    stop(&runtime).await;
}
