//! Successor contracts exercised through the real runtime, kernel and memory store.
//! Explicit scanner calls and request times make failures reproducible without sleeps.

use super::*;
use crate::{
    successor::started_event_inputs,
    timeout::{WorkflowTimeoutViolation, submit_workflow_timeout},
    update::{UpdateResolution, UpdateWaitPolicy},
};
use proptest::prelude::*;
use prost::Message as _;
use tokeira_kernel::{
    BasicKernel, HistoryEvent, Kernel, Link, RetryState, StartWorkflowTaskRequest,
    WorkflowStartDelayElapsedRequest,
};
use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
use tokeira_storage::{InMemoryStore, WriteFence};
use tokeira_types::{Memo, SearchAttrValue, SearchAttributes};

fn clock() -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(20)
}

fn executor() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap()
}

async fn runtime() -> TokeiraRuntime<InMemoryStore> {
    let mut runtime = TokeiraRuntime::new_with_config(
        Arc::new(InMemoryStore::default()),
        RuntimeConfig::default(),
    );
    runtime.shutdown_timer_scanner().await.unwrap();
    runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    runtime.shutdown_wft_timeout_scanner().await.unwrap();
    runtime
}

fn request(bytes: Vec<u8>) -> StartRequest {
    let mut request = tests::sample_start_request(None, None);
    request.now = clock();
    request.request.received_at = request.now;
    request.input = Payloads(vec![Payload::new(bytes.clone())]);
    request.header = Some(Headers(
        [("header".into(), Payload::new(bytes.clone()))].into(),
    ));
    request.memo = Memo([("memo".into(), Payload::new(bytes))].into());
    request.search_attributes =
        SearchAttributes([("search".into(), SearchAttrValue::Int(7))].into());
    request.links = vec![Link::BatchJob {
        job_id: "original".into(),
    }];
    request.retry_policy = Some(RetryPolicy {
        initial_interval: Duration::milliseconds(100),
        backoff_coefficient: 1.0,
        maximum_interval: None,
        maximum_attempts: 3,
        non_retryable_error_types: Vec::new(),
    });
    request
}

fn failure() -> Payload {
    Payload::new(
        Failure {
            message: "failed attempt".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "retryable".into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

async fn seed_history(
    runtime: &TokeiraRuntime<InMemoryStore>,
    request: &StartRequest,
    history_mode: u8,
) -> WorkflowTaskCompletedRequest {
    let mut transition = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request.clone()))
        .unwrap();
    match history_mode {
        1 => {
            let mut value = serde_json::to_value(&transition.history_events[0].kind).unwrap();
            let mut fields = value
                .as_object_mut()
                .unwrap()
                .remove("WorkflowExecutionStartedV2")
                .unwrap();
            fields
                .as_object_mut()
                .unwrap()
                .remove("eager_execution_accepted");
            transition.history_events[0].kind =
                serde_json::from_value(serde_json::json!({"WorkflowExecutionStarted": fields}))
                    .unwrap();
        }
        2 => {
            transition.history_events.remove(0);
            transition.event_principals.remove(0);
        }
        3 => {
            transition.history_events[0].kind = HistoryEventKind::WorkflowExecutionCompleted {
                workflow_task_completed_event_id: 0,
                result: Payloads::default(),
                new_execution_run_id: None,
            };
        }
        _ => {}
    }
    runtime
        .repo
        .commit_transition(request.run_key, transition, WriteFence::Unfenced)
        .await
        .unwrap();
    start_task(runtime, request).await
}

async fn start_task(
    runtime: &TokeiraRuntime<InMemoryStore>,
    request: &StartRequest,
) -> WorkflowTaskCompletedRequest {
    let LoadedRun::Existing(state) = runtime.repo.load_run(request.run_key).await.unwrap() else {
        panic!("missing run")
    };
    let pending = state.pending_workflow_task.unwrap();
    let result = runtime
        .submit_for_owned_shard(
            request.run_key,
            Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: pending.logical_seq,
                worker_identity: WorkerIdentity("worker".into()),
                request_id: format!("start-task:{}:{}", request.run_id.0, pending.logical_seq.0),
                history_size_bytes: 0,
                advice_policy: continue_as_new_advice_policy(),
                deployment_transition: None,
                deployment_transition_revision_number: None,
                polled_task_queue: request.task_queue.clone(),
                now: request.now + Duration::milliseconds(1),
                target_version_changed_enabled: false,
                target_deployment_version: None,
            }),
        )
        .await
        .unwrap();
    let CommitResult::Applied { new_state } = result else {
        panic!("task start rejected")
    };
    let pending = new_state.pending_workflow_task.unwrap();
    completion_request(
        WorkflowTaskToken {
            run_key: request.run_key,
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap(),
            attempt: pending.attempt,
            shard_epoch: ShardEpoch::ZERO,
        },
        request,
    )
}

async fn timeout_at(
    runtime: &TokeiraRuntime<InMemoryStore>,
    request: &StartRequest,
    now: OffsetDateTime,
    violation: WorkflowTimeoutViolation,
) {
    let entry = runtime
        .workflow_timeout_tracking
        .snapshot()
        .into_iter()
        .find(|e| e.run_key == request.run_key)
        .unwrap();
    submit_workflow_timeout(
        runtime.repo.clone(),
        runtime.lanes.clone(),
        runtime.lanes.len(),
        entry,
        violation,
        now,
    )
    .await
    .unwrap();
}

fn continue_command(start: &StartRequest) -> WorkflowCommand {
    WorkflowCommand::ContinueAsNew {
        new_run_id: RunId::new(),
        workflow_type: start.workflow_type.clone(),
        task_queue: start.task_queue.clone(),
        input: start.input.clone(),
        memo: start.memo.clone(),
        search_attributes: start.search_attributes.clone(),
        workflow_execution_timeout: start.workflow_execution_timeout,
        workflow_run_timeout: start.workflow_run_timeout,
        workflow_task_timeout: start.workflow_task_timeout,
        retry_policy: start.retry_policy.clone(),
        header: start.header.clone(),
        initial_versioning_behavior: Default::default(),
        successor_versioning_info: None,
    }
}

async fn release_delay(
    runtime: &TokeiraRuntime<InMemoryStore>,
    run_key: RunKey,
    fired_at: OffsetDateTime,
) {
    runtime
        .submit_for_owned_shard(
            run_key,
            Command::WorkflowStartDelayElapsed(WorkflowStartDelayElapsedRequest { fired_at }),
        )
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn history_failure_preserves_close_and_update_waiter() {
    for history_mode in 0..4 {
        let runtime = runtime().await;
        let start = request(vec![1]);
        let mut completion = seed_history(&runtime, &start, history_mode).await;
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        runtime.update_registry.register(
            start.run_key,
            "update".into(),
            "name".into(),
            Payloads::default(),
            "worker".into(),
            0,
            UpdateWaitPolicy::Completed,
            tx,
        );
        completion.commands.insert(
            0,
            WorkflowCommand::UpdateRejected {
                update_id: "update".into(),
                failure: failure(),
            },
        );
        let before = runtime.repo.snapshot().await.unwrap();
        if history_mode < 2 {
            runtime.repo.fail_history_event_reads(1).await;
        }
        let error = runtime
            .complete_workflow_task(completion)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("successor start history"));
        assert_eq!(runtime.repo.snapshot().await.unwrap(), before);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        let waiters = runtime
            .update_registry
            .take_entry_waiters(start.run_key, "update");
        assert_eq!(waiters.len(), 1);
        runtime
            .update_registry
            .resolve_waiters(waiters, UpdateResolution::Rejected { failure: failure() });
        assert!(matches!(
            rx.await.unwrap(),
            UpdateResolution::Rejected { .. }
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_history_read_failure_remains_retryable() {
    for cron in [false, true] {
        let runtime = runtime().await;
        let mut start = request(vec![1]);
        start.workflow_run_timeout = Some(Duration::seconds(5));
        if cron {
            start.retry_policy = None;
            start.cron_schedule = Some("@every 2s".into());
        }
        runtime.start_workflow(start.clone()).await.unwrap();
        let entry = runtime
            .workflow_timeout_tracking
            .snapshot()
            .into_iter()
            .find(|entry| entry.run_key == start.run_key)
            .unwrap();
        let before = runtime.repo.snapshot().await.unwrap();
        runtime.repo.fail_history_event_reads(1).await;
        let result = submit_workflow_timeout(
            runtime.repo.clone(),
            runtime.lanes.clone(),
            runtime.lanes.len(),
            entry.clone(),
            WorkflowTimeoutViolation::RunTimeout,
            start.now + Duration::seconds(5),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(runtime.repo.snapshot().await.unwrap(), before);
        assert!(
            runtime
                .workflow_timeout_tracking
                .snapshot()
                .contains(&entry)
        );
        timeout_at(
            &runtime,
            &start,
            start.now + Duration::seconds(6),
            WorkflowTimeoutViolation::RunTimeout,
        )
        .await;
        assert!(successor(&runtime, &start).await.is_some());
    }
}

#[tokio::test(start_paused = true)]
async fn commands_after_close_are_still_rejected() {
    let runtime = runtime().await;
    let start = request(vec![1]);
    runtime.start_workflow(start.clone()).await.unwrap();
    let mut completion = completion(&runtime, &start).await;
    completion.commands.push(WorkflowCommand::CompleteWorkflow {
        result: Payloads::default(),
    });
    assert!(runtime.complete_workflow_task(completion).await.is_err());
    assert!(successor(&runtime, &start).await.is_none());
    assert_eq!(
        runtime
            .repo
            .list_runs_for_namespace(start.namespace_id)
            .await
            .unwrap(),
        vec![start.run_key]
    );
}

async fn completion(
    runtime: &TokeiraRuntime<InMemoryStore>,
    start: &StartRequest,
) -> WorkflowTaskCompletedRequest {
    let task = runtime
        .poll_workflow_task(
            QueueKey {
                namespace_id: start.namespace_id,
                task_queue: start.task_queue.clone(),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            WorkerIdentity("worker".into()),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    completion_request(task.token, start)
}

fn completion_request(
    token: WorkflowTaskToken,
    start: &StartRequest,
) -> WorkflowTaskCompletedRequest {
    WorkflowTaskCompletedRequest {
        client_discards_speculative_with_events: false,
        token,
        identity: WorkerIdentity("worker".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: Default::default(),
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands: vec![WorkflowCommand::FailWorkflow { failure: failure() }],
        force_new_workflow_task: false,
        limits: Default::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(start.now + Duration::milliseconds(100)),
        now: start.now + Duration::milliseconds(100),
        command_sizes: Vec::new(),
    }
}

async fn successor(
    runtime: &TokeiraRuntime<InMemoryStore>,
    start: &StartRequest,
) -> Option<(WorkflowState, HistoryEvent)> {
    let history = runtime
        .repo
        .read_history(start.run_key, 0, 64)
        .await
        .unwrap();
    let id = history.iter().find_map(|event| match event.kind {
        HistoryEventKind::WorkflowExecutionFailed {
            new_execution_run_id,
            ..
        }
        | HistoryEventKind::WorkflowExecutionCompleted {
            new_execution_run_id,
            ..
        }
        | HistoryEventKind::WorkflowExecutionTimedOut {
            new_execution_run_id,
            ..
        } => new_execution_run_id,
        HistoryEventKind::WorkflowExecutionContinuedAsNew { new_run_id, .. } => Some(new_run_id),
        _ => None,
    })?;
    let key = RunKey::derive(start.namespace_id, &start.workflow_id, id);
    let LoadedRun::Existing(state) = runtime.repo.load_run(key).await.unwrap() else {
        panic!("paired successor missing")
    };
    let event = runtime
        .repo
        .read_history(key, 0, 1)
        .await
        .unwrap()
        .remove(0);
    Some((state, event))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: successor-semantics, Property 1: history-owned metadata survives mutations and either start encoding.
    #[test]
    fn original_metadata_survives(kind in 0u8..5, legacy in any::<bool>(), shape in 0u8..3, bytes in prop::collection::vec(any::<u8>(), 0..48)) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(bytes);
            if shape < 2 { start.header = (shape == 1).then(Headers::default); start.memo = Memo::default(); start.search_attributes = SearchAttributes::default(); start.links.clear(); }
            start.cron_schedule = Some("@every 2s".into());
            if kind == 1 || kind == 2 || kind == 4 { start.retry_policy = None; }
            start.workflow_run_timeout = Some(Duration::seconds(5));
            let mut completion = seed_history(&runtime, &start, u8::from(legacy)).await;
            completion.commands = vec![
                WorkflowCommand::UpsertMemo(Memo([("memo".into(), Payload::new(b"changed"))].into())),
                WorkflowCommand::UpsertSearchAttributes(SearchAttributes([("search".into(), SearchAttrValue::Int(99))].into())),
            ];
            if kind < 3 {
                completion.commands.push(if kind == 1 { WorkflowCommand::CompleteWorkflow { result: Payloads::default() } } else { WorkflowCommand::FailWorkflow { failure: failure() } });
            }
            runtime.complete_workflow_task(completion).await.unwrap();
            if kind >= 3 {
                runtime.workflow_timeout_tracking.insert(crate::timeout::WorkflowTimeoutEntry {
                    run_key: start.run_key, shard_id: ShardId(0), workflow_execution_timeout: start.workflow_execution_timeout,
                    workflow_run_timeout: start.workflow_run_timeout, started_at: start.now, workflow_start_delay: start.workflow_start_delay,
                    first_run_started_at: start.first_run_started_at, has_retry_policy: start.retry_policy.is_some(),
                });
                timeout_at(&runtime, &start, start.now + Duration::seconds(5), WorkflowTimeoutViolation::RunTimeout).await;
            }
            let (_, event) = successor(&runtime, &start).await.unwrap();
            let actual = started_event_inputs(&event).unwrap();
            prop_assert_eq!(actual.header, start.header);
            prop_assert_eq!(actual.memo, start.memo);
            prop_assert_eq!(actual.search_attributes, start.search_attributes);
            prop_assert_eq!(actual.links, start.links);
            prop_assert_eq!(actual.input, start.input);
            prop_assert_eq!(actual.cron_schedule, start.cron_schedule);
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 2: the actual close selects failure and result presence.
    #[test]
    fn outcome_follows_cause(kind in 0u8..5, prior in 0u8..3, empty_result in any::<bool>(), bytes in prop::collection::vec(any::<u8>(), 1..48)) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(bytes.clone());
            start.cron_schedule = Some("@every 2s".into());
            start.last_completion_result = match prior { 0 => None, 1 => Some(Payloads::default()), _ => Some(Payloads(vec![Payload::new(bytes)])) };
            if kind == 1 || kind == 2 || kind == 4 { start.retry_policy = None; }
            start.workflow_run_timeout = Some(Duration::seconds(5));
            runtime.start_workflow(start.clone()).await.unwrap();
            let result = if empty_result { Payloads::default() } else { Payloads(vec![Payload::new(b"new result")]) };
            if kind >= 3 { timeout_at(&runtime, &start, start.now + Duration::seconds(5), WorkflowTimeoutViolation::RunTimeout).await; }
            else {
                let mut completion = completion(&runtime, &start).await;
                if kind == 1 { completion.commands = vec![WorkflowCommand::CompleteWorkflow { result: result.clone() }]; }
                runtime.complete_workflow_task(completion).await.unwrap();
            }
            let (_, event) = successor(&runtime, &start).await.unwrap();
            let (actual_failure, actual_result) = match event.kind {
                HistoryEventKind::WorkflowExecutionStartedV2 { continued_failure, last_completion_result, .. } => (continued_failure, last_completion_result),
                _ => panic!("new starts must be V2"),
            };
            let expected_failure = match kind { 1 => None, 3 | 4 => Some(workflow_task::workflow_run_timeout_failure()), _ => Some(failure()) };
            prop_assert_eq!(actual_failure.as_ref().map(|p| Failure::decode(p.data.as_slice()).unwrap()), expected_failure.as_ref().map(|p| Failure::decode(p.data.as_slice()).unwrap()));
            prop_assert_eq!(actual_result, match kind { 0 => None, 1 => Some(result), _ => start.last_completion_result });
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 3: terminal retry decisions can still select a cron successor.
    #[test]
    fn selection_matches_table(policy_case in 0u8..4, cron in any::<bool>()) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(vec![1]);
            start.cron_schedule = cron.then(|| "@every 2s".into());
            match policy_case {
                0 => start.retry_policy = None,
                2 => start.retry_policy.as_mut().unwrap().maximum_attempts = 1,
                3 => start.retry_policy.as_mut().unwrap().non_retryable_error_types = vec!["retryable".into()],
                _ => {}
            }
            runtime.start_workflow(start.clone()).await.unwrap();
            runtime.complete_workflow_task(completion(&runtime, &start).await).await.unwrap();
            let history = runtime.repo.read_history(start.run_key, 0, 64).await.unwrap();
            let recorded = history.iter().find_map(|event| match &event.kind { HistoryEventKind::WorkflowExecutionFailed { retry_state, .. } => Some(retry_state.clone()), _ => None }).unwrap();
            prop_assert_eq!(recorded, match policy_case { 0 => RetryState::RetryPolicyNotSet, 1 => RetryState::InProgress, 2 => RetryState::MaximumAttemptsReached, _ => RetryState::NonRetryableFailure });
            let next = successor(&runtime, &start).await;
            prop_assert_eq!(next.is_some(), policy_case == 1 || cron);
            if let Some((state, event)) = next {
                prop_assert_eq!(state.attempt, if policy_case == 1 { 2 } else { 1 });
                let HistoryEventKind::WorkflowExecutionStartedV2 { initiator, .. } = event.kind else { panic!("start") };
                prop_assert_eq!(initiator, Some(if policy_case == 1 { tokeira_kernel::ContinueAsNewInitiator::Retry } else { tokeira_kernel::ContinueAsNewInitiator::CronSchedule }));
            }
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 3: retry completion or exhaustion returns to the original cron chain.
    #[test]
    fn retry_chain_returns_to_cron(completes in any::<bool>(), backoff_ms in 1i64..2000) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(vec![1]);
            start.cron_schedule = Some("@every 2s".into());
            start.retry_policy.as_mut().unwrap().maximum_attempts = 2;
            start.retry_policy.as_mut().unwrap().initial_interval = Duration::milliseconds(backoff_ms);
            runtime.start_workflow(start.clone()).await.unwrap();
            runtime.complete_workflow_task(completion(&runtime, &start).await).await.unwrap();
            let (retry, _) = successor(&runtime, &start).await.unwrap();
            let execution_time = retry.started_at + retry.workflow_start_delay.unwrap();
            release_delay(&runtime, retry.run_key, execution_time).await;
            let mut next = start.clone(); next.run_key = retry.run_key; next.run_id = retry.run_id; next.now = execution_time;
            let mut completion = start_task(&runtime, &next).await;
            if completes { completion.commands = vec![WorkflowCommand::CompleteWorkflow { result: Payloads::default() }]; }
            runtime.complete_workflow_task(completion).await.unwrap();
            let (cron, event) = successor(&runtime, &next).await.unwrap();
            prop_assert_eq!(cron.attempt, 1);
            prop_assert_eq!(cron.first_execution_run_id, Some(start.run_id));
            let HistoryEventKind::WorkflowExecutionStartedV2 { cron_schedule, initiator, .. } = event.kind else { panic!("start") };
            prop_assert_eq!(cron_schedule, start.cron_schedule);
            prop_assert_eq!(initiator, Some(tokeira_kernel::ContinueAsNewInitiator::CronSchedule));
            if !completes {
                let history = runtime.repo.read_history(retry.run_key, 0, 64).await.unwrap();
                let retry_state = history.iter().find_map(|event| match &event.kind { HistoryEventKind::WorkflowExecutionFailed { retry_state, .. } => Some(retry_state), _ => None });
                prop_assert_eq!(retry_state, Some(&RetryState::MaximumAttemptsReached));
            }
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 4: execution-age minimum, scanner clock and timer eligibility agree.
    #[test]
    fn minimum_and_timer_agree(kind in 0u8..3, age_ms in 1i64..2000, delay_ms in 1i64..2000, backoff_ms in 1i64..1500, lag_ms in 0i64..2000) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(vec![1]);
            start.workflow_start_delay = Some(Duration::milliseconds(delay_ms));
            start.workflow_run_timeout = Some(Duration::milliseconds(age_ms));
            start.retry_policy.as_mut().unwrap().initial_interval = Duration::milliseconds(backoff_ms);
            if kind == 2 { start.retry_policy = None; start.cron_schedule = Some("@every 2s".into()); }
            runtime.start_workflow(start.clone()).await.unwrap();
            let execution_time = start.now + Duration::milliseconds(delay_ms);
            let now = execution_time + Duration::milliseconds(age_ms + if kind == 0 { 0 } else { lag_ms });
            let before_start = OffsetDateTime::now_utc();
            if kind == 0 {
                release_delay(&runtime, start.run_key, execution_time).await;
                let mut completion = completion(&runtime, &start).await;
                completion.now = now;
                runtime.complete_workflow_task(completion).await.unwrap();
            } else { timeout_at(&runtime, &start, now, WorkflowTimeoutViolation::RunTimeout).await; }
            let (state, _) = successor(&runtime, &start).await.unwrap();
            let raw = if kind == 2 { crate::schedule::cron_backoff_for_next_schedule("@every 2s", execution_time, now).unwrap() } else { Duration::milliseconds(backoff_ms) };
            let age = now - execution_time;
            let expected = if age + raw < Duration::seconds(1) { Duration::seconds(1) - age } else { raw };
            prop_assert!(state.started_at >= before_start && state.started_at <= OffsetDateTime::now_utc());
            prop_assert_eq!(state.workflow_start_delay, Some(expected));
            let deadline = state.started_at + expected;
            if expected > Duration::ZERO {
                prop_assert!(state.pending_workflow_task.is_none());
                prop_assert!(!runtime.repo.list_due_timers(deadline - Duration::nanoseconds(1), 64).await.unwrap().iter().any(|t| t.run_key == state.run_key));
                let due = runtime.repo.list_due_timers(deadline, 64).await.unwrap();
                prop_assert!(due.iter().any(|t| t.run_key == state.run_key && t.fire_at == deadline));
                release_delay(&runtime, state.run_key, deadline).await;
                let LoadedRun::Existing(ready) = runtime.repo.load_run(state.run_key).await.unwrap() else { panic!("successor") };
                prop_assert!(ready.pending_workflow_task.is_some());
            } else { prop_assert!(state.pending_workflow_task.is_some()); }
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 6: an execution deadline ends the chain regardless of policy or cron.
    #[test]
    fn expired_chain_is_terminal(timeout_ms in 1i64..10000, lag_ms in 0i64..2000, policy in any::<bool>(), cron in any::<bool>()) {
        executor().block_on(async {
            let runtime = runtime().await;
            let mut start = request(vec![1]);
            start.workflow_execution_timeout = Some(Duration::milliseconds(timeout_ms));
            start.workflow_run_timeout = Some(Duration::milliseconds(timeout_ms));
            if !policy { start.retry_policy = None; }
            start.cron_schedule = cron.then(|| "@every 1s".into());
            runtime.start_workflow(start.clone()).await.unwrap();
            timeout_at(&runtime, &start, start.now + Duration::milliseconds(timeout_ms + lag_ms), WorkflowTimeoutViolation::ExecutionTimeout).await;
            prop_assert!(successor(&runtime, &start).await.is_none());
            prop_assert_eq!(runtime.repo.find_latest_run(start.namespace_id, &start.workflow_id).await.unwrap(), Some(start.run_key));
            prop_assert_eq!(runtime.repo.list_runs_for_namespace(start.namespace_id).await.unwrap(), vec![start.run_key]);
            let LoadedRun::Existing(state) = runtime.repo.load_run(start.run_key).await.unwrap() else { panic!("closed run") };
            prop_assert_eq!(state.status, tokeira_types::ExecutionStatus::TimedOut);
            Ok(())
        })?;
    }

    // Feature: successor-semantics, Property 7: continue-as-new uses command metadata and original links.
    #[test]
    fn continue_as_new_uses_command_metadata(age_ms in 1i64..2000, bytes in prop::collection::vec(any::<u8>(), 0..48), absent in any::<bool>()) {
        executor().block_on(async {
            let runtime = runtime().await;
            let start = request(vec![0]);
            runtime.start_workflow(start.clone()).await.unwrap();
            let mut command_start = request(bytes);
            command_start.search_attributes = SearchAttributes([("search".into(), SearchAttrValue::Int(23))].into());
            if absent { command_start.header = None; command_start.memo = Memo::default(); command_start.search_attributes = SearchAttributes::default(); }
            let mut completion = completion(&runtime, &start).await;
            completion.now = start.now + Duration::milliseconds(age_ms);
            completion.commands = vec![WorkflowCommand::UpsertMemo(Memo([("memo".into(), Payload::new(b"current"))].into())), WorkflowCommand::UpsertSearchAttributes(SearchAttributes([("search".into(), SearchAttrValue::Int(99))].into())), continue_command(&command_start)];
            runtime.complete_workflow_task(completion).await.unwrap();
            let (state, event) = successor(&runtime, &start).await.unwrap();
            let actual = started_event_inputs(&event).unwrap();
            prop_assert_eq!(actual.header, command_start.header);
            prop_assert_eq!(actual.memo, command_start.memo);
            prop_assert_eq!(actual.search_attributes, command_start.search_attributes);
            prop_assert_eq!(actual.input, command_start.input);
            prop_assert_eq!(actual.links, start.links);
            prop_assert_eq!(state.workflow_start_delay, (age_ms < 1000).then(|| Duration::milliseconds(1000 - age_ms)));
            prop_assert!(state.started_at > start.now + Duration::milliseconds(age_ms));
            Ok(())
        })?;
    }

    // Feature: successor-semantics, exploration: original start metadata survives retry.
    #[test]
    fn exploration_original_metadata(bytes in prop::collection::vec(any::<u8>(), 1..8)) {
        let (expected, actual) = executor().block_on(async {
            let runtime = runtime().await;
            let request = request(bytes);
            runtime.start_workflow(request.clone()).await.unwrap();
            runtime.complete_workflow_task(completion(&runtime, &request).await).await.unwrap();
            let (_, event) = successor(&runtime, &request).await.unwrap();
            let metadata = match event.kind {
                HistoryEventKind::WorkflowExecutionStarted { header, links, .. }
                | HistoryEventKind::WorkflowExecutionStartedV2 { header, links, .. } => (header, links),
                _ => panic!("started event expected"),
            };
            ((request.header, request.links), metadata)
        });
        prop_assert_eq!(actual, expected);
    }

    // Feature: successor-semantics, exploration: a retry retains its cron schedule.
    #[test]
    fn exploration_retry_preserves_cron(minutes in 1u8..10) {
        let (actual, expected) = executor().block_on(async {
            let runtime = runtime().await;
            let mut request = request(vec![1]);
            request.cron_schedule = Some(format!("*/{minutes} * * * *"));
            runtime.start_workflow(request.clone()).await.unwrap();
            runtime.complete_workflow_task(completion(&runtime, &request).await).await.unwrap();
            let (_, event) = successor(&runtime, &request).await.unwrap();
            let cron = match event.kind {
                HistoryEventKind::WorkflowExecutionStarted { cron_schedule, .. }
                | HistoryEventKind::WorkflowExecutionStartedV2 { cron_schedule, .. } => cron_schedule,
                _ => panic!("started event expected"),
            };
            (cron, request.cron_schedule)
        });
        prop_assert_eq!(actual, expected);
    }

    // Feature: successor-semantics, exploration: a timeout retry carries its error and prior result.
    #[test]
    fn exploration_timeout_outcome(bytes in prop::collection::vec(any::<u8>(), 1..8)) {
        let (actual, expected) = executor().block_on(async {
            let runtime = runtime().await;
            let mut request = request(vec![1]);
            request.workflow_run_timeout = Some(Duration::seconds(5));
            request.last_completion_result = Some(Payloads(vec![Payload::new(bytes)]));
            runtime.start_workflow(request.clone()).await.unwrap();
            let entry = runtime.workflow_timeout_tracking.snapshot().into_iter().find(|e| e.run_key == request.run_key).unwrap();
            submit_workflow_timeout(runtime.repo.clone(), runtime.lanes.clone(), runtime.lanes.len(), entry,
                WorkflowTimeoutViolation::RunTimeout, request.now + Duration::seconds(5)).await.unwrap();
            let (_, event) = successor(&runtime, &request).await.unwrap();
            let outcome = match event.kind {
                HistoryEventKind::WorkflowExecutionStarted { continued_failure, last_completion_result, .. }
                | HistoryEventKind::WorkflowExecutionStartedV2 { continued_failure, last_completion_result, .. } => (continued_failure, last_completion_result),
                _ => panic!("started event expected"),
            };
            (outcome, (Some(workflow_task::workflow_run_timeout_failure()), request.last_completion_result))
        });
        prop_assert_eq!(actual, expected);
    }

    // Feature: successor-semantics, exploration: declining retry does not end an eligible cron chain.
    #[test]
    fn exploration_declined_retry_resumes_cron(minutes in 1u8..10) {
        let has_successor = executor().block_on(async {
            let runtime = runtime().await;
            let mut request = request(vec![1]);
            request.retry_policy.as_mut().unwrap().maximum_attempts = 1;
            request.cron_schedule = Some(format!("*/{minutes} * * * *"));
            runtime.start_workflow(request.clone()).await.unwrap();
            runtime.complete_workflow_task(completion(&runtime, &request).await).await.unwrap();
            successor(&runtime, &request).await.is_some()
        });
        prop_assert!(has_successor, "retry exhaustion must resume cron");
    }

    // Feature: successor-semantics, exploration: rapid retries respect the minimum execution interval.
    #[test]
    fn exploration_minimum_delay(age_ms in 1i64..400, backoff_ms in 1i64..400) {
        let delay = executor().block_on(async {
            let runtime = runtime().await;
            let mut request = request(vec![1]);
            request.retry_policy.as_mut().unwrap().initial_interval = Duration::milliseconds(backoff_ms);
            runtime.start_workflow(request.clone()).await.unwrap();
            let mut completion = completion(&runtime, &request).await;
            completion.now = request.now + Duration::milliseconds(age_ms);
            runtime.complete_workflow_task(completion).await.unwrap();
            successor(&runtime, &request).await.unwrap().0.workflow_start_delay
        });
        prop_assert_eq!(delay, Some(Duration::milliseconds(1000 - age_ms)));
    }

    // Feature: successor-semantics, exploration: an exact cron tick still observes the minimum interval.
    #[test]
    fn exploration_cron_minimum_delay(age_ms in 1i64..1000) {
        let delay = executor().block_on(async {
            let runtime = runtime().await;
            let mut request = request(vec![1]);
            request.now = clock() + Duration::seconds(1) - Duration::milliseconds(age_ms);
            request.request.received_at = request.now;
            request.retry_policy = None;
            request.cron_schedule = Some("@every 1s".into());
            runtime.start_workflow(request.clone()).await.unwrap();
            let mut completion = completion(&runtime, &request).await;
            completion.now = clock() + Duration::seconds(1);
            completion.commands = vec![WorkflowCommand::CompleteWorkflow { result: Payloads::default() }];
            runtime.complete_workflow_task(completion).await.unwrap();
            successor(&runtime, &request).await.unwrap().0.workflow_start_delay
        });
        prop_assert_eq!(delay, Some(Duration::milliseconds(1000 - age_ms)));
    }
}
