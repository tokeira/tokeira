//! Persisted successors retain their original metadata and cause-specific outcomes on the Temporal wire.
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use prost::Message as _;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokeira_edge::translate::history_serializer::history_event_to_proto;
use tokeira_kernel::{
    BasicKernel, Command, HistoryEventKind, Kernel, Link, LoadedRun, StartRequest, WorkflowCommand,
    WorkflowTaskCompletedRequest,
};
use tokeira_proto::{
    conversions::common::{
        headers_from_domain, memo_from_domain, payloads_from_domain, search_attributes_from_domain,
    },
    failure::{ApplicationFailureInfo, Failure, TimeoutFailureInfo, failure::FailureInfo},
    history::history_event::Attributes,
};
use tokeira_runtime::{RuntimeConfig, TokeiraRuntime, timeout::WorkflowTimeoutEntry};
use tokeira_storage::{InMemoryStore, RunRepository, WriteFence};
use tokeira_types::{
    Headers, Memo, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId,
    RetryPolicy, RunKey, SearchAttrValue, SearchAttributes, ShardId, TaskKind, TaskQueueName,
    WorkerIdentity, WorkflowId,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: successor-semantics, Properties 1/2: real runtime results survive storage and edge serialization.
    #[test]
    fn successor_history_reaches_wire(kind in 0u8..5, legacy in any::<bool>(), prior in 0u8..3, empty in any::<bool>(), bytes in prop::collection::vec(any::<u8>(), 1..32)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let mut start = start_request(None, (kind >= 3).then(|| Duration::milliseconds(1)));
            start.now = OffsetDateTime::now_utc() - Duration::seconds(10);
            start.request.received_at = start.now;
            start.input = Payloads(vec![Payload::new(bytes.clone())]);
            start.header = Some(Headers([("header".into(), Payload::new(bytes.clone()))].into()));
            start.memo = Memo([("memo".into(), Payload::new(bytes.clone()))].into());
            start.search_attributes = SearchAttributes([("search".into(), SearchAttrValue::Int(7))].into());
            start.links = vec![Link::BatchJob { job_id: "original".into() }];
            start.last_completion_result = match prior { 0 => None, 1 => Some(Payloads::default()), _ => Some(Payloads(vec![Payload::new(bytes)])) };
            start.cron_schedule = Some("@every 2s".into());
            start.retry_policy = (kind == 0 || kind == 3).then(|| RetryPolicy { initial_interval: Duration::seconds(1), backoff_coefficient: 1.0, maximum_interval: None, maximum_attempts: 2, non_retryable_error_types: Vec::new() });
            let mut initial = BasicKernel.apply(LoadedRun::Absent, Command::Start(start.clone())).unwrap();
            if legacy {
                let mut value = serde_json::to_value(&initial.history_events[0].kind).unwrap();
                let mut fields = value.as_object_mut().unwrap().remove("WorkflowExecutionStartedV2").unwrap();
                fields.as_object_mut().unwrap().remove("eager_execution_accepted");
                initial.history_events[0].kind = serde_json::from_value(serde_json::json!({"WorkflowExecutionStarted": fields})).unwrap();
            }
            let original_links = history_event_to_proto(&initial.history_events[0]).links;
            repo.commit_transition(start.run_key, initial, WriteFence::Unfenced).await.unwrap();
            let mut runtime = TokeiraRuntime::new_with_config(repo.clone(), RuntimeConfig::default());
            runtime.shutdown_timer_scanner().await.unwrap();
            runtime.shutdown_wft_timeout_scanner().await.unwrap();
            let failure = Failure { message: "failed attempt".into(), failure_info: Some(FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo { r#type: "retryable".into(), ..Default::default() })), ..Default::default() };
            let result = if empty { Payloads::default() } else { Payloads(vec![Payload::new(b"new result")]) };
            if kind >= 3 {
                runtime.workflow_timeout_tracking().insert(WorkflowTimeoutEntry { run_key: start.run_key, shard_id: ShardId(0), workflow_execution_timeout: None, workflow_run_timeout: start.workflow_run_timeout, started_at: start.now, workflow_start_delay: None, first_run_started_at: None, has_retry_policy: start.retry_policy.is_some() });
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
                let mut closed = false;
                for _ in 0..10000 {
                    if matches!(repo.load_run(start.run_key).await.unwrap(), LoadedRun::Existing(state) if !state.status.is_open()) { closed = true; break; }
                    tokio::task::yield_now().await;
                }
                assert!(closed, "scanner must commit the observed timeout");
                runtime.shutdown_workflow_timeout_scanner().await.unwrap();
            } else {
                runtime.shutdown_workflow_timeout_scanner().await.unwrap();
                let queue = QueueKey { namespace_id: start.namespace_id, task_queue: start.task_queue.clone(), task_kind: TaskKind::Workflow, deployment: None, build_id: None };
                runtime.republish_queue(queue.clone(), 64).await.unwrap();
                let task = runtime.poll_workflow_task(queue, WorkerIdentity("worker".into()), std::time::Duration::ZERO).await.unwrap().unwrap();
                runtime.complete_workflow_task(WorkflowTaskCompletedRequest {
                    client_discards_speculative_with_events: false, token: task.token, identity: WorkerIdentity("worker".into()), sdk_metadata: None, metering_metadata: None,
                    worker_version: None, versioning_behavior: Default::default(), deployment_version: None, worker_deployment_name: None, sticky: None,
                    commands: vec![WorkflowCommand::UpsertMemo(Memo([("memo".into(), Payload::new(b"changed"))].into())),
                        if kind == 1 { WorkflowCommand::CompleteWorkflow { result: result.clone() } } else { WorkflowCommand::FailWorkflow { failure: Payload::new(failure.encode_to_vec()) } }],
                    force_new_workflow_task: false, limits: Default::default(), delivered_update_ids: Vec::new(), request: RequestContext::unattributed(start.now + Duration::seconds(1)), now: start.now + Duration::seconds(1), command_sizes: Vec::new(),
                }).await.unwrap();
            }
            let history = repo.read_history(start.run_key, 0, 64).await.unwrap();
            let successor_id = history.iter().find_map(|event| match event.kind {
                HistoryEventKind::WorkflowExecutionFailed { new_execution_run_id, .. } | HistoryEventKind::WorkflowExecutionCompleted { new_execution_run_id, .. } | HistoryEventKind::WorkflowExecutionTimedOut { new_execution_run_id, .. } => new_execution_run_id,
                _ => None,
            }).unwrap();
            let key = RunKey::derive(start.namespace_id, &start.workflow_id, successor_id);
            let stored = repo.read_history_event(key, 1).await.unwrap().unwrap();
            let wire = history_event_to_proto(&stored);
            prop_assert!(wire.task_id > history_event_to_proto(history.last().unwrap()).task_id, "successor task IDs must increase across the close/start pair");
            prop_assert_eq!(wire.links, original_links);
            let Some(Attributes::WorkflowExecutionStartedEventAttributes(attrs)) = wire.attributes else { panic!("wire started event") };
            prop_assert_eq!(attrs.input, Some(payloads_from_domain(&start.input)));
            prop_assert_eq!(attrs.header, start.header.as_ref().map(headers_from_domain));
            prop_assert_eq!(attrs.memo, Some(memo_from_domain(&start.memo)));
            prop_assert_eq!(attrs.search_attributes, Some(search_attributes_from_domain(&start.search_attributes)));
            prop_assert_eq!(attrs.cron_schedule, "@every 2s");
            let expected_failure = match kind { 1 => None, 3 | 4 => Some(Failure { message: "workflow timeout".into(), failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo { timeout_type: tokeira_proto::enums::TimeoutType::StartToClose as i32, ..Default::default() })), ..Default::default() }), _ => Some(failure) };
            prop_assert_eq!(attrs.continued_failure, expected_failure);
            let expected_result = match kind { 0 => None, 1 => Some(result), _ => start.last_completion_result };
            prop_assert_eq!(attrs.last_completion_result, expected_result.as_ref().map(payloads_from_domain));
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn continue_as_new_task_ids_increase_across_runs() {
    let repo = Arc::new(InMemoryStore::default());
    let mut runtime = TokeiraRuntime::new_with_config(repo.clone(), RuntimeConfig::default());
    runtime.shutdown_timer_scanner().await.unwrap();
    runtime.shutdown_wft_timeout_scanner().await.unwrap();
    runtime.shutdown_workflow_timeout_scanner().await.unwrap();
    let mut start = start_request(None, None);
    start.now = OffsetDateTime::now_utc();
    start.request.received_at = start.now;
    runtime.start_workflow(start.clone()).await.unwrap();
    let queue = QueueKey {
        namespace_id: start.namespace_id,
        task_queue: start.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let task = runtime
        .poll_workflow_task(
            queue,
            WorkerIdentity("worker".into()),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap()
        .unwrap();
    let successor_id = tokeira_types::RunId::new();
    let now = OffsetDateTime::now_utc();
    runtime
        .complete_workflow_task(WorkflowTaskCompletedRequest {
            client_discards_speculative_with_events: false,
            token: task.token,
            identity: WorkerIdentity("worker".into()),
            sdk_metadata: None,
            metering_metadata: None,
            worker_version: None,
            versioning_behavior: Default::default(),
            deployment_version: None,
            worker_deployment_name: None,
            sticky: None,
            commands: vec![WorkflowCommand::ContinueAsNew {
                new_run_id: successor_id,
                workflow_type: start.workflow_type.clone(),
                task_queue: start.task_queue.clone(),
                input: start.input.clone(),
                memo: start.memo.clone(),
                search_attributes: start.search_attributes.clone(),
                workflow_execution_timeout: None,
                workflow_run_timeout: None,
                workflow_task_timeout: start.workflow_task_timeout,
                retry_policy: None,
                header: None,
                initial_versioning_behavior: Default::default(),
                successor_versioning_info: None,
            }],
            force_new_workflow_task: false,
            limits: Default::default(),
            delivered_update_ids: Vec::new(),
            request: RequestContext::unattributed(now),
            now,
            command_sizes: Vec::new(),
        })
        .await
        .unwrap();
    let history = repo.read_history(start.run_key, 0, 64).await.unwrap();
    let close = history_event_to_proto(history.last().unwrap());
    let key = RunKey::derive(start.namespace_id, &start.workflow_id, successor_id);
    let first = history_event_to_proto(&repo.read_history_event(key, 1).await.unwrap().unwrap());
    // A close/start pair must preserve the public ordering checked by
    // tests/continue_as_new_test.go:TestWorkflowContinueAsNewTaskID @ v1.31.0.
    assert!(first.task_id > close.task_id);
}

fn start_request(
    workflow_execution_timeout: Option<Duration>,
    workflow_run_timeout: Option<Duration>,
) -> StartRequest {
    let run_id = tokeira_types::RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::new(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId("workflow-timeout".to_string()),
        run_id,
        workflow_type: tokeira_types::WorkflowType("example".to_string()),
        task_queue: TaskQueueName("workflow-q".to_string()),
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
        input: Payloads::default(),
        header: None,
        memo: Memo::default(),
        search_attributes: SearchAttributes::default(),
        workflow_execution_timeout,
        workflow_run_timeout,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
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
            request_id: RequestId("req-timeout".to_string()),
            caller_identity: None,
            principal: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}
