//! Generated preservation checks exercise delivery and reconstruction against committed memory state.

use super::*;
use proptest::prelude::*;
use tokeira_kernel::{Priority, StickySpec, UpdateProtocolBody, WorkflowCommand, WorkflowTaskType};
use tokeira_storage::{InMemoryStore, WorkflowDiscoveryRange, WorkflowDispatchRouting};
use tokeira_types::{TaskQueueName, WorkflowId};

#[derive(Debug)]
struct FixedClock(OffsetDateTime);
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.0
    }
}

fn fixture() -> (
    Arc<InMemoryStore>,
    Arc<TokeiraRuntime<InMemoryStore>>,
    Arc<dyn Clock>,
) {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(
        OffsetDateTime::from_unix_timestamp(2_000_000_000).unwrap(),
    ));
    let store = Arc::new(InMemoryStore::default().with_clock(clock.clone()));
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        1,
        "preservation".into(),
        "127.0.0.1:0".into(),
        true,
        None,
        clock.clone(),
    ));
    (store, runtime, clock)
}
fn request(clock: &dyn Clock, index: usize) -> StartRequest {
    let mut request = tests::sample_start_request(None, None);
    request.namespace_id = NamespaceId(Uuid::from_u128(17));
    request.workflow_id = WorkflowId(format!("preservation-{index}"));
    request.run_id = RunId(Uuid::from_u128(index as u128 + 1));
    request.run_key = RunKey::derive(request.namespace_id, &request.workflow_id, request.run_id);
    request.now = clock.now();
    request.request = RequestContext {
        request_id: RequestId(format!("start-{index}")),
        ..RequestContext::unattributed(clock.now())
    };
    request
}
fn queue(request: &StartRequest) -> QueueKey {
    QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(concat!(env!("CARGO_MANIFEST_DIR"), "/src/runtime/queue_home_preservation_tests.proptest-regressions")))), ..ProptestConfig::default() })]
    // Feature: queue-home-delivery, Property 9: local normal delivery preserves priority and commits each task exactly once.
    #[test]
    fn property_9_local_delivery_priority(priorities in prop::collection::vec(1i32..6, 2..8)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (store, runtime, clock) = fixture();
            let q = queue(&request(clock.as_ref(), 0));
            for (index, priority) in priorities.iter().enumerate() {
                let mut req = request(clock.as_ref(), index);
                req.priority = Some(Priority { priority_key: *priority, fairness_key: String::new(), fairness_weight: 1.0 });
                runtime.start_workflow(req).await.unwrap();
            }
            let mut expected = priorities.clone(); expected.sort();
            for priority in expected {
                let started = runtime.poll_workflow_task(q.clone(), WorkerIdentity("worker".into()), std::time::Duration::ZERO).await.unwrap().unwrap();
                let LoadedRun::Existing(state) = store.load_run(started.run_key).await.unwrap() else { panic!("run missing") };
                assert_eq!(state.priority.unwrap().priority_key, priority);
                let history = store.read_history_to_end(started.run_key, 0).await.unwrap();
                assert_eq!(history.iter().filter(|e| matches!(e.kind, HistoryEventKind::WorkflowTaskStarted { .. })).count(), 1);
            }
            assert!(runtime.broker.poll_workflow_task(&q, &WorkerIdentity("worker".into()), std::time::Duration::ZERO).await.unwrap().is_none());
            discovery_tests::stop(&runtime).await;
        });
    }

    // Feature: queue-home-delivery, Property 9: eager and reserved starts suppress duplicate offers and retain the committed timeout.
    #[test]
    fn property_9_eager_and_reserved(eager in any::<bool>(), index in 0usize..1000, priority in 1i32..6) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (store, runtime, clock) = fixture(); let mut req = request(clock.as_ref(), index); let q = queue(&req); let key = req.run_key;
            req.priority = Some(Priority { priority_key: priority, fairness_key: String::new(), fairness_weight: 1.0 });
            req.eager_execution_accepted = eager; req.request.caller_identity = Some("worker".into());
            let poll = if eager { None } else {
                let r = runtime.clone(); let q = q.clone();
                let poll = tokio::spawn(async move { r.poll_workflow_task(q, WorkerIdentity("worker".into()), std::time::Duration::from_secs(3)).await.unwrap().unwrap() });
                while !runtime.broker.queues_with_waiters().await.contains(&queue(&req)) { tokio::task::yield_now().await; }
                Some(poll)
            };
            let result = runtime.start_workflow_with_policy(req).await.unwrap();
            let started = if let Some(poll) = poll { poll.await.unwrap() } else {
                let StartWorkflowResult::Started { eager_workflow_task: Some(started), .. } = result else { panic!("eager result") }; started
            };
            assert_eq!(started.run_key, key);
            assert!(runtime.broker.poll_workflow_task(&q, &WorkerIdentity("other".into()), std::time::Duration::ZERO).await.unwrap().is_none());
            let history = store.read_history_to_end(key, 0).await.unwrap();
            assert_eq!(history.iter().filter(|e| matches!(e.kind, HistoryEventKind::WorkflowTaskStarted { .. })).count(), 1);
            assert_eq!(history[0].kind.eager_execution_accepted(), eager);
            let tracking = runtime.wft_timeout_tracking.snapshot();
            assert_eq!(tracking.len(), 1); assert_eq!(tracking[0].started_event_id, started.token.started_event_id);
            discovery_tests::stop(&runtime).await;
        });
    }

    // Feature: queue-home-delivery, Property 9: speculative update delivery remains virtual and rejection preserves the prior history barrier.
    #[test]
    fn property_9_speculative_updates(index in 0usize..1000, payload in prop::collection::vec(any::<u8>(), 0..128)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (store, runtime, clock) = fixture(); let req = request(clock.as_ref(), index); let q = queue(&req); let key = req.run_key;
            runtime.start_workflow(req).await.unwrap();
            let first = runtime.poll_workflow_task(q.clone(), WorkerIdentity("worker".into()), std::time::Duration::ZERO).await.unwrap().unwrap();
            let update_id = format!("update-{index}");
            runtime.update_workflow(ExecutionRef { namespace_id: q.namespace_id, workflow_id: first.workflow_id.clone(), run_id: Some(first.run_id) }, update_id.clone(), "handler".into(), Payloads(vec![tokeira_types::Payload::new(payload.clone())]), RequestContext { request_id: RequestId(format!("admit-{index}")), ..RequestContext::unattributed(clock.now()) }, Duration::seconds(1), UpdateWaitPolicy::Admitted, 0).await.unwrap();
            let mut completion = completion_inline_tests::completion(&first, true); completion.now = clock.now(); completion.request = RequestContext::unattributed(clock.now());
            let outcome = runtime.complete_workflow_task_with_result(completion).await.unwrap();
            let inline = outcome.workflow_task.unwrap();
            let CommitResult::Applied { new_state } = outcome.commit else { panic!("commit") };
            assert_eq!(new_state.pending_workflow_task.as_ref().unwrap().task_type, WorkflowTaskType::Speculative);
            let transports = runtime.pending_update_transports(key, false); assert_eq!(transports.len(), 1); assert_eq!(transports[0].input.0[0].data, payload);
            let page = store.list_workflow_dispatch_page(&WorkflowDiscoveryRange { namespace_id: q.namespace_id, queue_name: q.task_queue.clone(), routing: WorkflowDispatchRouting::Live }, None, std::num::NonZeroU32::new(64).unwrap()).await.unwrap();
            assert!(page.candidates.is_empty());
            assert!(store.list_sticky_workflow_dispatch(ShardId(0), None, std::num::NonZeroU32::new(64).unwrap()).await.unwrap().candidates.is_empty());
            let mut reject = completion_inline_tests::completion(&inline, true); reject.now = clock.now(); reject.request = RequestContext::unattributed(clock.now());
            reject.commands.push(WorkflowCommand::ProtocolMessage { message_id: "reject".into(), body: UpdateProtocolBody::Rejected { update_id, failure: tokeira_types::Payload::new(Vec::new()) } });
            let rejected = runtime.complete_workflow_task_with_result(reject).await.unwrap();
            let CommitResult::Applied { new_state: after } = rejected.commit else { panic!("commit") };
            assert_eq!(after.last_event_id, new_state.last_event_id); assert_eq!(after.previous_started_event_id, first.token.started_event_id); assert!(after.pending_workflow_task.is_none());
            discovery_tests::stop(&runtime).await;
        });
    }

    // Feature: queue-home-delivery, Property 9: acquisition reconstructs the original sticky deadline and repairs derived intent without extending it.
    #[test]
    fn property_9_timeout_reconstruction_and_repair(index in 0usize..1000, seconds in 1i64..60, repetitions in 1usize..4) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let (store, runtime, clock) = fixture(); let req = request(clock.as_ref(), index); let q = queue(&req); let key = req.run_key;
            runtime.start_workflow(req).await.unwrap();
            let first = runtime.poll_workflow_task(q.clone(), WorkerIdentity("worker".into()), std::time::Duration::ZERO).await.unwrap().unwrap();
            let mut completion = completion_inline_tests::completion(&first, false); completion.now = clock.now(); completion.request = RequestContext::unattributed(clock.now());
            completion.force_new_workflow_task = true; completion.sticky = Some(StickySpec { queue: TaskQueueName("sticky".into()), schedule_to_start_timeout: Duration::seconds(seconds) });
            runtime.complete_workflow_task_with_result(completion).await.unwrap();
            let LoadedRun::Existing(before) = store.load_run(key).await.unwrap() else { panic!("state") };
            let deadline = before.pending_workflow_task.as_ref().unwrap().schedule_to_start_deadline.unwrap();
            for _ in 0..repetitions {
                runtime.relinquish_shard(ShardId(0)).await;
                runtime.recover_self_assigned_shard(ShardId(0), ShardEpoch::ZERO).await.unwrap();
                let entry = runtime.wft_timeout_tracking.snapshot().into_iter().find(|e| e.run_key == key).unwrap();
                assert_eq!(entry.kind, crate::wft_timeout::WftTimeoutKind::ScheduleToStart);
                assert_eq!(entry.started_at + entry.workflow_task_timeout, deadline);
                assert_eq!(store.load_run(key).await.unwrap(), LoadedRun::Existing(before.clone()));
                let page = store.list_sticky_workflow_dispatch(ShardId(0), None, std::num::NonZeroU32::new(64).unwrap()).await.unwrap();
                assert_eq!(page.candidates.len(), 1);
                assert_eq!(before.pending_workflow_task.as_ref().unwrap().schedule_to_start_deadline, Some(deadline));
            }
            discovery_tests::stop(&runtime).await;
        });
    }
}
