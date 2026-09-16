//! Queue admission and legacy-dispatch reference models for extension executors.

use super::*;
use proptest::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use tokeira_chasm::{DispatchableTask, ScheduledTask, Task, TaskId, TaskKind};
use tokeira_chasm_activity::{DISPATCH_TASK_ID, DispatchTask};
use tokeira_runtime::chasm::{
    ChasmTimerSweeper, DispatchSink, OutboxRebuildScanner, SideEffectExecutor,
};

#[test]
fn untargeted_task_token_preserves_v1_31_bytes() {
    let bytes = ActivityTaskToken::encode("a", "b", "c", 2, 7, None).unwrap();
    assert_eq!(
        bytes,
        b"\x0a\x01a\x1a\x01c\x28\x02\x32\x01b\x72\x0b\x0a\x01a\x12\x01b\x1a\x01c\x20\x07"
    );
    assert!(
        ActivityTaskToken::decode(&bytes)
            .unwrap()
            .version_target
            .is_none()
    );
    let version = target(Some(2));
    let targeted = ActivityTaskToken::encode("a", "b", "c", 2, 7, version.clone()).unwrap();
    assert!(targeted.starts_with(&bytes));
    assert_eq!(
        ActivityTaskToken::decode(&targeted).unwrap().version_target,
        version
    );
}

#[test]
fn evaluator_deadline_includes_running_attempt_and_callback_retry() {
    let mut state = ActivityState {
        status: ActivityStatus::Started as i32,
        started_time_nanos: 100,
        start_to_close_nanos: 20,
        ..Default::default()
    };
    assert_eq!(activity_and_callback_deadline(&state), Some(120));
    state
        .callbacks
        .push(tokeira_chasm_activity::ActivityCallback {
            state: tokeira_proto::enums::CallbackState::BackingOff as i32,
            next_attempt_time_nanos: 110,
            ..Default::default()
        });
    assert_eq!(activity_and_callback_deadline(&state), Some(110));
    state.callbacks[0].next_attempt_time_nanos = 130;
    assert_eq!(activity_and_callback_deadline(&state), Some(120));
    state.status = ActivityStatus::Completed as i32;
    assert_eq!(activity_and_callback_deadline(&state), Some(130));
    state.callbacks.clear();
    assert_eq!(activity_and_callback_deadline(&state), None);
}

fn target(index: Option<u8>) -> Option<DeploymentVersionTarget> {
    index.map(|index| DeploymentVersionTarget {
        deployment_name: format!("deployment-{}", index / 2),
        build_id: format!("build-{index}"),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 12: versioned admission
    // Every pickup matches exact admission and leaves all unmatched entries intact.
    #[test]
    fn queue_admission_matches_reference_model(
        entries in prop::collection::vec((prop::option::of(0u8..4), 0i64..20), 0..35),
        polls in prop::collection::vec((prop::option::of(0u8..4), 0i64..25), 1..45),
    ) {
        let queue = ActivityDispatchQueue::new();
        let mut model = Vec::new();
        for (index, (version, due)) in entries.into_iter().enumerate() {
            let entry = DispatchEntry { key: ExecutionKey::new("ns", index.to_string(), "run"), stamp: 1, fire_at: Some(due), target: target(version) };
            model.push(entry.clone());
            queue.enqueue("q".into(), entry);
        }
        for (version, now) in polls {
            let admitted = target(version);
            let expected = model.iter().position(|entry| entry.target == admitted && entry.fire_at.unwrap() <= now).map(|position| model.remove(position));
            let actual = queue.dequeue_due("q", now, admitted.as_ref());
            prop_assert_eq!(actual.as_ref().map(|entry| (&entry.key, &entry.target)), expected.as_ref().map(|entry| (&entry.key, &entry.target)));
            prop_assert_eq!(queue.next_due_at("q", admitted.as_ref()), model.iter().filter(|entry| entry.target == admitted).filter_map(|entry| entry.fire_at).min());
        }
    }
}

// The deleted sink's task-1-only interpretation is deliberately preserved here.
// Reference dispatch never reads root state or routes through an executor.
fn legacy_dispatch(queue: &ActivityDispatchQueue, key: ExecutionKey, task: DispatchableTask) {
    if task.task.task_type_id == DISPATCH_TASK_ID {
        let dispatch = DispatchTask::decode(&task.task.payload).unwrap();
        queue.enqueue(
            dispatch.task_queue,
            DispatchEntry {
                key,
                stamp: dispatch.stamp,
                fire_at: task.task.fire_at_unix_nanos,
                target: None,
            },
        );
    }
}

struct Fixture {
    bridge: Arc<ActivityBridge>,
    engine: Arc<ChasmEngine>,
    clock: Arc<AtomicI64>,
    queue: Arc<ActivityDispatchQueue>,
    collected: Option<Arc<CollectingDispatchSink>>,
    rebuild: OutboxRebuildScanner,
}

impl Fixture {
    fn new(reference: bool) -> Self {
        let mut builder = Registry::builder();
        ActivityLibrary::register(&mut builder).unwrap();
        let nodes = Arc::new(InMemoryChasmNodeStore::new());
        let mux = Arc::new(DispatchMultiplexer::default());
        let collected = reference.then(|| Arc::new(CollectingDispatchSink::default()));
        let sink: Arc<dyn DispatchSink> = match &collected {
            Some(sink) => sink.clone(),
            None => mux.clone(),
        };
        let clock = Arc::new(AtomicI64::new(100 * SEC));
        let read_clock = clock.clone();
        let engine = Arc::new(
            ChasmEngine::new(
                nodes.clone(),
                Arc::new(builder.build()),
                sink.clone(),
                Arc::new(CollectingVisibilitySink::default()),
            )
            .with_clock(Arc::new(move || read_clock.load(Ordering::SeqCst))),
        );
        let queue = Arc::new(ActivityDispatchQueue::new());
        let executor = Arc::new(ActivityDispatchExecutor::new(
            Arc::downgrade(&engine),
            queue.clone(),
        ));
        mux.register(executor.clone()).unwrap();
        let bridge = Arc::new(
            ActivityBridge::new(
                engine.clone(),
                ActivityConfig {
                    enable_standalone: true,
                    ..Default::default()
                },
                1000,
            )
            .with_dispatch_executor(executor),
        );
        Self {
            bridge,
            engine: engine.clone(),
            clock,
            queue,
            collected,
            rebuild: OutboxRebuildScanner::new(nodes, engine, sink),
        }
    }

    fn drain(&self) {
        if let Some(collected) = &self.collected {
            for (key, task) in std::mem::take(&mut *collected.dispatched.lock().unwrap()) {
                legacy_dispatch(&self.queue, key, task);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 7: activity dispatch equivalence
    // Executor and legacy task-1 delivery produce identical tasks and descriptions.
    #[test]
    fn dispatch_matches_legacy_lifecycle_scripts(script in prop::collection::vec((0u8..8, 0i64..8), 1..35), rebuild_at in 0usize..35) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let fixtures = [Fixture::new(false), Fixture::new(true)];
            let mut request = start_request();
            request.namespace_id = "namespace".into(); request.activity_id = "activity".into(); request.run_id = "run".into();
            request.schedule_to_start_nanos = 5 * SEC; request.schedule_to_close_nanos = 120 * SEC;
            request.heartbeat_nanos = 3 * SEC; request.maximum_attempts = 3;
            let key = ExecutionKey::new(&request.namespace_id, &request.activity_id, &request.run_id);
            for fixture in &fixtures { fixture.bridge.start(request.clone()).await.unwrap(); fixture.drain(); }
            let mut tokens: [Option<Vec<u8>>; 2] = [None, None];
            for (index, (action, advance)) in script.iter().copied().enumerate() {
                let mut served = Vec::new();
                for (side, fixture) in fixtures.iter().enumerate() {
                    fixture.clock.fetch_add(advance * SEC, Ordering::SeqCst);
                    if index == rebuild_at % script.len() { fixture.rebuild.rebuild_once().await.unwrap(); fixture.drain(); }
                    let state = fixture.bridge.describe(key.clone()).await.unwrap();
                    match action {
                        0 => { fixture.bridge.start(request.clone()).await.unwrap(); }
                        1 => {
                            let task = fixture.bridge.poll_activity_task(&request.task_queue, "worker", None).await.unwrap();
                            if let Some(task) = &task { tokens[side] = Some(task.task_token.clone()); }
                            served.push(task);
                        }
                        2 if matches!(state.status, ActivityStatus::Started | ActivityStatus::CancelRequested) => {
                            fixture.bridge.record_heartbeat(tokens[side].as_ref().unwrap(), &request.namespace_id, vec![7]).await.unwrap();
                        }
                        3 if state.status == ActivityStatus::Started => {
                            let failure = tokeira_proto::failure::Failure { message: "retry".into(), failure_info: Some(tokeira_proto::failure::failure::FailureInfo::ApplicationFailureInfo(Default::default())), ..Default::default() }.encode_to_vec();
                            fixture.bridge.record_failed(key.clone(), "retry".into(), failure, vec![7], "worker".into()).await.unwrap();
                        }
                        4 if matches!(state.status, ActivityStatus::Started | ActivityStatus::CancelRequested) => { fixture.bridge.record_completed(key.clone(), vec![8], "worker".into()).await.unwrap(); }
                        5 if !state.status.is_terminal() => { fixture.bridge.request_cancel(key.clone(), "client".into(), "cancel".into(), "stop".into()).await.unwrap(); }
                        6 if !state.status.is_terminal() => { fixture.bridge.terminate(key.clone(), "stop".into(), "terminate".into(), "client".into()).await.unwrap(); }
                        7 => {
                            ChasmTimerSweeper::new(fixture.engine.clone()).with_evaluator(fixture.bridge.archetype_id, fixture.bridge.clone()).sweep_once().await;
                        }
                        _ => {},
                    }
                    fixture.drain();
                }
                if !served.is_empty() { prop_assert_eq!(&served[0], &served[1]); }
                prop_assert_eq!(fixtures[0].bridge.describe(key.clone()).await.unwrap(), fixtures[1].bridge.describe(key.clone()).await.unwrap());
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn executor_dedupes_before_and_after_pickup() {
    let fixture = Fixture::new(false);
    let request = start_request();
    let key = ExecutionKey::new(&request.namespace_id, &request.activity_id, &request.run_id);
    fixture.bridge.start(request.clone()).await.unwrap();
    let executor = fixture.bridge.dispatch_executor.as_ref().unwrap();
    let task = ScheduledTask {
        id: TaskId::new(VersionedTransition::new(0, 1), 0),
        task_type_id: DISPATCH_TASK_ID,
        kind: TaskKind::SideEffect,
        payload: DispatchTask {
            stamp: 1,
            task_queue: request.task_queue.clone(),
        }
        .encode()
        .unwrap(),
        fire_at_unix_nanos: None,
    };
    executor.execute(&key, &task).await.unwrap();
    executor.execute(&key, &task).await.unwrap();
    assert!(
        fixture
            .bridge
            .poll_activity_task(&request.task_queue, "worker", None)
            .await
            .unwrap()
            .is_some()
    );
    executor.execute(&key, &task).await.unwrap();
    assert!(
        fixture
            .bridge
            .poll_activity_task(&request.task_queue, "worker", None)
            .await
            .unwrap()
            .is_none()
    );
}
