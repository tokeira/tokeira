//! Runtime delivery contracts for the temporary construction-only discovery mode.

use super::*;
use crate::{
    discovery::{
        QueueHome, QueueHomeProvider,
        tests::{Homes, home, transition},
    },
    shard::shard_for,
};
use proptest::prelude::*;
use tokeira_storage::{InMemoryStore, WorkflowDiscoveryRange, WorkflowDispatchRouting};

fn runtime(store: Arc<InMemoryStore>, home: QueueHome) -> TokeiraRuntime<InMemoryStore> {
    let homes: Arc<dyn QueueHomeProvider> = Arc::new(Homes(Mutex::new(home)));
    let runtime = TokeiraRuntime::new_with_delivery(
        store,
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        "discovery-test".into(),
        "127.0.0.1:0".into(),
        false,
        None,
        Some(homes),
    );
    {
        let mut owner = runtime.shard_owner.write().unwrap();
        for shard in 0..8 {
            owner.record_acquired(ShardId(shard), ShardEpoch::ZERO);
            owner.mark_active(ShardId(shard));
        }
    }
    runtime
}

fn queue() -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("discovery-runtime".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch, Property 6: Volatile offer loss and ambiguity
    // Restart heals an unstarted offer, but an unobserved committed start cannot be consumed twice.
    #[test]
    fn lost_offers_and_replies_preserve_durable_intent(started in any::<bool>(), home_id in 0u32..8, duplicates in 1usize..5) {
        let executor = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        executor.block_on(async {
            let store = Arc::new(InMemoryStore::with_shard_count(8));
            let first = runtime(store.clone(), home(home_id));
            let queue = queue();
            let item = (0..100).map(|index| transition(&queue, index)).find(|item| {
                execution_home_bundle(queue.namespace_id.0.as_bytes(), item.next_state.workflow_id.0.as_bytes(), 8)
                    != shard_for(item.next_state.run_key, 8)
            }).unwrap();
            let expected_home = execution_home_bundle(queue.namespace_id.0.as_bytes(), item.next_state.workflow_id.0.as_bytes(), 8);
            let task = offer(&item);
            let initial_seq = item.next_state.transition_seq;
            store.commit_transition(task.run_key, item, ShardEpoch::ZERO).await.unwrap();
            for _ in 0..duplicates { first.broker.publish_workflow_task(task.clone(), None).await; }
            if started {
                // Drop the response just as a caller whose transport lost it would.
                assert!(first.try_claim_workflow_task(queue.clone(), None, task.run_key, WorkerIdentity("lost-reply".into())).await.unwrap().is_some());
                let entries = first.wft_timeout_tracking.snapshot();
                let entry = entries.iter().find(|entry| entry.run_key == task.run_key).unwrap();
                assert_eq!(entry.shard_id, expected_home);
                assert_eq!(entry.kind, crate::wft_timeout::WftTimeoutKind::StartToClose);
            } else {
                assert!(first.broker.try_claim_workflow_task(&queue, task.run_key).await.is_some());
            }
            stop(&first).await;
            drop(first);
            let second = runtime(store.clone(), home((home_id + 1) % 8));
            if started {
                second.broker.publish_workflow_task(task.clone(), None).await;
                assert!(second.try_claim_workflow_task(queue.clone(), None, task.run_key, WorkerIdentity("retry".into())).await.unwrap().is_none());
            } else {
                let result = second.poll_workflow_activation(queue.clone(), None, WorkerIdentity("recovered".into()), tokio::time::Duration::from_secs(30)).await.unwrap();
                assert!(matches!(result, Some(WorkflowActivation::WorkflowTask(_))));
            }
            let LoadedRun::Existing(state) = store.load_run(task.run_key).await.unwrap() else { panic!("run missing") };
            assert_eq!(state.transition_seq, initial_seq.next());
            assert!(store.list_workflow_dispatch_page(&WorkflowDiscoveryRange {
                namespace_id: queue.namespace_id, queue_name: queue.task_queue, routing: WorkflowDispatchRouting::Live,
            }, None, std::num::NonZeroU32::new(64).unwrap()).await.unwrap().candidates.is_empty());
            stop(&second).await;
        });
    }
}

#[tokio::test]
async fn sticky_poll_without_normal_name_never_registers_periodic_discovery() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let runtime = runtime(store, home(4));
    for normal_queue in [None, Some(queue())] {
        assert!(
            runtime
                .poll_workflow_activation_with_kind(
                    queue(),
                    normal_queue,
                    true,
                    WorkerIdentity("sticky".into()),
                    tokio::time::Duration::ZERO
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(runtime.discovery.as_ref().unwrap().0.registered_ranges(), 0);
    stop(&runtime).await;
}

async fn stop(runtime: &TokeiraRuntime<InMemoryStore>) {
    runtime.runtime_shutdown.begin_shutdown();
    runtime
        .runtime_shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await
        .unwrap();
}

fn offer(item: &Transition) -> DispatchableWorkflowTask {
    DispatchableWorkflowTask {
        run_key: item.next_state.run_key,
        logical_seq: item
            .next_state
            .pending_workflow_task
            .as_ref()
            .unwrap()
            .logical_seq,
        queue: QueueKey {
            namespace_id: item.next_state.namespace_id,
            task_queue: item.next_state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        },
        sticky_preferred: None,
        normal_queue: None,
        sticky_deadline: None,
        priority: None,
        order: None,
    }
}

#[tokio::test]
async fn polling_without_any_publication_delivers_from_another_execution_home() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let runtime = runtime(store.clone(), home(3));
    let queue = queue();
    let item = (0..100)
        .map(|index| transition(&queue, index))
        .find(|item| {
            execution_home_bundle(
                queue.namespace_id.0.as_bytes(),
                item.next_state.workflow_id.0.as_bytes(),
                8,
            ) != ShardId(3)
        })
        .unwrap();
    let run_key = item.next_state.run_key;
    let original_seq = item.next_state.transition_seq;
    store
        .commit_transition(run_key, item, ShardEpoch::ZERO)
        .await
        .unwrap();
    assert!(!runtime.broker.has_runnable_backlog(&queue).await);
    let result = runtime
        .poll_workflow_activation(
            queue.clone(),
            None,
            WorkerIdentity("worker".into()),
            tokio::time::Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    let WorkflowActivation::WorkflowTask(task) = result else {
        panic!("expected workflow task")
    };
    assert_eq!(task.run_key, run_key);
    let LoadedRun::Existing(state) = store.load_run(run_key).await.unwrap() else {
        panic!("run missing")
    };
    assert_eq!(state.transition_seq, original_seq.next());
    assert!(
        store
            .list_workflow_dispatch_page(
                &WorkflowDiscoveryRange {
                    namespace_id: queue.namespace_id,
                    queue_name: queue.task_queue,
                    routing: WorkflowDispatchRouting::Live
                },
                None,
                std::num::NonZeroU32::new(64).unwrap()
            )
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    stop(&runtime).await;
}

#[tokio::test]
async fn overlapping_queue_homes_can_commit_only_one_start() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let first_home = home(2);
    let second_home = home(5);
    let first = runtime(store.clone(), first_home.clone());
    let second = runtime(store.clone(), second_home);
    let queue = queue();
    let item = transition(&queue, 0);
    let task = offer(&item);
    let seq = item.next_state.transition_seq;
    store
        .commit_transition(task.run_key, item, ShardEpoch::ZERO)
        .await
        .unwrap();
    first.broker.publish_workflow_task(task.clone(), None).await;
    second
        .broker
        .publish_workflow_task(task.clone(), None)
        .await;
    let (a, b) = tokio::join!(
        first.try_claim_workflow_task(
            queue.clone(),
            None,
            task.run_key,
            WorkerIdentity("first".into())
        ),
        second.try_claim_workflow_task(
            queue.clone(),
            None,
            task.run_key,
            WorkerIdentity("second".into())
        )
    );
    assert_eq!(
        usize::from(a.unwrap().is_some()) + usize::from(b.unwrap().is_some()),
        1
    );
    let LoadedRun::Existing(state) = store.load_run(task.run_key).await.unwrap() else {
        panic!("run missing")
    };
    assert_eq!(state.transition_seq, seq.next());
    first_home.cancel.cancel();
    first.broker.retire_offers(&first_home, &queue).await;
    let range = WorkflowDiscoveryRange {
        namespace_id: queue.namespace_id,
        queue_name: queue.task_queue,
        routing: WorkflowDispatchRouting::Live,
    };
    assert!(
        store
            .list_workflow_dispatch_page(&range, None, std::num::NonZeroU32::new(64).unwrap())
            .await
            .unwrap()
            .candidates
            .is_empty()
    );
    stop(&first).await;
    stop(&second).await;
}

#[tokio::test]
async fn stale_missing_closed_and_paused_offers_do_not_escape_the_poll() {
    for shape in 0..3 {
        let store = Arc::new(InMemoryStore::with_shard_count(8));
        let runtime = runtime(store.clone(), home(6));
        let queue = queue();
        let mut stale = transition(&queue, 0);
        let stale_offer = offer(&stale);
        if shape > 0 {
            stale.next_state.status = if shape == 1 {
                ExecutionStatus::Completed
            } else {
                ExecutionStatus::Paused
            };
            store
                .commit_transition(stale_offer.run_key, stale, ShardEpoch::ZERO)
                .await
                .unwrap();
        }
        let valid = transition(&queue, 1);
        let valid_offer = offer(&valid);
        store
            .commit_transition(valid_offer.run_key, valid, ShardEpoch::ZERO)
            .await
            .unwrap();
        runtime
            .broker
            .publish_workflow_task(stale_offer, None)
            .await;
        runtime
            .broker
            .publish_workflow_task(valid_offer.clone(), None)
            .await;
        let result = runtime
            .poll_workflow_activation(
                queue,
                None,
                WorkerIdentity("worker".into()),
                tokio::time::Duration::ZERO,
            )
            .await
            .unwrap()
            .unwrap();
        let WorkflowActivation::WorkflowTask(started) = result else {
            panic!("expected workflow task")
        };
        assert_eq!(started.run_key, valid_offer.run_key);
        stop(&runtime).await;
    }
}
