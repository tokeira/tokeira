//! Runtime delivery contracts for default durable discovery.

use super::*;
use crate::{
    discovery::{
        QueueHome,
        tests::{home, transition},
    },
    shard::shard_for,
};
use proptest::prelude::*;
use tokeira_storage::{InMemoryStore, WorkflowDiscoveryRange, WorkflowDispatchRouting, WriteFence};

fn runtime(store: Arc<InMemoryStore>, _home: QueueHome) -> TokeiraRuntime<InMemoryStore> {
    let runtime = TokeiraRuntime::new_with_nexus_and_shards_and_endpoint(
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
            store.commit_transition(task.run_key, item, WriteFence::Unfenced).await.unwrap();
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
    assert_eq!(runtime.discovery.0.registered_ranges(), 0);
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

pub(super) fn offer(item: &Transition) -> DispatchableWorkflowTask {
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
        .commit_transition(run_key, item, WriteFence::Unfenced)
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
        .commit_transition(task.run_key, item, WriteFence::Unfenced)
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
                .commit_transition(stale_offer.run_key, stale, WriteFence::Unfenced)
                .await
                .unwrap();
        }
        let valid = transition(&queue, 1);
        let valid_offer = offer(&valid);
        store
            .commit_transition(valid_offer.run_key, valid, WriteFence::Unfenced)
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

#[derive(Debug)]
struct ExecutionOwner(Arc<TokeiraRuntime<InMemoryStore>>);

#[async_trait::async_trait]
impl crate::WorkflowTaskStarter for ExecutionOwner {
    async fn start(
        &self,
        home: ShardId,
        offered: DispatchableWorkflowTask,
        worker: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        assert_eq!(self.0.active_shards(), [home]);
        self.0.start_forwarded_workflow_task(offered, worker).await
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch, Property 12: Next-pass recovery and eventual resolution
    // A finite sequence of lost publications/brokers is followed by a fair poll at a distinct queue owner.
    #[test]
    fn distinct_queue_and_execution_owners_recover_lost_publication(restarts in 0usize..3, prefix in 0usize..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let store = Arc::new(InMemoryStore::with_shard_count(8));
            let queue = queue();
            let queue_home = tokeira_types::bundle_for_partition(tokeira_types::workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8), 8);
            let item = (0..100).map(|n| transition(&queue, n)).find(|item| {
                execution_home_bundle(queue.namespace_id.0.as_bytes(), item.next_state.workflow_id.0.as_bytes(), 8) != queue_home
            }).unwrap();
            let run_key = item.next_state.run_key;
            let seq = item.next_state.transition_seq;
            let execution_home = execution_home_bundle(queue.namespace_id.0.as_bytes(), item.next_state.workflow_id.0.as_bytes(), 8);
            store.commit_transition(run_key, item, WriteFence::Unfenced).await.unwrap();
            // No publication is made at any point. New brokers carry no cursor or offer from their predecessors.
            for _ in 0..restarts {
                let lost = runtime(store.clone(), home(0));
                stop(&lost).await;
            }
            let a = runtime(store.clone(), home(queue_home.0));
            let b = Arc::new(runtime(store.clone(), home(execution_home.0)));
            for id in 0..8 {
                if id != queue_home.0 { a.shard_owner.write().unwrap().remove(ShardId(id)); }
                if id != execution_home.0 { b.shard_owner.write().unwrap().remove(ShardId(id)); }
            }
            a.set_workflow_task_starter(Arc::new(ExecutionOwner(b.clone())));
            for _ in 0..prefix {
                assert!(a.poll_workflow_activation(queue.clone(), None, WorkerIdentity("cancelled".into()), std::time::Duration::ZERO).await.unwrap().is_none());
            }
            let result = a.poll_workflow_activation(queue.clone(), None, WorkerIdentity("cross-node".into()), std::time::Duration::from_secs(30)).await.unwrap();
            let Some(WorkflowActivation::WorkflowTask(started)) = result else { panic!("next healthy pass must discover and start"); };
            assert_eq!(started.run_key, run_key);
            assert_eq!(started.token.shard_epoch, b.shard_owner.read().unwrap().epoch_of(execution_home).unwrap());
            assert_eq!(a.active_shards(), [queue_home]);
            let LoadedRun::Existing(state) = store.load_run(run_key).await.unwrap() else { panic!("missing run"); };
            assert_eq!(state.transition_seq, seq.next());
            assert!(state.pending_workflow_task.unwrap().started_event_id.is_some());
            assert!(!a.broker.has_runnable_backlog(&queue).await);
            stop(&a).await;
            stop(&b).await;
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch, Property 11: Routing, state, and delivery preservation
    // Normal intent contributes once; volatile speculative tasks remain deliverable with their priority/fairness.
    #[test]
    fn dispatch_statistics_do_not_double_count_offers(exact in any::<bool>(), speculative in any::<bool>(), priority in 1i32..=5, fairness in "[a-z]{0,8}", copies in 1usize..6, enable_fairness in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let store = Arc::new(InMemoryStore::with_shard_count(8));
            let runtime = runtime(store.clone(), home(0)).with_delivery_mode_provider(Arc::new(
                crate::task_ordering::ConfiguredDeliveryModeProvider::new(
                    crate::task_ordering::StaticDeliveryPolicy { enable_fairness },
                ),
            ));
            let mut queue = queue();
            if exact { queue.deployment = Some(DeploymentId("version".into())); queue.build_id = Some(BuildId("build".into())); }
            let mut item = transition(&queue, 0);
            item.next_state.deployment = queue.deployment.clone();
            item.next_state.build_id = queue.build_id.clone();
            item.next_state.priority = Some(tokeira_kernel::Priority { priority_key: priority, fairness_key: fairness.clone(), fairness_weight: 1.5 });
            if speculative { item.next_state.pending_workflow_task.as_mut().unwrap().task_type = tokeira_kernel::WorkflowTaskType::Speculative; }
            let mut task = offer(&item);
            task.queue = queue.clone();
            task.priority = item.next_state.priority.clone();
            store.commit_transition(task.run_key, item, WriteFence::Unfenced).await.unwrap();
            let state = store.load_run(task.run_key).await.unwrap();
            assert_eq!(runtime.republish_queue(queue.clone(), 64).await.unwrap(), 1);
            for _ in 0..copies {
                if speculative { runtime.broker.publish_speculative_workflow_task(task.clone(), None).await; }
                else { runtime.broker.publish_workflow_task(task.clone(), None).await; }
            }
            let stats = runtime.task_queue_backlog_stats_by_priority(&queue).await;
            assert_eq!(stats.values().map(|band| band.count).sum::<usize>(), 1);
            assert_eq!(stats[&priority].count, 1);
            assert_eq!(store.load_run(task.run_key).await.unwrap(), state);
            let handed = runtime.broker.try_claim_workflow_task(&queue, task.run_key).await.unwrap();
            assert_eq!(handed.0.priority, task.priority);
            if !speculative {
                assert_eq!(runtime.task_queue_backlog_stats_by_priority(&queue).await[&priority].count, 1, "an in-flight offer is still unconsumed durable intent");
            }
            stop(&runtime).await;
        });
    }
}

#[test]
fn upgrade_contract_survives_changelog_consumption() {
    let design = include_str!("../../../../.kiro/specs/workflow-dispatch/design.md");
    assert_eq!(
        design
            .lines()
            .filter(|line| *line == "Upgrade with every node stopped.")
            .count(),
        1
    );
}

#[derive(Debug, Default)]
struct UnavailableOwner {
    retry_offer: Option<crate::broker::InMemoryBroker>,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::WorkflowTaskStarter for UnavailableOwner {
    async fn start(
        &self,
        _home: ShardId,
        offered: DispatchableWorkflowTask,
        _worker: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(broker) = &self.retry_offer {
            assert_eq!(call, 0, "an expired poll must not retry ready owner work");
            tokio::time::advance(std::time::Duration::from_secs(6)).await;
            broker.expire_offers(tokio::time::Instant::now()).await;
            broker.publish_workflow_task(offered, None).await;
        }
        Err(crate::RetryableWorkflowTaskStart("owner unavailable".into()).into())
    }
}

#[tokio::test(start_paused = true)]
async fn unavailable_owner_keeps_polling_until_the_original_deadline() {
    for timeout in [
        std::time::Duration::ZERO,
        std::time::Duration::from_secs(12),
    ] {
        let store = Arc::new(InMemoryStore::with_shard_count(8));
        let queue = queue();
        let queue_home = tokeira_types::bundle_for_partition(
            tokeira_types::workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
            8,
        );
        let item = (0..100)
            .map(|n| transition(&queue, n))
            .find(|item| {
                execution_home_bundle(
                    queue.namespace_id.0.as_bytes(),
                    item.next_state.workflow_id.0.as_bytes(),
                    8,
                ) != queue_home
            })
            .unwrap();
        let initial_offer = offer(&item);
        store
            .commit_transition(item.next_state.run_key, item, WriteFence::Unfenced)
            .await
            .unwrap();
        let node = runtime(store.clone(), home(queue_home.0));
        for id in 0..8 {
            if id != queue_home.0 {
                node.shard_owner.write().unwrap().remove(ShardId(id));
            }
        }
        let owner = Arc::new(UnavailableOwner {
            retry_offer: timeout.is_zero().then(|| node.broker()),
            ..Default::default()
        });
        node.set_workflow_task_starter(owner.clone());
        node.broker.publish_workflow_task(initial_offer, None).await;
        let before = store.snapshot().await.unwrap();
        let start = tokio::time::Instant::now();
        let result = node
            .poll_workflow_activation(queue, None, WorkerIdentity("worker".into()), timeout)
            .await
            .unwrap();
        assert!(result.is_none());
        if timeout.is_zero() {
            assert_eq!(owner.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(start.elapsed(), std::time::Duration::from_secs(6));
        } else {
            assert_eq!(start.elapsed(), timeout);
        }
        assert_eq!(store.snapshot().await.unwrap(), before);
        stop(&node).await;
    }
}
