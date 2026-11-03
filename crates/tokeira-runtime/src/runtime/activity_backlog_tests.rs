//! Activity row observations stay useful without poll demand and count offers once.

use super::*;
use crate::discovery::{
    QueueHomeProvider,
    tests::{home, transition},
};
use proptest::prelude::*;
use tokeira_kernel::{ActivityState, HistoryEvent, HistoryEventKind, Priority};
use tokeira_storage::{InMemoryStore, InMemoryWorkerComputeRepository, WriteFence};

pub(super) fn activity_queue() -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("activity-backlog".into()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

pub(super) fn fixture(queue: &QueueKey, priorities: &[i32], future: bool) -> Transition {
    let mut item = transition(queue, 0);
    item.next_state.pending_workflow_task = None;
    item.next_state.deployment = queue.deployment.clone();
    item.next_state.build_id = queue.build_id.clone();
    let now = OffsetDateTime::now_utc();
    for (index, priority) in priorities.iter().enumerate() {
        let at = if future && index + 1 == priorities.len() {
            now + Duration::hours(1)
        } else {
            now - Duration::seconds(30)
        };
        let activity = ActivityState {
            activity_id: format!("activity-{index}"),
            activity_type: "work".into(),
            schedule_event_id: index as i64 + 3,
            task_queue: queue.task_queue.clone(),
            deployment: None,
            build_id: None,
            retired_input: Default::default(),
            retired_header: Default::default(),
            attempt: 1,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: Some(Duration::minutes(5)),
            heartbeat_timeout: None,
            scheduled_at: at,
            current_attempt_scheduled_at: Some(at),
            started_at: None,
            started_event_id: None,
            last_failure: None,
            started_identity: None,
            retry_last_worker_identity: None,
            heartbeat_details: None,
            cancel_requested: false,
            pause_info: None,
            stamp: 0,
            activity_reset: false,
            reset_heartbeats: false,
            priority: Some(Priority {
                priority_key: *priority,
                fairness_key: String::new(),
                fairness_weight: 1.0,
            }),
            last_attempt_complete_time: None,
            last_heartbeat_at: None,
        };
        item.history_events.push(HistoryEvent {
            event_id: activity.schedule_event_id,
            happened_at: at,
            kind: HistoryEventKind::ActivityTaskScheduled {
                workflow_task_completed_event_id: 2,
                activity_id: activity.activity_id.clone(),
                activity_type: activity.activity_type.clone(),
                task_queue: activity.task_queue.clone(),
                input: Payloads::default(),
                header: None,
                retry_policy: None,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: activity.start_to_close_timeout,
                heartbeat_timeout: None,
                priority: activity.priority.clone(),
            },
        });
        item.event_principals.push(None);
        item.next_state.last_event_id = activity.schedule_event_id;
        item.next_state
            .activities
            .insert(activity.activity_id.clone(), activity);
    }
    item
}

pub(super) async fn check_no_poller_count<R: RunRepository + 'static>(
    runtime: &TokeiraRuntime<R>,
    queue: &QueueKey,
) {
    assert_eq!(runtime.activity_broker.backlog_stats(queue).await.count, 0);
    let stats = runtime.task_queue_backlog_stats_by_priority(queue).await;
    assert_eq!(stats.get(&1).map(|band| band.count), Some(2));
    assert_eq!(stats.get(&3).map(|band| band.count), Some(1));
    assert_eq!(stats.values().map(|band| band.count).sum::<usize>(), 3);
    assert!(stats[&1].oldest_age >= std::time::Duration::from_secs(30));
    assert!(stats[&3].oldest_age >= std::time::Duration::from_secs(30));
}

#[tokio::test]
async fn activity_backlog_counts_rows_without_pollers() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let runtime = super::discovery_tests::runtime(store.clone(), home(0));
    runtime.ready().await.unwrap();
    let queue = activity_queue();
    let item = fixture(&queue, &[1, 1, 0, 5], true);
    store
        .commit_transition(item.next_state.run_key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    check_no_poller_count(&runtime, &queue).await;
    super::discovery_tests::stop(&runtime).await;
}

#[tokio::test]
async fn activity_backlog_observation_is_bounded_to_four_pages() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let runtime = super::discovery_tests::runtime(store.clone(), home(0));
    runtime.ready().await.unwrap();
    let queue = activity_queue();
    let bound = 4 * crate::discovery::PAGE_SIZE as usize;
    let item = fixture(&queue, &vec![3; bound + 17], false);
    store
        .commit_transition(item.next_state.run_key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    let stats = runtime.task_queue_backlog_stats_by_priority(&queue).await;
    assert_eq!(stats[&3].count, bound);
    super::discovery_tests::stop(&runtime).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-dispatch, Property 19
    // Durable intent counts once through discovery, duplicate publish, handout and committed start.
    #[test]
    fn activity_dispatch_statistics_do_not_double_count_offers(
        priority in 0i32..9, copies in 1usize..6, exact in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                let store = Arc::new(InMemoryStore::with_shard_count(8));
                let runtime = super::discovery_tests::runtime(store.clone(), home(0));
                runtime.ready().await.unwrap();
                let mut queue = activity_queue();
                if exact {
                    queue.deployment = Some(DeploymentId("version".into()));
                    queue.build_id = Some(BuildId("build".into()));
                }
                let item = fixture(&queue, &[priority], false);
                let state = item.next_state.clone();
                let owner = execution_home_bundle(
                    state.namespace_id.0.as_bytes(), state.workflow_id.0.as_bytes(), 8,
                );
                let row = tokeira_storage::derive_activity_dispatch(&state, owner)
                    .unwrap().into_values().next().unwrap();
                store.commit_transition(state.run_key, item, WriteFence::Unfenced).await.unwrap();
                let now = tokio::time::Instant::now();
                let home = runtime.discovery.1.local_home(&queue).unwrap();
                let _demand = runtime.activity_discovery.register_poll(
                    queue.clone(), home, now,
                ).unwrap();
                let (key, home, mut pass) = runtime.activity_discovery.select(
                    now, OffsetDateTime::now_utc(),
                ).unwrap();
                let source = crate::activity_discovery::ActivitySource {
                    repo: store.clone(),
                    registry: runtime.worker_deployment_registry.clone(),
                    owner: runtime.shard_owner.clone(),
                };
                let outcome = crate::activity_discovery::run_slice(
                    &source, &runtime.activity_discovery, &runtime.activity_broker,
                    &key, &home, &mut pass,
                ).await.unwrap();
                runtime.activity_discovery.complete(&key, outcome, pass, now);
                assert!(runtime.activity_broker.contains_activity_task(&row.task()).await);
                for _ in 0..copies {
                    runtime.activity_broker.publish_activity_task(row.task(), None).await.unwrap();
                }
                let band = if priority == 0 { 3 } else { priority.min(5) };
                let stats = runtime.task_queue_backlog_stats_by_priority(&queue).await;
                assert_eq!(stats.values().map(|band| band.count).sum::<usize>(), 1);
                assert_eq!(stats[&band].count, 1);
                let offered = runtime.poll_activity_task_offer(
                    queue.clone(), WorkerIdentity("worker".into()),
                    std::time::Duration::from_secs(30),
                ).await.unwrap().unwrap();
                assert_eq!(runtime.task_queue_backlog_stats_by_priority(&queue).await[&band].count,
                    1, "an in-flight offer is still unconsumed durable intent");
                let started = runtime.start_activity_task_offer(
                    offered, WorkerIdentity("worker".into()),
                ).await.unwrap();
                assert!(started.is_some());
                assert_eq!(runtime.task_queue_backlog_stats_by_priority(&queue).await
                    .values().map(|band| band.count).sum::<usize>(), 0);
                super::discovery_tests::stop(&runtime).await;
            });
    }
}

#[tokio::test]
async fn activity_worker_compute_counts_rows_without_pollers() {
    let store = Arc::new(InMemoryStore::with_shard_count(8));
    let runtime = super::discovery_tests::runtime(store.clone(), home(0));
    runtime.ready().await.unwrap();
    let mut queue = activity_queue();
    queue.deployment = Some(DeploymentId("deployment".into()));
    queue.build_id = Some(BuildId("build".into()));
    let item = fixture(&queue, &[1, 3, 5], true);
    let state = item.next_state.clone();
    let owner = execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        8,
    );
    let rows = tokeira_storage::derive_activity_dispatch(&state, owner).unwrap();
    store
        .commit_transition(state.run_key, item, WriteFence::Unfenced)
        .await
        .unwrap();
    let sampler = runtime.worker_compute_queue_sampler(
        Arc::new(InMemoryWorkerComputeRepository::default()),
        IncarnationId(Uuid::new_v4()),
    );
    for published in [false, true] {
        if published {
            for row in rows
                .values()
                .filter(|row| row.dispatch_at <= OffsetDateTime::now_utc())
            {
                runtime
                    .activity_broker
                    .publish_activity_task(row.task(), None)
                    .await
                    .unwrap();
            }
        }
        let samples = sampler
            .sample_once(&runtime.active_shards(), OffsetDateTime::now_utc())
            .await
            .unwrap();
        let count = samples
            .iter()
            .filter(|sample| {
                sample.key.task_type == tokeira_types::WorkerComputeTaskType::Activity
                    && sample.key.task_queue == queue.task_queue
                    && Some(&sample.key.deployment_name) == queue.deployment.as_ref()
                    && Some(&sample.key.build_id) == queue.build_id.as_ref()
            })
            .map(|sample| sample.backlog_count)
            .sum::<u64>();
        assert_eq!(
            count, 2,
            "due activity rows count once, published={published}"
        );
    }
    let foreign: Vec<_> = runtime
        .active_shards()
        .into_iter()
        .filter(|id| *id != owner)
        .collect();
    let samples = sampler
        .sample_once(&foreign, OffsetDateTime::now_utc())
        .await
        .unwrap();
    assert!(samples.iter().all(|sample| sample.backlog_count == 0));
    super::discovery_tests::stop(&runtime).await;
}
