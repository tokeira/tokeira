//! Generated demand and traversal contracts with an independent queue oracle.

use super::*;
use proptest::prelude::*;
use time::OffsetDateTime;
use tokeira_kernel::{
    BasicKernel, Command, Kernel, Priority, Transition, VersioningBehavior,
    WorkerDeploymentVersionRef, WorkflowVersioningInfo,
};
use tokeira_storage::{CommitResult, InMemoryStore};
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, RunKey, ShardEpoch, TaskKind, TaskQueueName, WorkerIdentity,
};

#[derive(Debug)]
pub(crate) struct Homes(pub(crate) Mutex<QueueHome>);

impl QueueHomeProvider for Homes {
    fn local_home(&self, _: &QueueKey) -> Option<QueueHome> {
        let home = self.0.lock().unwrap().clone();
        (!home.cancel.is_cancelled()).then_some(home)
    }
}

pub(crate) fn home(id: u32) -> QueueHome {
    QueueHome {
        id: ShardId(id),
        generation: 1,
        cancel: CancellationToken::new(),
    }
}

fn queue() -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("discovery".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

pub(crate) fn transition(queue: &QueueKey, index: usize) -> Transition {
    let mut request = crate::runtime::tests::sample_start_request(None, None);
    request.namespace_id = queue.namespace_id;
    request.task_queue = queue.task_queue.clone();
    request.workflow_id.0 = format!("discovery-{index}");
    request.deployment = queue.deployment.clone();
    request.build_id = queue.build_id.clone();
    request.now = OffsetDateTime::UNIX_EPOCH;
    BasicKernel
        .apply(LoadedRun::Absent, Command::Start(request))
        .unwrap()
}

fn offer(transition: &Transition) -> DispatchableWorkflowTask {
    let state = &transition.next_state;
    DispatchableWorkflowTask {
        run_key: state.run_key,
        queue: QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        },
        logical_seq: state.pending_workflow_task.as_ref().unwrap().logical_seq,
        sticky_preferred: None,
        normal_queue: None,
        sticky_deadline: None,
        priority: state.priority.clone(),
        order: None,
    }
}

async fn commit(store: &InMemoryStore, transition: Transition) {
    assert!(matches!(
        store
            .commit_transition(transition.next_state.run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap(),
        CommitResult::Applied { .. }
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch, Property 4: Bounded slices without prefix starvation
    // Held and incompatible prefixes consume examinations, never the admission budget.
    #[test]
    fn long_prefix_yields_without_restarting(prefix in 129usize..270, held_every in 2usize..7) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = Arc::new(InMemoryStore::with_shard_count(8));
            let queue = queue();
            let home = home(3);
            let broker = InMemoryBroker::with_discovery(Arc::new(Homes(Mutex::new(home.clone()))));
            let registry = DiscoveryRegistry::default();
            let _guard = registry.register_poll(queue.clone(), home.clone(), Instant::now());
            let deployment_registry = DeploymentRegistry::with_repositories(store.clone(), store.clone(), crate::WorkerRegistry::default());
            let source = WorkflowSource { repo: store.clone(), registry: Arc::new(RwLock::new(Some(deployment_registry))) };
            let mut target = RunKey::new();
            let mut execution_homes = std::collections::BTreeSet::new();
            for index in 0..=prefix {
                let mut item = transition(&queue, index);
                item.next_state.priority = Some(Priority { priority_key: if index < prefix { 1 } else { 5 }, fairness_key: String::new(), fairness_weight: 1.0 });
                if index < prefix && index % held_every != 0 {
                    item.next_state.versioning_info = Some(WorkflowVersioningInfo {
                        behavior: VersioningBehavior::Pinned,
                        deployment_version: Some(WorkerDeploymentVersionRef { deployment_name: "other".into(), build_id: "v1".into() }),
                        ..Default::default()
                    });
                }
                execution_homes.insert(tokeira_types::execution_home_bundle(queue.namespace_id.0.as_bytes(), item.next_state.workflow_id.0.as_bytes(), 8));
                if index == prefix { target = item.next_state.run_key; }
                if index < prefix && index % held_every == 0 {
                    broker.publish_workflow_task(offer(&item), None).await;
                }
                commit(&store, item).await;
            }
            assert!(execution_homes.len() > 1 && execution_homes.iter().any(|id| *id != home.id));
            let (key, _, mut pass) = registry.select(Instant::now()).unwrap();
            loop {
                let before = pass.examined;
                let pages = pass.pages;
                let outcome = run_slice(&source, &registry, &broker, &key, &home, &mut pass).await.unwrap();
                assert!(pass.examined - before <= 64);
                assert_eq!(pass.pages, pages + 1);
                if outcome != SliceOutcome::Yield { assert_eq!(outcome, SliceOutcome::Complete); break; }
            }
            assert_eq!(pass.examined, prefix + 1);
            assert_eq!(pass.admissions, 1);
            assert!(broker.knows_offer((target, tokeira_types::LogicalTaskSeq(1))).await);
        });
    }

    // Feature: workflow-dispatch, Property 5: Demand registration and home independence
    // Poll guards reference-count each physical target while sharing one family Live pass.
    #[test]
    fn demand_guards_match_reference_counts(versions in prop::collection::vec(0u8..4, 1..60), release in prop::collection::vec(any::<bool>(), 1..60)) {
        let registry = DiscoveryRegistry::default();
        let queue = queue();
        let home = home(7);
        let now = Instant::now();
        let mut guards = Vec::new();
        let mut expected = HashMap::<QueueKey, usize>::new();
        for version in versions {
            let mut target = queue.clone();
            if version > 0 { target.deployment = Some(DeploymentId("deployment".into())); target.build_id = Some(BuildId(version.to_string())); }
            *expected.entry(target.clone()).or_default() += 1;
            guards.push(Some(registry.register_poll(target, home.clone(), now)));
        }
        for (index, should_release) in release.into_iter().enumerate() {
            let slot = index % guards.len();
            if should_release && let Some(guard) = guards[slot].take() {
                *expected.get_mut(&guard.queue).unwrap() -= 1;
                drop(guard);
            }
        }
        expected.retain(|_, count| *count != 0);
        let state = registry.state.lock().unwrap();
        let live: Vec<_> = state.ranges.iter().filter(|(key, _)| key.live).collect();
        prop_assert_eq!(live.len(), 1);
        prop_assert_eq!(&live[0].1.demands, &expected);
        drop(state);
        drop(guards);
        registry.retire(now + IDLE_GRACE + Duration::from_secs(1));
        prop_assert!(registry.state.lock().unwrap().ranges.is_empty());
    }

    // Feature: workflow-dispatch, Property 4: Bounded slices without prefix starvation
    // Capacity stops discard continuation; each competing range gets a fresh head turn after release.
    #[test]
    fn capacity_return_restarts_fair_competing_ranges(count in 2usize..20, rounds in 1usize..6) {
        let registry = DiscoveryRegistry::default();
        let now = Instant::now();
        let guards: Vec<_> = (0..count).map(|_| registry.register_poll(queue(), home(2), now)).collect();
        for _ in 0..rounds {
            let mut selected = std::collections::HashSet::new();
            for _ in 0..count {
                let (key, _, mut pass) = registry.select(now).unwrap();
                prop_assert!(selected.insert(key.clone()));
                prop_assert!(pass.after.is_none());
                pass.after = Some(WorkflowDispatchPosition { priority_key: 3, scheduled_at: OffsetDateTime::UNIX_EPOCH, run_key: RunKey::new() });
                registry.complete(&key, SliceOutcome::Capacity, pass, now);
            }
            prop_assert!(registry.select(now).is_none());
            registry.capacity_returned(now);
        }
        drop(guards);
    }

    // Feature: workflow-dispatch, Property 5: Demand registration and home independence
    // Retiring one generation cannot evict its replacement or another overlapping home.
    #[test]
    fn home_replacement_and_restart_preserve_only_current_demand(count in 1usize..30, renewals in 1usize..5) {
        let mut registry = DiscoveryRegistry::default();
        let queue = queue();
        let now = Instant::now();
        let mut current = home(2);
        let overlap = home(5);
        let mut guards: Vec<_> = (0..count).map(|_| registry.register_poll(queue.clone(), current.clone(), now)).collect();
        let overlapping = registry.register_poll(queue.clone(), overlap, now);
        for _ in 0..renewals {
            current.cancel.cancel();
            current = QueueHome { generation: current.generation + 1, ..home(2) };
            let replacement = registry.register_poll(queue.clone(), current.clone(), now);
            registry.retire(now);
            guards.clear();
            guards.push(replacement);
            prop_assert_eq!(registry.registered_ranges(), 2);
            let (first, _, pass) = registry.select(now).unwrap();
            registry.complete(&first, SliceOutcome::Complete, pass, now);
            let (second, _, pass) = registry.select(now).unwrap();
            prop_assert_ne!(first.home, second.home);
            registry.complete(&second, SliceOutcome::Complete, pass, now);
            registry.capacity_returned(now + DISCOVERY_PERIOD);
            // Make both ranges eligible for the next simulated renewal.
            for entry in registry.state.lock().unwrap().ranges.values_mut() { entry.next_pass = now; }
        }
        drop(guards);
        drop(overlapping);
        registry = DiscoveryRegistry::default();
        prop_assert_eq!(registry.registered_ranges(), 0);
        let _renewed = registry.register_poll(queue, current, now);
        prop_assert_eq!(registry.registered_ranges(), 1);
    }
}

#[tokio::test]
async fn idle_renewal_resumes_cursor_and_active_ranges_receive_fair_turns() {
    assert_eq!(
        (
            PAGE_SIZE,
            SLICE_PAGES,
            PASS_ADMISSIONS,
            CONCURRENT_QUERIES,
            IDLE_CAPACITY
        ),
        (64, 1, 64, 8, 4096)
    );
    assert_eq!(
        (DISCOVERY_PERIOD, IDLE_GRACE),
        (Duration::from_secs(1), Duration::from_secs(30))
    );
    let registry = DiscoveryRegistry::default();
    let first = queue();
    let second = queue();
    let now = Instant::now();
    let guard = registry.register_poll(first.clone(), home(1), now);
    let _second_guard = registry.register_poll(second.clone(), home(1), now);
    let (key, _, mut pass) = registry.select(now).unwrap();
    pass.after = Some(WorkflowDispatchPosition {
        priority_key: 2,
        scheduled_at: OffsetDateTime::UNIX_EPOCH,
        run_key: RunKey::new(),
    });
    registry.complete(&key, SliceOutcome::Yield, pass.clone(), now);
    let (other, _, _) = registry.select(now).unwrap();
    assert_eq!(other.queue.namespace_id, second.namespace_id);
    drop(guard);
    assert!(registry.select(now).is_none());
    let _renewed = registry.register_poll(first, home(1), now + Duration::from_secs(1));
    let (resumed, _, resumed_pass) = registry.select(now + Duration::from_secs(1)).unwrap();
    assert_eq!(resumed, key);
    assert_eq!(resumed_pass.after, pass.after);
}

#[tokio::test]
async fn idle_pressure_never_evicts_active_registrations() {
    let registry = DiscoveryRegistry::default();
    let now = Instant::now();
    let active_queue = queue();
    let _active = registry.register_poll(active_queue.clone(), home(0), now);
    for index in 0..IDLE_CAPACITY + 10 {
        let mut idle_queue = active_queue.clone();
        idle_queue.task_queue.0 = index.to_string();
        drop(registry.register_poll(idle_queue, home(0), now));
    }
    registry.retire(Instant::now());
    let state = registry.state.lock().unwrap();
    assert_eq!(state.ranges.len(), IDLE_CAPACITY + 1);
    assert!(
        state
            .ranges
            .values()
            .any(|entry| entry.demands.contains_key(&active_queue))
    );
}

#[test]
fn evicted_query_cannot_complete_into_a_recreated_registration() {
    let registry = DiscoveryRegistry::default();
    let queue = queue();
    let now = Instant::now();
    let guard = registry.register_poll(queue.clone(), home(3), now);
    let (old_key, _, mut old_pass) = registry.select(now).unwrap();
    old_pass.after = Some(WorkflowDispatchPosition {
        priority_key: 3,
        scheduled_at: OffsetDateTime::UNIX_EPOCH,
        run_key: RunKey::new(),
    });
    drop(guard);
    let later = now + IDLE_GRACE + Duration::from_secs(1);
    registry.retire(later);
    assert!(old_pass.cancel.is_cancelled());
    let _replacement = registry.register_poll(queue, home(3), later);
    let (new_key, _, new_pass) = registry.select(later).unwrap();
    assert_eq!(old_key, new_key);
    assert_ne!(old_pass.registration, new_pass.registration);
    registry.complete(&old_key, SliceOutcome::Yield, old_pass, later);
    assert!(registry.select(later).is_none());
    assert!(new_pass.after.is_none());
    registry.complete(&new_key, SliceOutcome::Complete, new_pass, later);
}

#[tokio::test]
async fn broker_retains_taken_identity_and_keeps_speculative_delivery_separate() {
    let broker = InMemoryBroker::with_discovery(Arc::new(Homes(Mutex::new(home(0)))));
    let queue = queue();
    let item = transition(&queue, 0);
    let task = offer(&item);
    let key = (task.run_key, task.logical_seq);
    broker.publish_workflow_task(task.clone(), None).await;
    let (_, entered) = broker
        .try_claim_workflow_task(&queue, task.run_key)
        .await
        .unwrap();
    broker.publish_workflow_task(task.clone(), None).await;
    assert!(
        broker
            .try_claim_workflow_task(&queue, task.run_key)
            .await
            .is_none()
    );
    broker.finish_offer(key, entered).await;
    broker.publish_workflow_task(task.clone(), None).await;
    assert!(
        broker
            .try_claim_workflow_task(&queue, task.run_key)
            .await
            .is_some()
    );
    broker.finish_offer(key, entered).await;
    broker
        .expire_offers(Instant::now() + Duration::from_secs(6))
        .await;
    broker
        .publish_speculative_workflow_task(task.clone(), None)
        .await;
    broker
        .expire_offers(Instant::now() + Duration::from_secs(60))
        .await;
    assert!(
        broker
            .poll_workflow_task(&queue, &WorkerIdentity("worker".into()), Duration::ZERO)
            .await
            .unwrap()
            .is_some()
    );
}
