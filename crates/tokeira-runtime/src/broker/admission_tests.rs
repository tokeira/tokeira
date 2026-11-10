//! Generated acquisition and retention contracts, independent of discovery ticks.

use super::*;
use crate::{
    discovery::{DiscoveryRegistry, PlacementQueueHomes},
    shard::ShardOwner,
};
use proptest::prelude::*;
use std::{
    future::{Future, poll_fn},
    task::Poll,
};
use tokeira_types::{
    ShardEpoch, ShardId, TaskKind, bundle_for_partition, workflow_queue_partition,
};
use tokio_util::sync::CancellationToken;

fn queue(index: u32) -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(uuid::Uuid::nil()),
        task_queue: TaskQueueName(format!("admission-{index}")),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn home_id(queue: &QueueKey) -> ShardId {
    bundle_for_partition(
        workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}

fn fixture() -> (InMemoryBroker, Arc<PlacementQueueHomes>) {
    let mut owner = ShardOwner::new(8);
    for id in 0..8 {
        owner.record_acquired(ShardId(id), ShardEpoch::ZERO);
        owner.mark_active(ShardId(id));
    }
    let homes = Arc::new(PlacementQueueHomes {
        owner: Arc::new(StdRwLock::new(owner)),
        counts: StdRwLock::new((8, 8)),
    });
    (InMemoryBroker::with_discovery(homes.clone()), homes)
}

fn task(queue: &QueueKey, id: u32) -> DispatchableWorkflowTask {
    DispatchableWorkflowTask {
        run_key: RunKey(uuid::Uuid::from_u128(u128::from(id) + 1)),
        logical_seq: LogicalTaskSeq(1),
        queue: queue.clone(),
        sticky_preferred: None,
        normal_queue: None,
        sticky_deadline: None,
        priority: None,
        order: None,
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 5: cancellation fences dequeue before cleanup.
    #[test]
    fn property_5_cancelled_generation_cannot_dequeue(index in 0u32..100, copies in 1..8usize) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let queue = queue(index);
            let task = task(&queue, index);
            for _ in 0..copies { broker.publish_workflow_task(task.clone(), None).await; }
            homes.local_home(&queue).unwrap().cancel.cancel();
            prop_assert!(broker.try_claim_workflow_task(&queue, task.run_key).await.is_none(), "retired acquisition still exposes a ready offer");
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 2: colocated homes retain independent copies.
    #[test]
    fn property_2_colocated_distinct_homes(index in 0u32..100) {
        runtime().block_on(async {
            let (broker, _) = fixture();
            let first = queue(index);
            let second = (index + 1..index + 100).map(queue).find(|q| home_id(q) != home_id(&first)).unwrap();
            let task = task(&first, index);
            broker.publish_workflow_task(task.clone(), None).await;
            broker.publish_workflow_task(DispatchableWorkflowTask { queue: second.clone(), ..task.clone() }, None).await;
            prop_assert!(broker.try_claim_workflow_task(&first, task.run_key).await.is_some());
            prop_assert!(broker.try_claim_workflow_task(&second, task.run_key).await.is_some(), "another colocated home suppressed admission");
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 2: every publication source shares one retained incarnation.
    #[test]
    fn property_2_publication_discovery_races(sources in prop::collection::vec(0u8..3, 3..24), index in 0u32..100) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let queue = queue(index);
            let task = task(&queue, index);
            let home = homes.local_home(&queue).unwrap();
            let sources = [vec![0, 1, 2], sources].concat();
            let barrier = Arc::new(tokio::sync::Barrier::new(sources.len()));
            let mut attempts = tokio::task::JoinSet::new();
            for source in sources {
                let broker = broker.clone();
                let task = task.clone();
                let home = home.clone();
                let barrier = barrier.clone();
                attempts.spawn(async move {
                    barrier.wait().await;
                    match source {
                        0 => broker.publish_workflow_task(task, None).await,
                        1 => {
                            let mut reservation = broker.reserve_offers(&home, 1).await;
                            assert!(matches!(broker.publish_discovered(task, OffsetDateTime::UNIX_EPOCH, &mut reservation).await, OfferAdmission::Known | OfferAdmission::Admitted));
                        }
                        _ => { assert!(matches!(broker.admit_workflow_offer(task, OfferSource::Pushed, None, None, false).await, OfferAdmission::Known | OfferAdmission::Admitted)); }
                    }
                });
            }
            while let Some(result) = attempts.join_next().await { result.unwrap(); }
            let stamp = broker.try_take(&queue, None, &WorkerIdentity("worker".into()), None).await.unwrap().unwrap().1;
            prop_assert_eq!(broker.admit_workflow_offer(task.clone(), OfferSource::Pushed, None, None, false).await, OfferAdmission::Known);
            prop_assert!(broker.try_claim_workflow_task(&queue, task.run_key).await.is_none());
            broker.finish_offer((task.run_key, task.logical_seq), stamp.entered_at).await;
            prop_assert!(!broker.knows_offer((task.run_key, task.logical_seq)).await);
            prop_assert_eq!(broker.inner.lock().await.offers.available(home.id), crate::workflow_offers::HOME_CAPACITY);
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 2: a current fallback replaces one revision; old peer hints/results cannot undo it.
    #[test]
    fn property_2_same_home_retarget_and_late_result(index in 0u32..100, in_flight in any::<bool>(), copies in 1usize..8) {
        runtime().block_on(async {
            let (broker, _) = fixture();
            let sticky = queue(index);
            let normal = (index + 1..index + 1000).map(queue).find(|q| home_id(q) == home_id(&sticky)).unwrap();
            let worker = WorkerIdentity("sticky-worker".into());
            broker.record_workflow_poller(&sticky, &worker).await;
            let sticky_task = DispatchableWorkflowTask {
                sticky_preferred: Some(worker.clone()), normal_queue: Some(normal.clone()),
                sticky_deadline: Some(OffsetDateTime::now_utc() + time::Duration::seconds(2)),
                ..task(&sticky, index)
            };
            prop_assert_eq!(broker.admit_workflow_offer(sticky_task.clone(), OfferSource::Pushed, None, None, false).await, OfferAdmission::Admitted);
            let old = if in_flight {
                broker.try_take(&sticky, None, &worker, None).await.unwrap().unwrap().1
            } else {
                broker.inner.lock().await.sticky_ready[&sticky].front().unwrap().entered_at.into()
            };
            let normal_task = DispatchableWorkflowTask { queue: normal.clone(), sticky_preferred: None, normal_queue: None, ..sticky_task.clone() };
            for _ in 0..copies { broker.publish_workflow_task(normal_task.clone(), None).await; }
            if let Some(cancel) = old.cancel { prop_assert!(cancel.is_cancelled()); }
            prop_assert_eq!(broker.admit_workflow_offer(sticky_task.clone(), OfferSource::Pushed, None, None, false).await, OfferAdmission::Known);
            let current = broker.try_take(&normal, None, &worker, None).await.unwrap().unwrap().1;
            prop_assert_ne!(current.entered_at, old.entered_at);
            broker.finish_offer((normal_task.run_key, normal_task.logical_seq), old.entered_at).await;
            prop_assert_eq!(broker.admit_workflow_offer(normal_task.clone(), OfferSource::Pushed, None, None, false).await, OfferAdmission::Known);
            prop_assert!(broker.try_claim_workflow_task_for_worker(&sticky, sticky_task.run_key, Some(&worker)).await.is_none());
            broker.expire_offers(Instant::now() + Duration::from_secs(2)).await;
            prop_assert!(current.cancel.unwrap().is_cancelled());
            prop_assert!(!broker.knows_offer((normal_task.run_key, normal_task.logical_seq)).await);
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 2: a byte refusal returns the entire unused page reservation.
    #[test]
    fn property_2_full_releases_reservations(index in 0u32..100, slots in 1usize..65, bytes_full in any::<bool>()) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let queue = queue(index);
            let home = homes.local_home(&queue).unwrap();
            let mut reservation = broker.reserve_offers(&home, slots).await;
            let count = if bytes_full { 1 } else { crate::workflow_offers::QUEUE_CAPACITY };
            for id in 0..count {
                let hint = task(&queue, id as u32);
                prop_assert_eq!(broker.inner.lock().await.offers.admit(OfferAdmissionInput {
                    key: (hint.run_key, hint.logical_seq), queue: queue.clone(), home: &home, now: Instant::now(), reserved: false,
                    bytes: if bytes_full { crate::workflow_offers::BYTE_CAPACITY } else { serialized_hint_bytes(&hint) },
                    deadline: None, retarget: true,
                }).0, OfferAdmission::Admitted);
            }
            prop_assert_eq!(broker.publish_discovered(task(&queue, 10000), OffsetDateTime::UNIX_EPOCH, &mut reservation).await, OfferAdmission::Full);
            prop_assert_eq!(reservation.remaining(), 0);
            prop_assert_eq!(broker.inner.lock().await.offers.available(home.id), crate::workflow_offers::HOME_CAPACITY - count);
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 5: Known and new admissions recheck the receiver's captured acquisition under the lock.
    #[test]
    fn property_5_admission_rechecks_acquisition(index in 0u32..100, ending in 0u8..3, known in any::<bool>()) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let queue = queue(index);
            let task = task(&queue, index);
            if known { broker.publish_workflow_task(task.clone(), None).await; }
            let lock = broker.inner.lock().await;
            let mut admission = Box::pin(broker.admit_workflow_offer(task, OfferSource::Pushed, None, None, false));
            poll_fn(|cx| { assert!(admission.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
            if ending == 0 {
                let mut owner = homes.owner.write().unwrap();
                owner.record_acquired(home_id(&queue), ShardEpoch::ZERO);
                owner.mark_active(home_id(&queue));
            } else if ending == 1 { homes.local_home(&queue).unwrap().cancel.cancel(); }
            else { *homes.local_home(&queue).unwrap().deadline.lock().unwrap() = Some(Instant::now()); }
            drop(lock);
            prop_assert_eq!(admission.await, OfferAdmission::NotHome);
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 5: loss/deadline cleanup is independent of discovery and isolated from replacements.
    #[test]
    fn property_5_cleanup_without_discovery(index in 0u32..100, deadline in any::<bool>(), replace in any::<bool>(), slots in 1usize..65) {
        runtime().block_on(async {
            tokio::time::pause();
            let (broker, homes) = fixture();
            let registry = DiscoveryRegistry::default();
            let queue = queue(index);
            let old_home = homes.local_home(&queue).unwrap();
            if deadline { *old_home.deadline.lock().unwrap() = Some(Instant::now() + Duration::from_secs(1)); }
            let hint = task(&queue, index);
            let ready_hint = task(&queue, index + 1000);
            broker.publish_workflow_task(hint.clone(), None).await;
            broker.publish_workflow_task(ready_hint.clone(), None).await;
            let stamp = broker.try_take(&queue, None, &WorkerIdentity("worker".into()), None).await.unwrap().unwrap().1;
            let old_reservation = broker.reserve_offers(&old_home, slots).await;
            let _old_demand = registry.register_poll(queue.clone(), old_home.clone(), Instant::now());
            let stop = CancellationToken::new();
            let worker = tokio::spawn(broker.clone().run_retirement(registry.clone(), stop.clone()));
            let retired = broker.capacity_changed.notified();
            tokio::pin!(retired);
            retired.as_mut().enable();
            if deadline { tokio::time::advance(Duration::from_secs(1)).await; }
            else { homes.owner.write().unwrap().mark_draining(old_home.id); }
            if replace {
                let mut owner = homes.owner.write().unwrap();
                owner.record_acquired(old_home.id, ShardEpoch::ZERO);
                owner.mark_active(old_home.id);
            }
            let replacement = if replace {
                let new_home = homes.local_home(&queue).unwrap();
                broker.publish_workflow_task(hint.clone(), None).await;
                Some((broker.reserve_offers(&new_home, slots).await, registry.register_poll(queue.clone(), new_home, Instant::now())))
            } else { None };
            tokio::time::timeout(Duration::from_secs(2), retired).await.unwrap();
            prop_assert!(stamp.cancel.unwrap().is_cancelled());
            prop_assert!(!broker.inner.lock().await.acquisitions.contains_key(&(old_home.id, old_home.generation)));
            prop_assert_eq!(registry.registered_ranges(), usize::from(replace));
            prop_assert!(!broker.inner.lock().await.general_ready[&queue].iter().any(|entry| entry.task.run_key == ready_hint.run_key));
            broker.retire_acquisition(&old_home, "cancelled").await;
            drop(old_reservation);
            broker.finish_offer((hint.run_key, hint.logical_seq), stamp.entered_at).await;
            if replace {
                prop_assert!(broker.try_claim_workflow_task(&queue, hint.run_key).await.is_some());
                prop_assert_eq!(broker.inner.lock().await.offers.available(old_home.id), crate::workflow_offers::HOME_CAPACITY - slots - 1);
            } else { prop_assert!(!broker.inner.lock().await.offers.contains((hint.run_key, hint.logical_seq))); }
            drop(replacement);
            stop.cancel();
            worker.await.unwrap();
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 5: a remote sticky alias cannot claim a locally retained normal offer.
    #[test]
    fn property_5_sticky_poll_requires_its_home(index in 0u32..100) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let normal = queue(index);
            let sticky = (index + 1..index + 100).map(queue).find(|q| home_id(q) != home_id(&normal)).unwrap();
            homes.owner.write().unwrap().mark_draining(home_id(&sticky));
            let hint = task(&normal, index);
            broker.publish_workflow_task(hint.clone(), None).await;
            let worker = WorkerIdentity("worker".into());
            prop_assert!(broker.try_take(&sticky, Some(&normal), &worker, None).await.unwrap().is_none());
            prop_assert!(broker.try_claim_workflow_task_for_worker_with_normal(&sticky, Some(&normal), hint.run_key, Some(&worker)).await.is_none());
            prop_assert!(broker.try_claim_workflow_task(&normal, hint.run_key).await.is_some());
            Ok(())
        })?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: queue-home-delivery, Property 5: renewal postpones cleanup, but its absolute deadline still wakes it.
    #[test]
    fn property_5_renewed_deadline(index in 0u32..100, extension in 2u64..5) {
        runtime().block_on(async {
            tokio::time::pause();
            let (broker, homes) = fixture();
            let queue = queue(index);
            let home = homes.local_home(&queue).unwrap();
            let origin = Instant::now();
            *home.deadline.lock().unwrap() = Some(origin + Duration::from_secs(1));
            broker.publish_workflow_task(task(&queue, index), None).await;
            let stop = CancellationToken::new();
            let work = tokio::spawn(broker.clone().run_retirement(DiscoveryRegistry::default(), stop.clone()));
            let mut deadline_wait = Box::pin(home.retired());
            poll_fn(|cx| { assert!(deadline_wait.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
            *home.deadline.lock().unwrap() = Some(origin + Duration::from_secs(extension));
            tokio::time::advance(Duration::from_secs(1)).await;
            poll_fn(|cx| { assert!(deadline_wait.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
            prop_assert!(broker.inner.lock().await.offers.contains((task(&queue, index).run_key, LogicalTaskSeq(1))));
            let changed = broker.capacity_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            tokio::time::advance(Duration::from_secs(extension - 1)).await;
            prop_assert_eq!(deadline_wait.await, "deadline");
            tokio::time::timeout(Duration::from_secs(1), changed).await.unwrap();
            prop_assert!(!broker.inner.lock().await.offers.contains((task(&queue, index).run_key, LogicalTaskSeq(1))));
            stop.cancel();
            work.await.unwrap();
            Ok(())
        })?;
    }

    // Feature: queue-home-delivery, Property 5: a caller's stale page reservation cannot authorize the receiver's new acquisition.
    #[test]
    fn property_5_stale_page_reservation(index in 0u32..100, slots in 1usize..65) {
        runtime().block_on(async {
            let (broker, homes) = fixture();
            let queue = queue(index);
            let old = homes.local_home(&queue).unwrap();
            let mut reservation = broker.reserve_offers(&old, slots).await;
            {
                let mut owner = homes.owner.write().unwrap();
                owner.record_acquired(old.id, ShardEpoch::ZERO);
                owner.mark_active(old.id);
            }
            let hint = task(&queue, index);
            prop_assert_eq!(broker.publish_discovered(hint.clone(), OffsetDateTime::UNIX_EPOCH, &mut reservation).await, OfferAdmission::NotHome);
            prop_assert_eq!(reservation.remaining(), 0);
            prop_assert_eq!(broker.admit_workflow_offer(hint, OfferSource::Pushed, None, None, false).await, OfferAdmission::Admitted);
            broker.retire_acquisition(&old, "cancelled").await;
            prop_assert_eq!(broker.inner.lock().await.offers.available(old.id), crate::workflow_offers::HOME_CAPACITY - 1);
            Ok(())
        })?;
    }
}

async fn notification_pending(notify: &Notify) -> bool {
    let mut notified = Box::pin(notify.notified());
    poll_fn(|cx| Poll::Ready(notified.as_mut().poll(cx).is_ready())).await
}

#[tokio::test]
async fn retirement_wakes_only_for_new_acquisitions() {
    let (broker, homes) = fixture();
    let queue = queue(0);
    let home = homes.local_home(&queue).unwrap();
    drop(broker.reserve_offers(&home, 1).await);
    assert!(notification_pending(&broker.retirement_changed).await);
    drop(broker.reserve_offers(&home, 1).await);
    assert!(
        !notification_pending(&broker.retirement_changed).await,
        "repeat reservation woke retirement"
    );
    broker.publish_workflow_task(task(&queue, 0), None).await;
    assert!(
        !notification_pending(&broker.retirement_changed).await,
        "admission on a watched home woke retirement"
    );
    {
        let mut owner = homes.owner.write().unwrap();
        owner.record_acquired(home.id, ShardEpoch::ZERO);
        owner.mark_active(home.id);
    }
    broker.publish_workflow_task(task(&queue, 0), None).await;
    assert!(notification_pending(&broker.retirement_changed).await);
    broker.publish_workflow_task(task(&queue, 0), None).await;
    assert!(!notification_pending(&broker.retirement_changed).await);
    let registry = DiscoveryRegistry::default();
    let home = homes.local_home(&queue).unwrap();
    let _first = registry.register_poll(queue.clone(), home.clone(), Instant::now());
    assert!(notification_pending(&registry.retirement_wake).await);
    let _second = registry.register_poll(queue, home, Instant::now());
    assert!(!notification_pending(&registry.retirement_wake).await);
}

#[derive(Debug)]
struct CountingHomes {
    homes: Arc<PlacementQueueHomes>,
    calls: AtomicUsize,
}

impl QueueHomeProvider for CountingHomes {
    fn local_home(&self, queue: &QueueKey) -> Option<QueueHome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.homes.local_home(queue)
    }
}

#[tokio::test]
async fn expiry_does_not_query_placement_per_offer() {
    let (_, homes) = fixture();
    let counted = Arc::new(CountingHomes {
        homes: homes.clone(),
        calls: AtomicUsize::new(0),
    });
    let broker = InMemoryBroker::with_discovery(counted.clone());
    let queue = queue(0);
    for id in 0..32 {
        broker.publish_workflow_task(task(&queue, id), None).await;
    }
    counted.calls.store(0, Ordering::SeqCst);
    broker.expire_offers(Instant::now()).await;
    assert_eq!(
        counted.calls.load(Ordering::SeqCst),
        0,
        "expiry queried placement"
    );
    broker.publish_workflow_task(task(&queue, 32), None).await;
    assert_eq!(
        counted.calls.load(Ordering::SeqCst),
        2,
        "publication inspected other offers' homes"
    );
    let old = homes.local_home(&queue).unwrap();
    {
        let mut owner = homes.owner.write().unwrap();
        owner.record_acquired(old.id, ShardEpoch::ZERO);
        owner.mark_active(old.id);
    }
    assert!(
        broker
            .try_claim_workflow_task(&queue, task(&queue, 0).run_key)
            .await
            .is_none()
    );
    broker.retire_acquisition(&old, "cancelled").await;
    assert_eq!(
        broker.inner.lock().await.offers.available(old.id),
        crate::workflow_offers::HOME_CAPACITY
    );
}

#[derive(Debug)]
struct ElapsedClock {
    origin: Instant,
    wall: OffsetDateTime,
}

impl Clock for ElapsedClock {
    fn now(&self) -> OffsetDateTime {
        self.wall + time::Duration::try_from(self.origin.elapsed()).unwrap()
    }
}

#[tokio::test(start_paused = true)]
async fn local_fallback_preserves_absolute_sticky_deadline() {
    let (broker, _) = fixture();
    let wall = OffsetDateTime::UNIX_EPOCH;
    let broker = broker.with_clock(Arc::new(ElapsedClock {
        origin: Instant::now(),
        wall,
    }));
    let sticky = queue(0);
    let normal = queue(1);
    let deadline = wall + time::Duration::seconds(3);
    let hint = DispatchableWorkflowTask {
        sticky_preferred: Some(WorkerIdentity("unavailable".into())),
        normal_queue: Some(normal.clone()),
        sticky_deadline: Some(deadline),
        ..task(&sticky, 0)
    };
    broker.publish_workflow_task(hint.clone(), None).await;
    let revision = {
        let inner = broker.inner.lock().await;
        let entry = inner.general_ready[&normal].front().unwrap();
        assert_eq!(entry.task.sticky_deadline, Some(deadline));
        assert!(entry.task.sticky_preferred.is_none());
        entry.entered_at
    };
    tokio::time::advance(Duration::from_secs(2)).await;
    broker.publish_workflow_task(hint.clone(), None).await;
    {
        let inner = broker.inner.lock().await;
        let entry = inner.general_ready[&normal].front().unwrap();
        assert_eq!(entry.entered_at, revision);
        assert_eq!(entry.task.sticky_deadline, Some(deadline));
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    broker.expire_offers(Instant::now()).await;
    assert!(broker.inner.lock().await.general_ready[&normal].is_empty());
    assert!(!broker.knows_offer((hint.run_key, hint.logical_seq)).await);
    assert_eq!(
        broker
            .admit_workflow_offer(hint, OfferSource::Local, None, None, true)
            .await,
        OfferAdmission::Expired
    );
}

#[tokio::test(start_paused = true)]
async fn poll_skips_retired_entries_before_current_generation() {
    for sticky in [false, true] {
        let (broker, homes) = fixture();
        let queue = queue(0);
        let worker = WorkerIdentity("worker".into());
        broker.record_workflow_poller(&queue, &worker).await;
        for id in 0..2 {
            let hint = DispatchableWorkflowTask {
                sticky_preferred: sticky.then(|| worker.clone()),
                ..task(&queue, id)
            };
            broker.publish_workflow_task(hint, None).await;
        }
        let old = homes.local_home(&queue).unwrap();
        {
            let mut owner = homes.owner.write().unwrap();
            owner.record_acquired(old.id, ShardEpoch::ZERO);
            owner.mark_active(old.id);
        }
        let current = DispatchableWorkflowTask {
            sticky_preferred: sticky.then(|| worker.clone()),
            ..task(&queue, 2)
        };
        broker.publish_workflow_task(current.clone(), None).await;
        let result = broker
            .poll_workflow_task(&queue, &worker, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            matches!(result, Some(WorkflowPollResult::Queued(ref task, _)) if task.run_key == current.run_key),
            "a single poll stopped at retired entries: {result:?}"
        );
    }
}
