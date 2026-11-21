//! Acquisition retirement with the consumer and discovery futures held still.
//! Raw retained-set observations never invoke opportunistic broker cleanup.

use std::{future::Future, task::Poll};

use metrics_util::debugging::{DebugValue, DebuggingRecorder};

use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngAlgorithm, TestError, TestRng, TestRunner},
};
use tokeira_types::{
    ShardEpoch, ShardId, TaskKind, bundle_for_partition, workflow_queue_partition,
};

use super::*;
use crate::{discovery::PlacementQueueHomes, shard::ShardOwner};

fn queue(index: u32) -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId(uuid::Uuid::from_u128(23)),
        task_queue: TaskQueueName(format!("retirement-{index}")),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

fn home(queue: &QueueKey) -> ShardId {
    bundle_for_partition(
        workflow_queue_partition(queue.namespace_id, &queue.task_queue.0, 8),
        8,
    )
}

fn task(queue: &QueueKey, id: u32) -> DispatchableActivityTask {
    DispatchableActivityTask {
        run_key: RunKey(uuid::Uuid::from_u128(u128::from(id) + 1)),
        queue: queue.clone(),
        activity_id: "activity".into(),
        schedule_event_id: 7,
        attempt: 1,
        stamp: 0,
        dispatch_revision: 0,
        priority: None,
        order: None,
    }
}

fn fixture(
    index: u32,
) -> (
    InMemoryActivityBroker,
    Arc<PlacementQueueHomes>,
    QueueKey,
    QueueKey,
) {
    let selected = queue(index);
    let other = (index + 1..index + 256)
        .map(queue)
        .find(|q| home(q) != home(&selected))
        .unwrap();
    let mut owner = ShardOwner::new(8);
    for id in [home(&selected), home(&other)] {
        owner.record_acquired(id, ShardEpoch(1));
        owner.mark_active(id);
    }
    let homes = Arc::new(PlacementQueueHomes {
        owner: Arc::new(std::sync::RwLock::new(owner)),
        counts: std::sync::RwLock::new((8, 8)),
    });
    (
        InMemoryActivityBroker::with_discovery(homes.clone()),
        homes,
        selected,
        other,
    )
}

async fn expired(index: u32) -> bool {
    let (broker, homes, selected, _) = fixture(index);
    let acquisition = homes.local_home(&selected).unwrap();
    let worker = WorkerIdentity("waiting-worker".into());
    let poll = broker.poll_activity_offer_for_worker(&selected, &worker, Duration::from_secs(30));
    tokio::pin!(poll);
    std::future::poll_fn(|cx| {
        assert!(poll.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    broker
        .publish_activity_task(task(&selected, index), None)
        .await
        .unwrap();
    // The offer is younger than its retention lease; only the acquisition has
    // expired. Keep the waiting consumer unpolled until after that boundary.
    let elapsed = Duration::from_millis(u64::from(index));
    *acquisition.deadline.lock().unwrap() = Some(Instant::now() + elapsed);
    tokio::time::advance(elapsed).await;
    assert!(!acquisition.cancel.is_cancelled());
    assert!(!acquisition.valid());
    std::future::poll_fn(|cx| {
        Poll::Ready(match poll.as_mut().poll(cx) {
            Poll::Ready(Ok(Some(_))) => false,
            Poll::Pending | Poll::Ready(Ok(None)) => true,
            Poll::Ready(Err(error)) => panic!("poll failed: {error}"),
        })
    })
    .await
}

async fn cancelled(index: u32) -> bool {
    let (broker, homes, selected, other) = fixture(index);
    let stop = tokio_util::sync::CancellationToken::new();
    let work = tokio::spawn(
        broker
            .clone()
            .run_retirement(Default::default(), stop.clone()),
    );
    let acquisition = homes.local_home(&selected).unwrap();
    let ready = task(&selected, index);
    let flight = task(&selected, index + 1000);
    let unrelated = task(&other, index + 2000);
    for offer in [&ready, &flight, &unrelated] {
        broker
            .publish_activity_task(offer.clone(), None)
            .await
            .unwrap();
    }
    let held = broker
        .try_claim_activity_offer(&selected, flight.run_key, "activity")
        .await
        .unwrap();
    let reservation = broker.reserve_offers(&acquisition, &selected, 13).await;
    assert_eq!(reservation.remaining(), 13);
    let changed = broker.capacity_changed.notified();
    tokio::pin!(changed);
    changed.as_mut().enable();
    acquisition.cancel.cancel();
    changed.await;
    let retired = {
        let inner = broker.inner.lock().await;
        assert!(inner.offers.contains(&incarnation(&unrelated)));
        !inner.offers.contains(&incarnation(&ready))
            && !inner.offers.contains(&incarnation(&flight))
    };
    assert!(
        reservation.cancel.is_cancelled(),
        "an outstanding page must be invalidated"
    );
    drop(held);
    drop(reservation);
    stop.cancel();
    work.await.unwrap();
    retired
}

fn explore(check: impl Fn(u32) -> bool, reason: &'static str) {
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[31; 32]),
    );
    let checked = std::cell::Cell::new(0);
    let failures = std::cell::Cell::new(0);
    runner
        .run(&(1u32..64), |index| {
            if !check(index) {
                failures.set(failures.get() + 1);
            }
            checked.set(checked.get() + 1);
            Ok(())
        })
        .unwrap();
    let checked = checked.get();
    assert!(checked >= 100);
    if failures.get() == 0 {
        return;
    }
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[31; 32]),
    );
    let failure = runner.run(&(1u32..64), |index| {
        if check(index) {
            Ok(())
        } else {
            Err(TestCaseError::fail(reason))
        }
    });
    let Err(TestError::Fail(reason, minimal)) = failure else {
        panic!("missing counterexample: {failure:?}")
    };
    assert_eq!(minimal, 1);
    panic!("{checked} generated cases; minimal index={minimal}: {reason}");
}

// Feature: activity-queue-home-delivery, Property 3: Prompt isolated retirement.
#[test]
fn activity_queue_home_expired_deadline() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    explore(
        |index| rt.block_on(expired(index)),
        "waiting poll took an offer from an expired, uncancelled acquisition",
    );
}

// Feature: activity-queue-home-delivery, Property 3: Prompt isolated retirement.
#[test]
fn activity_queue_home_cancelled_without_discovery() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    explore(
        |index| rt.block_on(cancelled(index)),
        "cancelled acquisition retained ready and in-flight offers without discovery or broker cleanup",
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 3: Prompt isolated retirement.
    #[test]
    fn activity_queue_home_replacement_and_colocated_controls(index in 1u32..64, reserved in 1usize..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (broker, homes, selected, other) = fixture(index);
            let old = homes.local_home(&selected).unwrap();
            let stale = task(&selected, index);
            broker.publish_activity_task(stale.clone(), None).await.unwrap();
            let old_lease = broker.try_claim_activity_offer(&selected, stale.run_key, "activity").await.unwrap();
            let reservation = broker.reserve_offers(&old, &selected, reserved).await;
            old.cancel.cancel();
            { let mut owner = homes.owner.write().unwrap(); owner.record_acquired(old.id, ShardEpoch(1)); owner.mark_active(old.id); }
            let replacement = homes.local_home(&selected).unwrap();
            assert_ne!(old.generation, replacement.generation);
            let unrelated = task(&other, index + 1000);
            for task in [&stale, &unrelated] { broker.publish_activity_task(task.clone(), None).await.unwrap(); }
            broker.retire_offers(&old, &selected).await;
            drop(old_lease);
            drop(reservation);
            assert!(broker.try_claim_activity_offer(&selected, stale.run_key, "activity").await.is_some());
            assert!(broker.try_claim_activity_offer(&other, unrelated.run_key, "activity").await.is_some());
        });
    }
}

#[derive(Debug)]
struct MovingHomes(std::sync::RwLock<BTreeMap<ShardId, QueueHome>>);
impl QueueHomeProvider for MovingHomes {
    fn local_home(&self, queue: &QueueKey) -> Option<QueueHome> {
        self.0.read().unwrap().get(&home(queue)).cloned()
    }
}

async fn pending(future: std::pin::Pin<&mut impl Future>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 3: locked admission, reservation and both take paths observe renewed or retired ownership before delayed cleanup.
    #[test]
    fn activity_retirement_locked_barriers(index in 1u32..64, boundary in 0u8..4, loss in 0u8..4, slots in 1usize..32) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (_, initial, selected, other) = fixture(index);
            let old = initial.local_home(&selected).unwrap();
            let unrelated_home = initial.local_home(&other).unwrap();
            let homes = Arc::new(MovingHomes(std::sync::RwLock::new(BTreeMap::from([(old.id, old.clone()), (unrelated_home.id, unrelated_home.clone())]))));
            let broker = InMemoryActivityBroker::with_discovery(homes.clone());
            let registry = crate::activity_discovery::ActivityDiscoveryRegistry::default();
            let stop = tokio_util::sync::CancellationToken::new();
            let work = tokio::spawn(broker.clone().run_retirement(registry.clone(), stop.clone()));
            let origin = Instant::now();
            *old.deadline.lock().unwrap() = Some(origin + Duration::from_millis(100));
            let ready = task(&selected, index);
            let flight = task(&selected, index + 1000);
            let unrelated = task(&other, index + 2000);
            let unrelated_flight = task(&other, index + 4000);
            for task in [&ready, &flight, &unrelated, &unrelated_flight] { broker.publish_activity_task(task.clone(), None).await.unwrap(); }
            let unrelated_held = broker.try_claim_activity_offer(&other, unrelated_flight.run_key, "activity").await.unwrap();
            let held = broker.try_claim_activity_offer(&selected, flight.run_key, "activity").await.unwrap();
            let old_reservation = broker.reserve_offers(&old, &selected, slots).await;
            let other_reservation = broker.reserve_offers(&unrelated_home, &other, slots).await;
            let old_token = broker.inner.lock().await.ready[&selected].front().unwrap().token;
            let mut replacement = old.clone();
            replacement.generation += 1;
            replacement.cancel = Default::default();
            replacement.deadline = Default::default();
            let lock = broker.inner.lock().await;
            let admission = task(&selected, index + 3000);
            let operation = async {
                match boundary {
                    0 => broker.admit_activity_offer(crate::ActivityQueueOffer { task: admission.clone(), execution_home: ShardId(0), dispatch_at: OffsetDateTime::UNIX_EPOCH, expires_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5) }, Some(&old), OfferSource::Pushed, OffsetDateTime::UNIX_EPOCH, None).await == crate::ActivityOfferOutcome::Admitted,
                    1 => matches!(broker.try_take(&selected).await.unwrap(), ActivityTakeOutcome::Ready(_)),
                    2 => broker.try_claim_activity_offer(&selected, ready.run_key, "activity").await.is_some(),
                    _ => broker.reserve_offers(&old, &selected, 1).await.remaining() == 1,
                }
            };
            tokio::pin!(operation);
            pending(operation.as_mut()).await;
            match loss {
                0 => old.cancel.cancel(),
                1 => tokio::time::advance(Duration::from_millis(100)).await,
                2 => {
                    *old.deadline.lock().unwrap() = Some(origin + Duration::from_secs(1));
                    tokio::time::advance(Duration::from_millis(100)).await;
                }
                _ => { homes.0.write().unwrap().insert(old.id, replacement.clone()); }
            }
            drop(lock);
            prop_assert_eq!(operation.await, loss == 2);
            if loss == 3 { prop_assert!(old.valid(), "provider replacement must be checked independently of cancellation"); }
            // A cleanup result may arrive after a new generation has already
            // admitted the same incarnation. Keep every old guard alive across it.
            homes.0.write().unwrap().insert(old.id, replacement.clone());
            old.cancel.cancel();
            for task in [&ready, &flight] { broker.publish_activity_task(task.clone(), None).await.unwrap(); }
            let replacement_held = broker.try_claim_activity_offer(&selected, flight.run_key, "activity").await.unwrap();
            let replacement_reservation = broker.reserve_offers(&replacement, &selected, slots).await;
            let lock = broker.inner.lock().await;
            let cleanup = broker.retire_acquisition(&registry, &old, "cancelled");
            tokio::pin!(cleanup);
            pending(cleanup.as_mut()).await;
            prop_assert_eq!(lock.offers.accounting(&replacement), (1, 1, slots));
            drop(lock);
            cleanup.await;
            {
                let mut inner = broker.inner.lock().await;
                prop_assert_eq!(inner.offers.accounting(&old), (0, 0, 0));
                prop_assert_eq!(inner.offers.accounting(&replacement), (1, 1, slots));
                prop_assert_eq!(inner.offers.accounting(&unrelated_home), (1, 1, slots));
                inner.offers.remove_ready(&incarnation(&ready), old_token);
                prop_assert!(inner.offers.contains(&incarnation(&ready)));
            }
            drop(held); drop(old_reservation);
            broker.retire_acquisition(&registry, &old, "cancelled").await;
            {
                let inner = broker.inner.lock().await;
                prop_assert_eq!(inner.offers.accounting(&replacement), (1, 1, slots));
                prop_assert_eq!(inner.offers.accounting(&unrelated_home), (1, 1, slots));
            }
            drop(replacement_held); drop(replacement_reservation); drop(other_reservation); drop(unrelated_held);
            stop.cancel(); work.await.unwrap();
            Ok(())
        })?;
    }

    // Feature: activity-queue-home-delivery, Property 3: push-only deadline watchers reread renewals and detach ready, in-flight and reserved work without a discovery pass.
    #[test]
    fn activity_push_only_deadline_renewal(index in 1u32..64, extension in 2u64..5, slots in 1usize..32) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (broker, homes, selected, other) = fixture(index);
            let old = homes.local_home(&selected).unwrap();
            let origin = Instant::now();
            *old.deadline.lock().unwrap() = Some(origin + Duration::from_millis(100));
            let stop = tokio_util::sync::CancellationToken::new();
            let retirement = broker.clone().run_retirement(Default::default(), stop.clone());
            tokio::pin!(retirement);
            let ready = task(&selected, index);
            let flight = task(&selected, index + 1000);
            let unrelated = task(&other, index + 2000);
            for task in [&ready, &flight, &unrelated] { broker.publish_activity_task(task.clone(), None).await.unwrap(); }
            let held = broker.try_claim_activity_offer(&selected, flight.run_key, "activity").await.unwrap();
            let reservation = broker.reserve_offers(&old, &selected, slots).await;
            pending(retirement.as_mut()).await;
            let deadline = old.retired(); tokio::pin!(deadline);
            pending(deadline.as_mut()).await;
            *old.deadline.lock().unwrap() = Some(origin + Duration::from_millis(100 * extension));
            tokio::time::advance(Duration::from_millis(100)).await;
            pending(deadline.as_mut()).await;
            pending(retirement.as_mut()).await;
            prop_assert_eq!(broker.inner.lock().await.offers.accounting(&old), (1, 1, slots));
            let changed = broker.capacity_changed.notified(); tokio::pin!(changed); changed.as_mut().enable();
            tokio::time::advance(Duration::from_millis(100 * (extension - 1))).await;
            prop_assert_eq!(deadline.await, "deadline");
            tokio::select! { _ = &mut retirement => panic!("worker stopped"), _ = &mut changed => {} }
            let inner = broker.inner.lock().await;
            prop_assert_eq!(inner.offers.accounting(&old), (0, 0, 0));
            prop_assert!(reservation.cancel.is_cancelled());
            prop_assert!(inner.offers.contains(&incarnation(&unrelated)));
            drop(inner); drop(held); drop(reservation);
            stop.cancel(); retirement.await;
            Ok(())
        })?;
    }
}

#[tokio::test(start_paused = true)]
async fn activity_unmanaged_has_no_retention_or_hint() {
    let broker = InMemoryActivityBroker::default();
    let selected = queue(1);
    for id in 0..8200 {
        broker
            .publish_activity_task(task(&selected, id), None)
            .await
            .unwrap();
    }
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(broker.inner.lock().await.ready[&selected].len(), 8200);
    assert!(broker.inner.lock().await.acquisitions.is_empty());
    for id in 0..8200 {
        let task = task(&selected, id);
        assert!(
            !broker
                .inner
                .lock()
                .await
                .offers
                .contains(&incarnation(&task))
        );
        let taken = broker
            .try_claim_activity_offer(&selected, task.run_key, "activity")
            .await
            .unwrap();
        assert!(taken.execution_home.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn activity_execution_home_zero_is_a_real_hint() {
    let (broker, homes, selected, _) = fixture(1);
    let home = homes.local_home(&selected).unwrap();
    let offer = crate::ActivityQueueOffer {
        task: task(&selected, 1),
        execution_home: ShardId(0),
        dispatch_at: OffsetDateTime::UNIX_EPOCH,
        expires_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5),
    };
    assert_eq!(
        broker
            .admit_activity_offer(
                offer.clone(),
                Some(&home),
                OfferSource::Pushed,
                OffsetDateTime::UNIX_EPOCH,
                None
            )
            .await,
        crate::ActivityOfferOutcome::Admitted
    );
    let taken = broker
        .try_claim_activity_offer(&selected, offer.task.run_key, "activity")
        .await
        .unwrap();
    assert_eq!(taken.execution_home, Some(ShardId(0)));
}

#[tokio::test]
async fn activity_retirement_wakes_only_on_first_registration() {
    let (broker, homes, selected, _) = fixture(1);
    let home = homes.local_home(&selected).unwrap();
    let reservation = broker.reserve_offers(&home, &selected, 1).await;
    broker.retirement_changed.notified().await;
    drop(broker.reserve_offers(&home, &selected, 1).await);
    broker
        .publish_activity_task(task(&selected, 1), None)
        .await
        .unwrap();
    let changed = broker.retirement_changed.notified();
    tokio::pin!(changed);
    pending(changed.as_mut()).await;
    drop(reservation);
}

#[test]
fn activity_retirement_counter_is_scoped_and_idempotent() {
    let recorder = DebuggingRecorder::new();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(async {
                for reason in ["cancelled", "deadline", "shutdown"] {
                    let (broker, _, selected, _) = fixture(1);
                    let registry = crate::activity_discovery::ActivityDiscoveryRegistry::default();
                    let home = broker
                        .queue_homes
                        .as_ref()
                        .unwrap()
                        .local_home(&selected)
                        .unwrap();
                    let demand = registry
                        .register_poll(selected.clone(), home.clone(), Instant::now())
                        .unwrap();
                    broker
                        .publish_activity_task(task(&selected, 1), None)
                        .await
                        .unwrap();
                    if reason == "deadline" {
                        *home.deadline.lock().unwrap() = Some(Instant::now());
                    }
                    if reason == "cancelled" {
                        home.cancel.cancel();
                    }
                    broker.retire_acquisition(&registry, &home, reason).await;
                    broker.retire_acquisition(&registry, &home, reason).await;
                    drop(demand);
                }
            })
    });
    let snapshots = recorder.snapshotter().snapshot().into_vec();
    let retirement = snapshots
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == runtime_metrics::ACTIVITY_OFFER_RETIREMENTS_TOTAL
        })
        .collect::<Vec<_>>();
    assert_eq!(retirement.len(), 3);
    for (key, _, _, value) in retirement {
        let labels = key.key().labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].key(), "reason");
        assert!(["cancelled", "deadline", "shutdown"].contains(&labels[0].value()));
        assert_eq!(*value, DebugValue::Counter(1));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 3: a delayed deadline result revalidates renewal under the cleanup lock and the new deadline still retires without a tick.
    #[test]
    fn activity_renewal_at_cleanup_barrier(index in 1u32..64, slots in 1usize..32, extension in 2u64..10) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let (broker, homes, selected, _) = fixture(index);
            let home = homes.local_home(&selected).unwrap();
            let registry = crate::activity_discovery::ActivityDiscoveryRegistry::default();
            let demand = registry.register_poll(selected.clone(), home.clone(), Instant::now()).unwrap();
            let origin = Instant::now();
            *home.deadline.lock().unwrap() = Some(origin + Duration::from_millis(100));
            let ready = task(&selected, index); let flight = task(&selected, index + 1000);
            for task in [&ready, &flight] { broker.publish_activity_task(task.clone(), None).await.unwrap(); }
            let held = broker.try_claim_activity_offer(&selected, flight.run_key, "activity").await.unwrap();
            let reservation = broker.reserve_offers(&home, &selected, slots).await;
            tokio::time::advance(Duration::from_millis(100)).await;
            let reason = home.retired().await;
            prop_assert_eq!(reason, "deadline");
            let lock = broker.inner.lock().await;
            let cleanup = broker.retire_acquisition(&registry, &home, reason); tokio::pin!(cleanup);
            pending(cleanup.as_mut()).await;
            *home.deadline.lock().unwrap() = Some(origin + Duration::from_millis(100 * extension));
            drop(lock); cleanup.await;
            prop_assert_eq!(broker.offer_accounting(&home).await, (1, 1, slots));
            prop_assert!(!reservation.cancel.is_cancelled());
            prop_assert_eq!(registry.acquisitions().len(), 1);
            let stop = tokio_util::sync::CancellationToken::new();
            let work = tokio::spawn(broker.clone().run_retirement(registry.clone(), stop.clone()));
            let changed = broker.capacity_changed.notified(); tokio::pin!(changed); changed.as_mut().enable();
            tokio::time::advance(Duration::from_millis(100 * (extension - 1))).await;
            changed.await;
            prop_assert_eq!(broker.offer_accounting(&home).await, (0, 0, 0));
            prop_assert!(registry.acquisitions().is_empty());
            prop_assert!(reservation.cancel.is_cancelled());
            drop(reservation); drop(held); drop(demand); stop.cancel(); work.await.unwrap();
            Ok(())
        })?;
    }
}
