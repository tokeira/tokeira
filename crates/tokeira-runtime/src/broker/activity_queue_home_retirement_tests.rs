//! Acquisition retirement with the consumer and discovery futures held still.
//! Raw retained-set observations never invoke opportunistic broker cleanup.

use std::{future::Future, task::Poll};

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
    let reservation = broker.reserve_offers(&acquisition, 13).await;
    assert_eq!(reservation.remaining(), 13);
    let (sent, receipt) = tokio::sync::oneshot::channel();
    let cancelled = acquisition.cancel.clone();
    tokio::spawn(async move {
        cancelled.cancelled().await;
        sent.send(()).unwrap();
    });
    acquisition.cancel.cancel();
    // Cancellation receipt is independent of any discovery or broker method.
    // All offers and the page reservation remain owned by this test meanwhile.
    receipt.await.unwrap();
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
            let reservation = broker.reserve_offers(&old, reserved).await;
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
