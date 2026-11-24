//! Cancellation must invalidate paused pages and remove their registrations.
//! Reading the registry directly avoids making discovery perform the cleanup.

use proptest::{
    prelude::*,
    test_runner::{Config, FileFailurePersistence, RngAlgorithm, TestError, TestRng, TestRunner},
};

use super::*;
use crate::discovery::tests::{Homes, home};

fn queue() -> QueueKey {
    QueueKey {
        namespace_id: tokeira_types::NamespaceId(uuid::Uuid::from_u128(43)),
        task_queue: tokeira_types::TaskQueueName("retirement".into()),
        task_kind: tokeira_types::TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

async fn cancellation(copies: usize) -> (bool, bool) {
    let old = home(1);
    let broker = InMemoryActivityBroker::with_discovery(Arc::new(Homes(Mutex::new(old.clone()))));
    let registry = ActivityDiscoveryRegistry::default();
    let stop = CancellationToken::new();
    let work = tokio::spawn(
        broker
            .clone()
            .run_retirement(registry.clone(), stop.clone()),
    );
    let demands = (0..copies)
        .map(|_| {
            registry
                .register_poll(queue(), old.clone(), Instant::now())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let (_, _, pass) = registry
        .select(Instant::now(), OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let mut reservation = broker.reserve_offers(&old, &queue(), copies).await;
    reservation.cancel = pass.cancel.clone();
    assert_eq!(reservation.remaining(), copies);
    let changed = broker.offer_capacity_changed();
    tokio::pin!(changed);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(changed.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    old.cancel.cancel();
    changed.await;
    let removed = registry.state.lock().unwrap().ranges.is_empty();
    let page_cancelled = reservation.cancel.is_cancelled();
    // Hold both demand and reservation across the observation. Dropping demand
    // or completing the page would otherwise provide an incidental cleanup path.
    drop(demands);
    drop(reservation);
    stop.cancel();
    work.await.unwrap();
    (removed, page_cancelled)
}

// Feature: activity-queue-home-delivery, Property 3: Prompt isolated retirement.
#[test]
fn activity_queue_home_cancelled_registration_and_reservation() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 100,
            source_file: Some(file!()),
            failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
                "proptest-regressions",
            ))),
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[41; 32]),
    );
    let checked = std::cell::Cell::new(0);
    let failures = std::cell::Cell::new(0);
    runner
        .run(&(1usize..64), |copies| {
            let observed = rt.block_on(cancellation(copies));
            if observed != (true, true) {
                failures.set(failures.get() + 1);
            }
            checked.set(checked.get() + 1);
            Ok(())
        })
        .unwrap();
    assert!(checked.get() >= 100);
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
        TestRng::from_seed(RngAlgorithm::ChaCha, &[41; 32]),
    );
    let failure = runner.run(&(1usize..64), |copies| {
        let (removed, page_cancelled) = rt.block_on(cancellation(copies));
        if removed && page_cancelled { Ok(()) } else { Err(TestCaseError::fail(format!("cancelled acquisition: registration_removed={removed}, paused_page_reservation_cancelled={page_cancelled}; no discovery tick"))) }
    });
    let Err(TestError::Fail(reason, minimal)) = failure else {
        panic!("missing counterexample: {failure:?}")
    };
    assert_eq!(minimal, 1);
    panic!(
        "{} generated cases; minimal copies={minimal}: {reason}",
        checked.get()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 3: Prompt isolated retirement.
    #[test]
    fn activity_queue_home_registration_controls(copies in 1usize..64) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let registry = ActivityDiscoveryRegistry::default();
            let old = home(1);
            let old_demand = registry.register_poll(queue(), old.clone(), Instant::now()).unwrap();
            let (_, _, old_pass) = registry.select(Instant::now(), OffsetDateTime::UNIX_EPOCH).unwrap();
            old.cancel.cancel();
            let mut replacement = home(old.id.0);
            replacement.generation = old.generation + 1;
            let other = home(old.id.0 + 1);
            let mut demands = Vec::new();
            for acquisition in [&replacement, &other] {
                for _ in 0..copies { demands.push(registry.register_poll(queue(), acquisition.clone(), Instant::now()).unwrap()); }
            }
            registry.retire(Instant::now());
            assert!(old_pass.cancel.is_cancelled());
            let snapshot = registry.state.lock().unwrap().ranges.values()
                .map(|entry| (entry.cancel.is_cancelled(), entry.demands.values().sum::<usize>()))
                .collect::<Vec<_>>();
            assert_eq!(snapshot.len(), 2);
            assert!(snapshot.iter().all(|entry| *entry == (false, copies)));
            drop(old_demand);
            assert_eq!(registry.state.lock().unwrap().ranges.len(), 2);
            drop(demands);
        });
    }
}

#[derive(Debug)]
struct PageHomes(Mutex<HashMap<QueueKey, QueueHome>>);
impl crate::discovery::QueueHomeProvider for PageHomes {
    fn local_home(&self, queue: &QueueKey) -> Option<QueueHome> {
        self.0.lock().unwrap().get(queue).cloned()
    }
}

struct PausedPage {
    row: ActivityDispatchRow,
    release: Notify,
}
#[async_trait]
impl ActivityDiscoverySource for PausedPage {
    async fn page(
        &self,
        _: &ActivityDiscoveryRange,
        _: OffsetDateTime,
        _: Option<ActivityDispatchPosition>,
        _: NonZeroU32,
    ) -> Result<ActivityDispatchPage> {
        self.release.notified().await;
        Ok(ActivityDispatchPage {
            candidates: vec![tokeira_storage::ActivityDispatchCandidate {
                position: self.row.position(),
                row: Ok(self.row.clone()),
            }],
            last_examined: Some(self.row.position()),
            exhausted: true,
        })
    }
    async fn resolve(&self, row: &ActivityDispatchRow) -> Result<Option<DispatchableActivityTask>> {
        Ok(Some(row.task()))
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: activity-queue-home-delivery, Property 3: paused page results and drops cannot reclaim a replacement's reserved capacity or registration.
    #[test]
    fn activity_page_retirement_barrier(loss in 0u8..3, slots in 1usize..64, copies in 1usize..8) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let selected = queue();
            let mut other_queue = queue(); other_queue.task_queue.0.push_str("-other");
            let old = home(1); let other = home(2);
            let origin = Instant::now();
            *old.deadline.lock().unwrap() = Some(origin + std::time::Duration::from_millis(100));
            let homes = Arc::new(PageHomes(Mutex::new(HashMap::from([(selected.clone(), old.clone()), (other_queue.clone(), other.clone())]))));
            let broker = InMemoryActivityBroker::with_discovery(homes.clone());
            let registry = ActivityDiscoveryRegistry::default();
            let stop = CancellationToken::new();
            let work = tokio::spawn(broker.clone().run_retirement(registry.clone(), stop.clone()));
            let old_demand = registry.register_poll(selected.clone(), old.clone(), Instant::now()).unwrap();
            let (key, _, mut pass) = registry.select(Instant::now(), OffsetDateTime::UNIX_EPOCH).unwrap();
            let source = PausedPage { row: ActivityDispatchRow {
                key: uuid::Uuid::from_u128(1), incarnation: tokeira_storage::ActivityIncarnation { run_key: tokeira_types::RunKey(uuid::Uuid::from_u128(1)), activity_id: "activity".into(), schedule_event_id: 1, attempt: 1, stamp: 0 },
                queue_key: ActivityDiscoveryRange { namespace_id: selected.namespace_id, queue_name: selected.task_queue.clone() }.lookup_key(), queue: selected.clone(), execution_home: ShardId(0), priority: None, dispatch_revision: 0, dispatch_at: OffsetDateTime::UNIX_EPOCH,
            }, release: Notify::new() };
            let clock = crate::test_clock::TestClock::new();
            let mut page = Box::pin(run_slice_at(&source, &registry, &broker, &key, &old, &mut pass, &clock));
            std::future::poll_fn(|cx| { assert!(std::future::Future::poll(page.as_mut(), cx).is_pending()); std::task::Poll::Ready(()) }).await;
            prop_assert_eq!(broker.offer_accounting(&old).await, (0, 0, PASS_ADMISSIONS));
            let changed = broker.offer_capacity_changed(); tokio::pin!(changed);
            std::future::poll_fn(|cx| { assert!(std::future::Future::poll(changed.as_mut(), cx).is_pending()); std::task::Poll::Ready(()) }).await;
            match loss {
                0 => old.cancel.cancel(),
                1 => tokio::time::advance(std::time::Duration::from_millis(100)).await,
                _ => {
                    *old.deadline.lock().unwrap() = Some(origin + std::time::Duration::from_millis(200));
                    tokio::time::advance(std::time::Duration::from_millis(100)).await;
                    prop_assert_eq!(registry.state.lock().unwrap().ranges.len(), 1);
                    prop_assert_eq!(broker.offer_accounting(&old).await, (0, 0, PASS_ADMISSIONS));
                    tokio::time::advance(std::time::Duration::from_millis(100)).await;
                }
            }
            changed.await;
            prop_assert!(registry.state.lock().unwrap().ranges.is_empty());
            prop_assert_eq!(broker.offer_accounting(&old).await, (0, 0, 0));
            let mut replacement = home(old.id.0); replacement.generation = old.generation + 1;
            homes.0.lock().unwrap().insert(selected.clone(), replacement.clone());
            let mut demands = Vec::new();
            for (queue, acquisition) in [(&selected, &replacement), (&other_queue, &other)] {
                for _ in 0..copies { demands.push(registry.register_poll(queue.clone(), acquisition.clone(), Instant::now()).unwrap()); }
            }
            let reserved = broker.reserve_offers(&replacement, &selected, slots).await;
            let other_reserved = broker.reserve_offers(&other, &other_queue, slots).await;
            source.release.notify_one();
            prop_assert_eq!(page.as_mut().await.unwrap(), SliceOutcome::Cancelled);
            drop(page);
            registry.complete(&key, SliceOutcome::Complete, pass, Instant::now());
            drop(old_demand);
            broker.retire_acquisition(&registry, &old, "deadline").await;
            prop_assert_eq!(broker.offer_accounting(&replacement).await, (0, 0, slots));
            prop_assert_eq!(broker.offer_accounting(&other).await, (0, 0, slots));
            let snapshot: Vec<_> = registry.state.lock().unwrap().ranges.values().map(|entry| (entry.cancel.is_cancelled(), entry.demands.values().sum::<usize>(), entry.pass.is_none())).collect();
            prop_assert_eq!(snapshot, vec![(false, copies, true); 2]);
            drop(reserved); drop(other_reserved); drop(demands);
            stop.cancel(); work.await.unwrap();
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn activity_registration_wakes_retirement_once() {
    let registry = ActivityDiscoveryRegistry::default();
    let acquisition = home(1);
    let first = registry
        .register_poll(queue(), acquisition.clone(), Instant::now())
        .unwrap();
    registry.retirement_wake.notified().await;
    let second = registry
        .register_poll(queue(), acquisition, Instant::now())
        .unwrap();
    let changed = registry.retirement_wake.notified();
    tokio::pin!(changed);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(changed.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(first);
    drop(second);
}

#[tokio::test(start_paused = true)]
async fn activity_demand_only_retirement_without_tick() {
    let old = home(1);
    let broker = InMemoryActivityBroker::with_discovery(Arc::new(Homes(Mutex::new(old.clone()))));
    let registry = ActivityDiscoveryRegistry::default();
    let stop = CancellationToken::new();
    let work = tokio::spawn(
        broker
            .clone()
            .run_retirement(registry.clone(), stop.clone()),
    );
    let demand = registry
        .register_poll(queue(), old.clone(), Instant::now())
        .unwrap();
    let changed = broker.offer_capacity_changed();
    tokio::pin!(changed);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(changed.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    old.cancel.cancel();
    changed.await;
    assert!(registry.state.lock().unwrap().ranges.is_empty());
    assert!(registry.acquisitions().is_empty());
    drop(demand);
    stop.cancel();
    work.await.unwrap();
}
