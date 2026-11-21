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
    let mut reservation = broker.reserve_offers(&old, copies).await;
    reservation.cancel = pass.cancel.clone();
    assert_eq!(reservation.remaining(), copies);
    let (sent, receipt) = tokio::sync::oneshot::channel();
    let cancelled = old.cancel.clone();
    tokio::spawn(async move {
        cancelled.cancelled().await;
        sent.send(()).unwrap();
    });
    old.cancel.cancel();
    receipt.await.unwrap();
    let removed = registry.state.lock().unwrap().ranges.is_empty();
    let page_cancelled = reservation.cancel.is_cancelled();
    // Hold both demand and reservation across the observation. Dropping demand
    // or completing the page would otherwise provide an incidental cleanup path.
    drop(demands);
    drop(reservation);
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
