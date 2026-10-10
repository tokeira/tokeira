//! Placement conservation, page atomicity and restart contracts for memory.

use proptest::prelude::*;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{TimerOp, TimerState};
use tokeira_types::{RunKey, ShardEpoch, ShardId};
use uuid::Uuid;

use super::{projection_accumulator_tests::fresh_transition, *};
use crate::{
    PlacementPhase,
    placement_upgrade::{MOVE_KEYS, SCREEN_KEYS},
    prepare_execution_placement,
};

async fn seed(
    store: &InMemoryStore,
    index: u32,
    hot_misplaced: bool,
    timer_misplaced: bool,
) -> RunKey {
    let mut transition = fresh_transition(RunKey::new());
    transition.next_state.workflow_id.0 = format!("placement-{index}");
    let home = tokeira_types::execution_home_bundle(
        transition.next_state.namespace_id.0.as_bytes(),
        transition.next_state.workflow_id.0.as_bytes(),
        8,
    );
    let key = RunKey(Uuid::from_u128(
        u128::from(index) * 8 + u128::from((home.0 + 1) % 8),
    ));
    transition.next_state.run_key = key;
    let timer = TimerState {
        started_event_id: 1,
        timer_id: "kept-timer".into(),
        fire_at: OffsetDateTime::UNIX_EPOCH + Duration::days(10),
    };
    transition
        .next_state
        .timers
        .insert(timer.timer_id.clone(), timer.clone());
    transition.timer_ops.push(TimerOp::Upsert(timer.clone()));
    store
        .commit_transition(key, transition, ShardEpoch::ZERO)
        .await
        .unwrap();
    let mut inner = store.inner.lock().await;
    let legacy = ShardId((key.0.as_u128() as u32) % 8);
    if hot_misplaced {
        inner.run_shard_map.insert(key, legacy);
    }
    if timer_misplaced {
        let value = inner
            .timer_bucket
            .remove(&timer_position(key, home, &timer))
            .unwrap();
        inner
            .timer_bucket
            .insert(timer_position(key, legacy, &timer), value);
    }
    inner.workflow_dispatch.remove(&key);
    store.placement_ready.store(false, AtomicOrdering::Release);
    key
}

async fn semantic_snapshot(store: &InMemoryStore) -> Vec<u8> {
    let mut doc = SnapshotDoc::capture(&*store.inner.lock().await);
    doc.run_shard_map.clear();
    postcard::to_allocvec(&doc).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: workflow-dispatch. Physical relocation conserves semantic state through page restarts.
    #[test]
    fn placement_pages_conserve_rows_across_restarts(
        locations in prop::collection::vec((any::<bool>(), any::<bool>()), 1..140),
        interrupt in 0usize..12,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut store = InMemoryStore::with_shard_count(8);
            let mut expected = BTreeMap::new();
            for (index, (hot, timer)) in locations.iter().copied().enumerate() {
                let key = seed(&store, index as u32 + 1, hot, timer).await;
                let inner = store.inner.lock().await;
                let state = &inner.runs[&key];
                expected.insert(key, tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(), state.workflow_id.0.as_bytes(), 8));
            }
            let before = semantic_snapshot(&store).await;
            let mut previous = PlacementProgress::default();
            let mut page = 0;
            loop {
                let snapshot = store.snapshot().await.unwrap();
                if page == interrupt {
                    store.fail_bulk_write_after(0).await;
                    assert!(store.prepare_placement_page().await.is_err());
                    assert_eq!(store.snapshot().await.unwrap(), snapshot, "failed page must publish nothing");
                }
                let PlacementPage::Committed(progress) = store.prepare_placement_page().await.unwrap() else { panic!("memory cannot lose lock"); };
                let moved = progress.hot_moved + progress.timers_moved - previous.hot_moved - previous.timers_moved;
                let examined = progress.hot_examined + progress.timers_examined - previous.hot_examined - previous.timers_examined;
                assert!(examined <= if moved == 0 { SCREEN_KEYS } else { MOVE_KEYS } as u64);
                {
                    let inner = store.inner.lock().await;
                    for cost in &inner.modeled_transactions {
                        assert!(cost.rows <= crate::write_budget::MAX_ROWS_PER_TRANSACTION);
                        assert!(cost.bytes <= crate::write_budget::MAX_BYTES_PER_TRANSACTION);
                    }
                }
                previous = progress.clone();
                page += 1;
                if progress.complete() { break; }
                store = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
                assert!(!store.placement_ready(), "restore cannot bypass preparation");
                assert!(page < 30);
            }
            assert_eq!(semantic_snapshot(&store).await, before);
            let inner = store.inner.lock().await;
            assert_eq!(inner.run_shard_map.iter().map(|(k,v)| (*k,*v)).collect::<BTreeMap<_,_>>(), expected);
            assert_eq!(inner.timer_bucket.len(), locations.len());
            for key in inner.timer_bucket.keys() { assert_eq!(key.shard, shard_uuid(expected[&key.run_key])); }
            drop(inner);
            assert!(store.placement_ready());
            let before = store.snapshot().await.unwrap();
            prepare_execution_placement(&store).await.unwrap();
            assert_eq!(store.snapshot().await.unwrap(), before, "completed marker reads do not scan or write");
            for (key, home) in expected {
                store.reconcile_workflow_dispatch_run(home, key).await.unwrap();
                assert_eq!(store.inner.lock().await.workflow_dispatch[&key].execution_home, home);
            }
        });
    }
}

#[tokio::test]
async fn restored_misplaced_fixture_runs_both_walks_and_preserves_orphans() {
    let store = InMemoryStore::with_shard_count(8);
    seed(&store, 1, true, true).await;
    seed(&store, 2, false, true).await;
    let orphan = TimerPosition {
        shard: Uuid::from_u128(7),
        run_key: RunKey::new(),
        timer_id: "orphan".into(),
        fire_at: OffsetDateTime::UNIX_EPOCH,
    };
    store.inner.lock().await.timer_bucket.insert(
        orphan.clone(),
        TimerState {
            started_event_id: 1,
            timer_id: orphan.timer_id.clone(),
            fire_at: orphan.fire_at,
        },
    );
    let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
    assert!(!restored.placement_ready());
    let progress = prepare_execution_placement(&restored).await.unwrap();
    assert_eq!((progress.hot_moved, progress.timers_moved), (1, 2));
    assert!(
        restored
            .inner
            .lock()
            .await
            .timer_bucket
            .contains_key(&orphan)
    );
}

#[tokio::test]
async fn duplicate_timer_is_retained_but_conflicting_payload_aborts_page() {
    for conflicting in [false, true] {
        let store = InMemoryStore::with_shard_count(8);
        let key = seed(&store, 1, false, true).await;
        {
            let mut inner = store.inner.lock().await;
            let (source, timer) = inner.timer_bucket.first_key_value().unwrap();
            let mut timer = timer.clone();
            if conflicting {
                timer.fire_at += Duration::seconds(1);
            }
            let target = TimerPosition {
                shard: shard_uuid(inner.run_shard_map[&key]),
                ..source.clone()
            };
            inner.timer_bucket.insert(target, timer);
        }
        if conflicting {
            loop {
                let before = store.snapshot().await.unwrap();
                match store.prepare_placement_page().await {
                    Err(error) => {
                        let message = error.to_string();
                        assert!(message.contains("conflicting destination timer payload"));
                        assert!(
                            message.contains("stored_shard") && message.contains("computed_home")
                        );
                        assert_eq!(store.snapshot().await.unwrap(), before);
                        break;
                    }
                    Ok(PlacementPage::Committed(p)) => assert!(!p.complete()),
                    Ok(PlacementPage::Retry) => unreachable!(),
                }
            }
        } else {
            prepare_execution_placement(&store).await.unwrap();
            assert_eq!(store.inner.lock().await.timer_bucket.len(), 1);
        }
    }
}

#[tokio::test]
async fn unexplained_placement_and_invalid_identity_name_the_failed_row() {
    for invalid_identity in [false, true] {
        let store = InMemoryStore::with_shard_count(8);
        let key = seed(&store, 1, true, false).await;
        let mut inner = store.inner.lock().await;
        if invalid_identity {
            inner.runs.get_mut(&key).unwrap().run_key = RunKey::new();
        } else {
            inner.run_shard_map.insert(key, ShardId(100));
        }
        drop(inner);
        let before = store.snapshot().await.unwrap();
        let error = store
            .prepare_placement_page()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&format!("{key:?}")));
        assert!(error.contains("stored_shard") && error.contains("computed_home"));
        assert_eq!(store.snapshot().await.unwrap(), before);
    }
}

#[tokio::test]
async fn screening_page_can_commit_one_thousand_unchanged_rows() {
    let store = InMemoryStore::with_shard_count(8);
    for index in 1..=1_001 {
        seed(&store, index, false, false).await;
    }
    let PlacementPage::Committed(page) = store.prepare_placement_page().await.unwrap() else {
        unreachable!()
    };
    assert_eq!((page.hot_examined, page.hot_moved), (1_000, 0));
    assert!(matches!(page.phase, PlacementPhase::Hot(Some(_))));
    assert_eq!(
        store
            .inner
            .lock()
            .await
            .modeled_transactions
            .last()
            .unwrap()
            .rows,
        1
    );
}

#[tokio::test]
async fn exact_stopped_reset_recovers_placement_before_dispatch_repair() {
    let store = InMemoryStore::with_shard_count(8);
    let successor = crate::placement_upgrade_tests::reset_successor(&store)
        .await
        .unwrap();
    let key = successor.run_key;
    let old = ShardId((key.0.as_u128() as u32) % 8);
    let home;
    {
        let mut inner = store.inner.lock().await;
        home = inner.run_shard_map[&key];
        assert_ne!(home, old);
        inner.run_shard_map.insert(key, old);
        let timers: Vec<_> = inner
            .timer_bucket
            .iter()
            .filter(|(position, _)| position.run_key == key)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (position, timer) in timers {
            inner.timer_bucket.remove(&position);
            inner.timer_bucket.insert(
                TimerPosition {
                    shard: shard_uuid(old),
                    ..position
                },
                timer,
            );
        }
        inner.workflow_dispatch.remove(&key);
    }
    let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
    let before = semantic_snapshot(&restored).await;
    let progress = prepare_execution_placement(&restored).await.unwrap();
    assert_eq!((progress.hot_moved, progress.timers_moved), (1, 1));
    assert_eq!(semantic_snapshot(&restored).await, before);
    restored
        .reconcile_workflow_dispatch_run(home, key)
        .await
        .unwrap();
    assert_eq!(
        restored.inner.lock().await.workflow_dispatch[&key].execution_home,
        home
    );
}

#[tokio::test]
async fn placement_hot_pages_stop_at_the_byte_budget_before_the_key_budget() {
    let store = InMemoryStore::with_shard_count(8);
    for index in 1..=8 {
        let key = seed(&store, index, true, false).await;
        let mut inner = store.inner.lock().await;
        inner.runs.get_mut(&key).unwrap().memo.0.insert(
            "large".into(),
            tokeira_types::Payload::new(vec![1; 800_000]),
        );
    }
    let PlacementPage::Committed(progress) = store.prepare_placement_page().await.unwrap() else {
        unreachable!()
    };
    assert_eq!(progress.hot_moved, 5);
    assert_eq!(progress.hot_examined, 5);
    let inner = store.inner.lock().await;
    let cost = inner.modeled_transactions.last().unwrap();
    assert!(cost.bytes <= crate::write_budget::MAX_BYTES_PER_TRANSACTION);
}
