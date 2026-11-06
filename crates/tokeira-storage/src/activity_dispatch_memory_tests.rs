//! Memory adapter and failure injection for the shared activity storage contracts.

use proptest::prelude::*;

use super::*;
use crate::activity_dispatch_tests::{
    Backend, malformed_page_cases, pair_cases, pending_activity, scheduled_activity_events,
    staged_cases,
};

#[async_trait]
impl Backend for InMemoryStore {
    fn activity_repo(&self) -> &dyn RunRepository {
        self
    }

    fn activity_shard_count(&self) -> u32 {
        1
    }

    async fn clear_activity_rows(&self, run: RunKey) -> Result<()> {
        self.inner
            .lock()
            .await
            .activity_dispatch
            .retain(|(key, _), _| *key != run);
        Ok(())
    }

    async fn seed_activity_rows(&self, rows: &[(crate::ActivityDispatchRow, bool)]) -> Result<()> {
        let mut store = self.inner.lock().await;
        for (row, legacy) in rows {
            let activity = pending_activity(
                &row.incarnation.activity_id,
                row.incarnation.schedule_event_id,
            );
            let mut entry = ActivityDispatchEntry::from_row(row, &activity);
            if *legacy {
                entry.queue_key = None;
            }
            store.activity_dispatch.insert(
                (row.incarnation.run_key, row.incarnation.activity_id.clone()),
                entry,
            );
        }
        Ok(())
    }
}

#[test]
fn activity_legacy_disposal_is_bounded_and_conditional() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    crate::activity_dispatch_tests::disposal_cases(&InMemoryStore::default(), &runtime);
}

#[test]
fn activity_pair_rows_are_atomic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    pair_cases(&InMemoryStore::default(), &runtime);
}

#[test]
fn activity_start_observation_is_read_only_and_current() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    crate::activity_dispatch_tests::observation_cases(&InMemoryStore::default(), &runtime);
}

#[test]
fn activity_pages_preserve_corrupt_and_legacy_positions() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    malformed_page_cases(&InMemoryStore::default(), &runtime);
}

#[test]
fn activity_reset_publication_has_complete_rows() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    staged_cases(&InMemoryStore::default(), &runtime);
}

#[test]
fn activity_repair_covers_every_slot() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    crate::activity_dispatch_tests::repair_cases(&InMemoryStore::default(), &runtime);
}

#[tokio::test]
async fn activity_repair_yields_on_bytes_before_its_key_limit() {
    let store = InMemoryStore::default();
    let mut start = projection_accumulator_tests::fresh_transition(RunKey::new());
    let run = start.next_state.run_key;
    for prefix in ['a', 'b'] {
        let id = format!("{prefix}{}", "x".repeat(2 * 1024 * 1024));
        let activity = pending_activity(&id, 3);
        start.next_state.activities.insert(id, activity);
    }
    let state = projection_accumulator_tests::applied(
        store
            .commit_transition(run, start, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    store.clear_activity_rows(run).await.unwrap();
    let first = store
        .reconcile_activity_dispatch_page(ShardId(0), run, None, WriteFence::Unfenced)
        .await
        .unwrap();
    assert_eq!((first.examined, first.changed), (1, 1));
    let second = store
        .reconcile_activity_dispatch_page(ShardId(0), run, first.continuation, WriteFence::Unfenced)
        .await
        .unwrap();
    assert_eq!((second.examined, second.changed), (1, 1));
    assert_eq!(
        store.load_run(run).await.unwrap(),
        LoadedRun::Existing(state)
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 14
    // The store lock orders a competing owner transition and repair; resumed repair rereads its new sequence.
    #[test]
    fn activity_repair_serializes_with_competing_writers(writer_first in any::<bool>(), delete in any::<bool>(), replacement in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let mut start = projection_accumulator_tests::fresh_transition(RunKey::new());
            let run = start.next_state.run_key;
            for index in 0..130 {
                let activity = pending_activity(&format!("concurrent-{index}"), index + 3);
                start.next_state.activities.insert(activity.activity_id.clone(), activity);
            }
            let state = projection_accumulator_tests::applied(store.commit_transition(run, start, WriteFence::Unfenced).await.unwrap());
            store.clear_activity_rows(run).await.unwrap();
            let page = store.reconcile_activity_dispatch_page(ShardId(0), run, None, WriteFence::Unfenced).await.unwrap();
            let cursor = page.continuation;
            let mut change = projection_accumulator_tests::following(&state);
            if delete { change.next_state.activities.clear(); }
            else {
                change.next_state.activities.remove("concurrent-0");
                let mut activity = pending_activity("new-slot", 500);
                if replacement { activity.attempt = 2; }
                change.next_state.activities.insert("new-slot".into(), activity);
            }
            let guard = store.inner.lock().await;
            let (writer_entered, writer_waiting) = tokio::sync::oneshot::channel();
            let (repair_entered, repair_waiting) = tokio::sync::oneshot::channel();
            let write = { let store = store.clone(); async move {
                writer_entered.send(()).unwrap();
                store.commit_transition(run, change, WriteFence::Unfenced).await.unwrap()
            }};
            let repair = { let store = store.clone(); async move {
                repair_entered.send(()).unwrap();
                store.reconcile_activity_dispatch_page(ShardId(0), run, cursor, WriteFence::Unfenced).await.unwrap()
            }};
            // Current-thread execution runs each sender through its pending lock
            // acquisition before the receiver resumes; both operations are queued.
            let (writer, repairing) = if writer_first {
                let writer = tokio::spawn(write); writer_waiting.await.unwrap();
                let repairing = tokio::spawn(repair); repair_waiting.await.unwrap();
                (writer, repairing)
            } else {
                let repairing = tokio::spawn(repair); repair_waiting.await.unwrap();
                let writer = tokio::spawn(write); writer_waiting.await.unwrap();
                (writer, repairing)
            };
            drop(guard);
            let next = projection_accumulator_tests::applied(writer.await.unwrap());
            let repaired = repairing.await.unwrap();
            if writer_first {
                prop_assert_eq!(repaired.continuation.unwrap().observed_seq, Some(next.transition_seq));
            }
            crate::activity_dispatch_tests::repair_all(&store, ShardId(0), run, WriteFence::Unfenced).await;
            let expected = crate::derive_activity_dispatch(&next, ShardId(0)).unwrap();
            let actual = crate::activity_dispatch_tests::all_rows(&store, &next, 11).await.into_iter().map(|row| (row.key, row)).collect::<BTreeMap<_,_>>();
            prop_assert_eq!(actual, expected);
            prop_assert_eq!(store.load_run(run).await.unwrap(), LoadedRun::Existing(next));
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 14
    // A captured lease can repair through renewal, but never after release or takeover.
    #[test]
    fn activity_repair_preserves_the_captured_fence(loss in 0u8..3, orphan in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let home = ShardId(0);
            let LeaseOutcome::Acquired { epoch } = store.try_acquire_bundle(home, "old".into(), "endpoint".into()).await.unwrap() else { panic!("lease") };
            let fence = WriteFence::Lease(crate::ExecutionHomeFence { home, owner: "old".into(), epoch });
            let mut start = projection_accumulator_tests::fresh_transition(RunKey::new());
            let run = start.next_state.run_key;
            start.next_state.activities.insert("a".into(), pending_activity("a", 3));
            store.commit_transition(run, start, fence.clone()).await.unwrap();
            {
                let mut inner = store.inner.lock().await;
                if orphan { inner.runs.remove(&run); }
                else { inner.activity_dispatch.clear(); }
            }
            if loss == 0 { store.renew_bundle(home, "old".into(), epoch, "endpoint".into()).await.unwrap(); }
            else {
                store.relinquish_bundle(home, "old".into(), epoch).await.unwrap();
                if loss == 2 { store.try_acquire_bundle(home, "new".into(), "endpoint".into()).await.unwrap(); }
            }
            let before = store.snapshot().await.unwrap();
            let result = store.reconcile_activity_dispatch_page(home, run, None, fence).await;
            if loss == 0 { prop_assert!(result.is_ok()); }
            else {
                prop_assert!(result.unwrap_err().is::<LeaseFenceLost>());
                prop_assert_eq!(store.snapshot().await.unwrap(), before);
            }
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 2
    // A crash between staging transactions leaves rows unreachable until complete publication.
    #[test]
    fn interrupted_activity_stage_never_publishes(count in 1001usize..1010, stop in 1usize..3) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let events = scheduled_activity_events(count);
            let chunks = events.chunks(400).map(<[_]>::to_vec).collect();
            let (base, fork) = crate::bulk_write_tests::commit_base(&store, chunks).await;
            let run_id = RunId::new();
            let run = RunKey::derive(base.namespace_id, &base.workflow_id, run_id);
            store.fail_bulk_write_after(stop).await;
            prop_assert!(store.materialize_reset_successor(base.run_key, fork, run_id, Some(base.run_key), WriteFence::Unfenced).await.is_err());
            prop_assert!(matches!(store.load_run(run).await.unwrap(), LoadedRun::Absent), "staged state is not published");
            prop_assert_eq!(store.find_latest_run(base.namespace_id, &base.workflow_id).await.unwrap(), Some(base.run_key));
            {
                let inner = store.inner.lock().await;
                let rows = inner.activity_dispatch.keys().filter(|(key, _)| *key == run).count();
                prop_assert!(rows < count);
                if stop == 2 { prop_assert!(rows > 0); }
                for total in &inner.modeled_transactions {
                    prop_assert!(total.rows <= crate::write_budget::MAX_ROWS_PER_TRANSACTION);
                    prop_assert!(total.bytes <= crate::write_budget::MAX_BYTES_PER_TRANSACTION);
                }
            }
            store.purge_run(run).await.unwrap();
            prop_assert!(!store.inner.lock().await.activity_dispatch.keys().any(|(key, _)| *key == run));
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 3: staged publication replaces orphan slots before visibility.
    #[test]
    fn staged_activity_publication_removes_orphan_prefix(orphan_count in 65usize..140, wanted_count in 1usize..8, collision in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let (base, fork) = crate::bulk_write_tests::commit_base(&store, vec![scheduled_activity_events(wanted_count)]).await;
            let run_id = RunId::new();
            let run = RunKey::derive(base.namespace_id, &base.workflow_id, run_id);
            let mut orphan = base.clone();
            orphan.run_key = run;
            orphan.run_id = run_id;
            orphan.status = ExecutionStatus::Running;
            orphan.closed_at = None;
            orphan.activities.clear();
            for index in 0..orphan_count {
                let id = format!("orphan-{index}");
                orphan.activities.insert(id.clone(), pending_activity(&id, 999));
            }
            let rows = crate::derive_activity_dispatch(&orphan, ShardId(0)).unwrap().into_values().map(|row| (row, false)).collect::<Vec<_>>();
            store.seed_activity_rows(&rows).await.unwrap();
            let foreign = if collision {
                let mut row = rows[0].0.clone();
                row.incarnation.run_key = RunKey::new();
                row.key = crate::activity_dispatch_key(run, "staged-0");
                store.seed_activity_rows(&[(row.clone(), false)]).await.unwrap();
                Some(row)
            } else { None };
            let result = store.materialize_reset_successor(base.run_key, fork, run_id, Some(base.run_key), WriteFence::Unfenced).await;
            if let Some(foreign) = foreign {
                prop_assert!(result.is_err());
                prop_assert!(matches!(store.load_run(run).await.unwrap(), LoadedRun::Absent));
                let inner = store.inner.lock().await;
                prop_assert_eq!(inner.activity_dispatch[&(foreign.incarnation.run_key, foreign.incarnation.activity_id.clone())].key, foreign.key);
            } else {
                result.unwrap();
                let LoadedRun::Existing(state) = store.load_run(run).await.unwrap() else { panic!("published"); };
                let wanted = crate::derive_activity_dispatch(&state, ShardId(0)).unwrap();
                let inner = store.inner.lock().await;
                let actual = inner.activity_dispatch.iter().filter(|((key, _), _)| *key == run).map(|(_, value)| value.row().unwrap()).map(|row| (row.key, row)).collect::<BTreeMap<_, _>>();
                prop_assert_eq!(actual, wanted);
                prop_assert_eq!(state.activities.len(), wanted_count);
                for total in &inner.modeled_transactions { prop_assert!(total.rows <= crate::write_budget::MAX_ROWS_PER_TRANSACTION); }
            }
            Ok(())
        })?;
    }

    // Feature: activity-dispatch, Property 2
    // A physical slot occupied by another raw identity fails before any state is published.
    #[test]
    fn activity_slot_collision_refuses_the_whole_commit(seed in any::<u64>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let store = InMemoryStore::default();
            let mut start = projection_accumulator_tests::fresh_transition(RunKey::new());
            let run = start.next_state.run_key;
            let activity = pending_activity("wanted", 3);
            start.next_state.activities.insert("wanted".into(), activity.clone());
            let mut row = crate::derive_activity_dispatch(&start.next_state, ShardId(0)).unwrap().into_values().next().unwrap();
            row.incarnation.activity_id = format!("colliding-{seed}");
            store.seed_activity_rows(&[(row, false)]).await.unwrap();
            let before = store.snapshot().await.unwrap();
            prop_assert!(store.commit_transition(run, start, WriteFence::Unfenced).await.is_err());
            prop_assert_eq!(store.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }
}

#[test]
fn eager_completion_has_no_dispatch_rows() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    crate::activity_dispatch_tests::eager_completion_cases(&InMemoryStore::default(), &runtime);
}
