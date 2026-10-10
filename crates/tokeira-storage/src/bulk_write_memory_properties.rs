//! Properties of the paged bulk writes on the in-memory store
//! (`bounded-bulk-writes`), which shares DSQL's pages, records and transaction
//! limits.

use proptest::prelude::*;
use tokeira_kernel::HistoryEventKind;
use tokeira_types::EventPrincipal;

use super::*;
use crate::{
    BacklogPayload,
    bulk_write_tests::{
        Backend, OwnedRows, activity_queue, backlog_entry, commit, commit_base, signal, timer,
        with_events,
    },
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
    write_budget::{MAX_BYTES_PER_TRANSACTION, MAX_RESET_BATCH_BYTES, MAX_ROWS_PER_TRANSACTION},
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

/// Property 3: no run has both mutable state and a record, and a run with a
/// record is absent from every lookup.
async fn check_invariant(store: &InMemoryStore) -> Result<(), TestCaseError> {
    let inner = store.inner.lock().await;
    for run_key in inner.bulk_writes.keys() {
        prop_assert!(
            !inner.runs.contains_key(run_key),
            "{run_key:?} has state and a record"
        );
        prop_assert!(!inner.execution_index.values().any(|key| key == run_key));
        prop_assert!(!inner.current_open.values().any(|key| key == run_key));
        prop_assert!(!inner.current_execution.values().any(|key| key == run_key));
    }
    Ok(())
}

/// Property 1: every covered transaction stayed within the budgets.
async fn check_budgets(store: &InMemoryStore) -> Result<(), TestCaseError> {
    for total in &store.inner.lock().await.modeled_transactions {
        prop_assert!(
            total.rows <= MAX_ROWS_PER_TRANSACTION && total.bytes <= MAX_BYTES_PER_TRANSACTION,
            "a transaction changed {} rows and wrote {} bytes",
            total.rows,
            total.bytes
        );
    }
    Ok(())
}

async fn clear_failure(store: &InMemoryStore) {
    store.inner.lock().await.bulk_write_failure = None;
}

/// A closed run with `batches.len()` history batches, each holding that many
/// small events, and the given numbers of request records, timer rows,
/// activity dispatch rows and backlog entries, seeded as rows the purge must
/// remove whatever wrote them.
async fn closed_run(
    store: &InMemoryStore,
    batches: &[usize],
    dedupe: usize,
    timers: usize,
    dispatch: usize,
    backlog: usize,
) -> WorkflowState {
    let mut state = applied(commit(store, fresh_transition(RunKey::new())).await);
    for (batch, &events) in batches.iter().enumerate() {
        let first = state.last_event_id + 1;
        let kinds = (0..events)
            .map(|event| signal(batch * 4 + event, 8))
            .collect();
        state = applied(commit(store, with_events(following(&state), first, kinds)).await);
    }
    let mut close = following(&state);
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(close.next_state.started_at);
    close.next_state.pending_workflow_task = None;
    let state = applied(commit(store, close).await);

    let run_key = state.run_key;
    let queue = activity_queue(state.namespace_id);
    let mut inner = store.inner.lock().await;
    for index in 0..dedupe {
        inner.request_dedupe.insert(
            (
                state.namespace_id,
                state.workflow_id.0.clone(),
                format!("request-{index}"),
            ),
            RequestRecord {
                namespace_id: state.namespace_id,
                workflow_id: state.workflow_id.clone(),
                run_id: state.run_id,
                run_key,
                request_id: RequestId(format!("request-{index}")),
                first_seen_transition_seq: state.transition_seq,
            },
        );
    }
    for index in 0..timers {
        let timer_id = format!("timer-{index}");
        inner.timer_bucket.insert(
            TimerPosition {
                run_key,
                timer_id: timer_id.clone(),
                shard: shard_uuid(ShardId(0)),
                fire_at: state.started_at,
            },
            tokeira_kernel::TimerState {
                timer_id,
                started_event_id: 1,
                fire_at: state.started_at,
            },
        );
    }
    for index in 0..dispatch {
        let BacklogPayload::Activity {
            activity_id, input, ..
        } = backlog_entry(run_key, &queue, index, 8).payload
        else {
            unreachable!("backlog_entry builds activity entries");
        };
        inner.activity_dispatch.insert(
            (run_key, activity_id.clone()),
            ActivityDispatchEntry {
                task: DispatchableActivityTask {
                    run_key,
                    queue: queue.clone(),
                    activity_id,
                    input,
                    schedule_event_id: 5,
                    attempt: 1,
                    dispatch_revision: 0,
                    stamp: 0,
                    priority: None,
                    order: None,
                },
                dispatch_at: state.started_at,
                schedule_to_close_timeout: None,
                schedule_to_start_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
            },
        );
    }
    for index in 0..backlog {
        inner
            .dispatch_backlog
            .push_back(backlog_entry(run_key, &queue, index, 8));
    }
    drop(inner);
    state
}

async fn delete_first(store: &InMemoryStore, state: &WorkflowState) {
    let result = store
        .delete_run_for_bundle(
            state.run_key,
            ShardId(0),
            DeleteRunRequest {
                expected_seq: state.transition_seq,
                deleted_at: state.started_at,
            },
            ShardEpoch::ZERO,
        )
        .await
        .expect("the first transaction fits the budgets");
    assert!(matches!(result, DeleteRunResult::Deleted { .. }));
}

/// A principal whose name is `len` bytes, so that a batch's principals can
/// reach the batch budget before its events do.
fn principal(len: usize) -> EventPrincipal {
    EventPrincipal {
        principal_type: "user".into(),
        name: "x".repeat(len),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    // Feature: bounded-bulk-writes, Property 4: A purge finishes, removes only
    // its run's rows, and removes history last
    #[test]
    fn property_a_purge_finishes_and_removes_only_its_runs_rows(
        batches in prop::collection::vec(1usize..3, 0..1_200),
        dedupe in 0usize..2_200,
        timers in 0usize..1_100,
        dispatch in 0usize..1_100,
        backlog in 0usize..1_100,
        stops in prop::collection::vec(0usize..6, 0..4),
        concurrent in any::<bool>(),
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let other = closed_run(&store, &[1, 2], 7, 3, 3, 3).await;
            let other_rows = store.owned_rows(other.run_key).await.unwrap();
            let run = closed_run(&store, &batches, dedupe, timers, dispatch, backlog).await;
            delete_first(&store, &run).await;
            check_invariant(&store).await?;

            for stop in stops {
                store.fail_bulk_write_after(stop).await;
                let _ = store.purge_run(run.run_key).await;
                clear_failure(&store).await;
                check_invariant(&store).await?;
                let rows = store.owned_rows(run.run_key).await.unwrap();
                if rows.history < batches.len() {
                    prop_assert_eq!(
                        (rows.request_dedupe, rows.timers, rows.activity_dispatch, rows.backlog),
                        (0, 0, 0, 0),
                        "history went before another table's rows"
                    );
                }
                if !store.inner.lock().await.bulk_writes.contains_key(&run.run_key) {
                    prop_assert_eq!(rows, OwnedRows::default());
                }
            }
            if concurrent {
                let (first, second) =
                    tokio::join!(store.purge_run(run.run_key), store.purge_run(run.run_key));
                first.unwrap();
                second.unwrap();
            } else {
                store.purge_run(run.run_key).await.unwrap();
            }
            prop_assert_eq!(store.owned_rows(run.run_key).await.unwrap(), OwnedRows::default());
            prop_assert!(!store.inner.lock().await.bulk_writes.contains_key(&run.run_key));
            prop_assert_eq!(store.owned_rows(other.run_key).await.unwrap(), other_rows);
            check_budgets(&store).await
        })?;
    }

    // Feature: bounded-bulk-writes, Property 5: A materialized successor is
    // complete, or invisible
    // Feature: bounded-bulk-writes, Property 6: An abandoned materialization
    // leaves nothing behind
    #[test]
    fn property_a_materialization_completes_or_leaves_nothing(
        signals in prop::collection::vec(0usize..300_000, 1..20),
        timers in 0usize..1_300,
        stop in prop::option::of(0usize..12),
        abandon_after in prop::option::of(0usize..12),
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let mut chunks = signals
                .iter()
                .enumerate()
                .map(|(index, &size)| vec![signal(index, size)])
                .collect::<Vec<_>>();
            let timer_events = (0..timers).map(timer).collect::<Vec<_>>();
            chunks.extend(timer_events.chunks(900).map(<[HistoryEventKind]>::to_vec));
            let (base, fork) = commit_base(&store, chunks).await;
            let expected = store
                .find_latest_run(base.namespace_id, &base.workflow_id)
                .await
                .unwrap();
            let successor_run_id = RunId::new();
            let successor =
                RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
            if let Some(stop) = stop {
                store.fail_bulk_write_after(stop).await;
            }
            let materialize = store.materialize_reset_successor(
                base.run_key,
                fork,
                successor_run_id,
                expected,
            );
            let result = match abandon_after {
                Some(yields) => {
                    // A purge that runs beside the materialization switches
                    // its record, so the materialization's later transactions
                    // are stragglers. None of them may write: a row written
                    // after the purge had passed its table would outlive it.
                    let (result, purged) = tokio::join!(materialize, async {
                        for _ in 0..yields {
                            tokio::task::yield_now().await;
                        }
                        store.purge_run(successor).await
                    });
                    let recorded = store.inner.lock().await.bulk_writes.contains_key(&successor);
                    if purged.is_ok() && !recorded && result.is_err() {
                        prop_assert_eq!(
                            store.owned_rows(successor).await.unwrap(),
                            OwnedRows::default()
                        );
                    }
                    result
                }
                None => materialize.await,
            };
            clear_failure(&store).await;
            check_invariant(&store).await?;

            if result.is_ok() {
                // Property 5, once the final transaction committed.
                prop_assert!(!store.inner.lock().await.bulk_writes.contains_key(&successor));
                prop_assert_eq!(
                    store.find_latest_run(base.namespace_id, &base.workflow_id).await.unwrap(),
                    Some(successor)
                );
                let mut prefix = store.read_history_to_end(base.run_key, 0).await.unwrap();
                prefix.retain(|event| event.event_id < fork);
                prop_assert_eq!(store.read_history_to_end(successor, 0).await.unwrap(), prefix);
                let batches = store.history_batches(successor).await.unwrap();
                prop_assert!(batches
                    .iter()
                    .all(|batch| batch.events == 1 || batch.bytes <= MAX_RESET_BATCH_BYTES));
                let (_, stats) = store.load_run_with_stats(successor).await.unwrap();
                prop_assert_eq!(
                    stats.history_size_bytes,
                    batches.iter().map(|batch| batch.bytes as i64).sum::<i64>()
                );
                let LoadedRun::Existing(state) = store.load_run(successor).await.unwrap() else {
                    return Err(TestCaseError::fail("successor missing"));
                };
                prop_assert_eq!(state.timers.len(), timers);
                prop_assert_eq!(store.owned_rows(successor).await.unwrap().timers, timers);
            } else {
                // Property 5 before the final transaction, and Property 6.
                prop_assert!(matches!(
                    store.load_run(successor).await.unwrap(),
                    LoadedRun::Absent
                ));
                prop_assert_eq!(
                    store.find_latest_run(base.namespace_id, &base.workflow_id).await.unwrap(),
                    Some(base.run_key)
                );
                store.purge_run(successor).await.unwrap();
                prop_assert_eq!(store.owned_rows(successor).await.unwrap(), OwnedRows::default());
                prop_assert!(!store.inner.lock().await.bulk_writes.contains_key(&successor));
                prop_assert!(matches!(
                    store.load_run(successor).await.unwrap(),
                    LoadedRun::Absent
                ));
            }
            check_budgets(&store).await
        })?;
    }

    // Feature: bounded-bulk-writes, Property 10: A successor replaces only the
    // run the pointer named at admission
    #[test]
    fn property_a_successor_replaces_only_the_run_the_pointer_named(
        base_open in any::<bool>(),
        between in 0u8..4,
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let (mut base, fork) = commit_base(&store, vec![vec![signal(0, 16)]]).await;
            if base_open {
                // Reopen the closed fixture: an open base the reset terminates
                // is current and open at admission.
                let mut reopen = following(&base);
                reopen.next_state.status = ExecutionStatus::Running;
                reopen.next_state.closed_at = None;
                base = applied(commit(&store, reopen).await);
            }
            let admitted = store
                .find_latest_run(base.namespace_id, &base.workflow_id)
                .await
                .unwrap();
            match between {
                1 => {
                    // A start of the same workflow id. While the base is open
                    // the store refuses it, and the pointer stays.
                    let run_id = RunId::new();
                    let mut start = fresh_transition(RunKey::derive(
                        base.namespace_id,
                        &base.workflow_id,
                        run_id,
                    ));
                    start.next_state.namespace_id = base.namespace_id;
                    start.next_state.workflow_id = base.workflow_id.clone();
                    start.next_state.run_id = run_id;
                    let _ = store
                        .commit_transition(start.next_state.run_key, start, ShardEpoch::ZERO)
                        .await;
                }
                2 if base_open => {
                    let mut close = following(&base);
                    close.next_state.status = ExecutionStatus::Completed;
                    close.next_state.closed_at = Some(close.next_state.started_at);
                    close.next_state.pending_workflow_task = None;
                    base = applied(commit(&store, close).await);
                }
                3 if !base_open => delete_first(&store, &base).await,
                _ => {}
            }
            let before = store
                .find_latest_run(base.namespace_id, &base.workflow_id)
                .await
                .unwrap();
            let successor_run_id = RunId::new();
            let successor =
                RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
            let result = store
                .materialize_reset_successor(base.run_key, fork, successor_run_id, admitted)
                .await;
            prop_assert_eq!(result.is_ok(), before == admitted, "{:?}", result);
            let after = store
                .find_latest_run(base.namespace_id, &base.workflow_id)
                .await
                .unwrap();
            if result.is_ok() {
                prop_assert_eq!(after, Some(successor));
            } else {
                prop_assert_eq!(after, before);
                prop_assert!(matches!(
                    store.load_run(successor).await.unwrap(),
                    LoadedRun::Absent
                ));
            }
            check_invariant(&store).await
        })?;
    }

    // Feature: bounded-bulk-writes, Property 2: A failed spill keeps what it
    // persisted and re-publishes the rest
    #[test]
    fn property_a_failed_spill_keeps_the_pages_before_it(
        sizes in prop::collection::vec(0usize..900_000, 0..40),
        small in 0usize..2_600,
        stop in prop::option::of(0usize..6),
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let run_key = RunKey::new();
            let queue = activity_queue(NamespaceId::new());
            let entries = sizes
                .iter()
                .chain(std::iter::repeat_n(&8, small))
                .enumerate()
                .map(|(index, &size)| backlog_entry(run_key, &queue, index, size))
                .collect::<Vec<_>>();
            if let Some(stop) = stop {
                store.fail_bulk_write_after(stop).await;
            }
            let result = store.persist_to_backlog(entries.clone()).await;
            clear_failure(&store).await;
            let persisted = match &result {
                Ok(()) => entries.len(),
                Err(error) => error.persisted,
            };
            let stored = store
                .inner
                .lock()
                .await
                .dispatch_backlog
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            prop_assert_eq!(&stored[..], &entries[..persisted]);
            check_budgets(&store).await
        })?;
    }

    // Feature: bounded-bulk-writes, Property 7: The copied history is split at
    // event boundaries within the batch budget
    #[test]
    fn property_reset_batches_are_cut_at_event_boundaries_within_the_budget(
        sizes in prop::collection::vec(0usize..150_000, 0..40),
        principals in prop::collection::vec(0usize..200_000, 0..40),
    ) {
        let events = sizes
            .iter()
            .enumerate()
            .map(|(index, &size)| tokeira_kernel::HistoryEvent {
                event_id: index as i64 + 1,
                happened_at: OffsetDateTime::UNIX_EPOCH,
                kind: signal(index, size),
            })
            .collect::<Vec<_>>();
        let principals = (0..events.len())
            .map(|index| principals.get(index).filter(|size| **size > 0).map(|size| principal(*size)))
            .collect::<Vec<_>>();
        let batches = crate::codec::reset_history_batches(&events, &principals, MAX_RESET_BATCH_BYTES)
            .unwrap();
        prop_assert_eq!(
            &batches,
            &crate::codec::reset_history_batches(&events, &principals, MAX_RESET_BATCH_BYTES).unwrap()
        );
        let mut next = 0;
        for (index, batch) in batches.iter().enumerate() {
            prop_assert_eq!(batch.start, next);
            prop_assert!(batch.end > batch.start);
            next = batch.end;
            let events_len = crate::codec::encode_history_events(&events[batch.clone()]).unwrap().len();
            let principals_len = crate::codec::encode_history_principals(&principals[batch.clone()]).unwrap().len();
            prop_assert!(
                batch.len() == 1 || (events_len <= MAX_RESET_BATCH_BYTES && principals_len <= MAX_RESET_BATCH_BYTES)
            );
            // Greedy: a batch closes only when its next event would take it over.
            if let Some(following) = batches.get(index + 1) {
                let grown = batch.start..following.start + 1;
                let grown_events = crate::codec::encode_history_events(&events[grown.clone()]).unwrap().len();
                let grown_principals = crate::codec::encode_history_principals(&principals[grown]).unwrap().len();
                prop_assert!(grown_events > MAX_RESET_BATCH_BYTES || grown_principals > MAX_RESET_BATCH_BYTES);
            }
        }
        prop_assert_eq!(next, events.len());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    // Feature: bounded-bulk-writes, Property 8: The timer scanner keeps a
    // materializing run's timers
    #[test]
    fn property_the_scanner_keeps_a_materializing_runs_timers(case in 0u8..5) {
        block_on(async {
            let store = InMemoryStore::default();
            let (run_key, reason) = match case {
                // A successor whose materialization stopped after its record.
                0 => {
                    let (base, fork) = commit_base(&store, vec![vec![signal(0, 16)]]).await;
                    let run_id = RunId::new();
                    store.fail_bulk_write_after(1).await;
                    store
                        .materialize_reset_successor(base.run_key, fork, run_id, Some(base.run_key))
                        .await
                        .unwrap_err();
                    clear_failure(&store).await;
                    (RunKey::derive(base.namespace_id, &base.workflow_id, run_id), StaleTimer::RunMissing)
                }
                // A run the kernel missed, whose final transaction has since committed.
                1 => (closed_run(&store, &[1], 0, 0, 0, 0).await.run_key, StaleTimer::RunMissing),
                // A deleted run, being purged.
                2 => {
                    let run = closed_run(&store, &[1], 0, 0, 0, 0).await;
                    delete_first(&store, &run).await;
                    (run.run_key, StaleTimer::RunMissing)
                }
                // A row with neither state nor a record.
                3 => (RunKey::new(), StaleTimer::RunMissing),
                // A closed run.
                _ => (closed_run(&store, &[1], 0, 0, 0, 0).await.run_key, StaleTimer::RunClosed),
            };
            let fire_at = OffsetDateTime::UNIX_EPOCH;
            store.inner.lock().await.timer_bucket.insert(
                TimerPosition { run_key, timer_id: "due".into(), shard: shard_uuid(ShardId(0)), fire_at },
                tokeira_kernel::TimerState {
                    timer_id: "due".into(),
                    started_event_id: 1,
                    fire_at,
                },
            );
            let due = DueTimer { run_key, timer_id: "due".into(), fire_at };
            let deleted = store.delete_due_timer_if_matches(&due, reason).await.unwrap();
            prop_assert_eq!(deleted, case >= 2);
            prop_assert_eq!(
                store.inner.lock().await.timer_bucket.keys().any(|key| key.run_key == run_key && key.timer_id == "due"),
                case < 2
            );
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn the_first_transaction_changes_the_pointer_state_and_record() {
    let store = InMemoryStore::default();
    let run = closed_run(&store, &[1], 3, 2, 2, 2).await;
    let before = store.owned_rows(run.run_key).await.unwrap();
    store.inner.lock().await.modeled_transactions.clear();
    delete_first(&store, &run).await;
    let inner = store.inner.lock().await;
    assert_eq!(
        inner.modeled_transactions,
        [crate::write_budget::WriteCost {
            // The tombstone, the pointer, the run's state and the record; the
            // closed run has no dispatch row.
            rows: 4,
            bytes: crate::codec::encode_projection_context(
                &inner.projection_log.last().unwrap().context
            )
            .unwrap()
            .len(),
        }]
    );
    assert_eq!(
        inner.bulk_writes[&run.run_key].phase,
        BulkWritePhase::Purging
    );
    drop(inner);
    let after = store.owned_rows(run.run_key).await.unwrap();
    assert_eq!(
        after,
        OwnedRows {
            hot: 0,
            workflow_dispatch: 0,
            ..before
        }
    );
}

// A purge that finishes while a materialization is still copying leaves
// nothing behind: every later copy transaction reads the switch and writes
// nothing (Property 6).
#[tokio::test]
async fn a_straggler_copy_writes_nothing_once_a_purge_has_run() {
    let store = InMemoryStore::default();
    // Twenty events of about 600 KB, one to a batch, copied in four pages.
    let chunks = (0..20).map(|index| vec![signal(index, 600_000)]).collect();
    let (base, fork) = commit_base(&store, chunks).await;
    let successor_run_id = RunId::new();
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
    let (result, purged) = tokio::join!(
        store.materialize_reset_successor(base.run_key, fork, successor_run_id, Some(base.run_key)),
        async {
            // The record and the first page commit first.
            tokio::task::yield_now().await;
            store.purge_run(successor).await
        }
    );
    purged.unwrap();
    assert!(
        result.is_err(),
        "the purge's switch stops the materialization"
    );
    assert_eq!(
        store.owned_rows(successor).await.unwrap(),
        OwnedRows::default()
    );
    assert!(
        !store
            .inner
            .lock()
            .await
            .bulk_writes
            .contains_key(&successor)
    );
    assert!(matches!(
        store.load_run(successor).await.unwrap(),
        LoadedRun::Absent
    ));
}

#[tokio::test]
async fn a_purge_of_an_abandoned_materialization_removes_its_history_and_timers() {
    let store = InMemoryStore::default();
    let chunks = vec![
        vec![signal(0, 600_000)],
        vec![signal(1, 600_000)],
        (0..10).map(timer).collect(),
    ];
    let (base, fork) = commit_base(&store, chunks).await;
    let successor_run_id = RunId::new();
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
    // The record and the one copy page commit; the final transaction fails.
    store.fail_bulk_write_after(2).await;
    store
        .materialize_reset_successor(base.run_key, fork, successor_run_id, Some(base.run_key))
        .await
        .unwrap_err();
    let rows = store.owned_rows(successor).await.unwrap();
    assert!(rows.history > 0 && rows.timers == 10 && rows.hot == 0);
    store.purge_run(successor).await.unwrap();
    assert_eq!(
        store.owned_rows(successor).await.unwrap(),
        OwnedRows::default()
    );
    assert!(store.inner.lock().await.bulk_writes.is_empty());
}

#[tokio::test]
async fn the_records_of_a_shard_list_in_run_key_order() {
    let store = InMemoryStore::default();
    let mut runs = Vec::new();
    for _ in 0..5 {
        let run = closed_run(&store, &[1], 0, 0, 0, 0).await;
        delete_first(&store, &run).await;
        runs.push(run.run_key);
    }
    runs.sort();
    let first = store
        .list_run_bulk_writes(ShardId(0), None, 3)
        .await
        .unwrap();
    let rest = store
        .list_run_bulk_writes(ShardId(0), first.last().map(|record| record.run_key), 3)
        .await
        .unwrap();
    let listed = first
        .iter()
        .chain(&rest)
        .map(|record| record.run_key)
        .collect::<Vec<_>>();
    assert_eq!(listed, runs);
    assert!(
        store
            .list_run_bulk_writes(ShardId(1), None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_snapshot_carries_the_records() {
    let store = InMemoryStore::default();
    let run = closed_run(&store, &[1], 1, 0, 0, 0).await;
    delete_first(&store, &run).await;
    let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
    assert_eq!(
        restored.inner.lock().await.bulk_writes,
        store.inner.lock().await.bulk_writes
    );
    restored.purge_run(run.run_key).await.unwrap();
    assert_eq!(
        restored.owned_rows(run.run_key).await.unwrap(),
        OwnedRows::default()
    );
}
