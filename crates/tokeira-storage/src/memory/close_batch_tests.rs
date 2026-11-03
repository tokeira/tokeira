//! Bounded close publication, expansion and obsolete-operation reclamation.

use proptest::prelude::*;
use tokeira_kernel::{ChildReleaseState, ChildWorkflowState, ParentClosePolicy};
use tokeira_types::WorkflowType;

use super::*;
use crate::{
    DefinitiveOutcome, DeliveryGuard, IntentKind, IntentPayload, IntentRetirement,
    memory::{
        outbound_tests::close,
        projection_accumulator_tests::{applied, fresh_transition},
    },
    outbound::close_batch,
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn parent(count: usize, id_len: usize, mixed: bool) -> Transition {
    let mut initial = fresh_transition(RunKey::new());
    let state = &mut initial.next_state;
    for i in 0..count {
        let workflow_id = WorkflowId(format!("{i:04}{}", "x".repeat(id_len.saturating_sub(4))));
        let event = i64::try_from(i + 10).unwrap();
        let policy = if mixed && i % 3 == 0 {
            ParentClosePolicy::Abandon
        } else if mixed && i % 3 == 1 {
            ParentClosePolicy::RequestCancel
        } else {
            ParentClosePolicy::Terminate
        };
        let run = RunId::new();
        state.children.insert(
            workflow_id.clone(),
            ChildWorkflowState {
                child_workflow_id: workflow_id.clone(),
                namespace_id: state.namespace_id,
                namespace: None,
                workflow_type: WorkflowType("child".into()),
                header: None,
                child_run_id: (policy == ParentClosePolicy::Abandon).then_some(run),
                initiated_event_id: event,
                started_event_id: None,
                parent_close_policy: policy,
            },
        );
        if policy == ParentClosePolicy::Abandon {
            state.outbound.as_mut().unwrap().child_releases.insert(
                event,
                ChildReleaseState {
                    namespace_id: state.namespace_id,
                    workflow_id,
                    initiating_event: event,
                    child_run_id: run,
                    parent_started_event_id: Some(event + 100),
                    parent_close_policy: policy,
                },
            );
        }
    }
    initial
}

fn within(cost: WriteCost, budget: WriteBudget) {
    assert!(
        cost.rows <= budget.rows && cost.bytes <= budget.bytes,
        "outbound transaction {cost:?} exceeds {budget:?}"
    );
}

async fn rows(store: &InMemoryStore) -> Vec<crate::IntentRow> {
    store
        .inner
        .lock()
        .await
        .outbound_intents
        .values()
        .cloned()
        .collect()
}

async fn restart(store: InMemoryStore, budget: WriteBudget) -> InMemoryStore {
    let operations: BTreeMap<_, _> = {
        let inner = store.inner.lock().await;
        inner
            .runs
            .values()
            .filter(|run| run.outbound.is_some())
            .flat_map(|run| {
                let home = tokeira_types::execution_home_bundle(
                    run.namespace_id.0.as_bytes(),
                    run.workflow_id.0.as_bytes(),
                    InMemoryStore::effective_shard_count(&inner),
                );
                crate::derive_operations(run, home).unwrap()
            })
            .map(|row| (row.incarnation.key, row))
            .collect()
    };
    let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
    assert_eq!(restored.inner.lock().await.outbound_operations, operations);
    restored.set_outbound_write_budget(budget).await;
    restored
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
    #[test]
    fn close_outbound_writes_stay_within_budget(
        count in 1usize..55, limit in 6usize..20, id_len in 4usize..1001,
        child_result in any::<bool>(), restart_pages in any::<bool>(),
    ) {
        block_on(async {
            let budget = WriteBudget {
                rows: limit,
                bytes: (count * (id_len + 80) + 3_000).min(100_000),
            };
            let mut store = InMemoryStore::default();
            store.set_outbound_write_budget(budget).await;
            let mut initial = parent(count, id_len, true);
            if child_result {
                initial.next_state.parent_run_key = Some(RunKey::new());
                initial.next_state.parent_workflow_id = Some(WorkflowId("grandparent".into()));
                initial.next_state.parent_initiated_event_id = 1;
            }
            let open = applied(
                store
                    .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let closing = close(&open);
            let mut direct =
                crate::derive_close_intents(Some(&open), &closing, ShardId(0)).unwrap();
            direct.sort_by_key(|row| row.key);
            applied(
                store
                    .commit_transition(open.run_key, closing, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            within(
                *store
                    .inner
                    .lock()
                    .await
                    .modeled_outbound_transactions
                    .last()
                    .unwrap(),
                budget,
            );
            let committed = rows(&store).await;
            if close_batch::fits(close_batch::intents_cost(&direct).unwrap(), budget) {
                prop_assert_eq!(&committed, &direct);
            } else {
                prop_assert_eq!(
                    committed
                        .iter()
                        .filter(|row| row.key.kind == IntentKind::CloseBatch)
                        .count(),
                    1
                );
                prop_assert_eq!(committed.len(), 1 + usize::from(child_result));
                let batch = committed
                    .iter()
                    .find(|row| row.key.kind == IntentKind::CloseBatch)
                    .unwrap();
                prop_assert_eq!(batch.key.ordinal, 0);
                prop_assert_eq!(&batch.target, &None);
                let IntentPayload::CloseBatch { obligations } = &batch.payload else {
                    unreachable!()
                };
                let expected: Vec<_> = direct
                    .iter()
                    .filter(|row| row.key.kind != IntentKind::ChildResult)
                    .map(|row| (row.key.ordinal, row.payload.clone()))
                    .collect();
                let mut actual = obligations.clone();
                // Derivation groups release and policy rows; identity ordering is independent.
                actual.sort_by_key(|(ordinal, payload)| {
                    (
                        matches!(payload, IntentPayload::ChildRelease { .. }),
                        *ordinal,
                    )
                });
                prop_assert_eq!(actual, expected);
            }
            let mut inserted: BTreeMap<_, _> = committed
                .into_iter()
                .filter(|row| row.key.kind != IntentKind::CloseBatch)
                .map(|row| (row.key, row))
                .collect();
            if !committed_is_direct(&direct, budget) {
                for key in inserted.keys() {
                    store
                        .retire_intent(
                            IntentRetirement {
                                key: *key,
                                outcome: DefinitiveOutcome::Absent,
                            },
                            WriteFence::Unfenced,
                        )
                        .await
                        .unwrap();
                }
            }
            while let Some(batch) = rows(&store)
                .await
                .into_iter()
                .find(|row| row.key.kind == IntentKind::CloseBatch)
            {
                if restart_pages {
                    store = restart(store, budget).await;
                }
                prop_assert!(
                    store
                        .expand_close_batch(batch.key, WriteFence::Unfenced)
                        .await
                        .unwrap()
                );
                within(
                    *store
                        .inner
                        .lock()
                        .await
                        .modeled_outbound_transactions
                        .last()
                        .unwrap(),
                    budget,
                );
                let before_retry = store.snapshot().await.unwrap();
                let tallies = store.inner.lock().await.modeled_outbound_transactions.len();
                prop_assert!(
                    !store
                        .expand_close_batch(batch.key, WriteFence::Unfenced)
                        .await
                        .unwrap()
                );
                prop_assert_eq!(store.snapshot().await.unwrap(), before_retry);
                prop_assert_eq!(
                    store.inner.lock().await.modeled_outbound_transactions.len(),
                    tallies
                );
                for row in rows(&store)
                    .await
                    .into_iter()
                    .filter(|row| row.key.kind != IntentKind::CloseBatch)
                {
                    prop_assert!(
                        inserted.insert(row.key, row.clone()).is_none(),
                        "inserted twice"
                    );
                    store
                        .retire_intent(
                            IntentRetirement {
                                key: row.key,
                                outcome: DefinitiveOutcome::Absent,
                            },
                            WriteFence::Unfenced,
                        )
                        .await
                        .unwrap();
                }
            }
            if committed_is_direct(&direct, budget) {
                prop_assert_eq!(rows(&store).await, direct);
            } else {
                prop_assert_eq!(inserted.into_values().collect::<Vec<_>>(), direct);
                prop_assert!(rows(&store).await.is_empty());
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
    #[test]
    fn close_batch_preserves_direct_successor(count in 8usize..40, bytes in 0usize..1_500) {
        block_on(async {
            let budget = WriteBudget {
                rows: 6,
                bytes: 100_000,
            };
            let store = InMemoryStore::default();
            store.set_outbound_write_budget(budget).await;
            let initial = parent(count, 30, true);
            let open = applied(
                store
                    .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let (closing, mut next) = crate::atomic_successors_test_support::pair(
                &open,
                0,
                Default::default(),
                Default::default(),
                None,
            );
            next.next_state.run_key = RunKey::derive(
                next.next_state.namespace_id,
                &next.next_state.workflow_id,
                next.next_state.run_id,
            );
            let request = super::successor_tests::start_request(&next.next_state, &open, bytes);
            let expected_children =
                crate::derive_close_intents(Some(&open), &closing, ShardId(0)).unwrap();
            let (_, successor) = crate::outbound::successor::freeze(
                open.run_key,
                &closing,
                Box::new(request.clone()),
                ShardId(0),
            )
            .unwrap();
            applied(
                store
                    .commit_successor_fallback(
                        open.run_key,
                        closing,
                        Box::new(request),
                        WriteFence::Unfenced,
                    )
                    .await
                    .unwrap(),
            );
            within(
                *store
                    .inner
                    .lock()
                    .await
                    .modeled_outbound_transactions
                    .last()
                    .unwrap(),
                budget,
            );
            let committed = rows(&store).await;
            prop_assert_eq!(committed.len(), 2);
            prop_assert!(committed.contains(&successor));
            while let Some(batch) = rows(&store)
                .await
                .into_iter()
                .find(|row| row.key.kind == IntentKind::CloseBatch)
            {
                store
                    .expand_close_batch(batch.key, WriteFence::Unfenced)
                    .await
                    .unwrap();
                within(
                    *store
                        .inner
                        .lock()
                        .await
                        .modeled_outbound_transactions
                        .last()
                        .unwrap(),
                    budget,
                );
            }
            let mut expected = expected_children;
            expected.push(successor);
            expected.sort_by_key(|row| row.key);
            prop_assert_eq!(rows(&store).await, expected);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 4: Ownership changes cannot authorize stale commits
    #[test]
    fn close_batch_expansion_keeps_original_fence(count in 8usize..40, lost in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            store
                .set_outbound_write_budget(WriteBudget {
                    rows: 6,
                    bytes: 100_000,
                })
                .await;
            let initial = parent(count, 10, true);
            let open = applied(
                store
                    .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            applied(
                store
                    .commit_transition(open.run_key, close(&open), WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let key = rows(&store).await[0].key;
            store.inner.lock().await.bundle_leases.insert(
                ShardId(0),
                (
                    Some("source".into()),
                    ShardEpoch(if lost { 2 } else { 1 }),
                    None,
                ),
            );
            let before = store.snapshot().await.unwrap();
            let fence = WriteFence::Lease(crate::ExecutionHomeFence {
                home: ShardId(0),
                owner: "source".into(),
                epoch: ShardEpoch(1),
            });
            let result = store.expand_close_batch(key, fence).await;
            if lost {
                prop_assert!(
                    result
                        .unwrap_err()
                        .downcast_ref::<crate::LeaseFenceLost>()
                        .is_some()
                );
                prop_assert_eq!(store.snapshot().await.unwrap(), before);
            } else {
                prop_assert!(result.unwrap());
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof
    #[test]
    fn close_batch_and_expanded_children_pin_parent(count in 9usize..40, cut in 0usize..12) {
        block_on(async {
            let budget = WriteBudget {
                rows: 6,
                bytes: 100_000,
            };
            let mut store = InMemoryStore::default();
            store.set_outbound_write_budget(budget).await;
            let initial = parent(count, 10, true);
            let open = applied(
                store
                    .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let closed = applied(
                store
                    .commit_transition(open.run_key, close(&open), WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            store
                .delete_run_for_bundle(
                    closed.run_key,
                    ShardId(0),
                    crate::DeleteRunRequest {
                        expected_seq: closed.transition_seq,
                        deleted_at: closed.started_at,
                    },
                    WriteFence::Unfenced,
                )
                .await
                .unwrap();
            let mut page = 0;
            while let Some(batch) = rows(&store)
                .await
                .into_iter()
                .find(|row| row.key.kind == IntentKind::CloseBatch)
            {
                store.purge_run(closed.run_key).await.unwrap();
                prop_assert_eq!(
                    store.load_outbound_source(closed.run_key).await.unwrap(),
                    LoadedRun::Existing(closed.clone())
                );
                if page == cut {
                    store = restart(store, budget).await;
                }
                store
                    .expand_close_batch(batch.key, WriteFence::Unfenced)
                    .await
                    .unwrap();
                page += 1;
            }
            let children = rows(&store).await;
            prop_assert_eq!(children.len(), count);
            for row in children {
                store.purge_run(closed.run_key).await.unwrap();
                prop_assert!(matches!(
                    store.load_outbound_source(closed.run_key).await.unwrap(),
                    LoadedRun::Existing(_)
                ));
                store
                    .retire_intent(
                        IntentRetirement {
                            key: row.key,
                            outcome: DefinitiveOutcome::Absent,
                        },
                        WriteFence::Unfenced,
                    )
                    .await
                    .unwrap();
            }
            store.purge_run(closed.run_key).await.unwrap();
            prop_assert_eq!(
                store.load_outbound_source(closed.run_key).await.unwrap(),
                LoadedRun::Absent
            );
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
    #[test]
    fn obsolete_operation_pages_converge_without_delivery(
        count in 25usize..90, limit in 4usize..12,
        absent in any::<bool>(), keep_open in any::<bool>(),
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let budget = WriteBudget {
                rows: limit,
                bytes: 100_000,
            };
            store.set_outbound_write_budget(budget).await;
            let initial = parent(count, 10, false);
            let open = applied(
                store
                    .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let old = crate::derive_operations(&open, ShardId(0)).unwrap();
            let mut ending = close(&open);
            if keep_open {
                ending.next_state.status = open.status;
                ending.history_events.clear();
                ending.event_principals.clear();
            }
            let ended = applied(
                store
                    .commit_transition(open.run_key, ending, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            if absent {
                store.inner.lock().await.runs.remove(&open.run_key);
            }
            let target = fresh_transition(RunKey::new());
            let target = applied(
                store
                    .commit_transition(target.next_state.run_key, target, WriteFence::Unfenced)
                    .await
                    .unwrap(),
            );
            let mut iterations = 0;
            while store
                .inner
                .lock()
                .await
                .outbound_operations
                .keys()
                .any(|key| key.source_run == open.run_key)
            {
                let prior = store.inner.lock().await.outbound_operations.len();
                let guarded = store
                    .commit_outbound_transition(
                        target.run_key,
                        close(&target),
                        DeliveryGuard::Operation {
                            incarnation: old[0].incarnation,
                            source_seq: ended.transition_seq,
                        },
                        WriteFence::Unfenced,
                        WriteFence::Unfenced,
                    )
                    .await;
                prop_assert!(guarded.is_err());
                store
                    .reconcile_outbound_run(ShardId(0), open.run_key, WriteFence::Unfenced)
                    .await
                    .unwrap();
                let inner = store.inner.lock().await;
                within(*inner.modeled_outbound_transactions.last().unwrap(), budget);
                prop_assert!(inner.outbound_operations.len() < prior);
                iterations += 1;
            }
            prop_assert!(iterations > 1);
            prop_assert_eq!(
                store.load_run(target.run_key).await.unwrap(),
                LoadedRun::Existing(target)
            );
            Ok(())
        })?;
    }
}

fn committed_is_direct(rows: &[crate::IntentRow], budget: WriteBudget) -> bool {
    close_batch::fits(close_batch::intents_cost(rows).unwrap(), budget)
}

// Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
#[tokio::test]
async fn oversized_direct_successor_does_not_refuse_close() {
    for count in [0, 2, 8] {
        let budget = WriteBudget {
            rows: 6,
            bytes: 4_096,
        };
        let store = InMemoryStore::default();
        store.set_outbound_write_budget(budget).await;
        let initial = parent(count, 30, false);
        let open = applied(
            store
                .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                .await
                .unwrap(),
        );
        let (closing, mut next) = crate::atomic_successors_test_support::pair(
            &open,
            0,
            Default::default(),
            Default::default(),
            None,
        );
        next.next_state.run_key = RunKey::derive(
            next.next_state.namespace_id,
            &next.next_state.workflow_id,
            next.next_state.run_id,
        );
        let request = super::successor_tests::start_request(
            &next.next_state,
            &open,
            crate::value_parts::PART_BYTES + 1,
        );
        let mut expected = crate::derive_close_intents(Some(&open), &closing, ShardId(0)).unwrap();
        let (_, successor) = crate::outbound::successor::freeze(
            open.run_key,
            &closing,
            Box::new(request.clone()),
            ShardId(0),
        )
        .unwrap();
        let direct_cost = close_batch::intent_cost(&successor).unwrap();
        assert!(direct_cost.bytes > budget.bytes);
        assert!(direct_cost.rows > 1);
        applied(
            store
                .commit_successor_fallback(
                    open.run_key,
                    closing,
                    Box::new(request),
                    WriteFence::Unfenced,
                )
                .await
                .unwrap(),
        );
        let committed = rows(&store).await;
        assert!(committed.contains(&successor));
        assert_eq!(committed.len(), 1 + usize::from(count != 0));
        let inner = store.inner.lock().await;
        let tally = *inner.modeled_outbound_transactions.last().unwrap();
        within(
            WriteCost {
                rows: tally.rows,
                bytes: tally.bytes - direct_cost.bytes,
            },
            budget,
        );
        let intent_rows = direct_cost.rows + usize::from(count != 0);
        assert_eq!(
            inner.outbound_operations.len(),
            count.saturating_sub(budget.rows.saturating_sub(intent_rows)),
        );
        drop(inner);
        while let Some(batch) = rows(&store)
            .await
            .into_iter()
            .find(|row| row.key.kind == IntentKind::CloseBatch)
        {
            store
                .expand_close_batch(batch.key, WriteFence::Unfenced)
                .await
                .unwrap();
            within(
                *store
                    .inner
                    .lock()
                    .await
                    .modeled_outbound_transactions
                    .last()
                    .unwrap(),
                budget,
            );
        }
        expected.push(successor);
        expected.sort_by_key(|row| row.key);
        assert_eq!(rows(&store).await, expected);
    }
}

// Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
#[tokio::test]
async fn close_batch_at_maximum_children_and_workflow_id_length() {
    for mixed in [false, true] {
        let store = InMemoryStore::default();
        let initial = parent(2_000, 1_000, mixed);
        let open = applied(
            store
                .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
                .await
                .unwrap(),
        );
        let closing = close(&open);
        let mut expected = crate::derive_close_intents(Some(&open), &closing, ShardId(0)).unwrap();
        expected.sort_by_key(|row| row.key);
        applied(
            store
                .commit_transition(open.run_key, closing, WriteFence::Unfenced)
                .await
                .unwrap(),
        );
        within(
            *store
                .inner
                .lock()
                .await
                .modeled_outbound_transactions
                .last()
                .unwrap(),
            WriteBudget::TRANSACTION,
        );
        let initial_rows = rows(&store).await;
        assert_eq!(initial_rows.len(), 1);
        assert!(close_batch::intent_cost(&initial_rows[0]).unwrap().rows > 1);
        while let Some(batch) = rows(&store)
            .await
            .into_iter()
            .find(|row| row.key.kind == IntentKind::CloseBatch)
        {
            store
                .expand_close_batch(batch.key, WriteFence::Unfenced)
                .await
                .unwrap();
            within(
                *store
                    .inner
                    .lock()
                    .await
                    .modeled_outbound_transactions
                    .last()
                    .unwrap(),
                WriteBudget::TRANSACTION,
            );
        }
        assert_eq!(rows(&store).await, expected);
    }
}

// Feature: outbound-dispatch-recovery, Property 21: A close's outbound writes stay within budget
#[tokio::test]
async fn nested_close_batch_is_refused_without_writes() {
    let store = InMemoryStore::default();
    store
        .set_outbound_write_budget(WriteBudget {
            rows: 4,
            bytes: 100_000,
        })
        .await;
    let initial = parent(10, 10, false);
    let open = applied(
        store
            .commit_transition(initial.next_state.run_key, initial, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    applied(
        store
            .commit_transition(open.run_key, close(&open), WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    let key = rows(&store).await[0].key;
    if let IntentPayload::CloseBatch { obligations } = &mut store
        .inner
        .lock()
        .await
        .outbound_intents
        .get_mut(&key)
        .unwrap()
        .payload
    {
        obligations.push((
            100,
            IntentPayload::CloseBatch {
                obligations: Vec::new(),
            },
        ));
    }
    let before = store.snapshot().await.unwrap();
    let error = store
        .expand_close_batch(key, WriteFence::Unfenced)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<crate::RecoveryError>(),
        Some(crate::RecoveryError::InvalidIdentity { .. })
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
}
