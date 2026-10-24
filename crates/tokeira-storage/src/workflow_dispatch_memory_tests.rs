//! Memory-side adapters for the shared durable workflow dispatch contracts.

use super::*;
use crate::{
    BacklogPayload, ExecutionHomeFence,
    workflow_dispatch_tests::{
        Backend, ordered_pages, reset_materialization, routing_and_home_pages, run_atomic_cases,
        speculative_legacy_delivery,
    },
};
use tokeira_types::LogicalTaskSeq;

// Feature: workflow-dispatch, Property 9: Ownership loss fences repair
#[tokio::test]
async fn workflow_dispatch_repair_rejects_released_owner() {
    let store = InMemoryStore::default();
    let home = ShardId(0);
    let LeaseOutcome::Acquired { epoch } = store
        .try_acquire_bundle(home, "old".into(), "127.0.0.1:7233".into())
        .await
        .unwrap()
    else {
        panic!("initial lease must be acquired")
    };
    let transition = super::projection_accumulator_tests::fresh_transition(RunKey::new());
    let key = transition.next_state.run_key;
    store
        .commit_transition(key, transition, WriteFence::Unfenced)
        .await
        .unwrap();
    store.inner.lock().await.workflow_dispatch.remove(&key);
    store
        .relinquish_bundle(home, "old".into(), epoch)
        .await
        .unwrap();
    let error = store
        .reconcile_workflow_dispatch_run(
            home,
            key,
            WriteFence::Lease(ExecutionHomeFence {
                home,
                owner: "old".into(),
                epoch,
            }),
        )
        .await
        .expect_err("a released owner must not repair dispatch");
    assert!(error.is::<LeaseFenceLost>());
    assert!(store.row(key).await.unwrap().is_none());
}

#[async_trait]
impl Backend for InMemoryStore {
    fn repo(&self) -> &dyn RunRepository {
        self
    }

    async fn row(&self, key: RunKey) -> Result<Option<WorkflowDispatchRow>> {
        Ok(self.inner.lock().await.workflow_dispatch.get(&key).cloned())
    }

    async fn seed_stale_row(&self, state: &WorkflowState) -> Result<()> {
        let row = derive_workflow_dispatch(state, ShardId(0)).unwrap();
        self.inner
            .lock()
            .await
            .workflow_dispatch
            .insert(state.run_key, row);
        Ok(())
    }
}

#[test]
fn workflow_dispatch_atomic_reference_traces() {
    run_atomic_cases(
        &InMemoryStore::default(),
        &tokio::runtime::Runtime::new().unwrap(),
    );
}

#[tokio::test]
async fn workflow_dispatch_ordered_pages_and_snapshot_reconstruction() {
    let store = InMemoryStore::default();
    ordered_pages(&store).await;
    routing_and_home_pages(&store).await;
    let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
    assert_eq!(
        store.inner.lock().await.workflow_dispatch,
        restored.inner.lock().await.workflow_dispatch
    );
}

#[tokio::test]
async fn workflow_dispatch_preserves_speculative_legacy_delivery() {
    speculative_legacy_delivery(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn workflow_dispatch_reset_materialization() {
    reset_materialization(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn workflow_dispatch_queue_pages_span_execution_homes() {
    let store = InMemoryStore::with_shard_count(8);
    let initial = super::projection_accumulator_tests::fresh_transition(RunKey::new());
    let range = WorkflowDiscoveryRange {
        namespace_id: initial.next_state.namespace_id,
        queue_name: initial.next_state.task_queue.clone(),
        routing: crate::WorkflowDispatchRouting::Live,
    };
    let mut homes = std::collections::BTreeSet::new();
    for index in 0..32 {
        let mut transition = initial.clone();
        transition.next_state.run_key = RunKey::new();
        transition.next_state.workflow_id.0 = format!("home-{index}");
        let key = transition.next_state.run_key;
        let home = tokeira_types::execution_home_bundle(
            range.namespace_id.0.as_bytes(),
            transition.next_state.workflow_id.0.as_bytes(),
            8,
        );
        homes.insert(home);
        store
            .commit_transition(key, transition, WriteFence::Unfenced)
            .await
            .unwrap();
        let row = store.row(key).await.unwrap().unwrap();
        assert_eq!(row.execution_home, home);
        assert!(
            store
                .list_workflow_dispatch_for_home(home, None, std::num::NonZeroU32::new(64).unwrap())
                .await
                .unwrap()
                .contains(&key)
        );
    }
    assert!(homes.len() > 1);
    let page = store
        .list_workflow_dispatch_page(&range, None, std::num::NonZeroU32::new(64).unwrap())
        .await
        .unwrap();
    assert_eq!(page.candidates.len(), 32);
}

#[tokio::test]
async fn workflow_dispatch_digest_collision_advances_past_rejected_coordinates() {
    let store = InMemoryStore::default();
    let mut wanted = super::projection_accumulator_tests::fresh_transition(RunKey::new());
    wanted.next_state.priority = Some(tokeira_kernel::Priority {
        priority_key: 5,
        fairness_key: String::new(),
        fairness_weight: 1.0,
    });
    let mut collision = wanted.clone();
    collision.next_state.run_key = RunKey::new();
    collision.next_state.workflow_id.0 = "collision".into();
    collision.next_state.task_queue.0 = "different-queue".into();
    collision.next_state.priority.as_mut().unwrap().priority_key = 1;
    let range = WorkflowDiscoveryRange {
        namespace_id: wanted.next_state.namespace_id,
        queue_name: wanted.next_state.task_queue.clone(),
        routing: crate::WorkflowDispatchRouting::Live,
    };
    super::projection_accumulator_tests::commit(&store, wanted, false, WriteFence::Unfenced)
        .await
        .unwrap();
    super::projection_accumulator_tests::commit(&store, collision, false, WriteFence::Unfenced)
        .await
        .unwrap();
    let state = store.inner.lock().await;
    let page = workflow_dispatch_page_with_keys(
        &state,
        &range,
        None,
        std::num::NonZeroU32::new(1).unwrap(),
        |_| range.lookup_keys(),
    );
    assert!(!range.matches(&page.candidates[0]));
    let next = workflow_dispatch_page_with_keys(
        &state,
        &range,
        page.last_examined,
        std::num::NonZeroU32::new(1).unwrap(),
        |_| range.lookup_keys(),
    );
    assert!(range.matches(&next.candidates[0]));
}

#[test]
fn workflow_dispatch_generated_ordered_traversal() {
    crate::workflow_dispatch_tests::run_page_cases(
        &InMemoryStore::with_shard_count(8),
        &tokio::runtime::Runtime::new().unwrap(),
    );
}

#[test]
fn workflow_dispatch_generated_sticky_recovery() {
    crate::workflow_dispatch_tests::run_sticky_recovery_cases(
        &InMemoryStore::with_shard_count(8),
        &tokio::runtime::Runtime::new().unwrap(),
    );
}

#[async_trait]
impl crate::workflow_dispatch_tests::RepairBackend for InMemoryStore {
    async fn remove_row(&self, key: RunKey) -> Result<()> {
        self.inner.lock().await.workflow_dispatch.remove(&key);
        Ok(())
    }
    async fn legacy_recovery_flag(&self, _key: RunKey) -> Result<()> {
        // Memory derives recovery eligibility; it has no nullable persisted flag.
        Ok(())
    }
    async fn authority(&self, _keys: &[RunKey]) -> Result<Vec<u8>> {
        // The snapshot contains authority and every other store collection;
        // dispatch is deliberately reconstructed, not serialized.
        Ok(self.snapshot().await?)
    }
    async fn finish_repair_case(&self, keys: &[RunKey]) -> Result<()> {
        let mut store = self.inner.lock().await;
        for key in keys {
            store.runs.remove(key);
            store.workflow_dispatch.remove(key);
        }
        Ok(())
    }
}

#[test]
fn workflow_dispatch_generated_complete_repair() {
    crate::workflow_dispatch_tests::run_repair_cases(
        &InMemoryStore::default(),
        &tokio::runtime::Runtime::new().unwrap(),
    );
}

#[tokio::test]
async fn repair_rejects_wrong_home_and_invalid_encoding_without_partial_changes() {
    let store = InMemoryStore::default();
    let transition = super::projection_accumulator_tests::fresh_transition(RunKey::new());
    let key = transition.next_state.run_key;
    store
        .commit_transition(key, transition, WriteFence::Unfenced)
        .await
        .unwrap();
    let row = store.row(key).await.unwrap();
    assert!(
        store
            .reconcile_workflow_dispatch_run(ShardId(1), key, WriteFence::Unfenced)
            .await
            .is_err()
    );
    store
        .inner
        .lock()
        .await
        .runs
        .get_mut(&key)
        .unwrap()
        .pending_workflow_task
        .as_mut()
        .unwrap()
        .logical_seq = tokeira_types::LogicalTaskSeq(u64::MAX);
    let before = store.snapshot().await.unwrap();
    assert!(
        store
            .reconcile_workflow_dispatch_run(ShardId(0), key, WriteFence::Unfenced)
            .await
            .is_err()
    );
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert_eq!(store.row(key).await.unwrap(), row);
}

#[test]
fn dispatch_encoding_budget_rejects_column_and_row_overflow() {
    let state = super::projection_accumulator_tests::fresh_transition(RunKey::new()).next_state;
    let mut row = derive_workflow_dispatch(&state, ShardId(0)).unwrap();
    row.queue_name.0 = "q".repeat(1024 * 1024);
    assert!(row.validate().is_ok());
    row.queue_name.0.push('q');
    assert!(row.validate().is_err());
    row.queue_name.0.pop();
    row.normal_queue_name.0 = "n".repeat(1024 * 1024 - 4096);
    assert!(row.validate().is_ok());
    row.normal_queue_name.0.push('n');
    assert!(row.validate().is_err());
}

#[async_trait]
impl crate::workflow_dispatch_tests::DisposalBackend for InMemoryStore {
    async fn disposal_authority(&self, _keys: &[RunKey]) -> Result<Vec<u8>> {
        let state = self.inner.lock().await;
        Ok(format!(
            "{:?}{:?}{:?}{:?}",
            state.runs, state.history, state.transition_audit, state.projection_log
        )
        .into_bytes())
    }
    async fn backlog_bytes(&self, queue: &QueueKey) -> Result<Vec<Vec<u8>>> {
        self.inner
            .lock()
            .await
            .dispatch_backlog
            .iter()
            .filter(|entry| &entry.queue == queue)
            .map(|entry| Ok(format!("{entry:?}").into_bytes()))
            .collect()
    }
}

#[test]
fn workflow_dispatch_generated_legacy_disposal() {
    crate::workflow_dispatch_tests::run_disposal_cases(
        &InMemoryStore::default(),
        &tokio::runtime::Runtime::new().unwrap(),
    );
}

#[tokio::test]
async fn workflow_disposal_is_bounded_and_failure_leaves_the_entire_page_unchanged() {
    let store = InMemoryStore::default();
    let state = super::projection_accumulator_tests::fresh_transition(RunKey::new()).next_state;
    let queue = QueueKey {
        namespace_id: state.namespace_id,
        task_queue: state.task_queue,
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let entries = (0..130)
        .map(|_| BacklogEntry {
            run_key: RunKey::new(),
            queue: queue.clone(),
            payload: BacklogPayload::Workflow {
                logical_seq: LogicalTaskSeq::ONE,
            },
            priority: None,
            order: DeliveryOrder::default(),
            scheduled_at: state.started_at,
        })
        .collect();
    store.persist_to_backlog(entries).await.unwrap();
    let before = store.snapshot().await.unwrap();
    store.fail_bulk_write_after(0).await;
    assert!(store.dispose_workflow_backlog_page(None).await.is_err());
    assert_eq!(store.snapshot().await.unwrap(), before);
    let mut after = None;
    let mut sizes = vec![];
    loop {
        let page = store.dispose_workflow_backlog_page(after).await.unwrap();
        sizes.push(page.deleted);
        if page.exhausted {
            break;
        }
        after = page.last_examined;
    }
    assert_eq!(sizes, [64, 64, 2]);
    assert!(
        store
            .inner
            .lock()
            .await
            .modeled_transactions
            .iter()
            .all(
                |cost| cost.rows <= crate::write_budget::MAX_ROWS_PER_TRANSACTION
                    && cost.bytes <= crate::write_budget::MAX_BYTES_PER_TRANSACTION
            )
    );
}

// Feature: workflow-dispatch, Property 9: Ownership loss fences repair
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    #[test]
    fn workflow_dispatch_repair_generated_ownership(
        home_number in 0u32..8, delete in proptest::bool::ANY,
        loss in 0u8..3, repair_first in proptest::bool::ANY,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryStore::with_shard_count(8);
            let home = ShardId(home_number);
            let mut transition = super::projection_accumulator_tests::fresh_transition(RunKey::new());
            transition.next_state.workflow_id = (0..).map(|i| WorkflowId(format!("repair-fence-{i}")))
                .find(|id| tokeira_types::execution_home_bundle(
                    transition.next_state.namespace_id.0.as_bytes(), id.0.as_bytes(), 8) == home).unwrap();
            let key = transition.next_state.run_key;
            let LeaseOutcome::Acquired { epoch } = store.try_acquire_bundle(home, "old".into(), "endpoint".into()).await.unwrap() else { panic!("acquire") };
            let fence = WriteFence::Lease(ExecutionHomeFence { home, owner: "old".into(), epoch });
            store.commit_transition(key, transition, fence.clone()).await.unwrap();
            {
                let mut inner = store.inner.lock().await;
                if delete { inner.runs.remove(&key); }
                else { inner.workflow_dispatch.remove(&key); }
            }
            let authority = store.load_run(key).await.unwrap();
            if repair_first {
                store.reconcile_workflow_dispatch_run(home, key, fence.clone()).await.unwrap();
                assert_eq!(store.row(key).await.unwrap().is_some(), !delete);
            }
            if loss == 0 {
                assert_eq!(store.renew_bundle(home, "old".into(), epoch, "endpoint".into()).await.unwrap(), LeaseOutcome::Renewed { epoch });
            } else {
                store.relinquish_bundle(home, "old".into(), epoch).await.unwrap();
                if loss == 2 {
                    let LeaseOutcome::Acquired { epoch: next } = store.try_acquire_bundle(home, "new".into(), "endpoint".into()).await.unwrap() else { panic!("successor") };
                    store.reconcile_workflow_dispatch_run(home, key, WriteFence::Lease(ExecutionHomeFence { home, owner: "new".into(), epoch: next })).await.unwrap();
                }
            }
            let row_before = store.row(key).await.unwrap();
            let result = store.reconcile_workflow_dispatch_run(home, key, fence).await;
            if loss == 0 {
                assert!(result.is_ok());
                assert_eq!(store.row(key).await.unwrap().is_some(), !delete);
            } else {
                assert!(result.unwrap_err().is::<LeaseFenceLost>());
                assert_eq!(store.row(key).await.unwrap(), row_before);
            }
            assert_eq!(store.load_run(key).await.unwrap(), authority);
            let wrong_home = WriteFence::Lease(ExecutionHomeFence { home: ShardId((home_number+1)%8), owner: "old".into(), epoch });
            assert!(store.reconcile_workflow_dispatch_run(home, key, wrong_home).await.is_err());
        });
    }
}
