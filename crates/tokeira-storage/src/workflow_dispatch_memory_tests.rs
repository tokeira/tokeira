//! Memory-side adapters for the shared durable workflow dispatch contracts.

use super::*;
use crate::workflow_dispatch_tests::{
    Backend, ordered_pages, reset_materialization, routing_and_home_pages, run_atomic_cases,
    speculative_legacy_delivery,
};

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
            .commit_transition(key, transition, ShardEpoch::ZERO)
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
    super::projection_accumulator_tests::commit(&store, wanted, false, ShardEpoch::ZERO)
        .await
        .unwrap();
    super::projection_accumulator_tests::commit(&store, collision, false, ShardEpoch::ZERO)
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
