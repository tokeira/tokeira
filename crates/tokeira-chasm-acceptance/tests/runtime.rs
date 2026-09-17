//! Recovery and generated delivery/clock proofs over a shared durable repository.
#![cfg(test)]

#[path = "support/commands.rs"]
mod commands;
#[path = "support/runtime.rs"]
mod fixture;
#[path = "support/trace.rs"]
mod trace;

use std::sync::Arc;

use fixture::{Clock, QUEUE, SECOND, Stack, key, reference, target};
use proptest::prelude::*;
use prost::Message;
use tokeira_chasm::{Component, ExecutionKey, Registry, archetype_id_for_fqn};
use tokeira_chasm_acceptance::{AcceptanceLibrary, OperationOutcome, Resource};
use tokeira_chasm_activity::statemachine::terminal_outcome;
use tokeira_projection::{
    ComponentVisibility, InMemoryVisibilityStore, PageBounds, SearchAttrType, SortOrder,
    VisibilitySink, VisibilityStore, compile_filter,
};
use tokeira_proto::{
    enums::CallbackState,
    failure::{Failure, failure::FailureInfo},
};
use tokeira_runtime::chasm::{OutcomeApplied, ProjectionVisibilitySink, VisibilityRepairScanner};
use tokeira_storage::{ChasmNodeRepository, InMemoryChasmNodeStore};
use tokeira_types::{ArchetypeId, NamespaceId, SearchAttrValue};
use uuid::Uuid;

async fn create_and_update(stack: &Stack) {
    let handle = stack.handle();
    assert_eq!(
        commands::create(&handle, key(), "r1", "initial", target(), QUEUE)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        commands::update(&handle, &reference(key()), 1, "desired-two")
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn restart_delivers_a_committed_terminal_outcome_without_a_request() {
    let store = Arc::new(InMemoryChasmNodeStore::new());
    let clock = Clock::new(100 * SECOND);
    let stack = Stack::new(store.clone(), &clock, false);
    create_and_update(&stack).await;
    let task = stack.poll().await.unwrap();
    stack.complete(&task, false).await;
    assert_eq!(
        stack.activity_state(&task).await.callbacks[0].state(),
        CallbackState::Scheduled
    );
    assert_eq!(
        commands::read(&stack.handle(), &reference(key()))
            .await
            .unwrap()
            .observed_generation,
        1
    );
    let weak = Arc::downgrade(&stack.engine);
    drop(stack);
    assert!(weak.upgrade().is_none());
    let rebuilt = Stack::new(store, &clock, true);
    rebuilt.rebuild.rebuild_once().await.unwrap();
    let view = commands::read(&rebuilt.handle(), &reference(key()))
        .await
        .unwrap();
    assert_eq!(view.observed_generation, 2);
    assert_eq!(view.history.len(), 1);
    assert!(view.active_operation.is_none());
    assert_eq!(
        rebuilt.activity_state(&task).await.callbacks[0].state(),
        CallbackState::Succeeded
    );
}

#[tokio::test]
async fn restart_rearms_retry_and_fires_only_at_its_deadline() {
    let store = Arc::new(InMemoryChasmNodeStore::new());
    let clock = Clock::new(100 * SECOND);
    let stack = Stack::new(store.clone(), &clock, true);
    create_and_update(&stack).await;
    let task = stack.poll().await.unwrap();
    stack.complete(&task, true).await;
    let deadline = stack.engine.armed_timer(&key()).unwrap();
    assert_eq!(deadline, 101 * SECOND);
    drop(stack);
    let rebuilt = Stack::new(store, &clock, true);
    rebuilt.rebuild.rebuild_once().await.unwrap();
    assert_eq!(rebuilt.engine.armed_timer(&key()), Some(deadline));
    clock.set(deadline - 1);
    assert_eq!(rebuilt.sweeper.sweep_once().await, 0);
    assert!(rebuilt.queue.snapshot().is_empty());
    clock.set(deadline);
    assert_eq!(rebuilt.sweeper.sweep_once().await, 1);
    let queued = rebuilt.queue.snapshot();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].key.business_id, "resource/gen-2/try-2");
    assert_eq!(queued[0].target, Some(target()));
    let retry = rebuilt.poll().await.unwrap();
    assert_ne!(retry.run_id, task.run_id);
    rebuilt.complete(&retry, false).await;
    let view = commands::read(&rebuilt.handle(), &reference(key()))
        .await
        .unwrap();
    assert_eq!(view.observed_generation, 2);
    assert_eq!(view.retry_attempt, 0);
    assert!(view.last_failure.is_empty());
    assert_eq!(view.history.len(), 2);
}

#[tokio::test]
async fn typed_resource_attributes_are_queryable_through_the_real_projection_adapter() {
    let visibility = InMemoryVisibilityStore::default();
    let namespace = NamespaceId(Uuid::parse_str(&key().namespace_id).unwrap());
    for (name, kind) in [
        ("DeploymentStatus", SearchAttrType::Keyword),
        ("DesiredGeneration", SearchAttrType::Int),
        ("ObservedGeneration", SearchAttrType::Int),
    ] {
        visibility
            .register_attr(namespace, name.into(), kind)
            .await
            .unwrap();
    }
    let adapter = Arc::new(ProjectionVisibilitySink::new(
        Arc::new(VisibilitySink::new(visibility.clone())),
        1,
    ));
    let nodes = Arc::new(InMemoryChasmNodeStore::new());
    let stack = Stack::with_visibility(nodes.clone(), &Clock::new(100 * SECOND), true, adapter);
    create_and_update(&stack).await;
    stack.complete(&stack.poll().await.unwrap(), false).await;
    commands::update(&stack.handle(), &reference(key()), 2, "desired-three")
        .await
        .unwrap();
    stack.complete(&stack.poll().await.unwrap(), true).await;
    let mut filter = compile_filter(
        Some("DesiredGeneration = 3 AND ObservedGeneration = 2"),
        namespace,
        &visibility,
    )
    .await
    .unwrap();
    filter.archetype = Some(ArchetypeId(archetype_id_for_fqn(Resource::FQN)));
    let rows = visibility
        .list_executions(
            namespace,
            &filter,
            SortOrder::Default,
            &PageBounds {
                limit: 10,
                after: None,
            },
        )
        .await
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].business_id, key().business_id);
    assert_eq!(
        rows[0].search_attributes.0["DesiredGeneration"],
        SearchAttrValue::Int(3)
    );
    assert_eq!(
        rows[0].search_attributes.0["ObservedGeneration"],
        SearchAttrValue::Int(2)
    );
    assert_eq!(
        rows[0].search_attributes.0["DeploymentStatus"],
        SearchAttrValue::Keyword("Failed".into())
    );
    // Feature: chasm-extension-visibility, Property 3: repair recreates the same queried image from committed roots.
    let archetype = ArchetypeId(archetype_id_for_fqn(Resource::FQN));
    let original = ComponentVisibility::new(Arc::new(visibility), namespace, archetype)
        .list(Default::default())
        .await
        .unwrap();
    let repaired = InMemoryVisibilityStore::default();
    for (name, kind) in [
        ("DeploymentStatus", SearchAttrType::Keyword),
        ("DesiredGeneration", SearchAttrType::Int),
        ("ObservedGeneration", SearchAttrType::Int),
    ] {
        repaired
            .register_attr(namespace, name.into(), kind)
            .await
            .unwrap();
    }
    let query = ComponentVisibility::new(Arc::new(repaired.clone()), namespace, archetype);
    assert_eq!(query.count(None).await.unwrap(), 0);
    let mut registry = Registry::builder();
    registry.register_library::<AcceptanceLibrary>().unwrap();
    let registry = registry.build();
    let repair = VisibilityRepairScanner::new(
        nodes,
        Arc::new(VisibilitySink::new(repaired)),
        Arc::new(move |id, bytes| registry.visibility_snapshot(id, bytes).ok().flatten()),
        1,
    );
    assert_eq!(repair.repair_once().await.unwrap().rebuilt, 1);
    assert_eq!(query.list(Default::default()).await.unwrap(), original);
    repair.repair_once().await.unwrap();
    assert_eq!(query.list(Default::default()).await.unwrap(), original);
}

async fn delivery_case(failed: bool, crash: u8, deleted: bool, start_time: i64) {
    let store = Arc::new(InMemoryChasmNodeStore::new());
    let clock = Clock::new(start_time);
    let stack = Stack::new(store.clone(), &clock, crash == 2);
    create_and_update(&stack).await;
    let staged = stack
        .engine
        .root_node(&key())
        .await
        .unwrap()
        .unwrap()
        .metadata
        .outbox
        .side_effect_tasks[0]
        .clone();
    let task = stack.poll().await.unwrap();
    stack.complete(&task, failed).await;
    if crash == 1 {
        let outcome = terminal_outcome(&stack.activity_state(&task).await).unwrap();
        assert!(matches!(
            stack
                .engine
                .apply_side_effect_outcome(&key(), staged.task_type_id, staged.id, outcome)
                .await
                .unwrap(),
            OutcomeApplied::Applied(_)
        ));
    }
    if deleted {
        store.delete_execution(&key()).await.unwrap();
    }
    drop(stack);
    let rebuilt = Stack::new(store.clone(), &clock, true);
    rebuilt.rebuild.rebuild_once().await.unwrap();
    let state = rebuilt.activity_state(&task).await;
    let callback = &state.callbacks[0];
    if deleted && crash < 2 {
        assert_eq!(callback.state(), CallbackState::Failed);
        let failure = Failure::decode(callback.last_attempt_failure.as_slice()).unwrap();
        for part in [&key().namespace_id, &key().business_id, &key().run_id] {
            assert!(failure.message.contains(part));
        }
        assert!(
            matches!(failure.failure_info, Some(FailureInfo::ApplicationFailureInfo(info)) if info.non_retryable)
        );
        assert_eq!(callback.next_attempt_time_nanos, 0);
    } else {
        // A settled callback remains succeeded if its parent is deleted later;
        // missing-target failure is a decision made only on a pending delivery.
        assert_eq!(callback.state(), CallbackState::Succeeded);
    }
    assert_eq!(callback.attempt, 1);
    if !deleted {
        let view = commands::read(&rebuilt.handle(), &reference(key()))
            .await
            .unwrap();
        assert_eq!(view.history.len(), 1);
        assert_eq!(view.history[0].generation, 2);
        assert_eq!(
            view.history[0].outcome(),
            if failed {
                OperationOutcome::Failed
            } else {
                OperationOutcome::Completed
            }
        );
        assert_eq!(view.observed_generation, if failed { 1 } else { 2 });
        let before = rebuilt.engine.root_node(&key()).await.unwrap();
        rebuilt.rebuild.rebuild_once().await.unwrap();
        assert_eq!(rebuilt.engine.root_node(&key()).await.unwrap(), before);
    }
    let activity_key = ExecutionKey::new(
        key().namespace_id,
        task.activity_id.clone(),
        task.run_id.clone(),
    );
    assert!(
        rebuilt
            .engine
            .root_node(&activity_key)
            .await
            .unwrap()
            .unwrap()
            .metadata
            .outbox
            .is_empty()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 11: internal delivery exactly once
    // Crashes replay only held work; deleting a pending target fails permanently.
    #[test]
    fn internal_delivery_survives_crashes(failed in any::<bool>(), crash in 0u8..3, deleted in any::<bool>(), seconds in 1i64..10000) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(delivery_case(failed, crash, deleted, seconds * SECOND));
    }

    // Feature: chasm-extension-archetypes, Property 14: clock determinism
    // Exact persisted/derived traces differ only in mapped executor-minted run ids.
    #[test]
    fn virtual_clock_replay_is_deterministic(seed in 1i64..10000, script in prop::collection::vec((0u8..8, 0u8..35), 1..33)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let first = trace::run(seed * SECOND, &script).await;
            let second = trace::run(seed * SECOND, &script).await;
            prop_assert_eq!(first, second);
            Ok(())
        })?;
    }
}
