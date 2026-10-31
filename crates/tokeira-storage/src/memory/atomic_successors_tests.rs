//! Pair publication is indivisible across every durable in-memory map.

use proptest::prelude::*;

use super::*;
use crate::{
    PairCommitResult,
    memory::projection_accumulator_tests::{applied, fresh_transition},
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn pair(state: &WorkflowState, kind: u8, bytes: Vec<u8>) -> (Transition, Transition) {
    crate::atomic_successors_test_support::pair(
        state,
        kind,
        tokeira_types::Payloads(vec![tokeira_types::Payload::new(bytes)]),
        Default::default(),
        None,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Feature: atomic-successors, Property 1: A close and its successor commit together.
    #[test]
    fn pair_is_atomic_at_every_failure(kind in 0u8..4, bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        block_on(async {
            // The omitted call models a crash before the transaction. Every
            // other cut interrupts preparation before its publication boundary.
            for failure in 0..=7 {
                let store = InMemoryStore::default();
                let start = fresh_transition(RunKey::new());
                let state = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
                let (closing, successor) = pair(&state, kind, bytes.clone());
                let successor_key = successor.next_state.run_key;
                let before = store.snapshot().await.unwrap();
                if failure < 7 {
                    store.inner.lock().await.pair_failure = Some(failure);
                    let result = store.commit_transition_pair(state.run_key, closing.clone(), successor_key, successor.clone(), WriteFence::Unfenced).await;
                    prop_assert!(result.is_err(), "injected cut {failure} was not reached");
                }
                prop_assert_eq!(store.snapshot().await.unwrap(), before, "pair failure exposed a partial write at cut {}", failure);
                let result = store.commit_transition_pair(state.run_key, closing, successor_key, successor, WriteFence::Unfenced).await.unwrap();
                prop_assert!(matches!(result, PairCommitResult::Applied { .. }), "pair applies");
                prop_assert!(matches!(store.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(s) if !s.status.is_open()));
                prop_assert!(matches!(store.load_run(successor_key).await.unwrap(), LoadedRun::Existing(s) if s.status.is_open()));
                prop_assert_eq!(store.inner.lock().await.current_execution.get(&(state.namespace_id, state.workflow_id.0)).copied(), Some(successor_key));
            }
            Ok(())
        })?;
    }

    // Feature: atomic-successors, Property 2: The pointer moves only from the closing run.
    #[test]
    fn pair_requires_closing_pointer(pointer in 0u8..3, open in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = fresh_transition(RunKey::new());
            let state = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let (closing, successor) = pair(&state, 0, Vec::new());
            let successor_key = successor.next_state.run_key;
            let workflow_key = (state.namespace_id, state.workflow_id.0.clone());
            {
                let mut inner = store.inner.lock().await;
                let key = if pointer == 1 { state.run_key } else { RunKey::new() };
                if pointer == 0 { inner.current_execution.remove(&workflow_key); }
                else {
                    inner.current_execution.insert(workflow_key.clone(), key);
                    let mut current = state.clone(); current.run_key = key;
                    current.status = if open { ExecutionStatus::Running } else { ExecutionStatus::Completed };
                    // The pointer's status must not relax identity equality. Keep
                    // the closing run's sequence intact even when it is current.
                    if pointer == 2 { inner.runs.insert(key, current); }
                    if open { inner.current_open.insert(workflow_key.clone(), key); }
                    else { inner.current_open.remove(&workflow_key); }
                }
            }
            let before = store.snapshot().await.unwrap();
            let result = store.commit_transition_pair(state.run_key, closing, successor_key, successor, WriteFence::Unfenced).await.unwrap();
            if pointer == 1 { prop_assert!(matches!(result, PairCommitResult::Applied { .. }), "pair applies"); }
            else {
                prop_assert!(matches!(result, PairCommitResult::Conflict { .. }), "a foreign or absent pointer must conflict");
                prop_assert_eq!(store.snapshot().await.unwrap(), before);
            }
            Ok(())
        })?;
    }
}
