//! Publication survives restart and never exposes a subset of its participants.

use super::*;
use crate::{
    PublicationResult, RunPreparation,
    memory::projection_accumulator_tests::{applied, fresh_transition},
};
use proptest::prelude::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

async fn fixture() -> (InMemoryStore, WorkflowState, RunPreparation) {
    let store = InMemoryStore::default();
    let mut start = fresh_transition(RunKey::new());
    let state = &mut start.next_state;
    state.run_key = RunKey::derive(state.namespace_id, &state.workflow_id, state.run_id);
    let state = applied(
        store
            .commit_transition(state.run_key, start, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    let (closing, mut successor) = crate::atomic_successors_test_support::pair(
        &state,
        0,
        tokeira_types::Payloads::default(),
        Default::default(),
        None,
    );
    let next = &mut successor.next_state;
    next.run_key = RunKey::derive(next.namespace_id, &next.workflow_id, next.run_id);
    let manifest = RunPreparation {
        request_key: format!("{}-reset-request", state.run_key.0),
        expected_current: Some(state.run_key),
        participants: vec![closing],
        new_run: Box::new(successor),
        successor_intent: None,
    };
    (store, state, manifest)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 12: Reset publication is all or nothing
    #[test]
    fn staged_publication_is_invisible_and_survives_restart(restart in any::<bool>(), lose_reply in any::<bool>()) {
        block_on(async {
            let (store, state, manifest) = fixture().await;
            let target = manifest.new_run.next_state.run_key;
            let token = store.prepare_run_publication(Box::new(manifest.clone()), WriteFence::Unfenced).await.unwrap();
            prop_assert_eq!(store.load_run(target).await.unwrap(), LoadedRun::Absent);
            prop_assert!(store.read_history(target, 0, 100).await.unwrap().is_empty());
            prop_assert_eq!(store.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(state.clone()));
            let store = if restart { InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap() } else { store };
            let selected = store.prepare_run_publication(Box::new(manifest), WriteFence::Unfenced).await.unwrap();
            prop_assert_eq!(&selected, &token);
            let result = store.publish_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap();
            prop_assert!(matches!(result, PublicationResult::Applied { .. }), "publication applies");
            prop_assert!(matches!(store.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(s) if !s.status.is_open()));
            prop_assert!(matches!(store.load_run(target).await.unwrap(), LoadedRun::Existing(s) if s.status.is_open()));
            prop_assert_eq!(store.inner.lock().await.current_execution.get(&(state.namespace_id, state.workflow_id.0)).copied(), Some(target));
            if lose_reply {
                prop_assert!(matches!(store.publish_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap(), PublicationResult::AlreadyPublished { .. }), "lost reply resolves");
            }
            let pending = store.read_published_preparations(token.home, crate::ScanBound::Capture, None, 1).await.unwrap();
            prop_assert_eq!(&pending.rows, &vec![token.clone()]);
            prop_assert!(!store.abandon_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap());
            prop_assert!(store.complete_run_preparation(token.clone(), WriteFence::Unfenced).await.unwrap());
            let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            prop_assert!(restored.read_published_preparations(token.home, crate::ScanBound::Capture, None, 1).await.unwrap().rows.is_empty());
            prop_assert!(matches!(restored.publish_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap(), PublicationResult::AlreadyPublished { .. }), "compact generation proof survives manifest reclamation");
            prop_assert!(!restored.abandon_prepared_run(token, WriteFence::Unfenced).await.unwrap());
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 12: Reset publication is all or nothing
    #[test]
    fn changed_participant_or_pointer_keeps_all_staged_data_invisible(change_pointer in any::<bool>(), late_purge in any::<bool>()) {
        block_on(async {
            let (store, state, manifest) = fixture().await;
            let target = manifest.new_run.next_state.run_key;
            let token = store.prepare_run_publication(Box::new(manifest.clone()), WriteFence::Unfenced).await.unwrap();
            {
                let mut inner = store.inner.lock().await;
                if change_pointer { inner.current_execution.insert((state.namespace_id, state.workflow_id.0.clone()), RunKey::new()); }
                else { inner.runs.get_mut(&state.run_key).unwrap().transition_seq = state.transition_seq.next(); }
            }
            let before = store.snapshot().await.unwrap();
            prop_assert!(matches!(store.publish_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap(), PublicationResult::Conflict { .. }), "changed authority conflicts");
            prop_assert_eq!(store.snapshot().await.unwrap(), before);
            prop_assert!(store.abandon_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap());
            let replacement = store.prepare_run_publication(Box::new(manifest), WriteFence::Unfenced).await.unwrap();
            prop_assert_ne!(&replacement, &token);
            if late_purge { prop_assert!(!store.abandon_prepared_run(token, WriteFence::Unfenced).await.unwrap()); }
            prop_assert_eq!(store.load_run(target).await.unwrap(), LoadedRun::Absent);
            prop_assert!(store.inner.lock().await.preparations.contains_key(&replacement.id));
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 15: Large recovery data round-trips within bounded writes
    #[test]
    fn manifest_preserves_required_state_extensions_and_detects_corruption(bytes in 0usize..2048, bit in 0u8..8) {
        block_on(async {
            let (_, _, mut manifest) = fixture().await;
            manifest.new_run.next_state.outbound.as_mut().unwrap().reset_request_id = Some(RequestId("r".repeat(bytes + 1)));
            let encoded = manifest.encode().unwrap();
            prop_assert_eq!(RunPreparation::decode(&encoded).unwrap(), manifest.clone());
            let token = crate::PreparationToken::new(&manifest.new_run.next_state, ShardId(0), &encoded);
            let mut corrupt = encoded;
            let last = corrupt.len() - 1;
            corrupt[last] ^= 1 << bit;
            prop_assert!(token.decode_manifest(&corrupt).is_err());
            Ok(())
        })?;
    }
}
