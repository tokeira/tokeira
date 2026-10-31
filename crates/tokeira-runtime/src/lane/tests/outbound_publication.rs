//! Multi-run publication invalidates even cached kernel no-ops at admission.

use super::*;
use tokeira_types::TransitionSeq;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 12: Reset publication is all or nothing
    #[test]
    fn cached_noop_waiting_at_publication_reloads_after_generation_change(sequence in 1u64..1000, advances in 1u64..100) {
        Runtime::new().unwrap().block_on(async {
            let run = RunKey::new();
            let mut state = sample_state(run); state.transition_seq = TransitionSeq(sequence);
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), Vec::new());
            let owner = test_shard_owner();
            let home = execution_home_bundle(state.namespace_id.0.as_bytes(), state.workflow_id.0.as_bytes(), owner.read().unwrap().shard_count());
            let barrier = owner.read().unwrap().write_barrier(home);
            let exclusive = barrier.write_owned().await;
            let mut cache = LaneCache::new(&LaneConfig::default());
            cache.synchronize_generation(&owner);
            cache.insert(run, LoadedRun::Existing(state.clone()));
            let kernel = MockKernel::new(SmallVec::new()).with_noop();
            let updates = UpdateRegistry::new();
            let mut admission = None;
            let pending = handle_message_with_cache(&kernel, &repo, &owner, &updates, run, sample_command("noop"), 1, &mut cache, &mut admission);
            tokio::pin!(pending);
            tokio::select! {
                biased;
                _ = &mut pending => panic!("no-op passed exclusive publication barrier"),
                _ = tokio::task::yield_now() => {}
            }
            prop_assert!(kernel.state.lock().unwrap().applied_commands.is_empty());
            state.transition_seq = TransitionSeq(sequence + advances);
            repo.state.lock().await.loaded = LoadedRun::Existing(state.clone());
            owner.write().unwrap().invalidate_published_states();
            drop(exclusive);
            let (result, _, _) = pending.await.unwrap();
            let CommitResult::Applied { new_state } = result else { panic!("no-op applies"); };
            prop_assert_eq!(new_state.transition_seq, state.transition_seq);
            prop_assert_eq!(repo.snapshot().await.1, 0);
            Ok(())
        })?;
    }
}
