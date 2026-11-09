//! The shared lane cache across ownership changes, with several runs' loads
//! in flight at once.

use super::*;

/// What reads a run's image in the first round.
#[derive(Clone, Copy, Debug)]
enum Reader {
    Activation,
    Lookup,
}

fn reader() -> impl Strategy<Value = Reader> {
    prop_oneof![Just(Reader::Activation), Just(Reader::Lookup)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: lane-run-concurrency, Property 7: The cache never feeds a commit a stale-generation image
    // However an ownership change overtakes runs' loads, and whichever task
    // moves the shared cache forward first, every commit's image was read
    // under the owner's generation at that commit, and the cache stays
    // within its capacity.
    #[test]
    fn the_cache_never_feeds_a_commit_a_stale_generation_image(
        readers in prop::collection::vec(reader(), 1..5),
        warm in any::<bool>(),
        bump_during_loads in any::<bool>(),
        lookup_after_bump in any::<bool>(),
        metadata_first in any::<bool>(),
        capacity in 1usize..4,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let owner = test_shard_owner();
            let keys: Vec<RunKey> = readers.iter().map(|_| RunKey::new()).collect();
            let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
            repo.stamp_generations(owner.clone()).await;
            let lane = spawn_slotted_lane(
                0,
                MockKernel::new(SmallVec::new()),
                repo.clone(),
                owner.clone(),
                LaneConfig {
                    cache_max_entries: capacity,
                    ..LaneConfig::default()
                },
                Arc::new(Semaphore::new(keys.len())),
            );
            if warm {
                for (index, key) in keys.iter().enumerate() {
                    lane.submit(*key, sample_command(&format!("warm-{index}"))).await.unwrap();
                }
                if metadata_first {
                    // An outbound metadata commit joins the run's queue and
                    // evicts its image, so the next command reloads.
                    let LoadedRun::Existing(mut next) = repo.snapshot().await.2 else {
                        panic!("the warm commits left an image");
                    };
                    next.run_key = keys[0];
                    let metadata = Transition {
                        expected_seq: next.transition_seq,
                        next_state: next,
                        history_events: SmallVec::new(),
                        event_principals: SmallVec::new(),
                        request_dedupe_ops: SmallVec::new(),
                        activity_ops: SmallVec::new(),
                        timer_ops: SmallVec::new(),
                        dispatch_ops: SmallVec::new(),
                        events_numbered_at_close: 0,
                        workflow_task_heartbeat_timed_out: false,
                        growth_limits: None,
                    };
                    lane.commit_outbound_metadata(
                        Box::new(metadata),
                        crate::outbound::driver::Attempt::capture(&owner, ShardId(0)).unwrap(),
                    )
                    .await
                    .unwrap();
                }
                // Every warm image now predates the owner's generation.
                owner.write().unwrap().invalidate_published_states();
            }

            let mut held = Vec::new();
            let mut releases = Vec::new();
            for key in &keys {
                let (hold, release) = repo.hold_run_load(*key).await;
                held.push(hold);
                releases.push(release);
            }
            let mut first_round = tokio::task::JoinSet::new();
            for (index, (key, reader)) in keys.iter().zip(&readers).enumerate() {
                let lane = lane.clone();
                let repo = repo.clone();
                let key = *key;
                let reader = *reader;
                first_round.spawn(async move {
                    match reader {
                        Reader::Activation => lane
                            .submit(key, sample_command(&format!("first-{index}")))
                            .await
                            .map(drop),
                        Reader::Lookup => lane.execution_identity(key, &repo).await.map(drop),
                    }
                });
            }
            for hold in held {
                hold.await.unwrap();
            }
            // Every first-round load has started under the current generation
            // and is parked; an ownership change now overtakes all of them.
            if bump_during_loads {
                owner.write().unwrap().invalidate_published_states();
            }
            if lookup_after_bump {
                // A lookup for another run moves the shared cache forward
                // before the parked loads come back.
                lane.execution_identity(RunKey::new(), &repo).await.unwrap();
            }
            for release in releases {
                let _ = release.send(());
            }
            while let Some(done) = first_round.join_next().await {
                done.unwrap().unwrap();
            }

            // A second command per run commits from whatever the cache holds.
            for (index, key) in keys.iter().enumerate() {
                lane.submit(*key, sample_command(&format!("second-{index}"))).await.unwrap();
            }

            let stamps = repo.stamps().await;
            prop_assert!(!stamps.is_empty());
            for (image, generation) in stamps {
                prop_assert_eq!(image, generation, "a commit used an image from an older generation");
            }
            prop_assert!(lane.shared.lock().cache.entries.len() <= capacity);
            Ok(())
        })?;
    }
}

#[test]
fn the_cache_refuses_an_image_from_an_older_generation() {
    let mut cache = LaneCache::new(&LaneConfig::default());
    let key = RunKey::new();
    cache.synchronize_to(2);
    cache.insert_at(key, LoadedRun::Existing(sample_state(key)), 1);
    assert!(cache.get(key).is_none(), "an older image was installed");

    // A newer image moves the cache forward, and an older read can't move it
    // back.
    cache.insert_at(key, LoadedRun::Existing(sample_state(key)), 3);
    assert_eq!(cache.generation, 3);
    assert!(!cache.synchronize_to(2));
    assert!(cache.get(key).is_some());

    // An older image also drops the run's entry rather than leave it beside a
    // write the cache refused.
    cache.insert_at(key, LoadedRun::Existing(sample_state(key)), 2);
    assert!(cache.get(key).is_none());
}
