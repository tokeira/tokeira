//! Per-run order and exclusion while runs on one lane interleave.

use super::*;

#[derive(Clone, Debug)]
enum Step {
    /// Submit the next command for run `n`.
    Submit(usize),
    /// Release run `n`'s parked commit, if it has one.
    Release(usize),
}

fn steps(runs: usize) -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(
        prop_oneof![
            3 => (0..runs).prop_map(Step::Submit),
            1 => (0..runs).prop_map(Step::Release),
        ],
        1..48,
    )
}

fn run_label(run: usize, seq: usize) -> String {
    format!("{run}-{seq}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: lane-run-concurrency, Property 1: Per-run order holds under concurrency
    // However runs' submissions interleave and whichever runs' commits are
    // held, no run has two activations in flight and the kernel sees each
    // run's commands in the order the lane accepted them.
    #[test]
    fn per_run_order_holds_under_concurrency(
        (runs, plan) in (1usize..5).prop_flat_map(|runs| (Just(runs), steps(runs))),
        parked in prop::collection::vec(any::<bool>(), 4),
        drain in 1u32..4,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let keys: Vec<RunKey> = (0..runs).map(|_| RunKey::new()).collect();
            let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
            let mut releases: Vec<Option<oneshot::Sender<()>>> = Vec::new();
            for (run, key) in keys.iter().enumerate() {
                if parked[run] {
                    let (_held, release) = repo.hold_run_commit(*key).await;
                    releases.push(Some(release));
                } else {
                    releases.push(None);
                }
            }
            let kernel = MockKernel::new(SmallVec::new());
            let lane = spawn_lane(
                kernel.clone(),
                repo.clone(),
                MockPublisher::new(),
                test_shard_owner(),
                crate::activity_timeout::ActivityTrackingState::default(),
                crate::timeout::WorkflowTimeoutTrackingState::default(),
                crate::wft_timeout::WftTimeoutTrackingState::default(),
                crate::nexus::NexusTimeoutTrackingState::default(),
                crate::UpdateRegistry::new(),
                LaneConfig {
                    max_drain_per_activation: drain,
                    ..LaneConfig::default()
                },
            );

            let mut accepted: Vec<Vec<String>> = vec![Vec::new(); runs];
            let mut replies = Vec::new();
            for step in &plan {
                match *step {
                    Step::Submit(run) => {
                        let label = run_label(run, accepted[run].len());
                        let (reply_tx, reply_rx) = oneshot::channel();
                        lane.try_enqueue(LaneMessage::new(
                            0,
                            keys[run],
                            sample_command(&label),
                            reply_tx,
                        ))
                        .unwrap();
                        accepted[run].push(label);
                        replies.push(reply_rx);
                    }
                    Step::Release(run) => {
                        if let Some(release) = releases[run].take() {
                            let _ = release.send(());
                        }
                    }
                }
            }
            for release in releases.iter_mut().filter_map(Option::take) {
                let _ = release.send(());
            }
            for reply in replies {
                let applied = matches!(reply.await.unwrap().unwrap(), CommitResult::Applied { .. });
                prop_assert!(applied);
            }

            prop_assert!(!repo.overlapped().await, "a run had two activations at once");
            let mut applied: Vec<Vec<String>> = vec![Vec::new(); runs];
            for command in kernel.snapshot().0 {
                let Command::Signal(signal) = command else {
                    panic!("unexpected command {command:?}");
                };
                let run: usize = signal.signal_name.split('-').next().unwrap().parse().unwrap();
                applied[run].push(signal.signal_name);
            }
            prop_assert_eq!(applied, accepted);
            prop_assert_eq!(lane.reserved_claims(), 0);
            Ok(())
        })?;
    }
}

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn enqueue(
    lane: &LaneHandle,
    run_key: RunKey,
    label: &str,
) -> oneshot::Receiver<Result<CommitResult>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    lane.try_enqueue(LaneMessage::new(
        0,
        run_key,
        sample_command(label),
        reply_tx,
    ))
    .unwrap();
    reply_rx
}

/// Wait until `count` of `held` have fired, in whatever order.
async fn wait_for_held(held: Vec<oneshot::Receiver<()>>, count: usize) {
    let (tx, mut rx) = mpsc::channel(held.len().max(1));
    for hold in held {
        let tx = tx.clone();
        tokio::spawn(async move {
            if hold.await.is_ok() {
                let _ = tx.send(()).await;
            }
        });
    }
    drop(tx);
    for _ in 0..count {
        rx.recv().await.expect("a parked commit reported held");
    }
}

/// A store seeded with one started run per key not marked absent.
async fn seeded_store(states: &[WorkflowState], absent: &[bool]) -> tokeira_storage::InMemoryStore {
    let repo = tokeira_storage::InMemoryStore::default();
    for (state, absent) in states.iter().zip(absent) {
        if !absent {
            crate::projection_accumulator_tests::commit_state(
                &repo,
                state.clone(),
                DurableTransitionSeq::ZERO,
            )
            .await;
        }
    }
    repo
}

/// The signals a run's history holds, by event id and name.
async fn signal_history(
    repo: &tokeira_storage::InMemoryStore,
    run_key: RunKey,
) -> Vec<(i64, String)> {
    repo.read_history(run_key, 0, 10_000)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|event| match event.kind {
            HistoryEventKind::WorkflowExecutionSignaled { signal_name, .. } => {
                Some((event.event_id, signal_name))
            }
            _ => None,
        })
        .collect()
}

fn describe(reply: Result<CommitResult>) -> String {
    match reply {
        Ok(CommitResult::Applied { new_state }) => format!(
            "applied seq={:?} last_event_id={}",
            new_state.transition_seq, new_state.last_event_id
        ),
        Ok(other) => format!("{other:?}"),
        Err(error) => format!("error: {error:#}"),
    }
}

#[derive(Clone, Debug)]
enum HandoffStep {
    Submit(usize),
    Relinquish,
    Reacquire,
}

fn handoff_steps(runs: usize) -> impl Strategy<Value = Vec<HandoffStep>> {
    prop::collection::vec(
        prop_oneof![
            4 => (0..runs).prop_map(HandoffStep::Submit),
            1 => Just(HandoffStep::Relinquish),
            1 => Just(HandoffStep::Reacquire),
        ],
        1..40,
    )
}

/// Run `order`'s commands, one per entry naming its run, through a lane of
/// `slots` slots over a fresh store, and return every reply and each run's
/// signal history.
async fn drive_lane(
    slots: usize,
    drain: u32,
    states: &[WorkflowState],
    absent: &[bool],
    order: &[usize],
) -> (Vec<String>, Vec<Vec<(i64, String)>>) {
    let repo = seeded_store(states, absent).await;
    let lane = spawn_slotted_lane(
        0,
        MockKernel::new(SmallVec::new()),
        repo.clone(),
        test_shard_owner(),
        LaneConfig {
            max_drain_per_activation: drain,
            ..LaneConfig::default()
        },
        Arc::new(Semaphore::new(slots)),
    );
    let mut sent = vec![0usize; states.len()];
    let mut replies = Vec::new();
    for run in order {
        let label = run_label(*run, sent[*run]);
        sent[*run] += 1;
        replies.push(enqueue(&lane, states[*run].run_key, &label));
    }
    let mut described = Vec::new();
    for reply in replies {
        described.push(describe(reply.await.unwrap()));
    }
    let mut histories = Vec::new();
    for state in states {
        histories.push(signal_history(&repo, state.run_key).await);
    }
    (described, histories)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: lane-run-concurrency, Property 2: A slow run doesn't delay another run on the same lane
    // With a second slot free, every command for other runs on the lane
    // completes while one run's commit is held.
    #[test]
    fn a_slow_run_does_not_delay_another_run_on_the_same_lane(
        slots in 2usize..5,
        others in prop::collection::vec(0usize..3, 1..12),
    ) {
        multi_thread_runtime().block_on(async {
            let keys: Vec<RunKey> = (0..4).map(|_| RunKey::new()).collect();
            let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
            let (held, release) = repo.hold_run_commit(keys[0]).await;
            let lane = spawn_slotted_lane(
                0,
                MockKernel::new(SmallVec::new()),
                repo.clone(),
                test_shard_owner(),
                LaneConfig::default(),
                Arc::new(Semaphore::new(slots)),
            );
            let mut slow = enqueue(&lane, keys[0], "slow");
            held.await.unwrap();

            let replies: Vec<_> = others
                .iter()
                .enumerate()
                .map(|(index, other)| enqueue(&lane, keys[1 + other], &format!("other-{index}")))
                .collect();
            for reply in replies {
                let applied = matches!(reply.await.unwrap(), Ok(CommitResult::Applied { .. }));
                prop_assert!(applied);
            }
            prop_assert!(
                matches!(slow.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
                "the slow run finished before its commit was released"
            );
            let _ = release.send(());
            let applied = matches!(slow.await.unwrap(), Ok(CommitResult::Applied { .. }));
            prop_assert!(applied);
            Ok(())
        })?;
    }

    // Feature: lane-run-concurrency, Property 3: The bound on activations is never exceeded
    // Across any number of lanes sharing `N` slots, activations in flight
    // never pass `N` and reach `min(N, ready runs)` when the work is there.
    #[test]
    fn the_bound_on_activations_is_never_exceeded(
        lanes in 1usize..4,
        slots in 1usize..5,
        homes in prop::collection::vec(0usize..4, 1..10),
    ) {
        multi_thread_runtime().block_on(async {
            let keys: Vec<RunKey> = homes.iter().map(|_| RunKey::new()).collect();
            let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
            let node_slots = Arc::new(Semaphore::new(slots));
            let owner = test_shard_owner();
            let handles: Vec<LaneHandle> = (0..lanes)
                .map(|lane_id| {
                    spawn_slotted_lane(
                        lane_id,
                        MockKernel::new(SmallVec::new()),
                        repo.clone(),
                        owner.clone(),
                        LaneConfig::default(),
                        node_slots.clone(),
                    )
                })
                .collect();
            let mut held = Vec::new();
            let mut releases = Vec::new();
            for key in &keys {
                let (hold, release) = repo.hold_run_commit(*key).await;
                held.push(hold);
                releases.push(release);
            }
            let replies: Vec<_> = keys
                .iter()
                .zip(&homes)
                .enumerate()
                .map(|(index, (key, home))| {
                    enqueue(&handles[home % lanes], *key, &format!("run-{index}"))
                })
                .collect();

            let expected = slots.min(keys.len());
            wait_for_held(held, expected).await;
            // Every run's commit is gated, so the activations holding slots
            // are exactly the parked ones.
            prop_assert_eq!(node_slots.available_permits(), slots - expected);
            prop_assert_eq!(repo.commit_high_water().await, expected);
            let active: usize = handles.iter().map(LaneHandle::active_activations).sum();
            prop_assert_eq!(active, expected);

            for release in releases {
                let _ = release.send(());
            }
            for reply in replies {
                let applied = matches!(reply.await.unwrap(), Ok(CommitResult::Applied { .. }));
                prop_assert!(applied);
            }
            prop_assert!(repo.commit_high_water().await <= slots);
            Ok(())
        })?;
    }

    // Feature: lane-run-concurrency, Property 4: No command is lost or duplicated across a shard handoff
    // However submissions interleave with relinquishing and re-acquiring the
    // shard, every accepted command gets exactly one reply, and each run's
    // history holds exactly the commands answered `Applied`, each once.
    #[test]
    fn no_command_is_lost_or_duplicated_across_a_shard_handoff(
        (runs, plan) in (1usize..4).prop_flat_map(|runs| (Just(runs), handoff_steps(runs))),
        slots in 1usize..4,
    ) {
        multi_thread_runtime().block_on(async {
            let states: Vec<WorkflowState> =
                (0..runs).map(|_| sample_state(RunKey::new())).collect();
            let repo = seeded_store(&states, &vec![false; runs]).await;
            let owner = test_shard_owner();
            let lane = spawn_slotted_lane(
                0,
                MockKernel::new(SmallVec::new()),
                repo.clone(),
                owner.clone(),
                LaneConfig::default(),
                Arc::new(Semaphore::new(slots)),
            );
            let mut epoch = 0;
            let mut owned = true;
            let mut accepted = Vec::new();
            let mut sent = vec![0usize; runs];
            for step in &plan {
                match *step {
                    HandoffStep::Submit(run) => {
                        let label = run_label(run, sent[run]);
                        sent[run] += 1;
                        let reply = enqueue(&lane, states[run].run_key, &label);
                        accepted.push((run, label, reply));
                    }
                    HandoffStep::Relinquish if owned => {
                        let mut owner = owner.write().unwrap();
                        owner.mark_draining(ShardId(0));
                        owner.remove(ShardId(0));
                        owned = false;
                    }
                    HandoffStep::Reacquire if !owned => {
                        epoch += 1;
                        let mut owner = owner.write().unwrap();
                        let _ = owner.record_acquired(ShardId(0), ShardEpoch(epoch));
                        owner.mark_active(ShardId(0));
                        owned = true;
                    }
                    HandoffStep::Relinquish | HandoffStep::Reacquire => {}
                }
            }

            let mut applied: Vec<Vec<String>> = vec![Vec::new(); runs];
            for (run, label, reply) in accepted {
                // A dropped sender would mean an accepted command lost its
                // reply; the oneshot makes a second reply impossible.
                let reply = reply.await.expect("every accepted command gets a reply");
                if matches!(reply, Ok(CommitResult::Applied { .. })) {
                    applied[run].push(label);
                }
            }
            for (run, state) in states.iter().enumerate() {
                let history: Vec<String> = signal_history(&repo, state.run_key)
                    .await
                    .into_iter()
                    .map(|(_, name)| name)
                    .collect();
                prop_assert_eq!(&history, &applied[run]);
            }
            prop_assert_eq!(lane.reserved_claims(), 0);
            Ok(())
        })?;
    }

    // Feature: lane-run-concurrency, Property 9: Below the bounds, replies match the serial lane
    // The same per-run command sequences get the same replies and leave the
    // same histories through a one-slot lane and a lane of several slots.
    #[test]
    fn below_the_bounds_replies_match_the_serial_lane(
        (runs, order) in (1usize..5)
            .prop_flat_map(|runs| (Just(runs), prop::collection::vec(0..runs, 1..32))),
        absent in prop::collection::vec(prop::bool::weighted(0.2), 5),
        slots in 2usize..5,
        drain in 1u32..4,
    ) {
        multi_thread_runtime().block_on(async {
            let states: Vec<WorkflowState> =
                (0..runs).map(|_| sample_state(RunKey::new())).collect();
            let absent = &absent[..runs];
            let serial = drive_lane(1, drain, &states, absent, &order).await;
            let concurrent = drive_lane(slots, drain, &states, absent, &order).await;
            prop_assert_eq!(serial, concurrent);
            Ok(())
        })?;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_every_handle_still_answers_every_queued_command() {
    let keys: Vec<RunKey> = (0..2).map(|_| RunKey::new()).collect();
    let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
    let (held, release) = repo.hold_run_commit(keys[0]).await;
    let lane = spawn_slotted_lane(
        0,
        MockKernel::new(SmallVec::new()),
        repo.clone(),
        test_shard_owner(),
        LaneConfig::default(),
        Arc::new(Semaphore::new(2)),
    );
    let mut replies = Vec::new();
    replies.push(enqueue(&lane, keys[0], "a-0"));
    held.await.unwrap();
    for index in 1..4 {
        replies.push(enqueue(&lane, keys[0], &format!("a-{index}")));
        replies.push(enqueue(&lane, keys[1], &format!("b-{index}")));
    }
    let shared = lane.shared.clone();
    drop(lane);
    let _ = release.send(());

    for reply in replies {
        assert!(matches!(
            reply.await.expect("every queued command gets a reply"),
            Ok(CommitResult::Applied { .. })
        ));
    }
    // The dispatcher ends once its queues are empty and its activations have
    // ended; nothing else can mark the lane closed.
    while !shared.lock().closed {
        tokio::task::yield_now().await;
    }
    assert_eq!(shared.lock().queues.queued(), 0);
    assert_eq!(shared.lock().queues.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_activation_that_ends_without_finishing_leaves_its_run_runnable() {
    let keys: Vec<RunKey> = (0..2).map(|_| RunKey::new()).collect();
    let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
    let lane = spawn_slotted_lane(
        0,
        MockKernel::new(SmallVec::new()),
        repo.clone(),
        test_shard_owner(),
        LaneConfig::default(),
        Arc::new(Semaphore::new(2)),
    );
    let lost = enqueue(&lane, keys[0], PANIC_SIGNAL);
    let later = enqueue(&lane, keys[0], "after");
    let other = enqueue(&lane, keys[1], "other");

    assert!(
        lost.await.is_err(),
        "the abandoned command's reply sender is dropped"
    );
    assert!(matches!(
        later.await.unwrap(),
        Ok(CommitResult::Applied { .. })
    ));
    assert!(matches!(
        other.await.unwrap(),
        Ok(CommitResult::Applied { .. })
    ));
    assert_eq!(lane.active_activations(), 0);
    assert_eq!(lane.reserved_claims(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_heartbeat_reports_in_flight_activations_as_active_actors() {
    let keys: Vec<RunKey> = (0..2).map(|_| RunKey::new()).collect();
    let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
    let (held_a, release_a) = repo.hold_run_commit(keys[0]).await;
    let (held_b, release_b) = repo.hold_run_commit(keys[1]).await;
    let owner = test_shard_owner();
    let lane = spawn_slotted_lane(
        0,
        MockKernel::new(SmallVec::new()),
        repo.clone(),
        owner.clone(),
        LaneConfig::default(),
        Arc::new(Semaphore::new(2)),
    );
    let first_a = enqueue(&lane, keys[0], "a-0");
    let first_b = enqueue(&lane, keys[1], "b-0");
    held_a.await.unwrap();
    held_b.await.unwrap();
    let queued = enqueue(&lane, keys[0], "a-1");

    let inputs = crate::membership::HeartbeatInputs::from_runtime_components(
        &owner.read().unwrap(),
        crate::RuntimeDrainState::Active,
        std::slice::from_ref(&lane),
        0,
        0.0,
        0,
    );
    assert_eq!(inputs.lane_pressures[0].active_actors, 2);
    assert_eq!(inputs.lane_pressures[0].runnable_depth, 1);
    assert_eq!(inputs.active_actor_count, 2);

    let _ = release_a.send(());
    let _ = release_b.send(());
    for reply in [first_a, first_b, queued] {
        assert!(matches!(
            reply.await.unwrap(),
            Ok(CommitResult::Applied { .. })
        ));
    }
}

#[test]
fn the_node_bound_is_the_larger_of_the_lane_count_and_the_commit_ceiling() {
    assert_eq!(node_activation_bound(8, None), 8);
    assert_eq!(node_activation_bound(8, Some(2)), 8);
    assert_eq!(node_activation_bound(8, Some(25)), 25);
    // A standalone lane is a node of one lane.
    assert_eq!(node_activation_bound(1, None), 1);
    assert_eq!(node_activation_bound(1, Some(4)), 4);
    assert_eq!(node_activation_bound(0, None), 1);
}

// A single-threaded runtime makes the order lanes join the slot queue
// deterministic: each `yield_now` lets a woken dispatcher run until it is
// parked on the semaphore before the test takes its next step. A multi-thread
// runtime gives no signal for "this lane is now waiting for a slot".
#[tokio::test]
async fn a_lane_waiting_for_a_slot_keeps_its_place_while_its_runs_become_ready() {
    let keys: Vec<RunKey> = (0..3).map(|_| RunKey::new()).collect();
    let repo = MockRepo::new(LoadedRun::Existing(sample_state(keys[0])), Vec::new());
    let (first_held, first_release) = repo.hold_run_commit(keys[0]).await;
    let (second_held, second_release) = repo.hold_run_commit(keys[1]).await;
    let node_slots = Arc::new(Semaphore::new(1));
    let only_slot = node_slots.clone().try_acquire_owned().unwrap();
    let owner = test_shard_owner();
    let lanes: Vec<LaneHandle> = (0..2)
        .map(|lane_id| {
            spawn_slotted_lane(
                lane_id,
                MockKernel::new(SmallVec::new()),
                repo.clone(),
                owner.clone(),
                LaneConfig::default(),
                node_slots.clone(),
            )
        })
        .collect();

    let mut replies = vec![enqueue(&lanes[0], keys[0], "a-0")];
    tokio::task::yield_now().await;
    replies.push(enqueue(&lanes[1], keys[1], "b-0"));
    tokio::task::yield_now().await;
    // Another run on the first lane becomes ready and wakes its dispatcher
    // while it is still queued ahead of the second lane.
    replies.push(enqueue(&lanes[0], keys[2], "a-1"));
    tokio::task::yield_now().await;

    drop(only_slot);
    let winner = tokio::select! {
        biased;
        _ = first_held => 0,
        _ = second_held => 1,
    };
    assert_eq!(
        winner, 0,
        "the lane that queued first for a slot gets it first"
    );

    let _ = first_release.send(());
    let _ = second_release.send(());
    for reply in replies {
        assert!(matches!(
            reply.await.expect("every queued command gets a reply"),
            Ok(CommitResult::Applied { .. })
        ));
    }
}
