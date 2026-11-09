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
