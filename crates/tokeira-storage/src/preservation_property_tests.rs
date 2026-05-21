//! Preservation property tests for commit fencing correctness.
//!
//! These tests capture the BASELINE behavior that must be preserved after the
//! fencing fix is applied. They are expected to PASS on unfixed code.
//!
//! **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**
//!
//! Properties tested:
//! - Zero-epoch bypass: commits with `ShardEpoch::ZERO` skip fencing entirely
//! - Matching-epoch commit: when caller epoch matches durable epoch, result is Applied
//! - OCC conflict on stale transition_seq: result is Conflict regardless of epoch
//! - Lane OCC retry loop: retries up to max_occ_retries before surfacing error

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{LoadedRun, PendingWorkflowTask, Transition, WorkflowState};
    use tokeira_types::{
        ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
        ShardEpoch, ShardId, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };

    use crate::{
        api::{CommitResult, LeaseOutcome, LeaseRepository, RunRepository},
        memory::InMemoryStore,
    };

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            sticky: None,
            pause_info: None,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: None,
            original_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
        }
    }

    fn start_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        }
    }

    /// Helper: create a follow-up transition from an existing state.
    fn followup_transition(state: &WorkflowState) -> Transition {
        let mut next_state = state.clone();
        next_state.transition_seq = state.transition_seq.next();
        Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        }
    }

    // ─── Property: Zero-Epoch Bypass ────────────────────────────────────────────
    //
    // For all commits with `ShardEpoch::ZERO`, the result depends only on
    // `transition_seq` OCC — fencing is skipped entirely. This preserves the
    // single-node compose and test experience.
    //
    // **Validates: Requirements 3.1**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn property_zero_epoch_bypass_skips_fencing(
            shard_id_val in 0u32..32,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(32);
                let bundle = ShardId(shard_id_val);

                // Establish a non-zero durable epoch
                let outcome = store
                    .try_acquire_bundle(
                        bundle,
                        "owner".to_owned(),
                        "127.0.0.1:7233".to_owned(),
                    )
                    .await
                    .unwrap();
                let LeaseOutcome::Acquired { epoch } = outcome else {
                    panic!("expected acquire");
                };
                prop_assert!(epoch.0 > 0, "epoch should be non-zero after acquire");

                // Create a run via commit_transition with ZERO (no fencing)
                let run_key = RunKey::new();
                let t = start_transition(run_key);
                let result = store
                    .commit_transition(run_key, t, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(
                    is_applied,
                    "commit_transition with ZERO should apply regardless of durable epoch"
                );

                // Now use commit_transition_for_bundle with ZERO — should also skip fencing
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state) = loaded else {
                    panic!("expected existing run");
                };
                let transition = followup_transition(&state);
                let ehb = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    32,
                );
                let result = store
                    .commit_transition_for_bundle(run_key, ehb, transition, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(
                    is_applied,
                    "commit_transition_for_bundle with ZERO should apply (fencing skipped), got {:?}",
                    result
                );

                Ok(())
            }).unwrap();
        }
    }

    // ─── Property: Matching-Epoch Commit ────────────────────────────────────────
    //
    // For all commits where caller epoch matches durable epoch, result is
    // `CommitResult::Applied` (assuming `transition_seq` is current).
    //
    // **Validates: Requirements 3.2**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn property_matching_epoch_produces_applied(
            shard_id_val in 0u32..32,
        ) {
            // shard_id_val is used only to vary the random seed; the actual
            // bundle is derived from the workflow state so the lease matches.
            let _ = shard_id_val;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(32);

                // Create a run first so we know its execution-home bundle
                let run_key = RunKey::new();
                let t = start_transition(run_key);
                let state = &t.next_state;

                // Derive the execution-home bundle from the workflow state
                let bundle = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    32,
                );

                // Acquire the correct bundle to establish a durable epoch
                let outcome = store
                    .try_acquire_bundle(
                        bundle,
                        "owner".to_owned(),
                        "127.0.0.1:7233".to_owned(),
                    )
                    .await
                    .unwrap();
                let LeaseOutcome::Acquired { epoch } = outcome else {
                    panic!("expected acquire");
                };

                // Commit the initial transition
                let result = store
                    .commit_transition(run_key, t, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(is_applied, "initial commit should apply");

                // Commit via commit_transition_for_bundle with matching epoch
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state) = loaded else {
                    panic!("expected existing run");
                };
                let transition = followup_transition(&state);
                let result = store
                    .commit_transition_for_bundle(run_key, bundle, transition, epoch)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(
                    is_applied,
                    "matching epoch should produce Applied, got {:?}",
                    result
                );

                Ok(())
            }).unwrap();
        }
    }

    // ─── Property: OCC Conflict on Stale transition_seq ─────────────────────────
    //
    // For all OCC conflicts (stale `transition_seq`), result is
    // `CommitResult::Conflict` regardless of epoch.
    //
    // **Validates: Requirements 3.3**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn property_stale_transition_seq_produces_conflict_regardless_of_epoch(
            shard_id_val in 0u32..32,
            use_zero_epoch in proptest::bool::ANY,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(32);
                let bundle = ShardId(shard_id_val);

                // Acquire the bundle
                let outcome = store
                    .try_acquire_bundle(
                        bundle,
                        "owner".to_owned(),
                        "127.0.0.1:7233".to_owned(),
                    )
                    .await
                    .unwrap();
                let LeaseOutcome::Acquired { epoch } = outcome else {
                    panic!("expected acquire");
                };

                // Create a run
                let run_key = RunKey::new();
                let t = start_transition(run_key);
                let result = store
                    .commit_transition(run_key, t, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(is_applied, "initial commit should apply");

                // Load state — transition_seq is now 1
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state) = loaded else {
                    panic!("expected existing run");
                };

                // Create a transition with a STALE expected_seq (0 instead of current 1)
                let mut next_state = state.clone();
                next_state.transition_seq = TransitionSeq(2);
                let stale_transition = Transition {
                    expected_seq: TransitionSeq::ZERO, // stale — current is 1
                    next_state,
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };

                // Choose epoch based on the property parameter
                let commit_epoch = if use_zero_epoch {
                    ShardEpoch::ZERO
                } else {
                    epoch
                };

                let result = store
                    .commit_transition_for_bundle(run_key, bundle, stale_transition, commit_epoch)
                    .await
                    .unwrap();
                let is_conflict = matches!(result, CommitResult::Conflict { .. });
                prop_assert!(
                    is_conflict,
                    "stale transition_seq should produce Conflict regardless of epoch (epoch={:?}, use_zero={}), got {:?}",
                    commit_epoch,
                    use_zero_epoch,
                    result
                );

                Ok(())
            }).unwrap();
        }
    }

    // ─── Property: In-Memory Store Fencing Parity ───────────────────────────────
    //
    // The in-memory store produces the same fencing behavior as expected for
    // non-zero epochs: rejects stale epochs, skips for zero epoch.
    //
    // **Validates: Requirements 3.4**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn property_in_memory_store_fencing_parity(
            _seed in 0u32..1000,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(32);

                // Create a run first so we know its execution-home bundle
                let run_key = RunKey::new();
                let t = start_transition(run_key);
                let state = &t.next_state;

                // Derive the execution-home bundle for this workflow
                let bundle = tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    32,
                );

                // Acquire the correct bundle at epoch 1
                let outcome = store
                    .try_acquire_bundle(
                        bundle,
                        "owner-a".to_owned(),
                        "127.0.0.1:7233".to_owned(),
                    )
                    .await
                    .unwrap();
                let LeaseOutcome::Acquired { epoch: epoch_a } = outcome else {
                    panic!("expected acquire");
                };

                // Commit the initial transition
                store
                    .commit_transition(run_key, t, ShardEpoch::ZERO)
                    .await
                    .unwrap();

                // Advance epoch: owner-b takes over
                store
                    .relinquish_bundle(bundle, "owner-a".to_owned(), epoch_a)
                    .await
                    .unwrap();
                let outcome = store
                    .try_acquire_bundle(
                        bundle,
                        "owner-b".to_owned(),
                        "127.0.0.1:7234".to_owned(),
                    )
                    .await
                    .unwrap();
                let LeaseOutcome::Acquired { epoch: epoch_b } = outcome else {
                    panic!("expected acquire");
                };
                prop_assert!(epoch_b.0 > epoch_a.0);

                // Load state for follow-up transition
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state) = loaded else {
                    panic!("expected existing run");
                };

                // Test 1: commit_transition with non-zero STALE epoch → Conflict
                let transition1 = followup_transition(&state);
                let result = store
                    .commit_transition(run_key, transition1, epoch_a)
                    .await
                    .unwrap();
                let is_conflict = matches!(result, CommitResult::Conflict { .. });
                prop_assert!(
                    is_conflict,
                    "stale non-zero epoch should produce Conflict in commit_transition, got {:?}",
                    result
                );

                // Test 2: commit_transition with non-zero MATCHING epoch → Applied
                let transition2 = followup_transition(&state);
                let result = store
                    .commit_transition(run_key, transition2, epoch_b)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(
                    is_applied,
                    "matching non-zero epoch should produce Applied in commit_transition, got {:?}",
                    result
                );

                // Test 3: commit_transition_for_bundle with stale epoch → Conflict
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state2) = loaded else {
                    panic!("expected existing run");
                };
                let transition3 = followup_transition(&state2);
                let result = store
                    .commit_transition_for_bundle(run_key, bundle, transition3, epoch_a)
                    .await
                    .unwrap();
                let is_conflict = matches!(result, CommitResult::Conflict { .. });
                prop_assert!(
                    is_conflict,
                    "stale epoch in commit_transition_for_bundle should produce Conflict, got {:?}",
                    result
                );

                // Test 4: commit_transition_for_bundle with matching epoch → Applied
                let transition4 = followup_transition(&state2);
                let result = store
                    .commit_transition_for_bundle(run_key, bundle, transition4, epoch_b)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(
                    is_applied,
                    "matching epoch in commit_transition_for_bundle should produce Applied, got {:?}",
                    result
                );

                Ok(())
            }).unwrap();
        }
    }

    // ─── Property: Lane OCC Retry Loop ──────────────────────────────────────────
    //
    // The lane OCC retry loop retries up to `max_occ_retries` before surfacing
    // error. We test this by injecting conflicts via InMemoryStore's
    // `inject_conflict` mechanism.
    //
    // **Validates: Requirements 3.5**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        #[test]
        fn property_occ_retry_loop_retries_up_to_max(
            max_retries in 1u32..6,
            injected_conflicts in 1u32..8,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(1);

                // Create a run
                let run_key = RunKey::new();
                let t = start_transition(run_key);
                let result = store
                    .commit_transition(run_key, t, ShardEpoch::ZERO)
                    .await
                    .unwrap();
                let is_applied = matches!(result, CommitResult::Applied { .. });
                prop_assert!(is_applied, "initial commit should apply");

                // Inject conflicts — each call to commit_transition will
                // return Conflict until the counter is exhausted
                store.inject_conflict(run_key, injected_conflicts as usize).await;

                // Now attempt commits in a retry loop (simulating the lane)
                let mut attempts = 0u32;
                let final_result: Result<(), String>;

                loop {
                    let loaded = store.load_run(run_key).await.unwrap();
                    let LoadedRun::Existing(state) = loaded else {
                        panic!("expected existing run");
                    };
                    let transition = followup_transition(&state);
                    let result = store
                        .commit_transition(run_key, transition, ShardEpoch::ZERO)
                        .await
                        .unwrap();

                    match result {
                        CommitResult::Applied { .. } => {
                            final_result = Ok(());
                            break;
                        }
                        CommitResult::Conflict { reason } => {
                            if attempts >= max_retries {
                                final_result = Err(format!(
                                    "OCC retry exhausted after {} conflicts: {}",
                                    attempts + 1,
                                    reason
                                ));
                                break;
                            }
                            attempts += 1;
                        }
                        CommitResult::Duplicate => {
                            final_result = Ok(());
                            break;
                        }
                    }
                }

                // Verify the retry behavior matches expectations
                if injected_conflicts <= max_retries {
                    // Should eventually succeed — conflicts exhausted before retry limit
                    prop_assert!(
                        final_result.is_ok(),
                        "should succeed when injected_conflicts ({}) <= max_retries ({}), got {:?}",
                        injected_conflicts,
                        max_retries,
                        final_result
                    );
                } else {
                    // Should fail — retry limit hit before conflicts exhausted
                    prop_assert!(
                        final_result.is_err(),
                        "should fail when injected_conflicts ({}) > max_retries ({}), got {:?}",
                        injected_conflicts,
                        max_retries,
                        final_result
                    );
                }

                Ok(())
            }).unwrap();
        }
    }
}
