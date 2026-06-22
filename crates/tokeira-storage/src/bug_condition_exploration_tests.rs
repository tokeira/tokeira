//! Bug condition exploration tests for commit fencing correctness.
//!
//! These tests validate that the fencing fix works correctly:
//! - Mode 1: `commit_transition_for_bundle` with a stale epoch returns Conflict
//! - The property test verifies this holds for any shard
//!
//! Modes 2/3 (unfenced activity start/retry) were removed because the fix is
//! structural: the runtime callers now route through `commit_transition_for_bundle`
//! instead of calling `commit_transition` directly with `ShardEpoch::ZERO`.
//! The preservation tests confirm that ZERO-epoch bypass still works for
//! single-node/test paths.
//!
//! **Validates: Requirements 2.1, 2.5**

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
            cancel_requested: false,
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
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
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

    /// Mode 1: Verify that `commit_transition_for_bundle` with a stale epoch
    /// returns `CommitResult::Conflict`.
    ///
    /// This validates the fix: the epoch check and mutation write now execute
    /// atomically within the same lock scope. A stale caller is rejected.
    ///
    /// **Validates: Requirements 2.1, 2.5**
    #[tokio::test]
    async fn mode1_stale_epoch_commit_for_bundle_returns_conflict() {
        let store = InMemoryStore::with_shard_count(32);
        let bundle = ShardId(7);

        // Runtime A acquires the bundle at epoch 1
        let outcome = store
            .try_acquire_bundle(bundle, "runtime-a".to_owned(), "127.0.0.1:7233".to_owned())
            .await
            .unwrap();
        let LeaseOutcome::Acquired {
            epoch: runtime_a_epoch,
        } = outcome
        else {
            panic!("expected initial acquire");
        };

        // Create a run
        let run_key = RunKey::new();
        let t = start_transition(run_key);
        let result = store
            .commit_transition(run_key, t, ShardEpoch::ZERO)
            .await
            .unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));

        // Advance epoch: Runtime B takes over
        store
            .relinquish_bundle(bundle, "runtime-a".to_owned(), runtime_a_epoch)
            .await
            .unwrap();
        let outcome = store
            .try_acquire_bundle(bundle, "runtime-b".to_owned(), "127.0.0.1:7234".to_owned())
            .await
            .unwrap();
        let LeaseOutcome::Acquired {
            epoch: runtime_b_epoch,
        } = outcome
        else {
            panic!("expected acquire by runtime-b");
        };
        assert!(runtime_b_epoch.0 > runtime_a_epoch.0);

        // Load state for the transition
        let loaded = store.load_run(run_key).await.unwrap();
        let LoadedRun::Existing(state) = loaded else {
            panic!("expected existing run");
        };
        let mut next_state = state.clone();
        next_state.transition_seq = state.transition_seq.next();
        let transition = Transition {
            expected_seq: state.transition_seq,
            next_state,
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        };

        // Runtime A (stale) calls commit_transition_for_bundle with its old epoch.
        // After the fix, this is rejected atomically because the epoch check
        // and write happen under the same lock.
        let result = store
            .commit_transition_for_bundle(run_key, bundle, transition, runtime_a_epoch)
            .await
            .unwrap();

        assert!(
            matches!(result, CommitResult::Conflict { .. }),
            "commit_transition_for_bundle with stale epoch {:?} should return Conflict \
             (durable epoch is now {:?}), but got {:?}",
            runtime_a_epoch,
            runtime_b_epoch,
            result
        );
    }

    // Property-based test: for any shard, commit_transition_for_bundle with a
    // stale epoch returns Conflict. This validates the fix holds universally.
    //
    // **Validates: Requirements 2.1, 2.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn property_stale_epoch_commit_for_bundle_returns_conflict(
            shard_id_val in 0u32..32,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let store = InMemoryStore::with_shard_count(32);
                let bundle = ShardId(shard_id_val);

                // Acquire the bundle at epoch 1
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

                // Create a run
                let run_key = RunKey::new();
                let t = start_transition(run_key);
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

                // Load and create a follow-up transition
                let loaded = store.load_run(run_key).await.unwrap();
                let LoadedRun::Existing(state) = loaded else {
                    panic!("expected existing run");
                };
                let mut next_state = state.clone();
                next_state.transition_seq = state.transition_seq.next();
                let transition = Transition {
                    expected_seq: state.transition_seq,
                    next_state,
                    history_events: Default::default(),
                    request_dedupe_ops: Default::default(),
                    activity_ops: Default::default(),
                    timer_ops: Default::default(),
                    dispatch_ops: Default::default(),
                    projection_ops: Default::default(),
                };

                // Stale caller uses commit_transition_for_bundle with old epoch.
                // After the fix, this returns Conflict.
                let result = store
                    .commit_transition_for_bundle(run_key, bundle, transition, epoch_a)
                    .await
                    .unwrap();

                prop_assert!(
                    matches!(result, CommitResult::Conflict { .. }),
                    "commit_transition_for_bundle with stale epoch {:?} should return \
                     Conflict (durable epoch is {:?}), but got {:?}",
                    epoch_a,
                    epoch_b,
                    result
                );
                Ok(())
            }).unwrap();
        }
    }
}
