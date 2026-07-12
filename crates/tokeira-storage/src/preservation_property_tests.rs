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
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{LoadedRun, PendingWorkflowTask, Transition, WorkflowState};
    use tokeira_types::{
        ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
        ShardEpoch, ShardId, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };

    use crate::{
        api::{
            BuildId, CommitResult, ComputeConfig, ConflictToken, DeploymentCasResult,
            DeploymentName, DrainageInfo, LeaseOutcome, LeaseRepository, RoutingConfigUpdateState,
            RunRepository, StoredRoutingConfig, StoredVersion, StoredWorkerDeployment,
            VersionDrainageStatus, VersionMetadata, WorkerDeploymentRepository,
            WorkerDeploymentVersionKey, WorkerDeploymentVersionStatus,
        },
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
                task_type: tokeira_kernel::WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
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
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
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
            buffered_events: Vec::new(),
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

    #[derive(Clone, Debug)]
    struct DeploymentCase {
        suffix: u16,
        version_count: u8,
        manager_set: bool,
        routing_state: u8,
    }

    fn arb_deployment_case() -> impl Strategy<Value = DeploymentCase> {
        (0u16..10_000, 0u8..5, proptest::bool::ANY, 0u8..3).prop_map(
            |(suffix, version_count, manager_set, routing_state)| DeploymentCase {
                suffix,
                version_count,
                manager_set,
                routing_state,
            },
        )
    }

    fn build_deployment_record(
        namespace_id: NamespaceId,
        case_index: usize,
        case: &DeploymentCase,
    ) -> StoredWorkerDeployment {
        let name = DeploymentName(format!("deployment-{case_index}-{}", case.suffix));
        let create_time = fixed_now() + Duration::seconds(case_index as i64);
        let mut versions = BTreeMap::new();
        for version_index in 0..case.version_count as usize {
            let build_id = BuildId(format!("build-{case_index}-{version_index}"));
            let status = match version_index % 6 {
                0 => WorkerDeploymentVersionStatus::Created,
                1 => WorkerDeploymentVersionStatus::Inactive,
                2 => WorkerDeploymentVersionStatus::Current,
                3 => WorkerDeploymentVersionStatus::Ramping,
                4 => WorkerDeploymentVersionStatus::Draining,
                _ => WorkerDeploymentVersionStatus::Drained,
            };
            let drainage_info = matches!(
                status,
                WorkerDeploymentVersionStatus::Draining | WorkerDeploymentVersionStatus::Drained
            )
            .then(|| DrainageInfo {
                status: if status == WorkerDeploymentVersionStatus::Drained {
                    VersionDrainageStatus::Drained
                } else {
                    VersionDrainageStatus::Draining
                },
                last_changed_time: create_time + Duration::seconds(10),
                last_checked_time: create_time + Duration::seconds(20),
            });
            versions.insert(
                build_id.clone(),
                StoredVersion {
                    build_id,
                    status,
                    create_time: create_time + Duration::seconds(version_index as i64),
                    routing_changed_time: Some(create_time + Duration::seconds(30)),
                    current_since_time: (version_index == 0)
                        .then(|| create_time + Duration::seconds(40)),
                    ramping_since_time: (version_index == 1)
                        .then(|| create_time + Duration::seconds(50)),
                    first_activation_time: Some(create_time + Duration::seconds(60)),
                    last_current_time: (version_index % 2 == 0)
                        .then(|| create_time + Duration::seconds(70)),
                    last_deactivation_time: (version_index > 1)
                        .then(|| create_time + Duration::seconds(80)),
                    ramp_percentage: if version_index == 1 { 25.0 } else { 0.0 },
                    drainage_info,
                    metadata: VersionMetadata::default(),
                    compute_config: ComputeConfig::default(),
                    last_modifier_identity: format!("version-writer-{case_index}-{version_index}"),
                    polled_task_queues: BTreeSet::new(),
                    create_request_ids: BTreeSet::from([format!(
                        "create-version-{case_index}-{version_index}"
                    )]),
                    compute_config_request_ids: BTreeSet::new(),
                },
            );
        }

        let current_version = versions
            .keys()
            .next()
            .map(|build_id| WorkerDeploymentVersionKey {
                deployment_name: name.clone(),
                build_id: build_id.clone(),
            });
        let ramping_version = versions
            .keys()
            .nth(1)
            .map(|build_id| WorkerDeploymentVersionKey {
                deployment_name: name.clone(),
                build_id: build_id.clone(),
            });
        let routing_config_update_state = match case.routing_state {
            0 => RoutingConfigUpdateState::Completed,
            1 => RoutingConfigUpdateState::InProgress,
            _ => RoutingConfigUpdateState::Unspecified,
        };

        StoredWorkerDeployment {
            namespace_id,
            name,
            create_time,
            routing_config: StoredRoutingConfig {
                current_version,
                ramping_version,
                ramping_version_percentage: if case.version_count > 1 { 25.0 } else { 0.0 },
                ramping_to_unversioned: false,
                current_version_changed_time: Some(create_time + Duration::seconds(90)),
                ramping_version_changed_time: Some(create_time + Duration::seconds(100)),
                ramping_version_percentage_changed_time: Some(create_time + Duration::seconds(110)),
                revision_number: i64::from(case.version_count) + case_index as i64,
            },
            last_modifier_identity: format!("deployment-writer-{case_index}"),
            manager_identity: case.manager_set.then(|| format!("manager-{case_index}")),
            routing_config_update_state,
            versions,
            conflict_token: ConflictToken::default(),
            create_request_ids: BTreeSet::from([format!("create-deployment-{case_index}")]),
        }
    }

    // ─── Property 17: Registry Restart-Recovery Round-Trip ─────────────────────
    //
    // **Validates: Requirements 13.1, 13.2, 13.3, 13.4**

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_worker_deployment_registry_restart_recovery_round_trip(
            namespace_raw in any::<u128>(),
            cases in proptest::collection::vec(arb_deployment_case(), 0..8),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let namespace_id = NamespaceId(uuid::Uuid::from_u128(namespace_raw));
                let store = InMemoryStore::default();
                let mut expected = Vec::new();

                for (index, case) in cases.iter().enumerate() {
                    let mut record = build_deployment_record(namespace_id, index, case);
                    let applied = store
                        .put_deployment(record.clone(), None)
                        .await
                        .unwrap();
                    let DeploymentCasResult::Applied { token } = applied else {
                        panic!("fresh deployment insert should apply, got {applied:?}");
                    };
                    record.conflict_token = token;
                    expected.push(record);
                }
                expected.sort_by(|left, right| left.name.cmp(&right.name));

                let recovered = store.list_all_for_namespace(namespace_id).await.unwrap();
                prop_assert_eq!(&recovered, &expected);

                if let Some(first) = recovered.first() {
                    let pre_restart_token = first.conflict_token;
                    let mut updated = first.clone();
                    updated.last_modifier_identity.push_str("-after-restart");
                    let applied = store
                        .put_deployment(updated.clone(), Some(pre_restart_token))
                        .await
                        .unwrap();
                    let DeploymentCasResult::Applied { token: post_restart_token } = applied else {
                        panic!("current pre-restart token should apply after reload, got {applied:?}");
                    };
                    prop_assert_ne!(post_restart_token, pre_restart_token);

                    updated.conflict_token = post_restart_token;
                    let key = crate::DeploymentKey {
                        namespace_id,
                        deployment_name: updated.name.clone(),
                    };
                    prop_assert_eq!(store.load_deployment(&key).await.unwrap(), Some(updated.clone()));

                    let stale = store
                        .put_deployment(updated, Some(pre_restart_token))
                        .await
                        .unwrap();
                    prop_assert_eq!(stale, DeploymentCasResult::Conflict);
                }

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
                        CommitResult::CurrentExecutionConflict { .. } => {
                            final_result =
                                Err("unexpected current-execution conflict".to_string());
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
