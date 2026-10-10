//! Generated projection contracts and shared inputs for memory and live DSQL.
//! The oracle retains baseline derivation; these helpers only supply transitions
//! and inspect outputs, so production folding cannot redefine the expected list.

use proptest::prelude::*;
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, RequestDedupeOp, VersioningBehavior,
    limits::{RunGrowthLimits, RunLimit, RunLimitExceeded},
    state::WorkerDeploymentVersionRef,
};
use tokeira_types::{LogicalTaskSeq, Payloads, SearchAttrValue, WorkerIdentity};

use super::*;
use crate::{
    api::{
        ProjectionAccumulatorError, ProjectionContext, prepare_workflow_projection,
        seed_workflow_projection_accumulator,
    },
    projection_accumulator_oracle::{
        legacy_codec, workflow_projection_context_with_previous as baseline_image,
    },
};

/// A reported version, raw search-attribute shape, and unspecified-behaviour flag.
pub(crate) type Observation = (Option<String>, u8, bool);

/// Repeated versions, cleared reports, and all baseline accumulator attribute shapes.
pub(crate) fn observations() -> impl Strategy<Value = Vec<Observation>> {
    prop::collection::vec(
        (prop::option::of("[ab]{0,4}"), 0u8..5, any::<bool>()),
        1..16,
    )
}

/// A fresh ready run, independent of predecessor accumulation.
pub(crate) fn fresh_transition(run_key: RunKey) -> Transition {
    super::tests::start_transition(run_key)
}

/// Existing activity input shape for cross-backend size-exclusion checks.
pub(crate) fn activity_fixture() -> tokeira_kernel::ActivityState {
    super::tests::activity_state("input-exclusion")
}

/// Exercise either public commit surface with the same transition and fence.
pub(crate) async fn commit(
    repo: &dyn RunRepository,
    transition: Transition,
    bundle: bool,
    epoch: ShardEpoch,
) -> Result<CommitResult> {
    let key = transition.next_state.run_key;
    if bundle {
        repo.commit_transition_for_bundle(key, ShardId(0), transition, epoch)
            .await
    } else {
        repo.commit_transition(key, transition, epoch).await
    }
}

/// A new transition from the state actually returned by storage.
pub(crate) fn following(state: &WorkflowState) -> Transition {
    let mut transition = fresh_transition(state.run_key);
    transition.expected_seq = state.transition_seq;
    transition.next_state = state.clone();
    transition.next_state.transition_seq = state.transition_seq.next();
    transition
}

/// Extract committed state, rejecting unexpected admission outcomes in a contract.
pub(crate) fn applied(result: CommitResult) -> WorkflowState {
    let CommitResult::Applied { new_state } = result else {
        panic!("unexpected commit: {result:?}")
    };
    new_state
}

/// Isolate the state-size boundary from every other existing growth limit.
pub(crate) fn state_limit(bytes: usize) -> RunGrowthLimits {
    RunGrowthLimits {
        history_size_error: usize::MAX,
        history_size_warn: usize::MAX,
        history_count_error: usize::MAX,
        history_count_warn: usize::MAX,
        state_size_error: bytes,
        state_size_warn: 0,
        transaction_size: usize::MAX,
    }
}

/// A real replay prefix reporting v1 then v2, with the third WFT finishing at 10.
pub(crate) fn reset_history(state: &WorkflowState) -> Vec<HistoryEvent> {
    let start = HistoryEventKind::WorkflowExecutionStarted {
        initiator: None,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        input: Payloads::default(),
        memo: state.memo.clone(),
        search_attributes: state.search_attributes.clone(),
        request_id: "start".into(),
        header: None,
        workflow_start_delay: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        identity: "starter".into(),
        continued_execution_run_id: None,
        first_execution_run_id: Some(state.run_id),
        retry_policy: None,
        attempt: 1,
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: state.workflow_task_timeout,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: None,
        continued_failure: None,
        cron_schedule: None,
        last_completion_result: None,
        versioning_info: None,
        worker_deployment_name: None,
        priority: None,
    };
    let mut history = vec![HistoryEvent {
        event_id: 1,
        happened_at: state.started_at,
        kind: start,
    }];
    for index in 0..3 {
        let scheduled = 2 + index * 3;
        let logical_seq = LogicalTaskSeq((index + 1) as u64);
        let kinds = [
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq,
                task_queue: state.task_queue.clone(),
                workflow_task_timeout: state.workflow_task_timeout,
                attempt: 1,
            },
            HistoryEventKind::WorkflowTaskStarted {
                logical_seq,
                scheduled_event_id: scheduled,
                attempt: 1,
                identity: WorkerIdentity("worker".into()),
                request_id: format!("wft-{index}"),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                suggest_continue_as_new_reasons: Vec::new(),
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            },
            HistoryEventKind::WorkflowTaskCompleted {
                logical_seq,
                scheduled_event_id: scheduled,
                started_event_id: scheduled + 1,
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(WorkerDeploymentVersionRef {
                    deployment_name: "deployment".into(),
                    build_id: format!("v{}", index + 1),
                }),
                worker_deployment_name: None,
            },
        ];
        for kind in kinds {
            history.push(HistoryEvent {
                event_id: history.len() as i64 + 1,
                happened_at: state.started_at,
                kind,
            });
        }
    }
    history
}

/// Execute the frozen old reader, previous-image fold, and tag-dropping writer.
pub(crate) fn legacy_step(
    state: &WorkflowState,
    previous: Option<&ProjectionContext>,
    observation: &Observation,
    history_size: i64,
) -> (WorkflowState, ProjectionContext) {
    let bytes = crate::codec::encode_workflow_state(state).unwrap();
    let mut legacy = legacy_codec::decode_workflow_state(state.run_key, &bytes).unwrap();
    assert!(legacy.used_worker_deployment_versions.is_none());
    legacy.transition_seq = legacy.transition_seq.next();
    observe(&mut legacy, observation);
    let context = baseline_image(&legacy, previous, history_size).unwrap();
    let rewritten = legacy_codec::encode_workflow_state(&legacy).unwrap();
    let decoded = crate::codec::decode_workflow_state(state.run_key, &rewritten).unwrap();
    assert!(decoded.used_worker_deployment_versions.is_none());
    (decoded, context)
}

async fn memory_legacy_step(store: &InMemoryStore, key: RunKey, observation: &Observation) {
    let mut durable = store.inner.lock().await;
    let previous = durable
        .latest_projection_offsets
        .get(&key)
        .map(|offset| &durable.projection_log[*offset].context);
    let (state, context) = legacy_step(
        &durable.runs[&key],
        previous,
        observation,
        durable.history_size.get(&key).copied().unwrap_or(0),
    );
    durable
        .transition_audit
        .entry(key)
        .or_default()
        .push(TransitionAuditRecord {
            run_key: key,
            transition_seq: state.transition_seq,
            history_events: Vec::new(),
            activity_ops: Vec::new(),
            timer_ops: Vec::new(),
            dispatch_ops: Vec::new(),
        });
    durable.append_projection_record(ProjectionRecord {
        partition_id: partition_for(key),
        fanout: 1,
        run_key: key,
        transition_seq: state.transition_seq,
        context,
    });
    durable.runs.insert(key, state);
}

async fn prune_fixture_images(store: &InMemoryStore) {
    let mut durable = store.inner.lock().await;
    durable.projection_log.clear();
    durable.rebuild_projection_indexes();
}

/// Apply the existing completion bookkeeping and a baseline-accepted raw attribute.
pub(crate) fn observe(state: &mut WorkflowState, observation: &Observation) {
    let (version, raw, unspecified) = observation;
    state.apply_wft_versioning(
        if *unspecified {
            VersioningBehavior::Unspecified
        } else {
            VersioningBehavior::AutoUpgrade
        },
        version.as_ref().map(|build_id| WorkerDeploymentVersionRef {
            deployment_name: "deployment".to_owned(),
            build_id: build_id.clone(),
        }),
        None,
    );
    let value = match raw {
        0 => None,
        1 => Some(SearchAttrValue::KeywordList(Vec::new())),
        2 => Some(SearchAttrValue::KeywordList(vec![
            "".into(),
            "raw".into(),
            "raw".into(),
        ])),
        3 => Some(SearchAttrValue::Keyword("raw".into())),
        _ => Some(SearchAttrValue::Int(7)),
    };
    if let Some(value) = value {
        state
            .search_attributes
            .0
            .insert("TemporalUsedWorkerDeploymentVersions".into(), value);
    } else {
        state
            .search_attributes
            .0
            .remove("TemporalUsedWorkerDeploymentVersions");
    }
}

/// Extract the baseline accumulator, preserving order, repeats and empty strings.
pub(crate) fn image_versions(context: &ProjectionContext) -> Vec<String> {
    match context
        .search_attributes
        .0
        .get("TemporalUsedWorkerDeploymentVersions")
    {
        Some(SearchAttrValue::KeywordList(values)) => values.clone(),
        _ => Vec::new(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: projection-accumulator, Property 2: complete baseline image equivalence
    // Folding changes only the accumulator and emits the complete frozen baseline image.
    #[test]
    fn projection_accumulator_complete_image_equivalence(
        steps in observations(),
        seed in prop::collection::vec("[ab]{0,5}", 0..12),
        long_seed in any::<bool>(),
    ) {
        let mut state = fresh_transition(RunKey::new()).next_state;
        let mut seed = seed;
        if long_seed { seed.extend((0..160).map(|index| format!("historical-deployment:build-{index}"))); }
        state.used_worker_deployment_versions = Some(seed.clone());
        let mut previous = baseline_image(&state, None, 0).unwrap();
        previous.search_attributes.0.insert("TemporalUsedWorkerDeploymentVersions".into(), SearchAttrValue::KeywordList(seed));
        for (index, observation) in steps.iter().enumerate() {
            observe(&mut state, observation);
            let mut unchanged = state.clone();
            let history_size = (index as i64) * 17;
            let expected = baseline_image(&state, Some(&previous), history_size).unwrap();
            let actual = prepare_workflow_projection(&mut state, history_size).unwrap();
            prop_assert_eq!(&actual, &expected);
            prop_assert_eq!(state.used_worker_deployment_versions.as_ref(), Some(&image_versions(&actual)));
            unchanged.used_worker_deployment_versions.clone_from(&state.used_worker_deployment_versions);
            prop_assert_eq!(&state, &unchanged);
            prop_assert_eq!(prepare_workflow_projection(&mut state, history_size).unwrap(), actual);
            previous = expected;
        }
    }

    // Feature: projection-accumulator, Property 4: consistent non-durable legacy seeding
    // A seed-capable load validates one snapshot and never persists its readiness.
    #[test]
    fn projection_accumulator_legacy_load_is_consistent_and_nondurable(
        values in prop::collection::vec("[ab]{0,6}", 0..16),
        image_case in 0u8..8,
        attribute_case in 0u8..4,
        ready in any::<bool>(),
        partition in 0u32..64,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = InMemoryStore::default();
            let key = RunKey::new();
            let mut state = fresh_transition(key).next_state;
            state.transition_seq = TransitionSeq(5);
            state.used_worker_deployment_versions = ready.then(|| vec!["already-ready".into()]);
            let mut context = baseline_image(&state, None, 99).unwrap();
            match attribute_case {
                0 => { context.search_attributes.0.remove("TemporalUsedWorkerDeploymentVersions"); }
                1 => { context.search_attributes.0.insert("TemporalUsedWorkerDeploymentVersions".into(), SearchAttrValue::KeywordList(values.clone())); }
                2 => { context.search_attributes.0.insert("TemporalUsedWorkerDeploymentVersions".into(), SearchAttrValue::Keyword("different type".into())); }
                _ => { context.search_attributes.0.insert("TemporalUsedWorkerDeploymentVersions".into(), SearchAttrValue::KeywordList(Vec::new())); }
            }
            let mut record = ProjectionRecord { partition_id: partition, fanout: 1, run_key: key, transition_seq: TransitionSeq(5), context };
            match image_case {
                1 => record.transition_seq = TransitionSeq(3),
                3 => record.transition_seq = TransitionSeq(6),
                4 => record.run_key = RunKey::new(),
                5 => record.context.namespace_id = NamespaceId::new(),
                6 => record.context.workflow_id.0.push_str("-wrong"),
                7 => record.context.run_id = RunId::new(),
                _ => {}
            }
            {
                let mut durable = store.inner.lock().await;
                durable.runs.insert(key, state.clone());
                durable.history_size.insert(key, 99);
                if image_case != 0 {
                    durable.append_projection_record(record.clone());
                    // An invalid stored index/row must not seed a different run.
                    durable.latest_projection_offsets.insert(key, 0);
                }
            }
            let result = store.load_run_with_stats(key).await;
            let mut pure = state.clone();
            let pure_result = seed_workflow_projection_accumulator(&mut pure, (image_case != 0).then_some(&record));
            let invalid = !ready && image_case >= 3;
            assert_eq!(result.is_err(), invalid);
            assert_eq!(pure_result.is_err(), invalid);
            if let Ok((LoadedRun::Existing(loaded), stats)) = result {
                assert_eq!(loaded, pure);
                assert_eq!(stats.history_size_bytes, 99);
                let expected = if ready { vec!["already-ready".into()] } else if image_case != 0 && attribute_case == 1 { values } else { Vec::new() };
                assert_eq!(loaded.used_worker_deployment_versions, Some(expected));
            }
            let stored = store.inner.lock().await;
            assert_eq!(stored.runs[&key], state);
            assert_eq!(stored.projection_image_lookups.load(Ordering::Relaxed), usize::from(!ready));
            drop(stored);
            let snapshot = store.snapshot().await.unwrap();
            let restored = InMemoryStore::from_snapshot(&snapshot).unwrap();
            assert_eq!(restored.inner.lock().await.runs[&key], state);
        });
    }

    // Feature: projection-accumulator, Property 2: complete baseline image equivalence
    // Persisted, returned and reloaded state agree with every field of the frozen image.
    #[test]
    fn projection_accumulator_repository_images_match_baseline(steps in observations()) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                let store = InMemoryStore::default();
                let key = RunKey::new();
                let mut transition = fresh_transition(key);
                let mut previous = None;
                for observation in &steps {
                    observe(&mut transition.next_state, observation);
                    let expected = baseline_image(&transition.next_state, previous.as_ref(), 0).unwrap();
                    let new_state = applied(commit(&store, transition, bundle, ShardEpoch::ZERO).await.unwrap());
                    assert_eq!(new_state.used_worker_deployment_versions, Some(image_versions(&expected)));
                    assert_eq!(store.load_run(key).await.unwrap(), LoadedRun::Existing(new_state.clone()));
                    let durable = store.inner.lock().await;
                    assert_eq!(durable.runs[&key], new_state);
                    assert_eq!(durable.projection_log.last().unwrap().context, expected);
                    assert_eq!(durable.projection_image_lookups.load(Ordering::Relaxed), 0);
                    drop(durable);
                    transition = following(&new_state);
                    previous = Some(expected);
                }
            }
        });
    }

    // Feature: projection-accumulator, Property 1: no projection reads in commits
    // Readiness is required even for existing reset states at sequence zero, before any writes.
    #[test]
    fn projection_accumulator_unseeded_commit_writes_nothing(values in prop::collection::vec("[ab]{0,8}", 0..16)) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                for sequence in [0, 7] {
                    let store = InMemoryStore::default();
                    let key = RunKey::new();
                    let mut state = fresh_transition(key).next_state;
                    state.transition_seq = TransitionSeq(sequence);
                    state.used_worker_deployment_versions = None;
                    store.inner.lock().await.runs.insert(key, state.clone());
                    let before = store.snapshot().await.unwrap();
                    let mut transition = following(&state);
                    transition.next_state.search_attributes.0.insert("TemporalUsedWorkerDeploymentVersions".into(), SearchAttrValue::KeywordList(values.clone()));
                    transition.request_dedupe_ops.push(RequestDedupeOp { request_id: RequestId("not-written".into()) });
                    let error = commit(&store, transition, bundle, ShardEpoch::ZERO).await.unwrap_err();
                    assert!(error.downcast_ref::<ProjectionAccumulatorError>().is_some());
                    assert_eq!(store.snapshot().await.unwrap(), before);
                    assert_eq!(store.inner.lock().await.projection_image_lookups.load(Ordering::Relaxed), 0);
                }
            }
        });
    }

    // Feature: projection-accumulator, Property 7: failure isolation and existing commit contract
    // Earlier fences/deduplication keep their precedence; losing local folds change no durable map.
    #[test]
    fn projection_accumulator_failure_isolation(values in prop::collection::vec("[ab]{0,8}", 0..16)) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                let store = InMemoryStore::default();
                let key = RunKey::new();
                let mut start = fresh_transition(key);
                start.next_state.used_worker_deployment_versions = Some(values.clone());
                start.request_dedupe_ops.push(RequestDedupeOp { request_id: RequestId("duplicate".into()) });
                let winner = applied(commit(&store, start, bundle, ShardEpoch::ZERO).await.unwrap());
                for failure in 0..6 {
                    let before = store.snapshot().await.unwrap();
                    let mut transition = following(&winner);
                    transition.next_state.used_worker_deployment_versions = None;
                    let mut epoch = ShardEpoch::ZERO;
                    match failure {
                        0 => epoch = ShardEpoch(1),
                        1 => transition.expected_seq = TransitionSeq::ZERO,
                        2 => transition.request_dedupe_ops.push(RequestDedupeOp { request_id: RequestId("duplicate".into()) }),
                        3 => {},
                        4 => {
                            transition.next_state.used_worker_deployment_versions = Some(values.clone());
                            transition.next_state.transition_seq = TransitionSeq(u64::MAX);
                        },
                        _ => {
                            transition.expected_seq = TransitionSeq::ZERO;
                            transition.next_state.run_key = RunKey::new();
                            transition.next_state.run_id = RunId::new();
                        },
                    }
                    let result = commit(&store, transition, bundle, epoch).await;
                    match failure {
                        0 | 1 => assert!(matches!(result, Ok(CommitResult::Conflict { .. }))),
                        2 => assert!(matches!(result, Ok(CommitResult::Duplicate))),
                        3 | 4 => assert!(result.is_err()),
                        _ => assert!(matches!(result, Ok(CommitResult::CurrentExecutionConflict { .. }))),
                    }
                    assert_eq!(store.snapshot().await.unwrap(), before);
                }
                let mut close = following(&winner);
                close.next_state.status = ExecutionStatus::Completed;
                close.next_state.closed_at = Some(winner.started_at);
                let closed = applied(commit(&store, close, bundle, ShardEpoch::ZERO).await.unwrap());
                let home = tokeira_types::execution_home_bundle(closed.namespace_id.0.as_bytes(), closed.workflow_id.0.as_bytes(), InMemoryStore::effective_shard_count(&*store.inner.lock().await));
                let result = store.delete_run_for_bundle(key, home, DeleteRunRequest { expected_seq: closed.transition_seq, deleted_at: closed.started_at }, ShardEpoch::ZERO).await.unwrap();
                let DeleteRunResult::Deleted { tombstone } = result else { panic!("delete failed") };
                assert!(tombstone.context.search_attributes.0.is_empty());
                assert!(tombstone.context.memo.0.is_empty());
                assert_eq!(tombstone.context.run_id, closed.run_id);
            }
        });
    }

    // Feature: projection-accumulator, Property 8: exact state growth accounting
    // Every byte of the folded extension counts; only existing activity-input exclusion remains.
    #[test]
    fn projection_accumulator_growth_counts_folded_bytes(
        values in prop::collection::vec("[ab]{0,80}", 0..140),
        input in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                let store = InMemoryStore::default();
                let key = RunKey::new();
                let mut start = fresh_transition(key);
                start.growth_limits = Some(state_limit(0));
                let state = applied(commit(&store, start, bundle, ShardEpoch::ZERO).await.unwrap());
                let mut transition = following(&state);
                transition.next_state.used_worker_deployment_versions = Some(values.clone());
                observe(&mut transition.next_state, &(Some("new-observation".into()), 0, false));
                let mut activity = activity_fixture();
                activity.input = tokeira_types::Payloads(vec![tokeira_types::Payload::new(input.clone())]);
                let excluded = crate::codec::encode(&activity.input).unwrap().len();
                transition.next_state.activities.insert(activity.activity_id.clone(), activity);
                let mut folded = transition.next_state.clone();
                prepare_workflow_projection(&mut folded, 0).unwrap();
                let bytes = crate::codec::encode_workflow_state(&folded).unwrap();
                let measured = bytes.len() - excluded;
                assert_eq!(crate::codec::workflow_state_encoded_len(&folded).unwrap(), bytes.len());
                assert_eq!(crate::codec::measured_state_len(bytes.len(), &folded).unwrap(), measured);
                transition.growth_limits = Some(state_limit(measured - 1));
                let before = store.snapshot().await.unwrap();
                let error = commit(&store, transition.clone(), bundle, ShardEpoch::ZERO).await.unwrap_err();
                assert_eq!(error.downcast_ref::<RunLimitExceeded>().unwrap().limit, RunLimit::StateSize);
                assert_eq!(store.snapshot().await.unwrap(), before);
                transition.growth_limits = Some(state_limit(measured));
                let state = applied(commit(&store, transition, bundle, ShardEpoch::ZERO).await.unwrap());
                assert_eq!(state, folded);
                let mut close = following(&state);
                close.next_state.status = ExecutionStatus::Completed;
                close.growth_limits = Some(state_limit(0));
                applied(commit(&store, close, bundle, ShardEpoch::ZERO).await.unwrap());
            }
        });
    }

    // Feature: projection-accumulator, Property 3: new run boundaries
    // Replay copies routing but not prior accumulation; the first image observes only its current version.
    #[test]
    fn projection_accumulator_reset_boundaries(values in prop::collection::vec("[ab]{0,8}", 0..16)) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                for change in [false, true] {
                    let store = InMemoryStore::default();
                    let base = RunKey::new();
                    let mut start = fresh_transition(base);
                    start.next_state.used_worker_deployment_versions = Some(values.clone());
                    start.next_state.status = ExecutionStatus::Completed;
                    start.next_state.closed_at = Some(start.next_state.started_at);
                    start.history_events = reset_history(&start.next_state).into();
                    start.event_principals = vec![None; start.history_events.len()].into();
                    start.next_state.last_event_id = 10;
                    let base_state = applied(commit(&store, start, bundle, ShardEpoch::ZERO).await.unwrap());
                    let mut predecessor = base;
                    for generation in 0..2 {
                        let run_id = RunId::new();
                        let key = RunKey::derive(base_state.namespace_id, &base_state.workflow_id, run_id);
                        let fork = if generation == 0 { 10 } else { 7 };
                        let expected = store.find_latest_run(base_state.namespace_id, &base_state.workflow_id).await.unwrap();
                        store.materialize_reset_successor(predecessor, fork, run_id, expected).await.unwrap();
                        {
                            let mut durable = store.inner.lock().await;
                            assert_eq!(durable.runs[&key].used_worker_deployment_versions, Some(Vec::new()));
                            assert!(!durable.projection_log.iter().any(|row| row.run_key == key));
                            if generation == 1 {
                                // A reset produced by an old writer has no tag or image.
                                durable.runs.get_mut(&key).unwrap().used_worker_deployment_versions = None;
                            }
                        }
                        let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else { panic!("reset") };
                        assert_eq!(state.used_worker_deployment_versions, Some(Vec::new()));
                        let mut transition = following(&state);
                        if change { observe(&mut transition.next_state, &(Some("v3".into()), 0, false)); }
                        let expected = baseline_image(&transition.next_state, None, store.inner.lock().await.history_size[&key]).unwrap();
                        let new_state = applied(commit(&store, transition, bundle, ShardEpoch::ZERO).await.unwrap());
                        let current_version = if change { "deployment:v3" } else if generation == 0 { "deployment:v2" } else { "deployment:v1" };
                        assert_eq!(new_state.used_worker_deployment_versions, Some(vec![current_version.into()]));
                        assert_eq!(store.inner.lock().await.projection_log.last().unwrap().context, expected);
                        predecessor = key;
                    }
                }
            }
        });
    }

    // Feature: projection-accumulator, Property 6: restart legacy writers and pruning
    // Eviction, old writers and restart preserve images while retained; pruning needs both prerequisites.
    #[test]
    fn projection_accumulator_survives_restart_and_mixed_writers(
        steps in observations(),
        old_on_even in any::<bool>(),
        boundary in 0u8..3,
    ) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for bundle in [false, true] {
                let mut store = InMemoryStore::default();
                let mut key = RunKey::new();
                let mut start = fresh_transition(key);
                let mut activity = super::tests::activity_state("heartbeat");
                activity.last_heartbeat_at = Some(start.next_state.started_at);
                start.next_state.activities.insert(activity.activity_id.clone(), activity);
                if boundary == 1 { observe(&mut start.next_state, &(Some("inherited".into()), 0, false)); }
                if boundary == 2 {
                    start.history_events = reset_history(&start.next_state).into();
                    start.event_principals = vec![None; start.history_events.len()].into();
                    start.next_state.last_event_id = 10;
                    start.next_state.status = ExecutionStatus::Completed;
                }
                let first = applied(commit(&store, start, bundle, ShardEpoch::ZERO).await.unwrap());
                if boundary == 2 {
                    let new_id = RunId::new();
                    let expected = store.find_latest_run(first.namespace_id, &first.workflow_id).await.unwrap();
                    store.materialize_reset_successor(key, 10, new_id, expected).await.unwrap();
                    key = RunKey::derive(first.namespace_id, &first.workflow_id, new_id);
                }
                for (index, observation) in steps.iter().enumerate() {
                    let before_load = store.snapshot().await.unwrap();
                    let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else { panic!("load") };
                    assert_eq!(store.snapshot().await.unwrap(), before_load);
                    let (previous, history_size) = {
                        let durable = store.inner.lock().await;
                        (durable.latest_projection_offsets.get(&key).map(|offset| durable.projection_log[*offset].context.clone()), durable.history_size.get(&key).copied().unwrap_or(0))
                    };
                    let mut transition = following(&state);
                    observe(&mut transition.next_state, observation);
                    let expected = baseline_image(&transition.next_state, previous.as_ref(), history_size).unwrap();
                    if (index % 2 == 0) == old_on_even {
                        memory_legacy_step(&store, key, observation).await;
                        assert!(store.inner.lock().await.runs[&key].used_worker_deployment_versions.is_none());
                    } else {
                        let applied = applied(commit(&store, transition, bundle, ShardEpoch::ZERO).await.unwrap());
                        assert_eq!(applied.used_worker_deployment_versions, Some(image_versions(&expected)));
                        assert_eq!(store.inner.lock().await.runs[&key], applied);
                    }
                    assert_eq!(store.inner.lock().await.projection_log.last().unwrap().context, expected);
                    let persisted = store.snapshot().await.unwrap();
                    store = InMemoryStore::from_snapshot(&persisted).unwrap();
                    let LoadedRun::Existing(loaded) = store.load_run(key).await.unwrap() else { panic!("restored") };
                    assert_eq!(loaded.used_worker_deployment_versions, Some(image_versions(&expected)));
                    assert_eq!(store.snapshot().await.unwrap(), persisted);
                }

                let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else { panic!("final load") };
                let mut persist = following(&state);
                observe(&mut persist.next_state, &(None, 0, false));
                persist.next_state.used_worker_deployment_versions.as_mut().unwrap().push("retained-only".into());
                let ready = applied(commit(&store, persist, bundle, ShardEpoch::ZERO).await.unwrap());
                let persisted = store.snapshot().await.unwrap();
                let pruned = InMemoryStore::from_snapshot(&persisted).unwrap();
                prune_fixture_images(&pruned).await;
                for observation in &steps {
                    let LoadedRun::Existing(state) = store.load_run(key).await.unwrap() else { panic!("control") };
                    let mut transition = following(&state);
                    observe(&mut transition.next_state, observation);
                    let control = applied(commit(&store, transition.clone(), bundle, ShardEpoch::ZERO).await.unwrap());
                    let after_prune = applied(commit(&pruned, transition, bundle, ShardEpoch::ZERO).await.unwrap());
                    assert_eq!(control, after_prune);
                    assert_eq!(store.inner.lock().await.projection_log.last().unwrap().context, pruned.inner.lock().await.projection_log.last().unwrap().context);
                }

                let unseeded = InMemoryStore::from_snapshot(&persisted).unwrap();
                let legacy_blob = legacy_codec::encode_workflow_state(&ready).unwrap();
                unseeded.inner.lock().await.runs.insert(key, crate::codec::decode_workflow_state(key, &legacy_blob).unwrap());
                prune_fixture_images(&unseeded).await;
                let LoadedRun::Existing(lost) = unseeded.load_run(key).await.unwrap() else { panic!("negative control") };
                assert_eq!(lost.used_worker_deployment_versions, Some(Vec::new()));
                assert_ne!(lost.used_worker_deployment_versions, ready.used_worker_deployment_versions);

                let old_after_prune = InMemoryStore::from_snapshot(&persisted).unwrap();
                prune_fixture_images(&old_after_prune).await;
                memory_legacy_step(&old_after_prune, key, &(None, 0, false)).await;
                let LoadedRun::Existing(lost) = old_after_prune.load_run(key).await.unwrap() else { panic!("old writer negative control") };
                assert_eq!(lost.used_worker_deployment_versions, Some(Vec::new()));
                assert_ne!(lost.used_worker_deployment_versions, ready.used_worker_deployment_versions);
            }
        });
    }
}

#[test]
fn projection_accumulator_seed_errors_preserve_input_and_ready_state() {
    let mut state = fresh_transition(RunKey::new()).next_state;
    state.used_worker_deployment_versions = None;
    let mut record = ProjectionRecord {
        partition_id: 0,
        fanout: 1,
        run_key: RunKey::new(),
        transition_seq: TransitionSeq(state.transition_seq.0 + 1),
        context: baseline_image(&state, None, 0).unwrap(),
    };
    let before = state.clone();
    let error = seed_workflow_projection_accumulator(&mut state, Some(&record)).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionAccumulatorError>(),
        Some(ProjectionAccumulatorError::InvalidSeed {
            defect: "image belongs to another run",
            ..
        })
    ));
    assert_eq!(state, before);
    record.run_key = state.run_key;
    let error = seed_workflow_projection_accumulator(&mut state, Some(&record)).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ProjectionAccumulatorError>(),
        Some(ProjectionAccumulatorError::InvalidSeed {
            defect: "image sequence is newer than the loaded state",
            ..
        })
    ));
    assert_eq!(state, before);
    assert!(
        prepare_workflow_projection(&mut state, 0)
            .unwrap_err()
            .downcast::<ProjectionAccumulatorError>()
            .is_ok()
    );
    assert_eq!(state, before);
    state.used_worker_deployment_versions = Some(vec!["ready".into()]);
    let ready = state.clone();
    seed_workflow_projection_accumulator(&mut state, Some(&record)).unwrap();
    assert_eq!(state, ready);
    state.transition_seq = TransitionSeq(u64::MAX);
    let before = state.clone();
    assert!(prepare_workflow_projection(&mut state, 0).is_err());
    assert_eq!(state, before);
}
