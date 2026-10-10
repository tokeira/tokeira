//! Legacy single-run snapshots for runtime commit-consumer contracts.
//! Removing only tag 3 exercises the real repository load path while preserving
//! positional state, heartbeat extensions, images and every other durable map.

use std::sync::Arc;

use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, LoadedRun, ResetRequest, Transition, VersioningBehavior,
    WorkflowState,
};
use tokeira_storage::{
    CommitResult, InMemoryStore, RunRepository,
    codec::{
        self, USED_WORKER_DEPLOYMENT_VERSIONS_SECTION, WORKFLOW_STATE_ENVELOPE_VERSION,
        WORKFLOW_STATE_EXTENSION_MAGIC,
    },
    memory::{RUN_STATE_EXTENSION_SECTION, SNAPSHOT_EXTENSION_MAGIC},
};
use tokeira_types::{
    ExecutionRef, Payloads, RequestContext, RetryPolicy, RunId, RunKey, ShardEpoch, TransitionSeq,
    WorkerIdentity,
};

use crate::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};

/// Commit fixture state through the same public repository used by runtime.
pub(crate) async fn commit_state(
    repo: &InMemoryStore,
    state: WorkflowState,
    expected: TransitionSeq,
) -> WorkflowState {
    let key = state.run_key;
    let result = repo
        .commit_transition(
            key,
            Transition {
                expected_seq: expected,
                next_state: state,
                history_events: Default::default(),
                event_principals: Default::default(),
                request_dedupe_ops: Default::default(),
                activity_ops: Default::default(),
                timer_ops: Default::default(),
                dispatch_ops: Default::default(),
                events_numbered_at_close: 0,
                growth_limits: None,
            },
            ShardEpoch::ZERO,
        )
        .await
        .unwrap();
    let CommitResult::Applied { new_state } = result else {
        panic!("fixture commit refused: {result:?}")
    };
    new_state
}

/// Return a genuine legacy snapshot containing the supplied previous image list.
/// The input fixture must contain exactly one run; all other snapshot data survives.
pub(crate) async fn legacy_copy(
    repo: &InMemoryStore,
    key: RunKey,
    versions: Vec<String>,
) -> InMemoryStore {
    let LoadedRun::Existing(mut state) = repo.load_run(key).await.unwrap() else {
        panic!("missing fixture run")
    };
    let expected = state.transition_seq;
    state.transition_seq = expected.next();
    state.used_worker_deployment_versions = Some(versions);
    let state = commit_state(repo, state, expected).await;
    let positional = codec::encode(&(WORKFLOW_STATE_ENVELOPE_VERSION, &state)).unwrap();
    let blob = codec::encode_workflow_state(&state).unwrap();
    let extension = blob[positional.len()..].to_vec();
    let (magic, mut sections): (u32, Vec<(u32, Vec<u8>)>) = codec::decode(&extension).unwrap();
    assert_eq!(magic, WORKFLOW_STATE_EXTENSION_MAGIC);
    sections.retain(|(tag, _)| *tag != USED_WORKER_DEPLOYMENT_VERSIONS_SECTION);
    let original = codec::encode(&(
        SNAPSHOT_EXTENSION_MAGIC,
        vec![(
            RUN_STATE_EXTENSION_SECTION,
            codec::encode(&vec![(key, extension)]).unwrap(),
        )],
    ))
    .unwrap();
    let mut snapshot = repo.snapshot().await.unwrap();
    assert!(
        snapshot.ends_with(&original),
        "fixture must have exactly one run"
    );
    snapshot.truncate(snapshot.len() - original.len());
    if !sections.is_empty() {
        let legacy = codec::encode(&(magic, sections)).unwrap();
        snapshot.extend(
            codec::encode(&(
                SNAPSHOT_EXTENSION_MAGIC,
                vec![(
                    RUN_STATE_EXTENSION_SECTION,
                    codec::encode(&vec![(key, legacy)]).unwrap(),
                )],
            ))
            .unwrap(),
        );
    }
    let legacy = InMemoryStore::from_snapshot(&snapshot).unwrap();
    assert_eq!(legacy.snapshot().await.unwrap(), snapshot);
    tokeira_storage::prepare_execution_placement(&legacy)
        .await
        .unwrap();
    legacy
}

// Feature: projection-accumulator, Property 3: reset bookkeeping uses seed-capable state and stats.
#[tokio::test]
async fn projection_accumulator_runtime_reset_commits_from_materialized_state() {
    let parent = crate::runtime::workflow_task::tests::open_state("reset-source".into(), None);
    let policy = RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 1.0,
        maximum_interval: None,
        maximum_attempts: 3,
        non_retryable_error_types: Vec::new(),
    };
    let mut start = crate::runtime::workflow_task::build_retry_successor_start(
        &parent,
        None,
        &policy,
        Payloads::default(),
        RunId::new(),
    );
    start.workflow_start_delay = None;
    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let CommitResult::Applied {
        new_state: mut state,
    } = runtime.start_workflow(start).await.unwrap()
    else {
        panic!("start")
    };
    let pending = state.pending_workflow_task.take().unwrap();
    let key = state.run_key;
    let expected = state.transition_seq;
    state.transition_seq = expected.next();
    state.last_event_id = 4;
    let at = OffsetDateTime::now_utc();
    let events = vec![
        HistoryEvent {
            event_id: 3,
            happened_at: at,
            kind: HistoryEventKind::WorkflowTaskStarted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                attempt: 1,
                identity: WorkerIdentity("worker".into()),
                request_id: "started".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                suggest_continue_as_new_reasons: Vec::new(),
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            },
        },
        HistoryEvent {
            event_id: 4,
            happened_at: at,
            kind: HistoryEventKind::WorkflowTaskCompleted {
                logical_seq: pending.logical_seq,
                scheduled_event_id: pending.scheduled_event_id,
                started_event_id: 3,
                identity: WorkerIdentity("worker".into()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
            },
        },
    ];
    store
        .commit_transition(
            key,
            Transition {
                expected_seq: expected,
                next_state: state.clone(),
                history_events: events.into(),
                event_principals: vec![None; 2].into(),
                request_dedupe_ops: Default::default(),
                activity_ops: Default::default(),
                timer_ops: Default::default(),
                dispatch_ops: Default::default(),
                events_numbered_at_close: 0,
                growth_limits: None,
            },
            ShardEpoch::ZERO,
        )
        .await
        .unwrap();
    let store = Arc::new(legacy_copy(&store, key, vec!["pre-reset".into()]).await);
    let runtime = TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    );
    let new_run_id = RunId::new();
    runtime
        .reset_workflow(
            ExecutionRef {
                namespace_id: state.namespace_id,
                workflow_id: state.workflow_id.clone(),
                run_id: Some(state.run_id),
            },
            ResetRequest {
                fork_event_id: 4,
                new_run_id,
                reapply_exclude_signal: false,
                reapply_exclude_update: false,
                post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: None,
                reason: "test reset".into(),
                request: RequestContext::unattributed(at),
                now: at,
            },
        )
        .await
        .unwrap();
    let successor_key = RunKey::derive(state.namespace_id, &state.workflow_id, new_run_id);
    let (LoadedRun::Existing(successor), stats) =
        store.load_run_with_stats(successor_key).await.unwrap()
    else {
        panic!("successor")
    };
    assert!(successor.transition_seq > TransitionSeq::ZERO);
    assert_eq!(successor.used_worker_deployment_versions, Some(Vec::new()));
    assert!(stats.history_size_bytes > 0);
    assert!(successor.pending_workflow_task.is_some());
}
