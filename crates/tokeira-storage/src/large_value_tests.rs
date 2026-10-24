//! Shared contracts for values over the part size (`large-value-parts`): a run's
//! values, CHASM nodes' data and worker invocation requests. They run against the
//! in-memory stores in the default suite and against an ephemeral Aurora DSQL
//! cluster in the live suite.
//!
//! DSQL refuses a value over 1 MiB, so on DSQL each case needs its values stored in
//! parts. The in-memory stores store every value whole, and each case reads back
//! what it wrote.

use std::collections::{BTreeMap, BTreeSet};

use time::{Duration, OffsetDateTime};
use tokeira_chasm::{
    ChasmNode, ExecutionKey, LifecycleState, NodeMetadata, VersionedTransition,
    path::{self, PathSegment},
};
use tokeira_kernel::{HistoryEvent, HistoryEventKind, LoadedRun, RequestDedupeOp, WorkflowState};
use tokeira_types::{
    BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, ExecutionStatus,
    IncarnationId, NamespaceId, Payload, Payloads, RequestId, RunKey, ScalingGroupId,
    WorkerComputeControllerLifecycle, WorkerComputeGroupEligibility, WorkerComputeHealth,
    WorkerComputeInvokeReason, WorkerComputeProviderActionStatus, WorkerComputeTaskType,
};
use uuid::Uuid;

use crate::{
    ChasmNodeRepository, CurrentRun, ExpectedVersion, InMemoryChasmNodeStore, InMemoryStore,
    InMemoryWorkerComputeRepository, NodePersistOutcome, NodeWrite,
    WORKER_COMPUTE_RECORD_FORMAT_VERSION, WorkerComputeActionAttemptStart,
    WorkerComputeActionFinalization, WorkerComputeActionFinalizeResult,
    WorkerComputeControllerAdmission, WorkerComputeControllerCommitResult,
    WorkerComputeControllerRecord, WorkerComputeProviderAction, WorkerComputeRepository,
    WorkerComputeScalingGroupState,
    bulk_write_tests::{
        Backend, OwnedRows, commit, commit_base_from, delete_and_purge, materialize_and_check,
        noise, signal, with_events,
    },
    history_event_tests::assert_each_event_reads,
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

const MIB: usize = 1024 * 1024;

/// A payload of `len` bytes no store can shrink.
pub(crate) fn payloads(len: usize, seed: u64) -> Payloads {
    Payloads(vec![Payload::new(noise(len, seed))])
}

/// A batch of one event with a 2 MiB payload, a batch of two events that come to
/// nearly the 4 MiB a batch may hold, then batches within the part size.
fn large_batches() -> Vec<Vec<HistoryEventKind>> {
    vec![
        vec![signal(0, 2 * MIB)],
        vec![signal(1, 1_950_000), signal(2, 1_950_000)],
        (3..7).map(|index| signal(index, 300_000)).collect(),
    ]
}

/// Commit each chunk's events in a commit of its own, after `state`, and return
/// the last state with the events written.
async fn commit_chunks(
    backend: &impl Backend,
    mut state: WorkflowState,
    chunks: Vec<Vec<HistoryEventKind>>,
) -> (WorkflowState, Vec<HistoryEvent>) {
    let mut written = Vec::new();
    for kinds in chunks {
        let first = state.last_event_id + 1;
        let transition = with_events(following(&state), first, kinds);
        written.extend(transition.history_events.iter().cloned());
        state = applied(commit(backend, transition).await);
    }
    (state, written)
}

/// A run whose history holds batches over the part size reads back each event
/// whole, by each read of history.
pub(crate) async fn batches_over_the_part_size_read_back(backend: &impl Backend) {
    let start = applied(commit(backend, fresh_transition(RunKey::new())).await);
    let (state, written) = commit_chunks(backend, start, large_batches()).await;
    assert_eq!(
        backend
            .repo()
            .read_history_to_end(state.run_key, 0)
            .await
            .expect("reads the history"),
        written
    );
    assert_each_event_reads(backend, state.run_key).await;
    let audited = backend
        .repo()
        .read_transition_audit(state.run_key)
        .await
        .expect("reads the audit")
        .into_iter()
        .flat_map(|record| record.history_events)
        .collect::<Vec<_>>();
    assert_eq!(audited, written);
}

/// A run's state that grows over the part size, shrinks below it and grows again
/// loads back as each write left it, up to a state near the 8 MiB a run's state
/// may hold.
pub(crate) async fn a_state_over_the_part_size_loads_back(backend: &impl Backend) {
    let mut state = applied(commit(backend, fresh_transition(RunKey::new())).await);
    for len in [
        1_500_000,
        0,
        3_000_000,
        2_000_000,
        100,
        7 * MIB + MIB / 2,
        900_000,
    ] {
        let mut transition = following(&state);
        // The predecessor's result is held in the state alone, apart from the
        // visibility image, so it takes only the state over the part size.
        transition.next_state.last_completion_result = (len > 0).then(|| payloads(len, len as u64));
        state = applied(commit(backend, transition).await);
        assert_eq!(
            loaded(backend, state.run_key).await,
            state,
            "a {len}-byte result"
        );
    }
}

/// A closed base run whose start event carries a 2 MiB predecessor result, so
/// that its first batch and its state are over the part size, and the state that
/// a reset replays from it too. Its history also holds an event near the 4 MiB a
/// batch may hold.
async fn base_with_values_over_the_part_size(backend: &impl Backend) -> (WorkflowState, i64) {
    let mut start = fresh_transition(RunKey::new());
    start.next_state.last_completion_result = Some(payloads(2 * MIB, 7));
    commit_base_from(
        backend,
        start,
        vec![
            vec![signal(0, 16)],
            vec![signal(1, 4 * MIB - 128 * 1024)],
            vec![signal(2, 600_000)],
        ],
    )
    .await
}

// Feature: large-value-parts, Property 5: A reset copies values over the part size
/// A reset of a base whose history and state are over the part size copies the
/// history before the fork, and the successor's replayed state loads whole.
pub(crate) async fn a_reset_copies_values_over_the_part_size(backend: &impl Backend) {
    let (base, fork) = base_with_values_over_the_part_size(backend).await;
    let successor = materialize_and_check(backend, &base, fork).await;
    let state = loaded(backend, successor).await;
    assert_eq!(
        state.last_completion_result, base.last_completion_result,
        "the successor's replayed state holds the start event's result"
    );
}

/// A closed run whose state and history hold values over the part size, with
/// request records the purge removes ahead of its history. Returns the run's
/// last state and every event written.
pub(crate) async fn closed_run_with_split_values(
    backend: &impl Backend,
) -> (WorkflowState, Vec<HistoryEvent>) {
    let mut start = fresh_transition(RunKey::new());
    start.next_state.last_completion_result = Some(payloads(2 * MIB, 11));
    start.request_dedupe_ops = (0..8)
        .map(|index| RequestDedupeOp {
            request_id: RequestId(format!("request-{index}")),
        })
        .collect();
    let start = applied(commit(backend, start).await);
    let (state, mut written) = commit_chunks(backend, start, large_batches()).await;
    let first = state.last_event_id + 1;
    let mut close = with_events(following(&state), first, vec![signal(7, 1_200_000)]);
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(close.next_state.started_at);
    close.next_state.pending_workflow_task = None;
    written.extend(close.history_events.iter().cloned());
    (applied(commit(backend, close).await), written)
}

// Feature: large-value-parts, Property 6: Removing a value removes its parts
/// The deletion and purge of a run with split values leave none of its rows.
pub(crate) async fn deletion_of_a_run_with_split_values(backend: &impl Backend) {
    let (state, _) = closed_run_with_split_values(backend).await;
    delete_and_purge(backend, &state).await;
    assert_eq!(
        backend.owned_rows(state.run_key).await.unwrap(),
        OwnedRows::default()
    );
    assert!(matches!(
        backend.repo().load_run(state.run_key).await.unwrap(),
        LoadedRun::Absent
    ));
    assert_eq!(
        backend
            .repo()
            .read_history_to_end(state.run_key, 0)
            .await
            .unwrap(),
        Vec::new()
    );
}

/// The run's state as the store loads it.
pub(crate) async fn loaded(backend: &impl Backend, run_key: RunKey) -> WorkflowState {
    match backend
        .repo()
        .load_run(run_key)
        .await
        .expect("loads the run")
    {
        LoadedRun::Existing(state) => state,
        LoadedRun::Absent => panic!("the run {run_key:?} exists"),
    }
}

/// The archetype of the CHASM executions these cases write.
const NODE_ARCHETYPE: u32 = 7;

/// A node of [`NODE_ARCHETYPE`] at its first transition, holding `data`.
fn new_node(data: Option<Vec<u8>>) -> ChasmNode {
    ChasmNode {
        metadata: NodeMetadata::new(
            NODE_ARCHETYPE,
            Some(LifecycleState::Running),
            VersionedTransition::new(1, 1),
        ),
        data,
    }
}

/// What a read of nodes returned, without their data: each node's path, version
/// and data length, for a failure message of a readable size.
fn shape(nodes: &[(Vec<u8>, ChasmNode)]) -> Vec<(String, VersionedTransition, Option<usize>)> {
    nodes
        .iter()
        .map(|(path, node)| {
            (
                String::from_utf8_lossy(path).into_owned(),
                node.metadata.versioned_transition,
                node.data.as_ref().map(Vec::len),
            )
        })
        .collect()
}

/// Every read of the execution returns `nodes` whole: the execution, the subtree
/// at `child`, and the scan of every execution's root node.
async fn assert_reads(
    repo: &dyn ChasmNodeRepository,
    key: &ExecutionKey,
    nodes: &BTreeMap<Vec<u8>, ChasmNode>,
    child: &[u8],
) {
    let expected = nodes
        .iter()
        .map(|(path, node)| (path.clone(), node.clone()))
        .collect::<Vec<_>>();
    let execution = repo.load_execution(key).await.expect("loads the execution");
    assert!(
        execution == expected,
        "the execution read {:?}, not {:?}",
        shape(&execution),
        shape(&expected)
    );
    let expected_subtree = vec![(child.to_vec(), nodes[child].clone())];
    let subtree = repo
        .load_subtree(key, child)
        .await
        .expect("loads the subtree");
    assert!(
        subtree == expected_subtree,
        "the subtree read {:?}, not {:?}",
        shape(&subtree),
        shape(&expected_subtree)
    );
    let root = repo
        .scan_executions()
        .await
        .expect("scans the executions")
        .into_iter()
        .find(|(scanned, _)| scanned == key)
        .map(|(_, root)| root);
    assert!(
        root.as_ref() == nodes.get(&Vec::new()),
        "the scan read the root with {:?} bytes of data, not {:?}",
        root.as_ref().map(|root| root.data.as_ref().map(Vec::len)),
        nodes
            .get(&Vec::new())
            .map(|root| root.data.as_ref().map(Vec::len))
    );
}

/// A CHASM execution whose nodes' data grows over the part size, shrinks below it
/// and goes, written one transition at a time, reads back as each transition left
/// it, by every read of the execution; and its deletion leaves no node.
///
/// A standalone activity's node holds its input, its result and its last
/// heartbeat details, each a payload of up to 2 MiB; the root grows to three such
/// payloads.
pub(crate) async fn nodes_over_the_part_size_read_back(repo: &dyn ChasmNodeRepository) {
    let key = ExecutionKey::new(
        Uuid::new_v4().to_string(),
        "large-node",
        Uuid::new_v4().to_string(),
    );
    let child = path::encode(&[PathSegment::field("child")]).expect("a field path encodes");
    let sibling = path::encode(&[PathSegment::field("sibling")]).expect("a field path encodes");
    let mut nodes = BTreeMap::from([
        (Vec::new(), new_node(Some(noise(2 * MIB + 1, 1)))),
        (child.clone(), new_node(Some(noise(100, 2)))),
        (sibling, new_node(None)),
    ]);
    let batch = nodes
        .iter()
        .map(|(encoded_path, node)| NodeWrite {
            encoded_path: encoded_path.clone(),
            node: node.clone(),
            expected: ExpectedVersion::Absent,
        })
        .collect();
    let current = CurrentRun {
        run_id: key.run_id.clone(),
        request_id: "large-node".to_owned(),
        status: LifecycleState::Running,
        vt_epoch: VersionedTransition::new(1, 1),
    };
    assert_eq!(
        repo.persist_new_execution(&key, NODE_ARCHETYPE, batch, current, None)
            .await
            .expect("persists the new execution"),
        NodePersistOutcome::Applied
    );
    assert_reads(repo, &key, &nodes, &child).await;
    for (count, root_len, child_len) in [
        (2, Some(6 * MIB), Some(1_500_000)),
        (3, Some(300_000), None),
        (4, Some(3 * MIB), Some(2 * MIB)),
        (5, None, Some(50)),
    ] {
        let mut batch = Vec::new();
        for (encoded_path, len) in [(Vec::new(), root_len), (child.clone(), child_len)] {
            let node = nodes.get_mut(&encoded_path).expect("the node exists");
            let expected = ExpectedVersion::Vt(node.metadata.versioned_transition);
            node.metadata.versioned_transition = VersionedTransition::new(1, count);
            node.data = len.map(|len| noise(len, len as u64 ^ count as u64));
            batch.push(NodeWrite {
                encoded_path,
                node: node.clone(),
                expected,
            });
        }
        assert_eq!(
            repo.persist_dirty(&key, batch)
                .await
                .expect("persists the transition"),
            NodePersistOutcome::Applied,
            "transition {count}"
        );
        assert_reads(repo, &key, &nodes, &child).await;
    }
    repo.delete_execution(&key)
        .await
        .expect("deletes the execution");
    assert_eq!(
        shape(
            &repo
                .load_execution(&key)
                .await
                .expect("loads the execution")
        ),
        Vec::new()
    );
    assert!(
        !repo
            .scan_executions()
            .await
            .expect("scans the executions")
            .iter()
            .any(|(scanned, _)| scanned == &key)
    );
}

/// The scaling group the worker-compute cases use, with the configuration
/// fingerprint its actions carry.
const SCALING_GROUP: &str = "primary";

/// A controller for a deployment of its own, not yet admitted.
fn controller(namespace_id: NamespaceId, now: OffsetDateTime) -> WorkerComputeControllerRecord {
    WorkerComputeControllerRecord {
        format_version: WORKER_COMPUTE_RECORD_FORMAT_VERSION,
        key: ControllerInstanceKey {
            namespace_id,
            deployment_name: DeploymentId("large-requests".to_owned()),
            build_id: BuildId("build".to_owned()),
        },
        namespace_name: "namespace".to_owned(),
        revision: 0,
        lifecycle: WorkerComputeControllerLifecycle::Inactive,
        slot: None,
        owner: None,
        owner_epoch: 0,
        lease_until: None,
        groups: BTreeMap::from([(
            ScalingGroupId(SCALING_GROUP.to_owned()),
            WorkerComputeScalingGroupState {
                fingerprint: ConfigurationFingerprint::from_canonical_bytes(&[1]),
                effective_task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
                eligibility: WorkerComputeGroupEligibility::Eligible,
                health: WorkerComputeHealth::Active,
                activation_fingerprint: None,
                activation_status: None,
                last_scale_up_at: None,
                prior_dispatch_rates: BTreeMap::new(),
                last_action_id: None,
                last_failure_category: None,
            },
        )]),
        next_metrics_poll_at: Some(now),
        reconciled_at: now,
    }
}

/// A due action of `controller`'s group whose request is `request_data`.
fn action(
    controller: &WorkerComputeControllerRecord,
    request_data: Vec<u8>,
    now: OffsetDateTime,
) -> WorkerComputeProviderAction {
    let action_id = Uuid::new_v4();
    WorkerComputeProviderAction {
        action_id,
        due_bucket: WorkerComputeProviderAction::due_bucket(action_id),
        controller_key: controller.key.clone(),
        scaling_group: ScalingGroupId(SCALING_GROUP.to_owned()),
        configuration_fingerprint: ConfigurationFingerprint::from_canonical_bytes(&[1]),
        endpoint_name: "worker-compute".to_owned(),
        reason: WorkerComputeInvokeReason::NoSyncMatch,
        request_data,
        status: WorkerComputeProviderActionStatus::Pending,
        attempts: 0,
        attempt_started_at: None,
        claim_epoch: 0,
        next_attempt_at: now,
        claim: None,
        superseded_at: None,
        last_error_category: None,
        created_at: now,
        updated_at: now,
    }
}

/// Worker invocation requests on each side of the 960 KiB part size, up to the
/// 2 MiB the provider contract allows, each read back whole by every read of its
/// action: its claim, the start of its attempt and its finalization. Returns each
/// request by its action's id.
pub(crate) async fn requests_over_the_part_size_read_back(
    repo: &dyn WorkerComputeRepository,
) -> BTreeMap<Uuid, Vec<u8>> {
    // To the second, which every timestamp column keeps exactly.
    let now = OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp())
        .expect("now is a valid timestamp");
    let namespace_id = NamespaceId::new();
    let candidate = controller(namespace_id, now);
    let key = candidate.key.clone();
    let admission = repo
        .admit_controller(candidate, 1, now)
        .await
        .expect("admits the controller");
    assert!(
        matches!(admission, WorkerComputeControllerAdmission::Admitted(_)),
        "admission {admission:?}"
    );
    let claimed = repo
        .claim_controller(&key, IncarnationId::new(), now, now + Duration::minutes(5))
        .await
        .expect("claims the controller")
        .expect("the controller is claimable");
    let mut record = claimed.record.clone();
    let mut requests = BTreeMap::new();
    for (index, len) in [2 * MIB, 983_040, 983_041, 100].into_iter().enumerate() {
        let expected_revision = record.revision;
        record.revision += 1;
        let request = noise(len, index as u64);
        let action = action(&record, request.clone(), now);
        requests.insert(action.action_id, request);
        assert_eq!(
            repo.commit_decision(
                &claimed.claim,
                expected_revision,
                record.clone(),
                Some(action)
            )
            .await
            .expect("commits the decision"),
            WorkerComputeControllerCommitResult::Applied,
            "a {len}-byte request"
        );
    }
    let delivered = repo
        .claim_due_actions(
            namespace_id,
            IncarnationId::new(),
            now,
            now + Duration::minutes(5),
            requests.len() + 1,
        )
        .await
        .expect("claims the due actions");
    assert_eq!(
        delivered
            .iter()
            .map(|claimed| claimed.action.action_id)
            .collect::<BTreeSet<_>>(),
        requests.keys().copied().collect::<BTreeSet<_>>()
    );
    for claimed in delivered {
        let request = &requests[&claimed.action.action_id];
        assert!(
            &claimed.action.request_data == request,
            "the claim read {} bytes of a {}-byte request",
            claimed.action.request_data.len(),
            request.len()
        );
        match repo
            .begin_action_attempt(&claimed.claim, now)
            .await
            .expect("begins the attempt")
        {
            WorkerComputeActionAttemptStart::Started(action) => assert!(
                &action.request_data == request,
                "the attempt read {} bytes of a {}-byte request",
                action.request_data.len(),
                request.len()
            ),
            other => panic!("the attempt began as {other:?}"),
        }
        assert_eq!(
            repo.finalize_action(
                &claimed.claim,
                WorkerComputeActionFinalization::Delivered { completed_at: now },
            )
            .await
            .expect("finalizes the action"),
            WorkerComputeActionFinalizeResult::Applied {
                status: WorkerComputeProviderActionStatus::Delivered
            }
        );
    }
    requests
}

#[tokio::test]
async fn memory_nodes_over_the_part_size_read_back() {
    nodes_over_the_part_size_read_back(&InMemoryChasmNodeStore::new()).await;
}

#[tokio::test]
async fn memory_requests_over_the_part_size_read_back() {
    requests_over_the_part_size_read_back(&InMemoryWorkerComputeRepository::default()).await;
}

#[tokio::test]
async fn memory_batches_over_the_part_size_read_back() {
    batches_over_the_part_size_read_back(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn memory_a_state_over_the_part_size_loads_back() {
    a_state_over_the_part_size_loads_back(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn memory_a_reset_copies_values_over_the_part_size() {
    a_reset_copies_values_over_the_part_size(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn memory_deletion_of_a_run_with_split_values() {
    deletion_of_a_run_with_split_values(&InMemoryStore::default()).await;
}
