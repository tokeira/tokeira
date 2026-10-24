//! Shared contracts for a run's values over the part size (`large-value-parts`),
//! run against the in-memory store in the default suite and against an ephemeral
//! Aurora DSQL cluster in the live suite.
//!
//! DSQL refuses a value over 1 MiB, so on DSQL each case needs the run's values
//! stored in parts. The in-memory store stores every value whole, and each case
//! reads back what it wrote.

use tokeira_kernel::{HistoryEvent, HistoryEventKind, LoadedRun, RequestDedupeOp, WorkflowState};
use tokeira_types::{ExecutionStatus, Payload, Payloads, RequestId, RunKey};

use crate::{
    InMemoryStore,
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
