//! Shared contracts for `update-refusals` criterion 2.8, run against the in-memory
//! store in the default suite and against an ephemeral Aurora DSQL cluster in the
//! live suite: a run whose state an earlier release wrote, without the ids of its
//! completed updates, recovers them from its history when it loads, if it is open,
//! and keeps them from its next commit on.

use std::collections::BTreeSet;

use proptest::prelude::*;
use tokeira_kernel::{HistoryEventKind, LoadedRun, UpdateEventOutcome, WorkflowState};
use tokeira_types::{ExecutionStatus, Payload, Payloads, RunKey};

use crate::{
    InMemoryStore,
    bulk_write_tests::{Backend, commit, with_events},
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

/// Each update's acceptance and completion, as its history records them.
fn completions(update_ids: &BTreeSet<String>) -> Vec<HistoryEventKind> {
    update_ids
        .iter()
        .enumerate()
        .flat_map(|(index, update_id)| {
            [
                HistoryEventKind::WorkflowExecutionUpdateAccepted {
                    update_id: update_id.clone(),
                    update_name: "handler".into(),
                    input: Payloads::default(),
                    accepted_request_sequencing_event_id: 1,
                },
                HistoryEventKind::WorkflowExecutionUpdateCompletedV2 {
                    update_id: update_id.clone(),
                    update_name: "handler".into(),
                    accepted_event_id: 3 + 2 * i64::try_from(index).expect("few updates"),
                    outcome: UpdateEventOutcome::Success(Payloads(vec![Payload::new(
                        b"result".to_vec(),
                    )])),
                },
            ]
        })
        .collect()
}

/// Commit a run whose history records the completion of each of `update_ids`,
/// under a state that counts them but holds none of their ids, as an earlier
/// release wrote it: open, or closed. Returns the state as committed.
pub(crate) async fn run_from_before(
    backend: &impl Backend,
    update_ids: &BTreeSet<String>,
    open: bool,
) -> WorkflowState {
    let start = applied(commit(backend, fresh_transition(RunKey::new())).await);
    let first = start.last_event_id + 1;
    let mut transition = with_events(following(&start), first, completions(update_ids));
    transition.next_state.completed_update_count =
        u32::try_from(update_ids.len()).expect("few updates");
    transition.next_state.completed_updates.clear();
    if !open {
        transition.next_state.status = ExecutionStatus::Completed;
        transition.next_state.closed_at = Some(transition.next_state.started_at);
        transition.next_state.pending_workflow_task = None;
    }
    applied(commit(backend, transition).await)
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

// Feature: update-refusals, Property 5: A run from before this change recovers its ids
/// An open run from before this change loads with the ids of the completions its
/// history records, and keeps them once it commits again; a closed one loads as it
/// was written.
pub(crate) async fn a_run_from_before_recovers_its_completed_updates(
    backend: &impl Backend,
    update_ids: &BTreeSet<String>,
) {
    let open = run_from_before(backend, update_ids, true).await;
    let recovered = loaded(backend, open.run_key).await;
    assert_eq!(&recovered.completed_updates, update_ids);
    let next = applied(commit(backend, following(&recovered)).await);
    assert_eq!(&next.completed_updates, update_ids);
    assert_eq!(
        &loaded(backend, open.run_key).await.completed_updates,
        update_ids
    );

    let closed = run_from_before(backend, update_ids, false).await;
    assert!(
        loaded(backend, closed.run_key)
            .await
            .completed_updates
            .is_empty(),
        "a closed run needs no recovery"
    );
}

#[tokio::test]
async fn memory_a_run_from_before_recovers_its_completed_updates() {
    let update_ids = ["update-a", "update-b"].map(String::from).into();
    a_run_from_before_recovers_its_completed_updates(&InMemoryStore::default(), &update_ids).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // Feature: update-refusals, Property 5: A run from before this change recovers its ids
    #[test]
    fn memory_recovery_follows_any_history(
        update_ids in proptest::collection::btree_set("[a-z]{1,6}", 0..5),
    ) {
        let tokio = tokio::runtime::Runtime::new().expect("a tokio runtime");
        tokio.block_on(a_run_from_before_recovers_its_completed_updates(
            &InMemoryStore::default(),
            &update_ids,
        ));
    }
}
