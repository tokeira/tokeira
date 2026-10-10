//! Shared contracts for reading one history event (`activity-inputs-from-history`),
//! run against the in-memory store in the default suite and against an ephemeral
//! Aurora DSQL cluster in the live suite.
//!
//! An activity start reads its ActivityTaskScheduled event with
//! [`RunRepository::read_history_event`], one lookup that returns the batch holding
//! the event. Each case reads every event of a history, and the ids around it, one
//! at a time, so an event that opens a batch, one in its middle and one that closes
//! it are all read.

use proptest::prelude::*;
use tokeira_types::{RunId, RunKey};

use crate::{
    InMemoryStore, RunRepository,
    bulk_write_tests::{Backend, commit_base, materialize, signal, timer},
};

/// Read each event of `run_key`'s history by its id, and the ids just outside it:
/// each id finds exactly its event, and an id the history doesn't hold finds none.
pub(crate) async fn assert_each_event_reads(backend: &impl Backend, run_key: RunKey) {
    let history = backend
        .repo()
        .read_history_to_end(run_key, 0)
        .await
        .expect("reads the history");
    let last = history.last().map_or(0, |event| event.event_id);
    for event_id in -1..=last + 2 {
        let expected = usize::try_from(event_id - 1)
            .ok()
            .and_then(|index| history.get(index))
            .cloned();
        assert_eq!(
            backend
                .repo()
                .read_history_event(run_key, event_id)
                .await
                .expect("reads the event"),
            expected,
            "event {event_id} of a history of {last}"
        );
    }
}

/// Batches of one event and of several, written a commit at a time.
pub(crate) async fn read_from_batches_of_any_size(backend: &impl Backend) {
    let chunks = vec![
        vec![signal(0, 16)],
        (1..4).map(timer).collect(),
        vec![signal(1, 16)],
        (4..9).map(timer).collect(),
    ];
    let (base, _) = commit_base(backend, chunks).await;
    assert_each_event_reads(backend, base.run_key).await;
}

/// A reset successor's copied history, cut into batches of up to 512 KiB that each
/// hold several events.
pub(crate) async fn read_from_a_reset_successors_copied_batches(backend: &impl Backend) {
    let chunks = (0..12).map(|index| vec![signal(index, 100_000)]).collect();
    let (base, fork) = commit_base(backend, chunks).await;
    let successor_run_id = RunId::new();
    materialize(backend, &base, fork, successor_run_id, Some(base.run_key))
        .await
        .expect("materializes the successor");
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
    let batches = backend
        .history_batches(successor)
        .await
        .expect("lists the batches");
    assert!(
        batches.len() > 1 && batches.iter().any(|batch| batch.events > 2),
        "the copy is cut into batches of several events: {batches:?}"
    );
    assert_each_event_reads(backend, successor).await;
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

#[tokio::test]
async fn memory_read_from_batches_of_any_size() {
    read_from_batches_of_any_size(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn memory_read_from_a_reset_successors_copied_batches() {
    read_from_a_reset_successors_copied_batches(&InMemoryStore::default()).await;
}

#[tokio::test]
async fn memory_read_of_a_run_without_history_finds_nothing() {
    let store = InMemoryStore::default();
    assert_eq!(
        store
            .read_history_event(RunKey::new(), 1)
            .await
            .expect("reads"),
        None
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // Feature: activity-inputs-from-history, Property 3: One lookup finds the
    // scheduled event
    #[test]
    fn property_one_lookup_finds_each_event(
        cuts in prop::collection::vec(1usize..6, 0..8),
        reset in any::<bool>(),
    ) {
        block_on(async {
            let store = InMemoryStore::default();
            let chunks = cuts
                .iter()
                .enumerate()
                .map(|(chunk, &events)| (0..events).map(|index| timer(chunk * 8 + index)).collect())
                .collect();
            let (base, fork) = commit_base(&store, chunks).await;
            assert_each_event_reads(&store, base.run_key).await;
            if reset {
                let successor_run_id = RunId::new();
                materialize(&store, &base, fork, successor_run_id, Some(base.run_key))
                    .await
                    .expect("materializes the successor");
                let successor =
                    RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
                assert_each_event_reads(&store, successor).await;
            }
        });
    }
}
