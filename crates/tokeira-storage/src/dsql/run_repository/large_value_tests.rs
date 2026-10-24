//! The shared contracts for a run's values over the part size
//! (`large-value-parts`) against an operator-owned ephemeral Aurora DSQL
//! cluster, and the cases only DSQL can show: the parts a state leaves, reads
//! held against a purge or a commit, and the widest row the parts write. No
//! connection occurs unless the dedicated test URL is provided.

use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use tokio::runtime::Runtime;

use super::{
    bulk_write_tests::Fixture,
    pause_point::{self, PauseKey},
    *,
};
use crate::{
    bulk_write_tests::{Backend, OwnedRows, commit, delete_first, noise},
    dsql::value_parts::{self, PART_BYTES},
    large_value_tests::{
        a_reset_copies_values_over_the_part_size, a_state_over_the_part_size_loads_back,
        batches_over_the_part_size_read_back, closed_run_with_split_values,
        deletion_of_a_run_with_split_values, payloads,
    },
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

#[tokio::test]
async fn dsql_batches_over_the_part_size_read_back() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        batches_over_the_part_size_read_back(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_a_state_over_the_part_size_loads_back() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        a_state_over_the_part_size_loads_back(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_a_reset_copies_values_over_the_part_size() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        a_reset_copies_values_over_the_part_size(&fixture).await;
    }
    Ok(())
}

#[tokio::test]
async fn dsql_deletion_of_a_run_with_split_values() -> Result<()> {
    if let Some(fixture) = Fixture::connect().await? {
        deletion_of_a_run_with_split_values(&fixture).await;
    }
    Ok(())
}

/// Check that the run's row and `run_value_part` hold exactly `state`: its
/// parts join to its encoding, with no part beyond its count, and a state within
/// the part size is stored whole, with no count and no parts.
async fn assert_stored_exactly(fixture: &Fixture, state: &WorkflowState) -> Result<()> {
    let run_key = state.run_key;
    let (first, count) = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
        "SELECT state_data, state_parts FROM workflow_hot WHERE run_key = $1",
    )
    .bind(run_key.0)
    .fetch_one(&fixture.pool)
    .await?;
    let parts = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no, data FROM run_value_part
         WHERE run_key = $1 AND value_kind = 1 ORDER BY part_no",
    )
    .bind(run_key.0)
    .fetch_all(&fixture.pool)
    .await?;
    let encoded = codec::encode_workflow_state(state)?;
    assert_eq!(
        count.is_none() && parts.is_empty(),
        encoded.len() <= PART_BYTES,
        "a state of {} bytes",
        encoded.len()
    );
    let stored = value_parts::join(|| format!("the state of {run_key:?}"), first, count, parts)?;
    assert_eq!(stored, encoded);
    Ok(())
}

/// A state result's length: around a multiple of the part size, up to near the 8
/// MiB a run's state may hold.
fn result_len() -> impl Strategy<Value = usize> {
    (0usize..=8, -4_096isize..=4_096)
        .prop_map(|(parts, offset)| (parts * PART_BYTES).saturating_add_signed(offset))
        .prop_map(|len| len.min(7 * 1024 * 1024 + 512 * 1024))
}

// Feature: large-value-parts, Property 4: A replaced value leaves only its own parts
#[test]
fn dsql_a_state_leaves_only_its_own_parts() {
    let runtime = Runtime::new().expect("a tokio runtime");
    let Some(fixture) = runtime
        .block_on(Fixture::connect())
        .expect("the live fixture connects")
    else {
        return;
    };
    let result = TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..ProptestConfig::with_cases(8)
    })
    .run(
        &prop::collection::vec(result_len(), 1..6),
        |lens| -> TestCaseResult {
            runtime
                .block_on(async {
                    let mut state =
                        applied(commit(&fixture, fresh_transition(RunKey::new())).await);
                    assert_stored_exactly(&fixture, &state).await?;
                    for (index, len) in lens.into_iter().enumerate() {
                        let mut transition = following(&state);
                        transition.next_state.last_completion_result =
                            (len > 0).then(|| payloads(len, index as u64));
                        state = applied(commit(&fixture, transition).await);
                        assert_stored_exactly(&fixture, &state).await?;
                    }
                    anyhow::Ok(())
                })
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))
        },
    );
    result.expect("every write leaves exactly its state's parts");
}

/// A run's history, read while the run is purged: what the purge hasn't removed
/// yet, which is the end of `written`, each event whole.
async fn assert_reads_the_rest(
    repo: &DsqlRunRepository,
    run_key: RunKey,
    written: &[HistoryEvent],
) -> Result<Vec<HistoryEvent>> {
    let read = repo.read_history_to_end(run_key, 0).await?;
    assert!(
        written.ends_with(&read),
        "read {} of the {} events written, not the last of them",
        read.len(),
        written.len()
    );
    Ok(read)
}

// Feature: large-value-parts, Property 6: Removing a value removes its parts
/// A purge of a run with split values, held after each of its transactions while
/// the run's history is read. Each read returns the batches the purge hasn't
/// removed, each whole: a batch's parts go only after every batch row.
#[tokio::test]
async fn dsql_a_purge_held_after_each_transaction_leaves_whole_batches() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let (state, written) = closed_run_with_split_values(&fixture).await;
    let run_key = state.run_key;
    delete_first(&fixture, &state).await;

    let purge = repo.purge_run(run_key);
    tokio::pin!(purge);
    let (mut reached, mut resume) = pause_point::arm(PauseKey::Purge(run_key));
    let mut reads = Vec::new();
    loop {
        tokio::select! {
            result = &mut purge => {
                result?;
                break;
            }
            _ = &mut reached => {
                reads.push(assert_reads_the_rest(repo, run_key, &written).await?);
                // Hold the purge's next transaction, then let this one go on.
                let (next_reached, next_resume) = pause_point::arm(PauseKey::Purge(run_key));
                reached = next_reached;
                drop(std::mem::replace(&mut resume, next_resume));
            }
        }
    }
    // The request records went first, with every batch still there; the parts
    // went last, after every batch.
    assert_eq!(reads.first(), Some(&written));
    assert_eq!(reads.last(), Some(&Vec::new()));
    assert_eq!(fixture.owned_rows(run_key).await?, OwnedRows::default());
    Ok(())
}

/// A history read held between its first read of a page that holds split batches
/// and its read of the page with their parts, while a purge of the run runs to
/// its end. The read reads the page again with the parts in one snapshot, so it
/// returns each batch it returns whole.
#[tokio::test]
async fn dsql_a_history_read_held_before_its_parts_reads_whole_batches() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let (state, written) = closed_run_with_split_values(&fixture).await;
    let run_key = state.run_key;
    delete_first(&fixture, &state).await;

    let (reached, resume) = pause_point::arm(PauseKey::HistoryParts(run_key));
    let read = assert_reads_the_rest(repo, run_key, &written);
    tokio::pin!(read);
    tokio::select! {
        _ = reached => {}
        result = &mut read => panic!("the read met split batches without holding: {result:?}"),
    }
    repo.purge_run(run_key).await?;
    drop(resume);
    read.await?;
    Ok(())
}

/// A load held between its read of a split state's row and its read of the
/// state's parts, while a commit replaces the state with one of as many parts.
/// The load reads the row again with the parts in one snapshot, which begins
/// after the commit, so it returns the new state whole.
#[tokio::test]
async fn dsql_a_load_held_before_its_parts_reads_one_state() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    let mut start = fresh_transition(RunKey::new());
    start.next_state.last_completion_result = Some(payloads(2_500_000, 1));
    let state = applied(commit(&fixture, start).await);

    let (reached, resume) = pause_point::arm(PauseKey::StateParts(state.run_key));
    let load = repo.load_run(state.run_key);
    tokio::pin!(load);
    tokio::select! {
        _ = reached => {}
        result = &mut load => panic!("the load met a split state without holding: {result:?}"),
    }
    let mut replace = following(&state);
    replace.next_state.last_completion_result = Some(payloads(2_500_000, 2));
    let replaced = applied(commit(&fixture, replace).await);
    drop(resume);
    let LoadedRun::Existing(loaded) = load.await? else {
        panic!("the run exists");
    };
    assert_eq!(loaded, replaced);
    Ok(())
}

/// The widest row the parts write: a history batch whose events and principals
/// each hold a first part of the part size. DSQL commits it.
#[tokio::test]
async fn dsql_a_row_of_two_parts_commits() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let run_key = RunKey::new();
    let mut tx = fixture.pool.begin().await?;
    sqlx::query(
        "INSERT INTO history_batch
         (run_key, first_event_id, last_event_id, transition_seq, events_data, events_parts,
          principals_data, principals_parts, created_at)
         VALUES ($1, 1, 1, 1, $2, 2, $3, 2, now())",
    )
    .bind(run_key.0)
    .bind(noise(PART_BYTES, 1))
    .bind(noise(PART_BYTES, 2))
    .execute(&mut *tx)
    .await?;
    tx.commit()
        .await
        .expect("DSQL commits a row of two parts of the part size");
    sqlx::query("DELETE FROM history_batch WHERE run_key = $1")
        .bind(run_key.0)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}
