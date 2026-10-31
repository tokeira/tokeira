//! Operator-gated pair transaction contracts. No connection is opened without
//! TOKEIRA_DSQL_TEST_DATABASE_URL; run only on the live suite's disposable cluster.

use super::{bulk_write_tests::Fixture, *};
use crate::{
    PairCommitResult,
    atomic_successors_test_support::{pair, start},
    memory::projection_accumulator_tests::applied,
};
use std::collections::BTreeMap;
use tokeira_kernel::{HistoryEventKind, limits::RunGrowthLimits};
use tokeira_types::{Headers, Memo, Payload, Payloads};

async fn seed(repo: &dyn RunRepository) -> Result<WorkflowState> {
    let start = start();
    Ok(applied(
        repo.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced)
            .await?,
    ))
}

async fn assert_committed(fixture: &Fixture, closed: RunKey, successor: RunKey) -> Result<()> {
    let repo = fixture.store.run_repository();
    assert!(
        matches!(repo.load_run(closed).await?, LoadedRun::Existing(state) if !state.status.is_open())
    );
    let LoadedRun::Existing(state) = repo.load_run(successor).await? else {
        panic!("pair committed without successor workflow_hot row")
    };
    assert!(state.status.is_open());
    assert!(
        matches!(
            repo.read_history(successor, 0, 1)
                .await?
                .first()
                .map(|event| &event.kind),
            Some(
                HistoryEventKind::WorkflowExecutionStarted { .. }
                    | HistoryEventKind::WorkflowExecutionStartedV2 { .. }
            )
        ),
        "pair committed without successor started event"
    );
    let pointer = sqlx::query_as::<_, (Uuid, bool)>(
        "SELECT run_key, is_open FROM current_execution WHERE key = $1",
    )
    .bind(DsqlRunRepository::current_execution_key(
        state.namespace_id,
        &state.workflow_id,
    ))
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(pointer, (successor.0, true));
    Ok(())
}

#[tokio::test]
async fn atomic_successors_live() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let repo = fixture.store.run_repository();
    for kind in 0..4 {
        let state = seed(repo).await?;
        let (closing, successor) = pair(&state, kind, Payloads::default(), Memo::default(), None);
        let key = successor.next_state.run_key;
        assert!(matches!(
            repo.commit_transition_pair(
                state.run_key,
                closing,
                key,
                successor,
                WriteFence::Unfenced
            )
            .await?,
            PairCommitResult::Applied { .. }
        ));
        assert_committed(&fixture, state.run_key, key).await?;
    }

    let state = seed(repo).await?;
    let before = repo.read_history(state.run_key, 0, 100).await?;
    let (closing, mut successor) = pair(&state, 0, Payloads::default(), Memo::default(), None);
    // Seed the occupied key in an unrelated workflow, so the original pointer
    // still names the closing run when the successor-absence check rejects it.
    let occupied = seed(repo).await?;
    successor.next_state.run_key = occupied.run_key;
    assert!(matches!(
        repo.commit_transition_pair(
            state.run_key,
            closing,
            occupied.run_key,
            successor,
            WriteFence::Unfenced
        )
        .await?,
        PairCommitResult::Conflict { .. }
    ));
    assert_eq!(
        repo.load_run(state.run_key).await?,
        LoadedRun::Existing(state.clone())
    );
    assert_eq!(repo.read_history(state.run_key, 0, 100).await?, before);
    assert_eq!(
        repo.find_latest_run(state.namespace_id, &state.workflow_id)
            .await?,
        Some(state.run_key)
    );

    let state = seed(repo).await?;
    let before = repo.read_history(state.run_key, 0, 100).await?;
    let (closing, successor) = pair(&state, 2, Payloads::default(), Memo::default(), None);
    let key = successor.next_state.run_key;
    let replacement = RunKey::new();
    sqlx::query("UPDATE current_execution SET run_key = $1 WHERE key = $2")
        .bind(replacement.0)
        .bind(DsqlRunRepository::current_execution_key(
            state.namespace_id,
            &state.workflow_id,
        ))
        .execute(&fixture.pool)
        .await?;
    assert!(
        matches!(
            repo.commit_transition_pair(
                state.run_key,
                closing,
                key,
                successor,
                WriteFence::Unfenced
            )
            .await?,
            PairCommitResult::Conflict { .. }
        ),
        "pair overwrote another run's pointer"
    );
    assert_eq!(
        repo.load_run(state.run_key).await?,
        LoadedRun::Existing(state.clone())
    );
    assert_eq!(repo.load_run(key).await?, LoadedRun::Absent);
    assert_eq!(repo.read_history(state.run_key, 0, 100).await?, before);
    assert_eq!(
        repo.find_latest_run(state.namespace_id, &state.workflow_id)
            .await?,
        Some(replacement)
    );

    // Each payload stays below 2 MiB; the close's history stays below 4 MiB.
    // The successor repeats the memo in hot state and its projection, pushing
    // the combined write set beyond 10 MiB while each single run fits.
    let state = seed(repo).await?;
    let input = Payloads(vec![Payload::new(vec![0x51; 1536 * 1024])]);
    let memo = Memo(BTreeMap::from([(
        "memo".into(),
        Payload::new(vec![0x72; 1792 * 1024]),
    )]));
    let header = Some(Headers(BTreeMap::from([(
        "header".into(),
        Payload::new(vec![0x93; 384 * 1024]),
    )])));
    let (mut closing, mut successor) = pair(&state, 0, input, memo, header);
    closing.growth_limits = Some(RunGrowthLimits::default());
    successor.growth_limits = Some(RunGrowthLimits::default());
    let key = successor.next_state.run_key;
    assert_eq!(
        repo.commit_transition_pair(
            state.run_key,
            closing.clone(),
            key,
            successor.clone(),
            WriteFence::Unfenced
        )
        .await?,
        PairCommitResult::SizeRefused
    );
    assert_eq!(
        repo.load_run(state.run_key).await?,
        LoadedRun::Existing(state.clone())
    );
    assert_eq!(repo.load_run(key).await?, LoadedRun::Absent);
    assert!(matches!(
        repo.commit_transition(state.run_key, closing, WriteFence::Unfenced)
            .await?,
        CommitResult::Applied { .. }
    ));
    assert!(matches!(
        repo.commit_transition(key, successor, WriteFence::Unfenced)
            .await?,
        CommitResult::Applied { .. }
    ));
    assert_committed(&fixture, state.run_key, key).await?;
    Ok(())
}
