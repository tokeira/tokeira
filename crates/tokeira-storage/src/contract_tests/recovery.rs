//! Recovery expectations use fixture eligibility, independent of recovery predicates.

use proptest::prelude::*;
use time::Duration;

use super::*;
use crate::RecoveryCursor;

#[derive(Clone, Debug)]
pub(crate) struct RecoveryCase {
    pub(crate) coordinates: Vec<(bool, bool)>,
    pub(crate) limit: usize,
}

pub(crate) fn strategy() -> impl Strategy<Value = RecoveryCase> {
    (
        prop::collection::vec((any::<bool>(), any::<bool>()), 3..12),
        0usize..6,
    )
        .prop_map(|(coordinates, limit)| RecoveryCase { coordinates, limit })
}

pub(crate) async fn walk(
    backend: &impl ContractBackend,
    mut cursor: Option<RecoveryCursor>,
    limit: usize,
) -> Result<Vec<WorkflowState>> {
    let mut all = Vec::new();
    let mut cursors = Vec::new();
    for _ in 0..100 {
        let page = backend
            .repo()
            .list_recovery_candidates_for_shard(HOMES[0], cursor.as_ref(), limit)
            .await?;
        ensure!(
            page.states.len() <= limit.max(1),
            "recovery page exceeded limit"
        );
        all.extend(page.states);
        let Some(next) = page.next else {
            return Ok(all);
        };
        ensure!(!cursors.contains(&next), "recovery cursor repeated");
        cursors.push(next.clone());
        cursor = Some(next);
    }
    anyhow::bail!("recovery never terminated")
}

// Feature: storage-contract-tests, Property 2: Current-format recovery traverses every eligible run
pub(crate) async fn case(backend: &impl ContractBackend, case: RecoveryCase) -> Result<()> {
    let mut scope = FixtureScope::new(case.coordinates.len());
    let mut expected = Vec::new();
    for (i, &(eligible, local)) in case.coordinates.iter().enumerate() {
        let eligible = eligible || i < 2;
        let local = local || i < 2;
        scope.place(i, HOMES[usize::from(!local)]);
        let state = &mut scope.states[i];
        state.pending_workflow_task = None;
        state.workflow_run_timeout = eligible.then(|| Duration::hours(1));
        state.workflow_execution_timeout = None;
        if eligible && local {
            expected.push(state.clone());
        }
    }
    scoped(backend, &scope, async {
        for state in &scope.states {
            apply(backend, start(state)).await?;
        }
        equal(
            &walk(backend, None, case.limit).await?,
            &expected,
            "complete current-format recovery",
        )?;
        let first = backend
            .repo()
            .list_recovery_candidates_for_shard(HOMES[0], None, 1)
            .await?;
        equal(&first.states, &expected[..1].to_vec(), "first candidate")?;
        let cursor = first.next.context("nonterminal first candidate")?;
        backend.remove_hot(expected[0].run_key).await?;
        equal(
            &walk(backend, Some(cursor), case.limit).await?,
            &expected[1..].to_vec(),
            "continuation after cursor deletion",
        )
    })
    .await
}
