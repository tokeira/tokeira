//! `update-refusals` criterion 2.8 against an operator-owned ephemeral Aurora DSQL
//! cluster: an open run whose state an earlier release wrote recovers the ids of
//! its completed updates from the history in its load's snapshot, and a closed one
//! loads without reading its history. No connection occurs unless the dedicated
//! test URL is provided.

use std::collections::BTreeSet;

use proptest::{
    prelude::*,
    test_runner::{TestCaseResult, TestRunner},
};
use tokio::runtime::Runtime;

use super::{bulk_write_tests::Fixture, *};
use crate::{
    bulk_write_tests::Backend,
    update_refusals_tests::{
        a_run_from_before_recovers_its_completed_updates, loaded, run_from_before,
    },
};

// Feature: update-refusals, Property 5: A run from before this change recovers its ids
#[test]
fn dsql_a_run_from_before_recovers_its_completed_updates() {
    let runtime = Runtime::new().expect("a tokio runtime");
    let Some(fixture) = runtime
        .block_on(Fixture::connect())
        .expect("the live fixture connects")
    else {
        return;
    };
    let result = TestRunner::new(ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::with_cases(4)
    })
    .run(
        &proptest::collection::btree_set("[a-z]{1,6}", 0..5),
        |update_ids| -> TestCaseResult {
            runtime.block_on(a_run_from_before_recovers_its_completed_updates(
                &fixture,
                &update_ids,
            ));
            Ok(())
        },
    );
    result.expect("each run from before recovers its completed updates");
}

/// Replace the run's history batches with bytes no decoder takes.
async fn spoil_history(fixture: &Fixture, run_key: RunKey) -> Result<()> {
    sqlx::query("UPDATE history_batch SET events_data = $2 WHERE run_key = $1")
        .bind(run_key.0)
        .bind(vec![0xffu8; 4])
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

/// A closed run from before this change loads without reading its history: its
/// batches hold bytes no decoder takes, and the load still succeeds. An open one,
/// whose load recovers its ids from its history, fails on the same bytes.
#[tokio::test]
async fn dsql_a_closed_run_from_before_loads_without_its_history() -> Result<()> {
    let Some(fixture) = Fixture::connect().await? else {
        return Ok(());
    };
    let update_ids = BTreeSet::from(["update-a".to_string()]);
    let closed = run_from_before(&fixture, &update_ids, false).await;
    spoil_history(&fixture, closed.run_key).await?;
    assert!(
        loaded(&fixture, closed.run_key)
            .await
            .completed_updates
            .is_empty()
    );
    let open = run_from_before(&fixture, &update_ids, true).await;
    spoil_history(&fixture, open.run_key).await?;
    assert!(
        fixture.repo().load_run(open.run_key).await.is_err(),
        "an open run from before reads its history"
    );
    Ok(())
}
