//! Query plans for live suites outside this crate.
//!
//! A live suite that records the SQL the engine sent plans each statement's
//! text as it was sent, `$n` parameters included: Aurora DSQL accepts
//! `EXPLAIN (GENERIC_PLAN)`, PostgreSQL 16's plan for unknown parameter values,
//! so no argument has to be rebuilt. Nothing in a release calls this: the module
//! compiles only for tests and with `dsql-integration`.

use sqlx::{AssertSqlSafe, PgConnection, Row};

/// The generic plan of `sql`, one line per row, with output columns and
/// qualified names (`VERBOSE`).
///
/// `sql` must be a statement this engine sent; it is planned, not executed.
/// The plan goes over the simple-query protocol: the extended protocol would
/// prepare the `EXPLAIN` itself, whose `$n` then ask for values to bind. A
/// statement whose parameter types the planner can't infer from context is
/// refused with the planner's error.
pub async fn generic_plan(connection: &mut PgConnection, sql: &str) -> sqlx::Result<Vec<String>> {
    // SQL safety: `sql` is a statement the engine already sent, recorded by a
    // live suite; `EXPLAIN` without `ANALYZE` plans it and never runs it, and
    // this module never compiles into a release.
    sqlx::raw_sql(AssertSqlSafe(format!(
        "EXPLAIN (GENERIC_PLAN, VERBOSE) {sql}"
    )))
    .fetch_all(connection)
    .await?
    .iter()
    .map(|row| row.try_get::<String, _>(0))
    .collect()
}
