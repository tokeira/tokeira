#![cfg(feature = "dsql-integration")]

//! Read-only observations of suite leftovers on shared DSQL test clusters.
//! A caller supplies the output path explicitly; no workflow or cluster identity
//! is written, and a fresh cluster can be observed before schema bootstrap.

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn dsql_shared_cluster_counts() -> Result<()> {
    let (Ok(url), Ok(output)) = (
        std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL"),
        std::env::var("TOKEIRA_DSQL_COUNTS_OUTPUT"),
    ) else {
        return Ok(());
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    let counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT
         (SELECT count(*) FROM workflow_hot
          WHERE shard_id='746f6b65-6972-612d-7368-617200000000'
            AND (recovery_needed=true OR recovery_needed IS NULL)),
         (SELECT count(*) FROM workflow_dispatch
          WHERE shard_id='746f6b65-6972-612d-7368-617200000000')",
    )
    .fetch_one(&pool)
    .await;
    pool.close().await;
    let result = match counts {
        Ok((candidates, dispatch)) => {
            format!("{{\"recovery_candidates\":{candidates},\"dispatch_rows\":{dispatch}}}")
        }
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            "{\"schema\":\"not bootstrapped\"}".to_owned()
        }
        Err(error) => return Err(error.into()),
    };
    std::fs::write(output, result)?;
    Ok(())
}
