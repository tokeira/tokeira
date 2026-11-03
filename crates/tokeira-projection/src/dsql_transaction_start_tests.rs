//! Visibility transaction startup against an explicitly configured ephemeral DSQL
//! database. A single reservoir slot makes the next write depend on safe return.

use std::{
    path::Path,
    task::{Context, Waker},
    time::Duration,
};

use anyhow::{Context as _, Result, ensure};
use sqlx::postgres::PgPoolOptions;
use tokeira_storage::dsql::{
    DsqlConnectionDirector, DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig,
};
use tokeira_types::{ExecutionStatus, NamespaceId, RunKey};
use uuid::Uuid;

use super::{cursor_predicate, tests::live_image};
use crate::{
    ApplyOutcome, CompiledFilter, DsqlVisibilityStore, ExecutionRow, PageBounds, PageToken,
    ProjectionSink, SortOrder, VisibilityStore,
};

async fn wait_for_ready_connection(director: &DsqlConnectionDirector) -> Result<()> {
    // Distributed checkout is nonblocking; observe return/refill before borrowing.
    tokio::time::timeout(Duration::from_secs(30), async {
        while director.ready_connections() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("single reservoir connection did not become ready")
}

#[tokio::test]
#[ignore = "requires an explicitly configured ephemeral DSQL database"]
async fn dsql_cancelled_visibility_start_preserves_next_write() -> Result<()> {
    let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")
        .context("set TOKEIRA_DSQL_TEST_DATABASE_URL to an ephemeral DSQL database")?;
    ensure!(!url.trim().is_empty(), "DSQL test database URL is empty");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .connect(&url)
        .await?;
    let config = DsqlPoolConfig {
        reservoir: ReservoirConfig {
            target_ready: 1,
            inflight_limit: 1,
            ..Default::default()
        },
        migration: MigrationConfig {
            migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../tokeira-storage/migrations"),
        },
        ..Default::default()
    };
    let dsql = DsqlStore::from_database_url_for_tests(url, config).await?;
    let store = DsqlVisibilityStore::new(dsql.connection_director_arc());
    let director = dsql.connection_director();
    let namespace_id = NamespaceId(Uuid::new_v4());
    let run_key = RunKey(Uuid::new_v4());
    let image = live_image(run_key, namespace_id, 1, ExecutionStatus::Running);
    let result = async {
        dsql.migration_runner().apply(&pool).await?;
        let filter = CompiledFilter::default();
        let page = PageBounds {
            limit: 1,
            after: Some(PageToken {
                close_time: None,
                start_time: image.context.start_time,
                run_key,
                sort_order: SortOrder::StartTimeAsc,
            }),
        };
        ensure!(cursor_predicate(SortOrder::Default, page.after.as_ref(), 2).is_err());
        ensure!(director.reservoir_target() == 1);
        ensure!(director.ready_connections() == 1);
        {
            // A mismatched cursor fails synchronously after BEGIN, before any
            // SELECT. Once checkout consumes the ready slot, Pending can only
            // be SQLx startup: a completed BEGIN would return that cursor error.
            let mut read =
                store.list_component_executions(namespace_id, &filter, SortOrder::Default, &page);
            ensure!(
                read.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "visibility BEGIN did not suspend on its first poll"
            );
            ensure!(
                director.ready_connections() == 0,
                "visibility read suspended before checking out the connection"
            );
            drop(read);
        }
        wait_for_ready_connection(director).await?;
        ensure!(store.apply(&image, 0).await? == ApplyOutcome::Advanced);
        wait_for_ready_connection(director).await?;
        let stored = store
            .get_row(run_key)
            .await
            .context("committed visibility row was not readable")?;
        ensure!(stored == ExecutionRow::from_projection_record(&image));
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let shutdown = dsql.shutdown().await;
    let cleanup = async {
        for (table, delete, count) in [
            (
                "execution_visibility_current",
                "DELETE FROM execution_visibility_current WHERE namespace_id = $1",
                "SELECT COUNT(*) FROM execution_visibility_current WHERE namespace_id = $1",
            ),
            (
                "execution_visibility_rollup",
                "DELETE FROM execution_visibility_rollup WHERE namespace_id = $1",
                "SELECT COUNT(*) FROM execution_visibility_rollup WHERE namespace_id = $1",
            ),
        ] {
            sqlx::query(delete)
                .bind(namespace_id.0)
                .execute(&pool)
                .await?;
            let remaining: i64 = sqlx::query_scalar(count)
                .bind(namespace_id.0)
                .fetch_one(&pool)
                .await?;
            ensure!(
                remaining == 0,
                "visibility fixture was not cleaned from {table}"
            );
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    pool.close().await;
    [("store shutdown", shutdown), ("fixture cleanup", cleanup)]
        .into_iter()
        .fold(result, |result, (step, cleanup)| match (result, cleanup) {
            (Err(error), Err(cleanup)) => {
                Err(error.context(format!("{step} also failed: {cleanup:#}")))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
            _ => Ok(()),
        })
}
