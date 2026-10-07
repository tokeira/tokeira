//! Opt-in shared contracts against an operator-owned ephemeral Aurora DSQL
//! cluster. No connection occurs unless the dedicated test URL is provided.

use std::path::Path;

use sqlx::{PgPool, postgres::PgPoolOptions};

use super::*;
use crate::{
    WorkflowDispatchRow,
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
    workflow_dispatch_tests::{
        Backend, ordered_pages, reset_materialization, routing_and_home_pages, run_atomic_cases,
    },
};

struct Fixture {
    pool: PgPool,
    store: DsqlStore,
}

impl Fixture {
    async fn connect() -> Result<Self> {
        let url = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .min_connections(4)
            .idle_timeout(None)
            .connect(&url)
            .await?;
        let store = DsqlStore::from_database_url_for_tests(
            url,
            DsqlPoolConfig {
                migration: MigrationConfig {
                    migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
                },
                reservoir: ReservoirConfig {
                    target_ready: 4,
                    inflight_limit: 2,
                    ..ReservoirConfig::default()
                },
                shard_count: 1,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..DsqlPoolConfig::default()
            },
        )
        .await?;
        // A fresh cluster can change its catalog while ASYNC indexes finish.
        // Retry only the existing migration conflict outcome, leaving the runner
        // and its separately owned retry policy unchanged.
        for attempt in 0..20 {
            match store.migration_runner().apply(&pool).await {
                Ok(_) => break,
                Err(error)
                    if attempt < 19
                        && error
                            .downcast_ref::<sqlx::Error>()
                            .and_then(|error| error.as_database_error())
                            .and_then(|error| error.code())
                            .is_some_and(|code| matches!(code.as_ref(), "OC001" | "40001")) => {}
                Err(error) => return Err(error),
            }
        }
        let jobs = sqlx::query_as::<_, (String, String)>(
            "SELECT job_id, status FROM sys.jobs WHERE job_type = 'INDEX_BUILD'",
        )
        .fetch_all(&pool)
        .await?;
        for (job, status) in jobs {
            if status == "submitted" || status == "processing" {
                anyhow::ensure!(
                    sqlx::query_scalar::<_, bool>("CALL sys.wait_for_job($1)")
                        .bind(job)
                        .fetch_one(&pool)
                        .await?,
                    "index build did not complete"
                );
            } else {
                anyhow::ensure!(status == "completed", "index build failed: {status}");
            }
        }
        Ok(Self { pool, store })
    }
}

#[async_trait]
impl Backend for Fixture {
    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }

    async fn row(&self, key: RunKey) -> Result<Option<WorkflowDispatchRow>> {
        let mut query =
            sqlx::QueryBuilder::<sqlx::Postgres>::new(super::workflow_dispatch::SELECT_ROW);
        query.push(" WHERE run_key=").push_bind(key.0);
        query
            .build()
            .fetch_optional(&self.pool)
            .await?
            .map(super::workflow_dispatch::decode)
            .transpose()
    }

    async fn seed_stale_row(&self, state: &WorkflowState) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        super::workflow_dispatch::maintain(&mut tx, state, ShardId(0)).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[test]
fn workflow_dispatch_live_atomic_reference_traces() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::connect()).unwrap();
    run_atomic_cases(&fixture, &runtime);
    runtime.block_on(fixture.store.shutdown()).unwrap();
    runtime.block_on(fixture.pool.close());
}

#[tokio::test]
async fn workflow_dispatch_live_ordered_pages() {
    if std::env::var_os("TOKEIRA_DSQL_TEST_DATABASE_URL").is_none() {
        return;
    }
    let fixture = Fixture::connect().await.unwrap();
    ordered_pages(&fixture).await;
    routing_and_home_pages(&fixture).await;
    reset_materialization(&fixture).await;
    digest_collision(&fixture).await.unwrap();
    fixture.store.shutdown().await.unwrap();
    fixture.pool.close().await;
}

async fn digest_collision(fixture: &Fixture) -> Result<()> {
    let mut wanted = crate::memory::projection_accumulator_tests::fresh_transition(RunKey::new());
    wanted.next_state.priority = Some(tokeira_kernel::Priority {
        priority_key: 5,
        fairness_key: String::new(),
        fairness_weight: 1.0,
    });
    let range = crate::WorkflowDiscoveryRange {
        namespace_id: wanted.next_state.namespace_id,
        queue_name: wanted.next_state.task_queue.clone(),
        routing: crate::WorkflowDispatchRouting::Live,
    };
    let mut collision = wanted.clone();
    collision.next_state.run_key = RunKey::new();
    collision.next_state.workflow_id.0 = "collision".into();
    collision.next_state.task_queue.0 = "different-queue".into();
    collision
        .next_state
        .priority
        .as_mut()
        .expect("fixture has priority")
        .priority_key = 1;
    let collision_key = collision.next_state.run_key;
    crate::workflow_dispatch_tests::commit(fixture, wanted).await?;
    crate::workflow_dispatch_tests::commit(fixture, collision).await?;
    // Force a lookup collision while keeping authoritative raw coordinates.
    sqlx::query("UPDATE workflow_dispatch SET queue_key=$1 WHERE run_key=$2")
        .bind(&range.lookup_keys()[0])
        .bind(collision_key.0)
        .execute(&fixture.pool)
        .await?;
    let limit = std::num::NonZeroU32::new(1).expect("one is nonzero");
    let page = fixture
        .repo()
        .list_workflow_dispatch_page(&range, None, limit)
        .await?;
    assert!(!range.matches(&page.candidates[0]));
    let next = fixture
        .repo()
        .list_workflow_dispatch_page(&range, page.last_examined, limit)
        .await?;
    assert!(range.matches(&next.candidates[0]));
    Ok(())
}
