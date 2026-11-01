//! Explicitly selected live adapters for the shared semantic storage contracts.
//! SQL below only inspects/cleans owned fixtures; all tested reads and writes use
//! the production repository. Missing live configuration is an error, not a pass.

use std::{path::Path, sync::Arc};

use proptest::prelude::*;
use sqlx::{PgPool, Row, ValueRef, postgres::PgPoolOptions};

use super::*;
use crate::{
    LeaseRepository, ProjectionLog,
    contract_tests::{
        self as contract, ContractBackend, FixtureScope, Rows, SHARD_COUNT, history, recovery,
    },
    dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig},
};

// Every column is inspected, including request provenance and existing row values.
// The names are constants, while every fixture identity is a bound parameter.
const TABLES: &[(&str, &str)] = &[
    ("workflow_hot", "run_key"),
    ("history_batch", "run_key"),
    ("request_dedupe", "run_key"),
    ("current_execution", "run_key"),
    ("timer_bucket", "run_key"),
    ("activity_dispatch", "run_key"),
    ("workflow_dispatch", "run_key"),
    ("dispatch_backlog", "run_key"),
    ("run_value_part", "run_key"),
    ("projection_log", "run_key"),
    ("projection_image_part", "run_key"),
    ("run_operation", "source_run_key"),
    ("run_intent", "source_run_key"),
    ("run_intent_part", "source_run_key"),
    ("outbound_retained_run", "run_key"),
    ("outbound_parent_reference", "child_run_key"),
    ("run_creation_claim", "run_key"),
];

#[derive(Clone)]
struct Fixture {
    pool: PgPool,
    store: Arc<DsqlStore>,
}

impl Fixture {
    async fn connect() -> Result<Self> {
        let url = contract::required_url(std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL").ok())?;
        let pool = PgPoolOptions::new()
            .min_connections(2)
            .max_connections(2)
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
                    ..Default::default()
                },
                shard_count: SHARD_COUNT,
                projection_partition_count: 16,
                conflict_policy: CurrentExecutionConflictPolicy::Reject,
                ..Default::default()
            },
        )
        .await?;
        store.migration_runner().apply(&pool).await?;
        crate::prepare_execution_placement(store.run_repository()).await?;
        Ok(Self {
            pool,
            store: Arc::new(store),
        })
    }

    async fn stop(&self) -> Result<()> {
        let result = self.store.connection_director().shutdown().await;
        self.pool.close().await;
        result
    }
}

fn finish(result: Result<()>, stop: Result<()>) -> Result<()> {
    match (result, stop) {
        (Err(error), Err(stop)) => {
            Err(error.context(format!("fixture shutdown also failed: {stop:#}")))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}

#[async_trait]
impl ContractBackend for Fixture {
    fn repo(&self) -> &dyn RunRepository {
        self.store.run_repository()
    }
    fn log(&self) -> &dyn ProjectionLog {
        self.store.projection_log()
    }
    fn leases(&self) -> &dyn LeaseRepository {
        self.store.run_repository()
    }

    async fn rows(&self, scope: &FixtureScope) -> Result<Rows> {
        let mut observed = Rows::new();
        let keys: Vec<_> = scope.states.iter().map(|state| state.run_key.0).collect();
        for &(table, column) in TABLES {
            let mut values = Vec::new();
            {
                let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT * FROM ");
                query
                    .push(table)
                    .push(" WHERE ")
                    .push(column)
                    .push(" = ANY(")
                    .push_bind(&keys)
                    .push(")");
                let rows = query.build().fetch_all(&self.pool).await?;
                for row in rows {
                    let columns = (0..row.len())
                        .map(|index| {
                            let value = row.try_get_raw(index)?;
                            Ok(if value.is_null() {
                                None
                            } else {
                                Some(
                                    value
                                        .as_bytes()
                                        .map_err(|error| anyhow!("{error}"))?
                                        .to_vec(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    values.push(format!("{columns:?}"));
                }
            }
            values.sort();
            observed.insert(table.to_owned(), values);
        }
        Ok(observed)
    }

    async fn cleanup(&self, scope: &FixtureScope) -> Result<()> {
        let keys: Vec<_> = scope.states.iter().map(|state| state.run_key.0).collect();
        for &(table, column) in TABLES {
            {
                let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("DELETE FROM ");
                query
                    .push(table)
                    .push(" WHERE ")
                    .push(column)
                    .push(" = ANY(")
                    .push_bind(&keys)
                    .push(")");
                query.build().execute(&self.pool).await?;
            }
        }
        anyhow::ensure!(
            self.rows(scope).await?.values().all(Vec::is_empty),
            "owned DSQL rows survived cleanup"
        );
        Ok(())
    }

    async fn remove_hot(&self, key: RunKey) -> Result<()> {
        sqlx::query("DELETE FROM workflow_hot WHERE run_key = $1")
            .bind(key.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn expire_lease(&self, home: ShardId) -> Result<()> {
        sqlx::query(
            "UPDATE shard_lease SET lease_expiry = now() - interval '1 second' WHERE shard_id = $1",
        )
        .bind(DsqlRunRepository::shard_id_to_uuid(home))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn clear_lease(&self, home: ShardId, original_owner: &str) -> Result<()> {
        let removed = sqlx::query(
            "DELETE FROM shard_lease WHERE shard_id = $1 AND (owner = $2 OR owner = $3)",
        )
        .bind(DsqlRunRepository::shard_id_to_uuid(home))
        .bind(original_owner)
        .bind(format!("{original_owner}-new"))
        .execute(&self.pool)
        .await?;
        anyhow::ensure!(
            removed.rows_affected() == 1,
            "owned lease cleanup did not remove one row"
        );
        Ok(())
    }

    async fn seed_image(&self, image: &ProjectionRecord) -> Result<()> {
        sqlx::query("INSERT INTO projection_log (partition_id, fanout, run_key, transition_seq, context_data, ops_data, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(i32::try_from(image.partition_id)?).bind(i16::try_from(image.fanout)?).bind(image.run_key.0).bind(i64::try_from(image.transition_seq.0)?)
            .bind(codec::encode_projection_context(&image.context)?).bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA).bind(contract::now()).execute(&self.pool).await?;
        Ok(())
    }

    fn seeded_image_time(&self) -> Option<OffsetDateTime> {
        Some(contract::now())
    }

    async fn workflow_row(&self, key: RunKey) -> Result<Option<crate::WorkflowDispatchRow>> {
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
}

macro_rules! shared_property {
    ($name:ident, $property:expr, $strategy:path, $case:path) => {
        #[test]
        #[ignore = "requires an ephemeral Aurora DSQL database"]
        fn $name() -> Result<()> {
            let runtime = tokio::runtime::Runtime::new()?;
            let fixture = runtime.block_on(Fixture::connect())?;
            let result =
                contract::run_cases(stringify!($name), "dsql", $property, $strategy(), |case| {
                    runtime.block_on(Box::pin($case(&fixture, case)))
                });
            let stop = runtime.block_on(fixture.stop());
            finish(result, stop)
        }
    };
}

shared_property!(
    storage_contract_live_commit_outcomes,
    5,
    contract::commits::strategy,
    contract::commits::outcomes
);
shared_property!(
    storage_contract_live_competing_commits,
    6,
    contract::commits::strategy,
    contract::commits::competing
);
shared_property!(
    storage_contract_live_lease_fence,
    7,
    contract::commits::strategy,
    contract::commits::fence
);

shared_property!(
    storage_contract_live_projection_pages,
    3,
    contract::projection::strategy,
    contract::projection::pages
);
shared_property!(
    storage_contract_live_projection_deletion,
    4,
    contract::projection::strategy,
    contract::projection::deletion
);

#[test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
fn storage_contract_live_history() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime.block_on(Fixture::connect())?;
    let result = contract::run_cases(
        "storage_contract_live_history",
        "dsql",
        1,
        history::strategy(),
        |case| runtime.block_on(history::case(&fixture, case)),
    )
    .and_then(|()| runtime.block_on(history::boundaries(&fixture)));
    finish(result, runtime.block_on(fixture.stop()))
}

#[test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
fn storage_contract_live_recovery() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime.block_on(Fixture::connect())?;
    let result = contract::run_cases(
        "storage_contract_live_recovery",
        "dsql",
        2,
        recovery::strategy(),
        |case| runtime.block_on(recovery::case(&fixture, case)),
    );
    finish(result, runtime.block_on(fixture.stop()))
}

// Feature: storage-contract-tests, Property 8: DSQL recovery resumes across legacy phases
#[test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
fn storage_contract_live_legacy_recovery() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime.block_on(Fixture::connect())?;
    let result = contract::run_cases(
        "storage_contract_live_legacy_recovery",
        "dsql",
        8,
        (0usize..6, 0usize..6, 0usize..5, any::<bool>()),
        |(legacy, flagged, limit, change)| {
            runtime.block_on(async {
                let mut scope = FixtureScope::new(legacy + flagged);
                let mut legacy_indices = Vec::new();
                let mut flagged_indices = Vec::new();
                for i in 0..scope.states.len() {
                    scope.place(i, contract::HOMES[0]);
                    scope.states[i].pending_workflow_task = None;
                    scope.states[i].workflow_run_timeout = Some(Duration::hours(1));
                    let is_legacy = legacy_indices.len() < legacy
                        && (i % 2 == 1 || flagged_indices.len() == flagged);
                    if is_legacy {
                        legacy_indices.push(i);
                    } else {
                        flagged_indices.push(i);
                    }
                }
                contract::scoped(&fixture, &scope, async {
                    for (i, state) in scope.states.iter().enumerate() {
                        contract::apply(&fixture, contract::start(state)).await?;
                        if legacy_indices.contains(&i) {
                            sqlx::query(
                                "UPDATE workflow_hot SET recovery_needed = NULL WHERE run_key = $1",
                            )
                            .bind(state.run_key.0)
                            .execute(&fixture.pool)
                            .await?;
                        }
                    }
                    let first = fixture
                        .repo()
                        .list_recovery_candidates_for_shard(contract::HOMES[0], None, limit)
                        .await?;
                    if change && !legacy_indices.is_empty() {
                        let changed = legacy_indices[0];
                        sqlx::query(
                            "UPDATE workflow_hot SET recovery_needed = true WHERE run_key = $1",
                        )
                        .bind(scope.key(changed).0)
                        .execute(&fixture.pool)
                        .await?;
                        flagged_indices.push(changed);
                        flagged_indices.sort_unstable();
                    }
                    let mut actual = first.states;
                    if let Some(cursor) = first.next {
                        actual.extend(recovery::walk(&fixture, Some(cursor), limit).await?);
                    }
                    let expected: Vec<_> = legacy_indices
                        .iter()
                        .chain(&flagged_indices)
                        .map(|&index| scope.states[index].clone())
                        .collect();
                    contract::equal(
                        &actual,
                        &expected,
                        "legacy-before-flagged recovery, including permitted revisit",
                    )
                })
                .await
            })
        },
    );
    finish(result, runtime.block_on(fixture.stop()))
}

#[tokio::test]
#[ignore = "requires an ephemeral Aurora DSQL database"]
async fn storage_contract_live_limit_binding() -> Result<()> {
    let fixture = Fixture::connect().await?;
    let mut scope = FixtureScope::new(1);
    scope.place(0, contract::HOMES[0]);
    let result = contract::scoped(&fixture, &scope, async {
        contract::apply(&fixture, contract::start(&scope.states[0])).await?;
        let before = contract::observe(&fixture, &scope).await?;
        let projection = fixture.log().read_page(3, 1, None, usize::MAX).await;
        anyhow::ensure!(
            projection
                .as_ref()
                .is_err_and(|error| error.to_string().contains("projection read limit")),
            "expected diagnostic projection limit refusal: {projection:?}"
        );
        let recovery = fixture
            .repo()
            .list_recovery_candidates_for_shard(contract::HOMES[0], None, usize::MAX)
            .await;
        anyhow::ensure!(
            recovery
                .as_ref()
                .is_err_and(|error| error.downcast_ref::<std::num::TryFromIntError>().is_some()),
            "expected recovery limit conversion refusal: {recovery:?}"
        );
        contract::equal(
            &fixture
                .repo()
                .read_history(scope.key(0), 0, usize::MAX)
                .await?,
            &Vec::new(),
            "history uses a fixed SQL batch limit",
        )?;
        let wrong_home = fixture
            .repo()
            .commit_transition_for_bundle(
                scope.key(0),
                contract::HOMES[1],
                crate::memory::projection_accumulator_tests::following(&scope.states[0]),
                WriteFence::Lease(crate::ExecutionHomeFence {
                    home: contract::HOMES[0],
                    owner: "fixture".into(),
                    epoch: ShardEpoch(1),
                }),
            )
            .await;
        anyhow::ensure!(wrong_home.is_err(), "mismatched explicit home accepted");
        contract::equal(
            &contract::observe(&fixture, &scope).await?,
            &before,
            "refused bindings or home changed storage",
        )
    })
    .await;
    finish(result, fixture.stop().await)
}
