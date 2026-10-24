#![cfg(feature = "dsql-integration")]

//! The embedded engine copies the visibility attribute index into its versioned layout
//! when it starts on an operator-supplied Aurora DSQL cluster (`bounded-visibility-apply`,
//! the embedded path). No connection occurs unless the live environment is set: the
//! test URL, and the cluster's identifier, ARN, endpoint and Region.

use std::path::Path;

use anyhow::{Context as _, Result, ensure};
use tokeira_config::{
    DsqlMigrationPolicy, EmbeddedDsqlLimits, EmbeddedEngineConfig, EmbeddedStorageConfig,
    ExistingEmbeddedDsqlConfig,
};
use tokeira_engine::{Engine, TokeiraConfig};
use tokeira_projection::SearchAttrType;
use tokeira_storage::dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig, codec};
use tokeira_types::SearchAttrValue;
use uuid::Uuid;

/// The production copy's progress row.
const COPY_NAME: &str = "visibility_attr_cell_copy";

struct LiveCluster {
    url: String,
    cluster_id: String,
    cluster_arn: String,
    endpoint: String,
    region: String,
}

impl LiveCluster {
    fn from_env() -> Option<Self> {
        let var = |name: &str| std::env::var(name).ok();
        Some(Self {
            url: var("TOKEIRA_DSQL_TEST_DATABASE_URL")?,
            cluster_id: var("TOKEIRA_DSQL_TEST_CLUSTER_ID")?,
            cluster_arn: var("TOKEIRA_DSQL_TEST_CLUSTER_ARN")?,
            endpoint: var("TOKEIRA_DSQL_TEST_ENDPOINT")?,
            region: var("TOKEIRA_DSQL_TEST_REGION")?,
        })
    }
}

#[tokio::test]
async fn the_embedded_engine_copies_the_attribute_index_when_it_starts() -> Result<()> {
    let Some(cluster) = LiveCluster::from_env() else {
        return Ok(());
    };
    // Bring the schema to this release's target, as an operator's upgrade would.
    let pool = sqlx::PgPool::connect(&cluster.url).await?;
    let store = DsqlStore::from_database_url_for_tests(
        cluster.url.clone(),
        DsqlPoolConfig {
            // nextest starts this test in its crate directory, not the workspace root.
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    store.migration_runner().apply(&pool).await?;
    store.shutdown().await?;

    // A run indexed the earlier way: its visibility row, and one old index row
    // carrying the attribute's whole value. The cluster's copy starts again.
    let namespace_id = Uuid::new_v4();
    let run_key = Uuid::new_v4();
    let attr_type = SearchAttrType::Keyword.to_db_smallint();
    let value = codec::encode(&SearchAttrValue::Keyword("embedded".to_owned()))?;
    sqlx::query(
        "INSERT INTO execution_visibility_current (namespace_id, archetype_id, run_key, \
         business_id, run_id, authority_epoch, source_transition_seq, status_keyword, \
         lifecycle_state, start_time, update_time) \
         VALUES ($1, 0, $2, 'embedded-copy', $3, 4, 9, 'Running', 0, now(), now())",
    )
    .bind(namespace_id)
    .bind(run_key)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO execution_visibility_attr_index (namespace_id, run_key, attr_id, \
         attr_type, value_discriminator, keyword_value, value_data) \
         VALUES ($1, $2, 42, $3, 'embedded', 'embedded', $4)",
    )
    .bind(namespace_id)
    .bind(run_key)
    .bind(attr_type)
    .bind(&value)
    .execute(&pool)
    .await?;
    sqlx::query("DELETE FROM execution_visibility_attr_copy WHERE name = $1")
        .bind(COPY_NAME)
        .execute(&pool)
        .await?;

    let engine = Engine::start_with_embedded_config(EmbeddedEngineConfig {
        server: TokeiraConfig::default(),
        storage: EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
            region: cluster.region.clone(),
            cluster_id: cluster.cluster_id.clone(),
            cluster_arn: cluster.cluster_arn.clone(),
            endpoint: cluster.endpoint.clone(),
            migration_policy: DsqlMigrationPolicy::ValidateOnly,
            limits: EmbeddedDsqlLimits::default(),
        }),
        startup_timeout_ms: 600_000,
    })
    .await
    .context("the embedded engine starts")?;
    let shutdown = engine.shutdown().await;

    let completed: Option<Option<time::OffsetDateTime>> = sqlx::query_scalar(
        "SELECT completed_at FROM execution_visibility_attr_copy WHERE name = $1",
    )
    .bind(COPY_NAME)
    .fetch_optional(&pool)
    .await?;
    let cells: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_visibility_attr_cell WHERE namespace_id = $1 \
         AND run_key = $2 AND authority_epoch = 4 AND source_transition_seq = 9 \
         AND attr_id = 42 AND keyword_value = 'embedded'",
    )
    .bind(namespace_id)
    .bind(run_key)
    .fetch_one(&pool)
    .await?;
    let values: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT value_data FROM execution_visibility_attr_value WHERE namespace_id = $1 \
         AND run_key = $2 AND authority_epoch = 4 AND source_transition_seq = 9 AND attr_id = 42",
    )
    .bind(namespace_id)
    .bind(run_key)
    .fetch_all(&pool)
    .await?;
    for statement in [
        "DELETE FROM execution_visibility_attr_cell WHERE namespace_id = $1",
        "DELETE FROM execution_visibility_attr_value WHERE namespace_id = $1",
        "DELETE FROM execution_visibility_attr_index WHERE namespace_id = $1",
        "DELETE FROM execution_visibility_current WHERE namespace_id = $1",
    ] {
        sqlx::query(statement)
            .bind(namespace_id)
            .execute(&pool)
            .await?;
    }
    pool.close().await;

    shutdown.context("the embedded engine shuts down")?;
    ensure!(
        completed.flatten().is_some(),
        "the embedded engine's startup completed the copy"
    );
    ensure!(
        cells == 1,
        "the old row is a cell at the run's version, not {cells}"
    );
    ensure!(
        values == vec![value],
        "the attribute's value is stored once at the run's version"
    );
    Ok(())
}
