#![cfg(feature = "dsql-integration")]

//! Opt-in real-DSQL check that migrating to this release's target drops the
//! `activity_state` table an earlier release kept, under both runner paths
//! (`activity-state-drop`).
//!
//! The test needs an explicitly acknowledged, empty, disposable database. It
//! migrates it to the target an earlier release reached, fills `activity_state`
//! past DSQL's per-transaction row limit, migrates it with the plain runner path
//! that `tkr schema setup` uses, replays the drop under the embedded engine's
//! automatic path, and finally rebuilds an earlier release's table for the
//! automatic path to drop. It never resets the database.

use std::{
    path::PathBuf,
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Context as _, Result, ensure};
use sqlx::{Connection as _, PgConnection};
use time::Duration;
use tokeira_storage::dsql::{
    ConnectionControlLeaseRepository, ControlLeaseAcquireRequest, ControlLeaseClusterIdentity,
    MigrationConfig, MigrationRunner, OwnershipAdmissionGate, SchemaDecision,
    SchemaMigrationPolicy,
};

const DISPOSABLE_ACKNOWLEDGEMENT: &str = "MUTATE_DISPOSABLE_EMPTY_DATABASE";
const ROWS: i64 = 4_000;
/// Under DSQL's limit of 3,000 rows a transaction modifies.
const ROWS_PER_BATCH: i64 = 1_000;
const INDEXES: [&str; 3] = [
    "idx_activity_state_shard",
    "idx_activity_state_queue",
    "idx_activity_state_run_activity",
];

#[tokio::test]
#[ignore = "mutates an explicitly acknowledged disposable DSQL database; set TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_DATABASE_URL and TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_ACK"]
async fn migrating_to_the_target_drops_an_earlier_releases_activity_state() -> Result<()> {
    let database_url = std::env::var("TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_DATABASE_URL")
        .context(
            "TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_DATABASE_URL must name a disposable database",
        )?;
    let acknowledgement = std::env::var("TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_ACK")
        .context("TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_ACK must be set")?;
    ensure!(
        acknowledgement == DISPOSABLE_ACKNOWLEDGEMENT,
        "TOKEIRA_DSQL_ACTIVITY_STATE_DROP_TEST_ACK must equal {DISPOSABLE_ACKNOWLEDGEMENT}"
    );
    let mut connection = PgConnection::connect(&database_url).await?;
    let relations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema()",
    )
    .fetch_one(&mut connection)
    .await?;
    ensure!(
        relations == 0,
        "the database must be empty; it holds {relations} relations"
    );

    // The plain path, from empty, to the target an earlier release reached:
    // every migration before the drop.
    let earlier = migrations_before_the_drop()?;
    MigrationRunner::new(MigrationConfig {
        migrations_dir: earlier.clone(),
    })
    .apply_connection(&mut connection)
    .await?;
    std::fs::remove_dir_all(&earlier)?;
    ensure!(table_and_indexes(&mut connection).await? == (true, INDEXES.len()));
    fill_activity_state(&mut connection).await?;

    // The plain path to this release's target drops the filled table.
    let runner = MigrationRunner::embedded();
    runner.apply_connection(&mut connection).await?;
    assert_eq!(
        table_and_indexes(&mut connection).await?,
        (false, 0),
        "the target keeps no activity_state table or index"
    );
    let contract = MigrationRunner::compatibility_contract();
    let compatible = SchemaDecision::Compatible {
        current: contract.target_version,
        legacy_backfill: false,
    };
    assert_eq!(
        runner
            .assess_connection(
                &mut connection,
                &contract,
                SchemaMigrationPolicy::ValidateOnly
            )
            .await?,
        compatible
    );

    // The automatic path replays a drop whose ledger row was lost.
    let drop_version = drop_version(&runner)?;
    forget_from(&mut connection, drop_version).await?;
    migrate_automatically(&mut connection, &runner, drop_version).await?;
    assert_eq!(table_and_indexes(&mut connection).await?, (false, 0));
    assert_eq!(
        runner
            .assess_connection(
                &mut connection,
                &contract,
                SchemaMigrationPolicy::ValidateOnly
            )
            .await?,
        compatible
    );

    // The automatic path drops an earlier release's table, rows and indexes.
    forget_from(&mut connection, drop_version).await?;
    rebuild_activity_state(&mut connection).await?;
    fill_activity_state(&mut connection).await?;
    migrate_automatically(&mut connection, &runner, drop_version).await?;
    assert_eq!(table_and_indexes(&mut connection).await?, (false, 0));
    assert_eq!(
        runner
            .assess_connection(
                &mut connection,
                &contract,
                SchemaMigrationPolicy::ValidateOnly
            )
            .await?,
        compatible
    );
    Ok(())
}

fn migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

/// The version of the migration that drops `activity_state`.
fn drop_version(runner: &MigrationRunner) -> Result<u32> {
    runner
        .dry_run()?
        .into_iter()
        .find(|plan| plan.name == "drop_activity_state")
        .map(|plan| plan.version)
        .context("the release embeds a migration that drops activity_state")
}

/// A directory holding every migration before the one that drops
/// `activity_state`, or all of them when there is none.
fn migrations_before_the_drop() -> Result<PathBuf> {
    let directory = std::env::temp_dir().join(format!(
        "tokeira-activity-state-drop-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&directory)?;
    let mut files: Vec<_> = std::fs::read_dir(migrations_dir())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<_, _>>()?;
    files.sort();
    for file in files {
        if file.to_string_lossy().contains("__drop_activity_state") {
            break;
        }
        std::fs::copy(migrations_dir().join(&file), directory.join(&file))?;
    }
    Ok(directory)
}

/// Whether `activity_state` exists, and how many of its three indexes do.
async fn table_and_indexes(connection: &mut PgConnection) -> Result<(bool, usize)> {
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = 'activity_state'",
    )
    .fetch_one(&mut *connection)
    .await?;
    let indexes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE indexname = ANY($1)")
            .bind(INDEXES.as_slice())
            .fetch_one(&mut *connection)
            .await?;
    Ok((tables > 0, usize::try_from(indexes)?))
}

/// Write `ROWS` rows, in transactions of `ROWS_PER_BATCH`.
async fn fill_activity_state(connection: &mut PgConnection) -> Result<()> {
    for batch in 0..ROWS / ROWS_PER_BATCH {
        let mut transaction = connection.begin().await?;
        sqlx::query(
            "INSERT INTO activity_state \
             (run_key, schedule_event_id, shard_id, activity_id, queue_namespace, queue_name, attempt, state_data) \
             SELECT gen_random_uuid(), n, gen_random_uuid(), 'activity-' || n, gen_random_uuid(), 'queue', 1, '\\x00'::bytea \
             FROM generate_series($1::bigint, $2::bigint) AS n",
        )
        .bind(batch * ROWS_PER_BATCH + 1)
        .bind((batch + 1) * ROWS_PER_BATCH)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
    }
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM activity_state")
        .fetch_one(&mut *connection)
        .await?;
    ensure!(rows == ROWS, "activity_state holds {rows} rows, not {ROWS}");
    Ok(())
}

/// Forget `version` and every later one, ledger rows and compatibility records,
/// as a crash between a migration's statement and its ledger row leaves it.
async fn forget_from(connection: &mut PgConnection, version: u32) -> Result<()> {
    let version = i32::try_from(version)?;
    sqlx::query("DELETE FROM schema_version WHERE version >= $1")
        .bind(version)
        .execute(&mut *connection)
        .await?;
    sqlx::query("DELETE FROM schema_compatibility WHERE schema_version >= $1")
        .bind(version)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Recreate `activity_state` and its three indexes from the migrations that
/// built them, and wait for the index builds.
async fn rebuild_activity_state(connection: &mut PgConnection) -> Result<()> {
    for file in [
        "V007__activity_state.sql",
        "V014__idx_activity_state_shard.sql",
        "V015__idx_activity_state_queue.sql",
        "V021__idx_activity_state_run_activity.sql",
    ] {
        let sql = std::fs::read_to_string(migrations_dir().join(file))?;
        let mut transaction = connection.begin().await?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
    }
    let deadline = Instant::now() + StdDuration::from_secs(600);
    while table_and_indexes(connection).await? != (true, INDEXES.len())
        || building_indexes(connection).await? > 0
    {
        ensure!(
            Instant::now() < deadline,
            "activity_state's index builds never finished"
        );
        tokio::time::sleep(StdDuration::from_secs(5)).await;
    }
    Ok(())
}

async fn building_indexes(connection: &mut PgConnection) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM sys.jobs WHERE job_type = 'INDEX_BUILD' \
         AND status NOT IN ('completed', 'failed', 'cancelled')",
    )
    .fetch_one(&mut *connection)
    .await?)
}

/// Migrate from the version before the drop to the target under the automatic
/// policy, as the embedded engine does: under the schema-migration claim.
async fn migrate_automatically(
    connection: &mut PgConnection,
    runner: &MigrationRunner,
    drop_version: u32,
) -> Result<()> {
    let contract = MigrationRunner::compatibility_contract();
    let decision = runner
        .assess_connection(connection, &contract, SchemaMigrationPolicy::Automatic)
        .await?;
    assert_eq!(
        decision,
        SchemaDecision::Migrate {
            from: drop_version - 1,
            to: contract.target_version,
        }
    );
    runner
        .bootstrap_migration_coordination(connection, &decision)
        .await?;
    let leases = ConnectionControlLeaseRepository::new();
    leases.bootstrap(connection).await?;
    let mut guard = leases
        .acquire(
            connection,
            &ControlLeaseAcquireRequest {
                claim_name: "schema-migration".to_owned(),
                cluster: ControlLeaseClusterIdentity {
                    cluster_id: "activity-state-drop-integration".to_owned(),
                    cluster_arn:
                        "arn:aws:dsql:eu-west-2:000000000000:cluster/activity-state-drop-integration"
                            .to_owned(),
                },
                owner_id: format!("activity-state-drop-{}", uuid::Uuid::new_v4()),
                lease_duration: Duration::minutes(5),
                admission_margin: Duration::seconds(20),
                acquire_deadline: Instant::now() + StdDuration::from_secs(30),
            },
        )
        .await?;
    let gate = OwnershipAdmissionGate::for_guard(&guard);
    let application = runner
        .apply_decision(connection, &decision, &leases, &mut guard, &gate)
        .await;
    let release = leases.release(connection, &guard, &gate).await;
    application?;
    release?;
    Ok(())
}
