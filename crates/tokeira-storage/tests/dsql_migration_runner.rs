#![cfg(feature = "dsql-integration")]

//! Opt-in real-DSQL run of the plain migration runner paths (`migration-runner-occ-retry`).
//!
//! On a newly created, empty database, one path migrates the full embedded corpus in a
//! single run, then the other path finds nothing to apply. `MigrationRunner::apply_connection`
//! is what `tkr schema setup` uses; `MigrationRunner::apply` takes a pool. A schema
//! conflict (OC001) can't be provoked on demand, so the unit tests in `dsql::migration`
//! carry the retry; this test proves the rewired paths end to end, including their waits
//! for index builds. It never resets a database and refuses one that isn't empty.

use anyhow::{Context as _, Result, bail, ensure};
use sqlx::{Connection as _, PgConnection, postgres::PgPoolOptions};
use tokeira_storage::dsql::MigrationRunner;

const DISPOSABLE_ACKNOWLEDGEMENT: &str = "MIGRATE_DISPOSABLE_EMPTY_DATABASE";

#[tokio::test]
#[ignore = "mutates an explicitly acknowledged disposable DSQL database; set TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_DATABASE_URL and TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK"]
async fn plain_runner_paths_migrate_a_new_database() -> Result<()> {
    let database_url = std::env::var("TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_DATABASE_URL").context(
        "TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_DATABASE_URL must name a disposable database",
    )?;
    let acknowledgement = std::env::var("TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK")
        .context("TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK must be set")?;
    ensure!(
        acknowledgement == DISPOSABLE_ACKNOWLEDGEMENT,
        "TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_ACK must equal {DISPOSABLE_ACKNOWLEDGEMENT}"
    );
    // `connection` (the default) migrates through `apply_connection` and checks with
    // `apply`; `pool` does the reverse. Run each on its own new database.
    let first = std::env::var("TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_FIRST")
        .unwrap_or_else(|_| "connection".to_owned());

    let mut connection = PgConnection::connect(&database_url).await?;
    ensure_current_schema_is_empty(&mut connection).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;

    let runner = MigrationRunner::embedded();
    let planned = runner.dry_run()?.len();
    let (migrated, repeated) = match first.as_str() {
        "connection" => {
            let migrated = runner.apply_connection(&mut connection).await?;
            let repeated = runner.apply(&pool).await?;
            (migrated, repeated)
        }
        "pool" => {
            let migrated = runner.apply(&pool).await?;
            let repeated = runner.apply_connection(&mut connection).await?;
            (migrated, repeated)
        }
        other => bail!(
            "TOKEIRA_DSQL_MIGRATION_RUNNER_TEST_FIRST must be connection or pool, not {other}"
        ),
    };
    ensure!(
        migrated.applied == planned,
        "the {first} path applied {} of {planned} migrations",
        migrated.applied
    );
    ensure!(
        repeated.applied == 0,
        "a second run applied {} migrations",
        repeated.applied
    );

    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM schema_version")
        .fetch_one(&mut connection)
        .await?;
    ensure!(
        usize::try_from(recorded)? == planned,
        "schema_version records {recorded} migrations, expected {planned}"
    );
    Ok(())
}

async fn ensure_current_schema_is_empty(connection: &mut PgConnection) -> Result<()> {
    let relations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema()",
    )
    .fetch_one(&mut *connection)
    .await?;
    ensure!(
        relations == 0,
        "refusing to migrate a disposable test database with {relations} existing user-schema relations"
    );
    Ok(())
}
