#![cfg(feature = "dsql-integration")]

//! Opt-in real-DSQL recovery from the minimal V001 migration prefix.
//!
//! The test never resets a database. It mutates only an explicitly acknowledged
//! database whose current schema contains no relations, so an interrupted run requires
//! a new disposable database rather than an automated destructive cleanup.

use std::time::{Duration as StdDuration, Instant};

use anyhow::{Context as _, Result, ensure};
use sqlx::{Connection as _, PgConnection, Row as _};
use time::Duration;
use tokeira_storage::dsql::{
    ConnectionControlLeaseRepository, ControlLeaseAcquireOutcome, ControlLeaseAcquireRequest,
    ControlLeaseClusterIdentity, MigrationRunner, OwnershipAdmissionGate, SchemaDecision,
    SchemaMigrationPolicy,
};

const DISPOSABLE_ACKNOWLEDGEMENT: &str = "MUTATE_DISPOSABLE_EMPTY_DATABASE";

#[tokio::test]
#[ignore = "mutates an explicitly acknowledged disposable DSQL database; set TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_DATABASE_URL and TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_ACK"]
async fn partial_v001_prefix_converges_through_the_embedded_target() -> Result<()> {
    let database_url = std::env::var("TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_DATABASE_URL").context(
        "TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_DATABASE_URL must name a disposable database",
    )?;
    let acknowledgement = std::env::var("TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_ACK")
        .context("TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_ACK must be set")?;
    ensure!(
        acknowledgement == DISPOSABLE_ACKNOWLEDGEMENT,
        "TOKEIRA_DSQL_SCHEMA_BOOTSTRAP_TEST_ACK must equal {DISPOSABLE_ACKNOWLEDGEMENT}"
    );

    let mut connection = PgConnection::connect(&database_url).await?;
    ensure_current_schema_is_empty(&mut connection).await?;

    let runner = MigrationRunner::embedded();
    let migration_plan = runner.dry_run()?;
    let v1 = migration_plan
        .iter()
        .find(|migration| migration.version == 1)
        .context("embedded migration plan must contain V001")?;
    sqlx::query(&v1.sql).execute(&mut connection).await?;
    sqlx::query(
        "INSERT INTO schema_version (version, name, checksum, applied_at) \
         VALUES ($1, $2, $3, now())",
    )
    .bind(i32::try_from(v1.version)?)
    .bind(&v1.name)
    .bind(&v1.checksum)
    .execute(&mut connection)
    .await?;

    let contract = MigrationRunner::compatibility_contract();
    let decision = runner
        .assess_connection(&mut connection, &contract, SchemaMigrationPolicy::Automatic)
        .await?;
    assert_eq!(
        decision,
        SchemaDecision::Migrate {
            from: 1,
            to: contract.target_version,
        }
    );

    runner
        .bootstrap_migration_coordination(&mut connection, &decision)
        .await?;
    let leases = ConnectionControlLeaseRepository::new();
    leases.bootstrap(&mut connection).await?;
    let migration_guard = leases
        .acquire(
            &mut connection,
            &ControlLeaseAcquireRequest {
                claim_name: "schema-migration".to_owned(),
                cluster: ControlLeaseClusterIdentity {
                    cluster_id: "schema-bootstrap-integration".to_owned(),
                    cluster_arn:
                        "arn:aws:dsql:eu-west-2:000000000000:cluster/schema-bootstrap-integration"
                            .to_owned(),
                },
                owner_id: format!("schema-bootstrap-{}", uuid::Uuid::new_v4()),
                lease_duration: Duration::minutes(5),
                admission_margin: Duration::seconds(20),
                acquire_deadline: Instant::now() + StdDuration::from_secs(30),
            },
        )
        .await?;
    assert_eq!(migration_guard.outcome(), ControlLeaseAcquireOutcome::Clean);
    let migration_gate = OwnershipAdmissionGate::for_guard(&migration_guard);
    let application = runner
        .apply_decision(&mut connection, &decision, &migration_guard)
        .await;
    let release = leases
        .release(&mut connection, &migration_guard, &migration_gate)
        .await;
    application?;
    release?;

    verify_target_boundary(&mut connection, &migration_plan).await?;
    assert_eq!(
        runner
            .assess_connection(
                &mut connection,
                &contract,
                SchemaMigrationPolicy::ValidateOnly,
            )
            .await?,
        SchemaDecision::Compatible {
            current: contract.target_version,
            legacy_backfill: false,
        }
    );
    Ok(())
}

async fn ensure_current_schema_is_empty(connection: &mut PgConnection) -> Result<()> {
    let relations = sqlx::query(
        "SELECT table_name, table_type FROM information_schema.tables \
         WHERE table_schema = current_schema() ORDER BY table_name",
    )
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        relations.is_empty(),
        "refusing to mutate a disposable test database with {} existing user-schema relations",
        relations.len()
    );
    Ok(())
}

async fn verify_target_boundary(
    connection: &mut PgConnection,
    migration_plan: &[tokeira_storage::dsql::MigrationPlan],
) -> Result<()> {
    let contract = MigrationRunner::compatibility_contract();
    let applied =
        sqlx::query("SELECT version, name, checksum FROM schema_version ORDER BY version")
            .fetch_all(&mut *connection)
            .await?;
    let expected = migration_plan
        .iter()
        .filter(|migration| migration.version <= contract.target_version)
        .collect::<Vec<_>>();
    assert_eq!(applied.len(), expected.len());
    for (row, migration) in applied.iter().zip(expected) {
        assert_eq!(
            u32::try_from(row.try_get::<i32, _>("version")?)?,
            migration.version
        );
        assert_eq!(row.try_get::<String, _>("name")?, migration.name);
        assert_eq!(row.try_get::<String, _>("checksum")?, migration.checksum);
    }

    let (schema_version, digest) = sqlx::query_as::<_, (i32, String)>(
        "SELECT schema_version, migration_set_digest FROM schema_compatibility \
         ORDER BY schema_version DESC LIMIT 1",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(u32::try_from(schema_version)?, contract.target_version);
    assert_eq!(digest, contract.migration_set_digest);
    Ok(())
}
