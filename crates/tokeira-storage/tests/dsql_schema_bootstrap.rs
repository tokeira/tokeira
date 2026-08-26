#![cfg(feature = "dsql-integration")]

//! Opt-in real-DSQL recovery from the observed token-zero bootstrap state.
//!
//! The test never resets a database. It mutates only an explicitly acknowledged
//! database whose current schema contains no relations, seeds the exact metadata left
//! by the failed startup, and requires a new disposable database after an interrupted
//! run rather than performing automated destructive cleanup.

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
async fn token_zero_empty_ledger_state_converges_through_the_embedded_target() -> Result<()> {
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
    let contract = MigrationRunner::compatibility_contract();
    let initial_decision = runner
        .assess_connection(&mut connection, &contract, SchemaMigrationPolicy::Automatic)
        .await?;
    assert_eq!(
        initial_decision,
        SchemaDecision::Initialize {
            target: contract.target_version,
        }
    );

    runner
        .bootstrap_migration_coordination(&mut connection, &initial_decision)
        .await?;
    let leases = ConnectionControlLeaseRepository::new();
    leases.bootstrap(&mut connection).await?;
    let cluster = ControlLeaseClusterIdentity {
        cluster_id: "schema-bootstrap-integration".to_owned(),
        cluster_arn: "arn:aws:dsql:eu-west-2:000000000000:cluster/schema-bootstrap-integration"
            .to_owned(),
    };
    sqlx::query(
        "INSERT INTO tokeira_control_lease \
         (claim_name, cluster_id, cluster_arn, owner_id, fence_token, expires_at, updated_at) \
         VALUES ('schema-migration', $1, $2, NULL, 0, now(), now())",
    )
    .bind(&cluster.cluster_id)
    .bind(&cluster.cluster_arn)
    .execute(&mut connection)
    .await?;
    verify_observed_partial_bootstrap_state(&mut connection).await?;

    // A restart re-observes the empty authoritative ledger and must initialize
    // through the pre-existing token-zero coordination row without manual repair.
    let restart_decision = runner
        .assess_connection(&mut connection, &contract, SchemaMigrationPolicy::Automatic)
        .await?;
    assert_eq!(restart_decision, initial_decision);
    let mut migration_guard = leases
        .acquire(
            &mut connection,
            &ControlLeaseAcquireRequest {
                claim_name: "schema-migration".to_owned(),
                cluster,
                owner_id: format!("schema-bootstrap-{}", uuid::Uuid::new_v4()),
                lease_duration: Duration::minutes(5),
                admission_margin: Duration::seconds(20),
                acquire_deadline: Instant::now() + StdDuration::from_secs(30),
            },
        )
        .await?;
    assert_eq!(migration_guard.outcome(), ControlLeaseAcquireOutcome::Clean);
    assert_eq!(migration_guard.fence_token(), 1);
    let migration_gate = OwnershipAdmissionGate::for_guard(&migration_guard);
    let application = runner
        .apply_decision(
            &mut connection,
            &restart_decision,
            &leases,
            &mut migration_guard,
            &migration_gate,
        )
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

async fn verify_observed_partial_bootstrap_state(connection: &mut PgConnection) -> Result<()> {
    let ledger_rows = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM schema_version")
        .fetch_one(&mut *connection)
        .await?;
    assert_eq!(ledger_rows, 0);

    let compatibility_relations = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = 'schema_compatibility'",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(compatibility_relations, 0);

    let (owner_id, fence_token, expired) = sqlx::query_as::<_, (Option<String>, i64, bool)>(
        "SELECT owner_id, fence_token, expires_at <= now() \
             FROM tokeira_control_lease WHERE claim_name = 'schema-migration'",
    )
    .fetch_one(&mut *connection)
    .await?;
    assert_eq!(owner_id, None);
    assert_eq!(fence_token, 0);
    assert!(expired);
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
