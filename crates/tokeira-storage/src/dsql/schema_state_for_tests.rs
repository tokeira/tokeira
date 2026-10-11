//! Schema-ledger operations for live suites outside this crate.
//!
//! A node's schema check at distributed startup is tested live by putting the
//! ledger in the states the check must refuse, one migration behind the
//! release's target and one version past what the release reads, and by
//! showing that a refused start changed nothing. These functions do exactly
//! that and nothing in a release calls them: the module compiles only for
//! tests and with `dsql-integration`.

use sqlx::PgConnection;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    MigrationRunner, SchemaCompatibilityError, SchemaObservation,
    migration::read_schema_observation,
};

/// One `shard_lease` row: shard, owner, epoch and expiry.
pub type ShardLeaseRow = (Uuid, Option<String>, i64, OffsetDateTime);

/// What a refused start must leave as it found: the ledger, the newest
/// compatibility record and every shard lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaState {
    /// The ledger and the newest compatibility record.
    pub observation: SchemaObservation,
    /// Every shard lease, in shard order.
    pub shard_leases: Vec<ShardLeaseRow>,
}

/// Read the [`SchemaState`] without writing.
pub async fn state(connection: &mut PgConnection) -> Result<SchemaState, SchemaCompatibilityError> {
    Ok(SchemaState {
        observation: observe(connection).await?,
        shard_leases: shard_leases(connection).await?,
    })
}

/// Bring the schema to this release's target, whatever an earlier run left:
/// apply the release's migrations, which records any version an earlier run
/// forgot, then forget every version past what the release reads.
pub async fn bring_to_target(connection: &mut PgConnection) -> anyhow::Result<()> {
    MigrationRunner::embedded()
        .apply_connection(connection)
        .await?;
    let contract = MigrationRunner::compatibility_contract();
    forget_versions_from(connection, contract.maximum_readable_version + 1).await?;
    Ok(())
}

/// The ledger and the newest compatibility record, read as a schema
/// assessment reads them, without writing.
pub async fn observe(
    connection: &mut PgConnection,
) -> Result<SchemaObservation, SchemaCompatibilityError> {
    read_schema_observation(connection).await
}

/// Every shard lease, in shard order. A start that takes a lease, renews one
/// or releases one changes this.
pub async fn shard_leases(
    connection: &mut PgConnection,
) -> Result<Vec<ShardLeaseRow>, SchemaCompatibilityError> {
    Ok(sqlx::query_as::<_, ShardLeaseRow>(
        "SELECT shard_id, owner, epoch, lease_expiry FROM shard_lease ORDER BY shard_id",
    )
    .fetch_all(&mut *connection)
    .await?)
}

/// Forget `version` and every later version: their ledger rows and
/// compatibility records go, while the tables and indexes they built stay.
/// Applying the release's migrations again records them anew.
pub async fn forget_versions_from(
    connection: &mut PgConnection,
    version: u32,
) -> Result<(), SchemaCompatibilityError> {
    let version = ledger_version(version)?;
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

/// Record `version` as applied under a name and checksum no release has, as
/// a later release's migration appears to an earlier one.
pub async fn record_unknown_version(
    connection: &mut PgConnection,
    version: u32,
) -> Result<(), SchemaCompatibilityError> {
    sqlx::query(
        "INSERT INTO schema_version (version, name, checksum, applied_at) \
         VALUES ($1, 'later_release', 'later-release-checksum', now())",
    )
    .bind(ledger_version(version)?)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

fn ledger_version(version: u32) -> Result<i32, SchemaCompatibilityError> {
    i32::try_from(version).map_err(|error| {
        SchemaCompatibilityError::InvalidMetadata(format!(
            "schema version {version} is past the ledger's range: {error}"
        ))
    })
}
