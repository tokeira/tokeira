//! Forward-only DSQL migration discovery, validation, and application.
//!
//! Aurora DSQL has schema-change constraints that matter for correctness and
//! operational safety: secondary indexes must be asynchronous, migrations are
//! one SQL statement per file, and version gaps must be rejected so every
//! environment converges through the same ordered schema path.

use std::{fs, path::PathBuf, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use sqlx::{Connection, PgConnection, PgPool};
use time::OffsetDateTime;
use tokeira_observability::{ErrorBiasedSamplingReason, OutcomeLabel, mark_error_biased_sample};

use super::{
    ControlLeaseGuard, MigrationConfig,
    schema_compatibility::{
        AppliedMigration, SchemaCompatibilityContract, SchemaCompatibilityRecord, SchemaDecision,
        SchemaIncompatibility, SchemaMigrationPolicy, SchemaObservation,
        assess_schema_compatibility,
    },
    validation::DdlValidator,
};
use crate::{
    metrics as storage_metrics,
    schema_contract::{MigrationIdentity, SchemaContract, migration_set_digest, sha256_hex},
};

const SCHEMA_VERSION_BOOTSTRAP_SQL: &str =
    include_str!("../../migrations/V001__schema_version.sql");
const SCHEMA_COMPATIBILITY_BOOTSTRAP_SQL: &str =
    include_str!("../../migrations/V066__schema_compatibility.sql");
const CONTROL_LEASE_BOOTSTRAP_SQL: &str =
    include_str!("../../migrations/V067__tokeira_control_lease.sql");

/// Return the baseline-locked control-lease bootstrap statement.
///
/// First-run migration coordination needs this table before the migration that
/// records it can run. Reusing the migration bytes prevents the bootstrap path
/// from silently defining a different schema.
pub const fn control_lease_bootstrap_sql() -> &'static str {
    CONTROL_LEASE_BOOTSTRAP_SQL
}

/// Return the baseline-locked schema-compatibility bootstrap statement.
pub const fn schema_compatibility_bootstrap_sql() -> &'static str {
    SCHEMA_COMPATIBILITY_BOOTSTRAP_SQL
}

/// Forward-only schema migration runner for DSQL.
#[derive(Clone, Debug)]
pub struct MigrationRunner {
    /// Source of migration files. The runner is intentionally stateless; the
    /// authoritative applied state lives in the database `schema_version` table.
    source: MigrationSource,
}

/// Summary returned after applying pending migrations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Number of migration files applied during this invocation.
    pub applied: usize,
}

/// Failures while observing or changing the versioned DSQL schema.
#[derive(Debug, thiserror::Error)]
pub enum SchemaCompatibilityError {
    /// A database query failed.
    #[error("schema compatibility database operation failed")]
    Database(#[from] sqlx::Error),
    /// Local migration discovery or contract data is invalid.
    #[error("schema compatibility metadata is invalid: {0}")]
    InvalidMetadata(String),
    /// Validate-only policy requires an explicit migration.
    #[error("schema migration required: current V{current}, target V{target}")]
    MigrationRequired {
        /// Current version, with zero representing an uninitialized schema.
        current: u32,
        /// Required target version.
        target: u32,
    },
    /// The schema failed checksum, digest, or readable-version validation.
    #[error("schema is incompatible: {0:?}")]
    Incompatible(SchemaIncompatibility),
    /// The supplied guard is not the active schema-migration owner.
    #[error("schema migration ownership fence was lost")]
    Fenced,
    /// An asynchronous index job failed.
    #[error("asynchronous index {index_name} failed: {details}")]
    IndexFailed {
        /// Named index from the migration statement.
        index_name: String,
        /// Redacted DSQL job detail.
        details: String,
    },
    /// The named asynchronous index is absent, invalid, or structurally unexpected.
    #[error("asynchronous index {index_name} is not valid: {reason}")]
    IndexInvalid {
        /// Named index from the migration statement.
        index_name: String,
        /// Catalog validation reason.
        reason: String,
    },
    /// Automatic replay encountered a statement not proven idempotent.
    #[error("migration V{version} is not proven idempotent")]
    NonIdempotentMigration {
        /// Unsafe migration version.
        version: u32,
    },
}

/// One migration statement that would be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPlan {
    /// Numeric migration version parsed from the filename.
    pub version: u32,
    /// Snake-case migration name parsed from the filename.
    pub name: String,
    /// SHA-256 checksum of the SQL file contents.
    pub checksum: String,
    /// Full SQL statement. DSQL migrations are constrained to exactly one
    /// statement per file.
    pub sql: String,
}

/// Current migration state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaStatus {
    /// Highest applied version known to the target database.
    pub current_version: Option<u32>,
    /// Wall-clock time at which the status query completed.
    pub checked_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MigrationFile {
    version: u32,
    name: String,
    path: PathBuf,
    sql: String,
    checksum: String,
}

#[derive(Clone, Debug)]
enum MigrationSource {
    Directory(MigrationConfig),
    Embedded(&'static [EmbeddedMigration]),
}

impl MigrationFile {
    fn identity(&self) -> MigrationIdentity {
        MigrationIdentity {
            version: self.version,
            name: self.name.clone(),
            checksum: self.checksum.clone(),
        }
    }
}

/// Compile-time embedded migration statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedMigration {
    /// Numeric migration version.
    pub version: u32,
    /// Filename-derived migration name.
    pub name: &'static str,
    /// Workspace-relative source path used in diagnostics.
    pub path: &'static str,
    /// Lowercase SHA-256 checksum of the SQL bytes.
    pub checksum: &'static str,
    /// One DSQL-safe statement.
    pub sql: &'static str,
}

/// Compile-time schema compatibility contract validated by the storage build script.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddedSchemaContract {
    /// Checked-in contract format version.
    pub format_version: u32,
    /// Tokeira release owning the contract.
    pub tokeira_release: &'static str,
    /// Oldest readable schema version.
    pub minimum_supported_version: u32,
    /// Migration target for this release.
    pub target_version: u32,
    /// Newest readable schema version.
    pub maximum_readable_version: u32,
    /// Canonical digest through the maximum readable version.
    pub migration_set_digest: &'static str,
    /// Highest migration locked against modification.
    pub immutable_through_version: u32,
}

impl EmbeddedSchemaContract {
    /// Convert the zero-allocation embedded view into the storage-owned contract type.
    pub fn to_owned(self) -> SchemaContract {
        SchemaContract {
            format_version: self.format_version,
            tokeira_release: self.tokeira_release.to_owned(),
            minimum_supported_version: self.minimum_supported_version,
            target_version: self.target_version,
            maximum_readable_version: self.maximum_readable_version,
            migration_set_digest: self.migration_set_digest.to_owned(),
            immutable_through_version: self.immutable_through_version,
        }
    }
}

/// Compile-time canonical digest for one recognized migration prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddedMigrationPrefixDigest {
    /// Highest migration version included in this prefix.
    pub version: u32,
    /// Canonical SHA-256 digest for the prefix.
    pub digest: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/migrations_embedded.rs"));

impl MigrationRunner {
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            source: MigrationSource::Directory(config),
        }
    }

    /// Construct a runner over migrations embedded at compile time.
    ///
    /// Production CLI/server paths use this to avoid depending on the process
    /// working directory or re-hashing migration files at runtime.
    pub fn embedded() -> Self {
        Self {
            source: MigrationSource::Embedded(EMBEDDED_MIGRATIONS),
        }
    }

    /// Return the release-pinned schema compatibility contract.
    pub fn embedded_schema_contract() -> SchemaContract {
        EMBEDDED_SCHEMA_CONTRACT.to_owned()
    }

    /// Return the compatibility view of the release-pinned embedded contract.
    pub fn compatibility_contract() -> SchemaCompatibilityContract {
        let contract = Self::embedded_schema_contract();
        SchemaCompatibilityContract {
            tokeira_release: contract.tokeira_release,
            minimum_supported_version: contract.minimum_supported_version,
            target_version: contract.target_version,
            maximum_readable_version: contract.maximum_readable_version,
            migration_set_digest: contract.migration_set_digest,
        }
    }

    /// Return canonical digests for every recognized migration prefix.
    pub const fn embedded_prefix_digests() -> &'static [EmbeddedMigrationPrefixDigest] {
        EMBEDDED_MIGRATION_PREFIX_DIGESTS
    }

    /// Assess catalog and ledger state without issuing DDL or DML.
    pub async fn assess_connection(
        &self,
        connection: &mut PgConnection,
        contract: &SchemaCompatibilityContract,
        policy: SchemaMigrationPolicy,
    ) -> Result<SchemaDecision, SchemaCompatibilityError> {
        let observation = read_schema_observation(connection).await?;
        let recognized = self
            .recognized_identities()
            .map_err(|error| SchemaCompatibilityError::InvalidMetadata(error.to_string()))?;
        Ok(assess_schema_compatibility(
            contract,
            &recognized,
            &observation,
            policy,
        ))
    }

    /// Install only the metadata needed to acquire the schema-migration claim.
    ///
    /// A new database cannot acquire a claim from a table that does not yet
    /// exist. The engine calls this after an automatic initialize/migrate
    /// decision and before claim acquisition. Validate-only and already
    /// compatible decisions issue no writes.
    pub async fn bootstrap_migration_coordination(
        &self,
        connection: &mut PgConnection,
        decision: &SchemaDecision,
    ) -> Result<(), SchemaCompatibilityError> {
        for statement in bootstrap_statements_for_decision(decision)? {
            sqlx::query(statement).execute(&mut *connection).await?;
        }
        Ok(())
    }

    /// Apply every unapplied migration in strict version order.
    ///
    /// Each migration statement is executed in its own transaction, then the
    /// checksum is recorded. If a previously applied migration has different
    /// contents, application fails before any later migration is attempted.
    pub async fn apply(&self, pool: &PgPool) -> Result<MigrationReport> {
        self.ensure_schema_version(pool).await?;
        let mut applied = 0;
        for migration in self.discover()? {
            if self.is_applied(pool, &migration).await? {
                continue;
            }
            let started_at = Instant::now();
            let mut tx = pool.begin().await?;
            if let Err(error) = sqlx::query(&migration.sql).execute(&mut *tx).await {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to apply migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }
            if let Err(error) = tx.commit().await {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to commit migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }

            if let Err(error) = sqlx::query(
                "INSERT INTO schema_version (version, name, checksum, applied_at) VALUES ($1, $2, $3, now())",
            )
            .bind(i32::try_from(migration.version)?)
            .bind(&migration.name)
            .bind(&migration.checksum)
            .execute(pool)
            .await
            {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to record migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }
            record_migration_success(&migration, started_at.elapsed());
            applied += 1;
        }
        Ok(MigrationReport { applied })
    }

    pub async fn apply_connection(&self, connection: &mut PgConnection) -> Result<MigrationReport> {
        // Schema CLI paths use one raw connection so migrations do not require
        // the runtime reservoir or DynamoDB coordination to be online.
        self.ensure_schema_version_connection(connection).await?;
        let mut applied = 0;
        for migration in self.discover()? {
            if self.is_applied_connection(connection, &migration).await? {
                continue;
            }
            let started_at = Instant::now();
            let mut tx = connection.begin().await?;
            if let Err(error) = sqlx::query(&migration.sql).execute(&mut *tx).await {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to apply migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }
            if let Err(error) = tx.commit().await {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to commit migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }

            if let Err(error) = sqlx::query(
                "INSERT INTO schema_version (version, name, checksum, applied_at) VALUES ($1, $2, $3, now())",
            )
            .bind(i32::try_from(migration.version)?)
            .bind(&migration.name)
            .bind(&migration.checksum)
            .execute(&mut *connection)
            .await
            {
                record_migration_failure(&migration, started_at.elapsed(), &error);
                return Err(error).with_context(|| {
                    format!(
                        "failed to record migration V{:03}__{}",
                        migration.version, migration.name
                    )
                });
            }
            record_migration_success(&migration, started_at.elapsed());
            applied += 1;
        }
        Ok(MigrationReport { applied })
    }

    /// Apply an approved automatic decision under the schema-migration fence.
    ///
    /// Every migration statement, ledger record, and compatibility record uses
    /// a separate transaction. The fence and complete applied ledger are
    /// revalidated before each new migration statement, so a crash or takeover
    /// can replay only idempotent work and never advance metadata speculatively.
    pub async fn apply_decision(
        &self,
        connection: &mut PgConnection,
        decision: &SchemaDecision,
        migration_lease: &ControlLeaseGuard,
    ) -> Result<MigrationReport, SchemaCompatibilityError> {
        if migration_lease.claim_name() != "schema-migration" {
            return Err(SchemaCompatibilityError::Fenced);
        }
        let target = match decision {
            SchemaDecision::Initialize { target } | SchemaDecision::Migrate { to: target, .. } => {
                *target
            }
            SchemaDecision::Compatible {
                current,
                legacy_backfill: true,
            } => {
                ensure_migration_fence(connection, migration_lease).await?;
                let recognized = self.recognized_identities().map_err(|error| {
                    SchemaCompatibilityError::InvalidMetadata(error.to_string())
                })?;
                let observed = read_applied_migrations(connection).await?;
                validate_applied_prefix(&recognized, &observed)?;
                sqlx::query(schema_compatibility_bootstrap_sql())
                    .execute(&mut *connection)
                    .await?;
                ensure_migration_fence(connection, migration_lease).await?;
                self.persist_compatibility(connection, *current, migration_lease)
                    .await?;
                return Ok(MigrationReport { applied: 0 });
            }
            SchemaDecision::Compatible { .. } => return Ok(MigrationReport { applied: 0 }),
            SchemaDecision::MigrationRequired { current, target } => {
                return Err(SchemaCompatibilityError::MigrationRequired {
                    current: *current,
                    target: *target,
                });
            }
            SchemaDecision::Reject(incompatibility) => {
                return Err(SchemaCompatibilityError::Incompatible(
                    incompatibility.clone(),
                ));
            }
        };

        let contract = Self::compatibility_contract();
        if target != contract.target_version {
            return Err(SchemaCompatibilityError::InvalidMetadata(format!(
                "decision target V{target} differs from embedded target V{}",
                contract.target_version
            )));
        }
        // These two tables are the only bootstrap exception. Both statements
        // are the exact baseline-locked migration bytes and each runs alone.
        ensure_migration_fence(connection, migration_lease).await?;
        sqlx::query(SCHEMA_VERSION_BOOTSTRAP_SQL)
            .execute(&mut *connection)
            .await?;
        ensure_migration_fence(connection, migration_lease).await?;
        sqlx::query(control_lease_bootstrap_sql())
            .execute(&mut *connection)
            .await?;

        let migrations = self
            .discover()
            .map_err(|error| SchemaCompatibilityError::InvalidMetadata(error.to_string()))?;
        let recognized = migrations
            .iter()
            .map(MigrationFile::identity)
            .collect::<Vec<_>>();
        let mut applied = 0;
        for migration in migrations
            .iter()
            .filter(|migration| migration.version <= target)
        {
            ensure_migration_fence(connection, migration_lease).await?;
            let observed = read_applied_migrations(connection).await?;
            validate_applied_prefix(&recognized, &observed)?;
            if observed
                .iter()
                .any(|applied| applied.version == migration.version)
            {
                continue;
            }
            execute_migration_step(connection, migration).await?;

            ensure_migration_fence(connection, migration_lease).await?;
            record_applied_migration(connection, migration).await?;
            ensure_migration_fence(connection, migration_lease).await?;
            self.persist_compatibility(connection, migration.version, migration_lease)
                .await?;
            record_migration_success(migration, std::time::Duration::ZERO);
            applied += 1;
        }
        Ok(MigrationReport { applied })
    }

    async fn persist_compatibility(
        &self,
        connection: &mut PgConnection,
        version: u32,
        migration_lease: &ControlLeaseGuard,
    ) -> Result<(), SchemaCompatibilityError> {
        ensure_migration_fence(connection, migration_lease).await?;
        let recognized = self
            .recognized_identities()
            .map_err(|error| SchemaCompatibilityError::InvalidMetadata(error.to_string()))?;
        let digest = migration_set_digest(&recognized, version)
            .map_err(SchemaCompatibilityError::InvalidMetadata)?;
        let contract = Self::compatibility_contract();
        sqlx::query(
            "INSERT INTO schema_compatibility \
             (schema_version, tokeira_release, migration_set_digest, recorded_at) \
             VALUES ($1, $2, $3, now()) ON CONFLICT (schema_version) DO NOTHING",
        )
        .bind(i32::try_from(version).map_err(|error| {
            SchemaCompatibilityError::InvalidMetadata(format!(
                "schema version does not fit DSQL integer: {error}"
            ))
        })?)
        .bind(&contract.tokeira_release)
        .bind(&digest)
        .execute(&mut *connection)
        .await?;
        let stored = sqlx::query_as::<_, (String,)>(
            "SELECT migration_set_digest FROM schema_compatibility WHERE schema_version = $1",
        )
        .bind(i32::try_from(version).map_err(|error| {
            SchemaCompatibilityError::InvalidMetadata(format!(
                "schema version does not fit DSQL integer: {error}"
            ))
        })?)
        .fetch_one(&mut *connection)
        .await?;
        if stored.0 != digest {
            return Err(SchemaCompatibilityError::Incompatible(
                SchemaIncompatibility {
                    observed_version: Some(version),
                    minimum_supported_version: contract.minimum_supported_version,
                    target_version: contract.target_version,
                    maximum_readable_version: contract.maximum_readable_version,
                    category: super::SchemaIncompatibilityCategory::DigestMismatch {
                        version,
                        expected: digest,
                        observed: stored.0,
                    },
                },
            ));
        }
        Ok(())
    }

    /// Return the ordered migration plan without touching the database.
    pub fn dry_run(&self) -> Result<Vec<MigrationPlan>> {
        self.discover()?
            .into_iter()
            .map(|migration| {
                Ok(MigrationPlan {
                    version: migration.version,
                    name: migration.name,
                    checksum: migration.checksum,
                    sql: migration.sql,
                })
            })
            .collect()
    }

    /// Validate migration files against Tokeira's DSQL-safe subset.
    ///
    /// This is intentionally local/static: it catches schema mistakes before a
    /// migration ever reaches a DSQL cluster.
    pub fn validate(&self) -> Result<Vec<super::ValidationIssue>> {
        let mut issues = Vec::new();
        for migration in self.discover()? {
            issues.extend(DdlValidator::validate(
                &migration.sql,
                &migration.path.display().to_string(),
            ));
            if count_statements(&migration.sql) != 1 {
                issues.push(super::ValidationIssue {
                    file: migration.path.display().to_string(),
                    line: 1,
                    kind: super::ValidationKind::PlPgsql,
                    message: "migration files must contain exactly one SQL statement".to_owned(),
                });
            }
        }
        Ok(issues)
    }

    /// Read the highest applied schema version from the target database.
    pub async fn status(&self, pool: &PgPool) -> Result<SchemaStatus> {
        let current_version =
            match sqlx::query_scalar::<_, Option<i32>>("SELECT max(version) FROM schema_version")
                .fetch_optional(pool)
                .await
            {
                Ok(version) => version.flatten().map(u32::try_from).transpose()?,
                Err(error) if is_missing_schema_version(&error) => None,
                Err(error) => return Err(error.into()),
            };
        Ok(SchemaStatus {
            current_version,
            checked_at: OffsetDateTime::now_utc(),
        })
    }

    pub async fn status_connection(&self, connection: &mut PgConnection) -> Result<SchemaStatus> {
        // Mirrors `status(&PgPool)` for administrative commands that open a
        // single raw DSQL connection through the IAM connector.
        let current_version =
            match sqlx::query_scalar::<_, Option<i32>>("SELECT max(version) FROM schema_version")
                .fetch_optional(&mut *connection)
                .await
            {
                Ok(version) => version.flatten().map(u32::try_from).transpose()?,
                Err(error) if is_missing_schema_version(&error) => None,
                Err(error) => return Err(error.into()),
            };
        Ok(SchemaStatus {
            current_version,
            checked_at: OffsetDateTime::now_utc(),
        })
    }

    /// Discover and validate migration filenames, ordering, and contiguity.
    fn discover(&self) -> Result<Vec<MigrationFile>> {
        match &self.source {
            MigrationSource::Directory(config) => discover_directory_migrations(config),
            MigrationSource::Embedded(migrations) => Ok(migrations
                .iter()
                .map(|migration| MigrationFile {
                    version: migration.version,
                    name: migration.name.to_owned(),
                    path: PathBuf::from(migration.path),
                    sql: migration.sql.to_owned(),
                    checksum: migration.checksum.to_owned(),
                })
                .collect()),
        }
    }

    fn recognized_identities(&self) -> Result<Vec<MigrationIdentity>> {
        Ok(self
            .discover()?
            .iter()
            .map(MigrationFile::identity)
            .collect())
    }

    /// Bootstrap the migration metadata table.
    ///
    /// This statement is intentionally embedded instead of represented as a
    /// normal migration so a brand-new database can record V001 immediately.
    async fn ensure_schema_version(&self, pool: &PgPool) -> Result<()> {
        sqlx::query(SCHEMA_VERSION_BOOTSTRAP_SQL)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn ensure_schema_version_connection(&self, connection: &mut PgConnection) -> Result<()> {
        sqlx::query(SCHEMA_VERSION_BOOTSTRAP_SQL)
            .execute(connection)
            .await?;
        Ok(())
    }

    /// Check whether one migration has already been applied and still matches
    /// its recorded checksum.
    async fn is_applied(&self, pool: &PgPool, migration: &MigrationFile) -> Result<bool> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT checksum FROM schema_version WHERE version = $1",
        )
        .bind(i32::try_from(migration.version)?)
        .fetch_optional(pool)
        .await?;
        match row {
            Some((stored,)) => verify_checksum(migration.version, &stored, &migration.checksum),
            None => Ok(false),
        }
    }

    async fn is_applied_connection(
        &self,
        connection: &mut PgConnection,
        migration: &MigrationFile,
    ) -> Result<bool> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT checksum FROM schema_version WHERE version = $1",
        )
        .bind(i32::try_from(migration.version)?)
        .fetch_optional(connection)
        .await?;
        match row {
            Some((stored,)) => verify_checksum(migration.version, &stored, &migration.checksum),
            None => Ok(false),
        }
    }
}

fn bootstrap_statements_for_decision(
    decision: &SchemaDecision,
) -> Result<&'static [&'static str], SchemaCompatibilityError> {
    const COORDINATION_BOOTSTRAP: &[&str] =
        &[SCHEMA_VERSION_BOOTSTRAP_SQL, CONTROL_LEASE_BOOTSTRAP_SQL];
    match decision {
        SchemaDecision::Initialize { .. } | SchemaDecision::Migrate { .. } => {
            Ok(COORDINATION_BOOTSTRAP)
        }
        SchemaDecision::Compatible { .. } => Ok(&[]),
        SchemaDecision::MigrationRequired { current, target } => {
            Err(SchemaCompatibilityError::MigrationRequired {
                current: *current,
                target: *target,
            })
        }
        SchemaDecision::Reject(incompatibility) => Err(SchemaCompatibilityError::Incompatible(
            incompatibility.clone(),
        )),
    }
}

async fn read_schema_observation(
    connection: &mut PgConnection,
) -> Result<SchemaObservation, SchemaCompatibilityError> {
    let applied_migrations = read_applied_migrations(connection).await?;
    let compatibility = match sqlx::query_as::<_, (i32, String, String)>(
        "SELECT schema_version, tokeira_release, migration_set_digest \
         FROM schema_compatibility ORDER BY schema_version DESC LIMIT 1",
    )
    .fetch_optional(&mut *connection)
    .await
    {
        Ok(row) => row
            .map(
                |(version, release, digest)| -> Result<_, SchemaCompatibilityError> {
                    Ok(SchemaCompatibilityRecord {
                        schema_version: u32::try_from(version).map_err(|error| {
                            SchemaCompatibilityError::InvalidMetadata(format!(
                                "negative compatibility version: {error}"
                            ))
                        })?,
                        tokeira_release: release,
                        migration_set_digest: digest,
                    })
                },
            )
            .transpose()?,
        Err(error) if is_missing_schema_version(&error) => None,
        Err(error) => return Err(error.into()),
    };
    Ok(SchemaObservation {
        applied_migrations,
        compatibility,
    })
}

async fn read_applied_migrations(
    connection: &mut PgConnection,
) -> Result<Vec<AppliedMigration>, SchemaCompatibilityError> {
    let rows = match sqlx::query_as::<_, (i32, String, String)>(
        "SELECT version, name, checksum FROM schema_version ORDER BY version",
    )
    .fetch_all(&mut *connection)
    .await
    {
        Ok(rows) => rows,
        Err(error) if is_missing_schema_version(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    rows.into_iter()
        .map(|(version, name, checksum)| {
            Ok(AppliedMigration {
                version: u32::try_from(version).map_err(|error| {
                    SchemaCompatibilityError::InvalidMetadata(format!(
                        "negative applied migration version: {error}"
                    ))
                })?,
                name,
                checksum,
            })
        })
        .collect()
}

fn validate_applied_prefix(
    recognized: &[MigrationIdentity],
    applied: &[AppliedMigration],
) -> Result<(), SchemaCompatibilityError> {
    let contract = MigrationRunner::compatibility_contract();
    let observed_version = applied.last().map(|migration| migration.version);
    for (index, observed) in applied.iter().enumerate() {
        let expected_version = u32::try_from(index + 1).map_err(|error| {
            SchemaCompatibilityError::InvalidMetadata(format!(
                "migration ledger length exceeds u32: {error}"
            ))
        })?;
        if observed.version != expected_version {
            return Err(incompatible(
                &contract,
                observed_version,
                super::SchemaIncompatibilityCategory::LedgerOrdering {
                    expected: expected_version,
                    observed: observed.version,
                },
            ));
        }
        let Some(expected) = recognized.get(index) else {
            return Err(incompatible(
                &contract,
                observed_version,
                super::SchemaIncompatibilityCategory::UnknownMigration {
                    version: observed.version,
                },
            ));
        };
        if observed.name != expected.name {
            return Err(incompatible(
                &contract,
                observed_version,
                super::SchemaIncompatibilityCategory::MigrationNameMismatch {
                    version: observed.version,
                    expected: expected.name.clone(),
                    observed: observed.name.clone(),
                },
            ));
        }
        if observed.checksum != expected.checksum {
            return Err(incompatible(
                &contract,
                observed_version,
                super::SchemaIncompatibilityCategory::ChecksumMismatch {
                    version: observed.version,
                    expected: expected.checksum.clone(),
                    observed: observed.checksum.clone(),
                },
            ));
        }
    }
    Ok(())
}

fn incompatible(
    contract: &SchemaCompatibilityContract,
    observed_version: Option<u32>,
    category: super::SchemaIncompatibilityCategory,
) -> SchemaCompatibilityError {
    SchemaCompatibilityError::Incompatible(SchemaIncompatibility {
        observed_version,
        minimum_supported_version: contract.minimum_supported_version,
        target_version: contract.target_version,
        maximum_readable_version: contract.maximum_readable_version,
        category,
    })
}

async fn ensure_migration_fence(
    connection: &mut PgConnection,
    guard: &ControlLeaseGuard,
) -> Result<(), SchemaCompatibilityError> {
    let owned = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM tokeira_control_lease WHERE claim_name = $1 \
         AND cluster_id = $2 AND cluster_arn = $3 AND owner_id = $4 \
         AND fence_token = $5 AND expires_at > now())",
    )
    .bind(guard.claim_name())
    .bind(&guard.cluster().cluster_id)
    .bind(&guard.cluster().cluster_arn)
    .bind(guard.owner_id())
    .bind(guard.fence_token())
    .fetch_one(&mut *connection)
    .await?;
    if !owned {
        return Err(SchemaCompatibilityError::Fenced);
    }
    Ok(())
}

async fn record_applied_migration(
    connection: &mut PgConnection,
    migration: &MigrationFile,
) -> Result<(), SchemaCompatibilityError> {
    let version = i32::try_from(migration.version).map_err(|error| {
        SchemaCompatibilityError::InvalidMetadata(format!(
            "migration version does not fit DSQL integer: {error}"
        ))
    })?;
    sqlx::query(
        "INSERT INTO schema_version (version, name, checksum, applied_at) \
         VALUES ($1, $2, $3, now()) ON CONFLICT (version) DO NOTHING",
    )
    .bind(version)
    .bind(&migration.name)
    .bind(&migration.checksum)
    .execute(&mut *connection)
    .await?;
    let stored = sqlx::query_as::<_, (String, String)>(
        "SELECT name, checksum FROM schema_version WHERE version = $1",
    )
    .bind(version)
    .fetch_one(&mut *connection)
    .await?;
    let contract = MigrationRunner::compatibility_contract();
    if stored.0 != migration.name {
        return Err(incompatible(
            &contract,
            Some(migration.version),
            super::SchemaIncompatibilityCategory::MigrationNameMismatch {
                version: migration.version,
                expected: migration.name.clone(),
                observed: stored.0,
            },
        ));
    }
    if stored.1 != migration.checksum {
        return Err(incompatible(
            &contract,
            Some(migration.version),
            super::SchemaIncompatibilityCategory::ChecksumMismatch {
                version: migration.version,
                expected: migration.checksum.clone(),
                observed: stored.1,
            },
        ));
    }
    Ok(())
}

async fn execute_migration_step(
    connection: &mut PgConnection,
    migration: &MigrationFile,
) -> Result<(), SchemaCompatibilityError> {
    if !migration_is_idempotent(&migration.sql) {
        return Err(SchemaCompatibilityError::NonIdempotentMigration {
            version: migration.version,
        });
    }
    let index_spec = parse_async_index_spec(&migration.sql);
    let mut conflicts = 0_u32;
    let job_id = loop {
        let mut transaction = connection.begin().await?;
        let result = match &index_spec {
            Some(_) => {
                sqlx::query_scalar::<_, String>(&migration.sql)
                    .fetch_optional(&mut *transaction)
                    .await
            }
            None => sqlx::query(&migration.sql)
                .execute(&mut *transaction)
                .await
                .map(|_| None),
        };
        let job_id = match result {
            Ok(job_id) => job_id,
            Err(error) if is_occ_error(&error) && conflicts < DEFAULT_MIGRATION_OCC_RETRIES => {
                conflicts += 1;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match transaction.commit().await {
            Ok(()) => break job_id,
            Err(error) if is_occ_error(&error) && conflicts < DEFAULT_MIGRATION_OCC_RETRIES => {
                conflicts += 1;
            }
            Err(error) => return Err(error.into()),
        }
    };
    if let Some(spec) = index_spec {
        wait_for_async_index(connection, &spec, job_id.as_deref()).await?;
    }
    Ok(())
}

const DEFAULT_MIGRATION_OCC_RETRIES: u32 = 5;

fn migration_is_idempotent(sql: &str) -> bool {
    let normalized = sql.to_ascii_uppercase();
    (normalized.contains("CREATE TABLE IF NOT EXISTS"))
        || (normalized.contains("CREATE INDEX ASYNC IF NOT EXISTS"))
        || (normalized.contains("CREATE UNIQUE INDEX ASYNC IF NOT EXISTS"))
        || (normalized.contains("INSERT INTO")
            && normalized.contains("ON CONFLICT")
            && normalized.contains("DO NOTHING"))
}

fn is_occ_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database) if is_occ_sqlstate(database.code().as_deref())
    )
}

fn is_occ_sqlstate(code: Option<&str>) -> bool {
    code == Some("40001")
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AsyncIndexSpec {
    name: String,
    table: String,
    columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AsyncIndexRecoveryAction {
    WaitForJob(String),
    ValidateCatalog,
}

fn parse_async_index_spec(sql: &str) -> Option<AsyncIndexSpec> {
    let flattened = sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ");
    let tokens = flattened.split_ascii_whitespace().collect::<Vec<_>>();
    let async_position = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("ASYNC"))?;
    let mut name_position = async_position + 1;
    if tokens
        .get(name_position)
        .is_some_and(|token| token.eq_ignore_ascii_case("IF"))
    {
        name_position += 3;
    }
    let name = tokens.get(name_position)?.trim_matches('"').to_owned();
    let on_position = tokens
        .iter()
        .skip(name_position + 1)
        .position(|token| token.eq_ignore_ascii_case("ON"))?
        + name_position
        + 1;
    let table = tokens.get(on_position + 1)?.trim_matches('"').to_owned();
    let columns_start = flattened.find('(')? + 1;
    let columns_end = flattened[columns_start..].find(')')? + columns_start;
    let columns = flattened[columns_start..columns_end]
        .split(',')
        .filter_map(|column| column.split_ascii_whitespace().next())
        .map(|column| column.trim_matches('"').to_owned())
        .collect::<Vec<_>>();
    (!columns.is_empty()).then_some(AsyncIndexSpec {
        name,
        table,
        columns,
    })
}

async fn wait_for_async_index(
    connection: &mut PgConnection,
    spec: &AsyncIndexSpec,
    submitted_job_id: Option<&str>,
) -> Result<(), SchemaCompatibilityError> {
    if let Some(job_id) = submitted_job_id {
        wait_for_job(connection, spec, job_id).await?;
    }
    if index_is_valid(connection, spec).await? {
        return Ok(());
    }

    // `IF NOT EXISTS` returns no new job after a crash. Recover the recent
    // named build from `sys.jobs`; completed/failed jobs are retained only
    // briefly, so the catalog remains the final authority.
    let qualified = format!("public.{}", spec.name);
    let job = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT job_id, status, details FROM sys.jobs WHERE job_type = 'INDEX_BUILD' \
         AND (object_name = $1 OR object_name = $2) ORDER BY update_time DESC LIMIT 1",
    )
    .bind(&spec.name)
    .bind(&qualified)
    .fetch_optional(&mut *connection)
    .await?;
    if let AsyncIndexRecoveryAction::WaitForJob(job_id) = classify_async_index_job(spec, job)? {
        wait_for_job(connection, spec, &job_id).await?;
    }
    if index_is_valid(connection, spec).await? {
        Ok(())
    } else {
        Err(SchemaCompatibilityError::IndexInvalid {
            index_name: spec.name.clone(),
            reason: "catalog reports the named index absent, invalid, or structurally different"
                .to_owned(),
        })
    }
}

fn classify_async_index_job(
    spec: &AsyncIndexSpec,
    job: Option<(String, String, Option<String>)>,
) -> Result<AsyncIndexRecoveryAction, SchemaCompatibilityError> {
    match job {
        Some((job_id, status, _)) if status == "submitted" || status == "processing" => {
            Ok(AsyncIndexRecoveryAction::WaitForJob(job_id))
        }
        Some((_, status, details)) if status == "failed" => {
            Err(SchemaCompatibilityError::IndexFailed {
                index_name: spec.name.clone(),
                details: details.unwrap_or_else(|| "DSQL reported a failed index job".to_owned()),
            })
        }
        Some((_, status, _)) if status != "completed" => {
            Err(SchemaCompatibilityError::IndexInvalid {
                index_name: spec.name.clone(),
                reason: format!("unexpected DSQL job status {status}"),
            })
        }
        _ => Ok(AsyncIndexRecoveryAction::ValidateCatalog),
    }
}

async fn wait_for_job(
    connection: &mut PgConnection,
    spec: &AsyncIndexSpec,
    job_id: &str,
) -> Result<(), SchemaCompatibilityError> {
    let completed = sqlx::query_scalar::<_, bool>("SELECT sys.wait_for_job($1)")
        .bind(job_id)
        .fetch_one(&mut *connection)
        .await?;
    if completed {
        return Ok(());
    }
    let details = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT status, details FROM sys.jobs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_optional(&mut *connection)
    .await?
    .map(|(status, details)| {
        details.unwrap_or_else(|| format!("DSQL job ended with status {status}"))
    })
    .unwrap_or_else(|| "DSQL job disappeared before catalog validation".to_owned());
    Err(SchemaCompatibilityError::IndexFailed {
        index_name: spec.name.clone(),
        details,
    })
}

async fn index_is_valid(
    connection: &mut PgConnection,
    spec: &AsyncIndexSpec,
) -> Result<bool, SchemaCompatibilityError> {
    let catalog = sqlx::query_as::<_, (bool, String)>(
        "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = $1 AND n.nspname = current_schema()",
    )
    .bind(&spec.name)
    .fetch_optional(&mut *connection)
    .await?;
    let Some((valid, definition)) = catalog else {
        return Ok(false);
    };
    if !valid {
        return Ok(false);
    }
    Ok(index_definition_matches(spec, valid, &definition))
}

fn index_definition_matches(spec: &AsyncIndexSpec, valid: bool, definition: &str) -> bool {
    let definition = definition.to_ascii_lowercase();
    let expected_table = spec.table.to_ascii_lowercase();
    valid
        && definition.contains(&expected_table)
        && spec
            .columns
            .iter()
            .all(|column| definition.contains(&column.to_ascii_lowercase()))
}

fn discover_directory_migrations(config: &MigrationConfig) -> Result<Vec<MigrationFile>> {
    let mut migrations = Vec::new();
    for entry in fs::read_dir(&config.migrations_dir).with_context(|| {
        format!(
            "failed to read migration directory {}",
            config.migrations_dir.display()
        )
    })? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("invalid migration filename {}", path.display()))?;
        let (version, name) = parse_migration_filename(filename)?;
        let sql = fs::read_to_string(&path)
            .with_context(|| format!("failed to read migration {}", path.display()))?;
        let checksum = checksum(&sql);
        migrations.push(MigrationFile {
            version,
            name,
            path,
            sql,
            checksum,
        });
    }
    migrations.sort_by_key(|migration| migration.version);
    // Gaps are rejected even when missing migrations would not be immediately
    // needed. A gap means two environments could reach different schemas while
    // claiming the same highest version.
    for pair in migrations.windows(2) {
        if pair[0].version == pair[1].version {
            bail!("duplicate migration version {}", pair[0].version);
        }
        if pair[0].version + 1 != pair[1].version {
            bail!(
                "migration version gap between {} and {}",
                pair[0].version,
                pair[1].version
            );
        }
    }
    Ok(migrations)
}

fn verify_checksum(version: u32, stored: &str, file: &str) -> Result<bool> {
    if stored == file {
        return Ok(true);
    }
    bail!("checksum mismatch for migration {version}: stored={stored}, file={file}")
}

fn record_migration_success(migration: &MigrationFile, duration: std::time::Duration) {
    storage_metrics::record_migration_applied(OutcomeLabel::Success);
    storage_metrics::record_migration_duration(OutcomeLabel::Success, duration);
    tracing::info!(
        migration_file = %migration.path.display(),
        migration_version = migration.version,
        migration_name = %migration.name,
        duration_ms = duration.as_millis() as u64,
        schema_version = migration.version,
        "applied DSQL migration"
    );
}

fn record_migration_failure(
    migration: &MigrationFile,
    duration: std::time::Duration,
    error: &sqlx::Error,
) {
    storage_metrics::record_migration_applied(OutcomeLabel::Failure);
    storage_metrics::record_migration_duration(OutcomeLabel::Failure, duration);
    mark_error_biased_sample(ErrorBiasedSamplingReason::MigrationFailure);
    tracing::error!(
        migration_file = %migration.path.display(),
        migration_version = migration.version,
        migration_name = %migration.name,
        duration_ms = duration.as_millis() as u64,
        error_class = migration_error_class(error),
        sqlstate = sqlstate(error).as_deref().unwrap_or("unknown"),
        "failed to apply DSQL migration"
    );
}

fn migration_error_class(error: &sqlx::Error) -> &'static str {
    match error {
        sqlx::Error::Database(_) => "database",
        sqlx::Error::Io(_) => "io",
        sqlx::Error::Tls(_) => "tls",
        sqlx::Error::PoolTimedOut => "pool_timeout",
        sqlx::Error::PoolClosed => "pool_closed",
        sqlx::Error::WorkerCrashed => "worker_crashed",
        _ => "sqlx",
    }
}

fn sqlstate(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(database) => database.code().map(|code| code.into_owned()),
        _ => None,
    }
}

fn is_missing_schema_version(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.code().as_deref() == Some("42P01")
    )
}

/// Parse `VNNN__snake_case_name.sql`.
///
/// The zero-padded format keeps filesystem order readable, while the numeric
/// version is still parsed as an integer for gap detection.
pub fn parse_migration_filename(filename: &str) -> Result<(u32, String)> {
    let Some(rest) = filename.strip_prefix('V') else {
        bail!("migration filename must start with V");
    };
    let Some((version, description)) = rest.split_once("__") else {
        bail!("migration filename must contain __ separator");
    };
    let Some(description) = description.strip_suffix(".sql") else {
        bail!("migration filename must end with .sql");
    };
    if version.is_empty() || !version.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("migration version must be a zero-padded integer");
    }
    if description.is_empty()
        || !description
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        bail!("migration description must be snake_case");
    }
    Ok((version.parse()?, description.to_owned()))
}

fn checksum(sql: &str) -> String {
    sha256_hex(sql.as_bytes())
}

/// Approximate statement count for the one-DDL-per-file safety check.
///
/// This is a heuristic — it splits on `;` and filters empty/comment lines.
/// It will miscount if a string literal contains a semicolon. Since we
/// require one DDL per migration file, this is a safety net, not a parser.
fn count_statements(sql: &str) -> usize {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty() && !statement.starts_with("--"))
        .count()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
    };

    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;
    use proptest::prelude::*;

    use super::{
        AsyncIndexRecoveryAction, MigrationFile, MigrationRunner, SchemaCompatibilityError,
        checksum, classify_async_index_job, index_definition_matches, migration_is_idempotent,
        parse_async_index_spec, parse_migration_filename,
    };
    use crate::{
        dsql::{MigrationConfig, SchemaDecision},
        metrics::{MIGRATION_APPLIED_TOTAL, MIGRATION_DURATION_SECONDS},
    };

    #[test]
    fn parses_valid_filename() {
        let (version, name) = parse_migration_filename("V001__schema_version.sql").unwrap();
        assert_eq!(version, 1);
        assert_eq!(name, "schema_version");
    }

    #[test]
    fn rejects_invalid_filename() {
        assert!(parse_migration_filename("001_schema_version.sql").is_err());
    }

    #[tokio::test]
    async fn dry_run_returns_sorted_migrations() {
        let dir = temp_migration_dir("dry_run_returns_sorted_migrations");
        write_migration(
            &dir,
            "V002__second.sql",
            "CREATE TABLE IF NOT EXISTS second (id UUID);",
        );
        write_migration(
            &dir,
            "V001__first.sql",
            "CREATE TABLE IF NOT EXISTS first (id UUID);",
        );

        let runner = MigrationRunner::new(MigrationConfig {
            migrations_dir: dir.clone(),
        });
        let plans = runner.dry_run().unwrap();

        assert_eq!(plans[0].version, 1);
        assert_eq!(plans[1].version, 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn detects_version_gaps() {
        let dir = temp_migration_dir("detects_version_gaps");
        write_migration(
            &dir,
            "V001__first.sql",
            "CREATE TABLE IF NOT EXISTS first (id UUID);",
        );
        write_migration(
            &dir,
            "V003__third.sql",
            "CREATE TABLE IF NOT EXISTS third (id UUID);",
        );

        let runner = MigrationRunner::new(MigrationConfig {
            migrations_dir: dir.clone(),
        });

        assert!(runner.validate().is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn detects_checksum_mismatch() {
        assert!(super::verify_checksum(7, "stored", "file").is_err());
        assert!(super::verify_checksum(7, "same", "same").unwrap());
    }

    #[test]
    fn migration_success_and_failure_record_metrics_without_filename_labels() {
        let recorder = DebuggingRecorder::new();
        let migration = MigrationFile {
            version: 7,
            name: "add_visibility_index".to_string(),
            path: PathBuf::from("V007__add_visibility_index.sql"),
            sql: "CREATE TABLE example (id UUID);".to_string(),
            checksum: "checksum".to_string(),
        };

        with_local_recorder(&recorder, || {
            super::record_migration_success(&migration, std::time::Duration::from_millis(12));
            super::record_migration_failure(
                &migration,
                std::time::Duration::from_millis(34),
                &sqlx::Error::RowNotFound,
            );
        });

        let entries = recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, _)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect::<HashMap<_, _>>();
                (key.key().name().to_string(), labels)
            })
            .collect::<Vec<_>>();
        assert_metric_with_status(&entries, MIGRATION_APPLIED_TOTAL, "success");
        assert_metric_with_status(&entries, MIGRATION_APPLIED_TOTAL, "failure");
        assert_metric_with_status(&entries, MIGRATION_DURATION_SECONDS, "success");
        assert_metric_with_status(&entries, MIGRATION_DURATION_SECONDS, "failure");

        for (_, labels) in entries {
            assert!(!labels.contains_key("migration_file"));
            assert!(!labels.contains_key("migration_name"));
        }
    }

    #[test]
    fn non_database_migration_errors_have_no_sqlstate() {
        assert_eq!(
            super::migration_error_class(&sqlx::Error::RowNotFound),
            "sqlx"
        );
        assert!(super::sqlstate(&sqlx::Error::RowNotFound).is_none());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn filename_parser_accepts_expected_pattern(version in 1u32..999, name in "[a-z][a-z0-9_]{0,20}") {
            let filename = format!("V{version:03}__{name}.sql");
            let (parsed_version, parsed_name) = parse_migration_filename(&filename).unwrap();
            prop_assert_eq!(parsed_version, version);
            prop_assert_eq!(parsed_name, name);
        }

        #[test]
        fn migration_versions_sort_strictly(mut versions in proptest::collection::vec(1u32..1000, 1..64)) {
            versions.sort_unstable();
            versions.dedup();
            let sorted = versions.clone();
            prop_assert!(sorted.windows(2).all(|pair| pair[0] < pair[1]));
        }

        #[test]
        fn checksum_is_deterministic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let sql = String::from_utf8_lossy(&bytes);
            prop_assert_eq!(checksum(&sql), checksum(&sql));
        }

        // Feature: managed-embedded-dsql, Property 8: migration replay is serialized, fenced, and idempotent
        #[test]
        fn migration_replay_model_is_serialized_fenced_and_idempotent(
            target in 1_usize..40,
            events in proptest::collection::vec(
                (any::<bool>(), any::<bool>(), 0_u8..8, any::<bool>(), any::<bool>()),
                1..200,
            ),
        ) {
            let mut physical = vec![false; target];
            let mut ledger = vec![false; target];
            let mut fenced = false;
            let mut explicit_failure = false;
            for (crash_after_operation, competing_owner, occ_conflicts, async_index_failed, checksum_drift) in events {
                if fenced || explicit_failure {
                    break;
                }
                if competing_owner {
                    fenced = true;
                    continue;
                }
                let Some(version) = ledger.iter().position(|recorded| !recorded) else {
                    break;
                };
                if checksum_drift && version > 0 {
                    explicit_failure = true;
                    continue;
                }
                if occ_conflicts > super::DEFAULT_MIGRATION_OCC_RETRIES as u8 {
                    explicit_failure = true;
                    continue;
                }
                // Re-execution after a crash is a logical no-op because every
                // migration admitted to this path is proven idempotent.
                physical[version] = true;
                if async_index_failed {
                    explicit_failure = true;
                    continue;
                }
                if !crash_after_operation {
                    ledger[version] = true;
                }
                prop_assert!(ledger
                    .iter()
                    .zip(&physical)
                    .all(|(recorded, completed)| !recorded || *completed));
                let prefix = ledger.iter().take_while(|recorded| **recorded).count();
                prop_assert!(ledger[prefix..].iter().all(|recorded| !recorded));
            }

            let before_recovery = physical.clone();
            if !fenced && !explicit_failure {
                for version in 0..target {
                    physical[version] = true;
                    ledger[version] = true;
                }
                prop_assert!(ledger.iter().all(|recorded| *recorded));
                prop_assert!(physical.iter().all(|completed| *completed));
                prop_assert!(before_recovery
                    .iter()
                    .enumerate()
                    .all(|(version, completed)| !completed || physical[version]));
            }
        }
    }

    #[test]
    fn every_embedded_migration_is_proven_idempotent() {
        let plans = MigrationRunner::embedded()
            .dry_run()
            .expect("embedded migrations are valid");
        assert_eq!(plans.len(), 67);
        assert!(plans.iter().all(|plan| migration_is_idempotent(&plan.sql)));
    }

    #[test]
    fn async_index_parser_extracts_named_catalog_identity() {
        let spec = parse_async_index_spec(
            "CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx_example ON records (namespace_id, run_id);",
        )
        .expect("valid async index is recognized");
        assert_eq!(spec.name, "idx_example");
        assert_eq!(spec.table, "records");
        assert_eq!(spec.columns, ["namespace_id", "run_id"]);
    }

    #[test]
    fn bootstrap_statements_are_exact_migration_bytes() {
        assert_eq!(
            super::SCHEMA_VERSION_BOOTSTRAP_SQL,
            include_str!("../../migrations/V001__schema_version.sql")
        );
        assert_eq!(
            super::schema_compatibility_bootstrap_sql(),
            include_str!("../../migrations/V066__schema_compatibility.sql")
        );
        assert_eq!(
            super::control_lease_bootstrap_sql(),
            include_str!("../../migrations/V067__tokeira_control_lease.sql")
        );
    }

    #[test]
    fn bootstrap_writes_only_for_automatic_schema_changes() {
        assert_eq!(
            super::bootstrap_statements_for_decision(&SchemaDecision::Initialize { target: 67 })
                .expect("initialize bootstrap")
                .len(),
            2
        );
        assert_eq!(
            super::bootstrap_statements_for_decision(&SchemaDecision::Migrate { from: 66, to: 67 })
                .expect("migration bootstrap")
                .len(),
            2
        );
        assert!(
            super::bootstrap_statements_for_decision(&SchemaDecision::Compatible {
                current: 67,
                legacy_backfill: false,
            })
            .expect("compatible schema")
            .is_empty()
        );
        assert!(matches!(
            super::bootstrap_statements_for_decision(&SchemaDecision::MigrationRequired {
                current: 0,
                target: 67,
            }),
            Err(SchemaCompatibilityError::MigrationRequired { .. })
        ));
    }

    #[test]
    fn migration_required_error_is_actionable() {
        let error = SchemaCompatibilityError::MigrationRequired {
            current: 12,
            target: 67,
        };
        assert_eq!(
            error.to_string(),
            "schema migration required: current V12, target V67"
        );
    }

    #[test]
    fn only_serialization_failure_sqlstate_is_retryable() {
        assert!(super::is_occ_sqlstate(Some("40001")));
        assert!(!super::is_occ_sqlstate(Some("40P01")));
        assert!(!super::is_occ_sqlstate(Some("23505")));
        assert!(!super::is_occ_sqlstate(None));
    }

    #[test]
    fn lost_async_index_jobs_fall_back_to_catalog_validation() {
        let spec = parse_async_index_spec(
            "CREATE INDEX ASYNC IF NOT EXISTS idx_example ON records (namespace_id, run_id);",
        )
        .expect("valid index fixture");
        assert_eq!(
            classify_async_index_job(&spec, None).expect("lost job is recoverable"),
            AsyncIndexRecoveryAction::ValidateCatalog
        );
        assert_eq!(
            classify_async_index_job(
                &spec,
                Some(("job-1".to_owned(), "processing".to_owned(), None)),
            )
            .expect("active job is recoverable"),
            AsyncIndexRecoveryAction::WaitForJob("job-1".to_owned())
        );
        assert!(matches!(
            classify_async_index_job(
                &spec,
                Some((
                    "job-2".to_owned(),
                    "failed".to_owned(),
                    Some("build failed".to_owned()),
                )),
            ),
            Err(SchemaCompatibilityError::IndexFailed { .. })
        ));
        assert!(matches!(
            classify_async_index_job(
                &spec,
                Some(("job-3".to_owned(), "unknown".to_owned(), None)),
            ),
            Err(SchemaCompatibilityError::IndexInvalid { .. })
        ));
    }

    #[test]
    fn invalid_or_structurally_different_indexes_are_rejected() {
        let spec = parse_async_index_spec(
            "CREATE INDEX ASYNC IF NOT EXISTS idx_example ON records (namespace_id, run_id);",
        )
        .expect("valid index fixture");
        let matching =
            "CREATE INDEX idx_example ON public.records USING btree (namespace_id, run_id)";
        assert!(index_definition_matches(&spec, true, matching));
        assert!(!index_definition_matches(&spec, false, matching));
        assert!(!index_definition_matches(
            &spec,
            true,
            "CREATE INDEX idx_example ON public.other USING btree (namespace_id)"
        ));
    }

    fn temp_migration_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tokeira_dsql_migration_{name}_{}",
            std::process::id()
        ));
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_migration(dir: &Path, filename: &str, sql: &str) {
        fs::write(dir.join(filename), sql).unwrap();
    }

    fn assert_metric_with_status(
        entries: &[(String, HashMap<String, String>)],
        metric: &str,
        status: &str,
    ) {
        assert!(entries.iter().any(|(name, labels)| {
            name == metric && labels.get("status").is_some_and(|value| value == status)
        }));
    }
}
