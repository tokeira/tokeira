//! Forward-only DSQL migration discovery, validation, and application.
//!
//! Aurora DSQL has schema-change constraints that matter for correctness and
//! operational safety: secondary indexes must be asynchronous, migrations are
//! one SQL statement per file, and version gaps must be rejected so every
//! environment converges through the same ordered schema path.

use std::{fs, path::PathBuf, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool};
use time::OffsetDateTime;
use tokeira_observability::{ErrorBiasedSamplingReason, OutcomeLabel, mark_error_biased_sample};

use super::{MigrationConfig, validation::DdlValidator};
use crate::metrics as storage_metrics;

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

/// Compile-time embedded migration statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedMigration {
    pub version: u32,
    pub name: &'static str,
    pub path: &'static str,
    pub checksum: &'static str,
    pub sql: &'static str,
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

    /// Bootstrap the migration metadata table.
    ///
    /// This statement is intentionally embedded instead of represented as a
    /// normal migration so a brand-new database can record V001 immediately.
    async fn ensure_schema_version(&self, pool: &PgPool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (version)
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn ensure_schema_version_connection(&self, connection: &mut PgConnection) -> Result<()> {
        // Keep this SQL byte-for-byte aligned with the pool variant. The two
        // entry points differ only by executor shape.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (version)
            )",
        )
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
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    format!("{:x}", hasher.finalize())
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

    use super::{MigrationFile, MigrationRunner, checksum, parse_migration_filename};
    use crate::{
        dsql::MigrationConfig,
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
