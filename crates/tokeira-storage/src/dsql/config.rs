//! Configuration for the DSQL storage foundation.
//!
//! These settings are intentionally internal to the DSQL backend. The server
//! configuration currently exposes only high-level deployment metadata; this
//! module captures the mechanical details needed to keep DSQL connections
//! healthy under IAM token lifetimes, per-class budgets, and migration safety
//! rules.

use std::path::PathBuf;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use time::Duration;

use crate::CurrentExecutionConflictPolicy;

/// IAM-authenticated DSQL connections should not be reused past this bound.
///
/// The reservoir retires connections before the hard cutoff so callers do not
/// receive a connection that will expire mid-transaction.
const DSQL_HARD_CUTOFF: Duration = Duration::minutes(15);

fn default_target_ready() -> usize {
    50
}

fn default_inflight_limit() -> usize {
    8
}

fn default_base_lifetime() -> Duration {
    Duration::minutes(10)
}

fn default_lifetime_jitter() -> Duration {
    Duration::minutes(2)
}

fn default_guard_window() -> Duration {
    Duration::seconds(45)
}

fn default_scan_interval() -> Duration {
    Duration::seconds(1)
}

fn default_rate_per_second() -> f64 {
    100.0
}

fn default_burst_capacity() -> u64 {
    1_000
}

fn default_shard_count() -> u32 {
    64
}

fn default_projection_partition_count() -> u32 {
    16
}

fn default_lease_duration() -> Duration {
    Duration::seconds(30)
}

fn default_migrations_dir() -> PathBuf {
    PathBuf::from("crates/tokeira-storage/migrations")
}

/// Internal reservoir configuration for DSQL connections.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservoirConfig {
    /// Desired number of warm connections available for checkout.
    ///
    /// This is a target, not a guarantee: rate limiting, endpoint failures, and
    /// class-budget pressure can temporarily keep the ready pool below it.
    #[serde(default = "default_target_ready")]
    pub target_ready: usize,
    /// Maximum number of connection creation attempts in flight.
    ///
    /// This protects the DSQL endpoint and IAM token path from a thundering herd
    /// after process start or after a transient network outage.
    #[serde(default = "default_inflight_limit")]
    pub inflight_limit: usize,
    /// Base connection lifetime before jitter is applied.
    #[serde(default = "default_base_lifetime")]
    pub base_lifetime: Duration,
    /// Positive jitter added to the base lifetime.
    ///
    /// Jitter prevents a process from retiring all warmed connections in one
    /// synchronized wave.
    #[serde(default = "default_lifetime_jitter")]
    pub lifetime_jitter: Duration,
    /// Time reserved before the DSQL hard cutoff.
    ///
    /// Returned and idle connections inside this guard window are retired
    /// instead of being handed to new work.
    #[serde(default = "default_guard_window")]
    pub guard_window: Duration,
    /// Interval for scanning the ready pool for near-expired connections.
    #[serde(default = "default_scan_interval")]
    pub scan_interval: Duration,
}

impl Default for ReservoirConfig {
    fn default() -> Self {
        Self {
            target_ready: default_target_ready(),
            inflight_limit: default_inflight_limit(),
            base_lifetime: default_base_lifetime(),
            lifetime_jitter: default_lifetime_jitter(),
            guard_window: default_guard_window(),
            scan_interval: default_scan_interval(),
        }
    }
}

impl ReservoirConfig {
    /// Validate bounds that protect connection correctness rather than just
    /// syntactic shape.
    pub fn validate(&self) -> Result<()> {
        if self.target_ready == 0 {
            bail!("reservoir target_ready must be greater than zero");
        }
        if self.inflight_limit == 0 {
            bail!("reservoir inflight_limit must be greater than zero");
        }
        if self.base_lifetime <= Duration::ZERO {
            bail!("reservoir base_lifetime must be positive");
        }
        if self.lifetime_jitter < Duration::ZERO {
            bail!("reservoir lifetime_jitter cannot be negative");
        }
        if self.guard_window <= Duration::ZERO {
            bail!("reservoir guard_window must be positive");
        }
        if self.scan_interval <= Duration::ZERO {
            bail!("reservoir scan_interval must be positive");
        }
        if self.base_lifetime + self.lifetime_jitter > DSQL_HARD_CUTOFF - self.guard_window {
            bail!("reservoir lifetime exceeds DSQL cutoff minus guard window");
        }
        Ok(())
    }

    pub fn max_lifetime_bound(&self) -> Duration {
        self.base_lifetime + self.lifetime_jitter
    }
}

/// Migration runner configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationConfig {
    /// Directory containing forward-only `VNNN__name.sql` migration files.
    ///
    /// The default points at the workspace migrations directory so tests and
    /// local runs exercise the same schema files.
    #[serde(default = "default_migrations_dir")]
    pub migrations_dir: PathBuf,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            migrations_dir: default_migrations_dir(),
        }
    }
}

/// IAM authentication settings for DSQL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlAuthConfig {
    /// DSQL cluster endpoint, without a PostgreSQL URL prefix.
    pub endpoint: String,
    /// Optional region override. If absent, region is inferred from the
    /// standard `*.dsql.{region}.on.aws` endpoint shape.
    #[serde(default)]
    pub region: Option<String>,
    /// IAM role used for schema migrations and other administrative actions.
    #[serde(default)]
    pub admin_role_arn: Option<String>,
    /// IAM role used by transition commits and runtime reads.
    #[serde(default)]
    pub runtime_role_arn: Option<String>,
    /// IAM role reserved for future read-only operators or projection readers.
    #[serde(default)]
    pub readonly_role_arn: Option<String>,
}

/// Credential role used by DSQL administrative and runtime paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DsqlRole {
    Admin,
    Runtime,
    Readonly,
}

impl DsqlAuthConfig {
    /// Check only local invariants. Network/auth failures are left to the
    /// connector so configuration errors fail before opening sockets.
    pub fn validate(&self) -> Result<()> {
        if self.endpoint.trim().is_empty() {
            bail!("dsql endpoint must not be empty");
        }
        Ok(())
    }

    pub fn role_arn_for(&self, role: DsqlRole) -> Option<&str> {
        match role {
            DsqlRole::Admin => self.admin_role_arn.as_deref(),
            DsqlRole::Runtime => self.runtime_role_arn.as_deref(),
            DsqlRole::Readonly => self.readonly_role_arn.as_deref(),
        }
    }

    pub fn resolved_region(&self) -> Option<String> {
        self.region
            .clone()
            .or_else(|| detect_region_from_endpoint(&self.endpoint))
    }

    pub fn connection_string(&self, user: &str, database: &str) -> Result<String> {
        self.validate()?;
        let mut connection = format!(
            "postgres://{}@{}:5432/{}",
            user.trim(),
            self.endpoint.trim(),
            database.trim()
        );
        if let Some(region) = self.resolved_region() {
            connection.push_str("?region=");
            connection.push_str(&region);
        }
        Ok(connection)
    }
}

/// Extract `{region}` from the canonical DSQL endpoint format.
pub fn detect_region_from_endpoint(endpoint: &str) -> Option<String> {
    let marker = ".dsql.";
    let suffix = ".on.aws";
    let start = endpoint.find(marker)? + marker.len();
    let rest = &endpoint[start..];
    let end = rest.find(suffix)?;
    let region = &rest[..end];
    (!region.is_empty()).then(|| region.to_owned())
}

/// DynamoDB coordination resources used by the DSQL connection reservoir.
///
/// The two table names are intentionally separate. The rate-limiter table has
/// one global token-bucket row, while the connection-lease table contains slot
/// block rows that represent cluster-wide connection capacity. Keeping them
/// separate lets each table use a narrow key shape and independent IAM/TTL
/// policies.
///
/// This is pure configuration data: it holds table names only. The live
/// `aws_sdk_dynamodb::Client` is a runtime resource, not config, and is
/// injected into [`crate::dsql::DsqlStore::connect`] explicitly. Keeping the
/// client out of config defaults is deliberate — a `Default` that builds an SDK
/// client would eagerly construct the default HTTPS/TLS stack (reading OS trust
/// roots) for a value that may be discarded by struct-update or only used in
/// tests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlCoordinationConfig {
    /// DynamoDB table containing the distributed connection-creation bucket.
    #[serde(default = "default_rate_limiter_table")]
    pub rate_limiter_table: String,
    /// DynamoDB table containing connection slot block leases.
    #[serde(default = "default_conn_lease_table")]
    pub conn_lease_table: String,
}

fn default_rate_limiter_table() -> String {
    "tokeira-dsql-rate-limiter".to_owned()
}

fn default_conn_lease_table() -> String {
    "tokeira-dsql-conn-lease".to_owned()
}

impl Default for DsqlCoordinationConfig {
    fn default() -> Self {
        Self {
            rate_limiter_table: default_rate_limiter_table(),
            conn_lease_table: default_conn_lease_table(),
        }
    }
}

/// DSQL pool foundation configuration.
///
/// This is a storage-internal configuration object rather than the public
/// `tokeirad.toml` schema. Tokeirad builds it from effective infrastructure and
/// placement settings so repository routing, projection partitioning, and the
/// reservoir all observe one consistent set of counts.
#[derive(Clone, Debug)]
pub struct DsqlPoolConfig {
    /// Warm-connection reservoir behavior.
    pub reservoir: ReservoirConfig,
    /// Migration discovery settings.
    pub migration: MigrationConfig,
    /// DynamoDB coordination resources for global rate and slot accounting.
    pub coordination: DsqlCoordinationConfig,
    /// Node-local connection creation rate.
    ///
    /// This caps new physical connections, not logical operation throughput.
    pub connection_rate_per_second: f64,
    /// Burst capacity for the node-local connection creation limiter.
    pub burst_capacity: u64,
    /// Runtime shard count used for deterministic run-key ownership.
    pub shard_count: u32,
    /// Projection partition count used when writing projection-log records.
    ///
    /// Writers and projection workers must use the same value; otherwise a
    /// worker fanout can miss records written into partitions it never scans.
    pub projection_partition_count: u32,
    /// Workflow-id conflict behavior used by repository start commits.
    pub conflict_policy: CurrentExecutionConflictPolicy,
    /// Duration added to the repository's application clock for shard leases.
    pub lease_duration: Duration,
}

impl Default for DsqlPoolConfig {
    fn default() -> Self {
        Self {
            reservoir: ReservoirConfig::default(),
            migration: MigrationConfig::default(),
            coordination: DsqlCoordinationConfig::default(),
            connection_rate_per_second: default_rate_per_second(),
            burst_capacity: default_burst_capacity(),
            shard_count: default_shard_count(),
            projection_partition_count: default_projection_partition_count(),
            conflict_policy: CurrentExecutionConflictPolicy::default(),
            lease_duration: default_lease_duration(),
        }
    }
}

impl DsqlPoolConfig {
    /// Validate config that must be true before constructing any background
    /// reservoir tasks.
    pub fn validate(&self) -> Result<()> {
        self.reservoir.validate()?;
        if self.connection_rate_per_second <= 0.0 {
            bail!("connection_rate_per_second must be positive");
        }
        if self.burst_capacity == 0 {
            bail!("burst_capacity must be greater than zero");
        }
        if self.coordination.rate_limiter_table.trim().is_empty() {
            bail!("rate_limiter_table must not be empty");
        }
        if self.coordination.conn_lease_table.trim().is_empty() {
            bail!("conn_lease_table must not be empty");
        }
        if self.shard_count == 0 {
            bail!("shard_count must be greater than zero");
        }
        if self.projection_partition_count == 0 {
            bail!("projection_partition_count must be greater than zero");
        }
        if self.lease_duration <= Duration::ZERO {
            bail!("lease_duration must be positive");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use time::Duration;

    use super::*;

    #[test]
    fn defaults_validate() {
        DsqlPoolConfig::default().validate().unwrap();
    }

    #[test]
    fn coordination_default_is_pure_and_panic_free() {
        // Regression guard for the original native-roots `debug_assert!`:
        // defaulting coordination config must not construct an SDK/TLS client.
        let coordination = DsqlCoordinationConfig::default();
        assert_eq!(coordination.rate_limiter_table, "tokeira-dsql-rate-limiter");
        assert_eq!(coordination.conn_lease_table, "tokeira-dsql-conn-lease");
        DsqlPoolConfig::default().validate().unwrap();
    }

    #[test]
    fn validates_lease_duration() {
        let mut config = DsqlPoolConfig {
            lease_duration: Duration::ZERO,
            ..DsqlPoolConfig::default()
        };
        assert!(config.validate().is_err());

        config.lease_duration = Duration::seconds(-1);
        assert!(config.validate().is_err());

        config.lease_duration = Duration::seconds(1);
        config.validate().unwrap();
    }

    #[test]
    fn validates_projection_partition_count() {
        let mut config = DsqlPoolConfig {
            projection_partition_count: 0,
            ..DsqlPoolConfig::default()
        };
        assert!(config.validate().is_err());

        config.projection_partition_count = 4;
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_reservoir_values() {
        let mut config = ReservoirConfig {
            target_ready: 0,
            ..ReservoirConfig::default()
        };
        assert!(config.validate().is_err());

        config = ReservoirConfig {
            target_ready: 1,
            base_lifetime: Duration::minutes(59),
            lifetime_jitter: Duration::minutes(1),
            ..ReservoirConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn detects_region_from_dsql_endpoint() {
        assert_eq!(
            detect_region_from_endpoint("cluster.dsql.us-east-1.on.aws"),
            Some("us-east-1".to_owned())
        );
    }

    #[test]
    fn preserves_role_arns_without_requiring_them() {
        let auth = DsqlAuthConfig {
            endpoint: "cluster.dsql.eu-west-1.on.aws".to_owned(),
            runtime_role_arn: Some("arn:aws:iam::123456789012:role/runtime".to_owned()),
            ..DsqlAuthConfig::default()
        };
        assert_eq!(
            auth.role_arn_for(DsqlRole::Runtime),
            Some("arn:aws:iam::123456789012:role/runtime")
        );
        assert_eq!(auth.role_arn_for(DsqlRole::Admin), None);
    }

    #[test]
    fn config_structs_round_trip_through_serde() {
        let auth = DsqlAuthConfig {
            endpoint: "cluster.dsql.eu-west-1.on.aws".to_owned(),
            region: Some("eu-west-1".to_owned()),
            ..DsqlAuthConfig::default()
        };
        let encoded = postcard::to_allocvec(&auth).unwrap();
        let decoded: DsqlAuthConfig = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, auth);
    }
}
