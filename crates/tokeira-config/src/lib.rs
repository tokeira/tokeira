pub mod loader;
pub use loader::{ConfigLoaderError, load_config, write_config_toml};

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(clap::Parser, Debug)]
#[command(name = "tokeirad", disable_version_flag = true)]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Print resolved configuration as TOML and exit.
    #[arg(long)]
    pub dump_config: bool,

    /// Print deterministic build provenance and exit.
    #[arg(long)]
    pub version: bool,

    /// Include all build provenance fields with `--version`.
    #[arg(long, requires = "version")]
    pub verbose: bool,

    /// Render `--version` output as stable JSON.
    #[arg(long, requires = "version")]
    pub json: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokeiraConfig {
    #[serde(default)]
    pub infrastructure: InfrastructureConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub capacity: CapacityConfig,
    #[serde(default)]
    pub emergency: EmergencyConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfrastructureConfig {
    #[serde(default = "default_cluster_name")]
    pub cluster_name: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub storage: ConfigStorageKind,
    #[serde(default)]
    pub dsql: DsqlInfraConfig,
    #[serde(default)]
    pub placement: PlacementMembershipConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementMembershipConfig {
    #[serde(default)]
    pub controller_endpoint: Option<String>,
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_reconnect_base_delay_ms")]
    pub reconnect_base_delay_ms: u64,
    #[serde(default = "default_reconnect_max_delay_ms")]
    pub reconnect_max_delay_ms: u64,
    #[serde(default = "default_node_host")]
    pub node_host: String,
    #[serde(default)]
    pub node_port: Option<u16>,
    #[serde(default = "default_placement_shard_count")]
    pub shard_count: u32,
    #[serde(default = "default_placement_bundle_count")]
    pub bundle_count: u32,
    #[serde(default = "default_placement_partition_count")]
    pub partition_count: u32,
    #[serde(default = "default_placement_hash_version")]
    pub hash_version: u32,
    #[serde(default = "default_routing_max_retries")]
    pub routing_max_retries: usize,
}

/// Server-side storage backend selector.
///
/// This lives in `tokeirad.toml` and is distinct from the deployment-layer
/// `tokeira_orchestrator::StorageKind` used by `tkr deployment create`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigStorageKind {
    /// Use the process-local in-memory repository and visibility store.
    #[default]
    InMemory,
    /// Use Aurora DSQL for run history, projection log, and visibility.
    Dsql,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlInfraConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub admin_role_arn: Option<String>,
    #[serde(default)]
    pub runtime_role_arn: Option<String>,
    #[serde(default)]
    pub readonly_role_arn: Option<String>,
    /// DynamoDB table name for distributed rate limiting.
    /// Written back by `tkr infra apply --module dsql`.
    #[serde(default)]
    pub rate_limiter_table: Option<String>,
    /// DynamoDB table name for distributed connection slot leasing.
    /// Written back by `tkr infra apply --module dsql`.
    #[serde(default)]
    pub conn_lease_table: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default = "default_true")]
    pub metrics_enabled: bool,
    #[serde(default)]
    pub otlp_enabled: bool,
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
    #[serde(default = "default_otlp_protocol")]
    pub otlp_protocol: OtlpProtocol,
    #[serde(default = "default_sample_rate")]
    pub trace_sample_rate: f64,
    #[serde(default = "default_log_format")]
    pub log_format: LogFormatConfig,
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
    #[serde(default)]
    pub otlp_metrics: OtlpMetricsConfig,
    #[serde(default = "default_leak_detection_deadline_ms")]
    pub leak_detection_deadline_ms: u64,
    #[serde(default)]
    pub alert_thresholds: AlertThresholdConfig,
    #[serde(default = "default_true")]
    pub dashboard_provisioning_enabled: bool,
    #[serde(default = "default_runbook_base_url")]
    pub runbook_base_url: String,
    #[serde(default = "default_smoke_test_timeout_ms")]
    pub smoke_test_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpMetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "default_otlp_protocol")]
    pub protocol: OtlpProtocol,
    #[serde(default = "default_otlp_metrics_max_buffered_batches")]
    pub max_buffered_batches: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertThresholdConfig {
    #[serde(default = "default_dsql_reservoir_exhaustion_ratio")]
    pub dsql_reservoir_exhaustion_ratio: f64,
    #[serde(default = "default_dsql_occ_conflict_rate_per_sec")]
    pub dsql_occ_conflict_rate_per_sec: f64,
    #[serde(default = "default_projection_checkpoint_lag_seconds")]
    pub projection_checkpoint_lag_seconds: u64,
    #[serde(default = "default_autoscaler_metric_staleness_seconds")]
    pub autoscaler_metric_staleness_seconds: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    Http,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormatConfig {
    #[default]
    Text,
    Json,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    #[serde(default = "default_retention_days")]
    pub default_retention_days: u32,
    #[serde(default)]
    pub namespace_creation: NamespaceCreationPolicy,
    #[serde(default)]
    pub quotas: QuotasConfig,
    #[serde(default)]
    pub compatibility: CompatibilityConfig,
}

/// Operator overrides that intentionally diverge from the pinned Temporal
/// behavioural baseline (`TEMPORAL_SERVER_COMPAT`).
///
/// Each flag, when set, turns on a feature that sits *ahead* of the baseline
/// behavioural claim. They default off so an unconfigured server matches the
/// baseline exactly; enabling one is a declared deviation that the compatibility
/// report surfaces rather than hides.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompatibilityConfig {
    /// Enable standalone (CHASM) activities — the public `*ActivityExecution`
    /// RPCs. Off by default: the baseline (`v1.31.0`) gates them off and answers
    /// `UNIMPLEMENTED` (`chasm/lib/activity/frontend.go @ v1.31.0`). When `true`,
    /// tokeira serves them — a deliberate deviation ahead of the baseline.
    #[serde(default)]
    pub enable_standalone_activities: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NamespaceCreationPolicy {
    #[default]
    Open,
    Controlled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotasConfig {
    #[serde(default = "default_max_workflow_timeout")]
    pub max_workflow_timeout_seconds: u64,
    #[serde(default = "default_max_signal_payload")]
    pub max_signal_payload_bytes: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    #[serde(default)]
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub dsql: DsqlCapacityConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceConfig {
    #[serde(default = "default_target_wf_starts")]
    pub target_workflow_starts_per_second: u32,
    #[serde(default = "default_target_p99")]
    pub target_p99_wft_latency_ms: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlCapacityConfig {
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_conn_rate")]
    pub connection_rate_per_second: u32,
    #[serde(default = "default_burst")]
    pub burst_capacity: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmergencyConfig {
    #[serde(default)]
    pub disable_stickiness: bool,
    #[serde(default)]
    pub freeze_projection: bool,
    #[serde(default)]
    pub cap_poll_admission: Option<u32>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("failed to serialize config as TOML: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("config validation failed:\n{}", .0.iter().map(|e| format!("  - {e}")).collect::<Vec<_>>().join("\n"))]
    Validation(Vec<ValidationError>),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("{field}: {message}")]
    Field { field: String, message: String },
}

impl Default for InfrastructureConfig {
    fn default() -> Self {
        Self {
            cluster_name: default_cluster_name(),
            region: default_region(),
            storage: ConfigStorageKind::InMemory,
            dsql: DsqlInfraConfig::default(),
            placement: PlacementMembershipConfig::default(),
            network: NetworkConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl Default for PlacementMembershipConfig {
    fn default() -> Self {
        Self {
            controller_endpoint: None,
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
            reconnect_base_delay_ms: default_reconnect_base_delay_ms(),
            reconnect_max_delay_ms: default_reconnect_max_delay_ms(),
            node_host: default_node_host(),
            node_port: None,
            shard_count: default_placement_shard_count(),
            bundle_count: default_placement_bundle_count(),
            partition_count: default_placement_partition_count(),
            hash_version: default_placement_hash_version(),
            routing_max_retries: default_routing_max_retries(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            metrics_addr: default_metrics_addr(),
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            otlp_enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            otlp_protocol: OtlpProtocol::Grpc,
            trace_sample_rate: default_sample_rate(),
            log_format: LogFormatConfig::Text,
            log_filter: default_log_filter(),
            otlp_metrics: OtlpMetricsConfig::default(),
            leak_detection_deadline_ms: default_leak_detection_deadline_ms(),
            alert_thresholds: AlertThresholdConfig::default(),
            dashboard_provisioning_enabled: true,
            runbook_base_url: default_runbook_base_url(),
            smoke_test_timeout_ms: default_smoke_test_timeout_ms(),
        }
    }
}

impl Default for OtlpMetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            protocol: OtlpProtocol::Grpc,
            max_buffered_batches: default_otlp_metrics_max_buffered_batches(),
        }
    }
}

impl Default for AlertThresholdConfig {
    fn default() -> Self {
        Self {
            dsql_reservoir_exhaustion_ratio: default_dsql_reservoir_exhaustion_ratio(),
            dsql_occ_conflict_rate_per_sec: default_dsql_occ_conflict_rate_per_sec(),
            projection_checkpoint_lag_seconds: default_projection_checkpoint_lag_seconds(),
            autoscaler_metric_staleness_seconds: default_autoscaler_metric_staleness_seconds(),
        }
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            default_retention_days: default_retention_days(),
            namespace_creation: NamespaceCreationPolicy::Open,
            quotas: QuotasConfig::default(),
            compatibility: CompatibilityConfig::default(),
        }
    }
}

impl Default for QuotasConfig {
    fn default() -> Self {
        Self {
            max_workflow_timeout_seconds: default_max_workflow_timeout(),
            max_signal_payload_bytes: default_max_signal_payload(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            target_workflow_starts_per_second: default_target_wf_starts(),
            target_p99_wft_latency_ms: default_target_p99(),
        }
    }
}

impl Default for DsqlCapacityConfig {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            connection_rate_per_second: default_conn_rate(),
            burst_capacity: default_burst(),
        }
    }
}

impl TokeiraConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let mut config: TokeiraConfig = toml::from_str(&content)?;
        config.apply_storage_defaults();
        config.validate()?;
        Ok(config)
    }

    pub fn resolve(config_path: Option<&Path>) -> Result<(Self, &'static str), ConfigError> {
        if let Some(path) = config_path {
            return Ok((Self::load(path)?, "cli --config"));
        }
        if let Ok(env_path) = std::env::var("TOKEIRA_CONFIG") {
            return Ok((Self::load(Path::new(&env_path))?, "TOKEIRA_CONFIG env"));
        }
        let mut config = Self::default();
        config.apply_storage_defaults();
        config.validate()?;
        Ok((config, "defaults"))
    }

    pub fn apply_storage_defaults(&mut self) {
        if self.infrastructure.storage == ConfigStorageKind::Dsql {
            if self.infrastructure.placement.shard_count == default_placement_shard_count() {
                self.infrastructure.placement.shard_count = 32;
            }
            if self.infrastructure.placement.partition_count == default_placement_partition_count()
            {
                self.infrastructure.placement.partition_count = 4;
            }
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut errors = Vec::new();

        let retention = self.policy.default_retention_days;
        if !(1..=36_500).contains(&retention) {
            errors.push(ValidationError::Field {
                field: "policy.default_retention_days".to_string(),
                message: format!("must be between 1 and 36500, got {retention}"),
            });
        }
        if self.capacity.performance.target_workflow_starts_per_second == 0 {
            errors.push(ValidationError::Field {
                field: "capacity.performance.target_workflow_starts_per_second".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if self.capacity.performance.target_p99_wft_latency_ms == 0 {
            errors.push(ValidationError::Field {
                field: "capacity.performance.target_p99_wft_latency_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        let sample_rate = self.infrastructure.observability.trace_sample_rate;
        if !(0.0..=1.0).contains(&sample_rate) || !sample_rate.is_finite() {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.trace_sample_rate".to_string(),
                message: format!("must be between 0.0 and 1.0, got {sample_rate}"),
            });
        }
        let observability = &self.infrastructure.observability;
        if observability.otlp_metrics.enabled
            && observability
                .otlp_metrics
                .endpoint
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.otlp_metrics.endpoint".to_string(),
                message: "is required when OTLP metrics are enabled".to_string(),
            });
        }
        if observability.otlp_metrics.max_buffered_batches == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.otlp_metrics.max_buffered_batches".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if observability.leak_detection_deadline_ms == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.leak_detection_deadline_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if observability.smoke_test_timeout_ms == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.smoke_test_timeout_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if observability.runbook_base_url.trim().is_empty() {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.runbook_base_url".to_string(),
                message: "must not be empty".to_string(),
            });
        }
        let alert_thresholds = &observability.alert_thresholds;
        if !(0.0..=1.0).contains(&alert_thresholds.dsql_reservoir_exhaustion_ratio)
            || !alert_thresholds.dsql_reservoir_exhaustion_ratio.is_finite()
        {
            errors.push(ValidationError::Field {
                field:
                    "infrastructure.observability.alert_thresholds.dsql_reservoir_exhaustion_ratio"
                        .to_string(),
                message: "must be finite and between 0.0 and 1.0".to_string(),
            });
        }
        if alert_thresholds.dsql_occ_conflict_rate_per_sec <= 0.0
            || !alert_thresholds.dsql_occ_conflict_rate_per_sec.is_finite()
        {
            errors.push(ValidationError::Field {
                field:
                    "infrastructure.observability.alert_thresholds.dsql_occ_conflict_rate_per_sec"
                        .to_string(),
                message: "must be finite and positive".to_string(),
            });
        }
        if alert_thresholds.projection_checkpoint_lag_seconds == 0 {
            errors.push(ValidationError::Field {
                field:
                    "infrastructure.observability.alert_thresholds.projection_checkpoint_lag_seconds"
                        .to_string(),
                message: "must be positive".to_string(),
            });
        }
        if alert_thresholds.autoscaler_metric_staleness_seconds == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.observability.alert_thresholds.autoscaler_metric_staleness_seconds".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if self.infrastructure.storage == ConfigStorageKind::Dsql
            && self
                .infrastructure
                .dsql
                .endpoint
                .as_deref()
                .unwrap_or("")
                .is_empty()
        {
            errors.push(ValidationError::Field {
                field: "infrastructure.dsql.endpoint".to_string(),
                message: "must be set when infrastructure.storage is dsql; run `tkr infra apply --module dsql` first".to_string(),
            });
        }
        if self
            .infrastructure
            .network
            .grpc_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            errors.push(ValidationError::Field {
                field: "infrastructure.network.grpc_addr".to_string(),
                message: format!(
                    "not a valid socket address: {:?}",
                    self.infrastructure.network.grpc_addr
                ),
            });
        }
        if self
            .infrastructure
            .network
            .metrics_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            errors.push(ValidationError::Field {
                field: "infrastructure.network.metrics_addr".to_string(),
                message: format!(
                    "not a valid socket address: {:?}",
                    self.infrastructure.network.metrics_addr
                ),
            });
        }
        let placement = &self.infrastructure.placement;
        if placement.heartbeat_interval_ms == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.heartbeat_interval_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if placement.reconnect_base_delay_ms == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.reconnect_base_delay_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if placement.reconnect_max_delay_ms < placement.reconnect_base_delay_ms {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.reconnect_max_delay_ms".to_string(),
                message: "must be greater than or equal to reconnect_base_delay_ms".to_string(),
            });
        }
        if placement.node_host.is_empty() {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.node_host".to_string(),
                message: "must not be empty".to_string(),
            });
        }
        if placement.shard_count == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.shard_count".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if placement.bundle_count == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.bundle_count".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if placement.partition_count == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.partition_count".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if placement.routing_max_retries == 0 {
            errors.push(ValidationError::Field {
                field: "infrastructure.placement.routing_max_retries".to_string(),
                message: "must be positive".to_string(),
            });
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Validation(errors))
        }
    }

    pub fn emergency_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.emergency.disable_stickiness {
            warnings.push("emergency override active: disable_stickiness = true".to_string());
        }
        if self.emergency.freeze_projection {
            warnings.push("emergency override active: freeze_projection = true".to_string());
        }
        if let Some(cap) = self.emergency.cap_poll_admission {
            warnings.push(format!(
                "emergency override active: cap_poll_admission = {cap}"
            ));
        }
        warnings
    }

    pub fn to_toml(&self) -> Result<String, ConfigError> {
        Ok(toml::to_string_pretty(self)?)
    }

    pub fn to_redacted_json(&self) -> serde_json::Value {
        let mut json = serde_json::to_value(self).expect("TokeiraConfig is serializable");
        redact_sensitive_fields(&mut json);
        let warnings = self.emergency_warnings();
        if !warnings.is_empty()
            && let serde_json::Value::Object(map) = &mut json
        {
            map.insert(
                "_warnings".to_string(),
                serde_json::Value::Array(
                    warnings
                        .into_iter()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            );
        }
        json
    }
}

fn redact_sensitive_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                let key = key.to_lowercase();
                if (key.contains("endpoint") || key.contains("arn")) && !value.is_null() {
                    *value = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_sensitive_fields(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_sensitive_fields(value);
            }
        }
        _ => {}
    }
}

fn default_cluster_name() -> String {
    "tokeira-local".to_string()
}

fn default_node_host() -> String {
    "127.0.0.1".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_heartbeat_interval_ms() -> u64 {
    5_000
}

fn default_reconnect_base_delay_ms() -> u64 {
    1_000
}

fn default_reconnect_max_delay_ms() -> u64 {
    30_000
}

fn default_placement_shard_count() -> u32 {
    1
}

fn default_placement_bundle_count() -> u32 {
    1
}

fn default_placement_partition_count() -> u32 {
    16
}

fn default_placement_hash_version() -> u32 {
    1
}

fn default_routing_max_retries() -> usize {
    3
}

fn default_grpc_addr() -> String {
    "0.0.0.0:7233".to_string()
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}

fn default_true() -> bool {
    true
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_otlp_protocol() -> OtlpProtocol {
    OtlpProtocol::Grpc
}

fn default_sample_rate() -> f64 {
    1.0
}

fn default_log_format() -> LogFormatConfig {
    LogFormatConfig::Text
}

fn default_log_filter() -> String {
    "info".to_string()
}

fn default_otlp_metrics_max_buffered_batches() -> usize {
    1024
}

fn default_leak_detection_deadline_ms() -> u64 {
    30_000
}

fn default_runbook_base_url() -> String {
    "docs/runbooks/observability".to_string()
}

fn default_smoke_test_timeout_ms() -> u64 {
    30_000
}

fn default_dsql_reservoir_exhaustion_ratio() -> f64 {
    0.90
}

fn default_dsql_occ_conflict_rate_per_sec() -> f64 {
    10.0
}

fn default_projection_checkpoint_lag_seconds() -> u64 {
    60
}

fn default_autoscaler_metric_staleness_seconds() -> u64 {
    30
}

fn default_retention_days() -> u32 {
    30
}

fn default_max_workflow_timeout() -> u64 {
    315_360_000
}

fn default_max_signal_payload() -> u32 {
    4_194_304
}

fn default_target_wf_starts() -> u32 {
    1_000
}

fn default_target_p99() -> u32 {
    50
}

fn default_max_connections() -> u32 {
    10_000
}

fn default_conn_rate() -> u32 {
    100
}

fn default_burst() -> u32 {
    1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use proptest::prelude::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parses_version_flags_without_config() {
        let cli = Cli::try_parse_from(["tokeirad", "--version", "--verbose"]).unwrap();
        assert!(cli.version);
        assert!(cli.verbose);
        assert!(!cli.json);

        let cli = Cli::try_parse_from(["tokeirad", "--version", "--json"]).unwrap();
        assert!(cli.version);
        assert!(cli.json);
    }

    #[test]
    fn empty_toml_uses_defaults() {
        let config: TokeiraConfig = toml::from_str("").unwrap();
        assert_eq!(config, TokeiraConfig::default());
        config.validate().unwrap();
    }

    #[test]
    fn dsql_storage_defaults_promote_legacy_values() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.storage = ConfigStorageKind::Dsql;

        config.apply_storage_defaults();

        assert_eq!(config.infrastructure.placement.shard_count, 32);
        assert_eq!(config.infrastructure.placement.partition_count, 4);
    }

    #[test]
    fn dsql_storage_defaults_preserve_non_legacy_values() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.storage = ConfigStorageKind::Dsql;
        config.infrastructure.placement.shard_count = 2;
        config.infrastructure.placement.partition_count = 8;

        config.apply_storage_defaults();

        assert_eq!(config.infrastructure.placement.shard_count, 2);
        assert_eq!(config.infrastructure.placement.partition_count, 8);
    }

    #[test]
    fn rejects_unknown_fields() {
        let error = toml::from_str::<TokeiraConfig>("[infrastructure]\nunknown = true\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown"));
    }

    #[test]
    fn collects_validation_errors() {
        let mut config = TokeiraConfig::default();
        config.policy.default_retention_days = 0;
        config
            .capacity
            .performance
            .target_workflow_starts_per_second = 0;
        config.infrastructure.observability.trace_sample_rate = 2.0;

        match config.validate().unwrap_err() {
            ConfigError::Validation(errors) => assert_eq!(errors.len(), 3),
            other => panic!("expected validation errors, got {other:?}"),
        }
    }

    #[test]
    fn observability_defaults_are_valid() {
        let config = TokeiraConfig::default();

        config.validate().unwrap();
        assert!(config.infrastructure.observability.metrics_enabled);
        assert!(!config.infrastructure.observability.otlp_metrics.enabled);
        assert!(
            config
                .infrastructure
                .observability
                .dashboard_provisioning_enabled
        );
        assert_eq!(
            config
                .infrastructure
                .observability
                .alert_thresholds
                .projection_checkpoint_lag_seconds,
            60
        );
    }

    #[test]
    fn otlp_metrics_enabled_requires_endpoint() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.observability.otlp_metrics.enabled = true;

        let errors = match config.validate().unwrap_err() {
            ConfigError::Validation(errors) => errors,
            other => panic!("expected validation errors, got {other:?}"),
        };
        assert!(errors.iter().any(|error| match error {
            ValidationError::Field { field, .. } =>
                field == "infrastructure.observability.otlp_metrics.endpoint",
        }));
    }

    #[test]
    fn toml_round_trip_preserves_config() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.cluster_name = "prod".to_string();
        config.infrastructure.placement.controller_endpoint =
            Some("http://127.0.0.1:7240".to_string());
        config.infrastructure.placement.bundle_count = 64;
        config.infrastructure.observability.log_format = LogFormatConfig::Json;
        config.emergency.cap_poll_admission = Some(10);

        let encoded = config.to_toml().unwrap();
        let decoded: TokeiraConfig = toml::from_str(&encoded).unwrap();
        assert_eq!(decoded, config);
    }

    #[test]
    fn placement_config_rejects_invalid_timing_and_counts() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.placement.heartbeat_interval_ms = 0;
        config.infrastructure.placement.reconnect_base_delay_ms = 10;
        config.infrastructure.placement.reconnect_max_delay_ms = 1;
        config.infrastructure.placement.bundle_count = 0;
        config.infrastructure.placement.routing_max_retries = 0;

        match config.validate().unwrap_err() {
            ConfigError::Validation(errors) => {
                assert!(
                    errors
                        .iter()
                        .any(|error| { error.to_string().contains("heartbeat_interval_ms") })
                );
                assert!(
                    errors
                        .iter()
                        .any(|error| { error.to_string().contains("reconnect_max_delay_ms") })
                );
                assert!(
                    errors
                        .iter()
                        .any(|error| error.to_string().contains("bundle_count"))
                );
                assert!(
                    errors
                        .iter()
                        .any(|error| { error.to_string().contains("routing_max_retries") })
                );
            }
            other => panic!("expected validation errors, got {other:?}"),
        }
    }

    #[test]
    fn resolve_prefers_cli_over_env_and_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cli_path = write_temp_config("cli", "cluster_name = \"cli\"");
        let env_path = write_temp_config("env", "cluster_name = \"env\"");
        unsafe {
            std::env::set_var("TOKEIRA_CONFIG", &env_path);
        }

        let (config, source) = TokeiraConfig::resolve(Some(cli_path.as_path())).unwrap();
        assert_eq!(source, "cli --config");
        assert_eq!(config.infrastructure.cluster_name, "cli");

        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
        }
        let _ = std::fs::remove_file(cli_path);
        let _ = std::fs::remove_file(env_path);
    }

    #[test]
    fn resolve_uses_env_before_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let env_path = write_temp_config("env-default", "cluster_name = \"env\"");
        unsafe {
            std::env::set_var("TOKEIRA_CONFIG", &env_path);
        }

        let (config, source) = TokeiraConfig::resolve(None).unwrap();
        assert_eq!(source, "TOKEIRA_CONFIG env");
        assert_eq!(config.infrastructure.cluster_name, "env");

        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
        }
        let _ = std::fs::remove_file(env_path);
    }

    #[test]
    fn redaction_preserves_listener_addresses() {
        let mut config = TokeiraConfig::default();
        config.infrastructure.dsql.endpoint = Some("secret-endpoint".to_string());
        let json = config.to_redacted_json();

        assert_eq!(json["infrastructure"]["dsql"]["endpoint"], "[redacted]");
        assert_eq!(
            json["infrastructure"]["network"]["grpc_addr"],
            config.infrastructure.network.grpc_addr
        );
        assert_eq!(
            json["infrastructure"]["network"]["metrics_addr"],
            config.infrastructure.network.metrics_addr
        );
    }

    #[test]
    fn emergency_warnings_are_added_to_redacted_json() {
        let mut config = TokeiraConfig::default();
        assert!(config.to_redacted_json().get("_warnings").is_none());
        config.emergency.disable_stickiness = true;
        assert!(config.to_redacted_json().get("_warnings").is_some());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_toml_round_trip(config in arb_valid_config()) {
            let encoded = config.to_toml().unwrap();
            let decoded: TokeiraConfig = toml::from_str(&encoded).unwrap();
            prop_assert_eq!(decoded, config);
        }

        #[test]
        fn property_unknown_fields_are_rejected(key in "[a-z_]{1,16}") {
            prop_assume!(!matches!(
                key.as_str(),
                "cluster_name" | "region" | "dsql" | "network" | "observability"
            ));
            let toml = format!("[infrastructure]\n{key} = true\n");
            prop_assert!(toml::from_str::<TokeiraConfig>(&toml).is_err());
        }

        #[test]
        fn property_retention_bounds(value in any::<u32>()) {
            let mut config = TokeiraConfig::default();
            config.policy.default_retention_days = value;
            prop_assert_eq!(config.validate().is_err(), !(1..=36_500).contains(&value));
        }

        #[test]
        fn property_positive_integer_validation(starts in any::<u32>(), latency in any::<u32>()) {
            let mut config = TokeiraConfig::default();
            config.capacity.performance.target_workflow_starts_per_second = starts;
            config.capacity.performance.target_p99_wft_latency_ms = latency;
            prop_assert_eq!(config.validate().is_err(), starts == 0 || latency == 0);
        }

        #[test]
        fn property_trace_sample_rate_bounds(rate in -10.0f64..10.0) {
            let mut config = TokeiraConfig::default();
            config.infrastructure.observability.trace_sample_rate = rate;
            prop_assert_eq!(config.validate().is_err(), !(0.0..=1.0).contains(&rate));
        }

        #[test]
        fn property_validation_error_collection(
            bad_retention in any::<bool>(),
            bad_starts in any::<bool>(),
            bad_latency in any::<bool>(),
            bad_rate in any::<bool>(),
        ) {
            let mut config = TokeiraConfig::default();
            let mut expected = 0;
            if bad_retention {
                config.policy.default_retention_days = 0;
                expected += 1;
            }
            if bad_starts {
                config.capacity.performance.target_workflow_starts_per_second = 0;
                expected += 1;
            }
            if bad_latency {
                config.capacity.performance.target_p99_wft_latency_ms = 0;
                expected += 1;
            }
            if bad_rate {
                config.infrastructure.observability.trace_sample_rate = 2.0;
                expected += 1;
            }

            match (expected, config.validate()) {
                (0, Ok(())) => {}
                (_, Err(ConfigError::Validation(errors))) => prop_assert_eq!(errors.len(), expected),
                (_, other) => prop_assert!(false, "unexpected validation result: {other:?}"),
            }
        }

        #[test]
        fn property_sensitive_field_redaction(endpoint in "[A-Za-z0-9._:/-]{1,64}") {
            let mut config = TokeiraConfig::default();
            config.infrastructure.dsql.endpoint = Some(endpoint);
            let json = config.to_redacted_json();

            prop_assert_eq!(
                json["infrastructure"]["dsql"]["endpoint"].as_str(),
                Some("[redacted]")
            );
            prop_assert_eq!(
                json["infrastructure"]["network"]["grpc_addr"].as_str(),
                Some(config.infrastructure.network.grpc_addr.as_str())
            );
            prop_assert_eq!(
                json["infrastructure"]["network"]["metrics_addr"].as_str(),
                Some(config.infrastructure.network.metrics_addr.as_str())
            );
        }

        #[test]
        fn property_emergency_warnings(
            disable_stickiness in any::<bool>(),
            freeze_projection in any::<bool>(),
            cap_poll_admission in proptest::option::of(1u32..1000),
        ) {
            let mut config = TokeiraConfig::default();
            config.emergency.disable_stickiness = disable_stickiness;
            config.emergency.freeze_projection = freeze_projection;
            config.emergency.cap_poll_admission = cap_poll_admission;
            let json = config.to_redacted_json();
            let has_override = disable_stickiness || freeze_projection || cap_poll_admission.is_some();

            prop_assert_eq!(json.get("_warnings").is_some(), has_override);
        }

        #[test]
        fn property_dsql_promotes_legacy_defaults_by_value(
            shard_count in 1u32..128,
            partition_count in 1u32..128,
        ) {
            let mut config = TokeiraConfig::default();
            config.infrastructure.storage = ConfigStorageKind::Dsql;
            config.infrastructure.placement.shard_count = shard_count;
            config.infrastructure.placement.partition_count = partition_count;

            config.apply_storage_defaults();

            let expected_shard_count = if shard_count == 1 { 32 } else { shard_count };
            let expected_partition_count = if partition_count == 16 { 4 } else { partition_count };
            prop_assert_eq!(config.infrastructure.placement.shard_count, expected_shard_count);
            prop_assert_eq!(config.infrastructure.placement.partition_count, expected_partition_count);
        }
    }

    fn arb_valid_config() -> impl Strategy<Value = TokeiraConfig> {
        (
            "[a-zA-Z0-9_-]{1,32}",
            "[a-z]{2}-[a-z]+-[0-9]",
            any::<bool>(),
            any::<bool>(),
            prop_oneof![Just(OtlpProtocol::Grpc), Just(OtlpProtocol::Http)],
            0.0f64..=1.0,
            prop_oneof![Just(LogFormatConfig::Text), Just(LogFormatConfig::Json)],
            1u32..=36_500,
            1u32..=100_000,
            1u32..=10_000,
        )
            .prop_map(
                |(
                    cluster_name,
                    region,
                    metrics_enabled,
                    otlp_enabled,
                    otlp_protocol,
                    trace_sample_rate,
                    log_format,
                    default_retention_days,
                    target_workflow_starts_per_second,
                    target_p99_wft_latency_ms,
                )| TokeiraConfig {
                    infrastructure: InfrastructureConfig {
                        storage: ConfigStorageKind::InMemory,
                        cluster_name,
                        region,
                        dsql: DsqlInfraConfig::default(),
                        placement: PlacementMembershipConfig::default(),
                        network: NetworkConfig::default(),
                        observability: ObservabilityConfig {
                            metrics_enabled,
                            otlp_enabled,
                            otlp_endpoint: default_otlp_endpoint(),
                            otlp_protocol,
                            trace_sample_rate,
                            log_format,
                            log_filter: default_log_filter(),
                            ..ObservabilityConfig::default()
                        },
                    },
                    policy: PolicyConfig {
                        default_retention_days,
                        namespace_creation: NamespaceCreationPolicy::Open,
                        quotas: QuotasConfig::default(),
                    },
                    capacity: CapacityConfig {
                        performance: PerformanceConfig {
                            target_workflow_starts_per_second,
                            target_p99_wft_latency_ms,
                        },
                        dsql: DsqlCapacityConfig::default(),
                    },
                    emergency: EmergencyConfig::default(),
                },
            )
    }

    fn write_temp_config(name: &str, infrastructure_body: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "tokeira-config-test-{name}-{}-{}.toml",
            std::process::id(),
            std::thread::current().name().unwrap_or("thread")
        ));
        std::fs::write(&path, format!("[infrastructure]\n{infrastructure_body}\n")).unwrap();
        path
    }
}
