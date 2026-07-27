//! Operator-facing metadata for every field in the strict production schema.
//!
//! The catalog is deliberately adjacent to [`crate::TokeiraConfig`]. Public
//! configuration documentation and the annotated example consume this data
//! instead of maintaining another informal list of accepted fields. Paths use
//! `[]` for array-of-table elements so one record documents every instance.

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};

/// Product-policy class for one production configuration field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConfigFieldClass {
    /// Exposes a pinned Temporal default without changing its stock posture.
    StockParity,
    /// Selects an available Temporal behavior through Tokeira's typed policy.
    ConfiguredParity,
    /// Selects behavior or topology that is specific to Tokeira.
    TokeiraNative,
    /// Defines deployment infrastructure or process connectivity.
    Infrastructure,
    /// Supplies an operator-owned capacity target or ceiling.
    Capacity,
    /// Restricts behavior during an explicitly declared emergency.
    Emergency,
}

impl ConfigFieldClass {
    /// Stable label used by generated operator documentation.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::StockParity => "stock parity",
            Self::ConfiguredParity => "configured parity",
            Self::TokeiraNative => "Tokeira native",
            Self::Infrastructure => "infrastructure",
            Self::Capacity => "capacity",
            Self::Emergency => "emergency",
        }
    }
}

/// Documentation metadata for one accepted leaf field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigFieldDocumentation {
    /// Dot-separated path; array-of-table instances use `[]`.
    pub path: &'static str,
    /// Product-policy classification.
    pub class: ConfigFieldClass,
    /// TOML-shaped default or field-level absence marker.
    pub default: &'static str,
    /// Whether this field is required when its containing optional section exists.
    pub required: bool,
    /// Whether changing the field requires restarting `tokeirad`.
    pub restart_required: bool,
    /// Owning Feature Catalog id, when the field controls public behavior.
    pub feature_id: Option<&'static str>,
    /// Concise operator guidance.
    pub guidance: &'static str,
}

macro_rules! field {
    ($path:literal, $class:ident, $default:literal, $required:literal, $feature:expr, $guidance:literal) => {
        ConfigFieldDocumentation {
            path: $path,
            class: ConfigFieldClass::$class,
            default: $default,
            required: $required,
            restart_required: true,
            feature_id: $feature,
            guidance: $guidance,
        }
    };
}

/// Complete audited inventory of production fields accepted by `TokeiraConfig`.
///
/// Live task-queue rates and fairness-weight overrides are intentionally absent:
/// they are authored through `UpdateTaskQueueConfig`, not startup TOML.
pub const CONFIG_FIELD_CATALOG: &[ConfigFieldDocumentation] = &[
    field!(
        "infrastructure.cluster_name",
        Infrastructure,
        "\"tokeira-local\"",
        false,
        None,
        "Stable cluster label used in operator output and telemetry."
    ),
    field!(
        "infrastructure.region",
        Infrastructure,
        "\"us-east-1\"",
        false,
        None,
        "AWS region used by regional infrastructure integrations."
    ),
    field!(
        "infrastructure.storage",
        Infrastructure,
        "\"in-memory\"",
        false,
        None,
        "Select in-memory development storage or Aurora DSQL."
    ),
    field!(
        "infrastructure.dsql.endpoint",
        Infrastructure,
        "<unset>",
        true,
        None,
        "Required when storage is dsql; normally written by the provisioner."
    ),
    field!(
        "infrastructure.dsql.region",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Optional DSQL signing region; defaults to infrastructure.region."
    ),
    field!(
        "infrastructure.dsql.admin_role_arn",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Provisioning identity for DSQL administrative operations."
    ),
    field!(
        "infrastructure.dsql.runtime_role_arn",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Runtime identity for authoritative DSQL reads and writes."
    ),
    field!(
        "infrastructure.dsql.readonly_role_arn",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Read-only identity for inspection tooling."
    ),
    field!(
        "infrastructure.dsql.rate_limiter_table",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Provisioned DynamoDB table for distributed rate limiting."
    ),
    field!(
        "infrastructure.dsql.conn_lease_table",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Provisioned DynamoDB table for distributed connection-slot leases."
    ),
    field!(
        "infrastructure.placement.controller_endpoint",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Placement-controller endpoint; absence selects the single-node path."
    ),
    field!(
        "infrastructure.placement.heartbeat_interval_ms",
        Infrastructure,
        "5000",
        false,
        None,
        "Node membership heartbeat interval."
    ),
    field!(
        "infrastructure.placement.reconnect_base_delay_ms",
        Infrastructure,
        "1000",
        false,
        None,
        "Initial controller reconnection delay."
    ),
    field!(
        "infrastructure.placement.reconnect_max_delay_ms",
        Infrastructure,
        "30000",
        false,
        None,
        "Maximum controller reconnection delay."
    ),
    field!(
        "infrastructure.placement.node_host",
        Infrastructure,
        "\"127.0.0.1\"",
        false,
        None,
        "Advertised node host; TOKEIRA_NODE_HOST may supply a per-pod value."
    ),
    field!(
        "infrastructure.placement.node_port",
        Infrastructure,
        "<unset>",
        false,
        None,
        "Advertised node port; defaults to the gRPC listener port."
    ),
    field!(
        "infrastructure.placement.shard_count",
        Infrastructure,
        "1",
        false,
        None,
        "Logical shard count; the DSQL profile promotes the legacy default to 32."
    ),
    field!(
        "infrastructure.placement.bundle_count",
        Infrastructure,
        "1",
        false,
        None,
        "Placement bundle count."
    ),
    field!(
        "infrastructure.placement.partition_count",
        Infrastructure,
        "16",
        false,
        None,
        "Storage partition count; the DSQL profile promotes the legacy default to 4."
    ),
    field!(
        "infrastructure.placement.hash_version",
        Infrastructure,
        "1",
        false,
        None,
        "Pinned placement hash algorithm version."
    ),
    field!(
        "infrastructure.placement.routing_max_retries",
        Infrastructure,
        "3",
        false,
        None,
        "Maximum retries after a stale placement route."
    ),
    field!(
        "infrastructure.network.grpc_addr",
        Infrastructure,
        "\"0.0.0.0:7233\"",
        false,
        None,
        "Temporal gRPC listener address."
    ),
    field!(
        "infrastructure.network.metrics_addr",
        Infrastructure,
        "\"0.0.0.0:9090\"",
        false,
        None,
        "Prometheus metrics listener address."
    ),
    field!(
        "infrastructure.observability.metrics_enabled",
        Infrastructure,
        "true",
        false,
        None,
        "Enable the Prometheus metrics endpoint."
    ),
    field!(
        "infrastructure.observability.otlp_enabled",
        Infrastructure,
        "false",
        false,
        None,
        "Enable OTLP trace export."
    ),
    field!(
        "infrastructure.observability.otlp_endpoint",
        Infrastructure,
        "\"http://localhost:4317\"",
        false,
        None,
        "OTLP trace collector endpoint."
    ),
    field!(
        "infrastructure.observability.otlp_protocol",
        Infrastructure,
        "\"grpc\"",
        false,
        None,
        "OTLP trace transport protocol."
    ),
    field!(
        "infrastructure.observability.trace_sample_rate",
        Infrastructure,
        "1.0",
        false,
        None,
        "Base trace sampling ratio from zero through one."
    ),
    field!(
        "infrastructure.observability.log_format",
        Infrastructure,
        "\"text\"",
        false,
        None,
        "Structured json or human-readable text logs."
    ),
    field!(
        "infrastructure.observability.log_filter",
        Infrastructure,
        "\"info\"",
        false,
        None,
        "Tracing filter applied at process startup."
    ),
    field!(
        "infrastructure.observability.otlp_metrics.enabled",
        Infrastructure,
        "false",
        false,
        None,
        "Enable OTLP metrics export."
    ),
    field!(
        "infrastructure.observability.otlp_metrics.endpoint",
        Infrastructure,
        "<unset>",
        true,
        None,
        "Collector endpoint required when OTLP metrics are enabled."
    ),
    field!(
        "infrastructure.observability.otlp_metrics.protocol",
        Infrastructure,
        "\"grpc\"",
        false,
        None,
        "OTLP metrics transport protocol."
    ),
    field!(
        "infrastructure.observability.otlp_metrics.max_buffered_batches",
        Infrastructure,
        "1024",
        false,
        None,
        "Maximum metrics batches buffered before backpressure."
    ),
    field!(
        "infrastructure.observability.leak_detection_deadline_ms",
        Infrastructure,
        "30000",
        false,
        None,
        "Deadline used by task-leak detection."
    ),
    field!(
        "infrastructure.observability.alert_thresholds.dsql_reservoir_exhaustion_ratio",
        Infrastructure,
        "0.9",
        false,
        None,
        "DSQL connection-reservoir alert ratio."
    ),
    field!(
        "infrastructure.observability.alert_thresholds.dsql_occ_conflict_rate_per_sec",
        Infrastructure,
        "10.0",
        false,
        None,
        "OCC-conflict alert rate."
    ),
    field!(
        "infrastructure.observability.alert_thresholds.projection_checkpoint_lag_seconds",
        Infrastructure,
        "60",
        false,
        None,
        "Projection checkpoint-lag alert threshold."
    ),
    field!(
        "infrastructure.observability.alert_thresholds.autoscaler_metric_staleness_seconds",
        Infrastructure,
        "30",
        false,
        None,
        "Autoscaler metric-staleness alert threshold."
    ),
    field!(
        "infrastructure.observability.dashboard_provisioning_enabled",
        Infrastructure,
        "true",
        false,
        None,
        "Provision bundled observability dashboards."
    ),
    field!(
        "infrastructure.observability.smoke_test_timeout_ms",
        Infrastructure,
        "30000",
        false,
        None,
        "Observability smoke-test timeout."
    ),
    field!(
        "policy.default_retention_days",
        StockParity,
        "30",
        false,
        Some("namespace-management"),
        "Default workflow-history retention for newly created namespaces."
    ),
    field!(
        "policy.namespace_creation",
        ConfiguredParity,
        "\"open\"",
        false,
        Some("namespace-management"),
        "Choose open or controlled namespace creation."
    ),
    field!(
        "policy.quotas.max_workflow_timeout_seconds",
        ConfiguredParity,
        "315360000",
        false,
        Some("workflow-start"),
        "Maximum admitted workflow execution timeout."
    ),
    field!(
        "policy.quotas.max_signal_payload_bytes",
        ConfiguredParity,
        "4194304",
        false,
        Some("workflow-signal"),
        "Maximum admitted signal payload size."
    ),
    field!(
        "policy.compatibility.enable_standalone_activities",
        ConfiguredParity,
        "false",
        false,
        Some("activity-executions"),
        "Enable the v1.31.0 preview standalone-activity surface."
    ),
    field!(
        "policy.task_queues.enable_fairness",
        ConfiguredParity,
        "false",
        false,
        Some("user-fairness"),
        "Opt in to weighted User Fairness; priority remains enabled."
    ),
    field!(
        "policy.nexus_endpoint_limits.name_max_length",
        StockParity,
        "200",
        false,
        Some("nexus-admin"),
        "Maximum Nexus endpoint name length."
    ),
    field!(
        "policy.nexus_endpoint_limits.external_url_max_length",
        StockParity,
        "4096",
        false,
        Some("nexus-admin"),
        "Maximum external Nexus URL length."
    ),
    field!(
        "policy.nexus_endpoint_limits.description_max_size",
        StockParity,
        "20000",
        false,
        Some("nexus-admin"),
        "Maximum encoded Nexus endpoint description size."
    ),
    field!(
        "policy.nexus_endpoint_limits.task_queue_max_length",
        StockParity,
        "1000",
        false,
        Some("nexus-admin"),
        "Maximum Nexus worker task-queue name length."
    ),
    field!(
        "policy.nexus_endpoint_limits.list_default_page_size",
        StockParity,
        "100",
        false,
        Some("nexus-admin"),
        "Default Nexus endpoint list page size."
    ),
    field!(
        "policy.nexus_endpoint_limits.list_max_page_size",
        StockParity,
        "1000",
        false,
        Some("nexus-admin"),
        "Maximum Nexus endpoint list page size."
    ),
    field!(
        "policy.nexus_completion.http_addr",
        TokeiraNative,
        "\"127.0.0.1:7253\"",
        false,
        Some("nexus-task-transport"),
        "Inbound Nexus completion-callback listener."
    ),
    field!(
        "policy.nexus_completion.system_callback_url",
        TokeiraNative,
        "\"http://127.0.0.1:7253\"",
        false,
        Some("nexus-task-transport"),
        "Callback URL that must be reachable from Nexus workers."
    ),
    field!(
        "policy.nexus_completion.retry_initial_interval_ms",
        StockParity,
        "1000",
        false,
        Some("nexus-task-transport"),
        "Initial asynchronous Nexus completion retry interval."
    ),
    field!(
        "policy.nexus_completion.retry_max_interval_ms",
        StockParity,
        "3600000",
        false,
        Some("nexus-task-transport"),
        "Maximum asynchronous Nexus completion retry interval."
    ),
    field!(
        "policy.nexus_completion.retry_backoff_coefficient",
        StockParity,
        "2.0",
        false,
        Some("nexus-task-transport"),
        "Asynchronous Nexus completion retry multiplier."
    ),
    field!(
        "policy.nexus_completion.retry_max_attempts",
        TokeiraNative,
        "0",
        false,
        Some("nexus-task-transport"),
        "Zero preserves v1.31.0's unbounded retry horizon; positive values impose a safety cap."
    ),
    field!(
        "policy.http_api.allowed_hosts",
        StockParity,
        "[\"*\"]",
        false,
        Some("http-json-api"),
        "Case-sensitive host patterns admitted by the HTTP/JSON gateway."
    ),
    field!(
        "policy.http_api.additional_forwarded_headers",
        ConfiguredParity,
        "[]",
        false,
        Some("http-json-api"),
        "Additional exact or trailing-star header rules forwarded to gRPC."
    ),
    field!(
        "policy.authorization.principal_attribution",
        ConfiguredParity,
        "false",
        false,
        Some("authorization"),
        "Write the authenticated principal to server-authored history metadata."
    ),
    field!(
        "policy.authorization.expose_authorizer_errors",
        ConfiguredParity,
        "false",
        false,
        Some("authorization"),
        "Expose authorizer implementation failures instead of generic denial."
    ),
    field!(
        "policy.authorization.jwt.issuers[].name",
        ConfiguredParity,
        "\"\"",
        true,
        Some("authorization"),
        "Stable operator label for one issuer profile."
    ),
    field!(
        "policy.authorization.jwt.issuers[].issuer",
        ConfiguredParity,
        "\"\"",
        true,
        Some("authorization"),
        "Exact signed iss value; issuer routing is case-sensitive and exact."
    ),
    field!(
        "policy.authorization.jwt.issuers[].jwks_uri",
        ConfiguredParity,
        "\"\"",
        true,
        Some("authorization"),
        "JWKS document URI for this exact issuer."
    ),
    field!(
        "policy.authorization.jwt.issuers[].audience",
        ConfiguredParity,
        "\"\"",
        false,
        Some("authorization"),
        "Optional exact audience; blank disables audience validation."
    ),
    field!(
        "policy.authorization.jwt.issuers[].refresh_interval",
        ConfiguredParity,
        "<unset>",
        false,
        Some("authorization"),
        "Optional positive JWKS refresh duration using ms, s, m, or h."
    ),
    field!(
        "policy.authorization.jwt.issuers[].permissions_claim",
        ConfiguredParity,
        "\"permissions\"",
        false,
        Some("authorization"),
        "JWT array claim containing Temporal namespace:role grants."
    ),
    field!(
        "policy.authorization.jwt.issuers[].grants[].match_sub",
        ConfiguredParity,
        "\"\"",
        true,
        Some("authorization"),
        "Full-string subject glob for supplemental grants."
    ),
    field!(
        "policy.authorization.jwt.issuers[].grants[].grant",
        ConfiguredParity,
        "[]",
        true,
        Some("authorization"),
        "Temporal namespace:role grants for matching JWT subjects."
    ),
    field!(
        "policy.authorization.aws_iam.grants[].match_arn",
        TokeiraNative,
        "\"\"",
        true,
        Some("aws-iam-bearer-authorization"),
        "Full-string STS caller-ARN glob."
    ),
    field!(
        "policy.authorization.aws_iam.grants[].grant",
        TokeiraNative,
        "[]",
        true,
        Some("aws-iam-bearer-authorization"),
        "Temporal namespace:role grants for matching AWS identities."
    ),
    field!(
        "capacity.performance.target_workflow_starts_per_second",
        Capacity,
        "1000",
        false,
        None,
        "Capacity-planning workflow-start target."
    ),
    field!(
        "capacity.performance.target_p99_wft_latency_ms",
        Capacity,
        "50",
        false,
        None,
        "Capacity-planning workflow-task p99 latency target."
    ),
    field!(
        "capacity.dsql.max_connections",
        Capacity,
        "10000",
        false,
        None,
        "Fleet-wide DSQL connection ceiling."
    ),
    field!(
        "capacity.dsql.connection_rate_per_second",
        Capacity,
        "100",
        false,
        None,
        "Fleet-wide DSQL connection establishment rate."
    ),
    field!(
        "capacity.dsql.burst_capacity",
        Capacity,
        "1000",
        false,
        None,
        "DSQL connection-rate burst allowance."
    ),
    field!(
        "emergency.disable_stickiness",
        Emergency,
        "false",
        false,
        None,
        "Emergency-only restriction that disables sticky execution."
    ),
    field!(
        "emergency.freeze_projection",
        Emergency,
        "false",
        false,
        None,
        "Emergency-only restriction that freezes projection advancement."
    ),
    field!(
        "emergency.cap_poll_admission",
        Emergency,
        "<unset>",
        false,
        None,
        "Emergency-only cap on concurrent poll admission."
    ),
];

#[cfg(test)]
const COMPLETE_ANNOTATED_CONFIG_FIXTURE: &str = r#"
# Infrastructure fields define process topology rather than Temporal behavior.
[infrastructure]
cluster_name = "catalog-test"
region = "us-east-1"
storage = "in-memory"

[infrastructure.dsql]
endpoint = "catalog-test.dsql.us-east-1.on.aws"
region = "us-east-1"
admin_role_arn = "arn:aws:iam::123456789012:role/admin"
runtime_role_arn = "arn:aws:iam::123456789012:role/runtime"
readonly_role_arn = "arn:aws:iam::123456789012:role/readonly"
rate_limiter_table = "rate-limiter"
conn_lease_table = "connection-leases"

[infrastructure.placement]
controller_endpoint = "http://127.0.0.1:8080"
heartbeat_interval_ms = 5000
reconnect_base_delay_ms = 1000
reconnect_max_delay_ms = 30000
node_host = "127.0.0.1"
node_port = 7233
shard_count = 1
bundle_count = 1
partition_count = 16
hash_version = 1
routing_max_retries = 3

[infrastructure.network]
grpc_addr = "0.0.0.0:7233"
metrics_addr = "0.0.0.0:9090"

[infrastructure.observability]
metrics_enabled = true
otlp_enabled = false
otlp_endpoint = "http://localhost:4317"
otlp_protocol = "grpc"
trace_sample_rate = 1.0
log_format = "text"
log_filter = "info"
leak_detection_deadline_ms = 30000
dashboard_provisioning_enabled = true
smoke_test_timeout_ms = 30000

[infrastructure.observability.otlp_metrics]
enabled = false
endpoint = "http://localhost:4317"
protocol = "grpc"
max_buffered_batches = 1024

[infrastructure.observability.alert_thresholds]
dsql_reservoir_exhaustion_ratio = 0.9
dsql_occ_conflict_rate_per_sec = 10.0
projection_checkpoint_lag_seconds = 60
autoscaler_metric_staleness_seconds = 30

# Stock and configured-parity behavior is explicit under policy.
[policy]
default_retention_days = 30
namespace_creation = "open"

[policy.quotas]
max_workflow_timeout_seconds = 315360000
max_signal_payload_bytes = 4194304

[policy.compatibility]
enable_standalone_activities = false

[policy.task_queues]
# Priority remains enabled when this field is absent; User Fairness does not.
enable_fairness = false

[policy.nexus_endpoint_limits]
name_max_length = 200
external_url_max_length = 4096
description_max_size = 20000
task_queue_max_length = 1000
list_default_page_size = 100
list_max_page_size = 1000

[policy.nexus_completion]
http_addr = "127.0.0.1:7253"
# This URL must be reachable from Nexus workers.
system_callback_url = "http://127.0.0.1:7253"
retry_initial_interval_ms = 1000
retry_max_interval_ms = 3600000
retry_backoff_coefficient = 2.0
retry_max_attempts = 0

[policy.http_api]
allowed_hosts = ["*"]
additional_forwarded_headers = []

[policy.authorization]
principal_attribution = true
expose_authorizer_errors = false

[[policy.authorization.jwt.issuers]]
name = "catalog"
# Routing requires an exact match to the token's signed iss value.
issuer = "https://issuer.example/catalog"
jwks_uri = "https://issuer.example/catalog/keys"
audience = "tokeira"
refresh_interval = "1m"
permissions_claim = "permissions"

[[policy.authorization.jwt.issuers.grants]]
match_sub = "system:serviceaccount:*"
grant = ["default:worker"]

[policy.authorization.aws_iam]
[[policy.authorization.aws_iam.grants]]
match_arn = "arn:aws:sts::123456789012:assumed-role/tokeira-*"
grant = ["default:worker"]

[capacity.performance]
target_workflow_starts_per_second = 1000
target_p99_wft_latency_ms = 50

[capacity.dsql]
max_connections = 10000
connection_rate_per_second = 100
burst_capacity = 1000

[emergency]
disable_stickiness = false
freeze_projection = false
cap_poll_admission = 100
"#;

#[cfg(test)]
fn collect_leaf_paths(value: &toml::Value, prefix: &str, paths: &mut BTreeSet<String>) {
    match value {
        toml::Value::Table(table) => {
            for (key, value) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                collect_leaf_paths(value, &path, paths);
            }
        }
        toml::Value::Array(values)
            if values
                .first()
                .is_some_and(|value| matches!(value, toml::Value::Table(_))) =>
        {
            for value in values {
                collect_leaf_paths(value, &format!("{prefix}[]"), paths);
            }
        }
        _ => {
            paths.insert(prefix.to_owned());
        }
    }
}

#[cfg(test)]
fn default_leaf_values(value: &toml::Value, prefix: &str, values: &mut BTreeMap<String, String>) {
    match value {
        toml::Value::Table(table) => {
            for (key, value) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                default_leaf_values(value, &path, values);
            }
        }
        _ => {
            values.insert(prefix.to_owned(), value.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TokeiraConfig;

    #[test]
    fn catalog_paths_are_unique_and_complete_fixture_is_valid() {
        let catalog_paths = CONFIG_FIELD_CATALOG
            .iter()
            .map(|entry| entry.path.to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            catalog_paths.len(),
            CONFIG_FIELD_CATALOG.len(),
            "configuration catalog paths must be unique"
        );
        assert!(
            CONFIG_FIELD_CATALOG
                .iter()
                .all(|entry| !entry.guidance.trim().is_empty()),
            "every production field requires operator guidance"
        );

        let parsed: TokeiraConfig =
            toml::from_str(COMPLETE_ANNOTATED_CONFIG_FIXTURE).expect("complete fixture must parse");
        parsed.validate().expect("complete fixture must validate");
        let encoded = parsed.to_toml().expect("complete fixture must serialize");
        let round_trip: TokeiraConfig =
            toml::from_str(&encoded).expect("complete fixture must round trip");
        assert_eq!(round_trip, parsed);

        let value: toml::Value =
            toml::from_str(COMPLETE_ANNOTATED_CONFIG_FIXTURE).expect("fixture value");
        let mut fixture_paths = BTreeSet::new();
        collect_leaf_paths(&value, "", &mut fixture_paths);
        assert_eq!(fixture_paths, catalog_paths);
    }

    #[test]
    fn catalog_defaults_match_the_empty_configuration() {
        let encoded = crate::TokeiraConfig::default()
            .to_toml()
            .expect("default config must serialize");
        let value: toml::Value = toml::from_str(&encoded).expect("default config value");
        let mut defaults = BTreeMap::new();
        default_leaf_values(&value, "", &mut defaults);
        let catalog = CONFIG_FIELD_CATALOG
            .iter()
            .map(|entry| (entry.path, entry.default))
            .collect::<BTreeMap<_, _>>();

        for (path, value) in defaults {
            assert_eq!(
                catalog.get(path.as_str()).copied(),
                Some(value.as_str()),
                "catalog default drifted for {path}"
            );
        }
    }
}
