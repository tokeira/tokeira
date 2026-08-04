pub mod documentation;
pub mod loader;
pub mod source;
pub use documentation::{CONFIG_FIELD_CATALOG, ConfigFieldClass, ConfigFieldDocumentation};
pub use loader::{ConfigLoaderError, load_config, load_config_from_source, write_config_toml};
pub use source::{CONFIG_ENV, ConfigSource};

use std::{collections::HashSet, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(clap::Parser, Debug)]
#[command(name = "tokeirad", disable_version_flag = true)]
pub struct Cli {
    /// Configuration locator: a file path, `file:<path>`, or `env:<VAR>`.
    #[arg(long)]
    pub config: Option<String>,

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
    /// Startup-static task-queue delivery policy.
    #[serde(default)]
    pub task_queues: TaskQueuePolicyConfig,
    /// Startup-static Worker Compute Controller policy.
    #[serde(default)]
    pub worker_compute: WorkerComputePolicyConfig,
    #[serde(default)]
    pub nexus_endpoint_limits: NexusEndpointLimitsConfig,
    #[serde(default)]
    pub nexus_completion: NexusCompletionConfig,
    /// Temporal HTTP/JSON gateway admission and metadata-forwarding policy.
    #[serde(default)]
    pub http_api: HttpApiPolicyConfig,
    /// Configured identity sources and authorization policy. Absence preserves
    /// the stock permissive server.
    #[serde(default)]
    pub authorization: Option<AuthorizationConfig>,
}

/// Startup-static task-queue delivery policy.
///
/// Temporal v1.31.0 enables priority-aware delivery but leaves User Fairness
/// disabled by default (`matching.useNewMatcher` and `matching.enableFairness`
/// in `common/dynamicconfig/constants.go @ v1.31.0`). Tokeira exposes only the
/// latter choice because the five-band priority matcher remains baseline
/// behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskQueuePolicyConfig {
    /// Opt in to weighted User Fairness for non-sticky task queues.
    #[serde(default)]
    pub enable_fairness: bool,
}

/// Startup-static policy for remote Worker Compute Controller reconciliation.
///
/// The controller is experimental and can cause configured providers to create
/// billable capacity, so an absent table deliberately remains inert.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerComputePolicyConfig {
    /// Start the process-local controller service.
    #[serde(default)]
    pub enabled: bool,
}

/// Static policy for Temporal's public HTTP/JSON compatibility gateway.
///
/// The gateway remains enabled when this section is absent. Defaults mirror
/// `frontend.httpAllowedHosts` and the empty operator-supplied forwarded-header
/// list in `http_api_server.go @ v1.31.0`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpApiPolicyConfig {
    /// Full-host, case-sensitive wildcard patterns; only `*` is special.
    #[serde(default = "default_http_api_allowed_hosts")]
    pub allowed_hosts: Vec<String>,
    /// Extra exact header names or prefixes ending in one `*`.
    #[serde(default)]
    pub additional_forwarded_headers: Vec<String>,
}

impl Default for HttpApiPolicyConfig {
    fn default() -> Self {
        Self {
            allowed_hosts: default_http_api_allowed_hosts(),
            additional_forwarded_headers: Vec::new(),
        }
    }
}

fn default_http_api_allowed_hosts() -> Vec<String> {
    vec!["*".to_owned()]
}

/// Presence-enabled authentication and authorization policy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationConfig {
    /// Stamp server-computed principals on history when enforcement is active.
    #[serde(default)]
    pub principal_attribution: bool,
    /// Expose authorizer implementation errors rather than the generic denial.
    #[serde(default)]
    pub expose_authorizer_errors: bool,
    /// JWT identity-source profiles.
    #[serde(default)]
    pub jwt: JwtAuthorizationConfig,
    /// Optional AWS IAM presigned-STS identity source.
    #[serde(default)]
    pub aws_iam: Option<AwsIamAuthorizationConfig>,
}

impl AuthorizationConfig {
    /// Whether at least one identity source activates enforcement.
    pub fn has_identity_source(&self) -> bool {
        !self.jwt.issuers.is_empty() || self.aws_iam.is_some()
    }
}

/// JWT authorization profiles.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtAuthorizationConfig {
    /// Issuer profiles routed by exact `iss` claim.
    #[serde(default)]
    pub issuers: Vec<JwtIssuerConfig>,
}

/// One exact-issuer JWT verification profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtIssuerConfig {
    /// Stable operator label for logs and metrics.
    #[serde(default)]
    pub name: String,
    /// Exact case-sensitive token `iss` value.
    #[serde(default)]
    pub issuer: String,
    /// JWKS document URI.
    #[serde(default)]
    pub jwks_uri: String,
    /// Optional exact audience. Blank disables audience validation.
    #[serde(default)]
    pub audience: String,
    /// Optional human duration (`ms`, `s`, `m`, or `h`) for JWKS refresh.
    #[serde(default)]
    pub refresh_interval: Option<String>,
    /// JWT array claim containing Temporal `namespace:role` entries.
    #[serde(default = "default_permissions_claim")]
    pub permissions_claim: String,
    /// Subject-to-role augmentation rules.
    #[serde(default)]
    pub grants: Vec<JwtGrantConfig>,
    /// Subject-to-exact-Worker-scope attenuation rules.
    #[serde(default)]
    pub worker_scopes: Vec<JwtWorkerScopeConfig>,
}

impl Default for JwtIssuerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            issuer: String::new(),
            jwks_uri: String::new(),
            audience: String::new(),
            refresh_interval: None,
            permissions_claim: default_permissions_claim(),
            grants: Vec::new(),
            worker_scopes: Vec::new(),
        }
    }
}

impl JwtIssuerConfig {
    /// Parsed refresh cadence after [`TokeiraConfig::validate`] has accepted it.
    pub fn refresh_interval_duration(&self) -> Option<std::time::Duration> {
        self.refresh_interval
            .as_deref()
            .and_then(parse_positive_duration)
    }
}

/// Subject-pattern role grants for one JWT issuer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtGrantConfig {
    /// Full-string, case-sensitive subject pattern.
    #[serde(default)]
    pub match_sub: String,
    /// Temporal `namespace:role` grants.
    #[serde(default)]
    pub grant: Vec<String>,
}

/// Subject-pattern to exact Worker-scope mapping for one JWT issuer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtWorkerScopeConfig {
    /// Full-string, case-sensitive subject pattern.
    #[serde(default)]
    pub match_sub: String,
    /// Exact namespace name.
    #[serde(default)]
    pub namespace: String,
    /// Non-empty normal task-queue allowlist.
    #[serde(default)]
    pub task_queues: Vec<String>,
    /// Exact Worker Deployment name.
    #[serde(default)]
    pub deployment_name: String,
    /// Exact Worker Build ID.
    #[serde(default)]
    pub build_id: String,
}

/// AWS IAM presigned-STS identity-source configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsIamAuthorizationConfig {
    /// ARN-to-role rules.
    #[serde(default)]
    pub grants: Vec<AwsIamGrantConfig>,
    /// ARN-to-exact-Worker-scope attenuation rules.
    #[serde(default)]
    pub worker_scopes: Vec<AwsIamWorkerScopeConfig>,
}

/// ARN-pattern role grants.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsIamGrantConfig {
    /// Full-string, case-sensitive STS caller-ARN pattern.
    #[serde(default)]
    pub match_arn: String,
    /// Temporal `namespace:role` grants.
    #[serde(default)]
    pub grant: Vec<String>,
}

/// ARN-pattern to exact Worker-scope mapping.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsIamWorkerScopeConfig {
    /// Full-string, case-sensitive STS caller-ARN pattern.
    #[serde(default)]
    pub match_arn: String,
    /// Exact namespace name.
    #[serde(default)]
    pub namespace: String,
    /// Non-empty normal task-queue allowlist.
    #[serde(default)]
    pub task_queues: Vec<String>,
    /// Exact Worker Deployment name.
    #[serde(default)]
    pub deployment_name: String,
    /// Exact Worker Build ID.
    #[serde(default)]
    pub build_id: String,
}

fn default_permissions_claim() -> String {
    "permissions".to_owned()
}

fn validate_authorization(authorization: &AuthorizationConfig, errors: &mut Vec<ValidationError>) {
    let mut names = HashSet::new();
    let mut issuers = HashSet::new();
    for (index, issuer) in authorization.jwt.issuers.iter().enumerate() {
        let base = format!("policy.authorization.jwt.issuers[{index}]");
        for (field, value) in [
            ("name", issuer.name.as_str()),
            ("issuer", issuer.issuer.as_str()),
            ("jwks_uri", issuer.jwks_uri.as_str()),
        ] {
            if value.trim().is_empty() {
                errors.push(ValidationError::Field {
                    field: format!("{base}.{field}"),
                    message: "must not be blank".to_owned(),
                });
            }
        }
        if !issuer.name.trim().is_empty() && !names.insert(issuer.name.clone()) {
            errors.push(ValidationError::Field {
                field: format!("{base}.name"),
                message: format!("duplicate issuer profile name {:?}", issuer.name),
            });
        }
        if !issuer.issuer.trim().is_empty() && !issuers.insert(issuer.issuer.clone()) {
            errors.push(ValidationError::Field {
                field: format!("{base}.issuer"),
                message: format!("duplicate issuer URL {:?}", issuer.issuer),
            });
        }
        if let Some(interval) = issuer.refresh_interval.as_deref()
            && parse_positive_duration(interval).is_none()
        {
            errors.push(ValidationError::Field {
                field: format!("{base}.refresh_interval"),
                message: "must be a positive duration using ms, s, m, or h".to_owned(),
            });
        }
        for (rule_index, rule) in issuer.grants.iter().enumerate() {
            validate_grants_rule(
                &format!("{base}.grants[{rule_index}].match_sub"),
                &format!("{base}.grants[{rule_index}].grant"),
                &rule.match_sub,
                &rule.grant,
                errors,
            );
        }
        for (rule_index, rule) in issuer.worker_scopes.iter().enumerate() {
            validate_worker_scope_rule(
                &format!("{base}.worker_scopes[{rule_index}]"),
                "match_sub",
                &rule.match_sub,
                &rule.namespace,
                &rule.task_queues,
                &rule.deployment_name,
                &rule.build_id,
                errors,
            );
        }
    }

    if let Some(aws_iam) = authorization.aws_iam.as_ref() {
        if aws_iam.grants.is_empty() && aws_iam.worker_scopes.is_empty() {
            errors.push(ValidationError::Field {
                field: "policy.authorization.aws_iam.grants".to_owned(),
                message: "must contain at least one ARN grant or Worker-Scope rule".to_owned(),
            });
        }
        for (index, rule) in aws_iam.grants.iter().enumerate() {
            validate_grants_rule(
                &format!("policy.authorization.aws_iam.grants[{index}].match_arn"),
                &format!("policy.authorization.aws_iam.grants[{index}].grant"),
                &rule.match_arn,
                &rule.grant,
                errors,
            );
        }
        for (index, rule) in aws_iam.worker_scopes.iter().enumerate() {
            validate_worker_scope_rule(
                &format!("policy.authorization.aws_iam.worker_scopes[{index}]"),
                "match_arn",
                &rule.match_arn,
                &rule.namespace,
                &rule.task_queues,
                &rule.deployment_name,
                &rule.build_id,
                errors,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_worker_scope_rule(
    base: &str,
    pattern_name: &str,
    pattern: &str,
    namespace: &str,
    task_queues: &[String],
    deployment_name: &str,
    build_id: &str,
    errors: &mut Vec<ValidationError>,
) {
    if let Err(error) = tokeira_auth::GlobPattern::new(pattern) {
        errors.push(ValidationError::Field {
            field: format!("{base}.{pattern_name}"),
            message: error.to_string(),
        });
    }
    if let Err(error) = tokeira_auth::WorkerScope::try_new(
        namespace.to_owned(),
        task_queues.to_vec(),
        deployment_name.to_owned(),
        build_id.to_owned(),
    ) {
        let field = match &error {
            tokeira_auth::WorkerScopeError::Blank { field }
            | tokeira_auth::WorkerScopeError::Wildcard { field } => {
                format!("{base}.{field}")
            }
            tokeira_auth::WorkerScopeError::EmptyTaskQueues
            | tokeira_auth::WorkerScopeError::DuplicateTaskQueue { .. } => {
                format!("{base}.task_queues")
            }
            tokeira_auth::WorkerScopeError::TaskQueue { index, .. } => {
                format!("{base}.task_queues[{index}]")
            }
        };
        errors.push(ValidationError::Field {
            field,
            message: error.to_string(),
        });
    }
}

fn validate_grants_rule(
    pattern_field: &str,
    grant_field: &str,
    pattern: &str,
    grants: &[String],
    errors: &mut Vec<ValidationError>,
) {
    if let Err(error) = tokeira_auth::GlobPattern::new(pattern) {
        errors.push(ValidationError::Field {
            field: pattern_field.to_owned(),
            message: error.to_string(),
        });
    }
    for (index, grant) in grants.iter().enumerate() {
        if let Err(error) = tokeira_auth::parse_grant(grant) {
            errors.push(ValidationError::Field {
                field: format!("{grant_field}[{index}]"),
                message: error.to_string(),
            });
        }
    }
}

fn parse_positive_duration(value: &str) -> Option<std::time::Duration> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60.0)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600.0)
    } else {
        return None;
    };
    let seconds = number.parse::<f64>().ok()? * multiplier;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Some(std::time::Duration::from_secs_f64(seconds))
}

/// Async Nexus operation completion-callback delivery (`nexus-async-completion` spec).
///
/// `http_addr` is the bind address of the inbound `POST /nexus/callback` HTTP listener;
/// it defaults to loopback (`127.0.0.1:7253`) because the only intended client is this
/// node's own firing runtime (see `system_callback_url`). Operators terminating
/// callbacks across hosts must widen it deliberately.
/// `system_callback_url` is the base URL the runtime POSTs to when firing a
/// `temporal://system` callback — the loopback v1.31.0 performs by routing the system
/// callback to the cluster's own frontend (`routeSystemCallbackRequest @ v1.31.0`); the
/// `/nexus/callback` path is appended in code.
///
/// The retry policy mirrors v1.31.0's callbacks component
/// (`components/callbacks/config.go @ v1.31.0`): exponential backoff with a 1s initial
/// interval, a 1h maximum interval, and (by default) **no expiration** — v1.31.0 uses
/// `backoff.NoInterval` for the expiration, i.e. no attempt cap; the operation's
/// schedule-to-close timeout and eventual resolution bound it in practice.
/// `retry_max_attempts == 0` encodes that unbounded default; a positive value is a
/// tokeira-local safety cap with no v1.31.0 analog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NexusCompletionConfig {
    #[serde(default = "default_nexus_completion_http_addr")]
    pub http_addr: String,
    #[serde(default = "default_nexus_completion_system_callback_url")]
    pub system_callback_url: String,
    #[serde(default = "default_nexus_completion_retry_initial_interval_ms")]
    pub retry_initial_interval_ms: u64,
    #[serde(default = "default_nexus_completion_retry_max_interval_ms")]
    pub retry_max_interval_ms: u64,
    #[serde(default = "default_nexus_completion_retry_backoff_coefficient")]
    pub retry_backoff_coefficient: f64,
    /// `0` = unbounded (v1.31.0 `NoInterval` expiration); a positive value caps attempts.
    #[serde(default = "default_nexus_completion_retry_max_attempts")]
    pub retry_max_attempts: u32,
}

impl Default for NexusCompletionConfig {
    fn default() -> Self {
        Self {
            http_addr: default_nexus_completion_http_addr(),
            system_callback_url: default_nexus_completion_system_callback_url(),
            retry_initial_interval_ms: default_nexus_completion_retry_initial_interval_ms(),
            retry_max_interval_ms: default_nexus_completion_retry_max_interval_ms(),
            retry_backoff_coefficient: default_nexus_completion_retry_backoff_coefficient(),
            retry_max_attempts: default_nexus_completion_retry_max_attempts(),
        }
    }
}

fn default_nexus_completion_http_addr() -> String {
    // Bind loopback by default: the only intended client is this node's own firing
    // runtime, which POSTs to `system_callback_url` (`http://127.0.0.1:7253`). The
    // listener reads the request body before validating the callback token, so
    // narrowing the default keeps it off untrusted networks unless an operator widens
    // it deliberately. Not a Temporal key — v1.31.0 routes system callbacks to its own
    // frontend and has no separate `/nexus/callback` listener, so this default carries
    // no conformance obligation.
    "127.0.0.1:7253".to_string()
}
fn default_nexus_completion_system_callback_url() -> String {
    "http://127.0.0.1:7253".to_string()
}
fn default_nexus_completion_retry_initial_interval_ms() -> u64 {
    1_000
}
fn default_nexus_completion_retry_max_interval_ms() -> u64 {
    3_600_000
}
fn default_nexus_completion_retry_backoff_coefficient() -> f64 {
    2.0
}
fn default_nexus_completion_retry_max_attempts() -> u32 {
    0
}

/// The six Nexus endpoint admin limits, modelled as config (raise, never hardcode)
/// with v1.31.0-faithful defaults. Sourced from `common/dynamicconfig/constants.go
/// @ v1.31.0`: `NexusEndpointNameMaxLength` (200), `NexusEndpointExternalURLMaxLength`
/// (`4*1024`), `NexusEndpointDescriptionMaxSize` (20000), `MaxIDLengthLimit` (1000,
/// the task-queue bound), `NexusEndpointListDefaultPageSize` (100),
/// `NexusEndpointListMaxPageSize` (1000). All global — the description-size knob is
/// namespace-scoped upstream but the endpoint client ignores namespace because
/// endpoints are global resources (`nexus_endpoint_client.go:60-62 @ v1.31.0`), so a
/// global default is faithful.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NexusEndpointLimitsConfig {
    #[serde(default = "default_endpoint_name_max_length")]
    pub name_max_length: usize,
    #[serde(default = "default_endpoint_external_url_max_length")]
    pub external_url_max_length: usize,
    #[serde(default = "default_endpoint_description_max_size")]
    pub description_max_size: usize,
    #[serde(default = "default_endpoint_task_queue_max_length")]
    pub task_queue_max_length: usize,
    #[serde(default = "default_endpoint_list_default_page_size")]
    pub list_default_page_size: usize,
    #[serde(default = "default_endpoint_list_max_page_size")]
    pub list_max_page_size: usize,
}

impl Default for NexusEndpointLimitsConfig {
    fn default() -> Self {
        Self {
            name_max_length: default_endpoint_name_max_length(),
            external_url_max_length: default_endpoint_external_url_max_length(),
            description_max_size: default_endpoint_description_max_size(),
            task_queue_max_length: default_endpoint_task_queue_max_length(),
            list_default_page_size: default_endpoint_list_default_page_size(),
            list_max_page_size: default_endpoint_list_max_page_size(),
        }
    }
}

fn default_endpoint_name_max_length() -> usize {
    200
}
fn default_endpoint_external_url_max_length() -> usize {
    4 * 1024
}
fn default_endpoint_description_max_size() -> usize {
    20_000
}
fn default_endpoint_task_queue_max_length() -> usize {
    1000
}
fn default_endpoint_list_default_page_size() -> usize {
    100
}
fn default_endpoint_list_max_page_size() -> usize {
    1000
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
    #[error("config source `{locator}` cannot be read: {reason}")]
    Source { locator: String, reason: String },
    #[error(
        "unknown config source scheme `{scheme}:` in `{locator}` — supported: a file path, `file:<path>`, `env:<VAR>`"
    )]
    UnknownScheme { scheme: String, locator: String },
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
            task_queues: TaskQueuePolicyConfig::default(),
            worker_compute: WorkerComputePolicyConfig::default(),
            nexus_endpoint_limits: NexusEndpointLimitsConfig::default(),
            nexus_completion: NexusCompletionConfig::default(),
            http_api: HttpApiPolicyConfig::default(),
            authorization: None,
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
        Self::from_source(&ConfigSource::File(path.to_path_buf()))
    }

    /// Load and validate a document from any source. One pipeline for every
    /// locator — read, parse, storage defaults, validate — so a document
    /// arriving through `env:` behaves exactly like the same bytes in a file.
    pub fn from_source(source: &ConfigSource) -> Result<Self, ConfigError> {
        let content = source.read()?;
        let mut config: TokeiraConfig = toml::from_str(&content)?;
        config.apply_storage_defaults();
        config.validate()?;
        Ok(config)
    }

    /// Resolve the effective configuration: the `--config` locator, then the
    /// `TOKEIRA_CONFIG` locator, then built-in defaults. The returned label
    /// names the winning locator for startup logs and `--dump-config`.
    pub fn resolve(config: Option<&str>) -> Result<(Self, String), ConfigError> {
        let (mut config, source) = match ConfigSource::from_cli_env(config)? {
            Some((source, label)) => (Self::from_source(&source)?, label),
            None => {
                let mut config = Self::default();
                config.apply_storage_defaults();
                config.validate()?;
                (config, "defaults".to_string())
            }
        };
        // Per-pod placement overrides from the environment. Under Kubernetes/ECS the
        // config document is a shared ConfigMap, so a node's own reachable membership
        // address cannot be baked into the file — every pod would advertise the same
        // host and the controller would route them all to one endpoint. The platform
        // injects each pod's address via the downward API (`status.podIP`); these
        // overrides let that per-pod value win over the shared file so a node advertises
        // its own membership endpoint. Applied after load so the environment wins; an
        // unset var leaves the file value untouched.
        config.apply_placement_env_overrides()?;
        Ok((config, source))
    }

    /// Override the membership advertise address from the environment.
    ///
    /// `TOKEIRA_NODE_HOST` (non-empty) sets `infrastructure.placement.node_host` and
    /// `TOKEIRA_NODE_PORT` (a valid `u16`) sets `infrastructure.placement.node_port`.
    /// These exist so a pod that shares a ConfigMap can still advertise its own
    /// reachable endpoint, injected per-pod from the downward API — an unset or empty
    /// var leaves the file value in place. A malformed `TOKEIRA_NODE_PORT` is a hard
    /// error rather than a silent fallback, so a misconfigured deployment fails loudly
    /// instead of advertising the wrong port and silently mis-routing membership.
    fn apply_placement_env_overrides(&mut self) -> Result<(), ConfigError> {
        if let Ok(host) = std::env::var("TOKEIRA_NODE_HOST") {
            let host = host.trim();
            if !host.is_empty() {
                self.infrastructure.placement.node_host = host.to_string();
            }
        }
        if let Ok(port) = std::env::var("TOKEIRA_NODE_PORT") {
            let port = port.trim();
            if !port.is_empty() {
                let parsed = port.parse::<u16>().map_err(|err| {
                    ConfigError::Validation(vec![ValidationError::Field {
                        field: "infrastructure.placement.node_port".to_string(),
                        message: format!("TOKEIRA_NODE_PORT must be a valid u16: {err}"),
                    }])
                })?;
                self.infrastructure.placement.node_port = Some(parsed);
            }
        }
        Ok(())
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
        let nexus_completion = &self.policy.nexus_completion;
        if nexus_completion
            .http_addr
            .parse::<std::net::SocketAddr>()
            .is_err()
        {
            errors.push(ValidationError::Field {
                field: "policy.nexus_completion.http_addr".to_string(),
                message: format!(
                    "not a valid socket address: {:?}",
                    nexus_completion.http_addr
                ),
            });
        }
        if nexus_completion.retry_initial_interval_ms == 0 {
            errors.push(ValidationError::Field {
                field: "policy.nexus_completion.retry_initial_interval_ms".to_string(),
                message: "must be positive".to_string(),
            });
        }
        if nexus_completion.retry_max_interval_ms < nexus_completion.retry_initial_interval_ms {
            errors.push(ValidationError::Field {
                field: "policy.nexus_completion.retry_max_interval_ms".to_string(),
                message: "must be greater than or equal to retry_initial_interval_ms".to_string(),
            });
        }
        if nexus_completion.retry_backoff_coefficient < 1.0 {
            errors.push(ValidationError::Field {
                field: "policy.nexus_completion.retry_backoff_coefficient".to_string(),
                message: "must be greater than or equal to 1.0".to_string(),
            });
        }
        for (index, rule) in self
            .policy
            .http_api
            .additional_forwarded_headers
            .iter()
            .enumerate()
        {
            let trimmed = rule.trim();
            let stars = trimmed.bytes().filter(|byte| *byte == b'*').count();
            if trimmed.is_empty() || stars > 1 || (stars == 1 && !trimmed.ends_with('*')) {
                errors.push(ValidationError::Field {
                    field: format!("policy.http_api.additional_forwarded_headers[{index}]"),
                    message: "must be a non-empty exact header or a prefix ending in one `*`"
                        .to_owned(),
                });
            }
        }
        if let Some(authorization) = self.policy.authorization.as_ref() {
            validate_authorization(authorization, &mut errors);
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
// Edition-2024 `std::env` mutation in tests, serialized by ENV_LOCK — each site
// carries its SAFETY comment.
#[allow(unsafe_code)]
mod tests {
    use super::*;
    use clap::Parser;
    use proptest::prelude::*;
    use std::{path::PathBuf, sync::Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // Feature: scoped-worker-authorization, Property 11: Configuration validation and round-trip
    proptest! {
        #[test]
        fn property_worker_scope_configuration_validation_and_round_trip(
            namespace in "[a-z]{1,12}",
            queue in "[a-z]{1,12}",
            deployment in "[a-z]{1,12}",
            build_id in "[a-z0-9]{1,12}",
        ) {
            let authorization = AuthorizationConfig {
                aws_iam: Some(AwsIamAuthorizationConfig {
                    grants: Vec::new(),
                    worker_scopes: vec![AwsIamWorkerScopeConfig {
                        match_arn: "arn:aws:sts::*:assumed-role/worker-*".to_owned(),
                        namespace: namespace.clone(),
                        task_queues: vec![queue.clone()],
                        deployment_name: deployment.clone(),
                        build_id: build_id.clone(),
                    }],
                }),
                ..Default::default()
            };
            let encoded = toml::to_string(&authorization).expect("serialize authorization");
            let decoded: AuthorizationConfig =
                toml::from_str(&encoded).expect("deserialize authorization");
            prop_assert_eq!(&decoded, &authorization);
            let mut errors = Vec::new();
            validate_authorization(&decoded, &mut errors);
            prop_assert!(errors.is_empty());

            let mut invalid = decoded;
            invalid
                .aws_iam
                .as_mut()
                .expect("AWS IAM")
                .worker_scopes[0]
                .task_queues[0] = "*".to_owned();
            let mut errors = Vec::new();
            validate_authorization(&invalid, &mut errors);
            let has_indexed_error = errors.iter().any(|error| matches!(
                error,
                ValidationError::Field { field, .. }
                    if field == "policy.authorization.aws_iam.worker_scopes[0].task_queues[0]"
            ));
            prop_assert!(has_indexed_error);
        }
    }

    #[test]
    fn authorization_section_is_presence_enabled_and_flags_are_inert_without_sources() {
        let absent: TokeiraConfig = toml::from_str("").expect("empty config");
        assert!(absent.policy.authorization.is_none());
        absent.validate().expect("stock defaults");

        let flags_only: TokeiraConfig = toml::from_str(
            r#"
                [policy.authorization]
                principal_attribution = true
                expose_authorizer_errors = true
            "#,
        )
        .expect("flags-only config");
        let authorization = flags_only.policy.authorization.as_ref().expect("section");
        assert!(!authorization.has_identity_source());
        flags_only.validate().expect("inert flags are valid");
    }

    #[test]
    fn authorization_identity_sources_parse_and_validate() {
        let config: TokeiraConfig = toml::from_str(
            r#"
                [policy.authorization]
                principal_attribution = true

                [[policy.authorization.jwt.issuers]]
                name = "eks-prod"
                issuer = "https://issuer.example/id/ABC"
                jwks_uri = "https://issuer.example/id/ABC/keys"
                audience = "tokeira"
                refresh_interval = "1m"

                [[policy.authorization.jwt.issuers.grants]]
                match_sub = "system:serviceaccount:prod:*"
                grant = ["prod:worker", "prod:write"]

                [[policy.authorization.jwt.issuers.worker_scopes]]
                match_sub = "system:serviceaccount:prod:*"
                namespace = "prod"
                task_queues = ["payments-worker"]
                deployment_name = "payments"
                build_id = "build-a"

                [policy.authorization.aws_iam]
                [[policy.authorization.aws_iam.grants]]
                match_arn = "arn:aws:sts::123456789012:assumed-role/tokeira-worker-*"
                grant = ["prod:worker"]

                [[policy.authorization.aws_iam.worker_scopes]]
                match_arn = "arn:aws:sts::123456789012:assumed-role/tokeira-worker-*"
                namespace = "prod"
                task_queues = ["payments-worker"]
                deployment_name = "payments"
                build_id = "build-a"
            "#,
        )
        .expect("authorization config");
        config.validate().expect("valid authorization config");
        let authorization = config.policy.authorization.expect("section");
        assert!(authorization.has_identity_source());
        assert_eq!(
            authorization.jwt.issuers[0].permissions_claim,
            "permissions"
        );
    }

    #[test]
    fn aws_iam_scope_only_source_is_valid() {
        let config: TokeiraConfig = toml::from_str(
            r#"
                [policy.authorization.aws_iam]
                [[policy.authorization.aws_iam.worker_scopes]]
                match_arn = "arn:aws:sts::*:assumed-role/worker-*"
                namespace = "prod"
                task_queues = ["payments-worker"]
                deployment_name = "payments"
                build_id = "build-a"
            "#,
        )
        .expect("authorization config");
        config.validate().expect("scope-only source is valid");
    }

    #[test]
    fn authorization_validation_names_every_invalid_field() {
        let config: TokeiraConfig = toml::from_str(
            r#"
                [policy.authorization]

                [[policy.authorization.jwt.issuers]]
                name = "duplicate"
                issuer = "https://issuer.example"
                jwks_uri = ""
                refresh_interval = "never"
                [[policy.authorization.jwt.issuers.grants]]
                match_sub = ""
                grant = ["prod:unknown"]

                [[policy.authorization.jwt.issuers]]
                name = "duplicate"
                issuer = "https://issuer.example"
                jwks_uri = "https://issuer.example/keys"

                [[policy.authorization.jwt.issuers]]

                [policy.authorization.aws_iam]
            "#,
        )
        .expect("structurally valid config");
        let ConfigError::Validation(errors) = config.validate().expect_err("must fail") else {
            panic!("expected validation error");
        };
        let fields = errors
            .iter()
            .map(|error| match error {
                ValidationError::Field { field, .. } => field.as_str(),
            })
            .collect::<Vec<_>>();
        for expected in [
            "policy.authorization.jwt.issuers[0].jwks_uri",
            "policy.authorization.jwt.issuers[0].refresh_interval",
            "policy.authorization.jwt.issuers[0].grants[0].match_sub",
            "policy.authorization.jwt.issuers[0].grants[0].grant[0]",
            "policy.authorization.jwt.issuers[1].name",
            "policy.authorization.jwt.issuers[1].issuer",
            "policy.authorization.jwt.issuers[2].name",
            "policy.authorization.jwt.issuers[2].issuer",
            "policy.authorization.jwt.issuers[2].jwks_uri",
            "policy.authorization.aws_iam.grants",
        ] {
            assert!(
                fields.contains(&expected),
                "missing validation for {expected}"
            );
        }
    }

    #[test]
    fn authorization_rejects_tls_listener_configuration() {
        // The edge authenticates bearer material only and receives TLS from an
        // upstream terminator. Rejecting these plausible listener knobs keeps
        // that architecture explicit instead of silently accepting dead config
        // (`authorization-foundation` Requirement 10.2).
        for field in ["tls", "tls_cert_file", "tls_key_file", "client_ca_file"] {
            let error = toml::from_str::<TokeiraConfig>(&format!(
                "[policy.authorization]\n{field} = true\n"
            ))
            .expect_err("authorization TLS listener fields must be unknown")
            .to_string();
            assert!(error.contains(field), "{error}");
        }
    }

    // Defaults mirror v1.31.0's callbacks retry policy (1s initial, 1h max, 2x, no cap)
    // and a co-located loopback HTTP listener for temporal://system firing.
    #[test]
    fn nexus_completion_defaults_match_v1_31_0_and_validate() {
        let cfg = NexusCompletionConfig::default();
        // Loopback by default — the listener's only intended client is this node's own
        // firing runtime (posts to `system_callback_url`); widening it is an operator
        // choice. Not a Temporal key, so no v1.31.0 conformance obligation on the bind.
        assert_eq!(cfg.http_addr, "127.0.0.1:7253");
        assert_eq!(cfg.system_callback_url, "http://127.0.0.1:7253");
        assert_eq!(cfg.retry_initial_interval_ms, 1_000);
        assert_eq!(cfg.retry_max_interval_ms, 3_600_000);
        assert!((cfg.retry_backoff_coefficient - 2.0).abs() < f64::EPSILON);
        assert_eq!(
            cfg.retry_max_attempts, 0,
            "0 == unbounded (v1.31.0 NoInterval)"
        );

        // A default config is valid.
        assert!(TokeiraConfig::default().validate().is_ok());
    }

    #[test]
    fn nexus_completion_rejects_invalid_values() {
        let mut config = TokeiraConfig::default();
        config.policy.nexus_completion.http_addr = "not-an-addr".to_string();
        config.policy.nexus_completion.retry_initial_interval_ms = 5_000;
        config.policy.nexus_completion.retry_max_interval_ms = 10;
        config.policy.nexus_completion.retry_backoff_coefficient = 0.5;
        let errors = config
            .validate()
            .expect_err("invalid nexus_completion must fail");
        let blob = match errors {
            ConfigError::Validation(errs) => errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
            other => panic!("expected validation errors, got {other:?}"),
        };
        assert!(blob.contains("nexus_completion.http_addr"), "{blob}");
        assert!(blob.contains("retry_max_interval_ms"), "{blob}");
        assert!(blob.contains("retry_backoff_coefficient"), "{blob}");

        // initial interval of 0 is independently rejected.
        let mut zero = TokeiraConfig::default();
        zero.policy.nexus_completion.retry_initial_interval_ms = 0;
        let zero_err = zero
            .validate()
            .expect_err("zero initial interval must fail");
        assert!(
            matches!(&zero_err, ConfigError::Validation(errs)
                if errs.iter().any(|e| e.to_string().contains("retry_initial_interval_ms"))),
            "{zero_err:?}"
        );
    }

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
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::set_var("TOKEIRA_CONFIG", &env_path);
        }

        let (config, source) = TokeiraConfig::resolve(Some(cli_path.to_str().unwrap())).unwrap();
        assert_eq!(source, format!("--config {}", cli_path.display()));
        assert_eq!(config.infrastructure.cluster_name, "cli");

        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
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
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::set_var("TOKEIRA_CONFIG", &env_path);
        }

        let (config, source) = TokeiraConfig::resolve(None).unwrap();
        assert_eq!(source, format!("TOKEIRA_CONFIG {}", env_path.display()));
        assert_eq!(config.infrastructure.cluster_name, "env");

        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
        }
        let _ = std::fs::remove_file(env_path);
    }

    #[test]
    fn resolve_reads_an_env_inline_document() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::set_var("TOKEIRA_CONFIG", "env:TOKEIRA_TEST_INLINE_DOC");
            std::env::set_var(
                "TOKEIRA_TEST_INLINE_DOC",
                "[infrastructure]\ncluster_name = \"inline\"",
            );
        }

        let (config, source) = TokeiraConfig::resolve(None).unwrap();
        assert_eq!(source, "TOKEIRA_CONFIG env:TOKEIRA_TEST_INLINE_DOC");
        assert_eq!(config.infrastructure.cluster_name, "inline");

        // A named-but-unset variable is fatal and repeats the locator — never
        // a silent fall-through to defaults.
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_TEST_INLINE_DOC");
        }
        let err = TokeiraConfig::resolve(None).unwrap_err();
        assert!(
            err.to_string().contains("env:TOKEIRA_TEST_INLINE_DOC"),
            "{err}"
        );

        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
        }
    }

    #[test]
    fn resolve_refuses_an_unknown_scheme() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
        }
        let err = TokeiraConfig::resolve(Some("s3:bucket/tokeirad.toml")).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownScheme { .. }), "{err:?}");
    }

    #[test]
    fn resolve_applies_placement_env_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_CONFIG");
            std::env::set_var("TOKEIRA_NODE_HOST", "10.0.4.7");
            std::env::set_var("TOKEIRA_NODE_PORT", "7233");
        }

        // The downward-API-injected host/port win over the file (here, the defaults),
        // so a pod sharing a ConfigMap advertises its own membership endpoint.
        let (config, _source) = TokeiraConfig::resolve(None).unwrap();
        assert_eq!(config.infrastructure.placement.node_host, "10.0.4.7");
        assert_eq!(config.infrastructure.placement.node_port, Some(7233));

        // A malformed port fails loudly rather than advertising the wrong port.
        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::set_var("TOKEIRA_NODE_PORT", "not-a-port");
        }
        let err = TokeiraConfig::resolve(None).unwrap_err();
        assert!(matches!(err, ConfigError::Validation(_)), "got {err:?}");

        // SAFETY: edition-2024 env mutation, serialized by ENV_LOCK held for the whole test.
        unsafe {
            std::env::remove_var("TOKEIRA_NODE_HOST");
            std::env::remove_var("TOKEIRA_NODE_PORT");
        }
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

    #[test]
    fn http_api_policy_defaults_and_header_rules_validate() {
        let decoded: TokeiraConfig = toml::from_str("").expect("default config");
        assert_eq!(decoded.policy.http_api.allowed_hosts, ["*"]);
        assert!(
            decoded
                .policy
                .http_api
                .additional_forwarded_headers
                .is_empty()
        );

        let mut config = TokeiraConfig::default();
        config.policy.http_api.additional_forwarded_headers = vec![
            "this-header-forwarded".to_owned(),
            "this-header-prefix-forwarded-*".to_owned(),
        ];
        assert!(config.validate().is_ok());

        for invalid in ["", "prefix-*suffix", "two-*-stars-*"] {
            config.policy.http_api.additional_forwarded_headers = vec![invalid.to_owned()];
            assert!(
                config.validate().is_err(),
                "invalid additional header rule `{invalid}` was accepted"
            );
        }

        let encoded = toml::to_string(&TokeiraConfig::default()).expect("serialize config");
        let round_trip: TokeiraConfig = toml::from_str(&encoded).expect("round trip config");
        assert_eq!(round_trip.policy.http_api, HttpApiPolicyConfig::default());
        assert!(
            toml::from_str::<TokeiraConfig>(
                "[policy.http_api]\nallowed_hosts = [\"*\"]\nunknown = true\n"
            )
            .is_err(),
            "strict config must reject unknown HTTP API policy fields"
        );
    }

    #[test]
    fn worker_compute_policy_is_strict_and_default_disabled() {
        let defaults: TokeiraConfig = toml::from_str("").expect("default config");
        assert!(!defaults.policy.worker_compute.enabled);

        let enabled: TokeiraConfig = toml::from_str("[policy.worker_compute]\nenabled = true\n")
            .expect("worker compute policy");
        assert!(enabled.policy.worker_compute.enabled);
        assert!(
            toml::from_str::<TokeiraConfig>(
                "[policy.worker_compute]\nenabled = false\nunknown = true\n"
            )
            .is_err(),
            "strict config must reject unknown Worker Compute policy fields"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: configuration-policy, Property 2: configuration defaulting and round-trip preservation
        #[test]
        fn property_toml_round_trip(config in arb_valid_config()) {
            let encoded = config.to_toml().unwrap();
            let decoded: TokeiraConfig = toml::from_str(&encoded).unwrap();
            prop_assert_eq!(decoded, config);
        }

        // Feature: configuration-policy, Property 2: configuration defaulting and round-trip preservation
        #[test]
        fn property_old_shape_defaults_fairness_without_changing_other_values(
            config in arb_valid_config(),
        ) {
            let encoded = config.to_toml().unwrap();
            let mut old_shape: toml::Value = toml::from_str(&encoded).unwrap();
            old_shape
                .get_mut("policy")
                .and_then(toml::Value::as_table_mut)
                .unwrap()
                .remove("task_queues");
            let old_shape = toml::to_string(&old_shape).unwrap();
            let decoded: TokeiraConfig = toml::from_str(&old_shape).unwrap();

            let mut expected = config;
            expected.policy.task_queues = TaskQueuePolicyConfig::default();
            prop_assert!(!decoded.policy.task_queues.enable_fairness);
            prop_assert_eq!(decoded, expected);
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
            any::<bool>(),
            any::<bool>(),
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
                    enable_fairness,
                    worker_compute_enabled,
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
                        compatibility: CompatibilityConfig::default(),
                        task_queues: TaskQueuePolicyConfig { enable_fairness },
                        worker_compute: WorkerComputePolicyConfig {
                            enabled: worker_compute_enabled,
                        },
                        nexus_endpoint_limits: NexusEndpointLimitsConfig::default(),
                        nexus_completion: NexusCompletionConfig::default(),
                        http_api: HttpApiPolicyConfig::default(),
                        authorization: None,
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
