//! Configuration for the in-process embedded engine.
//!
//! Embedded storage selection is deliberately separate from daemon infrastructure
//! configuration. A connection endpoint must never imply permission to create AWS
//! resources, and an invalid durable mode must never fall back to in-memory execution.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::{ConfigError, TokeiraConfig};

const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 900_000;
const DEFAULT_MAX_CONNECTIONS: usize = 8;
const DEFAULT_CONCURRENT_CONNECTION_CREATIONS: usize = 2;
const DEFAULT_CONNECTION_RATE_PER_SECOND: f64 = 8.0;
const DEFAULT_CONNECTION_BURST: u64 = 2;
const MAX_EMBEDDED_CONNECTIONS: usize = 16;
const MAX_CONCURRENT_CONNECTION_CREATIONS: usize = 4;
const MAX_CONNECTION_RATE_PER_SECOND: f64 = 16.0;
const MAX_CONNECTION_BURST: u64 = 4;

/// Complete startup configuration for an in-process engine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedEngineConfig {
    /// Ordinary server/runtime policy reused by the embedded service stack.
    pub server: TokeiraConfig,
    /// Explicit embedded storage selection.
    #[serde(default)]
    pub storage: EmbeddedStorageConfig,
    /// End-to-end budget shared by every embedded startup phase.
    #[serde(default = "default_embedded_startup_timeout_ms")]
    pub startup_timeout_ms: u64,
}

impl Default for EmbeddedEngineConfig {
    fn default() -> Self {
        Self {
            server: TokeiraConfig::default(),
            storage: EmbeddedStorageConfig::InMemory,
            startup_timeout_ms: DEFAULT_STARTUP_TIMEOUT_MS,
        }
    }
}

impl EmbeddedEngineConfig {
    /// Validate the server configuration and the selected embedded storage mode.
    ///
    /// Validation reports the selected mode's defect and never rewrites the mode or
    /// substitutes in-memory storage.
    pub fn validate(&self) -> Result<(), EmbeddedConfigError> {
        self.server
            .validate()
            .map_err(EmbeddedConfigError::Server)?;

        let mut errors = Vec::new();
        if self.startup_timeout_ms == 0 {
            errors.push(EmbeddedValidationError::new(
                "startup_timeout_ms",
                "must be positive",
            ));
        }

        match &self.storage {
            EmbeddedStorageConfig::InMemory => {}
            EmbeddedStorageConfig::ManagedDsql(config) => {
                validate_nonempty_path(
                    &config.descriptor_path,
                    "storage.descriptor_path",
                    &mut errors,
                );
                validate_nonempty(&config.region, "storage.region", &mut errors);
                validate_limits(&config.limits, &mut errors);
            }
            EmbeddedStorageConfig::ExistingDsql(config) => {
                validate_nonempty(&config.region, "storage.region", &mut errors);
                validate_nonempty(&config.cluster_id, "storage.cluster_id", &mut errors);
                validate_nonempty(&config.cluster_arn, "storage.cluster_arn", &mut errors);
                validate_nonempty(&config.endpoint, "storage.endpoint", &mut errors);
                validate_limits(&config.limits, &mut errors);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(EmbeddedConfigError::Validation(errors))
        }
    }

    /// Return the effective migration policy after explicit storage intent validates.
    ///
    /// Managed DSQL alone defaults to automatic migration. Existing DSQL carries a
    /// required policy in its serialized shape, while in-memory storage has none.
    pub fn effective_migration_policy(&self) -> Option<DsqlMigrationPolicy> {
        match &self.storage {
            EmbeddedStorageConfig::InMemory => None,
            EmbeddedStorageConfig::ManagedDsql(config) => Some(
                config
                    .migration_policy
                    .unwrap_or(DsqlMigrationPolicy::Automatic),
            ),
            EmbeddedStorageConfig::ExistingDsql(config) => Some(config.migration_policy),
        }
    }
}

/// Closed set of storage modes supported by one embedded engine process.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum EmbeddedStorageConfig {
    /// Preserve the existing ephemeral process-local behavior.
    #[default]
    InMemory,
    /// Create or recover a dedicated single-Region Aurora DSQL cluster.
    ManagedDsql(ManagedEmbeddedDsqlConfig),
    /// Use an operator-supplied Aurora DSQL cluster without lifecycle mutation.
    ExistingDsql(ExistingEmbeddedDsqlConfig),
}

impl<'de> Deserialize<'de> for EmbeddedStorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TaggedStorage {
            mode: String,
            #[serde(flatten)]
            fields: BTreeMap<String, serde_json::Value>,
        }

        let tagged = TaggedStorage::deserialize(deserializer)?;
        if tagged.mode == "in_memory" {
            if let Some(field) = tagged.fields.keys().next() {
                return Err(serde::de::Error::unknown_field(field, &[]));
            }
            return Ok(Self::InMemory);
        }

        let fields = serde_json::Value::Object(tagged.fields.into_iter().collect());
        match tagged.mode.as_str() {
            "managed_dsql" => serde_json::from_value(fields)
                .map(Self::ManagedDsql)
                .map_err(serde::de::Error::custom),
            "existing_dsql" => serde_json::from_value(fields)
                .map(Self::ExistingDsql)
                .map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["in_memory", "managed_dsql", "existing_dsql"],
            )),
        }
    }
}

/// Managed embedded DSQL configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedEmbeddedDsqlConfig {
    /// Explicit authority to create a missing descriptor or recover its cluster.
    pub intent: ManagedClusterIntent,
    /// Host-owned path to the crash-safe cluster descriptor.
    pub descriptor_path: PathBuf,
    /// AWS Region in which the dedicated cluster is managed.
    pub region: String,
    /// Managed mode defaults to automatic migration only after intent validation.
    #[serde(default)]
    pub migration_policy: Option<DsqlMigrationPolicy>,
    /// Deliberately small process-local connection-creation envelope.
    #[serde(default)]
    pub limits: EmbeddedDsqlLimits,
    /// Optional creation metadata; never used for discovery or identity.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

/// Explicit managed-cluster startup authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedClusterIntent {
    /// Create a cluster when no descriptor exists, otherwise recover its canonical ID.
    CreateOrRecover,
}

/// Configuration for an operator-supplied embedded DSQL cluster.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExistingEmbeddedDsqlConfig {
    /// AWS Region used for control-plane validation and authentication.
    pub region: String,
    /// Canonical Aurora DSQL cluster identifier.
    pub cluster_id: String,
    /// Canonical Aurora DSQL cluster ARN paired with `cluster_id`.
    pub cluster_arn: String,
    /// Refreshable connection locator; never a resource identity.
    pub endpoint: String,
    /// Explicit policy required for an operator-supplied cluster.
    pub migration_policy: DsqlMigrationPolicy,
    /// Deliberately small process-local connection-creation envelope.
    #[serde(default)]
    pub limits: EmbeddedDsqlLimits,
}

/// Schema migration policy selected at a DSQL startup boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DsqlMigrationPolicy {
    /// Validate the known schema and migrate idempotently to the release target.
    Automatic,
    /// Validate compatibility without changing schema state.
    ValidateOnly,
}

/// Bounded connection resources available to one embedded DSQL process.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedDsqlLimits {
    /// Maximum physical database connections.
    pub max_connections: usize,
    /// Maximum connection establishments simultaneously in flight.
    pub concurrent_connection_creations: usize,
    /// Maximum sustained connection-establishment rate.
    pub connection_rate_per_second: f64,
    /// Maximum connection-establishment token burst.
    pub connection_burst: u64,
}

impl Default for EmbeddedDsqlLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            concurrent_connection_creations: DEFAULT_CONCURRENT_CONNECTION_CREATIONS,
            connection_rate_per_second: DEFAULT_CONNECTION_RATE_PER_SECOND,
            connection_burst: DEFAULT_CONNECTION_BURST,
        }
    }
}

/// Failure returned while validating an embedded engine configuration.
#[derive(Debug, Error)]
pub enum EmbeddedConfigError {
    /// The reused server configuration is invalid.
    #[error("embedded server configuration is invalid: {0}")]
    Server(#[source] ConfigError),
    /// One or more embedded-only fields are invalid.
    #[error("embedded configuration validation failed:\n{}", .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n"))]
    Validation(Vec<EmbeddedValidationError>),
}

/// One field-level embedded configuration validation failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{field}: {message}")]
pub struct EmbeddedValidationError {
    /// Stable configuration field path.
    pub field: String,
    /// Actionable constraint description without the rejected value.
    pub message: String,
}

impl EmbeddedValidationError {
    fn new(field: &str, message: &str) -> Self {
        Self {
            field: field.to_owned(),
            message: message.to_owned(),
        }
    }
}

fn default_embedded_startup_timeout_ms() -> u64 {
    DEFAULT_STARTUP_TIMEOUT_MS
}

fn validate_nonempty(value: &str, field: &str, errors: &mut Vec<EmbeddedValidationError>) {
    if value.trim().is_empty() {
        errors.push(EmbeddedValidationError::new(field, "must not be empty"));
    }
}

fn validate_nonempty_path(value: &Path, field: &str, errors: &mut Vec<EmbeddedValidationError>) {
    if value.as_os_str().is_empty() {
        errors.push(EmbeddedValidationError::new(field, "must not be empty"));
    }
}

fn validate_limits(limits: &EmbeddedDsqlLimits, errors: &mut Vec<EmbeddedValidationError>) {
    if !(1..=MAX_EMBEDDED_CONNECTIONS).contains(&limits.max_connections) {
        errors.push(EmbeddedValidationError::new(
            "storage.limits.max_connections",
            "must be between 1 and 16 in embedded mode",
        ));
    }
    if !(1..=MAX_CONCURRENT_CONNECTION_CREATIONS).contains(&limits.concurrent_connection_creations)
    {
        errors.push(EmbeddedValidationError::new(
            "storage.limits.concurrent_connection_creations",
            "must be between 1 and 4 in embedded mode",
        ));
    }
    if limits.concurrent_connection_creations > limits.max_connections {
        errors.push(EmbeddedValidationError::new(
            "storage.limits.concurrent_connection_creations",
            "must not exceed max_connections",
        ));
    }
    if !limits.connection_rate_per_second.is_finite()
        || limits.connection_rate_per_second <= 0.0
        || limits.connection_rate_per_second > MAX_CONNECTION_RATE_PER_SECOND
    {
        errors.push(EmbeddedValidationError::new(
            "storage.limits.connection_rate_per_second",
            "must be finite and between 0 and 16 in embedded mode",
        ));
    }
    if !(1..=MAX_CONNECTION_BURST).contains(&limits.connection_burst) {
        errors.push(EmbeddedValidationError::new(
            "storage.limits.connection_burst",
            "must be between 1 and 4 in embedded mode",
        ));
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn absent_storage_defaults_only_to_in_memory() {
        let decoded: EmbeddedEngineConfig =
            toml::from_str("[server]\n").expect("embedded configuration with default storage");

        assert_eq!(decoded.storage, EmbeddedStorageConfig::InMemory);
        assert_eq!(decoded.startup_timeout_ms, DEFAULT_STARTUP_TIMEOUT_MS);
        assert_eq!(decoded.effective_migration_policy(), None);
        decoded.validate().expect("default embedded configuration");
    }

    #[test]
    fn exact_limit_defaults_and_maxima_validate() {
        assert_eq!(
            EmbeddedDsqlLimits::default(),
            EmbeddedDsqlLimits {
                max_connections: 8,
                concurrent_connection_creations: 2,
                connection_rate_per_second: 8.0,
                connection_burst: 2,
            }
        );

        let mut config = managed_config();
        config.storage = EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
            limits: EmbeddedDsqlLimits {
                max_connections: 16,
                concurrent_connection_creations: 4,
                connection_rate_per_second: 16.0,
                connection_burst: 4,
            },
            ..managed_dsql_config()
        });
        config.validate().expect("maximum embedded envelope");
    }

    #[test]
    fn managed_mode_requires_intent_and_defaults_migration_after_validation() {
        let missing_intent = r#"
            [server]
            [storage]
            mode = "managed_dsql"
            descriptor_path = "cluster.json"
            region = "us-east-1"
        "#;
        assert!(toml::from_str::<EmbeddedEngineConfig>(missing_intent).is_err());

        let config = managed_config();
        config.validate().expect("managed configuration");
        assert_eq!(
            config.effective_migration_policy(),
            Some(DsqlMigrationPolicy::Automatic)
        );
    }

    #[test]
    fn existing_mode_requires_identity_and_explicit_migration_policy() {
        let missing_policy = r#"
            [server]
            [storage]
            mode = "existing_dsql"
            region = "us-east-1"
            cluster_id = "abcdefghijklmnopqrstuvwx12"
            cluster_arn = "arn:aws:dsql:us-east-1:123456789012:cluster/abcdefghijklmnopqrstuvwx12"
            endpoint = "example.dsql.us-east-1.on.aws"
        "#;
        assert!(toml::from_str::<EmbeddedEngineConfig>(missing_policy).is_err());

        let config = EmbeddedEngineConfig {
            storage: EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
                region: String::new(),
                cluster_id: String::new(),
                cluster_arn: String::new(),
                endpoint: String::new(),
                migration_policy: DsqlMigrationPolicy::ValidateOnly,
                limits: EmbeddedDsqlLimits::default(),
            }),
            ..EmbeddedEngineConfig::default()
        };
        let error = config.validate().expect_err("empty identity must fail");
        let EmbeddedConfigError::Validation(errors) = error else {
            panic!("expected embedded validation errors");
        };
        let fields = errors
            .into_iter()
            .map(|error| error.field)
            .collect::<Vec<_>>();
        assert!(fields.contains(&"storage.region".to_owned()));
        assert!(fields.contains(&"storage.cluster_id".to_owned()));
        assert!(fields.contains(&"storage.cluster_arn".to_owned()));
        assert!(fields.contains(&"storage.endpoint".to_owned()));
    }

    #[test]
    fn unknown_fields_are_rejected_at_every_embedded_level() {
        for document in [
            "[server]\nunknown = true\n",
            "[server]\n[storage]\nmode = \"in_memory\"\nunknown = true\n",
            concat!(
                "[server]\n[storage]\nmode = \"managed_dsql\"\n",
                "intent = \"create_or_recover\"\ndescriptor_path = \"cluster.json\"\n",
                "region = \"us-east-1\"\nunknown = true\n"
            ),
        ] {
            assert!(
                toml::from_str::<EmbeddedEngineConfig>(document).is_err(),
                "unknown field accepted in {document}"
            );
        }
    }

    #[test]
    fn validation_errors_name_fields_without_echoing_values() {
        let sensitive_region = "secret-region-canary";
        let sensitive_path = PathBuf::from("");
        let mut config = managed_config();
        let EmbeddedStorageConfig::ManagedDsql(managed) = &mut config.storage else {
            panic!("managed fixture");
        };
        managed.region = String::new();
        managed.descriptor_path = sensitive_path;
        let message = config
            .validate()
            .expect_err("invalid managed fields")
            .to_string();
        assert!(message.contains("storage.region"), "{message}");
        assert!(message.contains("storage.descriptor_path"), "{message}");
        assert!(!message.contains(sensitive_region), "{message}");
    }

    // Feature: managed-embedded-dsql, Property 1: embedded configuration is explicit and closed
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_embedded_configuration_is_explicit_and_closed(
            mode in 0u8..3,
            max_connections in 0usize..20,
            concurrent_creations in 0usize..8,
            rate in -2.0f64..20.0,
            burst in 0u64..8,
            include_managed_policy in any::<bool>(),
        ) {
            let limits = EmbeddedDsqlLimits {
                max_connections,
                concurrent_connection_creations: concurrent_creations,
                connection_rate_per_second: rate,
                connection_burst: burst,
            };
            let config = EmbeddedEngineConfig {
                storage: match mode {
                    0 => EmbeddedStorageConfig::InMemory,
                    1 => EmbeddedStorageConfig::ManagedDsql(ManagedEmbeddedDsqlConfig {
                        migration_policy: include_managed_policy
                            .then_some(DsqlMigrationPolicy::ValidateOnly),
                        limits: limits.clone(),
                        ..managed_dsql_config()
                    }),
                    _ => EmbeddedStorageConfig::ExistingDsql(ExistingEmbeddedDsqlConfig {
                        region: "us-east-1".to_owned(),
                        cluster_id: "abcdefghijklmnopqrstuvwx12".to_owned(),
                        cluster_arn: "arn:aws:dsql:us-east-1:123456789012:cluster/abcdefghijklmnopqrstuvwx12".to_owned(),
                        endpoint: "example.dsql.us-east-1.on.aws".to_owned(),
                        migration_policy: DsqlMigrationPolicy::ValidateOnly,
                        limits: limits.clone(),
                    }),
                },
                ..EmbeddedEngineConfig::default()
            };

            let encoded = toml::to_string(&config).expect("serialize embedded config");
            let decoded: EmbeddedEngineConfig =
                toml::from_str(&encoded).expect("deserialize embedded config");
            prop_assert_eq!(&decoded, &config);
            prop_assert_eq!(
                std::mem::discriminant(&decoded.storage),
                std::mem::discriminant(&config.storage),
            );

            let limits_valid = (1..=16).contains(&limits.max_connections)
                && (1..=4).contains(&limits.concurrent_connection_creations)
                && limits.concurrent_connection_creations <= limits.max_connections
                && limits.connection_rate_per_second.is_finite()
                && limits.connection_rate_per_second > 0.0
                && limits.connection_rate_per_second <= 16.0
                && (1..=4).contains(&limits.connection_burst);
            let expected_valid = mode == 0 || limits_valid;
            prop_assert_eq!(decoded.validate().is_ok(), expected_valid);

            let expected_policy = match mode {
                0 => None,
                1 if include_managed_policy => Some(DsqlMigrationPolicy::ValidateOnly),
                1 => Some(DsqlMigrationPolicy::Automatic),
                _ => Some(DsqlMigrationPolicy::ValidateOnly),
            };
            prop_assert_eq!(decoded.effective_migration_policy(), expected_policy);
        }
    }

    fn managed_config() -> EmbeddedEngineConfig {
        EmbeddedEngineConfig {
            storage: EmbeddedStorageConfig::ManagedDsql(managed_dsql_config()),
            ..EmbeddedEngineConfig::default()
        }
    }

    fn managed_dsql_config() -> ManagedEmbeddedDsqlConfig {
        ManagedEmbeddedDsqlConfig {
            intent: ManagedClusterIntent::CreateOrRecover,
            descriptor_path: PathBuf::from("cluster.json"),
            region: "us-east-1".to_owned(),
            migration_policy: None,
            limits: EmbeddedDsqlLimits::default(),
            tags: BTreeMap::new(),
        }
    }
}
