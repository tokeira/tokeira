//! Controller process configuration (binary-level).
//!
//! Distinct from `tokeira_controller::ControllerConfig` which holds library-level
//! placement parameters. This struct owns the full process lifecycle config
//! including DSQL connection details, listen addresses, and loop intervals.

use serde::Deserialize;

/// Top-level process configuration loaded from TOML.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControllerProcessConfig {
    pub dsql_endpoint: String,
    #[serde(default = "default_dsql_region")]
    pub dsql_region: String,
    #[serde(default = "default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
    #[serde(default = "default_placement_interval_secs")]
    pub placement_interval_secs: u64,
    #[serde(default = "default_budget_interval_secs")]
    pub budget_interval_secs: u64,
    pub cluster_name: String,
    #[serde(default = "default_dsql_connection_rate_budget")]
    pub dsql_connection_rate_budget: f64,
    #[serde(default = "default_dsql_connection_capacity_budget")]
    pub dsql_connection_capacity_budget: u64,
    #[serde(default)]
    pub placement: PlacementTable,
    #[serde(default)]
    pub membership: MembershipTable,
}

/// Nested `[placement]` table with topology parameters.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlacementTable {
    #[serde(default = "default_bundle_count")]
    pub bundle_count: u32,
    #[serde(default = "default_partition_count")]
    pub partition_count: u32,
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,
    #[serde(default = "default_hash_version")]
    pub hash_version: u32,
}

impl Default for PlacementTable {
    fn default() -> Self {
        Self {
            bundle_count: default_bundle_count(),
            partition_count: default_partition_count(),
            shard_count: default_shard_count(),
            hash_version: default_hash_version(),
        }
    }
}

/// Nested `[membership]` table with timing parameters.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipTable {
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_grace_interval_secs")]
    pub grace_interval_secs: u64,
    #[serde(default = "default_snapshot_publish_interval_secs")]
    pub snapshot_publish_interval_secs: u64,
    #[serde(default = "default_budget_directive_validity_secs")]
    pub budget_directive_validity_secs: u64,
}

impl Default for MembershipTable {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: default_heartbeat_interval_secs(),
            grace_interval_secs: default_grace_interval_secs(),
            snapshot_publish_interval_secs: default_snapshot_publish_interval_secs(),
            budget_directive_validity_secs: default_budget_directive_validity_secs(),
        }
    }
}

impl ControllerProcessConfig {
    /// Fail-fast validation. Called immediately after TOML deserialization.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.dsql_endpoint.is_empty() {
            anyhow::bail!("dsql_endpoint must not be empty");
        }
        if self.cluster_name.is_empty() {
            anyhow::bail!("cluster_name must not be empty");
        }
        Ok(())
    }

    /// Convert process config into the library-level `ControllerConfig`.
    pub(crate) fn to_controller_config(&self) -> tokeira_controller::ControllerConfig {
        tokeira_controller::ControllerConfig {
            controller_addr: self.grpc_listen_addr.clone(),
            heartbeat_interval: time::Duration::seconds(
                self.membership.heartbeat_interval_secs as i64,
            ),
            grace_interval: time::Duration::seconds(self.membership.grace_interval_secs as i64),
            snapshot_publish_interval: time::Duration::seconds(
                self.membership.snapshot_publish_interval_secs as i64,
            ),
            bundle_count: self.placement.bundle_count,
            partition_count: self.placement.partition_count,
            shard_count: self.placement.shard_count,
            hash_version: self.placement.hash_version,
            budget_directive_validity: time::Duration::seconds(
                self.membership.budget_directive_validity_secs as i64,
            ),
            dsql_connection_rate_budget: self.dsql_connection_rate_budget,
            dsql_connection_capacity_budget: self.dsql_connection_capacity_budget,
        }
    }
}

// ── Defaults ────────────────────────────────────────────────────────────────

fn default_dsql_region() -> String {
    String::new()
}

fn default_grpc_listen_addr() -> String {
    "0.0.0.0:9091".to_owned()
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_owned()
}

fn default_placement_interval_secs() -> u64 {
    5
}

fn default_budget_interval_secs() -> u64 {
    10
}

fn default_dsql_connection_rate_budget() -> f64 {
    100.0
}

fn default_dsql_connection_capacity_budget() -> u64 {
    10_000
}

fn default_bundle_count() -> u32 {
    64
}

fn default_partition_count() -> u32 {
    1024
}

fn default_shard_count() -> u32 {
    64
}

fn default_hash_version() -> u32 {
    1
}

fn default_heartbeat_interval_secs() -> u64 {
    5
}

fn default_grace_interval_secs() -> u64 {
    30
}

fn default_snapshot_publish_interval_secs() -> u64 {
    5
}

fn default_budget_directive_validity_secs() -> u64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_valid_toml_with_all_defaults() {
        let toml_str = r#"
            dsql_endpoint = "cluster.dsql.us-east-1.on.aws"
            cluster_name = "test-cluster"
        "#;
        let config: ControllerProcessConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config.dsql_endpoint, "cluster.dsql.us-east-1.on.aws");
        assert_eq!(config.cluster_name, "test-cluster");
        assert_eq!(config.dsql_region, "");
        assert_eq!(config.grpc_listen_addr, "0.0.0.0:9091");
        assert_eq!(config.metrics_addr, "0.0.0.0:9090");
        assert_eq!(config.placement_interval_secs, 5);
        assert_eq!(config.budget_interval_secs, 10);
        assert_eq!(config.dsql_connection_rate_budget, 100.0);
        assert_eq!(config.dsql_connection_capacity_budget, 10_000);
        assert_eq!(config.placement.bundle_count, 64);
        assert_eq!(config.placement.partition_count, 1024);
        assert_eq!(config.placement.shard_count, 64);
        assert_eq!(config.placement.hash_version, 1);
        assert_eq!(config.membership.heartbeat_interval_secs, 5);
        assert_eq!(config.membership.grace_interval_secs, 30);
        assert_eq!(config.membership.snapshot_publish_interval_secs, 5);
        assert_eq!(config.membership.budget_directive_validity_secs, 60);

        config.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_fields() {
        let toml_str = r#"
            dsql_endpoint = "cluster.dsql.us-east-1.on.aws"
            cluster_name = "test-cluster"
            unknown_field = "oops"
        "#;
        let result: Result<ControllerProcessConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "error should mention unknown field: {err}"
        );
    }

    #[test]
    fn rejects_unknown_fields_in_nested_placement_table() {
        let toml_str = r#"
            dsql_endpoint = "cluster.dsql.us-east-1.on.aws"
            cluster_name = "test-cluster"

            [placement]
            bundle_count = 128
            bogus = true
        "#;
        let result: Result<ControllerProcessConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "error should mention unknown field: {err}"
        );
    }

    #[test]
    fn rejects_unknown_fields_in_nested_membership_table() {
        let toml_str = r#"
            dsql_endpoint = "cluster.dsql.us-east-1.on.aws"
            cluster_name = "test-cluster"

            [membership]
            heartbeat_interval_secs = 10
            typo_field = 42
        "#;
        let result: Result<ControllerProcessConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "error should mention unknown field: {err}"
        );
    }

    #[test]
    fn validation_fails_on_empty_dsql_endpoint() {
        let toml_str = r#"
            dsql_endpoint = ""
            cluster_name = "test-cluster"
        "#;
        let config: ControllerProcessConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("dsql_endpoint must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validation_fails_on_empty_cluster_name() {
        let toml_str = r#"
            dsql_endpoint = "cluster.dsql.us-east-1.on.aws"
            cluster_name = ""
        "#;
        let config: ControllerProcessConfig = toml::from_str(toml_str).unwrap();
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("cluster_name must not be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn config_path_from_args_defaults_to_controller_toml() {
        // When no args are provided (simulated by empty args after skip(1)),
        // the function should return "controller.toml".
        // We test the logic directly by examining the function's behavior.
        // Since config_path_from_args reads std::env::args, we test the
        // parsing logic inline here.
        let result = parse_config_path(Vec::<String>::new().into_iter());
        assert_eq!(result.unwrap(), std::path::PathBuf::from("controller.toml"));
    }

    #[test]
    fn config_path_from_args_parses_config_flag() {
        let args = vec!["--config".to_string(), "/etc/controller.toml".to_string()];
        let result = parse_config_path(args.into_iter());
        assert_eq!(
            result.unwrap(),
            std::path::PathBuf::from("/etc/controller.toml")
        );
    }

    #[test]
    fn config_path_from_args_parses_positional_path() {
        let args = vec!["/tmp/my-config.toml".to_string()];
        let result = parse_config_path(args.into_iter());
        assert_eq!(
            result.unwrap(),
            std::path::PathBuf::from("/tmp/my-config.toml")
        );
    }

    #[test]
    fn config_path_from_args_errors_on_missing_path_after_flag() {
        let args = vec!["--config".to_string()];
        let result = parse_config_path(args.into_iter());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("--config requires a path"),
            "should indicate missing path"
        );
    }

    /// Testable version of config_path_from_args that accepts an iterator
    /// instead of reading from std::env::args.
    fn parse_config_path(
        mut args: impl Iterator<Item = String>,
    ) -> anyhow::Result<std::path::PathBuf> {
        use anyhow::Context;

        let Some(first) = args.next() else {
            return Ok(std::path::PathBuf::from("controller.toml"));
        };
        if first == "--config" {
            args.next()
                .map(std::path::PathBuf::from)
                .context("--config requires a path")
        } else {
            Ok(std::path::PathBuf::from(first))
        }
    }
}
