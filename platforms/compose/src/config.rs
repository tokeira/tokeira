//! Compose platform configuration model.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::StorageKind;

/// Aurora DSQL settings for compose deployments that use persistent storage.
///
/// Managed mode lets the compose IaC module create/delete the cluster.
/// Preexisting mode records an externally managed endpoint without taking
/// provider ownership.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComposeDsqlConfig {
    /// Lifecycle mode for the DSQL cluster resource.
    #[serde(default)]
    pub mode: DsqlMode,
    /// Existing or provisioned cluster endpoint, written back after infra apply.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Optional cluster ARN recorded for adopted clusters.
    #[serde(default)]
    pub arn: Option<String>,
    /// Explicit AWS region used for cluster operations and runtime IAM auth.
    #[serde(default = "default_dsql_region")]
    pub region: String,
}

impl Default for ComposeDsqlConfig {
    fn default() -> Self {
        Self {
            mode: DsqlMode::Managed,
            endpoint: None,
            arn: None,
            region: default_dsql_region(),
        }
    }
}

/// DSQL cluster lifecycle mode for the compose platform.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DsqlMode {
    /// `tkr infra apply` creates, updates, and destroys the cluster.
    #[default]
    Managed,
    /// `tkr infra apply` adopts endpoint metadata and skips provider deletion.
    Preexisting,
}

fn default_dsql_region() -> String {
    "us-east-1".into()
}

fn default_storage_kind() -> StorageKind {
    StorageKind::InMemory
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokeiradServiceConfig {
    pub image: String,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_mimir_image")]
    pub mimir_image: String,
    pub mimir_replicas: u32,
    #[serde(default = "default_grafana_image")]
    pub grafana_image: String,
    pub grafana_replicas: u32,
    #[serde(default = "default_loki_image")]
    pub loki_image: String,
    pub loki_replicas: u32,
    #[serde(default = "default_alloy_image")]
    pub alloy_image: String,
    pub alloy_replicas: u32,
    #[serde(default = "default_aws_cli_image")]
    pub aws_cli_image: String,
    #[serde(default = "default_busybox_image")]
    pub busybox_image: String,
    pub grafana_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposeConfig {
    pub project_name: String,
    /// Storage backend selected for the compose deployment.
    #[serde(default = "default_storage_kind")]
    pub storage: StorageKind,
    /// DSQL settings used when `storage = "dsql"`.
    #[serde(default)]
    pub dsql: Option<ComposeDsqlConfig>,
    pub tokeirad: TokeiradServiceConfig,
    pub observability: ObservabilityConfig,
    /// Deployment directory path — populated by the CLI after loading.
    /// Not serialized to TOML.
    #[serde(skip)]
    pub deployment_dir: PathBuf,
}

impl Default for ComposeConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
            storage: StorageKind::InMemory,
            dsql: None,
            tokeirad: TokeiradServiceConfig {
                image: "tokeirad:latest".into(),
                grpc_port: 7233,
                metrics_port: 9090,
                replicas: 1,
            },
            observability: ObservabilityConfig {
                mimir_image: default_mimir_image(),
                mimir_replicas: 1,
                grafana_image: default_grafana_image(),
                grafana_replicas: 1,
                loki_image: default_loki_image(),
                loki_replicas: 1,
                alloy_image: default_alloy_image(),
                alloy_replicas: 1,
                aws_cli_image: default_aws_cli_image(),
                busybox_image: default_busybox_image(),
                grafana_port: 3000,
            },
            deployment_dir: PathBuf::new(),
        }
    }
}

fn default_mimir_image() -> String {
    "grafana/mimir:3.0.6".into()
}

fn default_grafana_image() -> String {
    "grafana/grafana-oss:12.4.3".into()
}

fn default_loki_image() -> String {
    "grafana/loki:3.7.1".into()
}

fn default_alloy_image() -> String {
    "grafana/alloy:v1.16.0".into()
}

fn default_aws_cli_image() -> String {
    "public.ecr.aws/aws-cli/aws-cli:latest".into()
}

fn default_busybox_image() -> String {
    "public.ecr.aws/docker/library/busybox:latest".into()
}
