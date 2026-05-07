//! Compose platform configuration model.

use serde::{Deserialize, Serialize};

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
    pub tokeirad: TokeiradServiceConfig,
    pub observability: ObservabilityConfig,
}

impl Default for ComposeConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
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
