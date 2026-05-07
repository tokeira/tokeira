use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcsConfig {
    pub project_name: String,
    pub environment: String,
    pub region: String,
    pub tags: HashMap<String, String>,
    pub services: ServiceConfigs,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfigs {
    pub edge_api: ReplicaServiceConfig,
    pub edge_poll: ReplicaServiceConfig,
    pub runtime: DaemonServiceConfig,
    pub projection: ReplicaServiceConfig,
    pub controller: ReplicaServiceConfig,
    pub autoscaler: ReplicaServiceConfig,
    pub admin: ReplicaServiceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaServiceConfig {
    pub image: String,
    pub desired_count: u32,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: Option<u16>,
    pub metrics_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonServiceConfig {
    pub image: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: u16,
    pub metrics_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default = "default_mimir_image")]
    pub mimir_image: String,
    pub mimir_cpu: u32,
    pub mimir_memory_mb: u32,
    #[serde(default = "default_loki_image")]
    pub loki_image: String,
    pub loki_cpu: u32,
    pub loki_memory_mb: u32,
    #[serde(default = "default_grafana_image")]
    pub grafana_image: String,
    pub grafana_cpu: u32,
    pub grafana_memory_mb: u32,
    #[serde(default = "default_alloy_image")]
    pub alloy_image: String,
    pub alloy_cpu: u32,
    pub alloy_memory_mb: u32,
    #[serde(default = "default_aws_cli_image")]
    pub aws_cli_image: String,
    #[serde(default = "default_busybox_image")]
    pub busybox_image: String,
    pub retention_days: u32,
}

impl Default for EcsConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
            environment: "dev".into(),
            region: "us-east-1".into(),
            tags: HashMap::new(),
            services: ServiceConfigs::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl Default for ServiceConfigs {
    fn default() -> Self {
        Self {
            edge_api: ReplicaServiceConfig::new("tokeirad:latest", 2, Some(7233), 9090),
            edge_poll: ReplicaServiceConfig::new("tokeirad:latest", 2, None, 9090),
            runtime: DaemonServiceConfig::new("tokeirad:latest", 7233, 9090),
            projection: ReplicaServiceConfig::new("tokeirad:latest", 1, None, 9090),
            controller: ReplicaServiceConfig::new("tokeirad:latest", 1, None, 9090),
            autoscaler: ReplicaServiceConfig::new("tokeirad:latest", 1, None, 9090),
            admin: ReplicaServiceConfig::new("tokeirad:latest", 1, Some(7233), 9090),
        }
    }
}

impl ReplicaServiceConfig {
    fn new(image: &str, desired_count: u32, grpc_port: Option<u16>, metrics_port: u16) -> Self {
        Self {
            image: image.into(),
            desired_count,
            cpu: 512,
            memory_mb: 1024,
            grpc_port,
            metrics_port,
        }
    }
}

impl DaemonServiceConfig {
    fn new(image: &str, grpc_port: u16, metrics_port: u16) -> Self {
        Self {
            image: image.into(),
            cpu: 1024,
            memory_mb: 2048,
            grpc_port,
            metrics_port,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            mimir_image: default_mimir_image(),
            mimir_cpu: 512,
            mimir_memory_mb: 2048,
            loki_image: default_loki_image(),
            loki_cpu: 512,
            loki_memory_mb: 2048,
            grafana_image: default_grafana_image(),
            grafana_cpu: 256,
            grafana_memory_mb: 1024,
            alloy_image: default_alloy_image(),
            alloy_cpu: 256,
            alloy_memory_mb: 512,
            aws_cli_image: default_aws_cli_image(),
            busybox_image: default_busybox_image(),
            retention_days: 30,
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
