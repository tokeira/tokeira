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
    pub mimir_image: String,
    pub mimir_replicas: u32,
    pub grafana_image: String,
    pub grafana_replicas: u32,
    pub loki_image: String,
    pub loki_replicas: u32,
    pub alloy_image: String,
    pub alloy_replicas: u32,
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
                image: "tokeirad:local".into(),
                grpc_port: 7233,
                metrics_port: 9090,
                replicas: 1,
            },
            observability: ObservabilityConfig {
                mimir_image: "grafana/mimir:3.0.6".into(),
                mimir_replicas: 1,
                grafana_image: "grafana/grafana-oss:12.4.3".into(),
                grafana_replicas: 1,
                loki_image: "grafana/loki:3.7.1".into(),
                loki_replicas: 1,
                alloy_image: "grafana/alloy:v1.16.0".into(),
                alloy_replicas: 1,
                grafana_port: 3000,
            },
        }
    }
}
