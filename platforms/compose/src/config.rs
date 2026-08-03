//! Compose deployment configuration admitted from the selected definition frontend.

use serde::{Deserialize, Serialize};
use tokeira_platform::error::ConfigError;

/// DSQL lifecycle choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DsqlMode {
    /// Create and own the cluster.
    Managed,
    /// Use a preexisting cluster.
    Preexisting,
}

/// Storage selected by a Compose deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Storage {
    /// Keep workflow state in memory.
    InMemory,
    /// Use Aurora DSQL plus coordination tables.
    Dsql(DsqlStorage),
}

/// Aurora DSQL storage settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlStorage {
    /// AWS region.
    pub region: String,
    /// Managed or adopted lifecycle.
    pub mode: DsqlMode,
    /// Adopted-cluster endpoint.
    pub endpoint: Option<String>,
    /// Adopted-cluster ARN.
    pub arn: Option<String>,
}

/// Tokeirad container settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tokeirad {
    /// Image reference.
    pub image: String,
    /// Desired container count.
    pub replicas: u32,
    /// Published Temporal gRPC port.
    pub grpc_port: u16,
    /// Published metrics port.
    pub metrics_port: u16,
}

/// One observability backend image and capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    /// Image reference.
    pub image: String,
    /// Desired container count.
    pub replicas: u32,
}

/// Grafana settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grafana {
    /// Image reference.
    pub image: String,
    /// Desired container count.
    pub replicas: u32,
    /// Published HTTP port.
    pub port: u16,
    /// Initial administrator password.
    pub admin_password: String,
}

/// Compose observability stack settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observability {
    /// Mimir container.
    pub mimir: Backend,
    /// Loki container.
    pub loki: Backend,
    /// Grafana container.
    pub grafana: Grafana,
    /// Alloy container.
    pub alloy: Backend,
}

/// Complete Compose configuration returned by a deployment definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeConfig {
    /// Workflow-state backend.
    pub storage: Storage,
    /// Tokeirad settings.
    pub tokeirad: Tokeirad,
    /// Observability stack settings.
    pub observability: Observability,
}

impl Default for ComposeConfig {
    fn default() -> Self {
        Self {
            storage: Storage::InMemory,
            tokeirad: Tokeirad {
                image: "tokeirad:latest".to_string(),
                replicas: 1,
                grpc_port: 7233,
                metrics_port: 9090,
            },
            observability: Observability {
                mimir: Backend {
                    image: "grafana/mimir:3.0.6".to_string(),
                    replicas: 1,
                },
                loki: Backend {
                    image: "grafana/loki:3.7.1".to_string(),
                    replicas: 1,
                },
                grafana: Grafana {
                    image: "grafana/grafana-oss:12.4.3".to_string(),
                    replicas: 1,
                    port: 3000,
                    admin_password: "admin".to_string(),
                },
                alloy: Backend {
                    image: "grafana/alloy:v1.16.0".to_string(),
                    replicas: 1,
                },
            },
        }
    }
}

/// Enforce Compose cross-field constraints without provider or filesystem access.
pub fn validate(config: &ComposeConfig) -> Result<(), ConfigError> {
    let require_container = |name: &str, image: &str, replicas: u32| {
        if image.is_empty() {
            return Err(ConfigError::validation(format!(
                "{name} image cannot be empty"
            )));
        }
        if replicas == 0 {
            return Err(ConfigError::validation(format!(
                "{name} replicas must be greater than zero"
            )));
        }
        Ok(())
    };
    require_container("tokeirad", &config.tokeirad.image, config.tokeirad.replicas)?;
    require_container(
        "mimir",
        &config.observability.mimir.image,
        config.observability.mimir.replicas,
    )?;
    require_container(
        "loki",
        &config.observability.loki.image,
        config.observability.loki.replicas,
    )?;
    require_container(
        "grafana",
        &config.observability.grafana.image,
        config.observability.grafana.replicas,
    )?;
    require_container(
        "alloy",
        &config.observability.alloy.image,
        config.observability.alloy.replicas,
    )?;
    if config.tokeirad.grpc_port == 0
        || config.tokeirad.metrics_port == 0
        || config.observability.grafana.port == 0
    {
        return Err(ConfigError::validation(
            "published Compose ports must be greater than zero",
        ));
    }
    if let Storage::Dsql(DsqlStorage {
        region,
        mode,
        endpoint,
        arn,
    }) = &config.storage
    {
        if region.is_empty() {
            return Err(ConfigError::validation("DSQL region cannot be empty"));
        }
        match mode {
            DsqlMode::Managed if endpoint.is_some() || arn.is_some() => {
                return Err(ConfigError::validation(
                    "managed DSQL storage cannot declare an endpoint or ARN",
                ));
            }
            DsqlMode::Preexisting if endpoint.is_none() || arn.is_none() => {
                return Err(ConfigError::validation(
                    "preexisting DSQL storage requires both endpoint and ARN",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preexisting_dsql_requires_complete_identity() {
        let mut config = crate::tests::reference_config();
        config.storage = Storage::Dsql(DsqlStorage {
            region: "eu-west-2".to_string(),
            mode: DsqlMode::Preexisting,
            endpoint: Some("cluster.dsql.eu-west-2.on.aws".to_string()),
            arn: None,
        });
        assert!(validate(&config).is_err());
    }
}
