//! Compose service definitions.
//!
//! Builds the list of [`ComposeService`] descriptors from config. Each
//! service carries a `module` field for correct ownership reporting in
//! the IaC and deploy engines.

use std::collections::HashMap;

use tokeira_compose::ComposeService;

use crate::config::ComposeConfig;

/// Module name for the tokeirad runtime service.
pub const MODULE_RUNTIME: &str = "runtime";

/// Module name for observability services (mimir, loki, grafana, alloy).
pub const MODULE_OBSERVABILITY: &str = "observability";

/// Build all compose service descriptors from config.
pub fn compose_services(config: &ComposeConfig) -> Vec<ComposeService> {
    let tokeirad_metrics = format!("host.docker.internal:{}", config.tokeirad.metrics_port);
    let state_dir = config.deployment_dir.join(".tokeira-state");
    let mimir_vol = format!("{}:/data", state_dir.join("mimir").display());
    let loki_vol = format!("{}:/loki", state_dir.join("loki").display());
    let grafana_vol = format!("{}:/var/lib/grafana", state_dir.join("grafana").display());
    vec![
        ComposeService {
            name: "mimir".into(),
            image: config.observability.mimir_image.clone(),
            ports: vec!["9009:9009".into()],
            volumes: vec![mimir_vol],
            environment: HashMap::new(),
            depends_on: Vec::new(),
            healthcheck: None,
        },
        ComposeService {
            name: "loki".into(),
            image: config.observability.loki_image.clone(),
            ports: vec!["3100:3100".into()],
            volumes: vec![loki_vol],
            environment: HashMap::new(),
            depends_on: Vec::new(),
            healthcheck: None,
        },
        ComposeService {
            name: "tokeirad".into(),
            image: config.tokeirad.image.clone(),
            ports: vec![
                format!(
                    "{}:{}",
                    config.tokeirad.grpc_port, config.tokeirad.grpc_port
                ),
                format!(
                    "{}:{}",
                    config.tokeirad.metrics_port, config.tokeirad.metrics_port
                ),
            ],
            volumes: Vec::new(),
            environment: HashMap::new(),
            depends_on: Vec::new(),
            healthcheck: None,
        },
        ComposeService {
            name: "grafana".into(),
            image: config.observability.grafana_image.clone(),
            ports: vec![format!(
                "{}:{}",
                config.observability.grafana_port, config.observability.grafana_port
            )],
            volumes: vec![grafana_vol],
            environment: HashMap::from([
                ("GF_SECURITY_ADMIN_USER".into(), "admin".into()),
                ("GF_SECURITY_ADMIN_PASSWORD".into(), "admin".into()),
            ]),
            depends_on: vec!["mimir".into(), "loki".into()],
            healthcheck: None,
        },
        ComposeService {
            name: "alloy".into(),
            image: config.observability.alloy_image.clone(),
            ports: vec!["4317:4317".into(), "4318:4318".into()],
            volumes: vec!["/var/run/docker.sock:/var/run/docker.sock".into()],
            environment: HashMap::from([
                ("TOKEIRAD_METRICS_TARGET".into(), tokeirad_metrics),
                (
                    "MIMIR_REMOTE_WRITE_URL".into(),
                    "http://mimir:9009/api/v1/push".into(),
                ),
                (
                    "LOKI_WRITE_URL".into(),
                    "http://loki:3100/loki/api/v1/push".into(),
                ),
            ]),
            depends_on: vec!["tokeirad".into(), "mimir".into(), "loki".into()],
            healthcheck: None,
        },
    ]
}

/// Map a service name to its owning module.
pub fn module_for_service(service_name: &str) -> &'static str {
    match service_name {
        "tokeirad" => MODULE_RUNTIME,
        "mimir" | "loki" | "grafana" | "alloy" => MODULE_OBSERVABILITY,
        _ => "unknown",
    }
}
