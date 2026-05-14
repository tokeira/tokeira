//! Compose service definitions.
//!
//! Builds the list of [`ComposeService`] descriptors from config. Each
//! service carries a `module` field for correct ownership reporting in
//! the IaC and deploy engines.

use std::collections::HashMap;

use tokeira_compose::ComposeService;
use tokeira_orchestrator::StorageKind;

use crate::config::ComposeConfig;

/// Module name for the tokeirad runtime service.
pub const MODULE_RUNTIME: &str = "runtime";

/// Module name for observability services (mimir, loki, grafana, alloy).
pub const MODULE_OBSERVABILITY: &str = "observability";

/// Build all compose service descriptors from config.
pub fn compose_services(config: &ComposeConfig) -> Vec<ComposeService> {
    let state_dir = config.deployment_dir.join(".tokeira-state");
    let config_dir = config.deployment_dir.join("config");
    let mimir_vol = format!("{}:/data", state_dir.join("mimir").display());
    let loki_vol = format!("{}:/loki", state_dir.join("loki").display());
    let grafana_vol = format!("{}:/var/lib/grafana", state_dir.join("grafana").display());
    let mimir_config_vol = format!(
        "{}:/etc/mimir/mimir.yaml",
        config_dir.join("mimir.yaml").display()
    );
    let loki_config_vol = format!(
        "{}:/etc/loki/loki.yaml",
        config_dir.join("loki.yaml").display()
    );
    let alloy_config_vol = format!(
        "{}:/etc/alloy/config.alloy",
        config_dir.join("alloy.alloy").display()
    );
    let grafana_provisioning_vol = format!(
        "{}:/etc/grafana/provisioning/",
        config_dir.join("grafana/provisioning").display()
    );
    let grafana_dashboards_vol = format!(
        "{}:/var/lib/grafana/dashboards/",
        config_dir.join("grafana/dashboards").display()
    );
    let mut tokeirad_volumes = Vec::new();
    let mut tokeirad_environment = HashMap::new();

    // Always mount tokeirad.toml so the container reads the operator's server config
    // (including storage kind and DSQL endpoint after writeback)
    let tokeirad_toml = config.deployment_dir.join("tokeirad.toml");
    if tokeirad_toml.exists() {
        tokeirad_volumes.push(format!(
            "{}:/etc/tokeira/tokeirad.toml:ro",
            tokeirad_toml.display()
        ));
        tokeirad_environment.insert(
            "TOKEIRA_CONFIG".into(),
            "/etc/tokeira/tokeirad.toml".into(),
        );
    }

    if config.storage == StorageKind::Dsql {
        // Docker requires absolute paths for bind mounts — expand ~ to the user's home directory
        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
        let aws_dir = format!("{home}/.aws");
        tokeirad_volumes.push(format!("{aws_dir}:/home/nonroot/.aws:ro"));
        tokeirad_environment.insert("HOME".into(), "/home/nonroot".into());
        let dsql = config.dsql.clone().unwrap_or_default();
        tokeirad_environment.insert("AWS_REGION".into(), dsql.region);
        for key in [
            "AWS_PROFILE",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_ROLE_ARN",
        ] {
            if let Ok(value) = std::env::var(key) {
                tokeirad_environment.insert(key.to_string(), value);
            }
        }
    }

    vec![
        ComposeService {
            name: "mimir".into(),
            image: config.observability.mimir_image.clone(),
            ports: vec!["9009:9009".into()],
            volumes: vec![mimir_vol, mimir_config_vol],
            environment: HashMap::new(),
            depends_on: Vec::new(),
            healthcheck: None,
            command: vec!["--config.file=/etc/mimir/mimir.yaml".into()],
        },
        ComposeService {
            name: "loki".into(),
            image: config.observability.loki_image.clone(),
            ports: vec!["3100:3100".into()],
            volumes: vec![loki_vol, loki_config_vol],
            environment: HashMap::new(),
            depends_on: Vec::new(),
            healthcheck: None,
            command: vec!["--config.file=/etc/loki/loki.yaml".into()],
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
            volumes: tokeirad_volumes,
            environment: tokeirad_environment,
            depends_on: Vec::new(),
            healthcheck: None,
            command: Vec::new(),
        },
        ComposeService {
            name: "grafana".into(),
            image: config.observability.grafana_image.clone(),
            ports: vec![format!(
                "{}:{}",
                config.observability.grafana_port, config.observability.grafana_port
            )],
            volumes: vec![
                grafana_vol,
                grafana_provisioning_vol,
                grafana_dashboards_vol,
            ],
            environment: HashMap::from([
                ("GF_SECURITY_ADMIN_USER".into(), "admin".into()),
                ("GF_SECURITY_ADMIN_PASSWORD".into(), "admin".into()),
            ]),
            depends_on: vec!["mimir".into(), "loki".into()],
            healthcheck: None,
            command: Vec::new(),
        },
        ComposeService {
            name: "alloy".into(),
            image: config.observability.alloy_image.clone(),
            ports: vec!["4317:4317".into(), "4318:4318".into()],
            volumes: vec![
                "/var/run/docker.sock:/var/run/docker.sock".into(),
                alloy_config_vol,
            ],
            environment: HashMap::new(),
            depends_on: vec!["tokeirad".into(), "mimir".into(), "loki".into()],
            healthcheck: None,
            command: vec!["run".into(), "/etc/alloy/config.alloy".into()],
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
