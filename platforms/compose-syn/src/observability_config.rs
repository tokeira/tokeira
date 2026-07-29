//! Generated observability configuration managed as IaC state.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use askama::Template;
use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokeira_iac as iac;

/// The module that owns the observability config-files resource.
const MODULE_OBSERVABILITY: &str = "observability";

const CONFIG_RESOURCE_ID: &str = "compose/observability-config-files";
const CONFIG_RESOURCE_TYPE: &str = "observability_config_files";
const CONFIG_DIR: &str = "config";
const ALLOY_CONFIG: &str = "config/alloy.alloy";
const MIMIR_CONFIG: &str = "config/mimir.yaml";
const LOKI_CONFIG: &str = "config/loki.yaml";
const GRAFANA_DATASOURCES: &str = "config/grafana/provisioning/datasources/datasources.yaml";
const GRAFANA_DASHBOARDS: &str = "config/grafana/provisioning/dashboards/dashboards.yaml";
const ALERT_RULES: &str = "config/mimir/rules/observability-alerts.yaml";
const GRPC_EDGE_DASHBOARD: &str = "config/grafana/dashboards/grpc-edge-health.json";
const BROKER_RUNTIME_DASHBOARD: &str = "config/grafana/dashboards/broker-runtime-health.json";
const STORAGE_PROJECTION_DASHBOARD: &str =
    "config/grafana/dashboards/storage-projection-health.json";
const LOG_EXPLORATION_DASHBOARD: &str = "config/grafana/dashboards/log-exploration.json";
const DSQL_CONNECTION_DASHBOARD: &str = "config/grafana/dashboards/dsql-connection-health.json";
const OCC_CONTENTION_DASHBOARD: &str = "config/grafana/dashboards/occ-contention.json";
const PLACEMENT_CONTROLLER_DASHBOARD: &str = "config/grafana/dashboards/placement-controller.json";
const AUTOSCALER_DASHBOARD: &str = "config/grafana/dashboards/autoscaler.json";
const PROJECTION_WORKERS_DASHBOARD: &str = "config/grafana/dashboards/projection-workers.json";
const INFRASTRUCTURE_HEALTH_DASHBOARD: &str =
    "config/grafana/dashboards/infrastructure-health.json";

const MANAGED_DIRECTORIES: &[&str] = &[
    "config/mimir/rules",
    "config/mimir",
    "config/grafana/provisioning/datasources",
    "config/grafana/provisioning/dashboards",
    "config/grafana/provisioning",
    "config/grafana/dashboards",
    "config/grafana",
    "config",
];

#[derive(Template, Debug)]
#[template(path = "alloy.alloy", escape = "none")]
pub struct AlloyConfigTemplate {
    pub metrics_target_host: String,
    pub metrics_target_port: u16,
    pub cluster: String,
    pub deployment: String,
    pub mimir_remote_write_url: String,
    pub loki_push_url: String,
}

#[derive(Template, Debug)]
#[template(path = "mimir.yaml", escape = "none")]
pub struct MimirConfigTemplate {
    pub http_port: u16,
}

#[derive(Template, Debug)]
#[template(path = "loki.yaml", escape = "none")]
pub struct LokiConfigTemplate {
    pub http_port: u16,
    pub retention_hours: u32,
}

#[derive(Template, Debug)]
#[template(path = "grafana-datasources.yaml", escape = "none")]
pub struct GrafanaDatasourcesTemplate {
    pub mimir_url: String,
    pub loki_url: String,
}

#[derive(Template, Debug)]
#[template(path = "grafana-dashboards.yaml", escape = "none")]
pub struct GrafanaDashboardProviderTemplate {
    pub dashboards_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityParams {
    pub metrics_target_host: String,
    pub metrics_target_port: u16,
    pub cluster: String,
    pub deployment: String,
    pub mimir_remote_write_url: String,
    pub loki_push_url: String,
    pub mimir_http_port: u16,
    pub loki_http_port: u16,
    pub loki_retention_hours: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedConfigFile {
    pub relative_path: PathBuf,
    pub contents: String,
}

#[derive(Debug, Error)]
pub enum ConfigGenError {
    #[error("invalid template parameter: {field} cannot be {reason}")]
    InvalidParameter { field: String, reason: String },
    #[error("failed to render template '{template}': {source}")]
    RenderFailed {
        template: String,
        #[source]
        source: askama::Error,
    },
    #[error("failed to write config file at {path}: {source}")]
    WriteFailed {
        path: String,
        #[source]
        source: io::Error,
    },
}

impl From<ConfigGenError> for iac::IacError {
    fn from(value: ConfigGenError) -> Self {
        iac::IacError::Other(anyhow::anyhow!(value))
    }
}

#[derive(Debug, Clone)]
pub struct ConfigGenerator;

impl ConfigGenerator {
    pub fn new(_deployment_dir: impl Into<PathBuf>) -> Self {
        Self
    }

    pub fn render_all(
        &self,
        params: &ObservabilityParams,
    ) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
        self.validate(params)?;
        Ok(vec![
            self.render_alloy(params)?,
            self.render_mimir(params)?,
            self.render_loki(params)?,
            self.render_grafana_datasources()?,
            self.render_grafana_dashboard_provider()?,
            self.alert_rules(),
            self.grpc_edge_dashboard(),
            self.broker_runtime_dashboard(),
            self.storage_projection_dashboard(),
            self.log_exploration_dashboard(),
            self.dsql_connection_dashboard(),
            self.occ_contention_dashboard(),
            self.placement_controller_dashboard(),
            self.autoscaler_dashboard(),
            self.projection_workers_dashboard(),
            self.infrastructure_health_dashboard(),
        ])
    }

    fn validate(&self, params: &ObservabilityParams) -> Result<(), ConfigGenError> {
        validate_non_empty("metrics_target_host", &params.metrics_target_host)?;
        validate_non_zero("metrics_target_port", params.metrics_target_port)?;
        validate_non_empty("cluster", &params.cluster)?;
        validate_non_empty("deployment", &params.deployment)?;
        validate_non_empty("mimir_remote_write_url", &params.mimir_remote_write_url)?;
        validate_non_empty("loki_push_url", &params.loki_push_url)?;
        validate_non_zero("mimir_http_port", params.mimir_http_port)?;
        validate_non_zero("loki_http_port", params.loki_http_port)?;
        Ok(())
    }

    fn render_alloy(
        &self,
        params: &ObservabilityParams,
    ) -> Result<RenderedConfigFile, ConfigGenError> {
        let template = AlloyConfigTemplate {
            metrics_target_host: params.metrics_target_host.clone(),
            metrics_target_port: params.metrics_target_port,
            cluster: params.cluster.clone(),
            deployment: params.deployment.clone(),
            mimir_remote_write_url: params.mimir_remote_write_url.clone(),
            loki_push_url: params.loki_push_url.clone(),
        };
        render_template(ALLOY_CONFIG, "alloy.alloy", &template)
    }

    fn render_mimir(
        &self,
        params: &ObservabilityParams,
    ) -> Result<RenderedConfigFile, ConfigGenError> {
        let template = MimirConfigTemplate {
            http_port: params.mimir_http_port,
        };
        render_template(MIMIR_CONFIG, "mimir.yaml", &template)
    }

    fn render_loki(
        &self,
        params: &ObservabilityParams,
    ) -> Result<RenderedConfigFile, ConfigGenError> {
        let template = LokiConfigTemplate {
            http_port: params.loki_http_port,
            retention_hours: params.loki_retention_hours,
        };
        render_template(LOKI_CONFIG, "loki.yaml", &template)
    }

    fn render_grafana_datasources(&self) -> Result<RenderedConfigFile, ConfigGenError> {
        let template = GrafanaDatasourcesTemplate {
            mimir_url: "http://mimir:9009/prometheus".into(),
            loki_url: "http://loki:3100".into(),
        };
        render_template(GRAFANA_DATASOURCES, "grafana-datasources.yaml", &template)
    }

    fn render_grafana_dashboard_provider(&self) -> Result<RenderedConfigFile, ConfigGenError> {
        let template = GrafanaDashboardProviderTemplate {
            dashboards_path: "/var/lib/grafana/dashboards".into(),
        };
        render_template(GRAFANA_DASHBOARDS, "grafana-dashboards.yaml", &template)
    }

    fn alert_rules(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(ALERT_RULES),
            contents: include_str!("../alerts/observability-alerts.yaml").to_string(),
        }
    }

    fn grpc_edge_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(GRPC_EDGE_DASHBOARD),
            contents: include_str!("../dashboards/grpc-edge-health.json").to_string(),
        }
    }

    fn broker_runtime_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(BROKER_RUNTIME_DASHBOARD),
            contents: include_str!("../dashboards/broker-runtime-health.json").to_string(),
        }
    }

    fn storage_projection_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(STORAGE_PROJECTION_DASHBOARD),
            contents: include_str!("../dashboards/storage-projection-health.json").to_string(),
        }
    }

    fn log_exploration_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(LOG_EXPLORATION_DASHBOARD),
            contents: include_str!("../dashboards/log-exploration.json").to_string(),
        }
    }

    fn dsql_connection_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(DSQL_CONNECTION_DASHBOARD),
            contents: include_str!("../dashboards/dsql-connection-health.json").to_string(),
        }
    }

    fn occ_contention_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(OCC_CONTENTION_DASHBOARD),
            contents: include_str!("../dashboards/occ-contention.json").to_string(),
        }
    }

    fn placement_controller_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(PLACEMENT_CONTROLLER_DASHBOARD),
            contents: include_str!("../dashboards/placement-controller.json").to_string(),
        }
    }

    fn autoscaler_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(AUTOSCALER_DASHBOARD),
            contents: include_str!("../dashboards/autoscaler.json").to_string(),
        }
    }

    fn projection_workers_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(PROJECTION_WORKERS_DASHBOARD),
            contents: include_str!("../dashboards/projection-workers.json").to_string(),
        }
    }

    fn infrastructure_health_dashboard(&self) -> RenderedConfigFile {
        RenderedConfigFile {
            relative_path: PathBuf::from(INFRASTRUCTURE_HEALTH_DASHBOARD),
            contents: include_str!("../dashboards/infrastructure-health.json").to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObservabilityConfigFilesResource {
    deployment_dir: PathBuf,
    params: ObservabilityParams,
}

impl ObservabilityConfigFilesResource {
    pub fn new(deployment_dir: PathBuf, params: ObservabilityParams) -> Self {
        Self {
            deployment_dir,
            params,
        }
    }

    pub fn resource_id_value() -> iac::ResourceId {
        iac::ResourceId(CONFIG_RESOURCE_ID.into())
    }

    pub fn desired_files(&self) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
        ConfigGenerator::new(&self.deployment_dir).render_all(&self.params)
    }

    fn ensure_directories(&self) -> Result<(), ConfigGenError> {
        for dir in [
            self.deployment_dir.join(CONFIG_DIR),
            self.deployment_dir
                .join("config/grafana/provisioning/datasources"),
            self.deployment_dir
                .join("config/grafana/provisioning/dashboards"),
            self.deployment_dir.join("config/grafana/dashboards"),
        ] {
            fs::create_dir_all(&dir).map_err(|source| ConfigGenError::WriteFailed {
                path: dir.display().to_string(),
                source,
            })?;
        }
        Ok(())
    }

    fn write_all(&self) -> Result<iac::ResourceState, iac::IacError> {
        let files = self.desired_files()?;
        self.ensure_directories()?;
        for file in &files {
            let path = self.deployment_dir.join(&file.relative_path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|source| ConfigGenError::WriteFailed {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
            // Self-heal a Docker bind-source stub: creating a container whose
            // bind source is missing makes the daemon manufacture an empty
            // DIRECTORY at the file's path (the pre-dependency-edge ordering
            // bug left these behind). `remove_dir` is non-recursive, so a
            // non-empty directory — real data — still refuses loudly.
            if path.is_dir() {
                fs::remove_dir(&path).map_err(|source| ConfigGenError::WriteFailed {
                    path: path.display().to_string(),
                    source,
                })?;
            }
            fs::write(&path, file.contents.as_bytes()).map_err(|source| {
                ConfigGenError::WriteFailed {
                    path: path.display().to_string(),
                    source,
                }
            })?;
        }
        Ok(self.state_from_files(&files))
    }

    fn state_from_files(&self, files: &[RenderedConfigFile]) -> iac::ResourceState {
        iac::ResourceState {
            resource_type: iac::ResourceType::new(CONFIG_RESOURCE_TYPE),
            physical_id: self.deployment_dir.join(CONFIG_DIR).display().to_string(),
            properties: properties_for_checksums(self.checksums_for_rendered(files), Vec::new()),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: MODULE_OBSERVABILITY.into(),
        }
    }

    fn checksums_for_rendered(
        &self,
        files: &[RenderedConfigFile],
    ) -> BTreeMap<String, serde_json::Value> {
        files
            .iter()
            .map(|file| {
                (
                    path_key(&file.relative_path),
                    file_property(file.contents.as_bytes()),
                )
            })
            .collect()
    }

    fn read_live_files(
        &self,
    ) -> Result<Option<BTreeMap<String, serde_json::Value>>, iac::IacError> {
        let files = self.desired_files()?;
        let mut live = BTreeMap::new();
        let mut missing = Vec::new();
        for file in files {
            let path = self.deployment_dir.join(&file.relative_path);
            match fs::read(&path) {
                Ok(contents) => {
                    live.insert(path_key(&file.relative_path), file_property(&contents));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    missing.push(path_key(&file.relative_path));
                }
                // An unreadable managed path — e.g. a Docker bind-source
                // DIRECTORY stub squatting where a file belongs — is drift,
                // not a refresh failure. Erroring here turns fail-closed into
                // fail-STUCK: it blocks the very destroy/apply that would
                // repair the corruption. Report it missing; the writer's
                // reconcile evicts empty stubs and rewrites the file.
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        %error,
                        "managed config path is unreadable — reporting as drifted/missing"
                    );
                    missing.push(path_key(&file.relative_path));
                }
            }
        }

        if live.is_empty() && !missing.is_empty() {
            return Ok(None);
        }

        Ok(Some(live_with_missing(live, missing)))
    }
}

#[async_trait]
impl iac::Resource for ObservabilityConfigFilesResource {
    fn resource_type(&self) -> iac::ResourceType {
        iac::ResourceType::new(CONFIG_RESOURCE_TYPE)
    }

    fn resource_id(&self) -> iac::ResourceId {
        Self::resource_id_value()
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        MODULE_OBSERVABILITY
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("configuration files")
    }

    async fn create(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        self.write_all()
    }

    async fn update(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        self.write_all()
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        for relative_path in managed_relative_paths() {
            let path = self.deployment_dir.join(relative_path);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                // A Docker bind-source DIRECTORY stub where our file belongs:
                // evict it (non-recursive — a non-empty directory is real
                // data and still refuses) rather than wedging the destroy.
                Err(_) if path.is_dir() => {
                    fs::remove_dir(&path).map_err(|error| {
                        iac::IacError::Other(anyhow::anyhow!(
                            "failed to remove directory stub at {}: {error}",
                            path.display()
                        ))
                    })?;
                }
                Err(error) => {
                    return Err(iac::IacError::Other(anyhow::anyhow!(
                        "failed to remove observability config file at {}: {error}",
                        path.display()
                    )));
                }
            }
        }

        for relative_dir in MANAGED_DIRECTORIES {
            remove_dir_if_empty(&self.deployment_dir.join(relative_dir))?;
        }
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &iac::ProvisionContext,
    ) -> Result<iac::DescribeResult, iac::IacError> {
        // `read_live_files` reads the managed config files from disk; their
        // absence is a confirmed Absent, not an unknown.
        let Some(properties) = self.read_live_files()? else {
            return Ok(iac::DescribeResult::Absent);
        };
        Ok(iac::DescribeResult::Present(iac::ResourceState {
            resource_type: self.resource_type(),
            physical_id: self.deployment_dir.join(CONFIG_DIR).display().to_string(),
            properties: json!({
                "deployment_dir": self.deployment_dir.display().to_string(),
                "files": properties.get("files").cloned().unwrap_or_else(|| json!({})),
                "missing": properties.get("missing").cloned().unwrap_or_else(|| json!([])),
            }),
            dependencies: self.dependencies(),
            created_at: String::new(),
            updated_at: String::new(),
            module: MODULE_OBSERVABILITY.into(),
        }))
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        let desired_files = match self.desired_files() {
            Ok(files) => files,
            Err(error) => {
                return iac::InternalChange::Update {
                    resource_id: self.resource_id(),
                    resource_type: self.resource_type(),
                    details: vec![iac::FieldDiff::observation(format!(
                        "failed to render desired config: {error}"
                    ))],
                };
            }
        };
        let desired = self.checksums_for_rendered(&desired_files);
        let current_files = current.properties.get("files");
        let current_missing = current.properties.get("missing");
        if current_files == Some(&json!(desired)) && current_missing == Some(&json!([])) {
            iac::InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            // Per-file evidence: which rendered file drifted (checksums
            // abbreviated), and which are missing on disk entirely.
            let to_strings = |value: Option<serde_json::Value>| {
                value
                    .and_then(|v| {
                        serde_json::from_value::<std::collections::BTreeMap<String, String>>(v).ok()
                    })
                    .unwrap_or_default()
            };
            let current_map = to_strings(current_files.cloned());
            let desired_map = to_strings(serde_json::to_value(&desired).ok());
            let mut details: Vec<iac::FieldDiff> = Vec::new();
            let mut names: Vec<&String> = current_map.keys().chain(desired_map.keys()).collect();
            names.sort();
            names.dedup();
            let short = |sum: Option<&String>| sum.map(|s| s.chars().take(12).collect::<String>());
            for name in names {
                let (before, after) = (current_map.get(name), desired_map.get(name));
                if before != after {
                    details.push(iac::FieldDiff {
                        field: name.clone(),
                        before: short(before),
                        after: short(after),
                    });
                }
            }
            if let Some(missing) = current_missing
                .and_then(|v| v.as_array())
                .filter(|list| !list.is_empty())
            {
                details.push(iac::FieldDiff::observation(format!(
                    "{} missing on disk",
                    tokeira_report_free_join(missing)
                )));
            }
            if details.is_empty() {
                // Same checksums but a differing shape (legacy record):
                // still an update, named as such.
                details.push(iac::FieldDiff::observation(
                    "config file record format changed",
                ));
            }
            iac::InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details,
            }
        }
    }

    /// What a config-tree change does, read from this file's own lifecycle
    /// paths (change-semantics task 4.2). Writes are in place (`write_all`
    /// overwrites the managed set); the delete genuinely removes the managed
    /// files — read from the delete implementation, as the spec demands —
    /// and is still reversible because the tree is a pure function of the
    /// definition: `create` re-renders it identically.
    fn change_semantics(&self, ctx: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
        // Cited by module identity, never repo layout; every name is a real
        // identifier in this module.
        const WRITE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::ObservabilityConfigFilesResource::{create,update} — write_all renders \
             the managed config set in place"
        ));
        const DELETE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::ObservabilityConfigFilesResource::delete — fs::remove_file over \
             managed_relative_paths(); refuses non-empty foreign directories"
        ));
        use iac::{
            ChangeKind, Confidence, DataEffect, Disruption, LifecycleOperation, ReplacementPolicy,
            Reversibility,
        };
        match ctx.kind {
            ChangeKind::Create => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: WRITE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: WRITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: WRITE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: WRITE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: DELETE,
                },
                statement: None,
            },
            // Definition-driven and drift-driven updates share `write_all`:
            // an in-place overwrite of the resource's own rendered
            // artifacts, which are derived entirely from the definition —
            // nothing operator-authored is touched, so the data is
            // preserved in the only sense that matters.
            ChangeKind::Update | ChangeKind::Replace => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::UpdatedInPlace,
                    citation: WRITE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: WRITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: WRITE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: WRITE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: WRITE,
                },
                statement: None,
            },
            ChangeKind::Delete => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: DELETE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: DELETE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: DELETE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Destroyed,
                    citation: DELETE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: WRITE,
                },
                statement: None,
            },
            ChangeKind::NoChange => iac::ChangeSemantics::default(),
        }
    }
}

/// Join a JSON string array for the one-line missing-files observation.
fn tokeira_report_free_join(values: &[serde_json::Value]) -> String {
    values
        .iter()
        .filter_map(|v| v.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), ConfigGenError> {
    if value.is_empty() {
        Err(ConfigGenError::InvalidParameter {
            field: field.into(),
            reason: "empty".into(),
        })
    } else {
        Ok(())
    }
}

fn validate_non_zero(field: &str, value: u16) -> Result<(), ConfigGenError> {
    if value == 0 {
        Err(ConfigGenError::InvalidParameter {
            field: field.into(),
            reason: "zero".into(),
        })
    } else {
        Ok(())
    }
}

fn render_template(
    relative_path: &str,
    template_name: &str,
    template: &impl Template,
) -> Result<RenderedConfigFile, ConfigGenError> {
    let contents = template
        .render()
        .map_err(|source| ConfigGenError::RenderFailed {
            template: template_name.into(),
            source,
        })?;
    Ok(RenderedConfigFile {
        relative_path: PathBuf::from(relative_path),
        contents,
    })
}

fn managed_relative_paths() -> &'static [&'static str] {
    &[
        ALLOY_CONFIG,
        MIMIR_CONFIG,
        ALERT_RULES,
        LOKI_CONFIG,
        GRAFANA_DATASOURCES,
        GRAFANA_DASHBOARDS,
        GRPC_EDGE_DASHBOARD,
        BROKER_RUNTIME_DASHBOARD,
        STORAGE_PROJECTION_DASHBOARD,
        LOG_EXPLORATION_DASHBOARD,
        DSQL_CONNECTION_DASHBOARD,
        OCC_CONTENTION_DASHBOARD,
        PLACEMENT_CONTROLLER_DASHBOARD,
        AUTOSCALER_DASHBOARD,
        PROJECTION_WORKERS_DASHBOARD,
        INFRASTRUCTURE_HEALTH_DASHBOARD,
    ]
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn file_property(contents: &[u8]) -> serde_json::Value {
    json!({
        "sha256": checksum(contents),
        "bytes": contents.len(),
    })
}

fn checksum(contents: &[u8]) -> String {
    let digest = Sha256::digest(contents);
    hex::encode(digest)
}

fn properties_for_checksums(
    files: BTreeMap<String, serde_json::Value>,
    missing: Vec<String>,
) -> serde_json::Value {
    json!({
        "files": files,
        "missing": missing,
    })
}

fn live_with_missing(
    files: BTreeMap<String, serde_json::Value>,
    missing: Vec<String>,
) -> BTreeMap<String, serde_json::Value> {
    let mut properties = BTreeMap::new();
    properties.insert("files".into(), json!(files));
    properties.insert("missing".into(), json!(missing));
    properties
}

fn remove_dir_if_empty(path: &Path) -> Result<(), iac::IacError> {
    if !path.exists() {
        return Ok(());
    }
    let mut entries = fs::read_dir(path).map_err(|error| {
        iac::IacError::Other(anyhow::anyhow!(
            "failed to inspect observability config directory at {}: {error}",
            path.display()
        ))
    })?;
    if entries.next().is_some() {
        return Ok(());
    }
    fs::remove_dir(path).map_err(|error| {
        iac::IacError::Other(anyhow::anyhow!(
            "failed to remove observability config directory at {}: {error}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod semantics_tests {
    use iac::{
        ChangeKind, Confidence, DataEffect, LifecycleOperation, Resource as _, SemanticsContext,
    };

    use super::*;

    fn resource() -> ObservabilityConfigFilesResource {
        ObservabilityConfigFilesResource::new(
            PathBuf::from("/tmp/x"),
            ObservabilityParams {
                metrics_target_host: "h".into(),
                metrics_target_port: 1,
                cluster: "c".into(),
                deployment: "d".into(),
                mimir_remote_write_url: "http://m".into(),
                loki_push_url: "http://l".into(),
                mimir_http_port: 2,
                loki_http_port: 3,
                loki_retention_hours: 4,
            },
        )
    }

    // Golden declarations (change-semantics task 4.5): classification and
    // confidence only. The headline pair: updates are genuinely in place
    // (write_all), and the delete genuinely destroys the managed files —
    // yet stays reversible because the tree re-renders identically from
    // the definition.
    #[test]
    fn config_tree_declarations_match_the_write_and_delete_paths() {
        let resource = resource();
        let declared = |kind: ChangeKind, diffs: &[iac::FieldDiff]| {
            resource.change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs: diffs,
            })
        };

        // Definition-driven and drift-driven updates share write_all.
        let update = declared(
            ChangeKind::Update,
            &[iac::FieldDiff::observation("config/mimir.yaml changed")],
        );
        let drift = declared(
            ChangeKind::Update,
            &[iac::FieldDiff::observation(
                "missing on disk: config/mimir.yaml",
            )],
        );
        assert_eq!(update, drift);
        assert!(matches!(
            update.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::UpdatedInPlace,
                ..
            }
        ));
        assert!(matches!(
            update.data_effect,
            Confidence::EngineFact {
                value: DataEffect::Preserved,
                ..
            }
        ));

        let delete = declared(ChangeKind::Delete, &[]);
        assert!(matches!(
            delete.data_effect,
            Confidence::EngineFact {
                value: DataEffect::Destroyed,
                ..
            }
        ));
        assert!(matches!(
            delete.reversibility,
            Confidence::EngineFact {
                value: iac::Reversibility::Reversible,
                ..
            }
        ));

        let create = declared(ChangeKind::Create, &[]);
        assert!(matches!(
            create.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::Created,
                ..
            }
        ));
        assert_eq!(
            declared(ChangeKind::NoChange, &[]),
            iac::ChangeSemantics::default()
        );
    }
}
