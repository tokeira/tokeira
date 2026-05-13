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

use crate::{compose::MODULE_OBSERVABILITY, config::ComposeConfig};

const CONFIG_RESOURCE_ID: &str = "compose/observability-config-files";
const CONFIG_RESOURCE_TYPE: &str = "observability_config_files";
const CONFIG_DIR: &str = "config";
const ALLOY_CONFIG: &str = "config/alloy.alloy";
const MIMIR_CONFIG: &str = "config/mimir.yaml";
const LOKI_CONFIG: &str = "config/loki.yaml";
const GRAFANA_DATASOURCES: &str = "config/grafana/provisioning/datasources/datasources.yaml";
const GRAFANA_DASHBOARDS: &str = "config/grafana/provisioning/dashboards/dashboards.yaml";
const GRPC_EDGE_DASHBOARD: &str = "config/grafana/dashboards/grpc-edge-health.json";
const BROKER_RUNTIME_DASHBOARD: &str = "config/grafana/dashboards/broker-runtime-health.json";
const STORAGE_PROJECTION_DASHBOARD: &str =
    "config/grafana/dashboards/storage-projection-health.json";

const MANAGED_DIRECTORIES: &[&str] = &[
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
    pub mimir_remote_write_url: String,
    pub loki_push_url: String,
    pub mimir_http_port: u16,
    pub loki_http_port: u16,
    pub loki_retention_hours: u32,
}

impl ObservabilityParams {
    pub fn from_config(config: &ComposeConfig) -> Self {
        Self {
            metrics_target_host: "tokeirad".into(),
            metrics_target_port: config.tokeirad.metrics_port,
            mimir_remote_write_url: "http://mimir:9009/api/v1/push".into(),
            loki_push_url: "http://loki:3100/loki/api/v1/push".into(),
            mimir_http_port: 9009,
            loki_http_port: 3100,
            loki_retention_hours: 168,
        }
    }
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
            self.grpc_edge_dashboard(),
            self.broker_runtime_dashboard(),
            self.storage_projection_dashboard(),
        ])
    }

    fn validate(&self, params: &ObservabilityParams) -> Result<(), ConfigGenError> {
        validate_non_empty("metrics_target_host", &params.metrics_target_host)?;
        validate_non_zero("metrics_target_port", params.metrics_target_port)?;
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
                Err(error) => {
                    return Err(iac::IacError::Other(anyhow::anyhow!(
                        "failed to read observability config file at {}: {error}",
                        path.display()
                    )));
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
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        let Some(properties) = self.read_live_files()? else {
            return Ok(None);
        };
        Ok(Some(iac::ResourceState {
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
                    details: format!("failed to render desired config: {error}"),
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
            iac::InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: "observability config files changed".into(),
            }
        }
    }
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
        LOKI_CONFIG,
        GRAFANA_DATASOURCES,
        GRAFANA_DASHBOARDS,
        GRPC_EDGE_DASHBOARD,
        BROKER_RUNTIME_DASHBOARD,
        STORAGE_PROJECTION_DASHBOARD,
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
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;
    use tempfile::TempDir;
    use tokeira_iac::Resource;

    use super::*;

    fn default_params() -> ObservabilityParams {
        ObservabilityParams {
            metrics_target_host: "tokeirad".into(),
            metrics_target_port: 9090,
            mimir_remote_write_url: "http://mimir:9009/api/v1/push".into(),
            loki_push_url: "http://loki:3100/loki/api/v1/push".into(),
            mimir_http_port: 9009,
            loki_http_port: 3100,
            loki_retention_hours: 168,
        }
    }

    fn resource(temp: &TempDir) -> ObservabilityConfigFilesResource {
        ObservabilityConfigFilesResource::new(temp.path().to_path_buf(), default_params())
    }

    proptest! {
        #[test]
        fn render_all_injects_parameters(
            host in "[a-z][a-z0-9-]{0,12}",
            metrics_port in 1u16..=65535,
            mimir_port in 1u16..=65535,
            loki_port in 1u16..=65535,
            retention_hours in 1u32..=720,
        ) {
            let params = ObservabilityParams {
                metrics_target_host: host.clone(),
                metrics_target_port: metrics_port,
                mimir_remote_write_url: format!("http://mimir:{mimir_port}/api/v1/push"),
                loki_push_url: format!("http://loki:{loki_port}/loki/api/v1/push"),
                mimir_http_port: mimir_port,
                loki_http_port: loki_port,
                loki_retention_hours: retention_hours,
            };
            let generator = ConfigGenerator::new(PathBuf::new());
            let files = generator.render_all(&params)?;
            let alloy = contents_for(&files, ALLOY_CONFIG);
            let mimir = contents_for(&files, MIMIR_CONFIG);
            let loki = contents_for(&files, LOKI_CONFIG);
            let datasources = contents_for(&files, GRAFANA_DATASOURCES);
            let metrics_target = format!("{host}:{metrics_port}");
            let mimir_port_line = format!("http_listen_port: {mimir_port}");
            let loki_port_line = format!("http_listen_port: {loki_port}");
            let loki_retention_line = format!("retention_period: {retention_hours}h");

            prop_assert!(alloy.contains(&metrics_target), "alloy target missing");
            prop_assert!(alloy.contains(&params.mimir_remote_write_url), "mimir write URL missing");
            prop_assert!(alloy.contains(&params.loki_push_url), "loki push URL missing");
            prop_assert!(mimir.contains(&mimir_port_line), "mimir port missing");
            prop_assert!(loki.contains(&loki_port_line), "loki port missing");
            prop_assert!(loki.contains(&loki_retention_line), "loki retention missing");
            prop_assert!(datasources.contains("http://mimir:9009/prometheus"), "mimir datasource URL missing");
            prop_assert!(datasources.contains("http://loki:3100"), "loki datasource URL missing");
        }

        #[test]
        fn render_all_rejects_invalid_parameters(case in 0u8..6) {
            let mut params = default_params();
            match case {
                0 => params.metrics_target_host.clear(),
                1 => params.metrics_target_port = 0,
                2 => params.mimir_remote_write_url.clear(),
                3 => params.loki_push_url.clear(),
                4 => params.mimir_http_port = 0,
                _ => params.loki_http_port = 0,
            }
            let generator = ConfigGenerator::new(PathBuf::new());
            let rejected = matches!(
                generator.render_all(&params),
                Err(ConfigGenError::InvalidParameter { .. })
            );
            prop_assert!(rejected, "invalid parameters should be rejected");
        }
    }

    #[test]
    fn render_all_outputs_expected_paths() {
        let generator = ConfigGenerator::new(PathBuf::new());
        let files = generator.render_all(&default_params()).unwrap();
        let actual: BTreeSet<String> = files
            .iter()
            .map(|file| path_key(&file.relative_path))
            .collect();
        let expected: BTreeSet<String> = managed_relative_paths()
            .iter()
            .map(|path| (*path).to_string())
            .collect();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn config_resource_create_writes_all_files() {
        let temp = TempDir::new().unwrap();
        let resource = resource(&temp);
        let ctx = iac::ProvisionContext::default();
        resource.create(&ctx).await.unwrap();

        for relative_path in managed_relative_paths() {
            assert!(temp.path().join(relative_path).is_file());
        }
    }

    #[tokio::test]
    async fn describe_returns_none_when_all_files_are_missing() {
        let temp = TempDir::new().unwrap();
        let resource = resource(&temp);
        let ctx = iac::ProvisionContext::default();
        assert!(resource.describe(&ctx).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn diff_tracks_matching_and_changed_files() {
        let temp = TempDir::new().unwrap();
        let resource = resource(&temp);
        let ctx = iac::ProvisionContext::default();
        let state = resource.create(&ctx).await.unwrap();
        let no_change = resource.diff(&state, &ctx);
        assert!(matches!(no_change, iac::InternalChange::NoChange { .. }));

        fs::write(temp.path().join(ALLOY_CONFIG), "changed").unwrap();
        let live_state = resource.describe(&ctx).await.unwrap().unwrap();
        let change = resource.diff(&live_state, &ctx);
        assert!(matches!(change, iac::InternalChange::Update { .. }));
    }

    #[tokio::test]
    async fn delete_removes_only_managed_files_and_empty_dirs() {
        let temp = TempDir::new().unwrap();
        let resource = resource(&temp);
        let ctx = iac::ProvisionContext::default();
        let state = resource.create(&ctx).await.unwrap();
        let unrelated = temp.path().join("config/unrelated.txt");
        fs::write(&unrelated, "keep").unwrap();

        resource.delete(&state, &ctx).await.unwrap();

        for relative_path in managed_relative_paths() {
            assert!(!temp.path().join(relative_path).exists());
        }
        assert!(unrelated.exists());
    }

    fn contents_for<'a>(files: &'a [RenderedConfigFile], path: &str) -> &'a str {
        files
            .iter()
            .find(|file| file.relative_path == PathBuf::from(path))
            .map(|file| file.contents.as_str())
            .unwrap()
    }
}
