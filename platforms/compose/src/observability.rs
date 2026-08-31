//! The observability configuration bundle: a platform-owned kind.
//!
//! Both the content (`observability/` beside the definitions) and this
//! machinery — parameter substitution, content digests, the managed file
//! tree, drift detection — are the platform's: the bundle is
//! Tokeira-opinionated deployment description, not Docker capability, so it
//! lives with the platform and joins its definition namespace. The Compose
//! resource crate contributes
//! only the fencing contract: the well-known resource identity
//! (`tokeira_compose::config_content_resource_id`) its `Service` consumers
//! key their `TOKEIRA_CONFIG_DIGEST` on.
//!
//! Content is loaded at realization from the definition source's own
//! directory (desired-source companions, staged with the definition). A
//! retained revision folder therefore renders THAT revision's content, and
//! a dashboard edit is a plannable change to the deployment, never a code
//! release.
//!
//! Content layout, relative to the definition source directory:
//!
//! ```text
//! observability/
//!     templates/    alloy.alloy, mimir.yaml, loki.yaml,
//!                   grafana-datasources.yaml, grafana-dashboards.yaml
//!     dashboards/   *.json — every file ships as a Grafana dashboard
//!     alerts/       observability-alerts.yaml
//! ```
//!
//! Templates carry `{{ name }}` placeholders substituted from the kind's
//! authored parameters. Substitution is strict both ways: a placeholder the
//! renderer does not know is refused naming the file and the placeholder, so
//! a content typo surfaces at plan, not as a silently-literal `{{ tpyo }}`
//! in a running container.

use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokeira_iac as iac;
use tokeira_observability::testing::{AlertRuleValidator, DashboardValidator};
use tokeira_platform::declaration::{
    DeploymentRef, ObservabilityCheck, ObservabilityCheckOutcome, ObservabilityCheckReport,
    ObservabilityCheckStatus,
};

mod kind;

pub use kind::{KINDS, NAMESPACE, ObservabilityConfiguration, decode, namespace};

/// The module that owns the observability config-files resource.
const MODULE_OBSERVABILITY: &str = "observability";

const CONFIG_DIR: &str = "config";
const ALLOY_CONFIG: &str = "config/alloy.alloy";
const MIMIR_CONFIG: &str = "config/mimir.yaml";
const LOKI_CONFIG: &str = "config/loki.yaml";
const GRAFANA_DATASOURCES: &str = "config/grafana/provisioning/datasources/datasources.yaml";
const GRAFANA_DASHBOARDS: &str = "config/grafana/provisioning/dashboards/dashboards.yaml";
const ALERT_RULES: &str = "config/mimir/rules/observability-alerts.yaml";
const GRAFANA_DASHBOARD_DIR: &str = "config/grafana/dashboards";
const EXPECTED_SCRAPE_JOBS: &[&str] = &["tokeirad", "alloy", "mimir", "loki", "grafana"];

/// Companion-content locations, relative to the definition source directory.
const CONTENT_DIR: &str = "observability";
const CONTENT_TEMPLATES: &str = "templates";
const CONTENT_DASHBOARDS: &str = "dashboards";
const CONTENT_ALERTS: &str = "alerts/observability-alerts.yaml";

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

/// The loaded companion content: template sources, dashboards, alert rules.
///
/// Loaded from the definition source's `observability/` directory at
/// realization time; the bytes here are exactly what the operator ships, and
/// every rendered digest derives from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityContent {
    alloy: String,
    mimir: String,
    loki: String,
    grafana_datasources: String,
    grafana_dashboard_provider: String,
    alerts: String,
    /// Dashboard file name → contents, in name order. Every `*.json` under
    /// `dashboards/` ships — adding a dashboard is adding a file.
    dashboards: Vec<(String, String)>,
}

impl ObservabilityContent {
    /// Load the content set from one definition source directory.
    ///
    /// Absence is stated, never papered over: a missing directory or
    /// template names the exact path the deployment's description requires.
    pub(crate) fn load(definition_dir: &Path) -> Result<Self, ConfigGenError> {
        let root = definition_dir.join(CONTENT_DIR);
        let templates = root.join(CONTENT_TEMPLATES);
        let read = |path: PathBuf| -> Result<String, ConfigGenError> {
            fs::read_to_string(&path).map_err(|source| match source.kind() {
                io::ErrorKind::NotFound => ConfigGenError::MissingContent {
                    path: path.display().to_string(),
                },
                _ => ConfigGenError::ReadFailed {
                    path: path.display().to_string(),
                    source,
                },
            })
        };
        let mut dashboards = Vec::new();
        let dashboards_dir = root.join(CONTENT_DASHBOARDS);
        let entries = fs::read_dir(&dashboards_dir).map_err(|source| match source.kind() {
            io::ErrorKind::NotFound => ConfigGenError::MissingContent {
                path: dashboards_dir.display().to_string(),
            },
            _ => ConfigGenError::ReadFailed {
                path: dashboards_dir.display().to_string(),
                source,
            },
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| ConfigGenError::ReadFailed {
                path: dashboards_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json")
                && let Some(name) = path.file_name().and_then(|name| name.to_str())
            {
                dashboards.push((name.to_string(), read(path.clone())?));
            }
        }
        // Deterministic rendering order regardless of directory iteration.
        dashboards.sort();
        Ok(Self {
            alloy: read(templates.join("alloy.alloy"))?,
            mimir: read(templates.join("mimir.yaml"))?,
            loki: read(templates.join("loki.yaml"))?,
            grafana_datasources: read(templates.join("grafana-datasources.yaml"))?,
            grafana_dashboard_provider: read(templates.join("grafana-dashboards.yaml"))?,
            alerts: read(root.join(CONTENT_ALERTS))?,
            dashboards,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityParams {
    pub(crate) metrics_target_host: String,
    pub(crate) metrics_target_port: u16,
    pub(crate) cluster: String,
    pub(crate) deployment: String,
    pub(crate) mimir_remote_write_url: String,
    pub(crate) loki_push_url: String,
    pub(crate) mimir_http_port: u16,
    pub(crate) loki_http_port: u16,
    pub(crate) loki_retention_hours: u32,
}

/// Engine identity of the rendered-configuration resource — the provider's
/// fencing contract, implemented here.
pub fn configuration_resource_id() -> iac::ResourceId {
    tokeira_compose::config_content_resource_id()
}

impl ObservabilityParams {
    #[cfg(test)]
    fn reference() -> Self {
        Self {
            metrics_target_host: "tokeirad".into(),
            metrics_target_port: 9090,
            cluster: "tokeira".into(),
            deployment: "tokeira".into(),
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
    pub(crate) relative_path: PathBuf,
    pub(crate) contents: String,
}

#[derive(Debug, Error)]
pub enum ConfigGenError {
    #[error("invalid template parameter: {field} cannot be {reason}")]
    InvalidParameter { field: String, reason: String },
    #[error("observability content is missing at {path}")]
    MissingContent { path: String },
    #[error("failed to read observability content at {path}: {source}")]
    ReadFailed {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error(
        "template '{template}' names unknown placeholder `{{{{ {placeholder} }}}}`; \
         known placeholders: {known}"
    )]
    UnknownPlaceholder {
        template: String,
        placeholder: String,
        known: String,
    },
    #[error("template '{template}' has an unterminated `{{{{` placeholder")]
    UnterminatedPlaceholder { template: String },
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

/// Load the companion content and render the complete desired file set.
pub(crate) fn desired_files(
    definition_dir: &Path,
    params: &ObservabilityParams,
) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
    render_all(&ObservabilityContent::load(definition_dir)?, params)
}

/// Render the complete desired file set from loaded content.
pub(crate) fn render_all(
    content: &ObservabilityContent,
    params: &ObservabilityParams,
) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
    validate_params(params)?;
    let mut files = vec![
        rendered(
            ALLOY_CONFIG,
            substitute(
                "alloy.alloy",
                &content.alloy,
                &[
                    ("metrics_target_host", params.metrics_target_host.clone()),
                    (
                        "metrics_target_port",
                        params.metrics_target_port.to_string(),
                    ),
                    ("cluster", params.cluster.clone()),
                    ("deployment", params.deployment.clone()),
                    (
                        "mimir_remote_write_url",
                        params.mimir_remote_write_url.clone(),
                    ),
                    ("loki_push_url", params.loki_push_url.clone()),
                ],
            )?,
        ),
        rendered(
            MIMIR_CONFIG,
            substitute(
                "mimir.yaml",
                &content.mimir,
                &[("http_port", params.mimir_http_port.to_string())],
            )?,
        ),
        rendered(
            LOKI_CONFIG,
            substitute(
                "loki.yaml",
                &content.loki,
                &[
                    ("http_port", params.loki_http_port.to_string()),
                    ("retention_hours", params.loki_retention_hours.to_string()),
                ],
            )?,
        ),
        rendered(
            GRAFANA_DATASOURCES,
            substitute(
                "grafana-datasources.yaml",
                &content.grafana_datasources,
                &[
                    ("mimir_url", "http://mimir:9009/prometheus".to_string()),
                    ("loki_url", "http://loki:3100".to_string()),
                ],
            )?,
        ),
        rendered(
            GRAFANA_DASHBOARDS,
            substitute(
                "grafana-dashboards.yaml",
                &content.grafana_dashboard_provider,
                &[("dashboards_path", "/var/lib/grafana/dashboards".to_string())],
            )?,
        ),
        rendered(ALERT_RULES, content.alerts.clone()),
    ];
    for (name, contents) in &content.dashboards {
        files.push(RenderedConfigFile {
            relative_path: PathBuf::from(GRAFANA_DASHBOARD_DIR).join(name),
            contents: contents.clone(),
        });
    }
    Ok(files)
}

fn rendered(relative_path: &str, contents: String) -> RenderedConfigFile {
    RenderedConfigFile {
        relative_path: PathBuf::from(relative_path),
        contents,
    }
}

/// Strict `{{ name }}` substitution. Every placeholder in the source must be
/// a known parameter — an unknown one is refused naming the template, the
/// placeholder, and the known set, so content typos surface at plan time
/// instead of shipping literally into a running container.
fn substitute(
    template: &str,
    source: &str,
    values: &[(&str, String)],
) -> Result<String, ConfigGenError> {
    let mut output = String::with_capacity(source.len());
    let mut rest = source;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            return Err(ConfigGenError::UnterminatedPlaceholder {
                template: template.to_string(),
            });
        };
        let placeholder = after[..end].trim();
        match values.iter().find(|(name, _)| *name == placeholder) {
            Some((_, value)) => output.push_str(value),
            None => {
                return Err(ConfigGenError::UnknownPlaceholder {
                    template: template.to_string(),
                    placeholder: placeholder.to_string(),
                    known: values
                        .iter()
                        .map(|(name, _)| *name)
                        .collect::<Vec<_>>()
                        .join(", "),
                });
            }
        }
        rest = &after[end + 2..];
    }
    output.push_str(rest);
    Ok(output)
}

fn validate_params(params: &ObservabilityParams) -> Result<(), ConfigGenError> {
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

#[derive(Debug, Clone)]
pub struct ObservabilityConfigFilesResource {
    deployment_dir: PathBuf,
    definition_dir: PathBuf,
    params: ObservabilityParams,
}

impl ObservabilityConfigFilesResource {
    /// The resource's one word: engine resource type and author-visible kind
    /// name. The kind and the selection entry recover it from here.
    pub(crate) const TYPE: &'static str = "ObservabilityConfiguration";

    pub(crate) fn new(
        deployment_dir: PathBuf,
        definition_dir: PathBuf,
        params: ObservabilityParams,
    ) -> Self {
        Self {
            deployment_dir,
            definition_dir,
            params,
        }
    }

    pub(crate) fn resource_id_value() -> iac::ResourceId {
        tokeira_compose::config_content_resource_id()
    }

    pub(crate) fn desired_files(&self) -> Result<Vec<RenderedConfigFile>, ConfigGenError> {
        desired_files(&self.definition_dir, &self.params)
    }

    fn validate_rendered(&self) -> anyhow::Result<()> {
        let files = self.desired_files()?;
        let alloy = rendered_file(&files, ALLOY_CONFIG)?;
        for job in EXPECTED_SCRAPE_JOBS {
            let declaration = format!("prometheus.scrape \"{job}\"");
            if !alloy.contents.contains(&declaration) {
                anyhow::bail!("rendered Alloy config is missing expected scrape job `{job}`");
            }
        }

        let mut dashboard_count = 0;
        for file in files.iter().filter(|file| {
            file.relative_path.starts_with(GRAFANA_DASHBOARD_DIR)
                && file
                    .relative_path
                    .extension()
                    .is_some_and(|extension| extension == "json")
        }) {
            DashboardValidator::validate_str(&file.relative_path, &file.contents)?;
            dashboard_count += 1;
        }
        if dashboard_count == 0 {
            anyhow::bail!("rendered observability tree contains no Grafana dashboards");
        }

        let alerts = rendered_file(&files, ALERT_RULES)?;
        AlertRuleValidator::validate_str(
            &alerts.relative_path,
            &alerts.contents,
            &self.definition_dir,
        )?;
        Ok(())
    }

    fn write_all(&self) -> Result<iac::ResourceState, iac::IacError> {
        let files = self.desired_files()?;
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
            resource_type: iac::ResourceType::new(Self::TYPE),
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

    /// The managed relative paths for one operation: the recorded set when
    /// state exists (so destroy removes exactly what was written, even after
    /// the content set changed), the freshly-rendered set otherwise.
    fn managed_paths(&self, current: Option<&iac::ResourceState>) -> Vec<String> {
        if let Some(files) = current
            .and_then(|state| state.properties.get("files"))
            .and_then(|files| files.as_object())
        {
            return files.keys().cloned().collect();
        }
        self.desired_files()
            .map(|files| {
                files
                    .iter()
                    .map(|file| path_key(&file.relative_path))
                    .collect()
            })
            .unwrap_or_default()
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
        iac::ResourceType::new(Self::TYPE)
    }

    fn validate_input(&self) -> Result<(), String> {
        self.validate_rendered().map_err(|error| error.to_string())
    }

    fn desired_manifest(&self) -> serde_json::Value {
        match self.desired_files() {
            Ok(files) => {
                let files = files
                    .iter()
                    .map(|file| {
                        (
                            path_key(&file.relative_path),
                            file_property(file.contents.as_bytes()),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                json!({ "files": files })
            }
            Err(error) => json!({ "files": {}, "content_error": error.to_string() }),
        }
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
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        for relative_path in self.managed_paths(Some(current)) {
            let path = self.deployment_dir.join(&relative_path);
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
            // Content that fails to load or render is a stated condition on
            // the plan — the definition says the deployment carries this
            // content, so the plan names the problem instead of silently
            // planning nothing.
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
    /// paths (operator-explanation Req 4.2). Writes are in place (`write_all`
    /// overwrites the managed set); the delete genuinely removes the managed
    /// files — read from the delete implementation, as the spec demands —
    /// and is still reversible because the tree is a pure function of the
    /// definition and its companion content: `create` re-renders it
    /// identically from the same revision.
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
            "::ObservabilityConfigFilesResource::delete — fs::remove_file over the \
             recorded managed set; refuses non-empty foreign directories"
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
                provider_assigned: Vec::new(),
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
                provider_assigned: Vec::new(),
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
                provider_assigned: Vec::new(),
            },
            ChangeKind::NoChange => iac::ChangeSemantics::default(),
        }
    }
}

/// Compose's read-only check over the desired observability resource realized
/// from one admitted deployment definition.
#[derive(Debug, Default)]
pub(crate) struct RenderedObservabilityCheck;

impl ObservabilityCheck for RenderedObservabilityCheck {
    fn check(
        &self,
        _deployment: &DeploymentRef,
        resources: &[std::sync::Arc<dyn iac::Resource>],
    ) -> anyhow::Result<ObservabilityCheckReport> {
        let resource = resources
            .iter()
            .find(|resource| resource.resource_type().0 == ObservabilityConfigFilesResource::TYPE)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the realized definition contains no `{}` resource",
                    ObservabilityConfigFilesResource::TYPE
                )
            })?;
        // Realization validates kind inputs, but the verb deliberately invokes
        // the rendered-content contract itself so the operator gets a direct
        // check of this deployment rather than trusting construction alone.
        resource.validate_input().map_err(anyhow::Error::msg)?;

        let manifest = resource.desired_manifest();
        let files = manifest
            .get("files")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| anyhow::anyhow!("rendered observability manifest has no file set"))?;
        let dashboard_count = files
            .keys()
            .filter(|path| path.starts_with(GRAFANA_DASHBOARD_DIR) && path.ends_with(".json"))
            .count();
        let alert_count = usize::from(files.contains_key(ALERT_RULES));

        Ok(ObservabilityCheckReport {
            checks: vec![
                ObservabilityCheckOutcome {
                    name: "compose-scrapes",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!(
                        "{} expected Alloy scrape jobs rendered",
                        EXPECTED_SCRAPE_JOBS.len()
                    ),
                },
                ObservabilityCheckOutcome {
                    name: "compose-dashboards",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!(
                        "{dashboard_count} rendered Grafana dashboards satisfy the style contract"
                    ),
                },
                ObservabilityCheckOutcome {
                    name: "compose-alerts",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!(
                        "{alert_count} rendered Mimir alert bundle satisfies the style contract"
                    ),
                },
                ObservabilityCheckOutcome {
                    name: "live-backend-query",
                    status: ObservabilityCheckStatus::Warn,
                    detail:
                        "live Mimir/Loki/Grafana queries require a reachable deployment endpoint"
                            .to_string(),
                },
            ],
        })
    }
}

fn rendered_file<'a>(
    files: &'a [RenderedConfigFile],
    relative_path: &str,
) -> anyhow::Result<&'a RenderedConfigFile> {
    files
        .iter()
        .find(|file| file.relative_path == Path::new(relative_path))
        .ok_or_else(|| anyhow::anyhow!("rendered observability file is missing: {relative_path}"))
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
mod content_tests {
    use tokeira_observability::testing::{AlertRuleValidator, DashboardValidator};

    fn shipped_content() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("observability")
    }

    // The platform owns its observability content, so it owns the style
    // contract over it: every shipped dashboard and alert rule validates.
    #[test]
    fn dashboards_follow_the_style_contract() {
        DashboardValidator::validate_directory(&shipped_content().join("dashboards")).unwrap();
    }

    #[test]
    fn alert_rules_follow_the_style_contract() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("platforms/compose sits two levels below the workspace root")
            .to_path_buf();
        AlertRuleValidator::validate_directory(&shipped_content().join("alerts"), &repo_root)
            .unwrap();
    }

    use super::*;

    /// Write a minimal companion-content tree: every template a
    /// single-placeholder line, one dashboard, one alert file.
    fn content_fixture(root: &Path) {
        let obs = root.join(CONTENT_DIR);
        fs::create_dir_all(obs.join(CONTENT_TEMPLATES)).expect("templates dir");
        fs::create_dir_all(obs.join(CONTENT_DASHBOARDS)).expect("dashboards dir");
        fs::create_dir_all(obs.join("alerts")).expect("alerts dir");
        let template = obs.join(CONTENT_TEMPLATES);
        fs::write(
            template.join("alloy.alloy"),
            "target {{ metrics_target_host }}:{{ metrics_target_port }}\n",
        )
        .expect("alloy");
        fs::write(template.join("mimir.yaml"), "port: {{ http_port }}\n").expect("mimir");
        fs::write(
            template.join("loki.yaml"),
            "port: {{ http_port }}\nretention: {{ retention_hours }}\n",
        )
        .expect("loki");
        fs::write(
            template.join("grafana-datasources.yaml"),
            "mimir: {{ mimir_url }}\nloki: {{ loki_url }}\n",
        )
        .expect("datasources");
        fs::write(
            template.join("grafana-dashboards.yaml"),
            "path: {{ dashboards_path }}\n",
        )
        .expect("dashboard provider");
        fs::write(obs.join(CONTENT_ALERTS), "groups: []\n").expect("alerts");
        fs::write(
            obs.join(CONTENT_DASHBOARDS).join("edge.json"),
            "{\"title\":\"edge\"}",
        )
        .expect("dashboard");
    }

    /// Upgrade the generic rendering fixture into a style-valid tree carrying
    /// every Alloy declaration that the operator check promises to verify.
    fn check_fixture(root: &Path) {
        content_fixture(root);
        let obs = root.join(CONTENT_DIR);
        let scrapes = EXPECTED_SCRAPE_JOBS
            .iter()
            .map(|job| format!("prometheus.scrape \"{job}\" {{}}"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            obs.join(CONTENT_TEMPLATES).join("alloy.alloy"),
            format!(
                "target {{{{ metrics_target_host }}}}:{{{{ metrics_target_port }}}}\n{scrapes}\n"
            ),
        )
        .expect("Alloy fixture");
        fs::write(
            obs.join(CONTENT_DASHBOARDS).join("edge.json"),
            r#"{"templating":{"list":[{"name":"datasource","type":"datasource"}]},"panels":[]}"#,
        )
        .expect("dashboard fixture");
    }

    fn check_resource(root: &Path) -> ObservabilityConfigFilesResource {
        ObservabilityConfigFilesResource::new(
            root.join("deployment"),
            root.to_path_buf(),
            ObservabilityParams::reference(),
        )
    }

    #[test]
    fn rendered_deployment_tree_passes_the_operator_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_fixture(dir.path());
        let resources: Vec<std::sync::Arc<dyn iac::Resource>> =
            vec![std::sync::Arc::new(check_resource(dir.path()))];

        let report = RenderedObservabilityCheck
            .check(
                &DeploymentRef {
                    name: "fixture".to_string(),
                    dir: dir.path().join("deployment"),
                },
                &resources,
            )
            .expect("valid rendered tree");

        assert_eq!(report.checks.len(), 4);
        assert_eq!(
            report.checks.last().map(|check| check.status),
            Some(ObservabilityCheckStatus::Warn)
        );
    }

    // The command validates the rendered deployment tree, not merely the
    // platform's shipped source. A post-render style defect must therefore
    // make this deployment-specific check fail.
    #[test]
    fn rendered_deployment_style_violation_fails_the_operator_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_fixture(dir.path());
        fs::write(
            dir.path()
                .join(CONTENT_DIR)
                .join(CONTENT_DASHBOARDS)
                .join("edge.json"),
            r#"{"panels":[]}"#,
        )
        .expect("invalid rendered dashboard fixture");
        let resources: Vec<std::sync::Arc<dyn iac::Resource>> =
            vec![std::sync::Arc::new(check_resource(dir.path()))];

        let error = RenderedObservabilityCheck
            .check(
                &DeploymentRef {
                    name: "fixture".to_string(),
                    dir: dir.path().join("deployment"),
                },
                &resources,
            )
            .expect_err("style violation must fail the operator check");

        assert!(
            error.to_string().contains("missing $datasource variable"),
            "validator refusal is preserved: {error}"
        );
    }

    // Content is an input: rendering substitutes the authored parameters
    // into the shipped bytes, every dashboard file ships, and a parameter or
    // content change moves the digests that fence consumers.
    #[test]
    fn renders_from_companion_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        content_fixture(dir.path());
        let files = desired_files(dir.path(), &ObservabilityParams::reference()).expect("render");
        let by_path: BTreeMap<String, &str> = files
            .iter()
            .map(|file| (path_key(&file.relative_path), file.contents.as_str()))
            .collect();
        assert_eq!(by_path[MIMIR_CONFIG], "port: 9009\n");
        assert_eq!(by_path[ALLOY_CONFIG], "target tokeirad:9090\n");
        assert_eq!(
            by_path["config/grafana/dashboards/edge.json"],
            "{\"title\":\"edge\"}"
        );
        assert_eq!(by_path[ALERT_RULES], "groups: []\n");

        // A parameter change moves the rendered bytes deterministically.
        let mut params = ObservabilityParams::reference();
        params.mimir_http_port = 9010;
        let changed = desired_files(dir.path(), &params).expect("render changed");
        assert_ne!(files, changed);
    }

    // Absence is stated: the refusal names the exact missing path, so the
    // operator learns which companion the definition requires.
    #[test]
    fn missing_content_names_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = desired_files(dir.path(), &ObservabilityParams::reference())
            .expect_err("missing content refuses");
        assert!(
            error.to_string().contains(CONTENT_DIR),
            "refusal names the content location: {error}"
        );
    }

    // Strict substitution: an unknown placeholder in shipped content is a
    // located refusal naming template, placeholder, and the known set —
    // never a literal `{{ typo }}` in a running container.
    #[test]
    fn unknown_placeholder_is_refused_by_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        content_fixture(dir.path());
        fs::write(
            dir.path()
                .join(CONTENT_DIR)
                .join(CONTENT_TEMPLATES)
                .join("mimir.yaml"),
            "port: {{ http_prot }}\n",
        )
        .expect("typo template");
        let error =
            desired_files(dir.path(), &ObservabilityParams::reference()).expect_err("typo refuses");
        let message = error.to_string();
        assert!(
            message.contains("http_prot"),
            "names the placeholder: {message}"
        );
        assert!(
            message.contains("mimir.yaml"),
            "names the template: {message}"
        );
        assert!(
            message.contains("http_port"),
            "lists the known set: {message}"
        );
    }
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
            PathBuf::from("/tmp/x"),
            ObservabilityParams::reference(),
        )
    }

    // Golden declarations (operator-explanation Req 4.5): classification and
    // confidence only. The headline pair: updates are genuinely in place
    // (write_all), and the delete genuinely destroys the managed files —
    // yet stays reversible because the tree re-renders identically from
    // the definition and its companion content.
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
