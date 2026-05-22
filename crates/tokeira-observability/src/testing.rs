//! Test utilities for observability contracts.
//!
//! These validators make dashboard and alert conventions executable. They are
//! intentionally deterministic and filesystem-only so default workspace tests do
//! not require Grafana, Prometheus, Loki, Mimir, Docker, or AWS credentials.

use std::{
    fmt::{self, Display},
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use crate::{ManifestError, MetricManifest, validate_manifests};

/// Validate manifests from tests without installing global recorders.
pub fn assert_manifests_valid(manifests: &[&MetricManifest]) -> Result<(), ManifestError> {
    validate_manifests(manifests)
}

/// Validation error for dashboard and alert artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactValidationError {
    /// File being validated.
    pub file: PathBuf,
    /// Logical field or panel path that failed.
    pub field: String,
    /// Operator-facing validation message.
    pub message: String,
}

impl ArtifactValidationError {
    fn new(file: impl Into<PathBuf>, field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            file: file.into(),
            field: field.into(),
            message: message.into(),
        }
    }
}

impl Display for ArtifactValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}: {}",
            self.file.display(),
            self.field,
            self.message
        )
    }
}

impl std::error::Error for ArtifactValidationError {}

/// Grafana dashboard style and portability validator.
///
/// The validator checks only conventions that are stable in source-controlled
/// JSON: datasource templating, operator-facing descriptions, explicit units,
/// and time-series presentation rules.
#[derive(Debug, Default)]
pub struct DashboardValidator;

impl DashboardValidator {
    /// Validate one dashboard JSON file.
    pub fn validate_file(path: &Path) -> Result<(), ArtifactValidationError> {
        let contents = fs::read_to_string(path).map_err(|error| {
            ArtifactValidationError::new(path, "file", format!("failed to read dashboard: {error}"))
        })?;
        Self::validate_str(path, &contents)
    }

    /// Validate all JSON dashboards in a directory.
    pub fn validate_directory(path: &Path) -> Result<(), ArtifactValidationError> {
        for entry in fs::read_dir(path).map_err(|error| {
            ArtifactValidationError::new(
                path,
                "directory",
                format!("failed to read dashboard directory: {error}"),
            )
        })? {
            let entry = entry.map_err(|error| {
                ArtifactValidationError::new(
                    path,
                    "directory",
                    format!("failed to read dashboard entry: {error}"),
                )
            })?;
            let entry_path = entry.path();
            if entry_path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                Self::validate_file(&entry_path)?;
            }
        }
        Ok(())
    }

    /// Validate dashboard JSON contents.
    pub fn validate_str(path: &Path, contents: &str) -> Result<(), ArtifactValidationError> {
        let dashboard: Value = serde_json::from_str(contents).map_err(|error| {
            ArtifactValidationError::new(path, "json", format!("invalid dashboard JSON: {error}"))
        })?;
        Self::validate_datasource_template(path, &dashboard)?;
        let panels = dashboard
            .get("panels")
            .and_then(Value::as_array)
            .ok_or_else(|| ArtifactValidationError::new(path, "panels", "missing panel list"))?;
        for panel in panels {
            Self::validate_panel(path, panel)?;
        }
        Ok(())
    }

    fn validate_datasource_template(
        path: &Path,
        dashboard: &Value,
    ) -> Result<(), ArtifactValidationError> {
        let templates = dashboard
            .pointer("/templating/list")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ArtifactValidationError::new(
                    path,
                    "templating.list",
                    "missing $datasource variable",
                )
            })?;
        let has_datasource = templates.iter().any(|template| {
            template.get("name").and_then(Value::as_str) == Some("datasource")
                && template.get("type").and_then(Value::as_str) == Some("datasource")
        });
        if has_datasource {
            Ok(())
        } else {
            Err(ArtifactValidationError::new(
                path,
                "templating.list",
                "missing $datasource variable",
            ))
        }
    }

    fn validate_panel(path: &Path, panel: &Value) -> Result<(), ArtifactValidationError> {
        let panel_type = panel
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if panel_type == "row" {
            return Ok(());
        }

        let title = panel
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("<untitled>");
        if panel
            .get("description")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(ArtifactValidationError::new(
                path,
                format!("panel[{title}].description"),
                "panel must include an operator-facing description",
            ));
        }

        if Self::requires_unit(panel_type)
            && panel
                .pointer("/fieldConfig/defaults/unit")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(ArtifactValidationError::new(
                path,
                format!("panel[{title}].fieldConfig.defaults.unit"),
                "panel must declare an explicit unit",
            ));
        }

        if panel_type == "timeseries" {
            let interpolation = panel
                .pointer("/fieldConfig/defaults/custom/lineInterpolation")
                .and_then(Value::as_str);
            if interpolation != Some("smooth") {
                return Err(ArtifactValidationError::new(
                    path,
                    format!("panel[{title}].fieldConfig.defaults.custom.lineInterpolation"),
                    "time-series panels must use smooth interpolation",
                ));
            }

            let show_points = panel
                .pointer("/fieldConfig/defaults/custom/showPoints")
                .and_then(Value::as_str);
            if show_points != Some("never") {
                return Err(ArtifactValidationError::new(
                    path,
                    format!("panel[{title}].fieldConfig.defaults.custom.showPoints"),
                    "time-series panels must not show point markers",
                ));
            }
        }

        if let Some(children) = panel.get("panels").and_then(Value::as_array) {
            for child in children {
                Self::validate_panel(path, child)?;
            }
        }
        Ok(())
    }

    fn requires_unit(panel_type: &str) -> bool {
        matches!(panel_type, "timeseries" | "stat" | "barchart" | "gauge")
    }
}

/// Alert rule portability validator.
///
/// Alert rules must carry bounded ownership metadata and point operators at a
/// stable runbook target. The validator accepts either repository-relative
/// runbooks or absolute HTTP(S) URLs.
#[derive(Debug, Default)]
pub struct AlertRuleValidator;

impl AlertRuleValidator {
    /// Validate all YAML alert rule files in a directory.
    pub fn validate_directory(
        path: &Path,
        repo_root: &Path,
    ) -> Result<(), ArtifactValidationError> {
        if !path.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(path).map_err(|error| {
            ArtifactValidationError::new(
                path,
                "directory",
                format!("failed to read alert directory: {error}"),
            )
        })? {
            let entry = entry.map_err(|error| {
                ArtifactValidationError::new(
                    path,
                    "directory",
                    format!("failed to read alert entry: {error}"),
                )
            })?;
            let entry_path = entry.path();
            if is_yaml_file(&entry_path) {
                Self::validate_file(&entry_path, repo_root)?;
            }
        }
        Ok(())
    }

    /// Validate one alert rule file.
    pub fn validate_file(path: &Path, repo_root: &Path) -> Result<(), ArtifactValidationError> {
        let contents = fs::read_to_string(path).map_err(|error| {
            ArtifactValidationError::new(
                path,
                "file",
                format!("failed to read alert rules: {error}"),
            )
        })?;
        Self::validate_str(path, &contents, repo_root)
    }

    /// Validate alert rule YAML contents.
    pub fn validate_str(
        path: &Path,
        contents: &str,
        repo_root: &Path,
    ) -> Result<(), ArtifactValidationError> {
        let yaml: serde_yaml::Value = serde_yaml::from_str(contents).map_err(|error| {
            ArtifactValidationError::new(path, "yaml", format!("invalid alert YAML: {error}"))
        })?;
        let groups = yaml
            .get("groups")
            .and_then(serde_yaml::Value::as_sequence)
            .ok_or_else(|| ArtifactValidationError::new(path, "groups", "missing alert groups"))?;
        for group in groups {
            let rules = group
                .get("rules")
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or_else(|| {
                    ArtifactValidationError::new(path, "groups.rules", "missing alert rules")
                })?;
            for rule in rules {
                Self::validate_rule(path, rule, repo_root)?;
            }
        }
        Ok(())
    }

    fn validate_rule(
        path: &Path,
        rule: &serde_yaml::Value,
        repo_root: &Path,
    ) -> Result<(), ArtifactValidationError> {
        let alert = rule
            .get("alert")
            .and_then(serde_yaml::Value::as_str)
            .unwrap_or("<unnamed>");
        let labels = rule.get("labels");
        let annotations = rule.get("annotations");
        for field in ["severity", "service"] {
            if yaml_string(labels, field).is_none() && yaml_string(annotations, field).is_none() {
                return Err(ArtifactValidationError::new(
                    path,
                    format!("alert[{alert}].{field}"),
                    "alert must declare bounded ownership metadata",
                ));
            }
        }
        for field in ["summary", "runbook_url"] {
            if yaml_string(annotations, field).is_none_or(str::is_empty) {
                return Err(ArtifactValidationError::new(
                    path,
                    format!("alert[{alert}].annotations.{field}"),
                    "alert must declare required annotation",
                ));
            }
        }
        let runbook_url = yaml_string(annotations, "runbook_url").unwrap();
        if is_stable_url(runbook_url) || repo_root.join(runbook_url).exists() {
            Ok(())
        } else {
            Err(ArtifactValidationError::new(
                path,
                format!("alert[{alert}].annotations.runbook_url"),
                format!("runbook target does not exist: {runbook_url}"),
            ))
        }
    }
}

fn yaml_string<'a>(value: Option<&'a serde_yaml::Value>, key: &str) -> Option<&'a str> {
    value?
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .filter(|value| !value.is_empty())
}

fn is_yaml_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| matches!(extension, "yaml" | "yml"))
}

fn is_stable_url(value: &str) -> bool {
    value.starts_with("https://") || value.starts_with("http://")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("crate should live under crates/")
            .to_path_buf()
    }

    #[test]
    fn compose_dashboards_follow_style_contract() {
        let dashboards = repo_root().join("platforms/compose/dashboards");

        DashboardValidator::validate_directory(&dashboards).unwrap();
    }

    #[test]
    fn dashboard_validator_rejects_missing_datasource_template() {
        let dashboard = r#"{"panels":[]}"#;

        let error = DashboardValidator::validate_str(Path::new("bad-dashboard.json"), dashboard)
            .unwrap_err();

        assert_eq!(error.field, "templating.list");
        assert!(error.to_string().contains("bad-dashboard.json"));
    }

    #[test]
    fn dashboard_validator_rejects_time_series_points() {
        let dashboard = r#"{
            "templating": {
                "list": [{"name":"datasource","type":"datasource"}]
            },
            "panels": [{
                "type":"timeseries",
                "title":"Latency",
                "description":"Latency over time",
                "fieldConfig": {
                    "defaults": {
                        "unit":"s",
                        "custom": {
                            "lineInterpolation":"linear",
                            "showPoints":"always"
                        }
                    }
                }
            }]
        }"#;

        let error = DashboardValidator::validate_str(Path::new("bad-dashboard.json"), dashboard)
            .unwrap_err();

        assert!(error.field.contains("lineInterpolation"));
    }

    #[test]
    fn current_alert_directory_is_valid_when_present() {
        let root = repo_root();
        let alerts = root.join("platforms/compose/alerts");

        AlertRuleValidator::validate_directory(&alerts, &root).unwrap();
    }

    #[test]
    fn alert_validator_requires_runbook_targets() {
        let alert = r#"
groups:
  - name: tokeira
    rules:
      - alert: DsqlReservoirExhaustion
        expr: vector(1)
        labels:
          severity: page
          service: tokeirad
        annotations:
          summary: Reservoir exhausted
          runbook_url: docs/runbooks/observability/missing.md
"#;

        let error = AlertRuleValidator::validate_str(Path::new("alerts.yaml"), alert, &repo_root())
            .unwrap_err();

        assert!(error.field.contains("runbook_url"));
    }
}
