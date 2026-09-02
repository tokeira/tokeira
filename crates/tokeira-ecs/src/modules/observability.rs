//! Definition-owned observability content, credentials, and Alloy rendering.
//!
//! Artifact loading follows the staged definition directory. Role helpers keep
//! S3 and Secrets Manager permissions scoped to the resources declared by the
//! definition, while [`AlloyRenderContext`] carries authored identity without
//! reconstructing it from defaults.

use std::{collections::HashMap, path::Path};

use tokeira_aws::{
    ResourceContext,
    resources::iam_role::{IamRole, IamRoleConfig},
};
use tokeira_iac::IacError;

use crate::config::EcsConfig;

/// One deployed observability document: its artifact-bucket key, content
/// type, and content, loaded from the deployment's observability content
/// directory.
#[derive(Debug, Clone)]
pub struct ObservabilityArtifact {
    pub key: String,
    pub content_type: &'static str,
    pub content: String,
}

/// Load the deployment's observability documents from `content_dir`: every
/// `dashboards/*.json` (sorted by file name, so the resource set is
/// deterministic) plus `alerts/observability-alerts.yaml`. The set is the
/// directory's contents — shipping a new dashboard is a content change,
/// not a code change.
pub fn load_observability_artifacts(
    content_dir: &Path,
) -> Result<Vec<ObservabilityArtifact>, IacError> {
    let dashboards_dir = content_dir.join("dashboards");
    let entries = std::fs::read_dir(&dashboards_dir).map_err(|error| {
        IacError::Provider(format!(
            "observability content not found: cannot read {} ({error}); the deployment's \
             observability directory carries dashboards/*.json and \
             alerts/observability-alerts.yaml",
            dashboards_dir.display()
        ))
    })?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            IacError::Provider(format!(
                "failed to enumerate {}: {error}",
                dashboards_dir.display()
            ))
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".json") {
            names.push(name.to_string());
        }
    }
    names.sort();
    let mut artifacts = Vec::new();
    for name in names {
        let path = dashboards_dir.join(&name);
        let content = std::fs::read_to_string(&path).map_err(|error| {
            IacError::Provider(format!("failed to read {}: {error}", path.display()))
        })?;
        artifacts.push(ObservabilityArtifact {
            key: format!("dashboards/{name}"),
            content_type: "application/json",
            content,
        });
    }
    let alerts_path = content_dir.join("alerts/observability-alerts.yaml");
    let content = std::fs::read_to_string(&alerts_path).map_err(|error| {
        IacError::Provider(format!(
            "observability content not found: cannot read {} ({error})",
            alerts_path.display()
        ))
    })?;
    artifacts.push(ObservabilityArtifact {
        key: "alerts/observability-alerts.yaml".to_string(),
        content_type: "application/yaml",
        content,
    });
    Ok(artifacts)
}

pub(crate) fn storage_role(
    role_name: String,
    bucket_name: String,
    rctx: &ResourceContext,
    module: &str,
) -> IamRole {
    let mut inline_policies = HashMap::new();
    inline_policies.insert(
        "s3-storage".to_owned(),
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                "Resource": [
                    format!("arn:aws:s3:::{bucket_name}"),
                    format!("arn:aws:s3:::{bucket_name}/*")
                ]
            }]
        })
        .to_string(),
    );
    IamRole::new(
        role_name,
        IamRoleConfig {
            trust_policy: ecs_tasks_assume_role_policy(),
            inline_policies,
            dependent_inline_policies: Vec::new(),
            managed_policy_arns: Vec::new(),
            module: module.to_owned(),
        },
        rctx,
    )
}

fn ecs_tasks_assume_role_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "ecs-tasks.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string()
}

pub(crate) fn grafana_secret_read_policy(config: &EcsConfig) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": format!(
                "arn:aws:secretsmanager:{}:*:secret:{}/grafana/admin-*",
                config.region, config.project_name
            )
        }]
    })
    .to_string()
}

/// Deployment identity required to render one Alloy sidecar configuration.
///
/// These values are labels and provider coordinates, not secrets. Keeping them
/// explicit prevents definition kinds from silently substituting canonical
/// model defaults for authored project, environment, or cluster values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlloyRenderContext<'a> {
    /// Deployment-scoped project name used in observability labels.
    pub project_name: &'a str,
    /// Operator-authored deployment environment.
    pub environment: &'a str,
    /// Operator-authored ECS cluster name.
    pub cluster_name: &'a str,
}

impl<'a> From<&'a EcsConfig> for AlloyRenderContext<'a> {
    fn from(config: &'a EcsConfig) -> Self {
        Self {
            project_name: &config.project_name,
            environment: &config.environment,
            cluster_name: &config.cluster.name,
        }
    }
}

/// Render the Alloy sidecar configuration for one canonical ECS workload.
///
/// Mimir and Loki are reached through their task-local Service Connect client
/// aliases. They deliberately do not use the independently authored private
/// ALB zone, and the rendered document contains no credential material.
pub fn render_alloy_config(service_name: &str, context: &AlloyRenderContext<'_>) -> String {
    let metrics_port = metrics_port_for(service_name);
    let target_kind = if matches!(
        service_name,
        "tokeira-mimir" | "tokeira-loki" | "tokeira-grafana"
    ) {
        "infrastructure"
    } else {
        "process"
    };
    format!(
        r#"prometheus.scrape "tokeira" {{
  targets         = [{{ __address__ = "localhost:{metrics_port}", service = "{service_name}", target_kind = "{target_kind}", cluster = "{}", deployment = "{}" }}]
  forward_to      = [prometheus.remote_write.mimir.receiver]
  scrape_interval = "15s"
  job_name        = "{service_name}"
}}

prometheus.remote_write "mimir" {{
  endpoint {{
    url = "http://tokeira-mimir:9009/api/v1/push"
  }}
  external_labels = {{
    service     = "{service_name}"
    service_name = "{service_name}"
    target_kind = "{target_kind}"
    cluster     = "{}"
    deployment  = "{}"
    environment = "{}"
    project     = "{}"
  }}
}}

discovery.docker "task" {{
  host = "unix:///var/run/docker.sock"
}}

discovery.relabel "task_logs" {{
  targets = discovery.docker.task.targets
  rule {{
    source_labels = ["__meta_docker_container_label_com_amazonaws_ecs_task_arn"]
    regex         = "TASK_ARN_PLACEHOLDER"
    action        = "keep"
  }}
}}

loki.source.docker "task" {{
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.relabel.task_logs.output
  forward_to = [loki.write.default.receiver]
}}

loki.write "default" {{
  endpoint {{
    url = "http://tokeira-loki:3100/loki/api/v1/push"
  }}
  external_labels = {{
    service_name = "{service_name}"
    environment  = "{}"
    project      = "{}"
    task_id      = "TASK_ID_PLACEHOLDER"
  }}
}}
"#,
        context.cluster_name,
        context.environment,
        context.cluster_name,
        context.environment,
        context.environment,
        context.project_name,
        context.environment,
        context.project_name
    )
}

fn metrics_port_for(service_name: &str) -> u16 {
    match service_name {
        "tokeira-mimir" => 9009,
        "tokeira-loki" => 3100,
        "tokeira-grafana" => 3000,
        _ => 9090,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Content-coupled coverage (loader completeness, dashboard/alert style
    // contracts, resource enumeration over real artifacts) lives with the
    // content itself in the `tokeira-ecs-deployment` package — this crate
    // never ships the observability tree.

    // A deployment without staged content refuses with the expected layout
    // named — the operator learns what belongs where, not just "not found".
    #[test]
    fn missing_content_names_the_expected_layout() {
        let missing = std::env::temp_dir().join("ecs-observability-missing-content-test");
        let error = load_observability_artifacts(&missing)
            .unwrap_err()
            .to_string();
        assert!(error.contains("dashboards/*.json"), "{error}");
        assert!(error.contains("observability-alerts.yaml"), "{error}");
    }

    #[test]
    fn grafana_task_role_is_scoped_to_admin_secret() {
        let policy: serde_json::Value =
            serde_json::from_str(&grafana_secret_read_policy(&EcsConfig::default()))
                .expect("grafana secret policy");

        assert_eq!(
            policy["Statement"][0]["Action"].as_str(),
            Some("secretsmanager:GetSecretValue")
        );
        assert_eq!(
            policy["Statement"][0]["Resource"].as_str(),
            Some("arn:aws:secretsmanager:eu-west-2:*:secret:tokeira/grafana/admin-*")
        );
    }

    #[test]
    fn alloy_config_contains_task_placeholders_and_localhost_scrape() {
        let model = EcsConfig::default();
        let config = render_alloy_config("tokeira-runtime", &AlloyRenderContext::from(&model));

        assert!(config.contains("localhost:9090"));
        assert!(config.contains("service = \"tokeira-runtime\""));
        assert!(config.contains("target_kind = \"process\""));
        assert!(config.contains("cluster = \"tokeira\""));
        assert!(config.contains("deployment = \"dev\""));
        assert!(config.contains("TASK_ARN_PLACEHOLDER"));
        assert!(config.contains("TASK_ID_PLACEHOLDER"));
        assert!(config.contains("loki.source.docker"));
        assert!(config.contains("http://tokeira-mimir:9009/api/v1/push"));
        assert!(config.contains("http://tokeira-loki:3100/loki/api/v1/push"));
    }

    #[test]
    fn infrastructure_alloy_config_uses_infrastructure_target_kind() {
        let model = EcsConfig::default();
        let config = render_alloy_config("tokeira-mimir", &AlloyRenderContext::from(&model));

        assert!(config.contains("localhost:9009"));
        assert!(config.contains("service = \"tokeira-mimir\""));
        assert!(config.contains("target_kind = \"infrastructure\""));
    }
}
