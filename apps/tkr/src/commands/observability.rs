//! Observability smoke checks for generated deployment telemetry paths.
//!
//! The command deliberately validates generated configuration before trying to
//! query live backends. That gives operators fast feedback for broken scrape,
//! dashboard, and alert provisioning even in private deployments where Mimir or
//! Loki may only be reachable through port forwarding. A selected deployment
//! uses its platform-owned generation path; `--path` validates an already
//! rendered config root without deployment admission.

use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
use tokeira_ecs_deployment::{
    modules::{ObservabilityModule, observability::all_alloy_services},
    services::EcsWorkload,
};
use tokeira_iac::{Module, ModuleContext};
use tokeira_observability::validation::{
    AlertRuleValidator, AlloyConfigValidator, DashboardValidator,
};

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

const ALLOY_CONFIG: &str = "alloy.alloy";
const DASHBOARD_DIR: &str = "grafana/dashboards";
const ALERT_RULE_DIR: &str = "mimir/rules";
const EXPECTED_SCRAPE_JOBS: &[&str] = &["tokeirad", "alloy", "mimir", "loki", "grafana"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckReport {
    pub checks: Vec<CheckOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckOutcome {
    pub name: &'static str,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckStatus {
    Pass,
    Warn,
}

pub(crate) fn run_selected(timeout_seconds: u64, ctx: DeploymentContext) -> Result<()> {
    let report = check_generated_observability(&ctx, timeout_seconds)?;
    emit_report(&report);
    Ok(())
}

pub(crate) fn run_path(path: &Path, timeout_seconds: u64) -> Result<()> {
    let report = check_rendered_observability(path, timeout_seconds)?;
    emit_report(&report);
    Ok(())
}

fn emit_report(report: &CheckReport) {
    for outcome in &report.checks {
        let status = match outcome.status {
            CheckStatus::Pass => "PASS",
            CheckStatus::Warn => "WARN",
        };
        println!("{status} {} - {}", outcome.name, outcome.detail);
    }
}

pub(crate) fn check_generated_observability(
    ctx: &DeploymentContext,
    timeout_seconds: u64,
) -> Result<CheckReport> {
    if timeout_seconds == 0 {
        bail!("observability check timeout must be positive");
    }

    let checks = match &ctx.platform_config {
        PlatformDeploymentConfig::Ecs(config) => ecs_checks(config, &ctx.path)?,
        PlatformDeploymentConfig::Local(_) => vec![CheckOutcome {
            name: "local-observability",
            status: CheckStatus::Warn,
            detail: "local deployments do not provision Mimir, Loki, Grafana, or Alloy".into(),
        }],
    };

    Ok(CheckReport { checks })
}

pub(crate) fn check_rendered_observability(
    path: &Path,
    timeout_seconds: u64,
) -> Result<CheckReport> {
    if timeout_seconds == 0 {
        bail!("observability check timeout must be positive");
    }
    if !path.is_dir() {
        bail!(
            "rendered observability path is not a directory: {}",
            path.display()
        );
    }

    let alloy_path = path.join(ALLOY_CONFIG);
    let alloy = fs::read_to_string(&alloy_path).with_context(|| {
        format!(
            "failed to read rendered Alloy config {}",
            alloy_path.display()
        )
    })?;
    AlloyConfigValidator::validate_scrape_jobs(&alloy_path, &alloy, EXPECTED_SCRAPE_JOBS)?;

    let dashboards = path.join(DASHBOARD_DIR);
    DashboardValidator::validate_directory(&dashboards)?;
    let dashboard_count = count_files(&dashboards, &["json"])?;
    if dashboard_count == 0 {
        bail!(
            "rendered observability path contains no Grafana dashboards under {}",
            dashboards.display()
        );
    }

    let alerts = path.join(ALERT_RULE_DIR);
    AlertRuleValidator::validate_directory(&alerts, path)?;
    let alert_count = count_files(&alerts, &["yaml", "yml"])?;
    if alert_count == 0 {
        bail!(
            "rendered observability path contains no Mimir alert rules under {}",
            alerts.display()
        );
    }

    Ok(CheckReport {
        checks: vec![
            pass(
                "rendered-scrapes",
                format!(
                    "{} expected Alloy scrape jobs present",
                    EXPECTED_SCRAPE_JOBS.len()
                ),
            ),
            pass(
                "rendered-dashboards",
                format!("{dashboard_count} Grafana dashboards satisfy the style contract"),
            ),
            pass(
                "rendered-alerts",
                format!("{alert_count} Mimir alert files satisfy the style contract"),
            ),
            warn(
                "live-backend-query",
                "live Mimir/Loki/Grafana queries require a reachable deployment endpoint",
            ),
        ],
    })
}

fn count_files(path: &Path, extensions: &[&str]) -> Result<usize> {
    let entries = fs::read_dir(path)
        .with_context(|| format!("failed to read rendered directory {}", path.display()))?;
    let mut count = 0;
    for entry in entries {
        let entry = entry
            .with_context(|| format!("failed to read rendered entry under {}", path.display()))?;
        if entry
            .path()
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extensions.contains(&extension))
        {
            count += 1;
        }
    }
    Ok(count)
}

fn ecs_checks(
    config: &tokeira_ecs_deployment::EcsConfig,
    deployment_dir: &std::path::Path,
) -> Result<Vec<CheckOutcome>> {
    let services = EcsWorkload::build_all(config);
    let observability = EcsWorkload::build_observability(config);
    if services.is_empty() || observability.is_empty() {
        bail!("ECS workload generation returned no services");
    }
    for service_name in all_alloy_services() {
        let alloy_config = tokeira_ecs_deployment::modules::observability::render_alloy_config(
            service_name,
            config,
        );
        require_contains(&alloy_config, "localhost:", "ECS Alloy local scrape")?;
        require_contains(
            &alloy_config,
            "loki.source.docker",
            "ECS Alloy log collection",
        )?;
        require_contains(
            &alloy_config,
            "TASK_ARN_PLACEHOLDER",
            "ECS task log scoping",
        )?;
    }

    let module = ObservabilityModule::new(config.clone(), deployment_dir.join("observability"));
    let state = tokeira_iac::InfraState::default();
    let extensions = std::collections::HashMap::new();
    let context = ModuleContext::new(&state, &extensions);
    let resources = module
        .resources(&context)
        .context("failed to enumerate ECS observability resources")?;
    let dashboard_artifacts = resources
        .iter()
        .filter(|resource| resource.resource_id().0.contains("dashboards/"))
        .count();
    let alert_artifacts = resources
        .iter()
        .filter(|resource| resource.resource_id().0.contains("alerts/"))
        .count();
    if dashboard_artifacts == 0 || alert_artifacts == 0 {
        bail!("ECS observability artifact provisioning is incomplete");
    }

    Ok(vec![
        pass(
            "ecs-scrapes",
            format!(
                "{} Alloy service configs render",
                all_alloy_services().len()
            ),
        ),
        pass(
            "ecs-log-collection",
            "Alloy configs use task-scoped Docker log collection",
        ),
        pass(
            "ecs-artifacts",
            format!(
                "{dashboard_artifacts} dashboards and {alert_artifacts} alert bundles included"
            ),
        ),
        warn(
            "live-backend-query",
            "live Mimir/Loki/Grafana queries require ECS port forwarding or private network access",
        ),
    ])
}

fn require_contains(contents: &str, needle: &str, context: &str) -> Result<()> {
    if contents.contains(needle) {
        Ok(())
    } else {
        bail!("{context} missing expected fragment: {needle}");
    }
}

fn pass(name: &'static str, detail: impl Into<String>) -> CheckOutcome {
    CheckOutcome {
        name,
        status: CheckStatus::Pass,
        detail: detail.into(),
    }
}

fn warn(name: &'static str, detail: impl Into<String>) -> CheckOutcome {
    CheckOutcome {
        name,
        status: CheckStatus::Warn,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::{
        deployment_dir::{DeploymentContext, PlatformDeploymentConfig},
        metadata::{DeploymentMetadata, DeploymentStatus},
    };
    use tokeira_orchestrator::StorageKind;
    use uuid::Uuid;

    fn context(platform_config: PlatformDeploymentConfig) -> DeploymentContext {
        DeploymentContext {
            name: "test".into(),
            path: PathBuf::new(),
            metadata: DeploymentMetadata {
                name: "test".into(),
                id: Uuid::nil(),
                platform: tokeira_orchestrator::PlatformId::new("local").expect("platform"),
                definition: None,
                deployment_repository: None,
                storage: StorageKind::InMemory,
                status: DeploymentStatus::Created,
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
            },
            platform_config,
        }
    }

    fn rendered_fixture(root: &Path) {
        std::fs::create_dir_all(root.join(DASHBOARD_DIR)).unwrap();
        std::fs::create_dir_all(root.join(ALERT_RULE_DIR)).unwrap();
        let scrapes = EXPECTED_SCRAPE_JOBS
            .iter()
            .map(|job| format!("prometheus.scrape \"{job}\" {{}}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(root.join(ALLOY_CONFIG), scrapes).unwrap();
        std::fs::write(
            root.join(DASHBOARD_DIR).join("health.json"),
            r#"{"templating":{"list":[{"name":"datasource","type":"datasource"}]},"panels":[]}"#,
        )
        .unwrap();
        std::fs::write(
            root.join(ALERT_RULE_DIR).join("health.yaml"),
            r#"groups:
  - name: health
    rules:
      - alert: HealthFailure
        expr: vector(1)
        labels:
          severity: page
          service: tokeirad
        annotations:
          summary: Health check failed
"#,
        )
        .unwrap();
    }

    #[test]
    fn ecs_check_validates_generated_artifacts() {
        // The artifact half of the check reads the deployment's staged
        // observability content — a deployment carries dashboards/*.json
        // plus alerts/observability-alerts.yaml.
        let deployment = tempfile::tempdir().unwrap();
        let content = deployment.path().join("observability");
        std::fs::create_dir_all(content.join("dashboards")).unwrap();
        std::fs::create_dir_all(content.join("alerts")).unwrap();
        std::fs::write(content.join("dashboards/engine-health.json"), "{}").unwrap();
        std::fs::write(
            content.join("alerts/observability-alerts.yaml"),
            "groups: []",
        )
        .unwrap();
        let mut ctx = context(PlatformDeploymentConfig::Ecs(Box::default()));
        ctx.path = deployment.path().to_path_buf();
        let report = check_generated_observability(&ctx, 30).unwrap();

        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "ecs-scrapes")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "ecs-artifacts")
        );
    }

    #[test]
    fn zero_timeout_is_rejected() {
        let error = check_generated_observability(
            &context(PlatformDeploymentConfig::Local(Default::default())),
            0,
        )
        .unwrap_err();

        assert!(error.to_string().contains("timeout must be positive"));
    }

    #[test]
    fn standalone_rendered_path_passes_without_a_deployment() {
        let rendered = tempfile::tempdir().unwrap();
        rendered_fixture(rendered.path());

        let report = check_rendered_observability(rendered.path(), 30).unwrap();

        assert_eq!(report.checks.len(), 4);
        assert_eq!(report.checks[0].name, "rendered-scrapes");
        assert_eq!(report.checks[3].status, CheckStatus::Warn);
    }

    #[test]
    fn standalone_rendered_path_rejects_dashboard_style_violations() {
        let rendered = tempfile::tempdir().unwrap();
        rendered_fixture(rendered.path());
        std::fs::write(
            rendered.path().join(DASHBOARD_DIR).join("health.json"),
            r#"{"panels":[]}"#,
        )
        .unwrap();

        let error = check_rendered_observability(rendered.path(), 30).unwrap_err();

        assert!(error.to_string().contains("missing $datasource variable"));
    }
}
