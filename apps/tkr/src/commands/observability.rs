//! Observability smoke checks for generated deployment telemetry paths.
//!
//! The command deliberately validates generated configuration before trying to
//! query live backends. That gives operators fast feedback for broken scrape,
//! dashboard, and alert provisioning even in private deployments where Mimir or
//! Loki may only be reachable through port forwarding.

use anyhow::{Context, Result, bail};
use tokeira_ecs_deployment::{
    modules::{ObservabilityModule, observability::all_alloy_services},
    services::EcsWorkload,
};
use tokeira_iac::{Module, ModuleContext};

use crate::{
    cli::ObservabilityAction,
    deployment_dir::{DeploymentContext, PlatformDeploymentConfig},
};

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

pub(crate) fn run(action: ObservabilityAction, ctx: DeploymentContext) -> Result<()> {
    let ObservabilityAction::Check { timeout_seconds } = action;
    let report = check_generated_observability(&ctx, timeout_seconds)?;

    for outcome in &report.checks {
        let status = match outcome.status {
            CheckStatus::Pass => "PASS",
            CheckStatus::Warn => "WARN",
        };
        println!("{status} {} - {}", outcome.name, outcome.detail);
    }
    Ok(())
}

pub(crate) fn check_generated_observability(
    ctx: &DeploymentContext,
    timeout_seconds: u64,
) -> Result<CheckReport> {
    if timeout_seconds == 0 {
        bail!("observability check timeout must be positive");
    }

    let checks = match &ctx.platform_config {
        PlatformDeploymentConfig::Compose(config) => compose_checks(config, &ctx.path, &ctx.name)?,
        PlatformDeploymentConfig::Ecs(config) => ecs_checks(config)?,
        PlatformDeploymentConfig::Local(_) => vec![CheckOutcome {
            name: "local-observability",
            status: CheckStatus::Warn,
            detail: "local deployments do not provision Mimir, Loki, Grafana, or Alloy".into(),
        }],
    };

    Ok(CheckReport { checks })
}

fn compose_checks(
    config: &tokeira_compose_deployment::ComposeConfig,
    deployment_dir: &std::path::Path,
    deployment: &str,
) -> Result<Vec<CheckOutcome>> {
    let generator = tokeira_compose_deployment::observability::ConfigGenerator::new(deployment_dir);
    let files = generator
        .render_all(
            &tokeira_compose_deployment::observability::ObservabilityParams::from_config(
                config, deployment,
            ),
        )
        .context("failed to render compose observability config")?;
    let alloy = rendered_file(&files, "config/alloy.alloy")?;
    let alerts = rendered_file(&files, "config/mimir/rules/observability-alerts.yaml")?;
    let dashboard_count = files
        .iter()
        .filter(|file| {
            file.relative_path
                .to_string_lossy()
                .starts_with("config/grafana/dashboards/")
        })
        .count();

    require_contains(
        alloy,
        "prometheus.scrape \"tokeirad\"",
        "compose Alloy tokeirad scrape",
    )?;
    require_contains(
        alloy,
        "prometheus.scrape \"mimir\"",
        "compose Alloy Mimir scrape",
    )?;
    require_contains(
        alerts,
        "DsqlReservoirExhaustion",
        "compose Mimir alert rules",
    )?;
    if dashboard_count == 0 {
        bail!("compose Grafana dashboard provisioning rendered no dashboards");
    }

    Ok(vec![
        pass(
            "compose-scrapes",
            "Alloy config contains process and infrastructure scrape jobs",
        ),
        pass(
            "compose-dashboards",
            format!("{dashboard_count} Grafana dashboards rendered"),
        ),
        pass("compose-alerts", "Mimir alert rules rendered"),
        warn(
            "live-backend-query",
            "live Mimir/Loki/Grafana queries require a reachable deployment endpoint",
        ),
    ])
}

fn ecs_checks(config: &tokeira_ecs_deployment::EcsConfig) -> Result<Vec<CheckOutcome>> {
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

    let module = ObservabilityModule::new(config.clone());
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

fn rendered_file<'a>(
    files: &'a [tokeira_compose_deployment::observability::RenderedConfigFile],
    path: &str,
) -> Result<&'a str> {
    files
        .iter()
        .find(|file| file.relative_path == *path)
        .map(|file| file.contents.as_str())
        .with_context(|| format!("rendered observability file missing: {path}"))
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
    use std::path::PathBuf;

    use super::*;
    use crate::{
        deployment_dir::{DeploymentContext, PlatformDeploymentConfig},
        metadata::{DeploymentMetadata, DeploymentStatus},
    };
    use tokeira_orchestrator::{PlatformKind, StorageKind};
    use uuid::Uuid;

    fn context(platform_config: PlatformDeploymentConfig) -> DeploymentContext {
        DeploymentContext {
            name: "test".into(),
            path: PathBuf::new(),
            metadata: DeploymentMetadata {
                name: "test".into(),
                id: Uuid::nil(),
                platform: PlatformKind::Local,
                launch_class: Some(tokeira_orchestrator::PlatformLaunchClass::LegacyInProcess),
                definition: None,
                storage: StorageKind::InMemory,
                status: DeploymentStatus::Created,
                created_at: "2026-01-01T00:00:00Z".into(),
                updated_at: "2026-01-01T00:00:00Z".into(),
            },
            platform_config,
        }
    }

    #[test]
    fn compose_check_validates_generated_artifacts() {
        let report = check_generated_observability(
            &context(PlatformDeploymentConfig::Compose(Box::default())),
            30,
        )
        .unwrap();

        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "compose-scrapes")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "compose-dashboards")
        );
    }

    #[test]
    fn ecs_check_validates_generated_artifacts() {
        let report = check_generated_observability(
            &context(PlatformDeploymentConfig::Ecs(Box::default())),
            30,
        )
        .unwrap();

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
}
