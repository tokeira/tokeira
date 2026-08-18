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
        PlatformDeploymentConfig::Ecs(config) => ecs_checks(config, &ctx.path)?,
        PlatformDeploymentConfig::Local(_) => vec![CheckOutcome {
            name: "local-observability",
            status: CheckStatus::Warn,
            detail: "local deployments do not provision Mimir, Loki, Grafana, or Alloy".into(),
        }],
    };

    Ok(CheckReport { checks })
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
    use std::path::PathBuf;

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
}
