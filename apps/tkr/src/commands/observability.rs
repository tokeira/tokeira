//! Observability smoke checks for generated deployment telemetry paths.
//!
//! Definition-backed deployments are forwarded to their platform declaration;
//! only that platform knows which observability stack and checks apply. This
//! module retains the local in-process deployment check and the explicit
//! `--grafana --path <dashboard.json>` validator.

use std::path::Path;

use anyhow::{Result, bail};
use tokeira_observability::validation::DashboardValidator;

use crate::deployment_dir::{DeploymentContext, PlatformDeploymentConfig};

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

pub(crate) fn run_grafana(path: &Path) -> Result<()> {
    let report = check_grafana_dashboard(path)?;
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

    let PlatformDeploymentConfig::Local(_) = &ctx.platform_config;
    let checks = vec![CheckOutcome {
        name: "local-observability",
        status: CheckStatus::Warn,
        detail: "local deployments do not provision Mimir, Loki, Grafana, or Alloy".into(),
    }];

    Ok(CheckReport { checks })
}

pub(crate) fn check_grafana_dashboard(path: &Path) -> Result<CheckReport> {
    DashboardValidator::validate_file(path)?;
    Ok(CheckReport {
        checks: vec![pass(
            "grafana-dashboard",
            format!(
                "{} satisfies the Grafana dashboard style contract",
                path.display()
            ),
        )],
    })
}

fn pass(name: &'static str, detail: impl Into<String>) -> CheckOutcome {
    CheckOutcome {
        name,
        status: CheckStatus::Pass,
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
                state: Default::default(),
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
    fn zero_timeout_is_rejected() {
        let error = check_generated_observability(
            &context(PlatformDeploymentConfig::Local(Default::default())),
            0,
        )
        .unwrap_err();

        assert!(error.to_string().contains("timeout must be positive"));
    }

    #[test]
    fn focused_grafana_path_validates_one_dashboard() {
        let rendered = tempfile::tempdir().unwrap();
        let dashboard = rendered.path().join("health.json");
        std::fs::write(
            &dashboard,
            r#"{"templating":{"list":[{"name":"datasource","type":"datasource"}]},"panels":[]}"#,
        )
        .unwrap();

        let report = check_grafana_dashboard(&dashboard).unwrap();

        assert_eq!(report.checks.len(), 1);
        assert_eq!(report.checks[0].name, "grafana-dashboard");
        assert_eq!(report.checks[0].status, CheckStatus::Pass);
    }

    #[test]
    fn focused_grafana_path_rejects_a_style_violation() {
        let rendered = tempfile::tempdir().unwrap();
        let dashboard = rendered.path().join("health.json");
        std::fs::write(&dashboard, r#"{"panels":[]}"#).unwrap();

        let error = check_grafana_dashboard(&dashboard).unwrap_err();

        assert!(error.to_string().contains("missing $datasource variable"));
    }
}
