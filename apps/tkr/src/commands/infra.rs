//! `tkr infra` — declare, diff, and reconcile cloud infrastructure.
//!
//! Wraps `tokeira_orchestrator::InfraEngine` with a `plan → confirm →
//! apply` workflow that:
//!
//! 1. Builds the desired composition for the selected platform.
//! 2. Refreshes state against the provider (so plan reflects reality).
//! 3. Renders a human-readable diff and/or spinners via [`ActionTuiHandle`].
//! 4. On `apply`, re-runs the engine, then writes any discovered values
//!    (DSQL endpoint, OpenSearch URL, etc.) back into the deployment's
//!    `tokeirad.toml` via [`write_tokeirad_writeback`].
//!
//! The ECS path additionally runs a pre-flight mirror validation
//! ([`validate_ecs_mirrors`]) so `infra apply` fails loud if the operator
//! forgot to populate the Mimir/Loki/Grafana/Alloy/aws-cli/busybox mirrors
//! via `tkr image mirror` first.

use std::{fs, path::Path};

use anyhow::Result;
use tokeira_aws::{DefaultEcrClient, EcrClient};
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_deploy_engine::ImageContext;
use tokeira_ecs_deployment::{EcsConfig, EcsDeployment};
use tokeira_iac::{Change, ChangeKind, ModuleSelection};
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{Deployment, InfraEngine};

use crate::{
    cli::InfraAction,
    deployment_dir::{
        DeploymentContext, DeploymentResolver, PlatformDeploymentConfig, TOKEIRAD_TOML,
    },
    metadata::DeploymentStatus,
    tui::{ActionTuiHandle, OutputFormat},
};

pub(crate) async fn run(
    action: InfraAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
    format: OutputFormat,
) -> Result<()> {
    match &ctx.platform_config {
        PlatformDeploymentConfig::Local(config) => {
            run_with_engine(action, deployments, &ctx, LocalDeployment, config, format).await
        }
        PlatformDeploymentConfig::Compose(config) => {
            run_with_engine(action, deployments, &ctx, ComposeDeployment, config, format).await
        }
        PlatformDeploymentConfig::Ecs(config) => {
            if matches!(&action, InfraAction::Apply { .. }) {
                validate_ecs_mirrors(config).await?;
            }
            run_with_engine(
                action,
                deployments,
                &ctx,
                EcsDeployment::new(),
                config,
                format,
            )
            .await
        }
    }
}

/// Pre-flight check for ECS `infra apply`.
///
/// Most observability images (Mimir, Loki, Grafana, Alloy, aws-cli,
/// busybox) are mirrored into a project-owned ECR repo rather than
/// pulled directly from Docker Hub — ECR avoids hitting anonymous rate
/// limits and keeps all image pulls inside the VPC. This function
/// verifies the operator has populated every mirror before we provision
/// infrastructure that would otherwise reference missing images.
///
/// Failing here saves the operator minutes of rollback work.
async fn validate_ecs_mirrors(config: &EcsConfig) -> Result<()> {
    let deployment = EcsDeployment::new();
    let mut image_ctx = ImageContext::default();
    deployment
        .register_image_extensions(config, &mut image_ctx)
        .await?;
    let images = tokeira_ecs_deployment::images::all(&image_ctx)?;
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .load()
        .await;
    let ecr = DefaultEcrClient::from_aws_config(&aws_config);
    let auth = ecr.get_authorization_token().await?;
    tokeira_ecs_deployment::gates::validate_mirrors(
        config,
        &auth.registry_host,
        &images,
        &image_ctx,
    )?;
    Ok(())
}

async fn run_with_engine<D>(
    action: InfraAction,
    deployments: &DeploymentResolver,
    ctx: &DeploymentContext,
    deployment: D,
    config: &D::Config,
    format: OutputFormat,
) -> Result<()>
where
    D: tokeira_orchestrator::Deployment,
{
    match action {
        InfraAction::Plan { module } => {
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let selection = module_selection(module);
            let composition = engine.compose(selection.clone())?;
            let tui = ActionTuiHandle::new(format);
            tui.install(engine.provision_context_mut());
            let result = engine.plan(&composition, selection).await;
            if let Ok(outcome) = &result {
                finish_progress(&tui, &outcome.changes);
                print_plan(&outcome.changes);
            } else {
                tui.print_summary();
            }
            result?;
        }
        InfraAction::Apply { yes, module } => {
            super::require_confirmation(yes, "infra apply")?;
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let selection = module_selection(module);
            let composition = engine.compose(selection.clone())?;
            let tui = ActionTuiHandle::new(format);
            tui.install(engine.provision_context_mut());
            let result = engine.apply(&composition, selection).await;
            if let Ok(changes) = &result {
                finish_progress(&tui, changes);
                print_plan(changes);
            } else {
                tui.print_summary();
            }
            result?;
            write_tokeirad_writeback(&ctx.path, engine.collect_writeback())?;
        }
        InfraAction::Destroy { yes, module } => {
            super::require_confirmation(yes, "infra destroy")?;
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let selection = module_selection(module);
            let composition = engine.compose(selection.clone())?;
            let tui = ActionTuiHandle::new(format);
            tui.install(engine.provision_context_mut());
            let result = engine.destroy(&composition, selection).await;
            if let Ok(changes) = &result {
                finish_progress(&tui, changes);
                print_plan(changes);
            } else {
                tui.print_summary();
            }
            result?;
            deployments.update_status(&ctx.name, DeploymentStatus::Created)?;
        }
        InfraAction::Status => {
            let state_dir = ctx.path.join("state");
            println!(
                "infrastructure state is stored under {}",
                state_dir.join("infra").display()
            );
        }
    }
    Ok(())
}

fn finish_progress(tui: &ActionTuiHandle, changes: &[Change]) {
    let skipped = changes
        .iter()
        .filter(|change| change.kind == ChangeKind::NoChange)
        .count();
    tui.record_skipped(skipped);
    tui.print_summary();
}

fn module_selection(module: Option<String>) -> ModuleSelection {
    module
        .map(|name| ModuleSelection::Only(vec![name]))
        .unwrap_or(ModuleSelection::All)
}

pub(crate) fn read_tokeirad_config(path: &Path) -> Result<tokeira_config::TokeiraConfig> {
    Ok(toml::from_str(&fs::read_to_string(path)?)?)
}

/// Persist values discovered during `infra apply` back into the
/// deployment's server config.
///
/// The engine collects writeback requests during apply (DSQL endpoint,
/// OpenSearch URL, ECR registry host, etc.). Rather than making operators
/// copy-paste these by hand, we patch them into `tokeirad.toml` using the
/// same dotted-key writer the `image push/mirror` commands use. A later
/// `tkr deploy apply` picks them up transparently.
pub(crate) fn write_tokeirad_writeback(
    deployment_path: &Path,
    values: Vec<(String, String)>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let path = deployment_path.join(TOKEIRAD_TOML);
    let borrowed = values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    tokeira_iac::write_config_values(&path, &borrowed)?;
    Ok(())
}

/// Render an IaC changelist using the `+/-/~/=` convention Terraform
/// operators already recognise. Each diffed field is printed with its
/// before/after value so the operator can eyeball the change.
fn print_plan(changes: &[Change]) {
    for change in changes {
        let marker = match change.kind {
            ChangeKind::Create => "+",
            ChangeKind::Update => "~",
            ChangeKind::Replace => "±",
            ChangeKind::Delete => "-",
            ChangeKind::NoChange => "=",
        };
        println!(
            "{marker} [{}] {}::{}",
            change.resource_type, change.module, change.resource
        );
        for detail in &change.details {
            println!(
                "    {}: {:?} -> {:?}",
                detail.field, detail.before, detail.after
            );
        }
    }
}
