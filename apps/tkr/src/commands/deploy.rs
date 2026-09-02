//! `tkr deploy` — service lifecycle: plan, apply, destroy, report status.
//!
//! `deploy apply` behaves differently per platform because the underlying
//! runtime engine does:
//!
//! - **Local**: spawn `tokeirad` as a foreground host process via
//!   [`crate::process::spawn_tokeirad`]. Status toggles to `Running` at
//!   spawn and back to `Stopped` when the process exits (normally or via
//!   ctrl-c).
//! - **Legacy ECS**: apply remains unavailable on directories created before
//!   the definition migration; new ECS deployments are forwarded to their
//!   married provisioner and never enter this module.
//!
//! Definition-bound platforms, including Compose, never enter this module;
//! `tkr` forwards their deploy verbs to the married provisioner.
//!
//! `deploy status` prefers the live [`crate::process::local_process_status`]
//! for local deployments over the metadata file so a crashed server is
//! surfaced as `Stopped` even if the metadata still says `Running`.

use anyhow::Result;
use tokeira_ecs_deployment::{EcsDeployment, EcsPlatform};
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::DeployEngine;

use crate::{
    cli::DeployAction,
    deployment_dir::{DeploymentContext, DeploymentResolver, PlatformDeploymentConfig},
    metadata::DeploymentStatus,
    process,
};

pub(crate) async fn run(
    action: DeployAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
) -> Result<()> {
    match action {
        DeployAction::Plan { explanation } => {
            super::refuse_explanation(explanation.as_deref())?;
            match &ctx.platform_config {
                PlatformDeploymentConfig::Local(config) => {
                    let mut engine = DeployEngine::new(LocalDeployment, config, &ctx.path).await?;
                    print_plan(&engine.plan().await?);
                }
                PlatformDeploymentConfig::Ecs(config) => {
                    let mut engine =
                        DeployEngine::new(EcsDeployment::new(&ctx.path), config, &ctx.path).await?;
                    print_plan(&engine.plan().await?);
                }
            }
        }
        DeployAction::Apply {
            yes,
            // Consumed only by the forwarded path (the deployment's own
            // provisioner); no in-process platform reads it since the legacy
            // compose driver retired.
            force: _,
            explanation,
        } => {
            super::refuse_explanation(explanation.as_deref())?;
            super::require_confirmation(yes, "deploy apply")?;
            match &ctx.platform_config {
                PlatformDeploymentConfig::Local(_) => {
                    deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
                    let result = process::spawn_tokeirad(&ctx.path).await;
                    deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
                    result?;
                }
                PlatformDeploymentConfig::Ecs(_) => {
                    anyhow::bail!("ECS deploy apply is not implemented yet");
                }
            }
        }
        DeployAction::Destroy { yes } => {
            super::require_confirmation(yes, "deploy destroy")?;
            match &ctx.platform_config {
                PlatformDeploymentConfig::Local(_) => {
                    process::stop_tokeirad(&ctx.path).await?;
                    deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
                    println!("destroyed local services for {}", ctx.name);
                }
                PlatformDeploymentConfig::Ecs(config) => {
                    let mut engine =
                        DeployEngine::new(EcsDeployment::new(&ctx.path), config, &ctx.path).await?;
                    let changes = engine.destroy(&EcsPlatform::new()).await?;
                    print_plan(&changes);
                    deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
                }
            }
        }
        DeployAction::Status => {
            let status = if ctx.metadata.platform.as_str() == "local" {
                process::local_process_status(&ctx.path)
            } else {
                ctx.metadata.status.clone()
            };
            println!("deployment {} status: {:?}", ctx.name, status);
        }
    }
    Ok(())
}

fn print_plan(changes: &[tokeira_deploy_engine::ServiceChange]) {
    for change in changes {
        println!("{:?} {} ({})", change.kind, change.service, change.module);
    }
}
