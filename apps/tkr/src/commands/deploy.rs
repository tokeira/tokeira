//! `tkr deploy` — service lifecycle: plan, apply, report status.
//!
//! `deploy apply` behaves differently per platform because the underlying
//! runtime engine does:
//!
//! - **Local**: spawn `tokeirad` as a foreground host process via
//!   [`crate::process::spawn_tokeirad`]. Status toggles to `Running` at
//!   spawn and back to `Stopped` when the process exits (normally or via
//!   ctrl-c).
//! - **Compose**: run a Compose-specific pre-flight validation, then have
//!   the [`DeployEngine`] reconcile the compose stack on disk. Status
//!   flips to `Running` once the stack is up.
//! - **ECS**: currently a `todo` surface — the runtime engine for ECS
//!   exists but this CLI path hasn't been wired yet (tracked separately).
//!
//! `deploy status` prefers the live [`crate::process::local_process_status`]
//! for local deployments over the metadata file so a crashed server is
//! surfaced as `Stopped` even if the metadata still says `Running`.

use anyhow::Result;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_deploy_engine::Platform;
use tokeira_ecs_deployment::EcsDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{DeployEngine, PlatformKind};

use crate::{
    cli::DeployAction,
    deployment_dir::{DeploymentContext, DeploymentResolver, PlatformDeploymentConfig},
    metadata::DeploymentStatus,
    process,
};

pub async fn run(
    action: DeployAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
) -> Result<()> {
    match action {
        DeployAction::Plan => match &ctx.platform_config {
            PlatformDeploymentConfig::Local(config) => {
                let mut engine = DeployEngine::new(LocalDeployment, config, &ctx.path).await?;
                print_plan(&engine.plan().await?);
            }
            PlatformDeploymentConfig::Compose(config) => {
                let mut engine = DeployEngine::new(ComposeDeployment, config, &ctx.path).await?;
                print_plan(&engine.plan().await?);
            }
            PlatformDeploymentConfig::Ecs(config) => {
                let mut engine = DeployEngine::new(EcsDeployment::new(), config, &ctx.path).await?;
                print_plan(&engine.plan().await?);
            }
        },
        DeployAction::Apply { yes, force } => {
            super::require_confirmation(yes, "deploy apply")?;
            match &ctx.platform_config {
                PlatformDeploymentConfig::Local(_) => {
                    deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
                    let result = process::spawn_tokeirad(&ctx.path).await;
                    deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
                    result?;
                }
                PlatformDeploymentConfig::Compose(config) => {
                    let compose_file = ctx.path.join("docker-compose.yml");
                    let deployment = ComposeDeployment;
                    let platform =
                        ComposeDeployment::compose_platform(&compose_file, &config.project_name)?;
                    deployment
                        .validate_for_deploy_apply(config, &platform)
                        .await?;
                    let mut engine = DeployEngine::new(deployment, config, &ctx.path).await?;
                    if force {
                        engine.clear_service_state().await?;
                    }
                    print_plan(&engine.apply(&platform as &dyn Platform).await?);
                    deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
                }
                PlatformDeploymentConfig::Ecs(_) => {
                    anyhow::bail!("ECS deploy apply is not implemented yet");
                }
            }
        }
        DeployAction::Status => {
            let status = if ctx.metadata.platform == PlatformKind::Local {
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
