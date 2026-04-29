use anyhow::Result;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_deploy_engine::Platform;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{DeployEngine, PlatformKind};

use crate::cli::DeployAction;
use crate::deployment_dir::{
    DeploymentContext, DeploymentResolver, PlatformDeploymentConfig,
};
use crate::metadata::DeploymentStatus;
use crate::process;

pub async fn run(
    action: DeployAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
) -> Result<()> {
    match action {
        DeployAction::Plan => match &ctx.platform_config {
            PlatformDeploymentConfig::Local(config) => {
                let mut engine =
                    DeployEngine::new(LocalDeployment, config, &ctx.path).await?;
                print_plan(&engine.plan().await?);
            }
            PlatformDeploymentConfig::Compose(config) => {
                let mut engine =
                    DeployEngine::new(ComposeDeployment, config, &ctx.path).await?;
                print_plan(&engine.plan().await?);
            }
        },
        DeployAction::Apply { yes } => {
            super::require_confirmation(yes, "deploy apply")?;
            match &ctx.platform_config {
                PlatformDeploymentConfig::Local(_) => {
                    deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
                    let result = process::spawn_tokeirad(&ctx.path).await;
                    deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
                    result?;
                }
                PlatformDeploymentConfig::Compose(config) => {
                    let mut engine =
                        DeployEngine::new(ComposeDeployment, config, &ctx.path).await?;
                    let compose_file = ctx.path.join("docker-compose.yml");
                    let platform = ComposeDeployment::compose_platform(
                        &compose_file,
                        &config.project_name,
                    )?;
                    print_plan(&engine.apply(&platform as &dyn Platform).await?);
                    deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
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
