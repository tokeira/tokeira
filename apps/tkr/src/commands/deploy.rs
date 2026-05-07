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
                let mut engine = DeployEngine::new(EcsDeployment, config, &ctx.path).await?;
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
                    let compose_file = ctx.path.join("docker-compose.yml");
                    let deployment = ComposeDeployment;
                    let platform =
                        ComposeDeployment::compose_platform(&compose_file, &config.project_name)?;
                    deployment
                        .validate_for_deploy_apply(config, &platform)
                        .await?;
                    let mut engine = DeployEngine::new(deployment, config, &ctx.path).await?;
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
