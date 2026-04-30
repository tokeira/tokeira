use anyhow::Result;

use crate::{
    cli::ScaleAction,
    deployment_dir::{DeploymentContext, DeploymentResolver},
    metadata::DeploymentStatus,
};

pub async fn run(
    action: ScaleAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
) -> Result<()> {
    let ops = super::PlatformOps::from_context(&ctx)?;
    match action {
        ScaleAction::Up { service, replicas } => {
            if let Some(service) = service {
                let replicas = replicas.unwrap_or(1);
                ops.scale_up(&service, replicas).await?;
                println!("scaled {service} up to {replicas} replicas");
            } else {
                for desired in ops.desired_replicas() {
                    ops.scale_up(&desired.service, desired.replicas).await?;
                    println!("scaled {} up to {}", desired.service, desired.replicas);
                }
            }
        }
        ScaleAction::Down { service, replicas } => {
            if let Some(service) = service {
                let replicas = replicas.unwrap_or(1);
                ops.scale_down(&service, replicas).await?;
                println!("scaled {service} down by {replicas}");
            } else {
                for desired in ops.desired_replicas() {
                    ops.scale_down(&desired.service, desired.replicas).await?;
                    println!("scaled {} down by {}", desired.service, desired.replicas);
                }
                deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
            }
        }
        ScaleAction::Status => {
            for desired in ops.desired_replicas() {
                println!("{}\tconfigured={}", desired.service, desired.replicas);
            }
        }
    }
    Ok(())
}
