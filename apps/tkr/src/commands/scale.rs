//! `tkr scale` — adjust service replica counts.
//!
//! Three modes:
//!
//! - `scale up|down <service> <replicas>` — explicit per-service targets.
//! - `scale up|down` (no args) — iterate every service the platform's
//!   `desired_replicas()` reports and apply the configured value. Used for
//!   "bring the whole stack up/down".
//! - `scale status` — print each service and its configured replica count
//!   (configured, not currently-running; the ops layer doesn't expose live
//!   counts uniformly across platforms yet).
//!
//! The bulk `scale down` with no args flips the deployment's metadata
//! status to `Stopped` so `tkr deploy status` reflects reality.

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
