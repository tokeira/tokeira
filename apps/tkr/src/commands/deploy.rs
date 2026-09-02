//! `tkr deploy` — service lifecycle: plan, apply, destroy, report status.
//!
//! `deploy apply` behaves differently per platform because the underlying
//! runtime engine does:
//!
//! - **Local**: spawn `tokeirad` as a foreground host process via
//!   [`crate::process::spawn_tokeirad`]. Status toggles to `Running` at
//!   spawn and back to `Stopped` when the process exits (normally or via
//!   ctrl-c).
//!
//! Definition-bound platforms, including Compose and ECS, never enter this
//! module; `tkr` forwards their deploy verbs to the married provisioner.
//!
//! `deploy status` prefers the live [`crate::process::local_process_status`]
//! for local deployments over the metadata file so a crashed server is
//! surfaced as `Stopped` even if the metadata still says `Running`.

use anyhow::Result;
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
            let PlatformDeploymentConfig::Local(config) = &ctx.platform_config;
            let mut engine = DeployEngine::new(LocalDeployment, config, &ctx.path).await?;
            print_plan(&engine.plan().await?);
        }
        DeployAction::Apply {
            yes,
            // Consumed only by the forwarded path (the deployment's own
            // provisioner); Local does not use force reconciliation.
            force: _,
            explanation,
        } => {
            super::refuse_explanation(explanation.as_deref())?;
            super::require_confirmation(yes, "deploy apply")?;
            deployments.update_status(&ctx.name, DeploymentStatus::Running)?;
            let result = process::spawn_tokeirad(&ctx.path).await;
            deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
            result?;
        }
        DeployAction::Destroy { yes } => {
            super::require_confirmation(yes, "deploy destroy")?;
            process::stop_tokeirad(&ctx.path).await?;
            deployments.update_status(&ctx.name, DeploymentStatus::Stopped)?;
            println!("destroyed local services for {}", ctx.name);
        }
        DeployAction::Status => {
            let status = process::local_process_status(&ctx.path);
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
