use std::fs;
use std::path::Path;

use anyhow::{Result, anyhow, bail};
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_iac::{Change, ChangeKind, ModuleSelection};
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::InfraEngine;

use crate::cli::InfraAction;
use crate::deployment_dir::TOKEIRAD_TOML;
use crate::deployment_dir::{
    DeploymentContext, DeploymentResolver, PlatformDeploymentConfig,
};
use crate::metadata::DeploymentStatus;

pub async fn run(
    action: InfraAction,
    deployments: &DeploymentResolver,
    ctx: DeploymentContext,
) -> Result<()> {
    match &ctx.platform_config {
        PlatformDeploymentConfig::Local(config) => {
            run_with_engine(action, deployments, &ctx, LocalDeployment, config).await
        }
        PlatformDeploymentConfig::Compose(config) => {
            run_with_engine(action, deployments, &ctx, ComposeDeployment, config).await
        }
    }
}

async fn run_with_engine<D>(
    action: InfraAction,
    deployments: &DeploymentResolver,
    ctx: &DeploymentContext,
    deployment: D,
    config: &D::Config,
) -> Result<()>
where
    D: tokeira_orchestrator::Deployment,
{
    match action {
        InfraAction::Plan { module } => {
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let composition = engine.compose(module_selection(module))?;
            print_plan(&engine.plan(&composition).await?);
        }
        InfraAction::Apply { yes, module } => {
            super::require_confirmation(yes, "infra apply")?;
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let composition = engine.compose(module_selection(module))?;
            print_plan(&engine.apply(&composition).await?);
            write_tokeirad_writeback(&ctx.path, engine.collect_writeback())?;
        }
        InfraAction::Destroy { yes, module } => {
            super::require_confirmation(yes, "infra destroy")?;
            let mut engine = InfraEngine::new(deployment, config, &ctx.path).await?;
            let composition = engine.compose(module_selection(module))?;
            print_plan(&engine.destroy(&composition).await?);
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

fn module_selection(module: Option<String>) -> ModuleSelection {
    module
        .map(|name| ModuleSelection::Only(vec![name]))
        .unwrap_or(ModuleSelection::All)
}

pub fn read_tokeirad_config(path: &Path) -> Result<tokeira_config::TokeiraConfig> {
    Ok(toml::from_str(&fs::read_to_string(path)?)?)
}

pub fn write_tokeirad_writeback(
    deployment_path: &Path,
    values: Vec<(String, String)>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let path = deployment_path.join(TOKEIRAD_TOML);
    let mut document = fs::read_to_string(&path)?.parse::<toml_edit::DocumentMut>()?;
    for (key, value) in values {
        set_dotted_string(&mut document, &key, &value)?;
    }
    fs::write(&path, document.to_string())?;
    Ok(())
}

fn set_dotted_string(
    document: &mut toml_edit::DocumentMut,
    dotted_key: &str,
    value: &str,
) -> Result<()> {
    let parts = dotted_key.split('.').collect::<Vec<_>>();
    if parts.is_empty() {
        bail!("empty writeback key");
    }
    let mut item = document.as_item_mut();
    for part in &parts[..parts.len() - 1] {
        if item.get(part).is_none() {
            item[part] = toml_edit::Item::Table(toml_edit::Table::new());
        }
        item = item
            .get_mut(part)
            .ok_or_else(|| anyhow!("failed to create TOML table {part}"))?;
    }
    item[parts[parts.len() - 1]] = toml_edit::value(value);
    Ok(())
}

fn print_plan(changes: &[Change]) {
    for change in changes {
        let marker = match change.kind {
            ChangeKind::Create => "+",
            ChangeKind::Update => "~",
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
