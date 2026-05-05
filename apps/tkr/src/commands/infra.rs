use std::{fs, path::Path};

use anyhow::{Result, anyhow, bail};
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_iac::{Change, ChangeKind, ModuleSelection};
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::InfraEngine;

use crate::{
    cli::InfraAction,
    deployment_dir::{
        DeploymentContext, DeploymentResolver, PlatformDeploymentConfig, TOKEIRAD_TOML,
    },
    metadata::DeploymentStatus,
    tui::{ActionTuiHandle, OutputFormat},
};

pub async fn run(
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
    }
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
            if let Ok(changes) = &result {
                finish_progress(&tui, changes);
                print_plan(changes);
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    proptest! {
        #[test]
        fn toml_writeback_round_trips(
            pairs in prop::collection::vec((dotted_key_strategy(), "[a-zA-Z0-9_-]{1,24}"), 1..30)
        ) {
            prop_assume!(!has_prefix_conflict(&pairs));
            let mut document = "".parse::<toml_edit::DocumentMut>()?;
            let mut expected = BTreeMap::new();

            for (key, value) in pairs {
                set_dotted_string(&mut document, &key, &value)
                    .map_err(|err| TestCaseError::fail(err.to_string()))?;
                expected.insert(key, value);
            }

            for (key, value) in expected {
                let actual = read_dotted_string(&document, &key);
                prop_assert_eq!(actual.as_deref(), Some(value.as_str()));
            }
        }

        #[test]
        fn toml_writeback_preserves_comments(
            first_comment in "[a-zA-Z0-9 _-]{1,40}",
            second_comment in "[a-zA-Z0-9 _-]{1,40}",
            value in "[a-zA-Z0-9_-]{1,24}"
        ) {
            let source = format!(
                "# {first_comment}\n[infrastructure]\n# {second_comment}\ndsql_endpoint = \"old\"\n"
            );
            let mut document = source.parse::<toml_edit::DocumentMut>()?;

            set_dotted_string(&mut document, "infrastructure.dsql_endpoint", &value)
                .map_err(|err| TestCaseError::fail(err.to_string()))?;

            let rendered = document.to_string();
            let expected_first = format!("# {first_comment}");
            let expected_second = format!("# {second_comment}");
            prop_assert!(rendered.contains(&expected_first));
            prop_assert!(rendered.contains(&expected_second));
            let actual = read_dotted_string(&document, "infrastructure.dsql_endpoint");
            prop_assert_eq!(actual.as_deref(), Some(value.as_str()));
        }
    }

    fn dotted_key_strategy() -> impl Strategy<Value = String> {
        prop::collection::vec("[a-zA-Z_][a-zA-Z0-9_]{0,10}", 1..5).prop_map(|parts| parts.join("."))
    }

    fn has_prefix_conflict(pairs: &[(String, String)]) -> bool {
        pairs.iter().any(|(left, _)| {
            pairs.iter().any(|(right, _)| {
                left != right
                    && right
                        .strip_prefix(left)
                        .is_some_and(|suffix| suffix.starts_with('.'))
            })
        })
    }

    fn read_dotted_string(document: &toml_edit::DocumentMut, dotted_key: &str) -> Option<String> {
        let mut item = document.as_item();
        for part in dotted_key.split('.') {
            item = item.get(part)?;
        }
        item.as_str().map(ToOwned::to_owned)
    }
}
