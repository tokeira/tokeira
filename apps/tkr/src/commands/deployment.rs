//! `tkr deployment` — CRUD and selection for named deployments.
//!
//! This module owns the operator-facing lifecycle (create / list / use /
//! destroy). It deliberately does NOT touch infrastructure or services:
//! `deployment create` writes template configs and records metadata, but
//! nothing tangible exists in AWS / Compose until `tkr infra apply` and
//! `tkr deploy apply` run against that deployment.
//!
//! The `Destroy` action removes the deployment directory from disk but
//! does NOT destroy cloud resources. Operators must run `tkr infra
//! destroy` first; this ordering is deliberate so a misplaced
//! `deployment destroy` never orphans AWS resources.

use anyhow::Result;

use crate::{
    cli::DeploymentAction,
    deployment_dir::{DeploymentResolver, normalize_name},
    deployment_lock, launcher,
    metadata::DeploymentMetadata,
    output::OutputFormatter,
};

pub(crate) async fn run(
    action: DeploymentAction,
    deployments: &DeploymentResolver,
    selected: Option<&str>,
    json: bool,
) -> Result<()> {
    match action {
        DeploymentAction::Create {
            name,
            platform,
            storage,
            region,
        } => {
            let resolved_name = name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let metadata =
                deployments.create(&resolved_name, platform.into(), storage.into(), region)?;
            // Forwarded (`.tkd`) platforms carry their own bound provisioner —
            // introduce `tkp` into the deployment at create.
            if deployments.is_forwarded(Some(&metadata.name))? {
                deployments.place_provisioner(&metadata.name)?;
                println!("provisioner: placed `tkp` in the deployment");
            }
            print_metadata(&metadata, json)?;
        }
        DeploymentAction::List => {
            let items = deployments.list()?;
            let output = OutputFormatter::new(json);
            if json {
                output.print_json(&items)?;
            } else if items.is_empty() {
                println!(
                    "no deployments found under {}",
                    deployments.root().display()
                );
            } else {
                let latest = deployments.latest_name();
                let rows = items
                    .into_iter()
                    .map(|item| {
                        let marker = if latest.as_deref() == Some(item.name.as_str()) {
                            "*"
                        } else {
                            " "
                        };
                        vec![
                            marker.to_string(),
                            item.name,
                            format!("{:?}", item.platform),
                            format!("{:?}", item.storage),
                            format!("{:?}", item.status),
                            item.updated_at,
                        ]
                    })
                    .collect::<Vec<_>>();
                output.print_table(&rows);
            }
        }
        DeploymentAction::Use { name } => {
            deployments.mark_latest(&name)?;
            println!("selected deployment {}", normalize_name(&name));
        }
        DeploymentAction::Destroy { name, yes } => {
            super::require_confirmation(yes, "deployment destroy")?;
            let name = normalize_name(&name);
            deployments.remove(&name)?;
            println!("destroyed deployment {name}");
        }
        DeploymentAction::Lock { name } => {
            let lock = deployment_lock::lock(deployments, name.as_deref())?;
            println!("locked deployment '{}' ({})", lock.name, lock.fingerprint);
            println!(
                "mutating commands now refuse any other deployment; `tkr deployment unlock --yes` to clear"
            );
        }
        DeploymentAction::Unlock { yes } => {
            super::require_confirmation(yes, "deployment unlock")?;
            match deployment_lock::unlock(deployments)? {
                Some(lock) => println!("unlocked deployment '{}'", lock.name),
                None => println!("no deployment lock was set"),
            }
        }
        DeploymentAction::Describe => {
            let dir = deployments.resolve_dir(selected)?;
            let extra = if json {
                vec!["--json".to_string()]
            } else {
                Vec::new()
            };
            launcher::launch(&dir, "describe", &extra).await?;
        }
        DeploymentAction::Apply => {
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch_apply(&dir).await?;
        }
        DeploymentAction::Upgrade => {
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch(&dir, "upgrade", &[]).await?;
        }
        DeploymentAction::Rollback => {
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch(&dir, "rollback", &[]).await?;
        }
    }
    Ok(())
}

fn print_metadata(metadata: &DeploymentMetadata, as_json: bool) -> Result<()> {
    if as_json {
        println!("{}", serde_json::to_string_pretty(metadata)?);
    } else {
        println!(
            "{}\t{:?}\t{:?}\t{:?}",
            metadata.name, metadata.platform, metadata.storage, metadata.status
        );
    }
    Ok(())
}
