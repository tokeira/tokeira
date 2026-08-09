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

use anyhow::{Context, Result, bail};
use tokeira_orchestrator::StorageKind;
use tokeira_provisioner::RecordedDefinition;

use crate::{
    catalog::{FrontendSource, PlatformCatalog, PlatformSource},
    cli::DeploymentAction,
    deployment_dir::{DefinitionSeed, DeploymentResolver, normalize_name},
    deployment_lock, launcher,
    metadata::DeploymentMetadata,
    output::OutputFormatter,
};

pub(crate) async fn run(
    action: DeploymentAction,
    deployments: &DeploymentResolver,
    selected: Option<&str>,
    json: bool,
    detail: bool,
) -> Result<()> {
    match action {
        DeploymentAction::Create {
            name,
            platform,
            format,
            storage,
            region,
            bundle,
            build_image,
        } => {
            let resolved_name = name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let storage = storage.into();
            if crate::legacy::LegacyPlatform::from_id(&platform).is_some() {
                if format.is_some() {
                    bail!("legacy platform `{platform}` does not use a definition frontend");
                }
                if bundle {
                    bail!("`--bundle` applies only to catalog-discovered platforms");
                }
                let pending =
                    deployments.begin_create(&resolved_name, platform, storage, region, None)?;
                let metadata = pending.publish()?;
                print_metadata(&metadata, json)?;
                return Ok(());
            }

            if storage != StorageKind::InMemory || region.is_some() {
                bail!(
                    "catalog-defined platforms take initial provider choices from their definition seed; create the deployment with defaults, then edit the recorded definition before first apply"
                );
            }
            let cwd = std::env::current_dir().context("cannot determine current directory")?;
            let workspace = crate::bundle_create::workspace_root_from(&cwd)?;
            let catalog = PlatformCatalog::from_workspace(&workspace)?;
            let platform_descriptor = catalog.platform(&platform)?;
            let (frontend_descriptor, definition_path, seed_path) =
                catalog.workspace_frontend(platform_descriptor, format.as_ref())?;
            let PlatformSource::Workspace(platform_package) = &platform_descriptor.source else {
                unreachable!("workspace catalog returned published platform coordinates");
            };
            let FrontendSource::Workspace(frontend_package) = &frontend_descriptor.source else {
                unreachable!("workspace catalog returned published frontend coordinates");
            };
            let seed = DefinitionSeed {
                definition: RecordedDefinition {
                    format: frontend_descriptor.format.clone(),
                    path: definition_path,
                },
                bytes: std::fs::read(&seed_path).with_context(|| {
                    format!("failed to read definition seed {}", seed_path.display())
                })?,
                parts: crate::deployment_dir::sibling_parts(&seed_path)?,
            };
            let pending =
                deployments.begin_create(&resolved_name, platform, storage, region, Some(seed))?;
            {
                if bundle {
                    let image = build_image.as_deref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "`--bundle` requires `--build-image <image>@sha256:<digest>` (the \
                             digest-pinned build container is an engine-identity input)"
                        )
                    })?;
                    crate::bundle_create::place_bundle_provisioner_at(
                        deployments,
                        pending.path(),
                        image,
                        &workspace,
                        platform_package,
                        frontend_package,
                    )
                    .await?;
                } else {
                    deployments.place_provisioner_at(
                        pending.path(),
                        &workspace,
                        platform_package,
                        frontend_package,
                    )?;
                }
                launcher::validate_staged_definition(pending.path()).await?;
                launcher::seed_staged_config(pending.path()).await?;
            }
            let metadata = pending.publish()?;
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
                            item.platform.to_string(),
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
            // The output contract's global flags travel with the forwarded
            // verb (`--json` complete model; `--detail` verification view).
            let mut extra = Vec::new();
            if json {
                extra.push("--json".to_string());
            }
            if detail {
                extra.push("--detail".to_string());
            }
            launcher::launch(&dir, &["describe"], &extra).await?;
        }
        DeploymentAction::Apply { yes } => {
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch_apply(&dir, yes, None, None).await?;
        }
        DeploymentAction::Upgrade => {
            let dir = deployments.resolve_dir(selected)?;
            // The sanctioned re-marry: candidate from the source pool, byte
            // idempotency, verb driven via the candidate, file re-placed on
            // success (never resolves the married copy as its own candidate).
            launcher::launch_upgrade(&dir).await?;
        }
        DeploymentAction::Rollback => {
            let dir = deployments.resolve_dir(selected)?;
            launcher::launch_rollback(&dir).await?;
        }
    }
    Ok(())
}

fn print_metadata(metadata: &DeploymentMetadata, as_json: bool) -> Result<()> {
    if as_json {
        println!("{}", serde_json::to_string_pretty(metadata)?);
    } else {
        println!(
            "{}\t{}\t{:?}\t{:?}",
            metadata.name, metadata.platform, metadata.storage, metadata.status
        );
    }
    Ok(())
}
