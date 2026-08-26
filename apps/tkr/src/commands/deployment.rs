//! `tkr deployment` — CRUD and selection for named deployments.
//!
//! This module owns the operator-facing lifecycle (create / list / use /
//! destroy). Creation realizes a durable Day-0 deployment. Destruction is its
//! reverse: services first, infrastructure second, and records last. The
//! directory remains authoritative recovery material until both live planes
//! report success.

use std::future::Future;

use anyhow::{Context, Result, bail};
use tokeira_deployment::RecordedDefinition;
use tokeira_orchestrator::StorageKind;

use crate::{
    cli::{DeployAction, DeploymentAction, InfraAction},
    deployment_dir::{DefinitionSeed, DeploymentResolver, load_context, normalize_name},
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
            dev_engine,
            build_image,
        } => {
            let resolved_name = name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let storage = storage.into();
            if crate::legacy::LegacyPlatform::from_id(&platform).is_some() {
                if format.is_some() {
                    bail!("legacy platform `{platform}` does not use a definition frontend");
                }
                if dev_engine || build_image.is_some() {
                    bail!("legacy platform `{platform}` takes no engine options");
                }
                let pending =
                    deployments.begin_create(&resolved_name, platform, storage, region, None)?;
                let metadata = pending.publish()?;
                print_metadata(&metadata, json)?;
                return Ok(());
            }

            if storage != StorageKind::InMemory || region.is_some() {
                bail!(
                    "discovered platforms take initial provider choices from their definition seed; create the deployment with defaults, then edit the recorded definition before first apply"
                );
            }
            let cwd = std::env::current_dir().context("cannot determine current directory")?;
            let workspace = crate::bundle_create::workspace_root_from(&cwd)?;
            let discovery =
                crate::platform_discovery::PlatformDiscovery::from_workspace(&workspace)?;
            let platform_descriptor = discovery.platform(&platform)?;
            let (frontend_descriptor, definition_path, seed_path) =
                discovery.workspace_frontend(platform_descriptor, format.as_ref())?;
            let platform_package = &platform_descriptor.package;
            let frontend_package = &frontend_descriptor.package;
            let sources = tokeira_platform_definition::read_source_set(
                &seed_path,
                Some(&frontend_descriptor.format),
            )?;
            let content = discovery
                .workspace_content(platform_descriptor)?
                .into_iter()
                .map(|file| (file.relative_path, file.bytes))
                .collect();
            let seed = DefinitionSeed {
                definition: RecordedDefinition {
                    format: frontend_descriptor.format.clone(),
                    path: definition_path,
                },
                bytes: sources.root,
                parts: sources.parts,
                content,
            };
            let pending =
                deployments.begin_create(&resolved_name, platform, storage, region, Some(seed))?;
            if dev_engine {
                crate::bundle_create::place_dev_provisioner_at(
                    pending.path(),
                    &workspace,
                    platform_package,
                    frontend_package,
                )
                .await?;
            } else {
                let image = build_image.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "deployment create obtains the engine as a verified hermetic bundle and \
                         needs `--build-image <image>@sha256:<digest>` (the digest-pinned build \
                         container is an engine-identity input); pass `--dev-engine` for a \
                         native workspace build (local deployments only)"
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
            }
            let facts = launcher::validate_staged_definition(pending.path()).await?;
            launcher::seed_staged_config(pending.path()).await?;
            launcher::realize_staged_deployment(pending.path(), &facts, 0).await?;
            let identity = facts.identity.ok_or_else(|| {
                anyhow::anyhow!(
                    "the staged check verified but reported no configuration identity; \
                     the deployment cannot claim its definition"
                )
            })?;
            let companions = facts.companions.unwrap_or_default();

            // Repository state lands in the staged dir (Requirement 2.3);
            // only the upload itself waits for the local commit.
            let staged_repository = crate::repository_setup::provision_staged(
                deployments.root(),
                pending.path(),
                &resolved_name,
            )
            .await?;
            let metadata = pending.publish()?;
            match crate::repository_setup::publish_birth(
                &deployments.path(&metadata.name),
                &staged_repository,
                identity,
                companions,
            )
            .await
            {
                Ok(receipt) => {
                    println!(
                        "repository: publication {} written to {}",
                        receipt.version,
                        staged_repository.config.locator.display()
                    );
                }
                // The deployment is committed; the publication is repairable
                // state, never a reason to unwind (Requirement 2.4).
                Err(error) => {
                    eprintln!(
                        "repository: the deployment is created, but its birth publication \
                         failed and is pending: {error:#}\n\
                         complete it with `tkr deployment publish`"
                    );
                }
            }
            print_metadata(&metadata, json)?;
        }
        DeploymentAction::List { repositories } => {
            if let Some(selector) = repositories {
                return crate::repository_setup::list_repositories(
                    deployments.root(),
                    &selector,
                    json,
                )
                .await;
            }
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
        DeploymentAction::Fetch {
            name,
            repository,
            trust_anchor,
        } => {
            crate::repository_setup::fetch(deployments, &name, &repository, &trust_anchor).await?;
        }
        DeploymentAction::Publish { transition, yes } => {
            super::require_confirmation(yes, "deployment publish")?;
            let dir = deployments.resolve_dir(selected)?;
            crate::repository_setup::publish_repair(&dir, transition.as_deref()).await?;
        }
        DeploymentAction::Refresh { yes } => {
            super::require_confirmation(yes, "deployment refresh")?;
            let dir = deployments.resolve_dir(selected)?;
            crate::repository_setup::refresh(&dir).await?;
        }
        DeploymentAction::Inspect => {
            let dir = deployments.resolve_dir(selected)?;
            crate::repository_setup::inspect(&dir, json).await?;
        }
        DeploymentAction::Use { name } => {
            deployments.mark_latest(&name)?;
            println!("selected deployment {}", normalize_name(&name));
        }
        DeploymentAction::Destroy { name, yes } => {
            super::require_confirmation(yes, "deployment destroy")?;
            let name = normalize_name(&name);
            let dir = deployments.resolve_dir(Some(&name))?;
            remove_after_teardown(deployments, &name, async {
                if deployments.uses_bound_provisioner(Some(&name))? {
                    let mut extra = vec!["--yes".to_string()];
                    if json {
                        extra.push("--json".to_string());
                    }
                    if detail {
                        extra.push("--detail".to_string());
                    }
                    launcher::launch(&dir, &["destroy"], &extra).await?;
                } else {
                    crate::commands::deploy::run(
                        DeployAction::Destroy { yes: true },
                        deployments,
                        load_context(deployments, Some(&name))?,
                    )
                    .await?;
                    let format = if json {
                        crate::tui::OutputFormat::Json
                    } else {
                        crate::tui::OutputFormat::Human
                    };
                    crate::commands::infra::run(
                        InfraAction::Destroy {
                            yes: true,
                            module: None,
                        },
                        deployments,
                        load_context(deployments, Some(&name))?,
                        format,
                    )
                    .await?;
                }
                Ok(())
            })
            .await?;
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

async fn remove_after_teardown<F>(
    deployments: &DeploymentResolver,
    name: &str,
    teardown: F,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    teardown.await?;
    deployments.remove(name)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn teardown_failure_retains_the_deployment_records() {
        let root = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(root.path().to_path_buf());
        let deployment = deployments.path("dev");
        std::fs::create_dir_all(&deployment).unwrap();

        let error = remove_after_teardown(&deployments, "dev", async {
            Err(anyhow::anyhow!("provider deletion failed"))
        })
        .await
        .expect_err("teardown failure must retain recovery records");

        assert!(error.to_string().contains("provider deletion failed"));
        assert!(deployment.exists());
    }

    #[tokio::test]
    async fn successful_teardown_removes_the_deployment_records() {
        let root = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(root.path().to_path_buf());
        let deployment = deployments.path("dev");
        std::fs::create_dir_all(&deployment).unwrap();

        remove_after_teardown(&deployments, "dev", async { Ok(()) })
            .await
            .unwrap();

        assert!(!deployment.exists());
    }
}
