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
use tokeira_orchestrator::{PlatformKind, PlatformLaunchClass};

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
    detail: bool,
) -> Result<()> {
    match action {
        DeploymentAction::Create {
            name,
            platform,
            storage,
            region,
            bundle,
            build_image,
        } => {
            let resolved_name = name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let platform: PlatformKind = platform.into();
            let storage = storage.into();
            let seed = if platform == PlatformKind::Compose {
                Some(crate::deployment_dir::compose_definition_seed(
                    storage,
                    region.as_deref(),
                )?)
            } else {
                None
            };
            let pending =
                deployments.begin_create(&resolved_name, platform, storage, region, seed)?;
            // Forwarded (`.tkd`) platforms carry their own bound provisioner —
            // introduce `tkp` into the deployment at create: the verified
            // hermetic bundle when `--bundle` opts in (task 18.3), else the
            // Phase-0 native dev copy.
            if pending.metadata().launch_class == Some(PlatformLaunchClass::BoundProvisioner) {
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
                    )
                    .await?;
                } else {
                    // (place_provisioner reports the resolution leg + digest.)
                    deployments.place_provisioner_at(pending.path())?;
                }
                launcher::validate_staged_definition(pending.path()).await?;
            } else if bundle {
                anyhow::bail!(
                    "`--bundle` applies only to forwarded (`.tkd`) platforms — this deployment \
                     is driven in-process"
                );
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
            launcher::launch_apply(&dir, yes, None).await?;
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
            "{}\t{:?}\t{:?}\t{:?}",
            metadata.name, metadata.platform, metadata.storage, metadata.status
        );
    }
    Ok(())
}
