//! Template TOML written at `tkr deployment create` time.
//!
//! Each platform contributes its own prototypical `deployment.toml` (the
//! platform-specific config) and `tokeirad.toml` (the server-side
//! [`tokeira_config::TokeiraConfig`]). The server-config branch also
//! round-trips the TOML through the parser as a compile-time safety net:
//! if a platform ships a template that no longer parses, `tkr deployment
//! create` will fail here with a clear error rather than minutes later
//! when `tokeirad` tries to start.

use anyhow::{Result, bail};
use tokeira_compose_deployment::provisioner::ComposeProvisioner;
use tokeira_ecs_deployment::EcsDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformKind, StorageKind};
use toml_edit::{DocumentMut, value};

/// The compose platform definition seeded at `tkr deployment create --platform
/// compose`, with the operator's `--storage`/`--region` **baked into `config()`**.
///
/// The compose platform is `.tkd`-defined and provisioned by a forwarded compose
/// `tkp` — the operator edits the `config()` values in this file; the structure
/// (`deployment(cfg, cx)`) is the platform's. In-memory keeps the shipped default;
/// DSQL rewrites `storage: Storage::InMemory` to `Storage::Dsql { .. }` so the
/// seeded `.tkd` (the interpreter's source of truth), `metadata.json.storage`, and
/// the prototypical `tokeirad.toml` all agree. (Transitional: embedded from the
/// compose platform crate, which owns its default definition.)
pub(crate) fn compose_definition(
    seed: &str,
    storage: StorageKind,
    region: Option<&str>,
) -> Result<String> {
    match storage {
        StorageKind::InMemory => Ok(seed.to_string()),
        StorageKind::Dsql => {
            const MARKER: &str = "storage: Storage::InMemory,";
            if !seed.contains(MARKER) {
                bail!(
                    "compose definition.tkd no longer contains `{MARKER}`; the `--storage dsql` \
                     bake-in in prototypical.rs needs updating"
                );
            }
            let region = region.unwrap_or("us-east-1");
            let dsql = format!(
                "storage: Storage::Dsql {{ region: \"{region}\".into(), mode: DsqlMode::Managed, \
                 endpoint: None, arn: None }},"
            );
            Ok(seed.replacen(MARKER, &dsql, 1))
        }
    }
}

pub(crate) fn deployment_config(
    platform: PlatformKind,
    storage: StorageKind,
    region: Option<&str>,
) -> Result<String> {
    let toml = match platform {
        PlatformKind::Local => LocalDeployment::prototypical_config(storage),
        PlatformKind::Compose => ComposeProvisioner::prototypical_config(storage),
        PlatformKind::Ecs => EcsDeployment::prototypical_config(storage),
    };
    if platform == PlatformKind::Compose && storage == StorageKind::Dsql {
        patch_dsql_region(toml, region.unwrap_or("us-east-1"))
    } else {
        Ok(toml)
    }
}

pub(crate) fn server_config(
    platform: PlatformKind,
    storage: StorageKind,
    region: Option<&str>,
) -> Result<String> {
    let toml = match platform {
        PlatformKind::Local => {
            let toml = LocalDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
        PlatformKind::Compose => {
            let toml = ComposeProvisioner::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
        PlatformKind::Ecs => {
            let toml = EcsDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
    };
    if storage == StorageKind::Dsql {
        patch_server_dsql_region(toml, region.unwrap_or("us-east-1"))
    } else {
        Ok(toml)
    }
}

fn patch_dsql_region(toml: String, region: &str) -> Result<String> {
    let mut document = toml.parse::<DocumentMut>()?;
    document["dsql"]["region"] = value(region);
    Ok(document.to_string())
}

fn patch_server_dsql_region(toml: String, region: &str) -> Result<String> {
    let mut document = toml.parse::<DocumentMut>()?;
    document["infrastructure"]["region"] = value(region);
    if let Some(dsql) = document
        .get_mut("infrastructure")
        .and_then(|i| i.get_mut("dsql"))
    {
        dsql["region"] = value(region);
    }
    Ok(document.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_compose_deployment::ComposeConfig;
    use tokeira_ecs_deployment::EcsConfig;

    fn compose_seed() -> String {
        std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../platforms/compose/definition.tkd"),
        )
        .expect("workspace Compose definition seed")
    }

    #[test]
    fn compose_definition_in_memory_keeps_the_shipped_default() {
        let tkd = compose_definition(&compose_seed(), StorageKind::InMemory, None).unwrap();
        assert!(
            tkd.contains("storage: Storage::InMemory,"),
            "config() default is in-memory"
        );
    }

    #[test]
    fn compose_definition_dsql_bakes_storage_and_region_into_config() {
        let tkd =
            compose_definition(&compose_seed(), StorageKind::Dsql, Some("eu-west-1")).unwrap();
        // config()'s storage line is rewritten to Dsql with the requested region;
        // the deployment() destructure (`Storage::Dsql { .. }`) is untouched.
        assert!(
            !tkd.contains("storage: Storage::InMemory,"),
            "the in-memory default was replaced"
        );
        assert!(
            tkd.contains("storage: Storage::Dsql { region: \"eu-west-1\".into()"),
            "region baked into config()"
        );
        assert!(tkd.contains("mode: DsqlMode::Managed"));
    }

    #[test]
    fn compose_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Compose, StorageKind::InMemory, None).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"public.ecr.aws/aws-cli/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        // One annotation, tool-neutral, on the one image the operator must
        // build locally; the ECS-only mirror notes no longer seed compose
        // deployments (they described another platform's workflow).
        assert!(toml.contains("build the tokeirad image locally before deploying"));
        assert!(!toml.contains("image mirror"));

        let _: ComposeConfig = toml::from_str(&toml).unwrap();
    }

    #[test]
    fn ecs_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Ecs, StorageKind::Dsql, None).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"amazon/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        assert!(toml.contains("populated by `tkr image push`"));
        assert!(toml.contains("populated by `tkr image mirror`"));

        let _: EcsConfig = toml::from_str(&toml).unwrap();
    }
}
