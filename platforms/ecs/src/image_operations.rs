//! Definition-bound ECS image inventory.
//!
//! The admitted definition is the sole image inventory: it names each local
//! build or upstream mirror together with its repository suffix and tag. No
//! image command reads or mutates deployment state.

use std::collections::HashSet;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokeira_deploy_engine::ImageSourceType;
use tokeira_platform::{
    author::from_located_value,
    declaration::{DeclaredImage, DeploymentRef, ImageOperations},
};

use crate::ops::evaluated_configuration;

#[derive(Debug, Deserialize)]
struct ImageConfiguration {
    images: ImageInventory,
}

#[derive(Debug, Deserialize)]
struct ImageInventory {
    tokeirad: BuildImage,
    autoscaler: BuildImage,
    mimir: MirrorImage,
    loki: MirrorImage,
    grafana: MirrorImage,
    alloy: MirrorImage,
    aws_cli: MirrorImage,
    busybox: MirrorImage,
}

#[derive(Debug, Deserialize)]
struct BuildImage {
    name: String,
    repository: String,
    tag: String,
}

#[derive(Debug, Deserialize)]
struct MirrorImage {
    name: String,
    repository: String,
    tag: String,
    upstream_ref: String,
}

/// ECS implementation of the optional definition-bound image capability.
#[derive(Debug)]
pub(crate) struct EcsImageOperations;

impl ImageOperations for EcsImageOperations {
    fn list(&self, deployment: &DeploymentRef) -> Result<Vec<DeclaredImage>> {
        resolve_images(deployment)
    }
}

fn resolve_images(deployment: &DeploymentRef) -> Result<Vec<DeclaredImage>> {
    let authored: ImageConfiguration = from_located_value(evaluated_configuration(deployment)?)
        .context("admitted ECS definition has no usable image configuration")?;
    let inventory = authored.images;
    let mut images = vec![
        resolve_build(deployment, "images.tokeirad", inventory.tokeirad)?,
        resolve_build(deployment, "images.autoscaler", inventory.autoscaler)?,
        resolve_mirror(deployment, "images.mimir", inventory.mimir)?,
        resolve_mirror(deployment, "images.loki", inventory.loki)?,
        resolve_mirror(deployment, "images.grafana", inventory.grafana)?,
        resolve_mirror(deployment, "images.alloy", inventory.alloy)?,
        resolve_mirror(deployment, "images.aws_cli", inventory.aws_cli)?,
        resolve_mirror(deployment, "images.busybox", inventory.busybox)?,
    ];
    validate_inventory(&images)?;
    // Definition field order is not an operator contract. Stable name order
    // keeps human and JSON inventory output identical across frontends.
    images.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(images)
}

fn resolve_build(
    deployment: &DeploymentRef,
    path: &str,
    image: BuildImage,
) -> Result<DeclaredImage> {
    let name = required(image.name, &format!("{path}.name"))?;
    let repository = repository(deployment, image.repository, path)?;
    let tag = required(image.tag, &format!("{path}.tag"))?;
    validate_tag(&tag).with_context(|| format!("invalid `{path}.tag`"))?;
    Ok(DeclaredImage {
        name,
        source_type: ImageSourceType::Build,
        repository,
        tag,
        upstream_ref: None,
    })
}

fn resolve_mirror(
    deployment: &DeploymentRef,
    path: &str,
    image: MirrorImage,
) -> Result<DeclaredImage> {
    let tag = required(image.tag, &format!("{path}.tag"))?;
    validate_tag(&tag).with_context(|| format!("invalid `{path}.tag`"))?;
    Ok(DeclaredImage {
        name: required(image.name, &format!("{path}.name"))?,
        source_type: ImageSourceType::Mirror,
        repository: repository(deployment, image.repository, path)?,
        tag,
        upstream_ref: Some(required(
            image.upstream_ref,
            &format!("{path}.upstream_ref"),
        )?),
    })
}

fn repository(deployment: &DeploymentRef, suffix: String, path: &str) -> Result<String> {
    let suffix = required(suffix, &format!("{path}.repository"))?;
    if suffix.starts_with('/')
        || suffix.ends_with('/')
        || suffix.contains(':')
        || suffix.contains('@')
        || suffix.split('/').any(str::is_empty)
    {
        bail!(
            "admitted ECS definition has invalid `{path}.repository` `{suffix}`; use an ECR repository suffix without a registry host or tag"
        );
    }
    Ok(format!("{}/{suffix}", deployment.name))
}

fn validate_inventory(images: &[DeclaredImage]) -> Result<()> {
    let mut names = HashSet::with_capacity(images.len());
    let mut repositories = HashSet::with_capacity(images.len());
    for image in images {
        if !names.insert(&image.name) {
            bail!(
                "admitted ECS definition has duplicate image name `{}`",
                image.name
            );
        }
        if !repositories.insert(&image.repository) {
            bail!(
                "admitted ECS definition has duplicate image repository `{}`",
                image.repository
            );
        }
    }
    Ok(())
}

fn required(value: String, path: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("admitted ECS definition has an empty `{path}`");
    }
    Ok(value)
}

fn validate_tag(tag: &str) -> Result<()> {
    let mut bytes = tag.bytes();
    let Some(first) = bytes.next() else {
        bail!("image tag must not be empty");
    };
    let first_valid = first.is_ascii_alphanumeric() || first == b'_';
    let rest_valid =
        bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'));
    if tag.len() > 128 || !first_valid || !rest_valid {
        bail!(
            "invalid image tag `{tag}`; use 1-128 ASCII letters, digits, `_`, `.`, or `-`, beginning with a letter, digit, or `_`"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_follows_both_admitted_definition_formats() {
        for format in ["tkd", "tkdp"] {
            let temp = tempfile::tempdir().expect("deployment directory");
            let deployment = crate::ops::tests::stage_definition(temp.path(), format);
            let images = EcsImageOperations
                .list(&deployment)
                .expect("definition-derived image inventory");

            assert_eq!(images.len(), 8);
            let autoscaler = images
                .iter()
                .find(|image| image.name == "tokeira-autoscaler")
                .expect("autoscaler image");
            assert_eq!(autoscaler.source_type, ImageSourceType::Build);
            assert_eq!(autoscaler.repository, "ops-fixture/tokeira-autoscaler");
            let mimir = images
                .iter()
                .find(|image| image.name == "grafana-mimir")
                .expect("Mimir image");
            assert_eq!(mimir.repository, "ops-fixture/mimir");
            assert_eq!(mimir.tag, "3.2.0");
            assert_eq!(mimir.upstream_ref.as_deref(), Some("grafana/mimir:3.2.0"));
        }
    }

    #[test]
    fn image_tags_are_validated_before_external_work() {
        for valid in ["latest", "v2026-09-02", "release_1.2"] {
            validate_tag(valid).expect("valid image tag");
        }
        for invalid in ["", ".leading", "contains/slash", "contains space"] {
            assert!(validate_tag(invalid).is_err(), "`{invalid}` must fail");
        }
    }
}
