//! Definition-bound ECS image publication.
//!
//! The admitted definition is the sole image inventory: it names each local
//! build or upstream mirror together with its repository suffix and tag. This
//! adapter selects from that inventory and invokes two lower layers: AWS owns
//! ECR authentication/repository preparation, while `tokeira-build` owns the
//! secret-aware Dagger publication mechanics. No image command reads or
//! mutates deployment state.

use std::{collections::HashSet, process::Stdio, time::Duration};

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokeira_aws::prepare_ecr_registry;
use tokeira_build::{MirrorRequest, PublishRequest, RegistryPassword, mirror_image, publish_image};
use tokeira_deploy_engine::ImageSourceType;
use tokeira_platform::{
    author::from_located_value,
    declaration::{DeclaredImage, DeploymentRef, ImageOperations, PublishedImage},
};

use crate::ops::evaluated_configuration;

const DAGGER_SESSION_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct ImageConfiguration {
    aws: ImageAwsConfiguration,
    images: ImageInventory,
}

#[derive(Debug, Deserialize)]
struct ImageAwsConfiguration {
    region: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedImage {
    name: String,
    source_type: ImageSourceType,
    repository: String,
    tag: String,
    upstream_ref: Option<String>,
    local_ref: Option<String>,
}

#[derive(Debug)]
struct ResolvedImages {
    region: String,
    images: Vec<ResolvedImage>,
}

/// ECS implementation of the optional definition-bound image capability.
#[derive(Debug)]
pub(crate) struct EcsImageOperations;

#[async_trait::async_trait]
impl ImageOperations for EcsImageOperations {
    fn list(&self, deployment: &DeploymentRef) -> Result<Vec<DeclaredImage>> {
        Ok(resolve_images(deployment)?
            .images
            .into_iter()
            .map(ResolvedImage::declared)
            .collect())
    }

    async fn push(
        &self,
        deployment: &DeploymentRef,
        image: Option<&str>,
        tag: &str,
    ) -> Result<Vec<PublishedImage>> {
        validate_tag(tag)?;
        let resolved = resolve_images(deployment)?;
        let selected = select_images(&resolved.images, ImageSourceType::Build, image)?;
        // Fail before credentials and repository mutations: a missing local
        // build is an operator workflow error, not an AWS partial apply.
        for image in &selected {
            let local_ref = image.local_ref.as_deref().ok_or_else(|| {
                anyhow::anyhow!("build image `{}` has no local reference", image.name)
            })?;
            if !local_image_exists(local_ref).await? {
                bail!(
                    "local image `{local_ref}` is missing; build it locally before `tkr image push --yes`"
                );
            }
        }

        let authorization = prepare_registry(deployment, &resolved.region, &selected).await?;
        let dagger = dagger_session().await?;
        let outcome = publish_builds(&dagger, &authorization, &selected, tag).await;
        let close = dagger
            .close()
            .await
            .context("close the Dagger image session");
        let published = outcome?;
        close?;
        Ok(published)
    }

    async fn mirror(
        &self,
        deployment: &DeploymentRef,
        image: Option<&str>,
    ) -> Result<Vec<PublishedImage>> {
        let resolved = resolve_images(deployment)?;
        let selected = select_images(&resolved.images, ImageSourceType::Mirror, image)?;
        let authorization = prepare_registry(deployment, &resolved.region, &selected).await?;
        let dagger = dagger_session().await?;
        let outcome = mirror_upstreams(&dagger, &authorization, &selected).await;
        let close = dagger
            .close()
            .await
            .context("close the Dagger image session");
        let published = outcome?;
        close?;
        Ok(published)
    }
}

impl ResolvedImage {
    fn declared(self) -> DeclaredImage {
        DeclaredImage {
            name: self.name,
            source_type: self.source_type,
            repository: self.repository,
            tag: self.tag,
            upstream_ref: self.upstream_ref,
        }
    }
}

fn resolve_images(deployment: &DeploymentRef) -> Result<ResolvedImages> {
    let authored: ImageConfiguration = from_located_value(evaluated_configuration(deployment)?)
        .context("admitted ECS definition has no usable image configuration")?;
    let region = required(authored.aws.region, "aws.region")?;
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
    Ok(ResolvedImages { region, images })
}

fn resolve_build(
    deployment: &DeploymentRef,
    path: &str,
    image: BuildImage,
) -> Result<ResolvedImage> {
    let name = required(image.name, &format!("{path}.name"))?;
    let repository = repository(deployment, image.repository, path)?;
    let tag = required(image.tag, &format!("{path}.tag"))?;
    validate_tag(&tag).with_context(|| format!("invalid `{path}.tag`"))?;
    Ok(ResolvedImage {
        local_ref: Some(format!("{name}:{tag}")),
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
) -> Result<ResolvedImage> {
    let tag = required(image.tag, &format!("{path}.tag"))?;
    validate_tag(&tag).with_context(|| format!("invalid `{path}.tag`"))?;
    Ok(ResolvedImage {
        name: required(image.name, &format!("{path}.name"))?,
        source_type: ImageSourceType::Mirror,
        repository: repository(deployment, image.repository, path)?,
        tag,
        upstream_ref: Some(required(
            image.upstream_ref,
            &format!("{path}.upstream_ref"),
        )?),
        local_ref: None,
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

fn validate_inventory(images: &[ResolvedImage]) -> Result<()> {
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

fn select_images<'a>(
    images: &'a [ResolvedImage],
    source_type: ImageSourceType,
    selected_name: Option<&str>,
) -> Result<Vec<&'a ResolvedImage>> {
    let candidates = images
        .iter()
        .filter(|image| image.source_type == source_type)
        .collect::<Vec<_>>();
    let Some(selected_name) = selected_name else {
        return Ok(candidates);
    };
    if let Some(image) = candidates.iter().find(|image| image.name == selected_name) {
        return Ok(vec![*image]);
    }
    let valid = candidates
        .iter()
        .map(|image| image.name.as_str())
        .collect::<Vec<_>>();
    bail!(
        "unknown {} image `{selected_name}`; valid {} images are: {}",
        source_label(source_type),
        source_label(source_type),
        valid.join(", ")
    )
}

fn source_label(source: ImageSourceType) -> &'static str {
    match source {
        ImageSourceType::Build => "build",
        ImageSourceType::Mirror => "mirror",
        ImageSourceType::Registry => "registry",
    }
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

async fn local_image_exists(image_ref: &str) -> Result<bool> {
    let output = tokio::process::Command::new("docker")
        .args(["image", "inspect", image_ref])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("could not run `docker image inspect`; install Docker and ensure it is on PATH")?;
    if output.status.success() {
        return Ok(true);
    }
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    if diagnostic.to_ascii_lowercase().contains("no such image") {
        Ok(false)
    } else {
        bail!(
            "could not inspect local image `{image_ref}` with Docker: {}",
            diagnostic.trim()
        )
    }
}

async fn prepare_registry(
    deployment: &DeploymentRef,
    region: &str,
    images: &[&ResolvedImage],
) -> Result<tokeira_aws::EcrAuthorization> {
    let repositories = images
        .iter()
        .map(|image| image.repository.clone())
        .collect::<Vec<_>>();
    prepare_ecr_registry(region, &deployment.name, &repositories)
        .await
        .with_context(|| {
            format!(
                "failed to prepare ECR in {region}; verify AWS credentials and ECR repository permissions"
            )
        })
}

async fn dagger_session() -> Result<dagger_sdk::Client> {
    let config = dagger_sdk::ClientConfig::builder()
        .isolated_cli_session()
        .session_startup_timeout(DAGGER_SESSION_STARTUP_TIMEOUT);
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    let config = {
        require_pinned_runner().await?;
        config.runner_host(tokeira_build::DAGGER_RELEASE.runner_host())
    };
    dagger_sdk::connect_with(
        config
            .build()
            .context("configure the isolated Dagger image session")?,
    )
    .await
    .context("failed to connect the Dagger image session")
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
async fn require_pinned_runner() -> Result<()> {
    let output = tokio::process::Command::new("docker")
        .args([
            "container",
            "inspect",
            "--format",
            "{{.State.Running}} {{.Config.Image}}",
            tokeira_build::DAGGER_RELEASE.container,
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .context("could not inspect the checksum-verified Dagger runner")?;
    let expected = format!("true {}\n", tokeira_build::DAGGER_RELEASE.image);
    if !output.status.success() || output.stdout != expected.as_bytes() {
        bail!(
            "the checksum-verified Apple Silicon Dagger runner is not active; run `tkr image build` once, then retry"
        );
    }
    Ok(())
}

async fn publish_builds(
    dagger: &dagger_sdk::Client,
    authorization: &tokeira_aws::EcrAuthorization,
    images: &[&ResolvedImage],
    tag: &str,
) -> Result<Vec<PublishedImage>> {
    let mut published = Vec::with_capacity(images.len());
    for image in images {
        let latest_ref = authorization.image_ref(&image.repository, "latest");
        let mut remote_refs = vec![latest_ref.clone()];
        let effective_ref = if tag == "latest" {
            latest_ref
        } else {
            let versioned = authorization.image_ref(&image.repository, tag);
            remote_refs.push(versioned.clone());
            versioned
        };
        let result = publish_image(
            &PublishRequest {
                local_image: image.local_ref.clone().ok_or_else(|| {
                    anyhow::anyhow!("build image `{}` has no local reference", image.name)
                })?,
                remote_refs: remote_refs.clone(),
                registry_host: authorization.registry_host.clone(),
                username: authorization.username.clone(),
                password: RegistryPassword::new(authorization.password()),
            },
            dagger,
        )
        .await?;
        let digest = one_digest(
            &image.name,
            result
                .published
                .iter()
                .map(|reference| reference.published_ref.as_str()),
        )?;
        published.push(PublishedImage {
            name: image.name.clone(),
            resolved_ref: effective_ref,
            digest,
            published_refs: remote_refs,
            skipped: false,
        });
    }
    Ok(published)
}

async fn mirror_upstreams(
    dagger: &dagger_sdk::Client,
    authorization: &tokeira_aws::EcrAuthorization,
    images: &[&ResolvedImage],
) -> Result<Vec<PublishedImage>> {
    let mut published = Vec::with_capacity(images.len());
    for image in images {
        let source_ref = image.upstream_ref.as_ref().ok_or_else(|| {
            anyhow::anyhow!("mirror image `{}` has no upstream reference", image.name)
        })?;
        let destination_ref = authorization.image_ref(&image.repository, &image.tag);
        if source_ref == &destination_ref {
            bail!(
                "image '{}' already names its tagged ECR destination and provides no immutable digest; author an upstream source or a digest-pinned destination",
                image.name
            );
        }
        let result = mirror_image(
            &MirrorRequest {
                source_ref: source_ref.clone(),
                remote_ref: destination_ref.clone(),
                registry_host: authorization.registry_host.clone(),
                username: authorization.username.clone(),
                password: RegistryPassword::new(authorization.password()),
            },
            dagger,
        )
        .await?;
        published.push(PublishedImage {
            name: image.name.clone(),
            resolved_ref: destination_ref.clone(),
            digest: digest_from_published_ref(&result.published_ref)?,
            published_refs: vec![destination_ref],
            skipped: false,
        });
    }
    Ok(published)
}

fn one_digest<'a>(image: &str, references: impl IntoIterator<Item = &'a str>) -> Result<String> {
    let mut digest = None;
    for reference in references {
        let candidate = digest_from_published_ref(reference)?;
        if digest.as_ref().is_some_and(|digest| digest != &candidate) {
            bail!(
                "published tags for image `{image}` resolved to different digests; refusing an ambiguous publication result"
            );
        }
        digest = Some(candidate);
    }
    digest.ok_or_else(|| anyhow::anyhow!("image `{image}` publication returned no references"))
}

fn digest_from_published_ref(reference: &str) -> Result<String> {
    let (_, digest) = reference.rsplit_once('@').ok_or_else(|| {
        anyhow::anyhow!("registry publication returned no digest-pinned reference: `{reference}`")
    })?;
    let Some(hex) = digest.strip_prefix("sha256:") else {
        bail!("registry publication returned unsupported digest `{digest}");
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("registry publication returned invalid SHA-256 digest `{digest}`");
    }
    Ok(digest.to_ascii_lowercase())
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
    fn image_filter_refuses_wrong_source_before_provider_work() {
        let images = vec![
            fixture_image("runtime", ImageSourceType::Build),
            fixture_image("metrics", ImageSourceType::Mirror),
        ];
        let error = select_images(&images, ImageSourceType::Build, Some("metrics"))
            .expect_err("a mirror cannot satisfy a build selection")
            .to_string();
        assert_eq!(
            error,
            "unknown build image `metrics`; valid build images are: runtime"
        );
        assert_eq!(
            select_images(&images, ImageSourceType::Mirror, None)
                .expect("all mirrors")
                .len(),
            1
        );
    }

    fn fixture_image(name: &str, source_type: ImageSourceType) -> ResolvedImage {
        ResolvedImage {
            name: name.into(),
            source_type,
            repository: format!("fixture/{name}"),
            tag: "latest".into(),
            upstream_ref: (source_type == ImageSourceType::Mirror)
                .then(|| format!("upstream/{name}:latest")),
            local_ref: (source_type == ImageSourceType::Build).then(|| format!("{name}:latest")),
        }
    }

    #[test]
    fn publication_digest_is_required_and_normalized() {
        let upper = "A".repeat(64);
        assert_eq!(
            digest_from_published_ref(&format!("example.invalid/repo@sha256:{upper}"))
                .expect("valid digest"),
            format!("sha256:{}", "a".repeat(64))
        );
        assert!(digest_from_published_ref("example.invalid/repo:latest").is_err());
        assert!(digest_from_published_ref("example.invalid/repo@sha512:abcd").is_err());
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
