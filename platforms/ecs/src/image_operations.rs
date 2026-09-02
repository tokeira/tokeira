//! Definition-bound ECS image publication.
//!
//! This module owns every ECS-specific decision behind the generic shell:
//! re-evaluating the admitted definition, selecting ECR repository names,
//! preparing those repositories, acquiring short-lived ECR authorization,
//! and running the Dagger publish/mirror pipelines. Credentials never leave
//! this module, and image commands never read or mutate deployment state.

use std::{collections::HashMap, process::Stdio, time::Duration};

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use tokeira_aws::{AwsClients, EcrClient as _};
use tokeira_build::{MirrorRequest, PublishRequest, RegistryPassword, mirror_image, publish_image};
use tokeira_deploy_engine::{Image, ImageContext, ImageSourceType};
use tokeira_ecs::images::{EcsImageConfig, ensure_ecr_repositories_from_images};
use tokeira_iac::ProvisionContext;
use tokeira_platform::{
    author::from_located_value,
    declaration::{DeclaredImage, DeploymentRef, ImageOperations, PublishedImage},
};

use crate::ops::evaluated_configuration;

const DAGGER_SESSION_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
struct ImageConfiguration {
    aws: ImageAwsConfiguration,
    observability: ImageObservabilityConfiguration,
}

#[derive(Debug, Deserialize)]
struct ImageAwsConfiguration {
    region: String,
}

#[derive(Debug, Deserialize)]
struct ImageObservabilityConfiguration {
    mimir: ImagePolicy,
    loki: ImagePolicy,
    grafana: ImagePolicy,
    alloy_image: String,
    aws_cli_image: String,
    busybox_image: String,
}

#[derive(Debug, Deserialize)]
struct ImagePolicy {
    image: String,
}

#[derive(Debug)]
struct ResolvedImages {
    region: String,
    context: ImageContext,
    images: Vec<Box<dyn Image>>,
}

/// ECS implementation of the optional definition-bound image capability.
#[derive(Debug)]
pub(crate) struct EcsImageOperations;

#[async_trait::async_trait]
impl ImageOperations for EcsImageOperations {
    fn list(&self, deployment: &DeploymentRef) -> Result<Vec<DeclaredImage>> {
        let resolved = resolve_images(deployment)?;
        declared_images(&resolved.images, &resolved.context)
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

        // This cheap host check precedes credentials, repository mutations,
        // and Dagger startup. A missing build is an operator workflow issue,
        // not an AWS failure, and must not leave partial provider effects.
        for image in &selected {
            let local_ref = format!("{}:latest", image.name());
            if !local_image_exists(&local_ref).await? {
                bail!(
                    "local image `{local_ref}` is missing; run `tkr image build` before `tkr image push --yes`"
                );
            }
        }

        let clients = AwsClients::load(Some(&resolved.region)).await;
        let ecr = clients.ecr_client();
        let authorization = ecr
            .get_authorization_token()
            .await
            .with_context(|| {
                format!(
                    "failed to authenticate with ECR in {}; verify AWS credentials and ecr:GetAuthorizationToken permission",
                    resolved.region
                )
            })?;
        ensure_repositories(deployment, &ecr, &selected, &resolved.context).await?;

        let dagger = dagger_session().await?;
        let outcome =
            publish_builds(&dagger, &authorization, &selected, &resolved.context, tag).await;
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
        let clients = AwsClients::load(Some(&resolved.region)).await;
        let ecr = clients.ecr_client();
        let authorization = ecr
            .get_authorization_token()
            .await
            .with_context(|| {
                format!(
                    "failed to authenticate with ECR in {}; verify AWS credentials and ecr:GetAuthorizationToken permission",
                    resolved.region
                )
            })?;
        ensure_repositories(deployment, &ecr, &selected, &resolved.context).await?;

        let dagger = dagger_session().await?;
        let outcome = mirror_upstreams(&dagger, &authorization, &selected, &resolved.context).await;
        let close = dagger
            .close()
            .await
            .context("close the Dagger image session");
        let published = outcome?;
        close?;
        Ok(published)
    }
}

fn resolve_images(deployment: &DeploymentRef) -> Result<ResolvedImages> {
    let authored: ImageConfiguration = from_located_value(evaluated_configuration(deployment)?)
        .context("admitted ECS definition has no usable image configuration")?;
    let region = required(authored.aws.region, "aws.region")?;
    let mut context = ImageContext::default();
    context.set_extension(EcsImageConfig {
        project_name: deployment.name.clone(),
        mimir_image: required(
            authored.observability.mimir.image,
            "observability.mimir.image",
        )?,
        loki_image: required(
            authored.observability.loki.image,
            "observability.loki.image",
        )?,
        grafana_image: required(
            authored.observability.grafana.image,
            "observability.grafana.image",
        )?,
        alloy_image: required(
            authored.observability.alloy_image,
            "observability.alloy_image",
        )?,
        aws_cli_image: required(
            authored.observability.aws_cli_image,
            "observability.aws_cli_image",
        )?,
        busybox_image: required(
            authored.observability.busybox_image,
            "observability.busybox_image",
        )?,
    });
    let images = tokeira_ecs::images::all(&context).map_err(anyhow::Error::new)?;
    Ok(ResolvedImages {
        region,
        context,
        images,
    })
}

fn required(value: String, path: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("admitted ECS definition has an empty `{path}`");
    }
    Ok(value)
}

fn declared_images(images: &[Box<dyn Image>], ctx: &ImageContext) -> Result<Vec<DeclaredImage>> {
    images
        .iter()
        .map(|image| {
            let desired = image.desired_ref(ctx).map_err(anyhow::Error::new)?;
            Ok(DeclaredImage {
                name: image.name().to_string(),
                source_type: image.source_type(),
                repository: desired.repository,
                tag: desired.tag,
                upstream_ref: desired.upstream_ref,
            })
        })
        .collect()
}

fn select_images<'a>(
    images: &'a [Box<dyn Image>],
    source_type: ImageSourceType,
    selected_name: Option<&str>,
) -> Result<Vec<&'a dyn Image>> {
    let candidates = images
        .iter()
        .filter(|image| image.source_type() == source_type)
        .map(Box::as_ref)
        .collect::<Vec<_>>();
    let Some(selected_name) = selected_name else {
        return Ok(candidates);
    };
    if let Some(image) = candidates
        .iter()
        .find(|image| image.name() == selected_name)
    {
        return Ok(vec![*image]);
    }
    let mut valid = candidates
        .iter()
        .map(|image| image.name())
        .collect::<Vec<_>>();
    valid.sort_unstable();
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

async fn ensure_repositories(
    deployment: &DeploymentRef,
    ecr: &dyn tokeira_aws::EcrClient,
    images: &[&dyn Image],
    image_ctx: &ImageContext,
) -> Result<()> {
    // Definition graphs do not author arbitrary provider tags. The same
    // ProvisionContext tag derivation as infrastructure apply still supplies
    // Name, Project, and ManagedBy, allowing its EcrRepository resources to
    // adopt repositories created by this pre-infrastructure command cleanly.
    let context = ProvisionContext::new(&deployment.name, HashMap::new());
    ensure_ecr_repositories_from_images(ecr, &context, images, image_ctx)
        .await
        .context("prepare deployment ECR repositories")
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
    images: &[&dyn Image],
    image_ctx: &ImageContext,
    tag: &str,
) -> Result<Vec<PublishedImage>> {
    let mut published = Vec::with_capacity(images.len());
    for image in images {
        let desired = image.desired_ref(image_ctx).map_err(anyhow::Error::new)?;
        let latest_ref = format!(
            "{}/{repository}:latest",
            authorization.registry_host,
            repository = desired.repository
        );
        let mut remote_refs = vec![latest_ref.clone()];
        let effective_ref = if tag == "latest" {
            latest_ref
        } else {
            let versioned = format!(
                "{}/{repository}:{tag}",
                authorization.registry_host,
                repository = desired.repository
            );
            remote_refs.push(versioned.clone());
            versioned
        };
        let result = publish_image(
            &PublishRequest {
                local_image: format!("{}:latest", image.name()),
                remote_refs: remote_refs.clone(),
                registry_host: authorization.registry_host.clone(),
                username: authorization.username.clone(),
                password: RegistryPassword::new(&authorization.password),
            },
            dagger,
        )
        .await?;
        let digest = one_digest(
            image.name(),
            result
                .published
                .iter()
                .map(|reference| reference.published_ref.as_str()),
        )?;
        published.push(PublishedImage {
            name: image.name().to_string(),
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
    images: &[&dyn Image],
    image_ctx: &ImageContext,
) -> Result<Vec<PublishedImage>> {
    let mut published = Vec::with_capacity(images.len());
    for image in images {
        let desired = image.desired_ref(image_ctx).map_err(anyhow::Error::new)?;
        let source_ref = desired.upstream_ref.ok_or_else(|| {
            anyhow::anyhow!(
                "image '{}' is Mirror but desired_ref.upstream_ref is None",
                image.name()
            )
        })?;
        let destination_ref = format!(
            "{}/{repository}:{tag}",
            authorization.registry_host,
            repository = desired.repository,
            tag = desired.tag
        );
        if source_ref == destination_ref {
            bail!(
                "image '{}' already names its tagged ECR destination and provides no immutable digest; author an upstream source or a digest-pinned destination",
                image.name()
            );
        }
        let result = mirror_image(
            &MirrorRequest {
                source_ref: source_ref.clone(),
                remote_ref: destination_ref.clone(),
                registry_host: authorization.registry_host.clone(),
                username: authorization.username.clone(),
                password: RegistryPassword::new(&authorization.password),
            },
            dagger,
        )
        .await?;
        published.push(PublishedImage {
            name: image.name().to_string(),
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
        bail!("registry publication returned unsupported digest `{digest}`");
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

            assert_eq!(images.len(), 7);
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
    fn image_filter_refuses_wrong_source_or_unknown_name_before_provider_work() {
        let mut context = ImageContext::default();
        context.set_extension(EcsImageConfig {
            project_name: "fixture".into(),
            mimir_image: "grafana/mimir:3.2.0".into(),
            loki_image: "grafana/loki:3.7.6".into(),
            grafana_image: "grafana/grafana:12.4.9".into(),
            alloy_image: "grafana/alloy:v1.19.0".into(),
            aws_cli_image: "amazon/aws-cli:2.17.0".into(),
            busybox_image: "busybox:1.36".into(),
        });
        let images = tokeira_ecs::images::all(&context).expect("image declarations");

        let error = select_images(&images, ImageSourceType::Build, Some("grafana"))
            .expect_err("a mirror cannot satisfy a build selection")
            .to_string();
        assert_eq!(
            error,
            "unknown build image `grafana`; valid build images are: tokeirad"
        );
        assert_eq!(
            select_images(&images, ImageSourceType::Mirror, None)
                .expect("all mirrors")
                .len(),
            6
        );
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
