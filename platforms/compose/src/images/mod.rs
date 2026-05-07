//! Compose image registry.

pub mod observability;
pub mod tokeirad;

use tokeira_deploy_engine::{Image, ImageContext, RuntimeError, validate_registry};

pub fn construct() -> Vec<Box<dyn Image>> {
    let mut images = tokeirad::all();
    images.extend(observability::all());
    images
}

pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError> {
    let images = construct();
    validate_registry(&images, ctx)?;
    Ok(images)
}

pub(crate) fn image_tag(upstream: &str) -> String {
    let without_digest = upstream.split('@').next().unwrap_or(upstream);
    let last_slash = without_digest.rfind('/');
    let last_colon = without_digest.rfind(':');
    match last_colon {
        Some(colon) if last_slash.is_none_or(|slash| colon > slash) => {
            without_digest[colon + 1..].to_owned()
        }
        _ => "latest".to_owned(),
    }
}

pub(crate) fn missing_config_error<T>() -> RuntimeError {
    RuntimeError::Image(format!(
        "image context missing extension: {}",
        std::any::type_name::<T>()
    ))
}

#[cfg(test)]
mod tests {
    use tokeira_deploy_engine::{DesiredImageRef, ImageSourceType, WritebackTarget};

    use super::*;
    use crate::config::ComposeConfig;

    #[test]
    fn compose_image_refs_and_writeback_targets_resolve_from_config() {
        let mut ctx = ImageContext::default();
        let config = ComposeConfig::default();
        ctx.set_extension(config.clone());

        let images = all(&ctx).expect("compose images");
        let tokeirad = images
            .iter()
            .find(|image| image.name() == "tokeirad")
            .expect("tokeirad image");

        assert_eq!(tokeirad.source_type(), ImageSourceType::Build);
        assert_eq!(
            tokeirad.desired_ref(&ctx).expect("desired ref"),
            DesiredImageRef {
                repository: "tokeira/tokeirad".to_owned(),
                tag: "latest".to_owned(),
                upstream_ref: None,
            }
        );
        assert_eq!(
            tokeirad.writeback_targets(&ctx),
            vec![WritebackTarget {
                field: "tokeirad.image"
            }]
        );

        assert_mirror(
            &images,
            &ctx,
            "grafana-mimir",
            "tokeira/grafana-mimir",
            &config.observability.mimir_image,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana-loki",
            "tokeira/grafana-loki",
            &config.observability.loki_image,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana",
            "tokeira/grafana",
            &config.observability.grafana_image,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana-alloy",
            "tokeira/grafana-alloy",
            &config.observability.alloy_image,
        );
        assert_mirror(
            &images,
            &ctx,
            "aws-cli",
            "tokeira/aws-cli",
            &config.observability.aws_cli_image,
        );
        assert_mirror(
            &images,
            &ctx,
            "busybox",
            "tokeira/busybox",
            &config.observability.busybox_image,
        );
    }

    #[test]
    fn image_tag_handles_digest_missing_tag_and_registry_port() {
        assert_eq!(image_tag("repo@sha256:abc"), "latest");
        assert_eq!(image_tag("repo:1.2.3@sha256:abc"), "1.2.3");
        assert_eq!(image_tag("localhost:5000/path/repo:tag"), "tag");
        assert_eq!(image_tag("localhost:5000/path/repo"), "latest");
    }

    #[test]
    fn empty_upstream_ref_returns_operator_error() {
        let mut ctx = ImageContext::default();
        let mut config = ComposeConfig::default();
        config.observability.mimir_image.clear();
        ctx.set_extension(config);

        let image = observability::MimirImage;
        let err = image
            .desired_ref(&ctx)
            .expect_err("empty upstream must fail");

        assert_eq!(
            err.to_string(),
            "Image error: image 'grafana-mimir' has empty upstream_ref in config"
        );
    }

    #[test]
    fn missing_observability_fields_deserialize_to_defaults() {
        let toml = r#"
project_name = "tokeira"

[tokeirad]
image = "tokeirad:latest"
grpc_port = 7233
metrics_port = 9090
replicas = 1

[observability]
mimir_image = "grafana/mimir:3.0.6"
mimir_replicas = 1
grafana_image = "grafana/grafana-oss:12.4.3"
grafana_replicas = 1
loki_image = "grafana/loki:3.7.1"
loki_replicas = 1
alloy_image = "grafana/alloy:v1.16.0"
alloy_replicas = 1
grafana_port = 3000
"#;

        let config: ComposeConfig = toml::from_str(toml).expect("compose config");

        assert_eq!(
            config.observability.aws_cli_image,
            "public.ecr.aws/aws-cli/aws-cli:latest"
        );
        assert_eq!(
            config.observability.busybox_image,
            "public.ecr.aws/docker/library/busybox:latest"
        );
    }

    fn assert_mirror(
        images: &[Box<dyn Image>],
        ctx: &ImageContext,
        name: &str,
        repository: &str,
        upstream: &str,
    ) {
        let image = images
            .iter()
            .find(|image| image.name() == name)
            .expect("mirror image");
        assert_eq!(image.source_type(), ImageSourceType::Mirror);
        assert_eq!(
            image.desired_ref(ctx).expect("desired ref"),
            DesiredImageRef {
                repository: repository.to_owned(),
                tag: image_tag(upstream),
                upstream_ref: Some(upstream.to_owned()),
            }
        );
    }
}
