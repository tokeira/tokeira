//! ECS image registry.

pub mod ensure;
pub mod observability;
pub mod tokeirad;

pub use ensure::ensure_ecr_repositories_from_images;

use tokeira_deploy_engine::{Image, ImageContext, RuntimeError, validate_registry};

/// Definition-derived inputs needed to resolve the ECS image registry.
///
/// This is intentionally narrower than [`crate::EcsConfig`]. Image
/// publication owns repository identity and upstream image selection; it
/// must not rebuild unrelated workload policy from legacy defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsImageConfig {
    /// Deployment name used as the ECR repository prefix.
    pub project_name: String,
    /// Upstream Grafana Mimir reference authored by the definition.
    pub mimir_image: String,
    /// Upstream Grafana Loki reference authored by the definition.
    pub loki_image: String,
    /// Upstream Grafana reference authored by the definition.
    pub grafana_image: String,
    /// Upstream Grafana Alloy reference authored by the definition.
    pub alloy_image: String,
    /// Upstream AWS CLI reference authored by the definition.
    pub aws_cli_image: String,
    /// Upstream BusyBox reference authored by the definition.
    pub busybox_image: String,
}

pub(crate) fn construct() -> Vec<Box<dyn Image>> {
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

    fn image_config() -> EcsImageConfig {
        let config = crate::config::EcsConfig::default();
        EcsImageConfig {
            project_name: config.project_name,
            mimir_image: config.observability.mimir_image,
            loki_image: config.observability.loki_image,
            grafana_image: config.observability.grafana_image,
            alloy_image: config.observability.alloy_image,
            aws_cli_image: config.observability.aws_cli_image,
            busybox_image: config.observability.busybox_image,
        }
    }

    #[test]
    fn ecs_image_refs_and_writeback_targets_resolve_from_config() {
        let mut ctx = ImageContext::default();
        let config = image_config();
        ctx.set_extension(config.clone());

        let images = all(&ctx).expect("ecs images");
        let tokeirad = images
            .iter()
            .find(|image| image.name() == "tokeirad")
            .expect("tokeirad image");

        assert_eq!(tokeirad.source_type(), ImageSourceType::Build);
        assert_eq!(
            tokeirad.desired_ref(&ctx).expect("desired ref"),
            DesiredImageRef {
                repository: format!("{}/tokeirad", config.project_name),
                tag: "latest".to_owned(),
                upstream_ref: None,
            }
        );
        assert_eq!(
            tokeirad.writeback_targets(&ctx),
            vec![
                WritebackTarget {
                    field: "services.edge_api.image",
                },
                WritebackTarget {
                    field: "services.edge_poll.image",
                },
                WritebackTarget {
                    field: "services.runtime.image",
                },
                WritebackTarget {
                    field: "services.projection.image",
                },
                WritebackTarget {
                    field: "services.controller.image",
                },
                WritebackTarget {
                    field: "services.autoscaler.image",
                },
                WritebackTarget {
                    field: "services.admin.image",
                },
            ]
        );

        assert_mirror(
            &images,
            &ctx,
            "grafana-mimir",
            "mimir",
            &config.mimir_image,
            &config.project_name,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana-loki",
            "loki",
            &config.loki_image,
            &config.project_name,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana",
            "grafana",
            &config.grafana_image,
            &config.project_name,
        );
        assert_mirror(
            &images,
            &ctx,
            "grafana-alloy",
            "alloy",
            &config.alloy_image,
            &config.project_name,
        );
        assert_mirror(
            &images,
            &ctx,
            "aws-cli",
            "aws-cli",
            &config.aws_cli_image,
            &config.project_name,
        );
        assert_mirror(
            &images,
            &ctx,
            "busybox",
            "busybox",
            &config.busybox_image,
            &config.project_name,
        );
    }

    #[test]
    fn empty_upstream_ref_returns_operator_error() {
        let mut ctx = ImageContext::default();
        let mut config = image_config();
        config.mimir_image.clear();
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

    fn assert_mirror(
        images: &[Box<dyn Image>],
        ctx: &ImageContext,
        name: &str,
        suffix: &str,
        upstream: &str,
        project: &str,
    ) {
        let image = images
            .iter()
            .find(|image| image.name() == name)
            .expect("mirror image");
        assert_eq!(image.source_type(), ImageSourceType::Mirror);
        assert_eq!(
            image.desired_ref(ctx).expect("desired ref"),
            DesiredImageRef {
                repository: format!("{project}/{suffix}"),
                tag: image_tag(upstream),
                upstream_ref: Some(upstream.to_owned()),
            }
        );
    }
}
