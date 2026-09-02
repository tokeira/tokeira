//! ECS image-publication gates.
//!
//! These checks refuse infrastructure or workload use until every image field
//! that a build or mirror operation owns points at the deployment's ECR
//! registry. They validate ownership metadata without performing mutations.

use tokeira_deploy_engine::{Image, ImageContext, ImageSourceType};

use crate::EcsConfig;

#[derive(Debug, thiserror::Error)]
pub enum EcsError {
    #[error("image writeback fields are not mirrored into ECR: {fields:?}; {remediation}")]
    UnmirroredImages {
        fields: Vec<&'static str>,
        remediation: String,
    },
    #[error("image writeback fields are not pushed into ECR: {fields:?}; {remediation}")]
    UnpushedImages {
        fields: Vec<&'static str>,
        remediation: String,
    },
}

pub fn validate_mirrors(
    cfg: &EcsConfig,
    registry: &str,
    images: &[Box<dyn Image>],
    ctx: &ImageContext,
) -> Result<(), EcsError> {
    validate_writeback_fields(
        cfg,
        registry,
        images,
        ctx,
        ImageSourceType::Mirror,
        |fields| EcsError::UnmirroredImages {
            fields,
            remediation: "run `tkr image mirror`".into(),
        },
    )
}

pub fn validate_builds(
    cfg: &EcsConfig,
    registry: &str,
    images: &[Box<dyn Image>],
    ctx: &ImageContext,
) -> Result<(), EcsError> {
    validate_writeback_fields(
        cfg,
        registry,
        images,
        ctx,
        ImageSourceType::Build,
        |fields| EcsError::UnpushedImages {
            fields,
            remediation: "run `tkr image push --tag <version>`".into(),
        },
    )
}

fn validate_writeback_fields<F>(
    cfg: &EcsConfig,
    registry: &str,
    images: &[Box<dyn Image>],
    ctx: &ImageContext,
    source_type: ImageSourceType,
    error: F,
) -> Result<(), EcsError>
where
    F: FnOnce(Vec<&'static str>) -> EcsError,
{
    let registry_prefix = format!("{registry}/");
    let mut invalid = Vec::new();
    for image in images
        .iter()
        .filter(|image| image.source_type() == source_type)
    {
        for target in image.writeback_targets(ctx) {
            let value = read_dotted_key(cfg, target.field).unwrap_or_default();
            if value.is_empty() || !value.starts_with(&registry_prefix) {
                invalid.push(target.field);
            }
        }
    }
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(error(invalid))
    }
}

fn read_dotted_key<'a>(cfg: &'a EcsConfig, field: &str) -> Option<&'a str> {
    match field {
        "services.edge_api.image" => Some(&cfg.services.edge_api.image),
        "services.edge_poll.image" => Some(&cfg.services.edge_poll.image),
        "services.runtime.image" => Some(&cfg.services.runtime.image),
        "services.projection.image" => Some(&cfg.services.projection.image),
        "services.controller.image" => Some(&cfg.services.controller.image),
        "services.autoscaler.image" => Some(&cfg.services.autoscaler.image),
        "services.admin.image" => Some(&cfg.services.admin.image),
        "observability.mimir_image" => Some(&cfg.observability.mimir_image),
        "observability.loki_image" => Some(&cfg.observability.loki_image),
        "observability.grafana_image" => Some(&cfg.observability.grafana_image),
        "observability.alloy_image" => Some(&cfg.observability.alloy_image),
        "observability.aws_cli_image" => Some(&cfg.observability.aws_cli_image),
        "observability.busybox_image" => Some(&cfg.observability.busybox_image),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::images;

    const REGISTRY: &str = "123456789012.dkr.ecr.us-east-1.amazonaws.com";

    #[test]
    fn validate_mirrors_rejects_default_upstream_refs() {
        let (config, image_ctx, images) = fixtures();

        let err = validate_mirrors(&config, REGISTRY, &images, &image_ctx)
            .expect_err("defaults are upstream refs");

        assert!(matches!(err, EcsError::UnmirroredImages { .. }));
    }

    #[test]
    fn validate_builds_rejects_default_local_refs() {
        let (config, image_ctx, images) = fixtures();

        let err = validate_builds(&config, REGISTRY, &images, &image_ctx)
            .expect_err("defaults are local refs");

        assert!(matches!(err, EcsError::UnpushedImages { .. }));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn mirror_gate_matches_observability_writeback_state(valid in any::<bool>()) {
            let (mut config, image_ctx, images) = fixtures();
            set_observability_refs(&mut config, valid);

            let result = validate_mirrors(&config, REGISTRY, &images, &image_ctx);

            prop_assert_eq!(result.is_ok(), valid);
        }

        #[test]
        fn build_gate_matches_service_writeback_state(valid in any::<bool>()) {
            let (mut config, image_ctx, images) = fixtures();
            set_service_refs(&mut config, valid);

            let result = validate_builds(&config, REGISTRY, &images, &image_ctx);

            prop_assert_eq!(result.is_ok(), valid);
        }
    }

    fn fixtures() -> (EcsConfig, ImageContext, Vec<Box<dyn Image>>) {
        let config = EcsConfig::default();
        let mut image_ctx = ImageContext::default();
        image_ctx.set_extension(config.clone());
        let images = images::all(&image_ctx).expect("image registry");
        (config, image_ctx, images)
    }

    fn set_observability_refs(config: &mut EcsConfig, valid: bool) {
        let prefix = if valid { REGISTRY } else { "docker.io" };
        config.observability.mimir_image = format!("{prefix}/tokeira/mimir:3.2.0");
        config.observability.loki_image = format!("{prefix}/tokeira/loki:3.7.6");
        config.observability.grafana_image = format!("{prefix}/tokeira/grafana:12.4.9");
        config.observability.alloy_image = format!("{prefix}/tokeira/alloy:v1.19.0");
        config.observability.aws_cli_image = format!("{prefix}/tokeira/aws-cli:2.17.0");
        config.observability.busybox_image = format!("{prefix}/tokeira/busybox:1.36");
    }

    fn set_service_refs(config: &mut EcsConfig, valid: bool) {
        let prefix = if valid { REGISTRY } else { "docker.io" };
        config.services.edge_api.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.edge_poll.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.runtime.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.projection.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.controller.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.autoscaler.image = format!("{prefix}/tokeira/tokeirad:latest");
        config.services.admin.image = format!("{prefix}/tokeira/tokeirad:latest");
    }
}
