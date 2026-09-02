//! Mirrored third-party observability image declarations.
//!
//! Each declaration keeps the upstream image as input while deriving a
//! deployment-owned ECR repository and an explicit configuration writeback
//! target.

use tokeira_deploy_engine::{
    DesiredImageRef, Image, ImageContext, ImageSourceType, RuntimeError, WritebackTarget,
};

use crate::{
    config::EcsConfig,
    images::{image_tag, missing_config_error},
};

macro_rules! mirror_image {
    ($struct_name:ident, $name:literal, $repo_suffix:literal, $field:ident, $target:literal) => {
        #[derive(Debug)]
        pub struct $struct_name;

        impl Image for $struct_name {
            fn name(&self) -> &str {
                $name
            }

            fn source_type(&self) -> ImageSourceType {
                ImageSourceType::Mirror
            }

            fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError> {
                let cfg = ctx
                    .extension::<EcsConfig>()
                    .ok_or_else(missing_config_error::<EcsConfig>)?;
                let upstream = cfg.observability.$field.clone();
                if upstream.is_empty() {
                    return Err(RuntimeError::Image(format!(
                        "image '{}' has empty upstream_ref in config",
                        $name
                    )));
                }
                Ok(DesiredImageRef {
                    repository: format!("{}/{}", cfg.project_name, $repo_suffix),
                    tag: image_tag(&upstream),
                    upstream_ref: Some(upstream),
                })
            }

            fn writeback_targets(&self, _ctx: &ImageContext) -> Vec<WritebackTarget> {
                vec![WritebackTarget { field: $target }]
            }
        }
    };
}

mirror_image!(
    MimirImage,
    "grafana-mimir",
    "mimir",
    mimir_image,
    "observability.mimir_image"
);
mirror_image!(
    LokiImage,
    "grafana-loki",
    "loki",
    loki_image,
    "observability.loki_image"
);
mirror_image!(
    GrafanaImage,
    "grafana",
    "grafana",
    grafana_image,
    "observability.grafana_image"
);
mirror_image!(
    AlloyImage,
    "grafana-alloy",
    "alloy",
    alloy_image,
    "observability.alloy_image"
);
mirror_image!(
    AwsCliImage,
    "aws-cli",
    "aws-cli",
    aws_cli_image,
    "observability.aws_cli_image"
);
mirror_image!(
    BusyBoxImage,
    "busybox",
    "busybox",
    busybox_image,
    "observability.busybox_image"
);

pub(crate) fn all() -> Vec<Box<dyn Image>> {
    vec![
        Box::new(MimirImage),
        Box::new(LokiImage),
        Box::new(GrafanaImage),
        Box::new(AlloyImage),
        Box::new(AwsCliImage),
        Box::new(BusyBoxImage),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ObservabilityConfig;

    #[test]
    fn mirror_stability_matches_default_config() {
        let mut ctx = ImageContext::default();
        let config = EcsConfig::default();
        ctx.set_extension(config.clone());

        assert_upstream(MimirImage, &ctx, &config.observability, |obs| {
            &obs.mimir_image
        });
        assert_upstream(LokiImage, &ctx, &config.observability, |obs| {
            &obs.loki_image
        });
        assert_upstream(GrafanaImage, &ctx, &config.observability, |obs| {
            &obs.grafana_image
        });
        assert_upstream(AlloyImage, &ctx, &config.observability, |obs| {
            &obs.alloy_image
        });
        assert_upstream(AwsCliImage, &ctx, &config.observability, |obs| {
            &obs.aws_cli_image
        });
        assert_upstream(BusyBoxImage, &ctx, &config.observability, |obs| {
            &obs.busybox_image
        });
    }

    fn assert_upstream<I>(
        image: I,
        ctx: &ImageContext,
        observability: &ObservabilityConfig,
        expected: fn(&ObservabilityConfig) -> &String,
    ) where
        I: Image,
    {
        assert_eq!(
            image
                .desired_ref(ctx)
                .expect("desired ref")
                .upstream_ref
                .expect("upstream ref"),
            *expected(observability)
        );
    }
}
