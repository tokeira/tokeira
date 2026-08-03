//! Platform-owned image inventory for Compose services.

use tokeira_deploy_engine::{
    DesiredImageRef, Image, ImageContext, ImageSourceType, RuntimeError, WritebackTarget,
    validate_registry,
};

use crate::config::ComposeConfig;

#[derive(Debug)]
struct ConfiguredImage {
    name: &'static str,
    repository: &'static str,
    field: &'static str,
    source: ImageSourceType,
    upstream: Option<fn(&ComposeConfig) -> &str>,
}

impl Image for ConfiguredImage {
    fn name(&self) -> &str {
        self.name
    }

    fn source_type(&self) -> ImageSourceType {
        self.source
    }

    fn desired_ref(&self, context: &ImageContext) -> Result<DesiredImageRef, RuntimeError> {
        let config = context.extension::<ComposeConfig>().ok_or_else(|| {
            RuntimeError::Image("image context is missing ComposeConfig".to_string())
        })?;
        let upstream = self.upstream.map(|select| select(config));
        if upstream.is_some_and(str::is_empty) {
            return Err(RuntimeError::Image(format!(
                "image '{}' has an empty upstream reference",
                self.name
            )));
        }
        Ok(DesiredImageRef {
            repository: self.repository.to_string(),
            tag: upstream
                .map(image_tag)
                .unwrap_or_else(|| "latest".to_string()),
            upstream_ref: upstream.map(str::to_string),
        })
    }

    fn writeback_targets(&self, _context: &ImageContext) -> Vec<WritebackTarget> {
        vec![WritebackTarget { field: self.field }]
    }
}

/// Construct the concrete inventory owned by the Compose platform.
pub fn construct() -> Vec<Box<dyn Image>> {
    vec![
        Box::new(ConfiguredImage {
            name: "tokeirad",
            repository: "tokeira/tokeirad",
            field: "tokeirad.image",
            source: ImageSourceType::Build,
            upstream: None,
        }),
        mirror(
            "grafana-mimir",
            "tokeira/grafana-mimir",
            "observability.mimir.image",
            |config| &config.observability.mimir.image,
        ),
        mirror(
            "grafana-loki",
            "tokeira/grafana-loki",
            "observability.loki.image",
            |config| &config.observability.loki.image,
        ),
        mirror(
            "grafana",
            "tokeira/grafana",
            "observability.grafana.image",
            |config| &config.observability.grafana.image,
        ),
        mirror(
            "grafana-alloy",
            "tokeira/grafana-alloy",
            "observability.alloy.image",
            |config| &config.observability.alloy.image,
        ),
    ]
}

/// Validate and return the platform inventory for one admitted config.
pub fn all(context: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError> {
    let images = construct();
    validate_registry(&images, context)?;
    Ok(images)
}

fn mirror(
    name: &'static str,
    repository: &'static str,
    field: &'static str,
    upstream: fn(&ComposeConfig) -> &str,
) -> Box<dyn Image> {
    Box::new(ConfiguredImage {
        name,
        repository,
        field,
        source: ImageSourceType::Mirror,
        upstream: Some(upstream),
    })
}

fn image_tag(upstream: &str) -> String {
    let without_digest = upstream.split('@').next().unwrap_or(upstream);
    let last_slash = without_digest.rfind('/');
    let last_colon = without_digest.rfind(':');
    match last_colon {
        Some(colon) if last_slash.is_none_or(|slash| colon > slash) => {
            without_digest[colon + 1..].to_string()
        }
        _ => "latest".to_string(),
    }
}
