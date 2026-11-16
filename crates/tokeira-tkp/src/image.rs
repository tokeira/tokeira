//! Generic rendering shell for platform-owned image declarations.
//!
//! Platforms interpret declarations. The shell only dispatches and renders
//! that capability. In particular, image commands do not create a second
//! deployment-state write path: the deployment engine remains the sole
//! persistence owner.

use anyhow::Result;
use tokeira_deploy_engine::ImageSourceType;

use crate::{engine::Engine, platform::Admitted};

pub(crate) fn list<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    source: Option<ImageSourceType>,
    json: bool,
) -> Result<()> {
    let operations = image_operations(engine)?;
    let images = operations
        .list(&admitted.deployment_ref)?
        .into_iter()
        .filter(|image| source.is_none_or(|source| image.source_type == source))
        .collect::<Vec<_>>();
    if json {
        let values = images
            .iter()
            .map(|image| {
                serde_json::json!({
                    "name": image.name,
                    "source_type": source_label(image.source_type),
                    "repository": image.repository,
                    "tag": image.tag,
                    "upstream_ref": image.upstream_ref,
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&values)?);
        return Ok(());
    }
    println!("NAME\tSOURCE\tREPOSITORY\tTAG\tUPSTREAM");
    for image in images {
        println!(
            "{}\t{}\t{}\t{}\t{}",
            image.name,
            source_label(image.source_type),
            image.repository,
            image.tag,
            image.upstream_ref.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

fn image_operations<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
) -> Result<&dyn tokeira_platform::declaration::ImageOperations> {
    engine
        .platform()
        .image_operations()
        .ok_or_else(|| anyhow::anyhow!("not applicable: this platform declares no image lifecycle"))
}

fn source_label(source: ImageSourceType) -> &'static str {
    match source {
        ImageSourceType::Build => "build",
        ImageSourceType::Mirror => "mirror",
        ImageSourceType::Registry => "registry",
    }
}
