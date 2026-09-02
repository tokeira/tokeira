//! Generic rendering shell for a platform-owned image lifecycle.
//!
//! Platforms interpret declarations, authenticate registries, and publish
//! images. The shell only dispatches and renders that capability. In
//! particular, image commands do not create a second deployment-state write
//! path: the deployment engine remains the sole persistence owner.

use anyhow::Result;
use tokeira_deploy_engine::ImageSourceType;
use tokeira_platform::declaration::PublishedImage;

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

pub(crate) async fn push<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    image: Option<&str>,
    tag: &str,
    json: bool,
) -> Result<()> {
    let operations = image_operations(engine)?;
    let published = operations
        .push(&admitted.deployment_ref, image, tag)
        .await?;
    render_publications("push", &published, json)
}

pub(crate) async fn mirror<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    image: Option<&str>,
    json: bool,
) -> Result<()> {
    let operations = image_operations(engine)?;
    let published = operations.mirror(&admitted.deployment_ref, image).await?;
    render_publications("mirror", &published, json)
}

fn image_operations<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &Engine<F>,
) -> Result<&dyn tokeira_platform::declaration::ImageOperations> {
    engine
        .platform()
        .image_operations()
        .ok_or_else(|| anyhow::anyhow!("not applicable: this platform declares no image lifecycle"))
}

fn render_publications(action: &str, publications: &[PublishedImage], json: bool) -> Result<()> {
    if json {
        let values = publications
            .iter()
            .map(|publication| {
                serde_json::json!({
                    "name": publication.name,
                    "resolved_ref": publication.resolved_ref,
                    "digest": publication.digest,
                    "published_refs": publication.published_refs,
                    "skipped": publication.skipped,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action": action,
                "images": values,
            }))?
        );
        return Ok(());
    }
    println!("# Image {}", title(action));
    for publication in publications {
        let status = if publication.skipped {
            "already resolved"
        } else {
            "published"
        };
        println!(
            "- `{}`: {status} as `{}` ({})",
            publication.name, publication.resolved_ref, publication.digest
        );
    }
    Ok(())
}

fn source_label(source: ImageSourceType) -> &'static str {
    match source {
        ImageSourceType::Build => "build",
        ImageSourceType::Mirror => "mirror",
        ImageSourceType::Registry => "registry",
    }
}

fn title(value: &str) -> String {
    let mut characters = value.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().chain(characters).collect(),
        None => String::new(),
    }
}
