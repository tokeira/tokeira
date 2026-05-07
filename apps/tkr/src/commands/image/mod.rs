pub mod local_inspector;

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use local_inspector::{DockerCliInspector, LocalImageInspector};
use tokeira_aws::{DefaultEcrClient, EcrClient};
use tokeira_build::{
    DefaultDaggerClient, MirrorRequest, PublishRequest, RegistryPassword, TokeiradBuildRequest,
    build_tokeirad_image, mirror_image, publish_image,
};
use tokeira_deploy_engine::{Image, ImageContext, ImageSourceType};
use tokeira_iac::ProvisionContext;
use tokeira_orchestrator::Deployment;

use crate::{
    cli::ImageCommand,
    commands::require_confirmation,
    deployment_dir::{DEPLOYMENT_TOML, DeploymentContext, PlatformDeploymentConfig},
    tui::OutputFormat,
};

pub async fn run(
    command: ImageCommand,
    deployment: Option<DeploymentContext>,
    format: OutputFormat,
) -> Result<()> {
    match command {
        ImageCommand::Build { arch, tag } => run_build(arch.into(), tag, format),
        ImageCommand::List { source_type } => {
            let ctx = deployment.ok_or_else(|| anyhow!("image list requires --deployment"))?;
            run_list(ctx, source_type.map(Into::into), format).await
        }
        ImageCommand::Push { tag, image, yes } => {
            let ctx = deployment.ok_or_else(|| anyhow!("image push requires --deployment"))?;
            run_push(ctx, tag, image, yes, format, &DockerCliInspector).await
        }
        ImageCommand::Mirror { image, yes } => {
            let ctx = deployment.ok_or_else(|| anyhow!("image mirror requires --deployment"))?;
            run_mirror(ctx, image, yes, format).await
        }
    }
}

pub fn run_build(
    arch: tokeira_build::Arch,
    tag: Option<String>,
    format: OutputFormat,
) -> Result<()> {
    ensure_dagger_session_available()?;
    let workspace_root = workspace_root_from_current_dir()?;
    let dagger = DefaultDaggerClient::from_env()?;
    let request = TokeiradBuildRequest {
        arch,
        tag,
        workspace_root,
    };
    let result = build_tokeirad_image(&request, &dagger)?;
    match format {
        OutputFormat::Human => {
            println!(
                "built {} for {}: {}",
                result.image_name,
                result.arch.as_str(),
                result.tags.join(", ")
            );
        }
        OutputFormat::Json => {
            let value = serde_json::json!({
                "action": "build",
                "image": result.image_name,
                "tags": result.tags,
                "arch": result.arch.as_str(),
            });
            println!("{}", serde_json::to_string(&value)?);
        }
    }
    Ok(())
}

async fn run_push(
    ctx: DeploymentContext,
    tag: String,
    image: Option<String>,
    yes: bool,
    format: OutputFormat,
    inspector: &dyn LocalImageInspector,
) -> Result<()> {
    require_confirmation(yes, "image push")?;
    let PlatformDeploymentConfig::Ecs(config) = &ctx.platform_config else {
        bail!("image push is only supported for ECS deployments");
    };
    let deployment = tokeira_ecs_deployment::EcsDeployment;
    let mut image_ctx = ImageContext::default();
    deployment
        .register_image_extensions(config, &mut image_ctx)
        .await?;
    let images = tokeira_ecs_deployment::images::all(&image_ctx)?;
    let selected = validate_image_filter(image.as_deref(), &images, ImageSourceType::Build)?;

    for selected_image in &selected {
        let local_ref = local_build_ref(*selected_image);
        if !inspector.image_exists(&local_ref).await? {
            bail!("local image '{local_ref}' does not exist; run `tkr image build` first");
        }
    }

    ensure_dagger_session_available()?;
    let provision_ctx = provision_context_for_ecs(config, &deployment).await?;
    let dagger = DefaultDaggerClient::from_env()?;
    let ecr = default_ecr_client(config).await;
    let auth = ecr.get_authorization_token().await?;
    tokeira_ecs_deployment::images::ensure_ecr_repositories_from_images(
        &ecr,
        &provision_ctx,
        &selected,
        &image_ctx,
    )
    .await?;

    let mut published_rows = Vec::new();
    let mut writebacks = Vec::new();
    for selected_image in selected {
        let desired = selected_image.desired_ref(&image_ctx)?;
        let remote_refs = publish_refs(&auth.registry_host, &desired.repository, &tag);
        let result = publish_image(
            &PublishRequest {
                local_image: local_build_ref(selected_image),
                remote_refs,
                registry_host: auth.registry_host.clone(),
                username: auth.username.clone(),
                password: RegistryPassword::new(auth.password.clone()),
            },
            &dagger,
        )?;
        let effective_ref = effective_ref(&auth.registry_host, &desired.repository, &tag);
        for target in selected_image.writeback_targets(&image_ctx) {
            writebacks.push((target.field.to_owned(), effective_ref.clone()));
        }
        for published in result.published {
            published_rows.push(serde_json::json!({
                "image": selected_image.name(),
                "remote_ref": published.remote_ref,
                "published_ref": published.published_ref,
            }));
        }
    }

    write_deployment_writeback(&ctx, &writebacks)?;
    print_json_or_human(
        format,
        "push",
        "published image refs",
        serde_json::json!({
            "action": "push",
            "published": published_rows,
            "writebacks": writebacks,
        }),
    )
}

async fn run_mirror(
    ctx: DeploymentContext,
    image: Option<String>,
    yes: bool,
    format: OutputFormat,
) -> Result<()> {
    require_confirmation(yes, "image mirror")?;
    let PlatformDeploymentConfig::Ecs(config) = &ctx.platform_config else {
        bail!("image mirror is only supported for ECS deployments");
    };
    let deployment = tokeira_ecs_deployment::EcsDeployment;
    let mut image_ctx = ImageContext::default();
    deployment
        .register_image_extensions(config, &mut image_ctx)
        .await?;
    let images = tokeira_ecs_deployment::images::all(&image_ctx)?;
    let selected = validate_image_filter(image.as_deref(), &images, ImageSourceType::Mirror)?;

    ensure_dagger_session_available()?;
    let provision_ctx = provision_context_for_ecs(config, &deployment).await?;
    let dagger = DefaultDaggerClient::from_env()?;
    let ecr = default_ecr_client(config).await;
    let auth = ecr.get_authorization_token().await?;
    tokeira_ecs_deployment::images::ensure_ecr_repositories_from_images(
        &ecr,
        &provision_ctx,
        &selected,
        &image_ctx,
    )
    .await?;

    let mut mirrored_rows = Vec::new();
    let mut writebacks = Vec::new();
    for selected_image in selected {
        let desired = selected_image.desired_ref(&image_ctx)?;
        let destination_ref = effective_ref(&auth.registry_host, &desired.repository, &desired.tag);
        let source_ref = desired.upstream_ref.clone().ok_or_else(|| {
            anyhow!(
                "image '{}' is Mirror but desired_ref.upstream_ref is None",
                selected_image.name()
            )
        })?;
        let skipped = source_ref == destination_ref
            || source_ref.starts_with(&format!("{}/{}", auth.registry_host, desired.repository));
        let published_ref = if skipped {
            None
        } else {
            Some(
                mirror_image(
                    &MirrorRequest {
                        source_ref: source_ref.clone(),
                        remote_ref: destination_ref.clone(),
                        registry_host: auth.registry_host.clone(),
                        username: auth.username.clone(),
                        password: RegistryPassword::new(auth.password.clone()),
                    },
                    &dagger,
                )?
                .published_ref,
            )
        };
        for target in selected_image.writeback_targets(&image_ctx) {
            writebacks.push((target.field.to_owned(), destination_ref.clone()));
        }
        mirrored_rows.push(serde_json::json!({
            "image": selected_image.name(),
            "source_ref": source_ref,
            "remote_ref": destination_ref,
            "published_ref": published_ref,
            "skipped": skipped,
        }));
    }

    write_deployment_writeback(&ctx, &writebacks)?;
    print_json_or_human(
        format,
        "mirror",
        "mirrored image refs",
        serde_json::json!({
            "action": "mirror",
            "mirrored": mirrored_rows,
            "writebacks": writebacks,
        }),
    )
}

async fn run_list(
    ctx: DeploymentContext,
    source_type: Option<ImageSourceType>,
    format: OutputFormat,
) -> Result<()> {
    let mut image_ctx = ImageContext::default();
    let images = match &ctx.platform_config {
        PlatformDeploymentConfig::Local(_) => bail!("local platform has no image set"),
        PlatformDeploymentConfig::Compose(config) => {
            let deployment = tokeira_compose_deployment::ComposeDeployment;
            deployment
                .register_image_extensions(config, &mut image_ctx)
                .await?;
            tokeira_compose_deployment::images::all(&image_ctx)?
        }
        PlatformDeploymentConfig::Ecs(config) => {
            let deployment = tokeira_ecs_deployment::EcsDeployment;
            deployment
                .register_image_extensions(config, &mut image_ctx)
                .await?;
            tokeira_ecs_deployment::images::all(&image_ctx)?
        }
    };
    let rows = images
        .iter()
        .filter(|image| source_type.is_none_or(|source| image.source_type() == source))
        .map(|image| {
            let desired = image.desired_ref(&image_ctx)?;
            Ok(serde_json::json!({
                "name": image.name(),
                "source_type": format!("{:?}", image.source_type()),
                "repository": desired.repository,
                "tag": desired.tag,
                "upstream_ref": desired.upstream_ref,
            }))
        })
        .collect::<std::result::Result<Vec<_>, tokeira_deploy_engine::RuntimeError>>()?;
    match format {
        OutputFormat::Human => {
            println!("NAME\tSOURCE\tREPOSITORY\tTAG\tUPSTREAM");
            for row in rows {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    row["name"].as_str().unwrap_or_default(),
                    row["source_type"].as_str().unwrap_or_default(),
                    row["repository"].as_str().unwrap_or_default(),
                    row["tag"].as_str().unwrap_or_default(),
                    row["upstream_ref"].as_str().unwrap_or("")
                );
            }
        }
        OutputFormat::Json => println!("{}", serde_json::to_string(&rows)?),
    }
    Ok(())
}

pub fn validate_image_filter<'a>(
    filter: Option<&str>,
    images: &'a [Box<dyn Image>],
    source: ImageSourceType,
) -> Result<Vec<&'a dyn Image>> {
    let mut valid_names = images
        .iter()
        .filter(|image| image.source_type() == source)
        .map(|image| image.name().to_owned())
        .collect::<Vec<_>>();
    valid_names.sort();
    let selected = match filter {
        None => images
            .iter()
            .filter(|image| image.source_type() == source)
            .map(|image| image.as_ref())
            .collect(),
        Some(name) => images
            .iter()
            .find(|image| image.source_type() == source && image.name() == name)
            .map(|image| vec![image.as_ref()])
            .ok_or_else(|| {
                anyhow!(
                    "unknown {source:?} image '{name}'; valid {source:?} images are: {}",
                    valid_names.join(", ")
                )
            })?,
    };
    Ok(selected)
}

fn ensure_dagger_session_available() -> Result<()> {
    if std::env::var_os("DAGGER_SESSION_PORT").is_some()
        && std::env::var_os("DAGGER_SESSION_TOKEN").is_some()
    {
        return Ok(());
    }
    bail!("Dagger session not available; run this command through `dagger run`")
}

async fn provision_context_for_ecs(
    config: &tokeira_ecs_deployment::EcsConfig,
    deployment: &tokeira_ecs_deployment::EcsDeployment,
) -> Result<ProvisionContext> {
    let mut ctx = ProvisionContext::new(config.project_name.clone(), config.tags.clone());
    deployment
        .register_infra_extensions(config, &mut ctx)
        .await?;
    Ok(ctx)
}

async fn default_ecr_client(config: &tokeira_ecs_deployment::EcsConfig) -> DefaultEcrClient {
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .load()
        .await;
    DefaultEcrClient::from_aws_config(&aws_config)
}

fn publish_refs(registry_host: &str, repository: &str, tag: &str) -> Vec<String> {
    let latest = format!("{registry_host}/{repository}:latest");
    if tag == "latest" {
        vec![latest]
    } else {
        vec![latest, format!("{registry_host}/{repository}:{tag}")]
    }
}

fn effective_ref(registry_host: &str, repository: &str, tag: &str) -> String {
    format!("{registry_host}/{repository}:{tag}")
}

fn local_build_ref(image: &dyn Image) -> String {
    format!("{}:latest", image.name())
}

fn write_deployment_writeback(ctx: &DeploymentContext, values: &[(String, String)]) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let path = ctx.path.join(DEPLOYMENT_TOML);
    if !path.exists() {
        bail!(
            "cannot write image refs because {} does not exist",
            path.display()
        );
    }
    let borrowed = values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    tokeira_iac::write_config_values(&path, &borrowed)?;
    Ok(())
}

fn print_json_or_human(
    format: OutputFormat,
    action: &str,
    human: &str,
    value: serde_json::Value,
) -> Result<()> {
    match format {
        OutputFormat::Human => println!("{action}: {human}"),
        OutputFormat::Json => println!("{}", serde_json::to_string(&value)?),
    }
    Ok(())
}

fn workspace_root_from_current_dir() -> Result<PathBuf> {
    let mut dir = std::env::current_dir().context("failed to read current directory")?;
    loop {
        if dir.join("rust-toolchain.toml").exists() && dir.join("Cargo.toml").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            bail!("could not find workspace root containing Cargo.toml and rust-toolchain.toml");
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use tokeira_deploy_engine::{DesiredImageRef, RuntimeError};

    use super::*;
    use crate::cli::{Cli, Command};

    #[test]
    fn parses_image_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "image", "list"])
                .expect("parse")
                .command,
            Command::Image(_)
        ));
        assert!(Cli::try_parse_from(["tkr", "image", "build"]).is_ok());
        assert!(
            Cli::try_parse_from(["tkr", "image", "build", "--arch", "amd64", "--tag", "v1"])
                .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "tkr",
                "--deployment",
                "prod",
                "image",
                "push",
                "--image",
                "tokeirad",
                "--yes"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "tkr",
                "--deployment",
                "prod",
                "image",
                "mirror",
                "--image",
                "grafana-mimir",
                "--yes"
            ])
            .is_ok()
        );
    }

    #[test]
    fn image_filter_reports_unknown_names() {
        let images: Vec<Box<dyn Image>> = vec![Box::new(TestImage)];

        let err = validate_image_filter(Some("tokierad"), &images, ImageSourceType::Build)
            .expect_err("unknown filter");

        assert!(err.to_string().contains("unknown Build image 'tokierad'"));
    }

    #[test]
    fn publish_refs_dedupes_latest() {
        assert_eq!(
            publish_refs("example.invalid", "tokeira/tokeirad", "latest"),
            vec!["example.invalid/tokeira/tokeirad:latest"]
        );
        assert_eq!(
            publish_refs("example.invalid", "tokeira/tokeirad", "v1"),
            vec![
                "example.invalid/tokeira/tokeirad:latest",
                "example.invalid/tokeira/tokeirad:v1"
            ]
        );
    }

    #[tokio::test]
    async fn mock_local_image_inspector_records_calls() {
        let inspector = local_inspector::MockLocalImageInspector::new(true);

        assert!(
            inspector
                .image_exists("tokeirad:latest")
                .await
                .expect("inspect")
        );

        assert_eq!(inspector.calls(), vec!["tokeirad:latest"]);
    }

    #[derive(Debug)]
    struct TestImage;

    impl Image for TestImage {
        fn name(&self) -> &str {
            "tokeirad"
        }

        fn source_type(&self) -> ImageSourceType {
            ImageSourceType::Build
        }

        fn desired_ref(&self, _ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError> {
            Ok(DesiredImageRef {
                repository: "tokeira/tokeirad".to_owned(),
                tag: "latest".to_owned(),
                upstream_ref: None,
            })
        }
    }
}
