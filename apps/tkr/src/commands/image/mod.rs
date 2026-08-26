//! `tkr image` — runtime image build, push, and mirror flows.
//!
//! All four subcommands (`build`, `list`, `push`, `mirror`) live here
//! because they share Dagger plumbing, the ECR auth path, and the
//! deployment-config writeback pattern.
//!
//! # The Dagger session
//!
//! `build`, `push`, and `mirror` each own one SDK session. The SDK provisions
//! and authenticates the CLI in-process — no `dagger run` wrapper or re-exec.
//! On Apple Silicon, tkr realizes the checksum-pinned fork engine as a persistent
//! local Docker runner. Tokeira isolates this owned session from ambient Dagger
//! development configuration. `list` needs no engine.
//!
//! # Image source types
//!
//! The deployment-engine layer labels every declared image as either
//! `Build` (we build it from sources; tokeirad and its variants),
//! `Mirror` (we copy an upstream image into our own ECR to avoid Docker
//! Hub rate limits and keep image pulls in-VPC; Grafana, Mimir, Loki,
//! Alloy, aws-cli, busybox) or `Registry` (pulled direct at deploy
//! time; nothing uses this today). `push` only operates on `Build`
//! images, `mirror` only on `Mirror` images.
//!
//! # Writeback
//!
//! Both `push` and `mirror` rewrite dotted keys in the deployment's
//! `deployment.toml` to point at the resulting ECR refs. That way a
//! subsequent `tkr infra apply` / `tkr deploy apply` sees the freshly
//! pushed image refs without operator intervention.

mod engine_bootstrap;
pub(crate) mod local_inspector;
mod progress;

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use local_inspector::{DockerCliInspector, LocalImageInspector};
use tokeira_aws::{DefaultEcrClient, EcrClient};
use tokeira_build::{
    MirrorRequest, PublishRequest, RegistryPassword, TokeiradBuildRequest, build_tokeirad_image,
    mirror_image, publish_image,
};
use tokeira_deploy_engine::{Image, ImageContext, ImageSourceType};
use tokeira_iac::ProvisionContext;
use tokeira_orchestrator::Deployment;

use crate::{
    cli::ImageCommand,
    commands::require_confirmation,
    deployment_dir::{DEPLOYMENT_TOML, DeploymentContext, PlatformDeploymentConfig},
    output,
    tui::OutputFormat,
};

use self::progress::ImageBuildProgress;

const DAGGER_SESSION_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn run(
    command: ImageCommand,
    deployment: Option<DeploymentContext>,
    format: OutputFormat,
) -> Result<()> {
    match command {
        ImageCommand::Build { arch, tag } => run_build(arch.into(), tag, format).await,
        ImageCommand::List { source_type } => {
            let ctx = deployment.ok_or_else(|| deployment_required("image list"))?;
            run_list(ctx, source_type.map(Into::into), format).await
        }
        ImageCommand::Push { tag, image, yes } => {
            let ctx = deployment.ok_or_else(|| deployment_required("image push"))?;
            run_push(ctx, tag, image, yes, format, &DockerCliInspector).await
        }
        ImageCommand::Mirror { image, yes } => {
            let ctx = deployment.ok_or_else(|| deployment_required("image mirror"))?;
            run_mirror(ctx, image, yes, format).await
        }
    }
}

/// Connect one owned Dagger session for a verb.
///
/// The SDK owns CLI acquisition and authentication. On Apple Silicon, tkr first
/// prepares the checksum-pinned fork engine and supplies its local runner URI. The
/// exact SDK release and runner remain deterministic even inside a Dagger development
/// shell, and startup is bounded independently of a potentially long image build.
pub(crate) async fn dagger_session() -> Result<dagger_sdk::Client> {
    dagger_session_inner(None).await
}

async fn dagger_session_with_progress(
    progress: Arc<ImageBuildProgress>,
) -> Result<dagger_sdk::Client> {
    dagger_session_inner(Some(progress)).await
}

async fn dagger_session_inner(
    progress: Option<Arc<ImageBuildProgress>>,
) -> Result<dagger_sdk::Client> {
    let runner_host = engine_bootstrap::runner_host(progress.clone()).await?;
    if let Some(progress) = &progress {
        progress.start_phase("Connecting to Dagger");
    }

    let config = dagger_client_config(runner_host, progress.clone())?;
    let client = dagger_sdk::connect_with(config)
        .await
        .context("failed to connect a Dagger session")?;
    if let Some(progress) = &progress {
        progress.finish_phase("Dagger session connected");
    }
    Ok(client)
}

fn dagger_client_config(
    runner_host: Option<String>,
    progress: Option<Arc<ImageBuildProgress>>,
) -> Result<dagger_sdk::ClientConfig> {
    let mut config = dagger_sdk::ClientConfig::builder()
        .isolated_cli_session()
        .session_startup_timeout(DAGGER_SESSION_STARTUP_TIMEOUT);
    if let Some(runner_host) = runner_host {
        config = config.runner_host(runner_host);
    }
    if let Some(progress) = progress {
        config = config.diagnostic_sink(progress);
    }
    config.build().context("configure the Dagger session")
}

pub(crate) async fn run_build(
    arch: tokeira_build::Arch,
    tag: Option<String>,
    format: OutputFormat,
) -> Result<()> {
    let workspace_root = workspace_root_from_current_dir()?;
    let progress = Arc::new(ImageBuildProgress::new());
    progress.announce("tokeirad", arch.as_str());
    let outcome = async {
        let client = dagger_session_with_progress(Arc::clone(&progress)).await?;
        let request = TokeiradBuildRequest {
            arch,
            tag,
            workspace_root,
        };
        progress.start_phase(format!("Building tokeirad for {}", arch.as_str()));
        let result = build_tokeirad_image(&request, &client).await?;
        progress.finish_phase(format!("Image built — {}", result.tags.join(", ")));
        client.close().await.context("close the Dagger session")?;
        Ok::<_, anyhow::Error>(result)
    }
    .await;
    let result = match outcome {
        Ok(result) => result,
        Err(error) => {
            progress.fail_phase("Image build failed");
            return Err(error);
        }
    };
    match format {
        OutputFormat::Human => {
            output::render_markdown(&image_build_report(
                &result.image_name,
                result.arch.as_str(),
                &result.tags,
                &result.toolchain_version,
            ));
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

fn image_build_report(
    image_name: &str,
    arch: &str,
    tags: &[String],
    toolchain_version: &str,
) -> String {
    let tags = tags
        .iter()
        .map(|tag| format!("`{tag}`"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "# Image Build\n**Built {image_name}** for {arch}\n\n## Completed\n\
         - image: {tags}\n\
         - architecture: `{arch}`\n\
         - toolchain: Rust `{toolchain_version}`\n"
    )
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
        bail!(
            "image push targets ECR and is only supported for ecs deployments. \
             Deployment '{}' is not an ecs deployment.",
            ctx.name
        );
    };
    let deployment = tokeira_ecs_deployment::EcsDeployment::new(&ctx.path);
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

    let provision_ctx = provision_context_for_ecs(config, &deployment).await?;
    let client = dagger_session().await?;
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
            &client,
        )
        .await?;
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

    client.close().await.context("close the Dagger session")?;
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
        bail!(
            "image mirror targets ECR and is only supported for ecs deployments. \
             Deployment '{}' is not an ecs deployment.",
            ctx.name
        );
    };
    let deployment = tokeira_ecs_deployment::EcsDeployment::new(&ctx.path);
    let mut image_ctx = ImageContext::default();
    deployment
        .register_image_extensions(config, &mut image_ctx)
        .await?;
    let images = tokeira_ecs_deployment::images::all(&image_ctx)?;
    let selected = validate_image_filter(image.as_deref(), &images, ImageSourceType::Mirror)?;

    let provision_ctx = provision_context_for_ecs(config, &deployment).await?;
    let client = dagger_session().await?;
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
                    &client,
                )
                .await?
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

    client.close().await.context("close the Dagger session")?;
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
        PlatformDeploymentConfig::Local(_) => bail!(
            "deployment '{}' runs on the local platform, which has no container images \
             (tokeirad runs directly as a host process). Use a compose or ecs deployment:\n\
             \ttkr --deployment <name> image list",
            ctx.name
        ),
        PlatformDeploymentConfig::Ecs(config) => {
            let deployment = tokeira_ecs_deployment::EcsDeployment::new(&ctx.path);
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

pub(crate) fn validate_image_filter<'a>(
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

/// Shared error constructor for image subcommands that need a deployment
/// context to resolve platform images. Points the operator at the global
/// `--deployment` flag so they do not have to grep the help output.
fn deployment_required(subcommand: &str) -> anyhow::Error {
    anyhow!(
        "{subcommand} requires a deployment to resolve its images. \
         pass `tkr --deployment <name> {subcommand}` or run `tkr deployment list` to see what is available"
    )
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

    #[test]
    fn image_build_report_is_a_complete_markdown_result() {
        let report = image_build_report(
            "tokeirad",
            "arm64",
            &["tokeirad:latest".to_owned(), "tokeirad:dev".to_owned()],
            "1.97.1",
        );

        assert!(report.starts_with("# Image Build\n**Built tokeirad** for arm64\n"));
        assert!(report.contains("- image: `tokeirad:latest`, `tokeirad:dev`\n"));
        assert!(report.contains("- architecture: `arm64`\n"));
        assert!(report.ends_with("- toolchain: Rust `1.97.1`\n"));
    }

    #[test]
    fn dagger_config_owns_source_selection_and_bounds_startup() {
        let config =
            dagger_client_config(Some("docker-container://tokeira-runner".to_owned()), None)
                .expect("the owned Dagger configuration is valid");

        assert!(config.uses_isolated_cli_session());
        assert_eq!(
            config.session_startup_timeout(),
            DAGGER_SESSION_STARTUP_TIMEOUT
        );
        assert_eq!(
            config.runner_host(),
            Some("docker-container://tokeira-runner")
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
