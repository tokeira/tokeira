//! `tkr image build` — deployment-independent runtime image construction.
//!
//! Image inventories and publication belong to definition-bound provisioners;
//! `tkr` owns only the workspace-source build operation.
//!
//! # The Dagger session
//!
//! Each build owns one SDK session. The SDK provisions and authenticates the
//! CLI in-process — no `dagger run` wrapper or re-exec.
//! On Apple Silicon, tkr realizes the checksum-pinned fork engine as a persistent
//! local Docker runner. Tokeira isolates this owned session from ambient Dagger
//! development configuration.

mod engine_bootstrap;
mod progress;

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use tokeira_build::{TokeiradBuildRequest, build_tokeirad_image};

use crate::{cli::ImageCommand, output, tui::OutputFormat};

use self::progress::ImageBuildProgress;

const DAGGER_SESSION_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn run(command: ImageCommand, format: OutputFormat) -> Result<()> {
    let ImageCommand::Build { arch, tag } = command;
    run_build(arch.into(), tag, format).await
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

/// Connect CI to the already-running pinned engine with an explicit lock policy.
///
/// Unlike image bootstrap, this path performs no provisioning. A missing runner
/// therefore fails with the remediation owned by the checksum-verified image flow.
pub(crate) async fn ci_dagger_session(
    workspace_root: &std::path::Path,
    lock_mode: dagger_sdk::LockMode,
) -> Result<dagger_sdk::Client> {
    let runner_host = engine_bootstrap::running_runner_host().await?;
    let config = dagger_sdk::ClientConfig::builder()
        .isolated_cli_session()
        .runner_host(runner_host)
        .workdir(workspace_root)
        .lock_mode(lock_mode)
        .session_startup_timeout(DAGGER_SESSION_STARTUP_TIMEOUT)
        .build()
        .context("configure the pinned Dagger CI session")?;
    dagger_sdk::connect_with(config)
        .await
        .context("failed to connect the pinned Dagger CI session")
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

pub(crate) fn workspace_root_from_current_dir() -> Result<PathBuf> {
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
    use super::*;
    use crate::cli::Cli;
    use clap::Parser;

    #[test]
    fn parses_image_commands() {
        assert!(Cli::try_parse_from(["tkr", "image", "build"]).is_ok());
        assert!(
            Cli::try_parse_from(["tkr", "image", "build", "--arch", "amd64", "--tag", "v1"])
                .is_ok()
        );
        assert!(Cli::try_parse_from(["tkr", "image", "push"]).is_err());
        assert!(Cli::try_parse_from(["tkr", "image", "mirror"]).is_err());
        assert!(Cli::try_parse_from(["tkr", "image", "list"]).is_err());
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
}
