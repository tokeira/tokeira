//! `tkr image build` — deployment-independent runtime image construction.
//!
//! Image inventories belong to definition-bound provisioners; `tkr` owns only
//! the workspace-source build operation.
//!
//! # The Dagger session
//!
//! Each build owns one SDK session. The SDK downloads its exact Dagger CLI
//! release, checksum-verified, and the CLI provisions the engine — no
//! `dagger run` wrapper or re-exec. tkr refuses to start inside another Dagger
//! session, so every session it opens is its own.
//!
//! The CLI starts in the system's temporary directory, outside the repository.
//! Started inside a Dagger workspace, stock Dagger leaves that workspace's
//! `dagger.lock` out of host directories, and the release train's Git checks
//! would read the lock as deleted. The pipelines pass absolute host paths, so
//! nothing else depends on where the CLI starts.

mod progress;

use std::{ffi::OsString, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use tokeira_build::{TokeiradBuildRequest, build_tokeirad_image};

use crate::{cli::ImageCommand, output, tui::OutputFormat};

use self::progress::ImageBuildProgress;

/// Variables through which an enclosing Dagger session, or another Dagger CLI
/// or engine, would take over the SDK's choice of engine.
const ENCLOSING_SESSION_VARIABLES: [&str; 3] = [
    "DAGGER_SESSION_PORT",
    "_EXPERIMENTAL_DAGGER_CLI_BIN",
    "_EXPERIMENTAL_DAGGER_RUNNER_HOST",
];

pub(crate) async fn run(command: ImageCommand, format: OutputFormat) -> Result<()> {
    let ImageCommand::Build { arch, tag } = command else {
        bail!("definition-bound image operations must be forwarded to the selected provisioner");
    };
    run_build(arch.into(), tag, format).await
}

/// Connect one owned Dagger session for a verb.
pub(crate) async fn dagger_session() -> Result<dagger_sdk::Client> {
    connect(dagger_sdk::ClientConfig::builder()).await
}

async fn dagger_session_with_progress(
    progress: Arc<ImageBuildProgress>,
) -> Result<dagger_sdk::Client> {
    progress.start_phase("Connecting to Dagger");
    let client =
        connect(dagger_sdk::ClientConfig::builder().diagnostic_sink(progress.clone())).await?;
    progress.finish_phase("Dagger session connected");
    Ok(client)
}

async fn connect(config: dagger_sdk::ClientConfigBuilder) -> Result<dagger_sdk::Client> {
    refuse_enclosing_session(std::env::var_os)?;
    dagger_sdk::connect_with(session_config(config)?)
        .await
        .context("failed to connect a Dagger session")
}

/// The configuration of a session tkr opens: its CLI starts in the system's
/// temporary directory, outside the repository.
fn session_config(config: dagger_sdk::ClientConfigBuilder) -> Result<dagger_sdk::ClientConfig> {
    config
        .workdir(std::env::temp_dir())
        .build()
        .context("configure the Dagger session")
}

/// Refuse to start while any of [`ENCLOSING_SESSION_VARIABLES`] is set.
fn refuse_enclosing_session(variable: impl Fn(&'static str) -> Option<OsString>) -> Result<()> {
    let set = ENCLOSING_SESSION_VARIABLES
        .into_iter()
        .filter(|&key| variable(key).is_some())
        .collect::<Vec<_>>();
    if set.is_empty() {
        return Ok(());
    }
    bail!(
        "tkr starts its own Dagger session and does not run inside another one; unset {}",
        set.join(", ")
    )
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
        assert!(Cli::try_parse_from(["tkr", "image", "list"]).is_ok());
        assert!(Cli::try_parse_from(["tkr", "image", "list", "--source-type", "mirror"]).is_ok());
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
    fn sessions_start_outside_any_other_dagger_session() {
        let unrelated = |key: &'static str| match key {
            "DAGGER_CLOUD_TOKEN" | "DAGGER_SESSION_TOKEN" => Some(OsString::from("set")),
            _ => None,
        };
        refuse_enclosing_session(unrelated).expect("no enclosing session variable is set");
    }

    #[test]
    fn sessions_start_their_cli_outside_the_repository() {
        let config =
            session_config(dagger_sdk::ClientConfig::builder()).expect("the session configures");
        assert_eq!(config.workdir(), Some(std::env::temp_dir().as_path()));
    }

    #[test]
    fn sessions_refuse_each_enclosing_session_variable() {
        for variable in ENCLOSING_SESSION_VARIABLES {
            let error = refuse_enclosing_session(|key| {
                if key == variable {
                    Some(OsString::new())
                } else {
                    None
                }
            })
            .expect_err("an enclosing session variable is refused");
            assert!(
                error.to_string().ends_with(&format!("; unset {variable}")),
                "{error}"
            );
        }

        let error = refuse_enclosing_session(|_| Some(OsString::from("1")))
            .expect_err("every enclosing session variable is refused");
        assert!(
            error
                .to_string()
                .ends_with("; unset DAGGER_SESSION_PORT, _EXPERIMENTAL_DAGGER_CLI_BIN, _EXPERIMENTAL_DAGGER_RUNNER_HOST"),
            "{error}"
        );
    }
}
