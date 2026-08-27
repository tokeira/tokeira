//! Verified local runner preparation for the fork's Apple Silicon Dagger release.
//!
//! The Rust SDK already owns CLI acquisition and exact runtime compatibility checks.
//! This module fills the engine-distribution gap on Apple Silicon: it caches the
//! release OCI archive only after checking its compiled size and SHA-256, loads it into
//! the developer's Docker host, and starts one content-named privileged runner. The
//! caller isolates this path from ambient Dagger development configuration.

use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use directories::ProjectDirs;
use tokeira_build::{DAGGER_ENGINE_BOOTSTRAP_COMMAND, DAGGER_RELEASE, DaggerRelease};
use uuid::Uuid;

use super::progress::ImageBuildProgress;

const CACHE_LOCK_NAME: &str = ".engine-bootstrap.lock";
const ENGINE_RELEASE: DaggerRelease = DAGGER_RELEASE;

#[derive(Clone, Debug)]
struct BootstrapTools {
    curl: PathBuf,
    shasum: PathBuf,
    docker: PathBuf,
}

trait CommandExecutor: Send + Sync {
    fn output(&self, program: &Path, arguments: &[OsString]) -> io::Result<Output>;
}

#[derive(Clone, Copy, Debug)]
struct SystemCommandExecutor;

impl CommandExecutor for SystemCommandExecutor {
    fn output(&self, program: &Path, arguments: &[OsString]) -> io::Result<Output> {
        Command::new(program).args(arguments).output()
    }
}

struct EngineBootstrap<E> {
    executor: E,
    tools: BootstrapTools,
    cache_root: PathBuf,
    release: DaggerRelease,
    progress: Option<Arc<ImageBuildProgress>>,
}

/// Returns a locally prepared runner URI only for the native Apple Silicon path.
///
/// Bootstrap work runs on Tokio's blocking pool because Docker, hashing, and the
/// cross-process cache lease are synchronous host operations.
pub(super) async fn runner_host(
    progress: Option<Arc<ImageBuildProgress>>,
) -> Result<Option<String>> {
    if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        return Ok(None);
    }

    let cache_root = ProjectDirs::from("", "", "tokeira")
        .ok_or_else(|| anyhow!("could not determine the Dagger engine cache directory"))?
        .cache_dir()
        .join("tkr")
        .join("dagger");
    let tools = BootstrapTools {
        curl: required_tool("curl")?,
        shasum: required_tool("shasum")?,
        docker: required_tool("docker")?,
    };
    let bootstrap = EngineBootstrap {
        executor: SystemCommandExecutor,
        tools,
        cache_root,
        release: ENGINE_RELEASE,
        progress,
    };
    let host = tokio::task::spawn_blocking(move || bootstrap.run())
        .await
        .context("the Dagger engine bootstrap task did not complete")??;
    Ok(Some(host))
}

/// Return the pinned runner only when image bootstrap has already realized it.
///
/// CI is deliberately fail-closed: it must never turn a check into an engine
/// provisioning operation. The image flow owns the checksum-verified bootstrap,
/// and this read-only probe tells the operator exactly how to run it.
pub(super) async fn running_runner_host() -> Result<String> {
    let docker = required_tool("docker")?;
    tokio::task::spawn_blocking(move || {
        let output = Command::new(docker)
            .args([
                "container",
                "inspect",
                "--format",
                "{{.State.Running}}",
                ENGINE_RELEASE.container,
            ])
            .output()
            .context("could not inspect the pinned Dagger engine")?;
        let running = output.status.success() && output.stdout == b"true\n";
        if !running {
            bail!(
                "pinned Dagger engine {} is not running; run `{}` once to bootstrap the checksum-verified engine, then retry",
                ENGINE_RELEASE.engine_version,
                DAGGER_ENGINE_BOOTSTRAP_COMMAND
            );
        }
        Ok(ENGINE_RELEASE.runner_host())
    })
    .await
    .context("the pinned Dagger engine probe did not complete")?
}

fn required_tool(name: &str) -> Result<PathBuf> {
    which::which(name).with_context(|| {
        format!("Dagger engine bootstrap requires `{name}` to be available on PATH")
    })
}

impl<E: CommandExecutor> EngineBootstrap<E> {
    fn run(&self) -> Result<String> {
        prepare_cache_root(&self.cache_root)?;
        let lock = open_lock(&self.cache_root.join(CACHE_LOCK_NAME))?;
        lock.lock()
            .context("could not lock the Dagger engine cache")?;

        let (archive, downloaded) = self.ensure_archive()?;
        let image_loaded = self.ensure_image(&archive)?;
        let container_started = self.ensure_container()?;
        if let Some(progress) = &self.progress {
            let source = if downloaded || image_loaded || container_started {
                "linux/arm64"
            } else {
                "cached"
            };
            progress.finish_phase(format!("Dagger runner ready — {source}"));
        }
        Ok(format!("docker-container://{}", self.release.container))
    }

    fn ensure_archive(&self) -> Result<(PathBuf, bool)> {
        let archive = self.cache_root.join(self.release.asset_name);
        if archive.exists() {
            reject_non_regular(&archive)?;
            if let Some(progress) = &self.progress {
                progress.start_phase("Verifying cached Dagger engine");
            }
            if self.archive_matches(&archive)? {
                if let Some(progress) = &self.progress {
                    progress.finish_phase("Dagger engine verified — cached");
                }
                return Ok((archive, false));
            }
            if let Some(progress) = &self.progress {
                progress.clear_phase();
            }
            fs::remove_file(&archive).with_context(|| {
                format!(
                    "could not replace invalid cached engine archive {}",
                    archive.display()
                )
            })?;
        }

        let temporary = self.cache_root.join(format!(
            ".{}.download-{}",
            self.release.asset_name,
            Uuid::new_v4()
        ));
        let result = (|| {
            if let Some(progress) = &self.progress {
                progress.start_phase("Downloading Dagger engine for Apple Silicon");
            } else {
                eprintln!("dagger: downloading the verified Apple Silicon engine…");
            }
            self.run_checked(
                &self.tools.curl,
                &[
                    "--fail".into(),
                    "--location".into(),
                    "--silent".into(),
                    "--show-error".into(),
                    "--proto".into(),
                    "=https".into(),
                    "--proto-redir".into(),
                    "=https".into(),
                    "--output".into(),
                    temporary.as_os_str().to_owned(),
                    self.release.asset_url.into(),
                ],
                "download the Dagger engine",
            )?;
            reject_non_regular(&temporary)?;
            if let Some(progress) = &self.progress {
                progress.finish_phase("Dagger engine downloaded — 358 MiB");
                progress.start_phase("Verifying Dagger engine archive");
            }
            if !self.archive_matches(&temporary)? {
                bail!("downloaded Dagger engine did not match the published size and SHA-256");
            }
            if let Some(progress) = &self.progress {
                progress.finish_phase("Dagger engine verified — SHA-256");
            }
            fs::rename(&temporary, &archive).with_context(|| {
                format!(
                    "could not publish the verified engine archive {}",
                    archive.display()
                )
            })?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        Ok((archive, true))
    }

    fn archive_matches(&self, archive: &Path) -> Result<bool> {
        if fs::metadata(archive)
            .with_context(|| format!("could not inspect {}", archive.display()))?
            .len()
            != self.release.asset_size
        {
            return Ok(false);
        }
        let output = self.run_checked(
            &self.tools.shasum,
            &["-a".into(), "256".into(), archive.as_os_str().to_owned()],
            "hash the Dagger engine archive",
        )?;
        let digest = parse_digest(&output.stdout)?;
        Ok(digest == self.release.asset_sha256)
    }

    fn ensure_image(&self, archive: &Path) -> Result<bool> {
        let inspected = self.run_command(
            &self.tools.docker,
            &[
                "image".into(),
                "inspect".into(),
                "--format".into(),
                "{{.Id}}".into(),
                self.release.image.into(),
            ],
        )?;
        if inspected.status.success() {
            return Ok(false);
        }
        if !contains_ascii_case_insensitive(&inspected.stderr, "no such image") {
            return Err(command_failure(
                "inspect the local Dagger engine image",
                &inspected,
            ));
        }

        if let Some(progress) = &self.progress {
            progress.start_phase("Loading Dagger engine into Docker");
        } else {
            eprintln!("dagger: loading the verified engine into Docker…");
        }
        let loaded = self.run_checked(
            &self.tools.docker,
            &[
                "load".into(),
                "--input".into(),
                archive.as_os_str().to_owned(),
            ],
            "load the Dagger engine image",
        )?;
        let loaded_reference = parse_loaded_image(&loaded.stdout, &loaded.stderr)?;
        self.run_checked(
            &self.tools.docker,
            &[
                "tag".into(),
                loaded_reference.into(),
                self.release.image.into(),
            ],
            "tag the Dagger engine image",
        )?;
        if let Some(progress) = &self.progress {
            progress.finish_phase("Dagger engine loaded into Docker");
        }
        Ok(true)
    }

    fn ensure_container(&self) -> Result<bool> {
        let inspected = self.run_command(
            &self.tools.docker,
            &[
                "container".into(),
                "inspect".into(),
                "--format".into(),
                "{{.State.Running}} {{.Config.Image}}".into(),
                self.release.container.into(),
            ],
        )?;
        if inspected.status.success() {
            let (running, image) = parse_container(&inspected.stdout)?;
            if image != self.release.image {
                bail!(
                    "Docker container '{}' exists with image '{}', expected '{}'; rename or remove the conflicting container",
                    self.release.container,
                    image,
                    self.release.image
                );
            }
            if !running {
                if let Some(progress) = &self.progress {
                    progress.start_phase("Starting cached Dagger runner");
                }
                self.run_checked(
                    &self.tools.docker,
                    &["start".into(), self.release.container.into()],
                    "start the Dagger engine container",
                )?;
                if let Some(progress) = &self.progress {
                    progress.finish_phase("Dagger runner started");
                }
                return Ok(true);
            }
            return Ok(false);
        }
        if !contains_ascii_case_insensitive(&inspected.stderr, "no such object")
            && !contains_ascii_case_insensitive(&inspected.stderr, "no such container")
        {
            return Err(command_failure(
                "inspect the local Dagger engine container",
                &inspected,
            ));
        }

        if let Some(progress) = &self.progress {
            progress.start_phase("Starting Dagger runner for Apple Silicon");
        } else {
            eprintln!("dagger: starting the Apple Silicon engine runner…");
        }
        self.run_checked(
            &self.tools.docker,
            &[
                "run".into(),
                "--detach".into(),
                "--name".into(),
                self.release.container.into(),
                "--restart".into(),
                "always".into(),
                "--privileged".into(),
                self.release.image.into(),
            ],
            "start the Dagger engine container",
        )?;
        if let Some(progress) = &self.progress {
            progress.finish_phase("Dagger runner started");
        }
        Ok(true)
    }

    fn run_command(&self, program: &Path, arguments: &[OsString]) -> Result<Output> {
        self.executor
            .output(program, arguments)
            .with_context(|| format!("could not run Dagger bootstrap tool {}", program.display()))
    }

    fn run_checked(
        &self,
        program: &Path,
        arguments: &[OsString],
        operation: &str,
    ) -> Result<Output> {
        let output = self.run_command(program, arguments)?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(command_failure(operation, &output))
        }
    }
}

fn prepare_cache_root(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("could not create Dagger engine cache {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("could not secure Dagger engine cache {}", path.display()))?;
    }
    Ok(())
}

fn open_lock(path: &Path) -> Result<File> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        bail!("Dagger engine cache lock is a symbolic link");
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .context("could not open the Dagger engine cache lock")
}

fn reject_non_regular(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("could not inspect {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "Dagger engine cache entry {} is not a regular file",
            path.display()
        );
    }
    Ok(())
}

fn parse_digest(stdout: &[u8]) -> Result<&str> {
    let output = std::str::from_utf8(stdout).context("shasum output was not UTF-8")?;
    let digest = output
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("shasum did not report a digest"))?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("shasum reported an invalid SHA-256 digest");
    }
    Ok(digest)
}

fn parse_loaded_image(stdout: &[u8], stderr: &[u8]) -> Result<String> {
    let stdout = std::str::from_utf8(stdout).context("Docker load output was not UTF-8")?;
    let stderr = std::str::from_utf8(stderr).context("Docker load diagnostics were not UTF-8")?;
    stdout
        .lines()
        .chain(stderr.lines())
        .find_map(|line| {
            line.strip_prefix("Loaded image: ")
                .or_else(|| line.strip_prefix("Loaded image ID: "))
        })
        .map(str::trim)
        .filter(|reference| !reference.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("Docker loaded the engine archive but did not report its image"))
}

fn parse_container(stdout: &[u8]) -> Result<(bool, String)> {
    let output = std::str::from_utf8(stdout).context("Docker inspect output was not UTF-8")?;
    let mut fields = output.split_whitespace();
    let running = match fields.next() {
        Some("true") => true,
        Some("false") => false,
        _ => bail!("Docker inspect reported an invalid engine container state"),
    };
    let image = fields
        .next()
        .ok_or_else(|| anyhow!("Docker inspect did not report the engine container image"))?;
    if fields.next().is_some() {
        bail!("Docker inspect reported an invalid engine container image");
    }
    Ok((running, image.to_owned()))
}

fn contains_ascii_case_insensitive(bytes: &[u8], needle: &str) -> bool {
    String::from_utf8_lossy(bytes)
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn command_failure(operation: &str, output: &Output) -> anyhow::Error {
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    let diagnostic = diagnostic.trim();
    if diagnostic.is_empty() {
        anyhow!("could not {operation}")
    } else {
        anyhow!("could not {operation}: {diagnostic}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsers_accept_the_exact_host_tool_shapes() {
        assert_eq!(
            parse_digest(
                b"29077fa248530162d29cbb089b41435dcfb512741dc4b206df798ad886108254  engine.tar\n"
            )
            .expect("valid digest parses"),
            ENGINE_RELEASE.asset_sha256
        );
        assert_eq!(
            parse_loaded_image(b"Loaded image ID: sha256:abc\n", b"").expect("image ID parses"),
            "sha256:abc"
        );
        assert_eq!(
            parse_loaded_image(b"", b"Loaded image: dagger-engine:release\n")
                .expect("named image parses"),
            "dagger-engine:release"
        );
        assert_eq!(
            parse_container(format!("false {}\n", ENGINE_RELEASE.image).as_bytes())
                .expect("container state parses"),
            (false, ENGINE_RELEASE.image.to_owned())
        );
    }

    #[test]
    fn parsers_reject_ambiguous_or_incomplete_output() {
        assert!(parse_digest(b"not-a-digest\n").is_err());
        assert!(parse_loaded_image(b"load complete\n", b"").is_err());
        assert!(parse_container(b"maybe image\n").is_err());
        assert!(parse_container(b"true\n").is_err());
        assert!(parse_container(b"true image extra\n").is_err());
    }

    #[test]
    fn release_coordinates_match_the_published_companion_assets() {
        assert!(
            ENGINE_RELEASE
                .asset_url
                .ends_with(ENGINE_RELEASE.asset_name)
        );
        assert_eq!(ENGINE_RELEASE.asset_size, 375_404_032);
        assert_eq!(ENGINE_RELEASE.asset_sha256.len(), 64);
        assert_eq!(
            ENGINE_RELEASE.runner_host(),
            "docker-container://tokeira-dagger-engine-rust3-arm64"
        );
    }

    #[test]
    fn case_insensitive_docker_absence_detection_is_bounded() {
        assert!(contains_ascii_case_insensitive(
            b"Error: No Such Image: fixture",
            "no such image"
        ));
        assert!(!contains_ascii_case_insensitive(
            b"Cannot connect to Docker",
            "no such image"
        ));
    }
}
