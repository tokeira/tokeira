//! Checksum-pinned host changie resolver used only for fragment authoring.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result, anyhow, bail};
use directories::ProjectDirs;
use tokeira_build::{
    CHANGIE_RELEASE, ChangieAsset, ReleaseError, admit_fragments, fragment_filename,
};
use uuid::Uuid;

const CACHE_LOCK_NAME: &str = ".changie.lock";

trait CommandExecutor {
    fn output(&self, program: &Path, arguments: &[String]) -> io::Result<Output>;
}

#[derive(Clone, Copy, Debug)]
struct SystemCommandExecutor;

impl CommandExecutor for SystemCommandExecutor {
    fn output(&self, program: &Path, arguments: &[String]) -> io::Result<Output> {
        Command::new(program).args(arguments).output()
    }
}

struct ChangieResolver<E> {
    executor: E,
    cache_root: PathBuf,
    curl: PathBuf,
    shasum: PathBuf,
    tar: PathBuf,
    asset: ChangieAsset,
}

/// Resolve the sole admitted changie binary without consulting ambient `PATH` candidates.
pub(crate) async fn pinned_changie() -> Result<PathBuf> {
    let asset = selected_asset()?;
    let cache_root = ProjectDirs::from("", "", "tokeira")
        .ok_or_else(|| anyhow!("could not determine the tkr tool cache directory"))?
        .cache_dir()
        .join("tkr")
        .join("tools")
        .join("changie")
        .join(CHANGIE_RELEASE.version)
        .join(asset.sha256);
    let resolver = ChangieResolver {
        executor: SystemCommandExecutor,
        cache_root,
        curl: required_tool("curl")?,
        shasum: required_tool("shasum")?,
        tar: required_tool("tar")?,
        asset,
    };
    tokio::task::spawn_blocking(move || resolver.resolve())
        .await
        .context("the changie resolver task did not complete")?
        .map_err(|error| {
            anyhow!(ReleaseError::Tool {
                reason: error.to_string(),
            })
        })
}

/// Create one fragment using a generated lowercase UUID version 4 Slice identity.
pub(crate) async fn create_fragment(
    workspace_root: &Path,
    kind: Option<&str>,
    body: Option<&str>,
) -> Result<PathBuf> {
    use std::io::IsTerminal as _;

    let interactive = std::io::stdin().is_terminal();
    if !interactive && kind.is_none() {
        return Err(changelog_error(
            workspace_root,
            "non-interactive fragment authoring requires --kind",
        ));
    }
    if !interactive && kind != Some("internal") && body.is_none() {
        return Err(changelog_error(
            workspace_root,
            "non-interactive non-internal fragment authoring requires --body",
        ));
    }
    let slice = Uuid::new_v4().hyphenated().to_string();
    let binary = pinned_changie().await?;
    let before = fragment_paths(workspace_root)?;
    let mut arguments = vec![
        "new".to_owned(),
        "--custom".to_owned(),
        format!("Slice={slice}"),
    ];
    if let Some(kind) = kind {
        arguments.push("--kind".to_owned());
        arguments.push(kind.to_owned());
    }
    if let Some(body) = body {
        arguments.push("--body".to_owned());
        arguments.push(body.to_owned());
    }
    let status = tokio::process::Command::new(&binary)
        .args(&arguments)
        .current_dir(workspace_root)
        .status()
        .await
        .with_context(|| format!("could not run pinned changie at {}", binary.display()))?;
    if !status.success() {
        return Err(changelog_error(
            workspace_root,
            format!("pinned changie refused the fragment (status {status})"),
        ));
    }
    let after = fragment_paths(workspace_root)?;
    let created = after.difference(&before).cloned().collect::<Vec<_>>();
    let [relative] = created.as_slice() else {
        remove_created(workspace_root, &created)?;
        return Err(changelog_error(
            workspace_root,
            format!(
                "pinned changie must create exactly one fragment, observed {} new paths",
                created.len()
            ),
        ));
    };
    if let Some(kind) = kind {
        let expected = PathBuf::from(".changes/unreleased")
            .join(fragment_filename(kind, &slice).map_err(|error| anyhow!(error.to_string()))?);
        if *relative != expected {
            remove_created(workspace_root, &created)?;
            return Err(changelog_error(
                relative,
                format!(
                    "pinned changie created {}, expected {}",
                    relative.display(),
                    expected.display()
                ),
            ));
        }
    }
    if let Err(error) = admit_fragments(workspace_root) {
        remove_created(workspace_root, &created)?;
        return Err(anyhow!(error));
    }
    Ok(relative.clone())
}

fn remove_created(workspace_root: &Path, created: &[PathBuf]) -> Result<()> {
    for relative in created {
        let path = workspace_root.join(relative);
        fs::remove_file(&path)
            .with_context(|| format!("remove invalid fragment created at {}", path.display()))?;
    }
    Ok(())
}

fn fragment_paths(workspace_root: &Path) -> Result<BTreeSet<PathBuf>> {
    let directory = workspace_root.join(".changes/unreleased");
    fs::read_dir(&directory)
        .with_context(|| format!("read fragment directory {}", directory.display()))?
        .map(|entry| {
            let path = entry?.path();
            path.strip_prefix(workspace_root)
                .map(Path::to_path_buf)
                .map_err(anyhow::Error::from)
        })
        .collect()
}

fn selected_asset() -> Result<ChangieAsset> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        (os, arch) => {
            return Err(anyhow!(ReleaseError::UnsupportedToolPlatform {
                platform: format!("{os}-{arch}"),
                remediation:
                    "run fragment authoring on a supported macOS or Linux x86_64/aarch64 host"
                        .to_owned(),
            }));
        }
    };
    CHANGIE_RELEASE
        .asset(platform)
        .ok_or_else(|| anyhow!("pinned changie asset table omitted {platform}"))
}

fn required_tool(name: &str) -> Result<PathBuf> {
    which::which(name).map_err(|source| {
        anyhow!(ReleaseError::Tool {
            reason: format!("changie bootstrap requires `{name}` on PATH: {source}"),
        })
    })
}

fn changelog_error(path: impl AsRef<Path>, reason: impl Into<String>) -> anyhow::Error {
    anyhow!(ReleaseError::Changelog {
        path: path.as_ref().to_path_buf(),
        reason: reason.into(),
    })
}

impl<E: CommandExecutor> ChangieResolver<E> {
    fn resolve(&self) -> Result<PathBuf> {
        fs::create_dir_all(&self.cache_root).with_context(|| {
            format!(
                "could not create changie cache {}",
                self.cache_root.display()
            )
        })?;
        let lock = open_lock(&self.cache_root.join(CACHE_LOCK_NAME))?;
        lock.lock()
            .context("could not lock the changie tool cache")?;
        let binary = self.cache_root.join("changie");
        if binary.is_file() && self.binary_version_matches(&binary)? {
            return Ok(binary);
        }
        if binary.exists() {
            fs::remove_file(&binary).with_context(|| {
                format!("could not replace invalid cached {}", binary.display())
            })?;
        }

        let nonce = Uuid::new_v4();
        let archive = self.cache_root.join(format!(".archive-{nonce}"));
        let extracted = self.cache_root.join(format!(".changie-{nonce}"));
        let result = (|| {
            self.run_checked(
                &self.curl,
                &[
                    "--fail".to_owned(),
                    "--location".to_owned(),
                    "--silent".to_owned(),
                    "--show-error".to_owned(),
                    "--proto".to_owned(),
                    "=https".to_owned(),
                    "--proto-redir".to_owned(),
                    "=https".to_owned(),
                    "--output".to_owned(),
                    archive.display().to_string(),
                    self.asset.url.to_owned(),
                ],
                "download pinned changie",
            )?;
            reject_non_regular(&archive)?;
            let digest = self.run_checked(
                &self.shasum,
                &[
                    "-a".to_owned(),
                    "256".to_owned(),
                    archive.display().to_string(),
                ],
                "hash pinned changie archive",
            )?;
            let observed = String::from_utf8_lossy(&digest.stdout)
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_owned();
            if !digest_matches(self.asset.sha256, &observed) {
                bail!(
                    "pinned changie archive digest mismatch: expected {}, observed {}",
                    self.asset.sha256,
                    observed
                );
            }
            self.run_checked(
                &self.tar,
                &[
                    "-xzf".to_owned(),
                    archive.display().to_string(),
                    "-O".to_owned(),
                    "changie".to_owned(),
                ],
                "extract pinned changie",
            )
            .and_then(|output| {
                fs::write(&extracted, output.stdout)
                    .with_context(|| format!("could not write extracted {}", extracted.display()))
            })?;
            let mut permissions = fs::metadata(&extracted)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&extracted, permissions)?;
            if !self.binary_version_matches(&extracted)? {
                bail!(
                    "pinned changie binary did not report version {}",
                    CHANGIE_RELEASE.version
                );
            }
            fs::rename(&extracted, &binary)
                .with_context(|| format!("could not atomically publish {}", binary.display()))?;
            Ok(binary.clone())
        })();
        let _ = fs::remove_file(&archive);
        let _ = fs::remove_file(&extracted);
        result
    }

    fn binary_version_matches(&self, binary: &Path) -> Result<bool> {
        let output = self
            .executor
            .output(binary, &["--version".to_owned()])
            .with_context(|| format!("could not probe {}", binary.display()))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(output.status.success() && version_output_matches(&stdout))
    }

    fn run_checked(&self, program: &Path, arguments: &[String], action: &str) -> Result<Output> {
        let output = self
            .executor
            .output(program, arguments)
            .with_context(|| format!("could not {action}"))?;
        if !output.status.success() {
            bail!(
                "could not {action}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(output)
    }
}

fn digest_matches(expected: &str, observed: &str) -> bool {
    expected.len() == 64
        && observed.len() == 64
        && expected == observed
        && observed
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, 'a'..='f'))
}

fn version_output_matches(output: &str) -> bool {
    output.trim() == format!("changie version v{}", CHANGIE_RELEASE.version)
}

fn open_lock(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("could not open changie cache lock {}", path.display()))
}

fn reject_non_regular(path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        bail!("refusing non-regular changie cache path {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 6: tool acquisition fails closed
        #[test]
        fn candidate_requires_supported_asset_digest_and_exact_version(
            supported in any::<bool>(),
            digest_matches_pin in any::<bool>(),
            version_matches_pin in any::<bool>(),
            _ambient_binary in proptest::option::of("/[a-z/]{1,32}"),
        ) {
            let asset = CHANGIE_RELEASE.asset("macos-aarch64").expect("pinned asset");
            let digest = if digest_matches_pin {
                asset.sha256.to_owned()
            } else {
                "0".repeat(64)
            };
            let version = if version_matches_pin {
                format!("changie version v{}", CHANGIE_RELEASE.version)
            } else {
                "changie version v0.0.0".to_owned()
            };
            let admitted = supported
                && digest_matches(asset.sha256, &digest)
                && version_output_matches(&version);
            prop_assert_eq!(
                admitted,
                supported && digest_matches_pin && version_matches_pin
            );
        }
    }
}
