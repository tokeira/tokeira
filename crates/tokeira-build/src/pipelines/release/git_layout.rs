//! The workspace's Git layout, expressed for the engine-side mount.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use crate::BuildError;

/// Where a workspace's Git state lives, expressed for the engine-side mount.
///
/// Linked worktrees keep their `.git` as a pointer into the common directory; the
/// container receives the common directory at `/repo.git` and a rewritten pointer,
/// so every Git command inside it sees the same objects and refs the host does.
#[derive(Debug)]
pub(crate) struct GitLayout {
    /// The shared Git common directory on the host.
    pub(crate) common_dir: PathBuf,
    /// Contents of the `.git` pointer file to write into the mounted workspace.
    pub(crate) worktree_pointer: String,
}

impl GitLayout {
    pub(crate) fn resolve(root: &Path) -> Result<Self, BuildError> {
        let common_dir = git_path(root, "--git-common-dir")?;
        let git_dir = git_path(root, "--absolute-git-dir")?;
        let relative = git_dir
            .strip_prefix(&common_dir)
            .map_err(|_| BuildError::Validation {
                reason: format!(
                    "worktree git directory {} is outside common directory {}",
                    git_dir.display(),
                    common_dir.display()
                ),
            })?;
        let engine_path = if relative.as_os_str().is_empty() {
            "/repo.git".to_owned()
        } else {
            format!("/repo.git/{}", relative.display())
        };
        Ok(Self {
            common_dir,
            worktree_pointer: format!("gitdir: {engine_path}\n"),
        })
    }
}

fn git_path(root: &Path, argument: &str) -> Result<PathBuf, BuildError> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["rev-parse", "--path-format=absolute", argument])
        .output()
        .map_err(|error| BuildError::Validation {
            reason: format!("could not inspect the workspace Git layout: {error}"),
        })?;
    if !output.status.success() {
        return Err(BuildError::Validation {
            reason: format!(
                "workspace Git layout inspection failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}
