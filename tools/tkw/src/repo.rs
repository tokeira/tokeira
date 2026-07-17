//! Repository discovery and git plumbing.
//!
//! tkw can be invoked from the main checkout *or from inside any linked
//! worktree* (an agent asking for `tkw ls`, a tidy cron with a worktree CWD).
//! Discovery therefore resolves the **main checkout** through git's common
//! directory rather than trusting the current directory: the common dir is the
//! shared `.git` of the main checkout regardless of which worktree we're in.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};

/// The resolved repository layout every subcommand operates on.
#[derive(Debug, Clone)]
pub(crate) struct Repo {
    /// The main checkout's root (parent of the shared `.git` directory).
    pub(crate) main_root: PathBuf,
    /// Where tkw-owned agent worktrees live. `$TKW_DIR`, defaulting to a
    /// sibling of the main checkout named `<checkout>-wt` (same filesystem, so
    /// kache reflinks keep working).
    pub(crate) worktrees_dir: PathBuf,
    /// `$CODEX_HOME` (default `~/.codex`) — only used to *classify* the
    /// ChatGPT app's managed worktrees, never to modify them.
    pub(crate) codex_home: PathBuf,
}

impl Repo {
    pub(crate) fn discover() -> Result<Self> {
        let common_dir = git_capture_in(
            Path::new("."),
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .context("not inside a git repository (run tkw from the checkout or a worktree)")?;
        let common_dir = PathBuf::from(common_dir.trim());
        let main_root = common_dir
            .parent()
            .context("git common dir has no parent")?
            .to_path_buf();

        let worktrees_dir = match std::env::var_os("TKW_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => {
                let name = main_root
                    .file_name()
                    .context("main checkout has no directory name")?
                    .to_string_lossy();
                main_root
                    .parent()
                    .context("main checkout has no parent directory")?
                    .join(format!("{name}-wt"))
            }
        };

        let codex_home = match std::env::var_os("CODEX_HOME") {
            Some(dir) => PathBuf::from(dir),
            None => home_dir()?.join(".codex"),
        };

        Ok(Self {
            main_root,
            worktrees_dir,
            codex_home,
        })
    }

    /// Run git against the main checkout, capturing stdout.
    pub(crate) fn git(&self, args: &[&str]) -> Result<String> {
        git_capture_in(&self.main_root, args)
    }

    /// Run git against the main checkout for its side effect, echoing output.
    pub(crate) fn git_passthrough(&self, args: &[&str]) -> Result<()> {
        let status = Command::new("git")
            .arg("-C")
            .arg(&self.main_root)
            .args(args)
            .status()
            .context("failed to spawn git")?;
        if !status.success() {
            bail!("git {} failed with {status}", args.join(" "));
        }
        Ok(())
    }
}

/// Run git in `dir`, returning trimmed stdout or a stderr-carrying error.
pub(crate) fn git_capture_in(dir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .context("failed to spawn git")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub(crate) fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}
