//! Worktree lifecycle for tkw-owned agent worktrees, and owner-aware listing.
//!
//! The one rule that shapes everything here: **tkw removes only worktrees it
//! created** (those under its own worktree directory). Claude Code locks and
//! sweeps its `.claude/worktrees/`; the ChatGPT app keeps an LRU of its
//! `$CODEX_HOME/worktrees/`. Fighting those lifecycles from a CLI would lose
//! work — so `rm`/`clean` refuse anything tkw doesn't own, while `ls` shows
//! the whole fleet so the operator sees every checkout in one place.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::{includes, repo::Repo};

/// Who manages a worktree's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Owner {
    /// The main checkout itself.
    Main,
    /// Created by `tkw new`; tkw may remove it.
    Tkw,
    /// Claude Code native worktree (locked/swept by Claude Code).
    Claude,
    /// ChatGPT app managed worktree (app keeps an LRU; detached HEAD until
    /// "Create branch here").
    Codex,
    /// Anything else (`git worktree add` by hand).
    Manual,
}

impl Owner {
    fn label(self) -> &'static str {
        match self {
            Owner::Main => "main",
            Owner::Tkw => "tkw",
            Owner::Claude => "claude",
            Owner::Codex => "codex",
            Owner::Manual => "manual",
        }
    }
}

/// One entry from `git worktree list --porcelain`.
#[derive(Debug, Clone)]
pub(crate) struct Worktree {
    pub(crate) path: PathBuf,
    /// Short branch name, or `None` for detached HEAD.
    pub(crate) branch: Option<String>,
    pub(crate) owner: Owner,
}

pub(crate) fn classify(path: &Path, repo: &Repo) -> Owner {
    if path == repo.main_root {
        Owner::Main
    } else if path.starts_with(&repo.worktrees_dir) {
        Owner::Tkw
    } else if path.starts_with(repo.main_root.join(".claude").join("worktrees")) {
        Owner::Claude
    } else if path.starts_with(repo.codex_home.join("worktrees")) {
        Owner::Codex
    } else {
        Owner::Manual
    }
}

pub(crate) fn list(repo: &Repo) -> Result<Vec<Worktree>> {
    let porcelain = repo.git(&["worktree", "list", "--porcelain"])?;
    let mut result = Vec::new();
    let mut path: Option<PathBuf> = None;
    let mut branch: Option<String> = None;
    for line in porcelain.lines() {
        if let Some(p) = line.strip_prefix("worktree ") {
            path = Some(PathBuf::from(p));
            branch = None;
        } else if let Some(b) = line.strip_prefix("branch ") {
            branch = Some(b.strip_prefix("refs/heads/").unwrap_or(b).to_string());
        } else if line.is_empty()
            && let Some(p) = path.take()
        {
            let owner = classify(&p, repo);
            result.push(Worktree {
                path: p,
                branch: branch.take(),
                owner,
            });
        }
    }
    // Porcelain output may omit the trailing blank line after the last block.
    if let Some(p) = path.take() {
        let owner = classify(&p, repo);
        result.push(Worktree {
            path: p,
            branch: branch.take(),
            owner,
        });
    }
    Ok(result)
}

pub(crate) fn new(name: &str, base: Option<&str>) -> Result<()> {
    // Names become a path component and a branch suffix; keep them boring so
    // neither interpretation can escape its directory.
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        || name.starts_with('.')
    {
        bail!("worktree name must be [A-Za-z0-9._-]+ and not start with '.': {name:?}");
    }
    let repo = Repo::discover()?;
    let path = repo.worktrees_dir.join(name);
    if path.exists() {
        bail!("{} already exists", path.display());
    }
    std::fs::create_dir_all(&repo.worktrees_dir).with_context(|| {
        format!(
            "failed to create worktree dir {}",
            repo.worktrees_dir.display()
        )
    })?;

    let branch = format!("agent/{name}");
    // PR-flow default: base on the remote default branch so unpushed local work
    // never leaks into agent branches (docs/agents/concurrent-agents.md, preflight).
    // `--base` declares a dependent task's parent; HEAD is the remote-less fallback.
    let base = match base {
        Some(b) => b.to_string(),
        None => {
            if repo
                .git(&["rev-parse", "--verify", "--quiet", "origin/main"])
                .is_ok()
            {
                "origin/main".to_string()
            } else {
                "HEAD".to_string()
            }
        }
    };
    let path_str = path.to_string_lossy().into_owned();
    repo.git_passthrough(&["worktree", "add", "-b", &branch, &path_str, &base])?;

    let copied = includes::copy_into(&repo, &path)?;

    println!();
    println!(
        "Worktree ready: {}  (branch {branch}, from {base}{})",
        path.display(),
        if copied > 0 {
            format!(", {copied} gitignored file(s) copied")
        } else {
            String::new()
        }
    );
    Ok(())
}

pub(crate) fn ls() -> Result<()> {
    let repo = Repo::discover()?;
    let worktrees = list(&repo)?;
    let width = worktrees
        .iter()
        .map(|w| w.path.to_string_lossy().len())
        .max()
        .unwrap_or(0);
    for w in &worktrees {
        println!(
            "{:<width$}  [{:<6}]  {}",
            w.path.display(),
            w.owner.label(),
            w.branch.as_deref().unwrap_or("(detached)"),
        );
    }
    Ok(())
}

pub(crate) fn rm(name: &str, force: bool) -> Result<()> {
    let repo = Repo::discover()?;
    let path = repo.worktrees_dir.join(name);
    let worktrees = list(&repo)?;
    let Some(target) = worktrees.iter().find(|w| w.path == path) else {
        bail!(
            "no tkw worktree named {name:?} under {} (see `tkw ls`)",
            repo.worktrees_dir.display()
        );
    };
    if target.owner != Owner::Tkw {
        bail!(
            "{} is {}-owned; tkw only removes worktrees it created",
            path.display(),
            target.owner.label()
        );
    }
    let path_str = path.to_string_lossy().into_owned();
    if force {
        repo.git_passthrough(&["worktree", "remove", "--force", &path_str])?;
    } else {
        repo.git_passthrough(&["worktree", "remove", &path_str])?;
    }
    repo.git_passthrough(&["worktree", "prune"])?;
    if let Some(branch) = &target.branch {
        println!("Removed {name} (branch {branch} kept; delete with: git branch -d {branch})");
    } else {
        println!("Removed {name}");
    }
    Ok(())
}

pub(crate) fn clean(base: Option<&str>) -> Result<()> {
    let repo = Repo::discover()?;
    clean_with(&repo, base)
}

/// Remove tkw-owned worktrees whose branch is an ancestor of `base` (default:
/// the main checkout's current branch). Dirty worktrees are reported and kept
/// — resolving a dirty tree is a human decision, not a sweep's.
pub(crate) fn clean_with(repo: &Repo, base: Option<&str>) -> Result<()> {
    let base = match base {
        Some(b) => b.to_string(),
        None => repo
            .git(&["symbolic-ref", "--quiet", "--short", "HEAD"])
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "main".to_string()),
    };
    println!("Sweeping tkw worktrees fully merged into {base:?}...");
    for w in list(repo)? {
        if w.owner != Owner::Tkw {
            continue;
        }
        let Some(branch) = &w.branch else {
            println!("  skip {} (detached HEAD)", w.path.display());
            continue;
        };
        let merged = repo
            .git(&["merge-base", "--is-ancestor", branch, &base])
            .is_ok();
        if !merged {
            continue;
        }
        let path_str = w.path.to_string_lossy().into_owned();
        match repo.git(&["worktree", "remove", &path_str]) {
            Ok(_) => {
                let _ = repo.git(&["branch", "-d", branch]);
                println!("  removed {} ({branch} merged)", w.path.display());
            }
            Err(_) => println!("  skip {} (dirty — resolve by hand)", w.path.display()),
        }
    }
    repo.git_passthrough(&["worktree", "prune"])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_at(main: &str) -> Repo {
        Repo {
            main_root: PathBuf::from(main),
            worktrees_dir: PathBuf::from("/work/tokeira-wt"),
            codex_home: PathBuf::from("/home/u/.codex"),
        }
    }

    #[test]
    fn classify_distinguishes_all_owners() {
        let repo = repo_at("/work/tokeira");
        assert_eq!(classify(Path::new("/work/tokeira"), &repo), Owner::Main);
        assert_eq!(
            classify(Path::new("/work/tokeira-wt/parser"), &repo),
            Owner::Tkw
        );
        assert_eq!(
            classify(Path::new("/work/tokeira/.claude/worktrees/fix"), &repo),
            Owner::Claude
        );
        assert_eq!(
            classify(Path::new("/home/u/.codex/worktrees/abc123"), &repo),
            Owner::Codex
        );
        assert_eq!(
            classify(Path::new("/work/elsewhere/tree"), &repo),
            Owner::Manual
        );
    }

    #[test]
    fn classify_does_not_confuse_prefix_names() {
        // `/work/tokeira-wt` must not classify as inside `/work/tokeira` —
        // starts_with on Path components (not strings) guarantees this.
        let repo = repo_at("/work/tokeira");
        assert_eq!(classify(Path::new("/work/tokeira-wt/x"), &repo), Owner::Tkw);
    }
}
