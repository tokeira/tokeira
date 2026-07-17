//! Copy gitignored-but-needed files into a fresh worktree.
//!
//! A fresh worktree contains tracked files only; local configuration such as
//! `.env` must be carried over explicitly. The patterns live in the tracked
//! `.worktreeinclude` at the repo root — the same file Claude Code and the
//! ChatGPT app read natively — so every agent's worktrees get identical
//! treatment. tkw applies it for the worktrees it creates (Kiro CLI, manual).
//!
//! Implementation note, and the reason this module exists at all: the obvious
//! approach — enumerate every ignored file and match it against the patterns —
//! is quadratic in practice and pathological in this repository (a populated
//! `target/` puts the ignored-file count above a million; enumerating it takes
//! over 30s before any matching starts). Instead we go pattern-first: each
//! `.worktreeinclude` line becomes git pathspecs handed to a single
//! `git ls-files`, so cost scales with the number of *matches*.

use std::{collections::BTreeSet, path::Path};

use anyhow::{Context, Result};

use crate::repo::Repo;

/// Translate one `.worktreeinclude` line into git pathspecs.
///
/// `.worktreeinclude` uses `.gitignore` glob semantics, where a pattern with
/// no slash matches at any depth. Git *pathspec* globs are rooted, so the
/// slashless case expands to two pathspecs: the root match and the any-depth
/// match.
pub(crate) fn pathspecs_for(pattern: &str) -> Vec<String> {
    if pattern.contains('/') {
        vec![format!(":(glob){pattern}")]
    } else {
        vec![format!(":(glob){pattern}"), format!(":(glob)**/{pattern}")]
    }
}

/// Parse `.worktreeinclude` (comments and blanks stripped). Missing file = no
/// patterns, which is fine: copying is best-effort enrichment, not setup.
fn patterns(main_root: &Path) -> Vec<String> {
    let Ok(content) = std::fs::read_to_string(main_root.join(".worktreeinclude")) else {
        return Vec::new();
    };
    content
        .lines()
        .map(|line| line.split('#').next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// Copy every gitignored file matching `.worktreeinclude` from the main
/// checkout into `dest`. Returns the number of files copied.
pub(crate) fn copy_into(repo: &Repo, dest: &Path) -> Result<usize> {
    let patterns = patterns(&repo.main_root);
    if patterns.is_empty() {
        return Ok(0);
    }

    let mut args: Vec<String> = [
        "ls-files",
        "--others",
        "--ignored",
        "--exclude-standard",
        "-z",
        "--",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for pattern in &patterns {
        args.extend(pathspecs_for(pattern));
    }
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = repo.git(&arg_refs)?;

    // BTreeSet: the root and any-depth pathspecs can both match one file.
    let files: BTreeSet<&str> = output.split('\0').filter(|f| !f.is_empty()).collect();
    let mut copied = 0;
    for file in files {
        let source = repo.main_root.join(file);
        if !source.is_file() {
            continue;
        }
        let target = dest.join(file);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        std::fs::copy(&source, &target)
            .with_context(|| format!("failed to copy {file} into the worktree"))?;
        println!("  copied {file}");
        copied += 1;
    }
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, process::Command};

    use super::*;

    #[test]
    fn slashless_patterns_match_at_any_depth() {
        assert_eq!(
            pathspecs_for(".env"),
            vec![":(glob).env".to_string(), ":(glob)**/.env".to_string()]
        );
    }

    #[test]
    fn slashed_patterns_stay_rooted() {
        assert_eq!(
            pathspecs_for("config/secrets.json"),
            vec![":(glob)config/secrets.json".to_string()]
        );
    }

    /// End-to-end against a real git repo: root and nested `.env` are copied,
    /// other ignored files are not.
    #[test]
    fn copies_only_matching_ignored_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let main = dir.path().join("main");
        let dest = dir.path().join("dest");
        std::fs::create_dir_all(main.join("svc")).expect("mkdir");
        std::fs::create_dir_all(&dest).expect("mkdir");
        let git = |args: &[&str]| {
            let status = Command::new("git")
                .arg("-C")
                .arg(&main)
                .args(args)
                .status()
                .expect("spawn git");
            assert!(status.success(), "git {args:?}");
        };
        git(&["init", "--quiet"]);
        std::fs::write(main.join(".gitignore"), ".env\n*.log\n").expect("write");
        std::fs::write(main.join(".worktreeinclude"), "# local env\n.env\n").expect("write");
        std::fs::write(main.join(".env"), "A=1\n").expect("write");
        std::fs::write(main.join("svc").join(".env"), "B=2\n").expect("write");
        std::fs::write(main.join("noise.log"), "ignored, not included\n").expect("write");

        let repo = Repo {
            main_root: main.clone(),
            worktrees_dir: PathBuf::from("/unused"),
            codex_home: PathBuf::from("/unused"),
        };
        let copied = copy_into(&repo, &dest).expect("copy_into");
        assert_eq!(copied, 2);
        assert!(dest.join(".env").is_file());
        assert!(dest.join("svc").join(".env").is_file());
        assert!(!dest.join("noise.log").exists());
    }
}
