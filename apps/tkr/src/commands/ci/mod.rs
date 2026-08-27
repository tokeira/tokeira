//! `tkr ci` — the pinned, in-process Dagger local-CI surface.
//!
//! Normal checks and builds use frozen workspace locks. Lock refresh is a named
//! live-resolution transaction, leaving `dagger.lock` as the sole reviewable host
//! mutation. No command re-execs through a wrapper or accepts ambient session state.

use std::{collections::BTreeSet, fs, path::Path, process::Command};

use anyhow::{Context, Result, bail};
use dagger_sdk::LockMode;
use serde::Serialize;
use tokeira_build::{
    CiBuildMode, CiBuildRequest, CiCheckReport, CiCheckRequest, run_ci_build, run_ci_checks,
};

use crate::cli::{CiCommand, CliCiCheck};

use super::image::{ci_dagger_session, workspace_root_from_current_dir};

pub(crate) async fn run(command: CiCommand, global_json: bool) -> Result<()> {
    match command {
        CiCommand::Check {
            json,
            update_lock,
            checks,
        } => run_check(global_json || json, update_lock, checks).await,
        CiCommand::Build { versioned, json } => run_build(global_json || json, versioned).await,
        CiCommand::LockUpdate { json } => run_lock_update(global_json || json).await,
    }
}

async fn run_check(json: bool, update_lock: bool, checks: Vec<CliCiCheck>) -> Result<()> {
    if update_lock {
        return run_lock_update(json).await;
    }
    let root = workspace_root_from_current_dir()?;
    let lock_before = read_optional(&root.join("dagger.lock"))?;
    let client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let report = run_ci_checks(
        &CiCheckRequest {
            workspace_root: root.clone(),
            checks: checks.into_iter().map(Into::into).collect(),
        },
        &client,
    )
    .await?;
    client
        .close()
        .await
        .context("close the Dagger CI session")?;
    let lock_after = read_optional(&root.join("dagger.lock"))?;
    if lock_before != lock_after {
        bail!("frozen CI modified dagger.lock; restore and review the unexpected lock mutation");
    }
    render_check_report(&report, json)?;
    if !report.passed() {
        bail!("tkr ci check failed");
    }
    Ok(())
}

impl From<CliCiCheck> for tokeira_build::CiCheck {
    fn from(check: CliCiCheck) -> Self {
        match check {
            CliCiCheck::ProtoMonotonicity => Self::ProtoMonotonicity,
            CliCiCheck::ServerCompatMonotonicity => Self::ServerCompatMonotonicity,
            CliCiCheck::BumpTrailer => Self::BumpTrailer,
            CliCiCheck::Fmt => Self::Fmt,
            CliCiCheck::Lint => Self::Lint,
            CliCiCheck::Check => Self::Check,
            CliCiCheck::Nextest => Self::Nextest,
            CliCiCheck::Doctests => Self::Doctests,
            CliCiCheck::Rustdoc => Self::Rustdoc,
            CliCiCheck::Deny => Self::Deny,
            CliCiCheck::Links => Self::Links,
        }
    }
}

async fn run_build(json: bool, versioned: bool) -> Result<()> {
    let root = workspace_root_from_current_dir()?;
    let lock_before = read_optional(&root.join("dagger.lock"))?;
    let client = ci_dagger_session(&root, LockMode::Frozen).await?;
    let mode = if versioned {
        CiBuildMode::Versioned
    } else {
        CiBuildMode::Dev
    };
    let report = run_ci_build(
        &CiBuildRequest {
            workspace_root: root.clone(),
            mode,
        },
        &client,
    )
    .await?;
    client
        .close()
        .await
        .context("close the Dagger CI session")?;
    if lock_before != read_optional(&root.join("dagger.lock"))? {
        bail!(
            "frozen CI build modified dagger.lock; restore and review the unexpected lock mutation"
        );
    }
    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!(
            "tkr ci build passed ({mode:?}); artifact: {}",
            report.artifact.display()
        );
    }
    Ok(())
}

async fn run_lock_update(json: bool) -> Result<()> {
    let root = workspace_root_from_current_dir()?;
    let lock_path = root.join("dagger.lock");
    let before = read_optional(&lock_path)?;
    let workspace_before = workspace_state_without_lock(&root)?;
    let client = ci_dagger_session(&root, LockMode::Live).await?;
    // These are the compatibility and bar consumers of the mutable inputs. Running
    // them in the explicit live transaction both discovers the pins and verifies the
    // resolved graph before it becomes a reviewable lock diff.
    let checks = run_ci_checks(
        &CiCheckRequest {
            workspace_root: root.clone(),
            checks: Vec::new(),
        },
        &client,
    )
    .await?;
    client
        .close()
        .await
        .context("close the Dagger lock-update session")?;
    let after = read_optional(&lock_path)?;
    if workspace_before != workspace_state_without_lock(&root)? {
        bail!("lock-update mutated a working-tree path other than dagger.lock");
    }
    let report = CiLockUpdateReport {
        changed: changed_lock_references(before.as_deref(), after.as_deref()),
        checks,
    };
    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("Dagger lock update complete");
        render_changed("container images", &report.changed.container_images);
        render_changed("Git references", &report.changed.git_references);
        render_changed("HTTP fetches", &report.changed.http_fetches);
        render_human_checks(&report.checks);
    }
    if !report.checks.passed() {
        bail!("Dagger locks were refreshed, but the resulting CI checks failed");
    }
    Ok(())
}

fn render_check_report(report: &CiCheckReport, json: bool) -> Result<()> {
    if json {
        // Requirement 6 makes this exact report the remote-runner handoff; wrapping
        // it in CLI metadata would force downstream consumers to re-shape evidence.
        println!("{}", serde_json::to_string(report)?);
    } else {
        render_human_checks(report);
        if report.passed() {
            println!("tkr ci check passed ({} checks)", report.results.len());
        }
    }
    Ok(())
}

fn render_human_checks(report: &CiCheckReport) {
    for result in &report.results {
        let marker = if result.passed { "PASS" } else { "FAIL" };
        println!("[{marker}] {}: {}", result.check.name(), result.summary);
        if !result.passed
            && let Some(details) = &result.details
        {
            println!("{details}");
        }
    }
}

fn render_changed(label: &str, values: &[String]) {
    if values.is_empty() {
        println!("{label}: unchanged");
    } else {
        println!("{label}:");
        for value in values {
            println!("  {value}");
        }
    }
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn workspace_state_without_lock(root: &Path) -> Result<Vec<u8>> {
    let diff = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args([
            "diff",
            "--binary",
            "HEAD",
            "--",
            ".",
            ":(exclude)dagger.lock",
        ])
        .output()
        .context("capture working-tree diff before lock update")?;
    if !diff.status.success() {
        bail!(
            "could not capture working-tree diff: {}",
            String::from_utf8_lossy(&diff.stderr).trim()
        );
    }
    let status = Command::new("git")
        .args(["-C"])
        .arg(root)
        .args(["status", "--porcelain=v1", "-z", "--untracked-files=normal"])
        .output()
        .context("capture working-tree paths before lock update")?;
    if !status.status.success() {
        bail!(
            "could not capture working-tree paths: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        );
    }
    let mut state = diff.stdout;
    for entry in status.stdout.split(|byte| *byte == 0) {
        if entry.ends_with(b" dagger.lock") || entry.ends_with(b"dagger.lock") {
            continue;
        }
        state.extend_from_slice(entry);
        state.push(0);
    }
    Ok(state)
}

#[derive(Debug, Serialize)]
struct CiLockUpdateReport {
    changed: ChangedLockReferences,
    checks: CiCheckReport,
}

#[derive(Debug, Default, Serialize)]
struct ChangedLockReferences {
    container_images: Vec<String>,
    git_references: Vec<String>,
    http_fetches: Vec<String>,
}

fn changed_lock_references(before: Option<&[u8]>, after: Option<&[u8]>) -> ChangedLockReferences {
    let before = lock_lines(before);
    let after = lock_lines(after);
    let changed = before
        .symmetric_difference(&after)
        .cloned()
        .collect::<Vec<_>>();
    let mut references = ChangedLockReferences::default();
    for line in changed {
        if line.contains("container.from") {
            references.container_images.push(line);
        } else if line.contains("git.") || line.contains("git://") || line.contains("github.com") {
            references.git_references.push(line);
        } else if line.contains("http") {
            references.http_fetches.push(line);
        }
    }
    references
}

fn lock_lines(content: Option<&[u8]>) -> BTreeSet<String> {
    content
        .map(String::from_utf8_lossy)
        .into_iter()
        .flat_map(|content| {
            content
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::cli::{CiCommand, Cli, Command};

    #[test]
    fn parses_ci_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "check"])
                .expect("parse check")
                .command,
            Command::Ci(args) if matches!(&args.command, CiCommand::Check {
                json: false,
                update_lock: false,
                checks,
            } if checks.is_empty())
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "check", "--check", "fmt", "--check", "lint"])
                .expect("parse selected checks")
                .command,
            Command::Ci(args) if matches!(&args.command, CiCommand::Check {
                json: false,
                update_lock: false,
                checks,
            } if matches!(checks.as_slice(), [CliCiCheck::Fmt, CliCiCheck::Lint]))
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "check", "--json"])
                .expect("parse JSON check")
                .command,
            Command::Ci(args) if matches!(&args.command, CiCommand::Check {
                json: true,
                update_lock: false,
                checks,
            } if checks.is_empty())
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "build"])
                .expect("parse dev build")
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::Build {
                versioned: false,
                json: false,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "build", "--versioned"])
                .expect("parse versioned build")
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::Build {
                versioned: true,
                json: false,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "lock-update", "--json"])
                .expect("parse lock update")
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::LockUpdate { json: true })
        ));
    }

    #[test]
    fn lock_diff_classifies_supply_chain_inputs() {
        let before = br#"[["version","1"]]
["","container.from",["debian:old","linux/arm64"],"sha256:old","pin"]
"#;
        let after = br#"[["version","1"]]
["","container.from",["debian:new","linux/arm64"],"sha256:new","pin"]
["","git.ref",["https://github.com/example/repo","main"],"abc","pin"]
"#;
        let changed = changed_lock_references(Some(before), Some(after));
        assert_eq!(changed.container_images.len(), 2);
        assert_eq!(changed.git_references.len(), 1);
        assert!(changed.http_fetches.is_empty());
    }
}
