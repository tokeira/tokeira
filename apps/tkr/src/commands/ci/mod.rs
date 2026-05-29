//! Compatibility CI command surface.
//!
//! These commands are intentionally thin: task 9 owns the CLI contract and
//! Dagger session handoff, while tasks 10–11 own the compatibility Dagger
//! module itself. Until that module exists, execution fails with a setup
//! message instead of silently substituting a local approximation.

use std::{path::Path, process::Stdio};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use tokio::process::Command;

use crate::cli::CiCommand;

const DAGGER_COMPAT_NOT_FOUND: &str = "Dagger compatibility module not found. Run 'tkr ci' after the Dagger module is scaffolded (task 10).";

pub async fn run(command: CiCommand, global_json: bool) -> Result<()> {
    if should_reexec_under_dagger() {
        return reexec_under_dagger(&command, global_json).await;
    }

    match command {
        CiCommand::Check { json, update_lock } => run_check(global_json || json, update_lock).await,
        CiCommand::Build { versioned, json } => run_build(global_json || json, versioned).await,
        CiCommand::LockUpdate { json } => run_lock_update(global_json || json).await,
    }
}

async fn run_check(json: bool, update_lock: bool) -> Result<()> {
    if update_lock {
        return run_lock_update(json).await;
    }
    compatibility_module_missing("check", json, None)
}

async fn run_build(json: bool, versioned: bool) -> Result<()> {
    compatibility_module_missing("build", json, Some(("versioned", versioned)))
}

async fn run_lock_update(json: bool) -> Result<()> {
    compatibility_module_missing("lock-update", json, None)
}

fn compatibility_module_missing(
    command: &'static str,
    json: bool,
    flag: Option<(&'static str, bool)>,
) -> Result<()> {
    if json {
        print_unsupported_json(command, flag);
    }
    bail!("{DAGGER_COMPAT_NOT_FOUND}")
}

fn should_reexec_under_dagger() -> bool {
    std::env::var_os("DAGGER_SESSION_PORT").is_none()
        || std::env::var_os("DAGGER_SESSION_TOKEN").is_none()
}

async fn reexec_under_dagger(command: &CiCommand, global_json: bool) -> Result<()> {
    if !dagger_module_present_in(Path::new(".")) {
        return compatibility_module_missing("dagger", global_json, None);
    }

    let current_exe = std::env::current_exe().context("failed to resolve current executable")?;
    let args = reexec_args(command, global_json);
    let status = Command::new("dagger")
        .arg("run")
        .arg("--")
        .arg(&current_exe)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .context(DAGGER_COMPAT_NOT_FOUND)?;

    if !status.success() {
        bail!("`dagger run` exited with status {status}");
    }
    Ok(())
}

fn dagger_module_present_in(root: &Path) -> bool {
    root.join("dagger.json").exists() || root.join(".dagger").exists()
}

fn reexec_args(command: &CiCommand, global_json: bool) -> Vec<String> {
    let mut args = Vec::new();
    if global_json {
        args.push("--json".to_string());
    }
    args.push("ci".to_string());

    match command {
        CiCommand::Check { json, update_lock } => {
            args.push("check".to_string());
            if *json {
                args.push("--json".to_string());
            }
            if *update_lock {
                args.push("--update-lock".to_string());
            }
        }
        CiCommand::Build { versioned, json } => {
            args.push("build".to_string());
            if *versioned {
                args.push("--versioned".to_string());
            }
            if *json {
                args.push("--json".to_string());
            }
        }
        CiCommand::LockUpdate { json } => {
            args.push("lock-update".to_string());
            if *json {
                args.push("--json".to_string());
            }
        }
    }

    args
}

fn print_unsupported_json(command: &'static str, flag: Option<(&'static str, bool)>) {
    let event = UnsupportedCiCommand {
        command,
        status: "unsupported",
        reason: DAGGER_COMPAT_NOT_FOUND,
        flag,
    };
    if let Ok(rendered) = serde_json::to_string(&event) {
        println!("{rendered}");
    }
}

#[derive(Debug, Serialize)]
struct UnsupportedCiCommand {
    command: &'static str,
    status: &'static str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    flag: Option<(&'static str, bool)>,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{dagger_module_present_in, reexec_args};
    use crate::cli::{CiCommand, Cli, Command};

    #[test]
    fn parses_ci_commands() {
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "check"])
                .unwrap()
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::Check {
                json: false,
                update_lock: false,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "check", "--json"])
                .unwrap()
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::Check {
                json: true,
                update_lock: false,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "build", "--versioned"])
                .unwrap()
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::Build {
                versioned: true,
                json: false,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["tkr", "ci", "lock-update", "--json"])
                .unwrap()
                .command,
            Command::Ci(args) if matches!(args.command, CiCommand::LockUpdate { json: true })
        ));
    }

    #[test]
    fn reexec_args_preserve_ci_flags() {
        let args = reexec_args(
            &CiCommand::Check {
                json: true,
                update_lock: true,
            },
            false,
        );
        assert_eq!(args, ["ci", "check", "--json", "--update-lock"]);

        let args = reexec_args(
            &CiCommand::Build {
                versioned: true,
                json: false,
            },
            true,
        );
        assert_eq!(args, ["--json", "ci", "build", "--versioned"]);
    }

    #[test]
    fn absent_dagger_module_is_detected() {
        let temp = tempfile::tempdir().expect("tempdir");
        assert!(!dagger_module_present_in(temp.path()));
    }
}
