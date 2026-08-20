//! Compatibility CI command surface.
//!
//! These commands are intentionally thin: the compatibility pipeline itself
//! does not exist yet, and execution fails with a plain statement of that
//! fact instead of silently substituting a local approximation. When the
//! pipeline arrives it connects its own in-process Dagger session, the same
//! way the image and bundle flows do — no wrapper process, no session
//! environment variables.

use anyhow::{Result, bail};
use serde::Serialize;

use crate::cli::CiCommand;

const CI_PIPELINE_MISSING: &str =
    "tkr ci is not yet implemented: this workspace defines no compatibility CI pipeline.";

pub(crate) async fn run(command: CiCommand, global_json: bool) -> Result<()> {
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
    ci_pipeline_missing("check", json, None)
}

async fn run_build(json: bool, versioned: bool) -> Result<()> {
    ci_pipeline_missing("build", json, Some(("versioned", versioned)))
}

async fn run_lock_update(json: bool) -> Result<()> {
    ci_pipeline_missing("lock-update", json, None)
}

fn ci_pipeline_missing(
    command: &'static str,
    json: bool,
    flag: Option<(&'static str, bool)>,
) -> Result<()> {
    if json {
        print_unsupported_json(command, flag);
    }
    bail!("{CI_PIPELINE_MISSING}")
}

fn print_unsupported_json(command: &'static str, flag: Option<(&'static str, bool)>) {
    let event = UnsupportedCiCommand {
        command,
        status: "unsupported",
        reason: CI_PIPELINE_MISSING,
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
}
