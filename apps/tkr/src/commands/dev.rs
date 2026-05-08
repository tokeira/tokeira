//! `tkr dev` — thin shim over common workspace cargo commands.
//!
//! Exists so contributors have a single discoverable verb for the
//! most common loops (build, test, clippy, fmt, docs) without having
//! to remember the exact flag combinations agreed upon in AGENTS.md
//! (workspace-wide, all-targets, nightly fmt, etc.).

use anyhow::{Result, bail};

use crate::cli::DevAction;

pub fn run(action: DevAction) -> Result<()> {
    let mut command = match action {
        DevAction::Build => {
            let mut command = std::process::Command::new("cargo");
            command.args(["build", "--workspace"]);
            command
        }
        DevAction::Test { crate_name } => {
            let mut command = std::process::Command::new("cargo");
            if let Some(crate_name) = crate_name {
                command.args(["test", "-p", &crate_name]);
            } else {
                command.args(["test", "--workspace"]);
            }
            command
        }
        DevAction::Check => {
            let mut command = std::process::Command::new("cargo");
            command.args(["check", "--workspace"]);
            command
        }
        DevAction::Lint => {
            let mut command = std::process::Command::new("cargo");
            command.args(["clippy", "--workspace", "--all-targets"]);
            command
        }
        DevAction::Fmt => {
            let mut command = std::process::Command::new("cargo");
            command.args(["+nightly", "fmt"]);
            command
        }
        DevAction::Docs => {
            let mut command = std::process::Command::new("cargo");
            command.args(["doc", "--workspace", "--no-deps"]);
            command
        }
    };
    let status = command.status()?;
    if !status.success() {
        bail!("developer command failed with {status}");
    }
    Ok(())
}
