//! Local AWS Session Manager client discovery and process ownership.
//!
//! ECS operations resolve provider targets themselves, but the AWS CLI and
//! Session Manager plugin own the interactive wire protocol. Values are
//! passed as distinct argv entries without a local shell; inherited stdio
//! gives the operator the terminal, and cancellation always terminates and
//! reaps the direct client process.

use std::{
    env,
    ffi::OsStr,
    future::Future,
    io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
};

use anyhow::{Context as _, Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionCommand {
    pub(super) program: PathBuf,
    pub(super) args: Vec<String>,
}

/// Resolve both clients before an operation creates a provider session.
///
/// The AWS CLI invokes the Session Manager plugin, so validating both up
/// front avoids allocating a short-lived remote session that the workstation
/// cannot attach to.
pub(super) fn require_client_tools() -> Result<PathBuf> {
    let aws_cli = required_executable("aws", "AWS CLI")?;
    required_executable("session-manager-plugin", "Session Manager plugin")?;
    Ok(aws_cli)
}

fn required_executable(name: &str, display_name: &str) -> Result<PathBuf> {
    find_executable_in(env::var_os("PATH").as_deref(), name).ok_or_else(|| {
        anyhow::anyhow!(
            "{display_name} executable `{name}` is not installed or not executable on PATH"
        )
    })
}

fn find_executable_in(path: Option<&OsStr>, name: &str) -> Option<PathBuf> {
    env::split_paths(path?).find_map(|directory| {
        let candidate = directory.join(name);
        is_executable(&candidate).then_some(candidate)
    })
}

fn is_executable(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

pub(super) async fn run_session(command: SessionCommand) -> Result<()> {
    let mut child = tokio::process::Command::new(&command.program)
        .args(&command.args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        // A cancelled provisioner future must not orphan a session even if
        // cancellation happens outside the explicit Ctrl-C branch.
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "failed to start AWS Session Manager through `{}`",
                command.program.display()
            )
        })?;

    wait_for_session(&mut child, tokio::signal::ctrl_c()).await
}

async fn wait_for_session<F>(child: &mut tokio::process::Child, interrupted: F) -> Result<()>
where
    F: Future<Output = io::Result<()>>,
{
    tokio::select! {
        status = child.wait() => check_session_status(status?),
        interrupted = interrupted => {
            interrupted.context("failed to listen for Ctrl-C while the Session Manager client was active")?;
            // Reap after terminating so neither a live client nor a zombie
            // survives the owning operation.
            if child.try_wait()?.is_none() {
                child.kill().await.context("failed to terminate the AWS Session Manager client")?;
            }
            Ok(())
        }
    }
}

fn check_session_status(status: ExitStatus) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        bail!("AWS Session Manager client exited with status {status}")
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[cfg(unix)]
    #[test]
    fn executable_lookup_requires_an_executable_regular_file() {
        let temp = tempfile::tempdir().expect("temporary PATH");
        let executable = temp.path().join("aws");
        std::fs::write(&executable, "").expect("fake executable");
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
            .expect("mark executable");
        std::fs::create_dir(temp.path().join("session-manager-plugin")).expect("fake directory");
        let search_path = env::join_paths([temp.path()]).expect("PATH value");

        assert_eq!(
            find_executable_in(Some(&search_path), "aws"),
            Some(executable)
        );
        assert_eq!(
            find_executable_in(Some(&search_path), "session-manager-plugin"),
            None
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_interrupt_terminates_and_reaps_the_session_process() {
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "read value"])
            .stdin(Stdio::piped())
            .spawn()
            .expect("blocking child process");

        wait_for_session(&mut child, std::future::ready(Ok(())))
            .await
            .expect("operator interruption is a clean session close");

        assert!(
            child.try_wait().expect("read child status").is_some(),
            "terminated child is reaped before the operation returns"
        );
    }
}
