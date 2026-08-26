//! Supervised spawn + wait for the local-platform `tokeirad` process.
//!
//! Only the `Local` platform actually runs `tokeirad` as a host process;
//! the Compose and ECS platforms run containers via their own deployment
//! engines. This module exists so `tkr deploy apply` on a local deployment
//! behaves like a well-mannered foreground supervisor:
//!
//! - Discovers the server binary in operator-predictable order: a `tokeirad`
//!   installed next to the running `tkr` binary (a packaged install), then
//!   `PATH`, then a `cargo run` fallback so a developer can iterate without
//!   `cargo install`. The choice is printed so the operator knows which one
//!   is serving.
//! - Writes `tokeirad.pid` for the lifetime of the child so
//!   [`local_process_status`] can report `Running` / `Stopped` from any
//!   subsequent `tkr deploy status` invocation.
//! - Forwards ctrl-c as SIGINT and a supervisor SIGTERM as SIGTERM, so the
//!   server drains gracefully (finishing with its final snapshot) whichever
//!   way the operator or a service manager stops `tkr`. If the child does
//!   not exit within 5s we escalate to `start_kill` (SIGKILL).

use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, bail};

use crate::{deployment_dir::TOKEIRAD_TOML, metadata::DeploymentStatus};

/// How `tokeirad` will be launched, in discovery-preference order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TokeiradLauncher {
    /// A `tokeirad` binary sitting next to the running `tkr` binary.
    Adjacent(PathBuf),
    /// A `tokeirad` resolved from `PATH`.
    OnPath,
    /// Development fallback: `cargo run --bin tokeirad`.
    CargoRun,
}

impl TokeiradLauncher {
    /// Pure selection over the two probe results, so the preference order is
    /// unit-testable without touching the filesystem or `PATH`.
    pub(crate) fn select(adjacent: Option<PathBuf>, on_path: bool) -> Self {
        match (adjacent, on_path) {
            (Some(path), _) => Self::Adjacent(path),
            (None, true) => Self::OnPath,
            (None, false) => Self::CargoRun,
        }
    }

    /// Discover using the real environment.
    fn discover() -> Self {
        let adjacent = std::env::current_exe().ok().and_then(|exe| {
            let candidate = exe
                .parent()?
                .join(format!("tokeirad{}", std::env::consts::EXE_SUFFIX));
            candidate.is_file().then_some(candidate)
        });
        Self::select(adjacent, which::which("tokeirad").is_ok())
    }

    /// The program and leading arguments; `--config <path>` is appended by
    /// the caller.
    fn command(&self) -> (String, Vec<String>) {
        match self {
            Self::Adjacent(path) => (path.display().to_string(), Vec::new()),
            Self::OnPath => ("tokeirad".to_string(), Vec::new()),
            Self::CargoRun => (
                "cargo".to_string(),
                vec![
                    "run".to_string(),
                    "--bin".to_string(),
                    "tokeirad".to_string(),
                    "--".to_string(),
                ],
            ),
        }
    }

    /// One operator-facing line naming what will serve and why.
    fn describe(&self) -> String {
        match self {
            Self::Adjacent(path) => {
                format!("using tokeirad installed beside tkr: {}", path.display())
            }
            Self::OnPath => "using tokeirad from PATH".to_string(),
            Self::CargoRun => {
                "tokeirad not found beside tkr or on PATH; building via `cargo run --bin tokeirad`"
                    .to_string()
            }
        }
    }
}

pub(crate) async fn spawn_tokeirad(deployment_path: &Path) -> Result<()> {
    let config_path = deployment_path.join(TOKEIRAD_TOML);

    let launcher = TokeiradLauncher::discover();
    println!("{}", launcher.describe());
    let (program, mut args) = launcher.command();
    args.push("--config".to_string());
    args.push(config_path.display().to_string());

    let mut child = tokio::process::Command::new(&program)
        .args(&args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "failed to spawn tokeirad with --config {}",
                config_path.display()
            )
        })?;
    let pid = child.id().unwrap_or_default();
    write_pid_file(deployment_path, pid)?;
    println!("started tokeirad pid={pid}");
    // Wait for natural exit, operator ctrl-c, or a supervisor SIGTERM (a
    // service manager stopping `tkr`). The matching signal is forwarded so
    // the server runs its graceful drain — including its final snapshot —
    // and the PID file is always cleaned up by *this* supervisor rather
    // than being orphaned by a bash trap.
    let status = tokio::select! {
        status = child.wait() => status?,
        signal = stop_signal() => {
            forward_signal(pid, signal);
            match tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
                Ok(status) => status?,
                // 5s grace exhausted — escalate to SIGKILL so the
                // operator's terminal is never left hanging on a
                // misbehaving server.
                Err(_) => {
                    let _ = child.start_kill();
                    child.wait().await?
                }
            }
        }
    };
    remove_pid_file(deployment_path)?;
    if !status.success() {
        bail!("tokeirad exited with {status}");
    }
    Ok(())
}

/// Ask the supervised local server to drain and wait until its supervisor
/// removes the PID sentinel.
///
/// A timeout refuses record deletion: the deployment directory contains the
/// configuration and state the still-running process may need, so callers
/// must never remove it merely because a stop signal was sent.
pub(crate) async fn stop_tokeirad(deployment_path: &Path) -> Result<bool> {
    let path = pid_file(deployment_path);
    if !path.exists() {
        return Ok(false);
    }
    let pid = fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?
        .trim()
        .parse::<u32>()
        .with_context(|| format!("{} contains an invalid process id", path.display()))?;

    #[cfg(unix)]
    forward_signal(pid, StopSignal::Terminate);
    #[cfg(not(unix))]
    forward_signal(pid, StopSignal::Interrupt);

    let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    if stopped.is_err() {
        bail!(
            "local tokeirad process {pid} did not stop within 5 seconds; deployment records were retained"
        );
    }
    Ok(true)
}

/// Which stop signal the supervisor received, to forward in kind.
#[derive(Clone, Copy, Debug)]
enum StopSignal {
    Interrupt,
    #[cfg(unix)]
    Terminate,
}

/// Wait for ctrl-c or (on Unix) SIGTERM.
async fn stop_signal() -> StopSignal {
    #[cfg(unix)]
    {
        let sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match sigterm {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => StopSignal::Interrupt,
                    _ = sigterm.recv() => StopSignal::Terminate,
                }
            }
            // If the handler cannot install, ctrl-c alone still supervises.
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                StopSignal::Interrupt
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        StopSignal::Interrupt
    }
}

// The one production `unsafe` in the workspace: libc FFI for signal forwarding.
#[allow(unsafe_code)]
fn forward_signal(pid: u32, signal: StopSignal) {
    #[cfg(unix)]
    {
        let signum = match signal {
            StopSignal::Interrupt => libc::SIGINT,
            StopSignal::Terminate => libc::SIGTERM,
        };
        // SAFETY: libc::kill is FFI; the only precondition is that `pid`
        // identifies a live process we are allowed to signal. We just
        // spawned this child so the permission check holds, and sending
        // a signal to a dead PID is a no-op that returns ESRCH which we
        // ignore.
        unsafe {
            let _ = libc::kill(pid as libc::pid_t, signum);
        }
    }

    #[cfg(not(unix))]
    let _ = (pid, signal);
}

/// Infer whether a local-platform deployment is currently running by the
/// presence of its PID sentinel. Stale PID files are theoretically possible
/// if the supervisor was SIGKILL'd, but in practice the write/remove
/// bracket in [`spawn_tokeirad`] keeps this trustworthy.
pub(crate) fn local_process_status(deployment_path: &Path) -> DeploymentStatus {
    if pid_file(deployment_path).exists() {
        DeploymentStatus::Running
    } else {
        DeploymentStatus::Stopped
    }
}

fn pid_file(deployment_path: &Path) -> PathBuf {
    deployment_path.join("tokeirad.pid")
}

fn write_pid_file(deployment_path: &Path, pid: u32) -> Result<()> {
    fs::write(pid_file(deployment_path), pid.to_string())?;
    Ok(())
}

fn remove_pid_file(deployment_path: &Path) -> Result<()> {
    let path = pid_file(deployment_path);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launcher_prefers_adjacent_then_path_then_cargo() {
        let adjacent = PathBuf::from("/opt/tokeira/bin/tokeirad");
        // An adjacent binary wins even when PATH also has one: the operator
        // installed them together and expects them to run together.
        assert_eq!(
            TokeiradLauncher::select(Some(adjacent.clone()), true),
            TokeiradLauncher::Adjacent(adjacent.clone())
        );
        assert_eq!(
            TokeiradLauncher::select(Some(adjacent.clone()), false),
            TokeiradLauncher::Adjacent(adjacent)
        );
        assert_eq!(
            TokeiradLauncher::select(None, true),
            TokeiradLauncher::OnPath
        );
        assert_eq!(
            TokeiradLauncher::select(None, false),
            TokeiradLauncher::CargoRun
        );
    }

    #[test]
    fn launcher_commands_accept_appended_config_flag() {
        for launcher in [
            TokeiradLauncher::Adjacent(PathBuf::from("/opt/tokeira/bin/tokeirad")),
            TokeiradLauncher::OnPath,
            TokeiradLauncher::CargoRun,
        ] {
            let (_, args) = launcher.command();
            // `--config <path>` is appended by the caller; the cargo fallback
            // must therefore already carry its `--` separator.
            if launcher == TokeiradLauncher::CargoRun {
                assert_eq!(args.last().map(String::as_str), Some("--"));
            } else {
                assert!(args.is_empty());
            }
            assert!(!launcher.describe().is_empty());
        }
    }

    #[tokio::test]
    async fn stopping_an_absent_local_process_is_idempotent() {
        let deployment = tempfile::tempdir().unwrap();
        assert!(!stop_tokeirad(deployment.path()).await.unwrap());
    }
}
