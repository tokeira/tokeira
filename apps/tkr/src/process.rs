//! Supervised spawn + wait for the local-platform `tokeirad` process.
//!
//! Only the `Local` platform actually runs `tokeirad` as a host process;
//! the Compose and ECS platforms run containers via their own deployment
//! engines. This module exists so `tkr deploy apply` on a local deployment
//! behaves like a well-mannered foreground supervisor:
//!
//! - Prefers an installed `tokeirad` on `PATH`; falls back to `cargo run`
//!   so a developer can iterate without `cargo install`.
//! - Writes `tokeirad.pid` for the lifetime of the child so
//!   [`local_process_status`] can report `Running` / `Stopped` from any
//!   subsequent `tkr deploy status` invocation.
//! - Forwards SIGINT (ctrl-c) to the child on Unix so a single ctrl-c in
//!   the operator's terminal brings the server down cleanly. If the child
//!   does not exit within 5s we escalate to `start_kill` (SIGKILL).

use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, bail};

use crate::{deployment_dir::TOKEIRAD_TOML, metadata::DeploymentStatus};

pub async fn spawn_tokeirad(deployment_path: &Path) -> Result<()> {
    let config_path = deployment_path.join(TOKEIRAD_TOML);

    // Prefer a tokeirad binary on PATH; fall back to `cargo run --bin tokeirad`
    // for development when the binary hasn't been installed.
    let (program, args) = if which::which("tokeirad").is_ok() {
        (
            "tokeirad".to_string(),
            vec!["--config".to_string(), config_path.display().to_string()],
        )
    } else {
        (
            "cargo".to_string(),
            vec![
                "run".to_string(),
                "--bin".to_string(),
                "tokeirad".to_string(),
                "--".to_string(),
                "--config".to_string(),
                config_path.display().to_string(),
            ],
        )
    };

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
    // Wait for either natural exit or operator ctrl-c. Ctrl-c in the
    // parent terminal normally reaches the child directly, but we also
    // explicitly forward SIGINT so the PID file is always cleaned up by
    // *this* supervisor rather than being orphaned by a bash trap.
    let status = tokio::select! {
        status = child.wait() => status?,
        _ = tokio::signal::ctrl_c() => {
            forward_sigint(pid);
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

fn forward_sigint(pid: u32) {
    #[cfg(unix)]
    unsafe {
        // SAFETY: libc::kill is FFI; the only precondition is that `pid`
        // identifies a live process we are allowed to signal. We just
        // spawned this child so the permission check holds, and sending
        // SIGINT to a dead PID is a no-op that returns ESRCH which we
        // ignore.
        let _ = libc::kill(pid as libc::pid_t, libc::SIGINT);
    }

    #[cfg(not(unix))]
    let _ = pid;
}

/// Infer whether a local-platform deployment is currently running by the
/// presence of its PID sentinel. Stale PID files are theoretically possible
/// if the supervisor was SIGKILL'd, but in practice the write/remove
/// bracket in [`spawn_tokeirad`] keeps this trustworthy.
pub fn local_process_status(deployment_path: &Path) -> DeploymentStatus {
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
