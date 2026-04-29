use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};

use crate::deployment_dir::TOKEIRAD_TOML;
use crate::metadata::DeploymentStatus;

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
    let status = tokio::select! {
        status = child.wait() => status?,
        _ = tokio::signal::ctrl_c() => {
            forward_sigint(pid);
            match tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
                Ok(status) => status?,
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
        let _ = libc::kill(pid as libc::pid_t, libc::SIGINT);
    }

    #[cfg(not(unix))]
    let _ = pid;
}

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
