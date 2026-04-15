use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{self, Command},
};

use anyhow::{Context, Result, anyhow, bail};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let version = parse_version_arg()?;
    let workspace_root = find_workspace_root()?;
    let upstream_dir = workspace_root.join("proto/upstream");
    let version_file = workspace_root.join("proto/UPSTREAM_VERSION");

    clean_upstream_dir(&upstream_dir)?;
    buf_export(&version, &upstream_dir)?;
    write_version_file(&version_file, &version)?;

    println!(
        "synced Temporal API protos {version} into {}",
        upstream_dir.display()
    );
    Ok(())
}

fn parse_version_arg() -> Result<String> {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "proto-sync".to_string());
    let Some(version) = args.next() else {
        bail!("usage: {program} <version>");
    };
    if args.next().is_some() {
        bail!("usage: {program} <version>");
    }
    Ok(version)
}

fn find_workspace_root() -> Result<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while let Some(parent) = dir.parent() {
        let cargo_toml = parent.join("Cargo.toml");
        if cargo_toml.is_file() {
            let contents = fs::read_to_string(&cargo_toml)
                .with_context(|| format!("read {}", cargo_toml.display()))?;
            if contents.contains("[workspace]") {
                return Ok(parent.to_path_buf());
            }
        }
        dir = parent.to_path_buf();
    }
    Err(anyhow!(
        "workspace root not found from {}",
        env!("CARGO_MANIFEST_DIR")
    ))
}

fn clean_upstream_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| format!("remove {}", path.display()))?;
    }
    fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    Ok(())
}

fn buf_export(version: &str, output: &Path) -> Result<()> {
    let status = Command::new("buf")
        .arg("export")
        .arg(format!("buf.build/temporalio/api:{version}"))
        .arg("--output")
        .arg(output)
        .status()
        .context("failed to invoke buf; ensure `buf` is installed and on PATH")?;

    if !status.success() {
        bail!("buf export failed for version {version} with status {status}");
    }
    Ok(())
}

fn write_version_file(path: &Path, version: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create {}", parent.display()))?;
    }
    fs::write(path, version).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
