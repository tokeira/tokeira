use std::{
    env, fs,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = find_workspace_root()?;
    let proto_root = workspace_root.join("proto");
    let conformance_dir = proto_root.join("tokeira/conformance");

    println!("cargo:rerun-if-changed={}", conformance_dir.display());

    let protos = discover_protos(&conformance_dir)?;
    if !protos.is_empty() {
        connectrpc_build::Config::new()
            .files(
                &protos
                    .iter()
                    .map(|path| {
                        path.to_str()
                            .ok_or_else(|| format!("non-UTF8 proto path: {}", path.display()))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .includes(&[proto_root
                .to_str()
                .ok_or("workspace proto root path is not UTF-8")?])
            .include_file("_connectrpc_conformance.rs")
            .compile()?;
    }

    Ok(())
}

fn find_workspace_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    loop {
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.is_file() {
            let contents = fs::read_to_string(&cargo_toml)?;
            if contents.contains("[workspace]") {
                return Ok(dir);
            }
        }
        if !dir.pop() {
            break;
        }
    }
    Err("workspace root not found".into())
}

fn discover_protos(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut protos = WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "proto"))
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    protos.sort();
    Ok(protos)
}
