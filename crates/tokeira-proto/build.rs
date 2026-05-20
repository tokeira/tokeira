use std::{
    env, fs,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = find_workspace_root()?;
    let proto_root = workspace_root.join("proto");
    let upstream_dir = proto_root.join("upstream");
    let internal_dir = proto_root.join("tokeira");
    let controller_dir = internal_dir.join("internal/controller");

    println!("cargo:rerun-if-changed={}", upstream_dir.display());
    println!("cargo:rerun-if-changed={}", internal_dir.display());

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);

    // ── Upstream (Temporal API) — tonic/prost only ───────────────────────────
    let upstream_protos = discover_protos(&upstream_dir)?;
    if !upstream_protos.is_empty() {
        tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .btree_map(["."])
            .file_descriptor_set_path(out_dir.join("tokeira_public_descriptor.bin"))
            .compile(&upstream_protos, &[upstream_dir.as_path()])?;
    }

    // ── Internal (Tokeira) — tonic/prost for backward compat ────────────────
    let internal_protos = discover_protos(&internal_dir)?;
    if !internal_protos.is_empty() {
        tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .btree_map(["."])
            .file_descriptor_set_path(out_dir.join("tokeira_internal_descriptor.bin"))
            .compile(
                &internal_protos,
                &[proto_root.as_path(), upstream_dir.as_path()],
            )?;
    }

    // ── Controller proto — connectrpc-build (buffa + service stubs) ─────────
    // Generates zero-copy view types and connect-rust service traits for the
    // internal controller surface. The tonic output above is retained for any
    // code that hasn't migrated yet.
    let controller_protos = discover_protos(&controller_dir)?;
    if !controller_protos.is_empty() {
        connectrpc_build::Config::new()
            .files(
                &controller_protos
                    .iter()
                    .map(|p| p.to_str().unwrap())
                    .collect::<Vec<_>>(),
            )
            .includes(&[proto_root.to_str().unwrap()])
            .include_file("_connectrpc_controller.rs")
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
