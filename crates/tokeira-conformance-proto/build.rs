//! Compile conformance protos with protox and generate Buffa/Connect bindings.
//! Descriptor bytes preserve imported types, options and source comments without
//! requiring an external protobuf compiler on the build host.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = find_workspace_root()?;
    let proto_root = workspace_root.join("proto");
    let conformance_dir = proto_root.join("tokeira/conformance");

    // Imports can live outside this package's own proto directory.
    println!("cargo:rerun-if-changed={}", proto_root.display());
    // Disabling generated file directives also disables this descriptor decode input.
    println!("cargo:rerun-if-env-changed=BUFFA_ELEMENT_MEMORY_LIMIT");

    let protos = discover_protos(&conformance_dir)?;
    if !protos.is_empty() {
        let mut compiler = protox::Compiler::new([&proto_root])?;
        compiler
            .include_imports(true)
            .include_source_info(true)
            .open_files(&protos)?;
        let descriptor = PathBuf::from(env::var("OUT_DIR")?).join("conformance-descriptors.bin");
        fs::write(&descriptor, compiler.encode_file_descriptor_set())?;
        // Precompiled descriptor mode selects proto-relative names, unlike
        // connectrpc-build's protoc mode, which accepts filesystem paths.
        let files = protos
            .iter()
            .map(|path| path.strip_prefix(&proto_root))
            .collect::<Result<Vec<_>, _>>()?;
        connectrpc_build::Config::new()
            .descriptor_set(descriptor)
            .files(&files)
            .emit_rerun_directives(false)
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
