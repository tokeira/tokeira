use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use regex::Regex;
use toml::Value;

const VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_VERSION";
const GIT_SHA_KEY: &str = "TOKEIRA_BUILD_INFO_GIT_SHA";
const PROTO_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_PROTO_VERSION";
const SERVER_COMPAT_KEY: &str = "TOKEIRA_BUILD_INFO_SERVER_COMPAT";
const RUST_TOOLCHAIN_KEY: &str = "TOKEIRA_BUILD_INFO_RUST_TOOLCHAIN";
const SOURCE_TREE_HASH_KEY: &str = "TOKEIRA_BUILD_INFO_SOURCE_TREE_HASH";
const FEATURE_MATRIX_DIGEST_KEY: &str = "TOKEIRA_BUILD_INFO_FEATURE_MATRIX_DIGEST";
const SDK_MATRIX_DIGEST_KEY: &str = "TOKEIRA_BUILD_INFO_SDK_MATRIX_DIGEST";
const BUILD_MODE_KEY: &str = "TOKEIRA_BUILD_INFO_BUILD_MODE";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Manifest {
    version: String,
    git_sha: String,
    proto_version: String,
    server_compat: String,
    rust_toolchain: String,
    source_tree_hash: String,
    feature_matrix_digest: String,
    sdk_matrix_digest: String,
    build_mode: String,
}

fn main() {
    println!("cargo:rerun-if-env-changed=TOKEIRA_BUILD_MANIFEST_PATH");

    let root = workspace_root();
    println!(
        "cargo:rerun-if-changed={}",
        root.join("rust-toolchain.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
    println!("cargo:rerun-if-changed=src/pinned.rs");

    let manifest = match resolve_manifest_path() {
        Some(path) => {
            let content = fs::read_to_string(&path).unwrap_or_else(|error| {
                panic!(
                    "failed to read build metadata manifest at {}: {error}",
                    path.display()
                )
            });
            parse_manifest(&content).unwrap_or_else(|error| {
                panic!(
                    "failed to parse build metadata manifest at {}: {error}",
                    path.display()
                )
            })
        }
        None => dev_fallback_manifest(&root)
            .unwrap_or_else(|error| panic!("failed to derive development build metadata: {error}")),
    };

    emit_manifest(&manifest);
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("../.."))
}

fn resolve_manifest_path() -> Option<PathBuf> {
    env::var_os("TOKEIRA_BUILD_MANIFEST_PATH").map(PathBuf::from)
}

fn parse_manifest(content: &str) -> Result<Manifest, String> {
    let pairs = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            line.split_once('=')
                .map(|(key, value)| (key.trim(), value.trim()))
                .ok_or_else(|| format!("manifest line is missing '=': {line}"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let value = |key: &str| -> Result<String, String> {
        pairs
            .iter()
            .find_map(|(candidate, value)| (*candidate == key).then(|| (*value).to_owned()))
            .ok_or_else(|| format!("manifest missing required key {key}"))
    };

    Ok(Manifest {
        version: value("TOKEIRA_VERSION")?,
        git_sha: value("TOKEIRA_GIT_SHA")?,
        proto_version: value("TEMPORAL_PROTO_VERSION")?,
        server_compat: value("TEMPORAL_SERVER_COMPAT")?,
        rust_toolchain: value("RUST_TOOLCHAIN")?,
        source_tree_hash: value("SOURCE_TREE_HASH")?,
        feature_matrix_digest: value("FEATURE_MATRIX_DIGEST")?,
        sdk_matrix_digest: value("SDK_MATRIX_DIGEST")?,
        build_mode: value("BUILD_MODE")?,
    })
}

fn dev_fallback_manifest(root: &Path) -> Result<Manifest, String> {
    let (proto_version, server_compat) =
        read_pinned_versions(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/pinned.rs"))?;

    Ok(Manifest {
        version: env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0-dev".to_owned()),
        git_sha: git_sha(root),
        proto_version,
        server_compat,
        rust_toolchain: read_rust_toolchain(&root.join("rust-toolchain.toml"))?,
        source_tree_hash: "0000000000000000000000000000000000000000000000000000000000000000"
            .to_owned(),
        feature_matrix_digest: "dev".to_owned(),
        sdk_matrix_digest: "dev".to_owned(),
        build_mode: "dev".to_owned(),
    })
}

fn read_pinned_versions(path: &Path) -> Result<(String, String), String> {
    let content =
        fs::read_to_string(path).map_err(|error| format!("failed to read pinned.rs: {error}"))?;
    let proto = capture_const(&content, "TEMPORAL_PROTO_VERSION")?;
    let compat = capture_const(&content, "TEMPORAL_SERVER_COMPAT")?;
    Ok((proto, compat))
}

fn capture_const(content: &str, name: &str) -> Result<String, String> {
    let pattern = format!(r#"pub const {name}: &str = "([^"]+)";"#);
    let regex = Regex::new(&pattern)
        .map_err(|error| format!("failed to build regex for {name}: {error}"))?;
    regex
        .captures(content)
        .and_then(|captures| captures.get(1))
        .map(|match_| match_.as_str().to_owned())
        .ok_or_else(|| format!("missing pinned const {name}"))
}

fn read_rust_toolchain(path: &Path) -> Result<String, String> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let value = content
        .parse::<Value>()
        .map_err(|error| format!("failed to parse rust-toolchain.toml: {error}"))?;
    value
        .get("toolchain")
        .and_then(|toolchain| toolchain.get("channel"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "rust-toolchain.toml missing [toolchain].channel".to_owned())
}

fn git_sha(root: &Path) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("--short=8")
        .arg("HEAD")
        .output();

    match output {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        _ => "unknown".to_owned(),
    }
}

fn emit_manifest(manifest: &Manifest) {
    emit(VERSION_KEY, &manifest.version);
    emit(GIT_SHA_KEY, &manifest.git_sha);
    emit(PROTO_VERSION_KEY, &manifest.proto_version);
    emit(SERVER_COMPAT_KEY, &manifest.server_compat);
    emit(RUST_TOOLCHAIN_KEY, &manifest.rust_toolchain);
    emit(SOURCE_TREE_HASH_KEY, &manifest.source_tree_hash);
    emit(FEATURE_MATRIX_DIGEST_KEY, &manifest.feature_matrix_digest);
    emit(SDK_MATRIX_DIGEST_KEY, &manifest.sdk_matrix_digest);
    emit(BUILD_MODE_KEY, &manifest.build_mode);
}

fn emit(key: &str, value: &str) {
    println!("cargo:rustc-env={key}={value}");
}
