use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use regex::Regex;
use toml::Value;

#[path = "src/provenance.rs"]
mod provenance;

const VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_VERSION";
const GIT_SHA_KEY: &str = "TOKEIRA_BUILD_INFO_GIT_SHA";
const SERVER_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_SERVER_VERSION";
const PROTO_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_PROTO_VERSION";
const SERVER_COMPAT_KEY: &str = "TOKEIRA_BUILD_INFO_SERVER_COMPAT";
const RUST_TOOLCHAIN_KEY: &str = "TOKEIRA_BUILD_INFO_RUST_TOOLCHAIN";
const SOURCE_TREE_HASH_KEY: &str = "TOKEIRA_BUILD_INFO_SOURCE_TREE_HASH";
const FEATURE_MATRIX_DIGEST_KEY: &str = "TOKEIRA_BUILD_INFO_FEATURE_MATRIX_DIGEST";
const SDK_MATRIX_DIGEST_KEY: &str = "TOKEIRA_BUILD_INFO_SDK_MATRIX_DIGEST";
const BUILD_MODE_KEY: &str = "TOKEIRA_BUILD_INFO_BUILD_MODE";
const SCHEMA_MIN_SUPPORTED_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_SCHEMA_MIN_SUPPORTED_VERSION";
const SCHEMA_TARGET_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_SCHEMA_TARGET_VERSION";
const SCHEMA_MAX_READABLE_VERSION_KEY: &str = "TOKEIRA_BUILD_INFO_SCHEMA_MAX_READABLE_VERSION";
const SCHEMA_MIGRATION_SET_DIGEST_KEY: &str = "TOKEIRA_BUILD_INFO_SCHEMA_MIGRATION_SET_DIGEST";

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaContractMetadata {
    minimum_supported_version: u32,
    target_version: u32,
    maximum_readable_version: u32,
    migration_set_digest: String,
}

fn main() {
    println!("cargo:rerun-if-env-changed=TOKEIRA_BUILD_MANIFEST_PATH");
    println!("cargo:rerun-if-env-changed=TOKEIRA_GIT_SHA");
    println!("cargo:rerun-if-env-changed=TOKEIRA_BUILD_INFO_GIT_SHA");
    println!("cargo:rerun-if-env-changed=TOKEIRA_SOURCE_TREE_HASH");
    println!("cargo:rerun-if-env-changed=CI");
    println!("cargo:rerun-if-env-changed=PROFILE");

    let Some(root) = workspace_root() else {
        packaged_build();
        return;
    };
    println!(
        "cargo:rerun-if-changed={}",
        root.join("rust-toolchain.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
    println!("cargo:rerun-if-changed=src/pinned.rs");
    println!("cargo:rerun-if-changed=src/provenance.rs");
    let schema_contract_path = root.join("crates/tokeira-storage/schema-contract.toml");
    println!("cargo:rerun-if-changed={}", schema_contract_path.display());

    let manifest_path = resolve_manifest_path();
    let parsed_manifest = match manifest_path.as_ref() {
        Some(path) => {
            let content = fs::read_to_string(path).unwrap_or_else(|error| {
                panic!(
                    "failed to read build metadata manifest at {}: {error}",
                    path.display()
                )
            });
            Some(parse_manifest(&content).unwrap_or_else(|error| {
                panic!(
                    "failed to parse build metadata manifest at {}: {error}",
                    path.display()
                )
            }))
        }
        None => None,
    };
    let injected_git_sha = injected_git_sha();
    let supplied_git_sha = parsed_manifest
        .as_ref()
        .map(|manifest| manifest.git_sha.as_str())
        .or(injected_git_sha.as_deref());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_owned());
    let ci_is_set = env::var_os("CI").is_some();
    let provenance =
        provenance::resolve_git_sha(&profile, ci_is_set, supplied_git_sha, || git_sha(&root))
            .unwrap_or_else(|error| panic!("build provenance gate failed: {error}"));
    if provenance.warn_local_release {
        println!(
            "cargo::warning=release build outside CI carries degraded TOKEIRA_GIT_SHA=dev provenance"
        );
    }

    let mut manifest = parsed_manifest.unwrap_or_else(|| {
        dev_fallback_manifest(&root)
            .unwrap_or_else(|error| panic!("failed to derive development build metadata: {error}"))
    });
    manifest.git_sha = provenance.value;
    if manifest_path.is_none() {
        if let Some(source_tree_hash) = non_empty_env("TOKEIRA_SOURCE_TREE_HASH") {
            manifest.source_tree_hash = source_tree_hash;
        }
        if profile == "release" && ci_is_set {
            manifest.build_mode = "versioned".to_owned();
        }
    }
    if provenance.warn_local_release {
        manifest.source_tree_hash = dev_source_tree_hash();
        manifest.build_mode = "dev".to_owned();
    }

    let schema_contract = read_schema_contract(&schema_contract_path).unwrap_or_else(|error| {
        panic!(
            "failed to read schema compatibility contract at {}: {error}",
            schema_contract_path.display()
        )
    });

    emit_manifest(&manifest);
    emit_schema_contract(&schema_contract);
}

/// Identity for a build of the published crate: a registry archive carries no
/// workspace, no git repository, and no toolchain pin. Everything is derived
/// from files packaged inside the crate, and the provenance gate deliberately
/// does not run — it is a control on Tokeira's own release pipeline, and a
/// downstream consumer's release CI (where `CI` is set but no
/// `TOKEIRA_GIT_SHA` can exist) must not fail on it.
fn packaged_build() {
    let pinned_path = manifest_dir().join("src/pinned.rs");
    let (proto_version, server_compat) = read_pinned_versions(&pinned_path)
        .unwrap_or_else(|error| panic!("failed to read packaged pins: {error}"));
    let pinned_source = fs::read_to_string(&pinned_path)
        .unwrap_or_else(|error| panic!("failed to read packaged pinned.rs: {error}"));
    let rust_toolchain = capture_const(&pinned_source, "PINNED_RUST_TOOLCHAIN")
        .unwrap_or_else(|error| panic!("failed to read packaged toolchain pin: {error}"));

    let manifest = Manifest {
        version: env::var("CARGO_PKG_VERSION").expect("Cargo sets CARGO_PKG_VERSION"),
        git_sha: "crates-io".to_owned(),
        proto_version,
        server_compat,
        rust_toolchain,
        source_tree_hash: dev_source_tree_hash(),
        feature_matrix_digest: "dev".to_owned(),
        sdk_matrix_digest: "dev".to_owned(),
        build_mode: "dev".to_owned(),
    };
    // The packaged copy of the storage crate's schema contract; a parity test
    // keeps the two byte-identical in the workspace.
    let schema_contract = read_schema_contract(&manifest_dir().join("schema-contract.toml"))
        .unwrap_or_else(|error| panic!("failed to read packaged schema contract: {error}"));

    emit_manifest(&manifest);
    emit_schema_contract(&schema_contract);
}

/// The Tokeira workspace root, or `None` when building outside it (a registry
/// archive, or a copy vendored into some other project's workspace). A bare
/// `[workspace]` ancestor is not enough — `cargo vendor` places this crate
/// inside arbitrary downstream workspaces — so the root must also carry the
/// storage crate's schema contract, the file workspace mode exists to read.
fn workspace_root() -> Option<PathBuf> {
    let mut dir = manifest_dir();
    loop {
        let manifest = dir.join("Cargo.toml");
        if manifest.is_file()
            && fs::read_to_string(&manifest)
                .map(|contents| contents.contains("[workspace]"))
                .unwrap_or(false)
            && dir
                .join("crates/tokeira-storage/schema-contract.toml")
                .is_file()
        {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

fn manifest_dir() -> PathBuf {
    // Cargo supplies this for every build-script invocation. Reading it at runtime keeps a
    // cached build-script valid when its workspace is moved without discarding the warm target.
    PathBuf::from(
        env::var("CARGO_MANIFEST_DIR")
            .expect("Cargo must set CARGO_MANIFEST_DIR for build scripts"),
    )
}

fn resolve_manifest_path() -> Option<PathBuf> {
    env::var_os("TOKEIRA_BUILD_MANIFEST_PATH").map(PathBuf::from)
}

fn injected_git_sha() -> Option<String> {
    non_empty_env("TOKEIRA_GIT_SHA").or_else(|| non_empty_env(GIT_SHA_KEY))
}

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
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
        read_pinned_versions(&manifest_dir().join("src/pinned.rs"))?;

    Ok(Manifest {
        version: env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0-dev".to_owned()),
        // The profile gate replaces this placeholder before emission.
        git_sha: "dev".to_owned(),
        proto_version,
        server_compat,
        rust_toolchain: read_rust_toolchain(&root.join("rust-toolchain.toml"))?,
        source_tree_hash: dev_source_tree_hash(),
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

fn read_schema_contract(path: &Path) -> Result<SchemaContractMetadata, String> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let value = content
        .parse::<Value>()
        .map_err(|error| format!("failed to parse schema-contract.toml: {error}"))?;
    let integer = |key: &str| -> Result<u32, String> {
        let raw = value
            .get(key)
            .and_then(Value::as_integer)
            .ok_or_else(|| format!("schema contract missing integer {key}"))?;
        u32::try_from(raw).map_err(|_| format!("schema contract {key} is out of range"))
    };
    let string = |key: &str| -> Result<String, String> {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("schema contract missing string {key}"))
    };

    if integer("format_version")? != 1 {
        return Err("schema contract format_version must be 1".to_owned());
    }
    let release = string("tokeira_release")?;
    let package_version = env::var("CARGO_PKG_VERSION")
        .map_err(|error| format!("CARGO_PKG_VERSION is unavailable: {error}"))?;
    if release != package_version {
        return Err(format!(
            "schema contract release {release} does not match package version {package_version}"
        ));
    }
    let minimum_supported_version = integer("minimum_supported_version")?;
    let target_version = integer("target_version")?;
    let maximum_readable_version = integer("maximum_readable_version")?;
    if minimum_supported_version == 0
        || minimum_supported_version > target_version
        || target_version > maximum_readable_version
    {
        return Err("schema versions must satisfy 0 < minimum <= target <= maximum".to_owned());
    }
    let migration_set_digest = string("migration_set_digest")?;
    let Some(hex) = migration_set_digest.strip_prefix("sha256:") else {
        return Err("schema migration-set digest must use the sha256: prefix".to_owned());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("schema migration-set digest must be lowercase SHA-256".to_owned());
    }

    Ok(SchemaContractMetadata {
        minimum_supported_version,
        target_version,
        maximum_readable_version,
        migration_set_digest,
    })
}

fn dev_source_tree_hash() -> String {
    "0000000000000000000000000000000000000000000000000000000000000000".to_owned()
}

fn git_sha(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("--short=8")
        .arg("HEAD")
        .output();

    match output {
        Ok(output) if output.status.success() => {
            Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
                .filter(|value| !value.is_empty())
        }
        _ => None,
    }
}

fn emit_manifest(manifest: &Manifest) {
    // Provenance belongs in SemVer build metadata so SDK release-version gates keep
    // comparing only the package version. The resulting opaque identity is threaded
    // into the edge constructor; the edge does not need to own build-provenance policy.
    let server_version = format!("{}+{}", manifest.version, manifest.git_sha);
    emit(VERSION_KEY, &manifest.version);
    emit(GIT_SHA_KEY, &manifest.git_sha);
    emit(SERVER_VERSION_KEY, &server_version);
    emit(PROTO_VERSION_KEY, &manifest.proto_version);
    emit(SERVER_COMPAT_KEY, &manifest.server_compat);
    emit(RUST_TOOLCHAIN_KEY, &manifest.rust_toolchain);
    emit(SOURCE_TREE_HASH_KEY, &manifest.source_tree_hash);
    emit(FEATURE_MATRIX_DIGEST_KEY, &manifest.feature_matrix_digest);
    emit(SDK_MATRIX_DIGEST_KEY, &manifest.sdk_matrix_digest);
    emit(BUILD_MODE_KEY, &manifest.build_mode);
}

fn emit_schema_contract(contract: &SchemaContractMetadata) {
    emit(
        SCHEMA_MIN_SUPPORTED_VERSION_KEY,
        &contract.minimum_supported_version.to_string(),
    );
    emit(
        SCHEMA_TARGET_VERSION_KEY,
        &contract.target_version.to_string(),
    );
    emit(
        SCHEMA_MAX_READABLE_VERSION_KEY,
        &contract.maximum_readable_version.to_string(),
    );
    emit(
        SCHEMA_MIGRATION_SET_DIGEST_KEY,
        &contract.migration_set_digest,
    );
}

fn emit(key: &str, value: &str) {
    println!("cargo:rustc-env={key}={value}");
}
