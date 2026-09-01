//! Compile-time build and compatibility metadata.
//!
//! This crate deliberately has no runtime dependencies and performs no I/O.
//! Build provenance is resolved by `build.rs`, embedded with `env!`, and then
//! consumed by binaries, logs, and compatibility RPCs as immutable data.

pub mod pinned;

#[cfg(test)]
#[path = "provenance.rs"]
mod provenance;

/// Tokeira package version embedded in the binary.
pub const TOKEIRA_VERSION: &str = env!("TOKEIRA_BUILD_INFO_VERSION");
/// Short source revision, or a dev-mode fallback when git is unavailable.
pub const TOKEIRA_GIT_SHA: &str = env!("TOKEIRA_BUILD_INFO_GIT_SHA");
/// Full source revision used to fence external tool bootstraps.
pub const TOKEIRA_SOURCE_REVISION: &str = env!("TOKEIRA_BUILD_INFO_SOURCE_REVISION");
/// SDK-visible server version with source identity in SemVer build metadata.
pub const SERVER_VERSION: &str = env!("TOKEIRA_BUILD_INFO_SERVER_VERSION");
/// Vendored upstream Temporal proto version.
pub const TEMPORAL_PROTO_VERSION: &str = env!("TOKEIRA_BUILD_INFO_PROTO_VERSION");
/// Temporal server version whose SDK-visible behavior Tokeira claims to match.
pub const TEMPORAL_SERVER_COMPAT: &str = env!("TOKEIRA_BUILD_INFO_SERVER_COMPAT");
/// Rust toolchain channel pinned by `rust-toolchain.toml`.
pub const RUST_TOOLCHAIN: &str = env!("TOKEIRA_BUILD_INFO_RUST_TOOLCHAIN");
/// Deterministic source-tree digest produced by the versioned Dagger build.
pub const SOURCE_TREE_HASH: &str = env!("TOKEIRA_BUILD_INFO_SOURCE_TREE_HASH");
/// Digest of the checked-in feature compatibility matrix.
pub const FEATURE_MATRIX_DIGEST: &str = env!("TOKEIRA_BUILD_INFO_FEATURE_MATRIX_DIGEST");
/// Digest of the checked-in SDK compatibility matrix.
pub const SDK_MATRIX_DIGEST: &str = env!("TOKEIRA_BUILD_INFO_SDK_MATRIX_DIGEST");
/// Build mode: `dev` for local fallback builds, `versioned` for manifest builds.
pub const BUILD_MODE: &str = env!("TOKEIRA_BUILD_INFO_BUILD_MODE");
/// Oldest Aurora DSQL schema version readable by this release.
pub const SCHEMA_MIN_SUPPORTED_VERSION: u32 =
    parse_decimal(env!("TOKEIRA_BUILD_INFO_SCHEMA_MIN_SUPPORTED_VERSION"));
/// Aurora DSQL schema version produced by this release's migrations.
pub const SCHEMA_TARGET_VERSION: u32 =
    parse_decimal(env!("TOKEIRA_BUILD_INFO_SCHEMA_TARGET_VERSION"));
/// Newest Aurora DSQL schema version readable by this release.
pub const SCHEMA_MAX_READABLE_VERSION: u32 =
    parse_decimal(env!("TOKEIRA_BUILD_INFO_SCHEMA_MAX_READABLE_VERSION"));
/// Canonical digest of every recognized Aurora DSQL migration.
pub const SCHEMA_MIGRATION_SET_DIGEST: &str =
    env!("TOKEIRA_BUILD_INFO_SCHEMA_MIGRATION_SET_DIGEST");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildInfo {
    pub tokeira_version: &'static str,
    pub tokeira_git_sha: &'static str,
    /// Full 40-hex source revision when the build can determine one (the build
    /// manifest, an injected environment value, or live Git, in that order); the
    /// same degraded sentinel as `tokeira_git_sha` for a local release build.
    pub tokeira_source_revision: &'static str,
    pub temporal_proto_version: &'static str,
    pub temporal_server_compat: &'static str,
    pub rust_toolchain: &'static str,
    pub source_tree_hash: &'static str,
    pub feature_matrix_digest: &'static str,
    pub sdk_matrix_digest: &'static str,
    pub build_mode: &'static str,
    /// Oldest Aurora DSQL schema version readable by this release.
    pub schema_min_supported_version: u32,
    /// Aurora DSQL schema version produced by this release's migrations.
    pub schema_target_version: u32,
    /// Newest Aurora DSQL schema version readable by this release.
    pub schema_max_readable_version: u32,
    /// Canonical digest of every recognized Aurora DSQL migration.
    pub schema_migration_set_digest: &'static str,
}

pub const fn summary() -> BuildInfo {
    BuildInfo {
        tokeira_version: TOKEIRA_VERSION,
        tokeira_git_sha: TOKEIRA_GIT_SHA,
        tokeira_source_revision: TOKEIRA_SOURCE_REVISION,
        temporal_proto_version: TEMPORAL_PROTO_VERSION,
        temporal_server_compat: TEMPORAL_SERVER_COMPAT,
        rust_toolchain: RUST_TOOLCHAIN,
        source_tree_hash: SOURCE_TREE_HASH,
        feature_matrix_digest: FEATURE_MATRIX_DIGEST,
        sdk_matrix_digest: SDK_MATRIX_DIGEST,
        build_mode: BUILD_MODE,
        schema_min_supported_version: SCHEMA_MIN_SUPPORTED_VERSION,
        schema_target_version: SCHEMA_TARGET_VERSION,
        schema_max_readable_version: SCHEMA_MAX_READABLE_VERSION,
        schema_migration_set_digest: SCHEMA_MIGRATION_SET_DIGEST,
    }
}

const fn parse_decimal(value: &str) -> u32 {
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut parsed = 0_u32;
    while index < bytes.len() {
        let byte = bytes[index];
        assert!(
            byte.is_ascii_digit(),
            "build metadata integer is not decimal"
        );
        parsed = match parsed.checked_mul(10) {
            Some(value) => value,
            None => panic!("build metadata integer overflow"),
        };
        parsed = match parsed.checked_add((byte - b'0') as u32) {
            Some(value) => value,
            None => panic!("build metadata integer overflow"),
        };
        index += 1;
    }
    parsed
}

#[cfg(test)]
mod tests {
    use std::{fs, process::Command};

    use super::*;

    #[test]
    fn build_info_exposes_the_storage_owned_schema_contract() {
        let info = summary();
        assert_eq!(info.schema_min_supported_version, 1);
        assert_eq!(info.schema_target_version, 67);
        assert_eq!(info.schema_max_readable_version, 67);
        assert_eq!(
            info.schema_migration_set_digest,
            "sha256:fb8d7c84c771a8cc9a9a8a53dca33a195d4ac7377e76df273171e7ee3d5e5892"
        );
    }

    #[test]
    fn server_version_is_semver_with_git_sha_build_metadata() {
        let package = tempfile::tempdir().expect("temporary semver package");
        fs::create_dir(package.path().join("src")).expect("create source directory");
        fs::write(
            package.path().join("Cargo.toml"),
            format!(
                "[package]\nname = \"server-version-check\"\nversion = \"{SERVER_VERSION}\"\nedition = \"2024\"\n"
            ),
        )
        .expect("write semver manifest");
        fs::write(package.path().join("src/lib.rs"), "").expect("write source");

        let output = Command::new("cargo")
            .args([
                "metadata",
                "--offline",
                "--no-deps",
                "--format-version=1",
                "--manifest-path",
            ])
            .arg(package.path().join("Cargo.toml"))
            .output()
            .expect("run cargo metadata");
        assert!(
            output.status.success(),
            "server version must parse as SemVer: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            SERVER_VERSION
                .rsplit_once('+')
                .map(|(_, metadata)| metadata),
            Some(TOKEIRA_GIT_SHA)
        );
    }

    // Both parity tests guard the packaged-build fallbacks: a registry archive
    // carries no workspace, so `build.rs` reads a pinned toolchain constant and
    // an in-crate copy of the storage crate's schema contract. In the
    // workspace, the authoritative files exist and must match exactly.

    #[test]
    fn pinned_toolchain_matches_the_workspace_toolchain_file() {
        let workspace_file = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../rust-toolchain.toml"
        ))
        .expect("read rust-toolchain.toml (workspace-only parity test)");
        let channel = workspace_file
            .lines()
            .find_map(|line| line.strip_prefix("channel = \""))
            .and_then(|rest| rest.strip_suffix('"'))
            .expect("rust-toolchain.toml carries a quoted channel");
        assert_eq!(
            pinned::PINNED_RUST_TOOLCHAIN,
            channel,
            "update PINNED_RUST_TOOLCHAIN alongside rust-toolchain.toml"
        );
    }

    #[test]
    fn packaged_schema_contract_matches_the_storage_owned_file() {
        let packaged = fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/schema-contract.toml"))
            .expect("read the packaged schema-contract copy");
        let authoritative = fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tokeira-storage/schema-contract.toml"
        ))
        .expect("read the storage schema contract (workspace-only parity test)");
        assert_eq!(
            packaged, authoritative,
            "re-copy crates/tokeira-storage/schema-contract.toml into tokeira-build-info"
        );
    }
}
