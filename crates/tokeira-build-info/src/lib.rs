//! Compile-time build and compatibility metadata.
//!
//! This crate deliberately has no runtime dependencies and performs no I/O.
//! Build provenance is resolved by `build.rs`, embedded with `env!`, and then
//! consumed by binaries, logs, and compatibility RPCs as immutable data.

pub mod pinned;

/// Tokeira package version embedded in the binary.
pub const TOKEIRA_VERSION: &str = env!("TOKEIRA_BUILD_INFO_VERSION");
/// Short source revision, or a dev-mode fallback when git is unavailable.
pub const TOKEIRA_GIT_SHA: &str = env!("TOKEIRA_BUILD_INFO_GIT_SHA");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildInfo {
    pub tokeira_version: &'static str,
    pub tokeira_git_sha: &'static str,
    pub temporal_proto_version: &'static str,
    pub temporal_server_compat: &'static str,
    pub rust_toolchain: &'static str,
    pub source_tree_hash: &'static str,
    pub feature_matrix_digest: &'static str,
    pub sdk_matrix_digest: &'static str,
    pub build_mode: &'static str,
}

pub const fn summary() -> BuildInfo {
    BuildInfo {
        tokeira_version: TOKEIRA_VERSION,
        tokeira_git_sha: TOKEIRA_GIT_SHA,
        temporal_proto_version: TEMPORAL_PROTO_VERSION,
        temporal_server_compat: TEMPORAL_SERVER_COMPAT,
        rust_toolchain: RUST_TOOLCHAIN,
        source_tree_hash: SOURCE_TREE_HASH,
        feature_matrix_digest: FEATURE_MATRIX_DIGEST,
        sdk_matrix_digest: SDK_MATRIX_DIGEST,
        build_mode: BUILD_MODE,
    }
}
