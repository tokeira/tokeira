//! The Dagger release that Tokeira's pipelines run on, and the dated nightly
//! that formats the workspace.

/// The Dagger CLI and engine release that `tokeira-dagger-sdk` 0.7 provisions.
///
/// The SDK downloads this CLI release, checksum-verified, and the CLI starts
/// the engine of the same release. Release plans record it as their Dagger
/// identity, so it advances with the SDK.
pub const DAGGER_VERSION: &str = "v1.0.0-beta.27";

/// Dated nightly whose rustfmt behavior defines the workspace formatting bar.
pub const CI_FMT_NIGHTLY: &str = "nightly-2026-06-16";
