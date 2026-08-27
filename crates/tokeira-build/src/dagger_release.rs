//! Exact Dagger engine, CLI, and Rust SDK release used by Tokeira pipelines.
//!
//! The engine and SDK are a compatibility pair: the vendored SDK rejects a
//! runner whose version or source revision differs from these coordinates.
//! Keeping the release here gives image bootstrap and CI one pin site instead
//! of letting their session policies drift independently.

/// One checksum-verified Dagger release consumed by local pipelines.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DaggerRelease {
    /// Rust SDK and matching CLI semantic version.
    pub sdk_version: &'static str,
    /// Engine semantic version reported by the pinned runner.
    pub engine_version: &'static str,
    /// Clean Dagger source revision embedded in the engine and SDK.
    pub source_revision: &'static str,
    /// Published OCI archive filename for the local runner.
    pub asset_name: &'static str,
    /// Published OCI archive URL for the local runner.
    pub asset_url: &'static str,
    /// Exact archive length checked before the image is loaded.
    pub asset_size: u64,
    /// Lowercase SHA-256 of the published archive.
    pub asset_sha256: &'static str,
    /// Local image reference produced by loading the verified archive.
    pub image: &'static str,
    /// Persistent local Docker container that hosts the pinned engine.
    pub container: &'static str,
}

impl DaggerRelease {
    /// Return the runner URI understood by the exact-release Dagger CLI.
    pub fn runner_host(self) -> String {
        format!("docker-container://{}", self.container)
    }
}

/// Dated nightly whose rustfmt behavior defines the workspace formatting bar.
pub const CI_FMT_NIGHTLY: &str = "nightly-2026-06-16";

/// Exact engine/CLI/SDK release used by every Tokeira Dagger session.
pub const DAGGER_RELEASE: DaggerRelease = DaggerRelease {
    sdk_version: "1.0.0-beta.11.rust.3",
    engine_version: "v1.0.0-beta.11.rust.3",
    source_revision: "501b57e0476dee5881b99a064c3c04173134ecc7",
    asset_name: "dagger-engine-v1.0.0-beta.11.rust.3-linux-arm64.oci.tar",
    asset_url: "https://github.com/iw/dagger/releases/download/sdk/rust/\
v1.0.0-beta.11.rust.3-apple-silicon/dagger-engine-v1.0.0-beta.11.rust.3-linux-arm64.oci.tar",
    asset_size: 375_404_032,
    asset_sha256: "29077fa248530162d29cbb089b41435dcfb512741dc4b206df798ad886108254",
    image: "tokeira/dagger-engine:v1.0.0-beta.11.rust.3-linux-arm64",
    container: "tokeira-dagger-engine-rust3-arm64",
};

/// Operator command that realizes the checksum-verified local runner.
pub const DAGGER_ENGINE_BOOTSTRAP_COMMAND: &str = "tkr image build";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_coordinates_are_one_exact_pair() {
        assert_eq!(DAGGER_RELEASE.sdk_version, "1.0.0-beta.11.rust.3");
        assert_eq!(
            DAGGER_RELEASE.engine_version.trim_start_matches('v'),
            DAGGER_RELEASE.sdk_version
        );
        assert_eq!(DAGGER_RELEASE.source_revision.len(), 40);
        assert_eq!(DAGGER_RELEASE.asset_sha256.len(), 64);
        assert_eq!(
            DAGGER_RELEASE.runner_host(),
            "docker-container://tokeira-dagger-engine-rust3-arm64"
        );
    }
}
