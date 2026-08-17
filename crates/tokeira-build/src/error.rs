use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("rust-toolchain.toml not found or unreadable at {path}")]
    ToolchainFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("rust-toolchain.toml could not be parsed: {0}")]
    ToolchainParse(String),

    #[error("unsupported architecture '{supplied}'; expected 'arm64' or 'amd64'")]
    UnsupportedArch { supplied: String },

    #[error("Dagger session not available and 'dagger' CLI is not on PATH")]
    DaggerMissing,

    #[error("publish failed for {remote_ref}: {source}")]
    Publish {
        remote_ref: String,
        #[source]
        source: eyre::Report,
    },

    #[error("mirror failed for {source_ref} -> {remote_ref}: {source}")]
    Mirror {
        source_ref: String,
        remote_ref: String,
        #[source]
        source: eyre::Report,
    },

    #[error("upstream source authentication failed")]
    UpstreamAuth,

    #[error("request validation failed: {reason}")]
    Validation { reason: String },

    #[error("bundle store: {0}")]
    BundleStore(#[from] tokeira_deployment::BundleStoreError),
}
