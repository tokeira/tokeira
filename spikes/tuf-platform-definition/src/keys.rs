//! Signing-key material for the spike: generated Ed25519 keys behind
//! `tough`'s `LocalKeySource`, and the role split a real deployment would use.
//!
//! TUF separates the offline trust anchor (root role) from the online
//! publishing roles (targets, snapshot, timestamp). The spike generates one
//! Ed25519 key per role so that separation is visible in the metadata; the
//! KMS variant (`kms.rs`) swaps individual `KeySource`s without touching
//! anything else — that substitutability is one of the claims under test.
//!
//! Ed25519 pkcs8 DER is written raw (no PEM wrapper): `tough::sign::parse_keypair`
//! feeds the bytes straight to `Ed25519KeyPair::from_pkcs8` before it tries
//! any PEM decoding.

use std::path::{Path, PathBuf};

use anyhow::Context;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use tough::key_source::{KeySource, LocalKeySource};

/// One generated key file per TUF role.
#[derive(Debug, Clone)]
pub struct RoleKeyFiles {
    /// Offline trust anchor.
    pub root: PathBuf,
    /// Signs the target inventory.
    pub targets: PathBuf,
    /// Signs the metadata snapshot.
    pub snapshot: PathBuf,
    /// Signs the freshness statement.
    pub timestamp: PathBuf,
}

impl RoleKeyFiles {
    /// Generate four fresh Ed25519 keys under `dir`.
    pub fn generate(dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let rng = SystemRandom::new();
        let write = |name: &str| -> anyhow::Result<PathBuf> {
            let doc = Ed25519KeyPair::generate_pkcs8(&rng)
                .map_err(|e| anyhow::anyhow!("ed25519 keygen: {e}"))?;
            let path = dir.join(format!("{name}.ed25519.der"));
            std::fs::write(&path, doc.as_ref())
                .with_context(|| format!("writing {}", path.display()))?;
            Ok(path)
        };
        Ok(Self {
            root: write("root")?,
            targets: write("targets")?,
            snapshot: write("snapshot")?,
            timestamp: write("timestamp")?,
        })
    }

    /// The key sources in role order (root, targets, snapshot, timestamp).
    pub fn sources(&self) -> Vec<Box<dyn KeySource>> {
        [&self.root, &self.targets, &self.snapshot, &self.timestamp]
            .into_iter()
            .map(|path| Box::new(LocalKeySource { path: path.clone() }) as Box<dyn KeySource>)
            .collect()
    }

    /// A single role's key source.
    pub fn source(&self, path: &Path) -> Box<dyn KeySource> {
        Box::new(LocalKeySource {
            path: path.to_path_buf(),
        })
    }
}
