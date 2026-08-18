//! Role keys as sources: local Ed25519 files or KMS-held RSA keys.
//!
//! A role key is a `tough::KeySource`; the publisher never learns which kind
//! it holds — that substitutability is the whole KMS story. Local Ed25519
//! files are raw PKCS#8 DER (`tough::sign::parse_keypair` feeds bytes to
//! `Ed25519KeyPair::from_pkcs8` before any PEM decoding); KMS keys are RSA
//! with `RSASSA_PSS_SHA_256` — the `tough-kms` 0.16 support surface, named
//! in the refusal when an unsupported spec is configured.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use serde::{Deserialize, Serialize};
use tough::{
    key_source::{KeySource, LocalKeySource},
    sign::Sign,
};

use super::error::PublishError;

/// One role's key, as configuration. `deny_unknown_fields` per the config
/// ownership rules; no environment variables are read (AWS SDK ambient
/// credential resolution is the sanctioned exception, inside `tough-kms`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum KeySourceConfig {
    /// Ed25519 PKCS#8 DER file, the local default.
    File {
        /// Absolute path to the key file.
        path: PathBuf,
    },
    /// KMS RSA signing key (`RSASSA_PSS_SHA_256` only at tough-kms 0.16).
    Kms {
        /// Key id, key ARN, or alias.
        key_id: String,
        /// Optional AWS profile for the KMS client.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        profile: Option<String>,
    },
}

impl KeySourceConfig {
    /// Construct the shareable key source this configuration names.
    pub fn source(&self) -> SharedKeySource {
        match self {
            Self::File { path } => SharedKeySource::new(LocalKeySource { path: path.clone() }),
            Self::Kms { key_id, profile } => SharedKeySource::new(tough_kms::KmsKeySource {
                profile: profile.clone(),
                key_id: key_id.clone(),
                client: None,
                signing_algorithm: tough_kms::KmsSigningAlgorithm::RsassaPssSha256,
            }),
        }
    }
}

/// One key configuration per TUF role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleKeyConfig {
    /// Offline trust anchor; exercised only when authoring/rotating root.
    pub root: KeySourceConfig,
    /// Online role: signs the target inventory.
    pub targets: KeySourceConfig,
    /// Online role: signs the metadata snapshot.
    pub snapshot: KeySourceConfig,
    /// Online role: signs the freshness statement.
    pub timestamp: KeySourceConfig,
}

impl RoleKeyConfig {
    /// Generate the local defaults for a local deployment: four fresh
    /// Ed25519 keys under `dir` (which must sit under the deployments root,
    /// outside the repository itself).
    pub fn generate_local(dir: &Path) -> Result<Self, PublishError> {
        std::fs::create_dir_all(dir).map_err(|error| {
            PublishError::Other(format!("creating key directory {}: {error}", dir.display()))
        })?;
        let rng = SystemRandom::new();
        let write = |name: &str| -> Result<KeySourceConfig, PublishError> {
            let doc =
                Ed25519KeyPair::generate_pkcs8(&rng).map_err(|error| PublishError::Signing {
                    role: "keygen",
                    error: error.to_string(),
                })?;
            let path = dir.join(format!("{name}.ed25519.der"));
            std::fs::write(&path, doc.as_ref()).map_err(|error| {
                PublishError::Other(format!("writing {}: {error}", path.display()))
            })?;
            Ok(KeySourceConfig::File { path })
        };
        Ok(Self {
            root: write("root")?,
            targets: write("targets")?,
            snapshot: write("snapshot")?,
            timestamp: write("timestamp")?,
        })
    }

    /// The online-role sources handed to the repository editor's signer.
    pub fn online_sources(&self) -> Vec<Box<dyn KeySource>> {
        vec![
            self.targets.source().boxed(),
            self.snapshot.source().boxed(),
            self.timestamp.source().boxed(),
        ]
    }
}

/// A shareable `KeySource`: `tough` takes `Box<dyn KeySource>` slices in
/// several shapes and `KeySource` is not clonable — the `Arc` shim lets one
/// configured source (file or KMS) serve every call site.
#[derive(Debug, Clone)]
pub struct SharedKeySource(Arc<dyn KeySource>);

impl SharedKeySource {
    /// Wrap a concrete key source.
    pub fn new(source: impl KeySource + 'static) -> Self {
        Self(Arc::new(source))
    }

    /// Mint a boxed clone for APIs that want ownership.
    pub fn boxed(&self) -> Box<dyn KeySource> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl KeySource for SharedKeySource {
    async fn as_sign(
        &self,
    ) -> Result<Box<dyn Sign>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.0.as_sign().await
    }

    async fn write(
        &self,
        value: &str,
        key_id_hex: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.0.write(value, key_id_hex).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_config_serde_round_trips_and_rejects_unknowns() {
        let config = RoleKeyConfig {
            root: KeySourceConfig::File {
                path: PathBuf::from("/keys/root.ed25519.der"),
            },
            targets: KeySourceConfig::Kms {
                key_id: "alias/deploy".to_string(),
                profile: None,
            },
            snapshot: KeySourceConfig::Kms {
                key_id: "arn:aws:kms:eu-west-2:1:key/k".to_string(),
                profile: Some("ops".to_string()),
            },
            timestamp: KeySourceConfig::File {
                path: PathBuf::from("/keys/timestamp.ed25519.der"),
            },
        };
        let json = serde_json::to_value(&config).unwrap();
        let back: RoleKeyConfig = serde_json::from_value(json).unwrap();
        assert_eq!(back, config);

        let unknown = serde_json::json!({"kind": "file", "path": "/k", "extra": true});
        assert!(serde_json::from_value::<KeySourceConfig>(unknown).is_err());
    }

    #[tokio::test]
    async fn generated_local_keys_load_and_sign() {
        let dir = tempfile::tempdir().unwrap();
        let config = RoleKeyConfig::generate_local(dir.path()).unwrap();
        // Every generated key parses back through tough's own loader and
        // produces a signer (KMS construction is config-only; never called).
        for source in [
            &config.root,
            &config.targets,
            &config.snapshot,
            &config.timestamp,
        ] {
            let signer = source.source().as_sign().await.expect("key loads");
            let _ = signer.tuf_key();
        }
    }
}
