//! Integrity verification (Requirement 8, task 4.2).
//!
//! Before a provisioner binary is executed, its bytes are verified against the
//! `sha256` recorded for its [`Target`] in the [`IntegrityManifest`]. A mismatch
//! **aborts** — the launcher must never run bytes that do not match the manifest
//! (Property 3). The manifest itself is CAS-guarded in the deployment envelope, so
//! it cannot be silently rewritten.

use sha2::{Digest, Sha256};

use crate::{BinaryArtifactDescriptor, IntegrityManifest, Target};

/// Failure verifying a binary against the integrity manifest.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IntegrityError {
    /// The manifest records no artifact for the requested target.
    #[error("no artifact for target '{0}' in the integrity manifest")]
    TargetNotFound(String),
    /// The bytes' checksum does not match the manifest — refuse to execute.
    #[error("integrity checksum mismatch for target '{target}': manifest sha256={expected}, actual={actual}")]
    ChecksumMismatch {
        target: String,
        expected: String,
        actual: String,
    },
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

impl BinaryArtifactDescriptor {
    /// Verify `bytes` against this descriptor's recorded `sha256`. Errors on
    /// mismatch — the caller must not execute the binary (Property 3).
    pub fn verify(&self, bytes: &[u8]) -> Result<(), IntegrityError> {
        let actual = sha256_hex(bytes);
        if actual == self.sha256 {
            Ok(())
        } else {
            Err(IntegrityError::ChecksumMismatch {
                target: self.target.0.clone(),
                expected: self.sha256.clone(),
                actual,
            })
        }
    }
}

impl IntegrityManifest {
    /// The descriptor recorded for `target`, if any.
    pub fn descriptor_for(&self, target: &Target) -> Option<&BinaryArtifactDescriptor> {
        self.artifacts.iter().find(|a| &a.target == target)
    }

    /// Verify a retrieved binary's `bytes` for `target` against the manifest
    /// before execution. Errors if the target is absent
    /// ([`TargetNotFound`](IntegrityError::TargetNotFound)) or the checksum
    /// mismatches ([`ChecksumMismatch`](IntegrityError::ChecksumMismatch)).
    pub fn verify_artifact(&self, bytes: &[u8], target: &Target) -> Result<(), IntegrityError> {
        let descriptor = self
            .descriptor_for(target)
            .ok_or_else(|| IntegrityError::TargetNotFound(target.0.clone()))?;
        descriptor.verify(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(bytes: &[u8], target: &str) -> IntegrityManifest {
        IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                version: "1.0.0".to_string(),
                target: Target(target.to_string()),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
        }
    }

    #[test]
    fn matching_bytes_verify() {
        let bytes = b"provisioner-binary-bytes";
        let target = Target("aarch64-unknown-linux-musl".to_string());
        let m = manifest(bytes, &target.0);
        m.verify_artifact(bytes, &target).expect("matching bytes verify");
    }

    #[test]
    fn tampered_bytes_abort() {
        let bytes = b"provisioner-binary-bytes";
        let target = Target("aarch64-unknown-linux-musl".to_string());
        let m = manifest(bytes, &target.0);
        let err = m
            .verify_artifact(b"tampered-bytes", &target)
            .expect_err("tampered bytes abort");
        assert!(matches!(err, IntegrityError::ChecksumMismatch { .. }));
    }

    #[test]
    fn unknown_target_is_not_found() {
        let bytes = b"bytes";
        let m = manifest(bytes, "aarch64-unknown-linux-musl");
        let err = m
            .verify_artifact(bytes, &Target("aarch64-apple-darwin".to_string()))
            .expect_err("unknown target errors");
        assert!(matches!(err, IntegrityError::TargetNotFound(_)));
    }

    #[test]
    fn sha256_hex_is_stable_and_lowercase() {
        let h = sha256_hex(b"abc");
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
