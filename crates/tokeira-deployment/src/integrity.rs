//! Integrity verification (Requirement 8, task 4.2).
//!
//! Before a provisioner binary is executed, its bytes are verified against the
//! `sha256` recorded for its [`Target`] in the [`IntegrityManifest`]. A mismatch
//! **aborts** — the launcher must never run bytes that do not match the manifest
//! (Property 3). The manifest itself is CAS-guarded in the deployment envelope, so
//! it cannot be silently rewritten.
//!
//! Two hardening layers sit under that guarantee:
//!
//! - **Manifest well-formedness** ([`IntegrityManifest::validate`]): checksum
//!   strings are *parsed* into a fixed 32-byte digest ([`Sha256Digest`]) rather
//!   than compared as loose strings, and a manifest that records the same target
//!   twice is rejected. Call it once the manifest is in hand (after the enclosing
//!   envelope has passed its own CAS check). It proves internal shape only — never
//!   that the manifest is authentic.
//! - **A self-defending verification path** ([`IntegrityManifest::verify_artifact`]):
//!   even without a prior `validate`, the verify path rejects a duplicate target
//!   for the artifact under verification, so an ambiguous manifest cannot slip a
//!   shadowed descriptor past the check.
//!
//! **On comparison:** the digest equality is a plain byte compare, deliberately
//! *not* constant-time. Constant-time comparison guards a *secret* value against a
//! timing oracle (MAC/tag forgery). Here the expected value is a public SHA-256
//! recorded in our own manifest and the attacker controls the input bytes, so a
//! forgery is a SHA-256 *preimage* problem, not a timing side-channel — a
//! constant-time compare would add a dependency and imply a threat that does not
//! apply. Do not "harden" this to `subtle`.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::{BinaryArtifactDescriptor, IntegrityManifest, Target};

/// Failure verifying a binary against the integrity manifest.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IntegrityError {
    /// The manifest records no artifact for the requested target.
    #[error("no artifact for target '{0}' in the integrity manifest")]
    TargetNotFound(String),
    /// The manifest records the same target more than once — ambiguous, so the
    /// verification refuses rather than pick one (a shadowed descriptor could
    /// otherwise hide a mismatched hash).
    #[error("duplicate artifact target '{target}' in the integrity manifest")]
    DuplicateTarget { target: String },
    /// The descriptor's recorded checksum is not a well-formed SHA-256 hex digest.
    /// Surfaced distinctly from a mismatch so a *corrupt manifest* is not reported
    /// as if the artifact bytes were wrong.
    #[error("invalid sha256 for target '{target}': {reason}; value={sha256}")]
    InvalidChecksumFormat {
        target: String,
        sha256: String,
        reason: ChecksumFormatError,
    },
    /// The byte length does not match the manifest. Checked before the digest for
    /// a faster, clearer failure on an obviously wrong artifact.
    #[error(
        "artifact size mismatch for target '{target}': manifest size={expected}, actual={actual}"
    )]
    SizeMismatch {
        target: String,
        expected: u64,
        actual: u64,
    },
    /// The bytes' checksum does not match the manifest — refuse to execute.
    #[error(
        "integrity checksum mismatch for target '{target}': manifest sha256={expected}, actual={actual}"
    )]
    ChecksumMismatch {
        target: String,
        expected: String,
        actual: String,
    },
}

/// Reason a manifest checksum string was rejected by [`Sha256Digest::parse_manifest_hex`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChecksumFormatError {
    #[error("checksum must be exactly 64 lowercase hex characters, got length {actual}")]
    InvalidLength { actual: usize },
    #[error("checksum must use lowercase hex")]
    UppercaseHex,
    #[error("checksum contains non-hex characters")]
    NonHex,
}

/// A parsed SHA-256 digest — a fixed 32 bytes, so comparison is over the decoded
/// value rather than a hex string (which could differ by case/padding while
/// naming the same digest).
///
/// Serializes as the canonical lowercase hex form; deserialization is strict
/// ([`parse_manifest_hex`](Self::parse_manifest_hex)), so a malformed digest is
/// rejected at the serde boundary rather than surviving as an unparseable
/// string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256Digest([u8; 32]);

impl serde::Serialize for Sha256Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for Sha256Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse_manifest_hex(&raw).map_err(serde::de::Error::custom)
    }
}

impl Sha256Digest {
    /// Parse a manifest checksum string. The manifest format is a canonical
    /// lowercase 64-char hex digest; anything else is a malformed manifest, not a
    /// mismatch. Rejecting uppercase explicitly (rather than lower-casing) keeps
    /// the manifest's on-the-wire form unambiguous.
    pub fn parse_manifest_hex(input: &str) -> Result<Self, ChecksumFormatError> {
        if input.len() != 64 {
            return Err(ChecksumFormatError::InvalidLength {
                actual: input.len(),
            });
        }
        if input.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(ChecksumFormatError::UppercaseHex);
        }
        if !input.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ChecksumFormatError::NonHex);
        }
        let decoded = hex::decode(input).map_err(|_| ChecksumFormatError::NonHex)?;
        let bytes: [u8; 32] =
            decoded
                .try_into()
                .map_err(|_| ChecksumFormatError::InvalidLength {
                    actual: input.len(),
                })?;
        Ok(Self(bytes))
    }

    /// The SHA-256 of `bytes`.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Lowercase hex rendering (the canonical manifest form).
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256Digest::from_bytes(bytes).to_hex()
}

impl BinaryArtifactDescriptor {
    /// Parse and validate this descriptor's recorded checksum, mapping a format
    /// error to the target-scoped [`IntegrityError::InvalidChecksumFormat`].
    pub(crate) fn expected_sha256(&self) -> Result<Sha256Digest, IntegrityError> {
        Sha256Digest::parse_manifest_hex(&self.sha256).map_err(|reason| {
            IntegrityError::InvalidChecksumFormat {
                target: self.target.0.clone(),
                sha256: self.sha256.clone(),
                reason,
            }
        })
    }

    /// Verify `bytes` against this descriptor's recorded size (when recorded) and
    /// `sha256`. Errors on mismatch — the caller must not execute the binary
    /// (Property 3).
    ///
    /// A recorded `size_bytes` of `0` means "unrecorded" and skips the size check
    /// (a real provisioner binary is never zero-length); any non-zero size is
    /// checked before hashing so an obviously wrong artifact fails fast.
    pub(crate) fn verify(&self, bytes: &[u8]) -> Result<(), IntegrityError> {
        if self.size_bytes != 0 {
            let actual = bytes.len() as u64;
            if actual != self.size_bytes {
                return Err(IntegrityError::SizeMismatch {
                    target: self.target.0.clone(),
                    expected: self.size_bytes,
                    actual,
                });
            }
        }
        let expected = self.expected_sha256()?;
        let actual = Sha256Digest::from_bytes(bytes);
        // Plain byte equality (NOT constant-time) — see the module note: the
        // expected digest is public and the input is attacker-controlled, so this
        // is a preimage problem, not a timing side-channel.
        if actual == expected {
            Ok(())
        } else {
            Err(IntegrityError::ChecksumMismatch {
                target: self.target.0.clone(),
                expected: expected.to_hex(),
                actual: actual.to_hex(),
            })
        }
    }
}

impl IntegrityManifest {
    /// Validate the manifest's internal shape: every recorded checksum parses as a
    /// canonical SHA-256, and no target appears twice. This proves well-formedness
    /// only — **not** authenticity; the enclosing deployment envelope must have
    /// passed its CAS/signature check before the manifest is trusted.
    pub fn validate(&self) -> Result<(), IntegrityError> {
        let mut seen = BTreeSet::new();
        for artifact in &self.artifacts {
            if !seen.insert(artifact.target.0.as_str()) {
                return Err(IntegrityError::DuplicateTarget {
                    target: artifact.target.0.clone(),
                });
            }
            artifact.expected_sha256()?;
        }
        Ok(())
    }

    /// The descriptor recorded for `target`, if any (first match; prefer
    /// [`verify_artifact`](Self::verify_artifact), which rejects a duplicate).
    pub fn descriptor_for(&self, target: &Target) -> Option<&BinaryArtifactDescriptor> {
        self.artifacts.iter().find(|a| &a.target == target)
    }

    /// Verify a retrieved binary's `bytes` for `target` against the manifest
    /// before execution. Errors if the target is absent
    /// ([`TargetNotFound`](IntegrityError::TargetNotFound)), recorded more than
    /// once ([`DuplicateTarget`](IntegrityError::DuplicateTarget) — ambiguous, so
    /// refuse rather than trust a shadowed descriptor), or the size/checksum
    /// mismatches.
    pub fn verify_artifact(&self, bytes: &[u8], target: &Target) -> Result<(), IntegrityError> {
        // Self-defending lookup: reject a duplicate target here (not only in
        // `validate`) so the security-critical path does not depend on a prior
        // `validate` call to be safe against an ambiguous manifest.
        let mut matching = self.artifacts.iter().filter(|a| &a.target == target);
        let descriptor = matching
            .next()
            .ok_or_else(|| IntegrityError::TargetNotFound(target.0.clone()))?;
        if matching.next().is_some() {
            return Err(IntegrityError::DuplicateTarget {
                target: target.0.clone(),
            });
        }
        descriptor.verify(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(bytes: &[u8], target: &str) -> BinaryArtifactDescriptor {
        BinaryArtifactDescriptor {
            target: Target(target.to_string()),
            sha256: sha256_hex(bytes),
            retrieval_ref: None,
            size_bytes: bytes.len() as u64,
        }
    }

    fn manifest(bytes: &[u8], target: &str) -> IntegrityManifest {
        IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![descriptor(bytes, target)],
            ..Default::default()
        }
    }

    #[test]
    fn matching_bytes_verify() {
        let bytes = b"provisioner-binary-bytes";
        let target = Target("aarch64-unknown-linux-musl".to_string());
        let m = manifest(bytes, &target.0);
        m.verify_artifact(bytes, &target)
            .expect("matching bytes verify");
    }

    #[test]
    fn tampered_bytes_of_same_length_abort_on_checksum() {
        // Equal length so the size check passes and the digest compare is exercised.
        let m = IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target("t".to_string()),
                sha256: sha256_hex(b"aaaa"),
                retrieval_ref: None,
                size_bytes: 4,
            }],
            ..Default::default()
        };
        let err = m
            .verify_artifact(b"bbbb", &Target("t".to_string()))
            .expect_err("tampered bytes abort");
        assert!(matches!(err, IntegrityError::ChecksumMismatch { .. }));
    }

    #[test]
    fn wrong_size_fails_fast_before_the_digest() {
        let bytes = b"provisioner-binary-bytes";
        let target = Target("t".to_string());
        let m = manifest(bytes, &target.0);
        let err = m
            .verify_artifact(b"short", &target)
            .expect_err("wrong size aborts");
        assert!(matches!(err, IntegrityError::SizeMismatch { .. }));
    }

    #[test]
    fn zero_size_is_treated_as_unrecorded_and_skips_the_size_check() {
        // size_bytes 0 = unrecorded: verification falls through to the digest.
        let bytes = b"real-artifact";
        let m = IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target("t".to_string()),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: 0,
            }],
            ..Default::default()
        };
        m.verify_artifact(bytes, &Target("t".to_string()))
            .expect("unrecorded size skips the size check and verifies by digest");
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
    fn duplicate_target_is_rejected_by_verify_and_validate() {
        let bytes = b"bytes";
        let target = "aarch64-unknown-linux-musl";
        let m = IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![descriptor(bytes, target), descriptor(bytes, target)],
            ..Default::default()
        };
        // The verification path is self-defending — it does not rely on validate().
        let err = m
            .verify_artifact(bytes, &Target(target.to_string()))
            .expect_err("ambiguous manifest refuses");
        assert!(matches!(err, IntegrityError::DuplicateTarget { .. }));
        // And validate() catches it up front.
        assert!(matches!(
            m.validate(),
            Err(IntegrityError::DuplicateTarget { .. })
        ));
    }

    #[test]
    fn validate_accepts_a_well_formed_manifest() {
        let m = manifest(b"bytes", "t");
        m.validate().expect("well-formed manifest validates");
    }

    #[test]
    fn malformed_checksum_is_invalid_format_not_mismatch() {
        for (raw, want) in [
            (
                sha256_hex(b"x").to_uppercase(),
                ChecksumFormatError::UppercaseHex,
            ),
            (
                "abcd".to_string(),
                ChecksumFormatError::InvalidLength { actual: 4 },
            ),
            ("z".repeat(64), ChecksumFormatError::NonHex),
        ] {
            let m = IntegrityManifest {
                provisioner_version: "1.0.0".to_string(),
                artifacts: vec![BinaryArtifactDescriptor {
                    target: Target("t".to_string()),
                    sha256: raw.clone(),
                    retrieval_ref: None,
                    size_bytes: 0,
                }],
                ..Default::default()
            };
            match m.validate() {
                Err(IntegrityError::InvalidChecksumFormat { reason, .. }) => {
                    assert_eq!(reason, want, "raw={raw}")
                }
                other => panic!("expected InvalidChecksumFormat for {raw}, got {other:?}"),
            }
        }
    }

    #[test]
    fn sha256_hex_is_stable_and_lowercase() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
