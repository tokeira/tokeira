//! Bind-time admission (task 16.2; Proposal 005 §The two gates).
//!
//! Whether obtained provisioner bytes may be **bound** to a deployment. This is
//! the second gate, distinct from the binding gate: [`check_binding`]
//! (crate::check_binding) compares the *running* engine against the *recorded*
//! one; admission decides whether candidate bytes are trustworthy to record at
//! all. `create` (and every later identity-advancing operation) runs it before
//! binding — **on a cache hit too**: caching accelerates production, it never
//! grants admission.
//!
//! Three checks, all mandatory:
//!
//! 1. **authority-vs-policy** — the bundle's [`BuildAuthority`] must satisfy
//!    the deployment's required [`AuthorityTier`]. Same-identity bytes from a
//!    lower tier are inadmissible (builds are not bit-reproducible, so a
//!    laptop build must never satisfy a production create — guardrail 1).
//! 2. **not revoked** — neither the engine identity nor the actual artifact
//!    digest may appear on the deny-list (guardrail 4: "cached forever" for a
//!    security artifact needs an escape hatch).
//! 3. **the bytes** — re-hashed against the integrity manifest
//!    ([`IntegrityManifest::verify_artifact`]); a build result's checksum is a
//!    claim, the bytes are the truth.
//!
//! Where the deny-list *lives* (S3 object, repo file) is open decision 4 of
//! Proposal 005; this module models it as data so every store can carry one.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{AuthorityTier, IntegrityError, IntegrityManifest, Sha256Digest, Target};

/// The deny-list checked at bind (Proposal 005, guardrail 4). Revocation is by
/// **engine identity** (a bad engine — every artifact of it) or by **artifact
/// digest** (specific bad bytes). Empty by default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RevocationList {
    /// Revoked engine identities, by [`EngineIdentity::digest`](crate::EngineIdentity::digest).
    #[serde(default)]
    pub revoked_identities: BTreeSet<Sha256Digest>,
    /// Revoked artifact byte digests.
    #[serde(default)]
    pub revoked_artifacts: BTreeSet<Sha256Digest>,
}

impl RevocationList {
    /// Whether nothing is revoked (the common case; lets callers skip loading).
    pub fn is_empty(&self) -> bool {
        self.revoked_identities.is_empty() && self.revoked_artifacts.is_empty()
    }
}

/// Refusal to bind candidate provisioner bytes to a deployment.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdmissionError {
    /// The bundle's build authority is below the deployment's required tier.
    #[error(
        "build authority {offered:?} does not satisfy the deployment's required tier \
         {required:?} — a lower-trust build of the same identity is not admissible"
    )]
    AuthorityInsufficient {
        required: AuthorityTier,
        offered: AuthorityTier,
    },
    /// The bundle's engine identity is on the deny-list.
    #[error("engine identity {digest} is revoked")]
    IdentityRevoked { digest: String },
    /// The artifact's byte digest is on the deny-list.
    #[error("artifact sha256 {digest} for target '{target}' is revoked")]
    ArtifactRevoked { target: String, digest: String },
    /// The bytes do not verify against the integrity manifest.
    #[error(transparent)]
    Integrity(#[from] IntegrityError),
}

/// Decide whether `bytes` (a candidate provisioner for `target`, described by
/// `manifest`) may be bound to a deployment requiring `required` authority.
///
/// Refuses on the first failing check; the byte re-hash runs last (it is the
/// expensive one, and a refusal on policy should not cost a hash of a binary).
/// The artifact-revocation check uses the digest of the **actual bytes** —
/// which the preceding verification proved equal to the manifest's claim.
pub fn admit_artifact(
    bytes: &[u8],
    target: &Target,
    manifest: &IntegrityManifest,
    required: AuthorityTier,
    revocations: &RevocationList,
) -> Result<(), AdmissionError> {
    if !manifest.authority.satisfies(required) {
        return Err(AdmissionError::AuthorityInsufficient {
            required,
            offered: manifest.authority.tier(),
        });
    }
    if let Some(identity) = &manifest.engine_identity {
        let digest = identity.digest();
        if revocations.revoked_identities.contains(&digest) {
            return Err(AdmissionError::IdentityRevoked {
                digest: digest.to_hex(),
            });
        }
    }
    manifest.verify_artifact(bytes, target)?;
    let actual = Sha256Digest::from_bytes(bytes);
    if revocations.revoked_artifacts.contains(&actual) {
        return Err(AdmissionError::ArtifactRevoked {
            target: target.0.clone(),
            digest: actual.to_hex(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryArtifactDescriptor, BuildAuthority, BuildProfile, EngineIdentity, sha256_hex,
    };

    fn identity() -> EngineIdentity {
        EngineIdentity {
            source_closure: Sha256Digest::from_bytes(b"src"),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "rustc 1.88.0".to_string(),
            build_container: Some(Sha256Digest::from_bytes(b"img")),
            features: ["provisioner".to_string()].into(),
            profile: BuildProfile::Dist,
        }
    }

    fn manifest(bytes: &[u8], target: &Target, authority: BuildAuthority) -> IntegrityManifest {
        IntegrityManifest {
            engine_identity: Some(identity()),
            authority,
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: target.clone(),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
        }
    }

    fn trusted_ci() -> BuildAuthority {
        BuildAuthority::TrustedCi {
            provider: "buildkite".to_string(),
            build_id: "b-1".to_string(),
            source_commit: "abc".to_string(),
        }
    }

    #[test]
    fn verified_sufficient_unrevoked_bytes_are_admitted() {
        let target = Target("t".to_string());
        let m = manifest(b"bytes", &target, trusted_ci());
        admit_artifact(
            b"bytes",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &RevocationList::default(),
        )
        .expect("admissible");
    }

    #[test]
    fn a_lower_authority_tier_is_refused_before_hashing() {
        let target = Target("t".to_string());
        // Tampered bytes AND insufficient authority: the refusal must be the
        // policy one — admission refuses on policy before paying the hash.
        let m = manifest(b"bytes", &target, BuildAuthority::LocalDeveloper);
        let err = admit_artifact(
            b"tampered",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &RevocationList::default(),
        )
        .expect_err("refused");
        assert_eq!(
            err,
            AdmissionError::AuthorityInsufficient {
                required: AuthorityTier::TrustedCi,
                offered: AuthorityTier::LocalDeveloper,
            }
        );
    }

    #[test]
    fn a_pre_identity_manifest_cannot_satisfy_a_trusted_deployment() {
        // Native dev manifests carry no identity and default LocalDeveloper —
        // the authority gate alone keeps them out of trusted deployments.
        let target = Target("t".to_string());
        let m = IntegrityManifest {
            provisioner_version: "0.1.0".to_string(),
            artifacts: vec![],
            ..Default::default()
        };
        let err = admit_artifact(
            b"bytes",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &RevocationList::default(),
        )
        .expect_err("refused");
        assert!(matches!(err, AdmissionError::AuthorityInsufficient { .. }));
    }

    #[test]
    fn tampered_bytes_are_refused() {
        let target = Target("t".to_string());
        let m = manifest(b"bytes", &target, trusted_ci());
        let err = admit_artifact(
            b"tampered!",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &RevocationList::default(),
        )
        .expect_err("refused");
        assert!(matches!(err, AdmissionError::Integrity(_)));
    }

    #[test]
    fn a_revoked_identity_is_refused_even_with_matching_bytes() {
        let target = Target("t".to_string());
        let m = manifest(b"bytes", &target, trusted_ci());
        let revocations = RevocationList {
            revoked_identities: [identity().digest()].into(),
            ..Default::default()
        };
        let err = admit_artifact(
            b"bytes",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &revocations,
        )
        .expect_err("refused");
        assert!(matches!(err, AdmissionError::IdentityRevoked { .. }));
    }

    #[test]
    fn revoked_artifact_bytes_are_refused() {
        let target = Target("t".to_string());
        let m = manifest(b"bytes", &target, trusted_ci());
        let revocations = RevocationList {
            revoked_artifacts: [Sha256Digest::from_bytes(b"bytes")].into(),
            ..Default::default()
        };
        let err = admit_artifact(
            b"bytes",
            &target,
            &m,
            AuthorityTier::TrustedCi,
            &revocations,
        )
        .expect_err("refused");
        assert!(matches!(err, AdmissionError::ArtifactRevoked { .. }));
    }

    #[test]
    fn an_empty_revocation_list_reports_empty() {
        assert!(RevocationList::default().is_empty());
        let nonempty = RevocationList {
            revoked_artifacts: [Sha256Digest::from_bytes(b"x")].into(),
            ..Default::default()
        };
        assert!(!nonempty.is_empty());
    }
}
