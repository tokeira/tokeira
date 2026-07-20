//! The provisioner bundle (task 18; Proposal 005 §Data model).
//!
//! A [`ProvisionerBundle`] is the unit the build produces and the CAS stores:
//! one engine identity's artifacts across targets, together with who built
//! them ([`BuildAuthority`]), the evidence that the exact bytes passed their
//! tests, and the provenance of the build itself. Two deployments on the same
//! engine share one bundle; the deployment's envelope records the bundle's
//! [`IntegrityManifest`] as its binding — trust always flows from that
//! CAS-guarded manifest, never from a stored blob.

use serde::{Deserialize, Serialize};

use crate::{
    BinaryArtifactDescriptor, BuildAuthority, EngineIdentity, IntegrityManifest, Sha256Digest,
};

/// Evidence that the bundle's bytes passed their tests. Bound to the bundle —
/// the evidence travels with the exact artifact set it vouches for, never
/// cached by identity alone (Proposal 005, guardrail 5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestEvidence {
    /// The command the build ran (audit; e.g. `cargo test --locked -p …`).
    pub command: String,
    /// Whether it passed. A bundle with failing tests is never published.
    pub passed: bool,
}

/// Provenance of the build that produced the bundle (the *build*, not the
/// *bytes* — the bytes are attested by the artifact checksums).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildManifest {
    /// Correlation id of the build request.
    pub request_id: String,
    /// The frozen source: the snapshot **tree** oid the build consumed
    /// (task 17 — never a live tree).
    pub source_tree_oid: String,
    /// The snapshot's audit commit (a reachable handle, not an identity input).
    pub snapshot_commit_oid: String,
    /// The exact toolchain that compiled the artifacts.
    pub toolchain: String,
    /// The builder implementation (e.g. `dagger-local`, a Buildkite build id).
    pub builder: String,
}

/// One verified provisioner build: identity × authority × artifacts × evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionerBundle {
    pub identity: EngineIdentity,
    pub authority: BuildAuthority,
    /// Human-facing version label (the semver; never a key).
    pub provisioner_version: String,
    /// Per-target artifact descriptors — the checksums the CAS and the
    /// deployment envelope verify against.
    pub artifacts: Vec<BinaryArtifactDescriptor>,
    pub tests: TestEvidence,
    pub build: BuildManifest,
}

impl ProvisionerBundle {
    /// The [`IntegrityManifest`] a deployment records at bind: the bundle's
    /// identity, authority, and artifact set — the manifest always describes
    /// the engine the binding names.
    pub fn integrity_manifest(&self) -> IntegrityManifest {
        IntegrityManifest {
            engine_identity: Some(self.identity.clone()),
            authority: self.authority.clone(),
            provisioner_version: self.provisioner_version.clone(),
            artifacts: self.artifacts.clone(),
        }
    }

    /// The bundle's identity digest — its CAS address half.
    pub fn identity_digest(&self) -> Sha256Digest {
        self.identity.digest()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BuildProfile, Target};

    fn bundle() -> ProvisionerBundle {
        ProvisionerBundle {
            identity: EngineIdentity {
                source_closure: Sha256Digest::from_bytes(b"src"),
                lock_closure: Sha256Digest::from_bytes(b"lock"),
                toolchain: "rustc 1.88.0".into(),
                build_container: Some(Sha256Digest::from_bytes(b"img")),
                features: ["provisioner".to_string()].into(),
                profile: BuildProfile::Dist,
            },
            authority: BuildAuthority::LocalDeveloper,
            provisioner_version: "0.1.0".into(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target("aarch64-apple-darwin".into()),
                sha256: crate::sha256_hex(b"tkp-bytes"),
                retrieval_ref: None,
                size_bytes: 9,
            }],
            tests: TestEvidence {
                command: "cargo test --locked -p tokeira-local-deployment".into(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "req-1".into(),
                source_tree_oid: "abc".into(),
                snapshot_commit_oid: "def".into(),
                toolchain: "1.88.0".into(),
                builder: "dagger-local".into(),
            },
        }
    }

    #[test]
    fn bundle_round_trips_through_serde() {
        let b = bundle();
        let json = serde_json::to_string(&b).expect("serialize");
        let back: ProvisionerBundle = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(b, back);
    }

    #[test]
    fn integrity_manifest_describes_the_bundle_engine() {
        let b = bundle();
        let manifest = b.integrity_manifest();
        assert_eq!(manifest.engine_identity.as_ref(), Some(&b.identity));
        assert_eq!(manifest.authority, b.authority);
        assert_eq!(manifest.artifacts, b.artifacts);
        // The derived manifest verifies the bundle's own artifact bytes.
        manifest
            .verify_artifact(b"tkp-bytes", &b.artifacts[0].target)
            .expect("bundle bytes verify against the derived manifest");
    }
}
