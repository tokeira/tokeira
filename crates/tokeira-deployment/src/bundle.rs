//! The provisioner bundle (task 18; Proposal 005 §Data model).
//!
//! A [`ProvisionerBundle`] is the unit the build produces and the CAS stores:
//! one engine identity's artifacts across targets, together with who built
//! them ([`BuildAuthority`]), the evidence that the exact bytes passed their
//! tests, and the provenance of the build itself. Two deployments on the same
//! engine share one bundle; the deployment's envelope records the bundle's
//! [`IntegrityManifest`] as its binding — trust always flows from that
//! CAS-guarded manifest, never from a stored blob.
//!
//! Statically assembled bundles also carry [`BoundProvisionerEvidence`]. Its
//! closure digests must equal the bundle's engine identity, while placement
//! supplies the independently resolved selection and generated-root evidence;
//! disagreement is an admission failure, never a runtime dispatch decision.

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};

use crate::{
    BinaryArtifactDescriptor, BuildAuthority, EngineIdentity, IntegrityManifest, Sha256Digest,
};

/// Basename of the bundle-manifest sidecar `tkr deployment create` places
/// next to the deployment's `tkp` (task 18.3). Staged creation records it as
/// the Day-0 integrity manifest after independently verifying that the placed
/// bytes are one of the manifest's artifacts.
pub const BUNDLE_MANIFEST_BASENAME: &str = "tkp.manifest.json";

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

/// Assembly and closure evidence for one statically bound provisioner.
///
/// The platform and frontend identifiers are admission facts rather than a
/// runtime dispatch inventory. The closure digests deliberately duplicate the
/// corresponding [`EngineIdentity`] fields: the bundle store can therefore
/// detect a manifest whose human-auditable provenance disagrees with its CAS
/// address before any artifact is placed or executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundProvisionerEvidence {
    /// Platform selected from trusted platform discovery.
    pub platform: PlatformId,
    /// Definition format selected independently from trusted frontend discovery.
    pub format: DefinitionFormatId,
    /// Exact Engine_Version the platform definition indicated at assembly.
    pub engine: String,
    /// Digest of the deterministic generated `Cargo.toml` and `main.rs` root.
    pub generated_root: Sha256Digest,
    /// Source closure including the frozen workspace tree and generated root.
    pub source_closure: Sha256Digest,
    /// Locked dependency closure of the shell, platform, and frontend roots.
    pub lock_closure: Sha256Digest,
}

/// Refusal to admit bound-provisioner provenance.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BoundProvisionerAdmissionError {
    /// A legacy bundle cannot satisfy a bound platform/frontend request.
    #[error("bundle carries no bound platform/frontend evidence")]
    MissingEvidence,
    /// One independently supplied assembly fact disagrees with the bundle.
    #[error("bound provisioner {field} mismatch: expected `{expected}`, bundle records `{actual}`")]
    Mismatch {
        /// Name of the disagreeing fact.
        field: &'static str,
        /// Value resolved by the current trusted request/discovery/seed path.
        expected: String,
        /// Value recorded by the bundle.
        actual: String,
    },
}

impl BoundProvisionerEvidence {
    fn ensure_matches(&self, expected: &Self) -> Result<(), BoundProvisionerAdmissionError> {
        macro_rules! require_equal {
            ($field:ident) => {
                if self.$field != expected.$field {
                    return Err(BoundProvisionerAdmissionError::Mismatch {
                        field: stringify!($field),
                        expected: expected.$field.to_string(),
                        actual: self.$field.to_string(),
                    });
                }
            };
        }

        require_equal!(platform);
        require_equal!(format);
        require_equal!(engine);
        for (field, actual, expected) in [
            (
                "generated_root",
                self.generated_root,
                expected.generated_root,
            ),
            (
                "source_closure",
                self.source_closure,
                expected.source_closure,
            ),
            ("lock_closure", self.lock_closure, expected.lock_closure),
        ] {
            if actual != expected {
                return Err(BoundProvisionerAdmissionError::Mismatch {
                    field,
                    expected: expected.to_hex(),
                    actual: actual.to_hex(),
                });
            }
        }
        Ok(())
    }

    fn ensure_engine_identity(
        &self,
        identity: &EngineIdentity,
    ) -> Result<(), BoundProvisionerAdmissionError> {
        if self.source_closure != identity.source_closure {
            return Err(BoundProvisionerAdmissionError::Mismatch {
                field: "source_closure",
                expected: identity.source_closure.to_hex(),
                actual: self.source_closure.to_hex(),
            });
        }
        if self.lock_closure != identity.lock_closure {
            return Err(BoundProvisionerAdmissionError::Mismatch {
                field: "lock_closure",
                expected: identity.lock_closure.to_hex(),
                actual: self.lock_closure.to_hex(),
            });
        }
        Ok(())
    }
}

/// One verified provisioner build: identity × authority × artifacts × evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionerBundle {
    pub identity: EngineIdentity,
    /// Static platform/frontend assembly evidence. Transitional direct-seed
    /// bundles omit it; bound admission never accepts that omission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bound: Option<BoundProvisionerEvidence>,
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
    /// Attach evidence after proving it names this bundle's engine closure.
    pub fn with_bound_evidence(
        mut self,
        evidence: BoundProvisionerEvidence,
    ) -> Result<Self, BoundProvisionerAdmissionError> {
        evidence.ensure_engine_identity(&self.identity)?;
        self.bound = Some(evidence);
        Ok(self)
    }

    /// Validate any recorded bound evidence against the bundle's CAS identity.
    ///
    /// Legacy direct-seed bundles carry no evidence and remain readable during
    /// migration. Call [`admit_bound`](Self::admit_bound) when a selected bound
    /// platform/frontend is required; that path rejects missing evidence.
    pub fn validate_bound_evidence(&self) -> Result<(), BoundProvisionerAdmissionError> {
        if let Some(evidence) = &self.bound {
            evidence.ensure_engine_identity(&self.identity)?;
        }
        Ok(())
    }

    /// Verify that a bundle loaded from an identity-addressed store still
    /// records the identity used to address it.
    pub(crate) fn admit_engine_identity(
        &self,
        expected: &EngineIdentity,
    ) -> Result<(), BoundProvisionerAdmissionError> {
        if &self.identity != expected {
            return Err(BoundProvisionerAdmissionError::Mismatch {
                field: "engine_identity",
                expected: expected.digest().to_hex(),
                actual: self.identity.digest().to_hex(),
            });
        }
        self.validate_bound_evidence()
    }

    /// Admit this bundle for one independently resolved assembly request.
    ///
    /// Callers construct `expected` from the trusted request, discovery, seed,
    /// and generated root. Every field is compared before placement; the
    /// recorded closure evidence must additionally agree with the bundle's
    /// [`EngineIdentity`].
    pub fn admit_bound(
        &self,
        expected: &BoundProvisionerEvidence,
    ) -> Result<(), BoundProvisionerAdmissionError> {
        let actual = self
            .bound
            .as_ref()
            .ok_or(BoundProvisionerAdmissionError::MissingEvidence)?;
        actual.ensure_matches(expected)?;
        actual.ensure_engine_identity(&self.identity)
    }

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
            bound: None,
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

    fn evidence(bundle: &ProvisionerBundle) -> BoundProvisionerEvidence {
        BoundProvisionerEvidence {
            platform: PlatformId::new("compose").expect("canonical platform"),
            format: DefinitionFormatId::new("tkd").expect("canonical format"),
            engine: "0.1.0".to_string(),
            generated_root: Sha256Digest::from_bytes(b"generated-root"),
            source_closure: bundle.identity.source_closure,
            lock_closure: bundle.identity.lock_closure,
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

    #[test]
    fn bound_evidence_round_trips_and_admits_the_exact_selection() {
        let base = bundle();
        let expected = evidence(&base);
        let bound = base
            .with_bound_evidence(expected.clone())
            .expect("evidence agrees with the engine identity");

        let json = serde_json::to_string(&bound).expect("serialize bound bundle");
        let decoded: ProvisionerBundle = serde_json::from_str(&json).expect("deserialize bundle");
        decoded
            .admit_bound(&expected)
            .expect("all independently resolved facts agree");
    }

    #[test]
    fn bound_admission_rejects_missing_or_disagreeing_evidence() {
        let base = bundle();
        let expected = evidence(&base);
        assert_eq!(
            base.admit_bound(&expected),
            Err(BoundProvisionerAdmissionError::MissingEvidence)
        );
        let bound = base
            .with_bound_evidence(expected.clone())
            .expect("evidence agrees with identity");

        let mut variants = Vec::new();
        let mut wrong = expected.clone();
        wrong.platform = PlatformId::new("ecs").expect("canonical platform");
        variants.push(("platform", wrong));
        let mut wrong = expected.clone();
        wrong.format = DefinitionFormatId::new("tkdp").expect("canonical format");
        variants.push(("format", wrong));
        let mut wrong = expected.clone();
        wrong.engine = "9.9.9".to_string();
        variants.push(("engine", wrong));
        let mut wrong = expected.clone();
        wrong.generated_root = Sha256Digest::from_bytes(b"other root");
        variants.push(("generated_root", wrong));
        let mut wrong = expected.clone();
        wrong.source_closure = Sha256Digest::from_bytes(b"other source");
        variants.push(("source_closure", wrong));
        let mut wrong = expected;
        wrong.lock_closure = Sha256Digest::from_bytes(b"other lock");
        variants.push(("lock_closure", wrong));

        for (field, wrong) in variants {
            let error = bound
                .admit_bound(&wrong)
                .expect_err("every independently resolved disagreement must refuse admission");
            assert!(matches!(
                error,
                BoundProvisionerAdmissionError::Mismatch {
                    field: actual,
                    ..
                } if actual == field
            ));
        }
    }

    #[test]
    fn closure_evidence_must_match_the_engine_identity() {
        let base = bundle();
        let mut wrong = evidence(&base);
        wrong.source_closure = Sha256Digest::from_bytes(b"different source");
        let error = base
            .with_bound_evidence(wrong)
            .expect_err("source evidence must agree with the CAS identity");
        assert!(matches!(
            error,
            BoundProvisionerAdmissionError::Mismatch {
                field: "source_closure",
                ..
            }
        ));
    }

    #[test]
    fn identity_address_must_match_the_bundle_manifest() {
        let actual = bundle();
        let mut requested = actual.identity.clone();
        requested.toolchain = "rustc 9.9.9".to_string();

        let error = actual
            .admit_engine_identity(&requested)
            .expect_err("a manifest under the wrong CAS key must be refused");
        assert!(matches!(
            error,
            BoundProvisionerAdmissionError::Mismatch {
                field: "engine_identity",
                ..
            }
        ));
    }
}
