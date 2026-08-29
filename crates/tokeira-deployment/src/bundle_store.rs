//! The content-addressed bundle store (task 18.2; Proposal 005 §Caching).
//!
//! Tokeira's authoritative answer to *"does a verified bundle exist for this
//! identity × authority?"* — keyed `authority-tier / identity-digest /
//! target`, over any [`StateBackend`], so one implementation serves the S3
//! CAS and local dev. Every layer of Dagger caching sits *below* this store;
//! this store sits *below* the trust boundary:
//!
//! - **Partitioned by authority** (guardrail 1): a resolve for a deployment
//!   requiring `TrustedCi` never even reads the `local_developer` partition —
//!   a laptop bundle of the same identity is a *miss*, not a lower-quality
//!   hit. Write-gating the trusted partitions to CI is Phase-3 policy
//!   (enforced at the credential boundary, not here).
//! - **A cache hit is a performance event, never a trust decision**: every
//!   [`resolve`](BundleStore::resolve) re-runs the full admission gate
//!   (task 16.2) — byte re-hash against the bundle's manifest, authority vs
//!   the deployment's floor, and the revocation deny-list, which lives at a
//!   fixed key in the store itself (`revocations.json` — decision 4's v1
//!   home: revoking is one CAS write, honoured by every subsequent bind).
//! - **Publish verifies before writing**: bytes that do not match their own
//!   descriptors never enter the store, and a bundle whose tests did not pass
//!   is refused outright. The manifest is written last — a partially-written
//!   bundle (crash between artifact and manifest writes) resolves as a miss,
//!   never as a half-trusted hit.

use std::collections::BTreeMap;

use tokeira_state::{StateBackend, StateError};

use crate::{
    AdmissionError, AuthorityTier, BoundProvisionerAdmissionError, EngineIdentity,
    ProvisionerBundle, RevocationList, Target, admission::admit_artifact,
};

/// A resolved cache hit: the bundle and the (admission-verified) bytes for
/// the requested target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBundle {
    pub bundle: ProvisionerBundle,
    pub bytes: Vec<u8>,
    /// The tier partition the hit came from (≥ the required floor).
    pub tier: AuthorityTier,
}

/// Failure publishing to or resolving from the bundle store.
#[derive(Debug, thiserror::Error)]
pub enum BundleStoreError {
    #[error(transparent)]
    Store(#[from] StateError),
    /// The bundle refuses admission (tampered bytes, insufficient authority,
    /// or revoked) — surfaced, never silently degraded to a miss: a failing
    /// verification on a *present* bundle is an integrity event the caller
    /// must see (Property 3), not a reason to quietly rebuild.
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    /// Assembly evidence disagrees with the bundle identity or selected root.
    #[error(transparent)]
    Evidence(#[from] BoundProvisionerAdmissionError),
    /// Publish refused: the offered bytes do not match the bundle's own
    /// descriptors, carry no descriptor, or the bundle's tests did not pass.
    #[error("refusing to publish: {0}")]
    PublishRefused(String),
    /// The stored manifest does not parse — corruption, surfaced loudly.
    #[error("stored bundle manifest is corrupt at {key}: {reason}")]
    CorruptManifest { key: String, reason: String },
}

/// The content-addressed bundle store over a [`StateBackend`].
pub struct BundleStore {
    backend: Box<dyn StateBackend>,
    prefix: String,
}

impl std::fmt::Debug for BundleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundleStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

fn tier_slug(tier: AuthorityTier) -> &'static str {
    match tier {
        AuthorityTier::LocalDeveloper => "local_developer",
        AuthorityTier::TrustedCi => "trusted_ci",
    }
}

/// Tiers that satisfy `floor`, searched highest-trust first.
fn tiers_at_or_above(floor: AuthorityTier) -> impl Iterator<Item = AuthorityTier> {
    [AuthorityTier::TrustedCi, AuthorityTier::LocalDeveloper]
        .into_iter()
        .filter(move |tier| *tier >= floor)
}

impl BundleStore {
    /// Construct a store rooted at `prefix` (e.g. `"bundles"`).
    pub fn new(backend: Box<dyn StateBackend>, prefix: impl Into<String>) -> Self {
        Self {
            backend,
            prefix: prefix.into(),
        }
    }

    fn manifest_key(&self, tier: AuthorityTier, identity: &EngineIdentity) -> String {
        format!(
            "{}/{}/{}/manifest.json",
            self.prefix,
            tier_slug(tier),
            identity.digest().to_hex()
        )
    }

    fn artifact_key(
        &self,
        tier: AuthorityTier,
        identity: &EngineIdentity,
        target: &Target,
    ) -> String {
        format!(
            "{}/{}/{}/{}",
            self.prefix,
            tier_slug(tier),
            identity.digest().to_hex(),
            target.0
        )
    }

    fn revocations_key(&self) -> String {
        format!("{}/revocations.json", self.prefix)
    }

    /// The store's revocation deny-list (absent → empty). Revocation is one
    /// CAS write to this key; every subsequent [`resolve`](Self::resolve)
    /// honours it.
    pub(crate) async fn load_revocations(&self) -> Result<RevocationList, BundleStoreError> {
        match self.backend.read_snapshot(&self.revocations_key()).await {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| BundleStoreError::CorruptManifest {
                    key: self.revocations_key(),
                    reason: e.to_string(),
                })
            }
            Err(StateError::NotFound(_)) => Ok(RevocationList::default()),
            Err(other) => Err(other.into()),
        }
    }

    /// Publish a verified bundle: every offered artifact must match the
    /// bundle's own descriptor (bytes are verified *before* anything is
    /// written), the bundle's tests must have passed, and the manifest is
    /// written last so a torn publish resolves as a miss.
    pub async fn publish(
        &self,
        bundle: &ProvisionerBundle,
        artifacts: &BTreeMap<Target, Vec<u8>>,
    ) -> Result<(), BundleStoreError> {
        bundle.validate_bound_evidence()?;
        if !bundle.tests.passed {
            return Err(BundleStoreError::PublishRefused(
                "the bundle's tests did not pass".to_string(),
            ));
        }
        if artifacts.is_empty() {
            return Err(BundleStoreError::PublishRefused(
                "no artifacts offered".to_string(),
            ));
        }
        let manifest = bundle.integrity_manifest();
        for (target, bytes) in artifacts {
            manifest
                .verify_artifact(bytes, target)
                .map_err(AdmissionError::from)?;
        }

        let tier = bundle.authority.tier();
        for (target, bytes) in artifacts {
            self.backend
                .write_snapshot(&self.artifact_key(tier, &bundle.identity, target), bytes)
                .await?;
        }
        let manifest_bytes = serde_json::to_vec_pretty(bundle).map_err(|e| {
            BundleStoreError::PublishRefused(format!("bundle does not serialize: {e}"))
        })?;
        self.backend
            .write_snapshot(&self.manifest_key(tier, &bundle.identity), &manifest_bytes)
            .await?;
        Ok(())
    }

    /// Resolve a verified bundle for `identity` × `target`, admissible at the
    /// deployment's `required` floor. Searches the qualifying tier partitions
    /// highest-trust first. `Ok(None)` is an honest miss (build it);
    /// `Err(Admission(..))` is a *present but inadmissible* bundle —
    /// surfaced, never degraded to a miss.
    pub async fn resolve(
        &self,
        identity: &EngineIdentity,
        required: AuthorityTier,
        target: &Target,
    ) -> Result<Option<ResolvedBundle>, BundleStoreError> {
        let revocations = self.load_revocations().await?;
        for tier in tiers_at_or_above(required) {
            let manifest_key = self.manifest_key(tier, identity);
            let bundle: ProvisionerBundle = match self.backend.read_snapshot(&manifest_key).await {
                Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                    BundleStoreError::CorruptManifest {
                        key: manifest_key,
                        reason: e.to_string(),
                    }
                })?,
                Err(StateError::NotFound(_)) => continue,
                Err(other) => return Err(other.into()),
            };
            bundle.admit_engine_identity(identity)?;
            let bytes = match self
                .backend
                .read_snapshot(&self.artifact_key(tier, identity, target))
                .await
            {
                Ok(bytes) => bytes,
                // Manifest present but this target's blob absent: a bundle
                // built for other targets — an honest miss for this one.
                Err(StateError::NotFound(_)) => continue,
                Err(other) => return Err(other.into()),
            };
            admit_artifact(
                &bytes,
                target,
                &bundle.integrity_manifest(),
                required,
                &revocations,
            )?;
            return Ok(Some(ResolvedBundle {
                bundle,
                bytes,
                tier,
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryArtifactDescriptor, BoundProvisionerEvidence, BuildAuthority, BuildProfile,
        Sha256Digest,
        bundle::{BuildManifest, TestEvidence},
        sha256_hex,
    };
    use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
    use tokeira_state::LocalBackend;

    fn store(root: &std::path::Path) -> BundleStore {
        BundleStore::new(Box::new(LocalBackend::new(root)), "bundles")
    }

    fn identity(marker: &[u8]) -> EngineIdentity {
        EngineIdentity {
            source_closure: Sha256Digest::from_bytes(marker),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "rustc 1.88.0".into(),
            build_container: None,
            features: ["provisioner".to_string()].into(),
            profile: BuildProfile::Dist,
        }
    }

    fn target() -> Target {
        Target("aarch64-apple-darwin".into())
    }

    fn bundle(marker: &[u8], authority: BuildAuthority, bytes: &[u8]) -> ProvisionerBundle {
        ProvisionerBundle {
            identity: identity(marker),
            bound: None,
            authority,
            provisioner_version: "0.1.0".into(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: target(),
                sha256: sha256_hex(bytes),
                retrieval_ref: None,
                size_bytes: bytes.len() as u64,
            }],
            tests: TestEvidence {
                command: "cargo test --locked".into(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "req".into(),
                source_tree_oid: "t".into(),
                snapshot_commit_oid: "c".into(),
                toolchain: "1.88.0".into(),
                builder: "dagger-local".into(),
            },
        }
    }

    fn trusted() -> BuildAuthority {
        BuildAuthority::TrustedCi {
            provider: "buildkite".into(),
            build_id: "b-1".into(),
            source_commit: "abc".into(),
        }
    }

    fn artifacts(bytes: &[u8]) -> BTreeMap<Target, Vec<u8>> {
        [(target(), bytes.to_vec())].into()
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

    #[tokio::test]
    async fn publish_then_resolve_round_trips_verified() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");

        store.publish(&b, &artifacts(b"tkp-bytes")).await.unwrap();
        let hit = store
            .resolve(&b.identity, AuthorityTier::LocalDeveloper, &target())
            .await
            .unwrap()
            .expect("hit");
        assert_eq!(hit.bytes, b"tkp-bytes");
        assert_eq!(hit.bundle, b);
        assert_eq!(hit.tier, AuthorityTier::LocalDeveloper);
    }

    #[tokio::test]
    async fn bound_evidence_survives_published_bundle_admission() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let base = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        let expected = evidence(&base);
        let bound = base
            .with_bound_evidence(expected.clone())
            .expect("consistent evidence");

        store
            .publish(&bound, &artifacts(b"tkp-bytes"))
            .await
            .expect("publish bound bundle");
        let hit = store
            .resolve(&bound.identity, AuthorityTier::LocalDeveloper, &target())
            .await
            .expect("resolve bundle")
            .expect("published hit");
        hit.bundle
            .admit_bound(&expected)
            .expect("published evidence remains exact");
    }

    #[tokio::test]
    async fn publish_refuses_evidence_that_disagrees_with_engine_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let mut bound = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        let mut wrong = evidence(&bound);
        wrong.lock_closure = Sha256Digest::from_bytes(b"wrong-lock");
        bound.bound = Some(wrong);

        let error = store
            .publish(&bound, &artifacts(b"tkp-bytes"))
            .await
            .expect_err("inconsistent evidence must not be published");
        assert!(matches!(
            error,
            BundleStoreError::Evidence(BoundProvisionerAdmissionError::Mismatch {
                field: "lock_closure",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn a_trusted_floor_never_reads_the_local_partition() {
        // Guardrail 1: same identity, laptop-built — a prod resolve must MISS,
        // not accept a lower-trust hit.
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        store.publish(&b, &artifacts(b"tkp-bytes")).await.unwrap();

        let miss = store
            .resolve(&b.identity, AuthorityTier::TrustedCi, &target())
            .await
            .unwrap();
        assert!(
            miss.is_none(),
            "a laptop bundle is a MISS for a trusted floor"
        );
    }

    #[tokio::test]
    async fn a_local_floor_prefers_the_trusted_partition() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        // Same identity published at both tiers with different (valid) bytes —
        // non-bit-reproducible builds.
        let local = bundle(b"e1", BuildAuthority::LocalDeveloper, b"local-bytes");
        store
            .publish(&local, &artifacts(b"local-bytes"))
            .await
            .unwrap();
        let ci = bundle(b"e1", trusted(), b"ci-bytes");
        store.publish(&ci, &artifacts(b"ci-bytes")).await.unwrap();

        let hit = store
            .resolve(&local.identity, AuthorityTier::LocalDeveloper, &target())
            .await
            .unwrap()
            .expect("hit");
        assert_eq!(hit.tier, AuthorityTier::TrustedCi, "highest tier wins");
        assert_eq!(hit.bytes, b"ci-bytes");
    }

    #[tokio::test]
    async fn tampered_stored_bytes_are_an_admission_error_not_a_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        store.publish(&b, &artifacts(b"tkp-bytes")).await.unwrap();
        // Corrupt the stored blob behind the store's back — straight through
        // the filesystem, as real corruption would arrive (the backend's
        // write_snapshot is idempotent-skip and would refuse to overwrite).
        std::fs::write(
            tmp.path().join(format!(
                "bundles/local_developer/{}/{}",
                b.identity.digest().to_hex(),
                target().0
            )),
            b"tampered!",
        )
        .unwrap();

        let err = store
            .resolve(&b.identity, AuthorityTier::LocalDeveloper, &target())
            .await
            .expect_err("tampering surfaces");
        assert!(matches!(err, BundleStoreError::Admission(_)));
    }

    #[tokio::test]
    async fn a_revoked_identity_is_refused_at_resolve() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        store.publish(&b, &artifacts(b"tkp-bytes")).await.unwrap();
        // Revoke via the store's own deny-list key — one CAS write.
        let backend = LocalBackend::new(tmp.path());
        use tokeira_state::StateBackend as _;
        let revocations = RevocationList {
            revoked_identities: [b.identity.digest()].into(),
            ..Default::default()
        };
        backend
            .write_snapshot(
                "bundles/revocations.json",
                &serde_json::to_vec(&revocations).unwrap(),
            )
            .await
            .unwrap();

        let err = store
            .resolve(&b.identity, AuthorityTier::LocalDeveloper, &target())
            .await
            .expect_err("revoked identity refuses");
        assert!(matches!(
            err,
            BundleStoreError::Admission(AdmissionError::IdentityRevoked { .. })
        ));
    }

    #[tokio::test]
    async fn publish_refuses_mismatched_bytes_and_failed_tests() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());

        // Bytes that don't match the bundle's own descriptor never enter.
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"real-bytes");
        let err = store
            .publish(&b, &artifacts(b"other-bytes"))
            .await
            .expect_err("mismatched bytes refuse");
        assert!(matches!(err, BundleStoreError::Admission(_)));
        // Nothing was written: resolve is a clean miss.
        assert!(
            store
                .resolve(&b.identity, AuthorityTier::LocalDeveloper, &target())
                .await
                .unwrap()
                .is_none()
        );

        // A failing-tests bundle is refused outright.
        let mut failing = bundle(b"e2", BuildAuthority::LocalDeveloper, b"bytes");
        failing.tests.passed = false;
        let err = store
            .publish(&failing, &artifacts(b"bytes"))
            .await
            .expect_err("failing tests refuse");
        assert!(matches!(err, BundleStoreError::PublishRefused(_)));
    }

    #[tokio::test]
    async fn a_missing_target_in_a_present_bundle_is_a_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path());
        let b = bundle(b"e1", BuildAuthority::LocalDeveloper, b"tkp-bytes");
        store.publish(&b, &artifacts(b"tkp-bytes")).await.unwrap();

        let other_target = Target("x86_64-unknown-linux-gnu".into());
        let miss = store
            .resolve(&b.identity, AuthorityTier::LocalDeveloper, &other_target)
            .await
            .unwrap();
        assert!(miss.is_none(), "an unbuilt target is an honest miss");
    }
}
