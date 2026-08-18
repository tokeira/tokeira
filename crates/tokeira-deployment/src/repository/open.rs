//! Verified load and claim enforcement.
//!
//! The consumer trusts exactly one thing out-of-band: the pinned root.json
//! bytes. Everything else — which metadata is current, which targets exist,
//! what bytes they must hash to — arrives through the TUF chain `tough`
//! enforces during load and on every target read. On top of TUF's own
//! guarantees, [`OpenRepository::verified_publication`] enforces the
//! Deployment Claim Contract: exactly one claim, claim/target agreement
//! across both halves, the identity recomputed with `tokeira_platform`'s
//! sole implementation, and the bundle manifest cross-checked artifact by
//! artifact. A publication whose targets verify individually but whose
//! claim is inconsistent is refused whole — nothing materializes.

use std::{path::Path, sync::Arc};

use tough::{ExpirationEnforcement, IntoVec as _, Repository, RepositoryLoader, TargetName};

use super::{
    claim::{CLAIM_KEY, DeploymentClaim},
    error::{OpenError, Refusal},
    locator::RepositoryLocator,
    publish::engine_target_name,
    transport::S3Transport,
};
use crate::ProvisionerBundle;

/// Expiration handling: `Safe` is the default; `BreakGlass` maps to
/// `tough`'s unsafe enforcement and exists only behind the explicit
/// operator flag whose report states that freshness was not enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Freshness {
    /// Refuse expired metadata (the default).
    #[default]
    Enforced,
    /// Load despite expiry — the operator's explicit break-glass.
    BreakGlass,
}

/// A loaded, chain-verified repository (claim not yet enforced).
pub struct OpenRepository {
    repo: Repository,
}

impl std::fmt::Debug for OpenRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenRepository").finish_non_exhaustive()
    }
}

/// Load and chain-verify the repository at `locator` from pinned trust.
///
/// `datastore` is where the TUF client persists trusted metadata — what
/// makes rollback detection hold across separate loads; the deployment dir
/// owns it (`state/repository/datastore/`). S3 residency needs `s3`.
pub async fn open(
    locator: &RepositoryLocator,
    trusted_root: &[u8],
    datastore: Option<&Path>,
    freshness: Freshness,
    s3: Option<aws_sdk_s3::Client>,
) -> Result<OpenRepository, OpenError> {
    // Refuse unusable anchors before any network fetch.
    serde_json::from_slice::<serde_json::Value>(trusted_root).map_err(|error| {
        OpenError::TrustAnchor {
            error: error.to_string(),
        }
    })?;
    let metadata = locator.metadata_url()?;
    let targets = locator.targets_url()?;
    let mut loader = RepositoryLoader::new(&trusted_root, metadata, targets);
    loader = match locator {
        RepositoryLocator::Local { .. } => loader.transport(tough::FilesystemTransport),
        RepositoryLocator::S3 { .. } => {
            let client = s3.ok_or_else(|| OpenError::Verification {
                error: "an S3 locator needs a configured S3 client".to_string(),
            })?;
            loader.transport(S3Transport::new(client))
        }
    };
    if let Some(dir) = datastore {
        loader = loader.datastore(dir);
    }
    if matches!(freshness, Freshness::BreakGlass) {
        loader = loader.expiration_enforcement(ExpirationEnforcement::Unsafe);
    }
    let repo = loader
        .load()
        .await
        .map_err(|error| OpenError::Verification {
            error: format!("{error}"),
        })?;
    Ok(OpenRepository { repo })
}

/// One artifact of the verified engine half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifact {
    /// The artifact's target triple.
    pub triple: String,
    /// Its engine binary target name.
    pub target: String,
    /// Size in bytes per the manifest.
    pub size_bytes: u64,
}

/// A publication with its claim fully enforced.
pub struct VerifiedPublication {
    repo: Repository,
    claim: DeploymentClaim,
    version: u64,
    manifest: ProvisionerBundle,
    artifacts: Vec<VerifiedArtifact>,
    config_targets: Vec<String>,
}

impl std::fmt::Debug for VerifiedPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedPublication")
            .field("version", &self.version)
            .field("deployment", &self.claim.deployment.name)
            .finish_non_exhaustive()
    }
}

impl OpenRepository {
    /// The accepted trust-anchor bytes after any root-version walk —
    /// callers re-pin these.
    pub fn trust_anchor(&self) -> Result<Vec<u8>, OpenError> {
        serde_json::to_vec_pretty(self.repo.root()).map_err(|error| OpenError::Verification {
            error: format!("re-serializing the accepted root: {error}"),
        })
    }

    /// Enforce the Deployment Claim Contract and return the publication.
    pub async fn verified_publication(self) -> Result<VerifiedPublication, Refusal> {
        let repo = self.repo;
        // Exactly one claim, riding the definition root's target.
        let mut claims = Vec::new();
        for (name, target) in repo.targets().signed.targets_iter() {
            if let Some(value) = target.custom.get(CLAIM_KEY) {
                claims.push((name.raw().to_string(), value.clone()));
            }
        }
        let (carrier, value) = match claims.len() {
            1 => claims.remove(0),
            0 => return Err(Refusal::ClaimMissing),
            count => return Err(Refusal::ClaimAmbiguous { count }),
        };
        let claim: DeploymentClaim =
            serde_json::from_value(value).map_err(|error| Refusal::ClaimInvalid {
                error: error.to_string(),
            })?;
        if claim.definition.root != carrier {
            return Err(Refusal::ClaimRootMismatch {
                claimed: claim.definition.root.clone(),
                carrier,
            });
        }
        // Every claimed companion resolves among the targets.
        for companion in &claim.definition.companions {
            let target = claim.companion_target(companion);
            if !target_exists(&repo, &target) {
                return Err(Refusal::ClaimCompanionMissing {
                    name: companion.clone(),
                    target,
                });
            }
        }
        // The identity recomputed with the sole implementation must equal
        // the claim's — over fetched bytes, in claimed order.
        let root_bytes = read_target(&repo, &carrier).await?;
        let mut served = Vec::new();
        for companion in &claim.definition.companions {
            let bytes = read_target(&repo, &claim.companion_target(companion)).await?;
            served.push((companion.clone(), Arc::from(bytes.into_boxed_slice())));
        }
        let computed = if served.is_empty() {
            tokeira_platform::definition::ConfigurationIdentity::compute(&claim.format, &root_bytes)
        } else {
            tokeira_platform::definition::ConfigurationIdentity::compute_set(
                &claim.format,
                &root_bytes,
                &served,
            )
        };
        if computed != claim.definition.identity {
            return Err(Refusal::IdentityMismatch {
                claimed_algorithm: claim.definition.identity.algorithm().to_string(),
                claimed_digest: claim.definition.identity.digest.clone(),
                computed_algorithm: computed.algorithm().to_string(),
                computed_digest: computed.digest,
            });
        }
        // The engine half: manifest present, identity bound, every artifact
        // agreeing with its target.
        if !target_exists(&repo, &claim.engine.manifest) {
            return Err(Refusal::EngineManifestMissing {
                target: claim.engine.manifest.clone(),
            });
        }
        let manifest_bytes = read_target(&repo, &claim.engine.manifest).await?;
        let manifest: ProvisionerBundle =
            serde_json::from_slice(&manifest_bytes).map_err(|error| {
                Refusal::EngineManifestInvalid {
                    error: error.to_string(),
                }
            })?;
        let manifest_digest = manifest.identity_digest().to_hex();
        if manifest_digest != claim.engine.identity_digest {
            return Err(Refusal::EngineIdentityMismatch {
                claimed: claim.engine.identity_digest.clone(),
                manifest: manifest_digest,
            });
        }
        let mut artifacts = Vec::new();
        for descriptor in &manifest.artifacts {
            let triple = descriptor.target.0.clone();
            let expected_target = engine_target_name(&triple);
            match &descriptor.retrieval_ref {
                Some(reference) if reference == &expected_target => {}
                other => {
                    return Err(Refusal::EngineArtifactMismatch {
                        target_triple: triple,
                        detail: format!(
                            "retrieval_ref {:?} does not name `{expected_target}`",
                            other
                        ),
                    });
                }
            }
            let Some(target) = lookup_target(&repo, &expected_target) else {
                return Err(Refusal::EngineArtifactMismatch {
                    target_triple: triple,
                    detail: format!("the publication carries no `{expected_target}` target"),
                });
            };
            let tuf_sha = hex::encode(&target.hashes.sha256);
            if tuf_sha != descriptor.sha256 {
                return Err(Refusal::EngineArtifactMismatch {
                    target_triple: triple,
                    detail: format!(
                        "manifest sha256 {} != publication sha256 {tuf_sha}",
                        descriptor.sha256
                    ),
                });
            }
            artifacts.push(VerifiedArtifact {
                triple,
                target: expected_target,
                size_bytes: descriptor.size_bytes,
            });
        }
        // Config-tree inventory: every target that is neither a definition
        // document nor engine material.
        let mut config_targets = Vec::new();
        for (name, _) in repo.targets().signed.targets_iter() {
            let raw = name.raw();
            let is_definition = raw == claim.definition.root
                || claim
                    .definition
                    .companions
                    .iter()
                    .any(|companion| claim.companion_target(companion) == raw);
            let is_engine = raw == claim.engine.manifest
                || artifacts.iter().any(|artifact| artifact.target == raw);
            if !is_definition && !is_engine {
                config_targets.push(raw.to_string());
            }
        }
        config_targets.sort();

        let version = repo.targets().signed.version.get();
        Ok(VerifiedPublication {
            repo,
            claim,
            version,
            manifest,
            artifacts,
            config_targets,
        })
    }
}

impl VerifiedPublication {
    /// The enforced claim.
    pub fn claim(&self) -> &DeploymentClaim {
        &self.claim
    }

    /// The publication version (shared by targets/snapshot/timestamp).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The verified bundle manifest.
    pub fn manifest(&self) -> &ProvisionerBundle {
        &self.manifest
    }

    /// The verified engine artifacts.
    pub fn artifacts(&self) -> &[VerifiedArtifact] {
        &self.artifacts
    }

    /// Config-tree target names (sorted).
    pub fn config_targets(&self) -> &[String] {
        &self.config_targets
    }

    /// Read one verified target's bytes (hash-checked by the TUF client as
    /// it streams).
    pub async fn read(&self, target: &str) -> Result<Vec<u8>, Refusal> {
        read_target(&self.repo, target).await
    }
}

fn lookup_target<'a>(repo: &'a Repository, name: &str) -> Option<&'a tough::schema::Target> {
    repo.targets()
        .signed
        .targets_iter()
        .find(|(candidate, _)| candidate.raw() == name)
        .map(|(_, target)| target)
}

fn target_exists(repo: &Repository, name: &str) -> bool {
    lookup_target(repo, name).is_some()
}

async fn read_target(repo: &Repository, name: &str) -> Result<Vec<u8>, Refusal> {
    let target_name = TargetName::new(name).map_err(|error| Refusal::TargetUnreadable {
        target: name.to_string(),
        error: error.to_string(),
    })?;
    let stream = repo
        .read_target(&target_name)
        .await
        .map_err(|error| Refusal::TargetUnreadable {
            target: name.to_string(),
            error: error.to_string(),
        })?
        .ok_or_else(|| Refusal::TargetUnreadable {
            target: name.to_string(),
            error: "absent from the publication".to_string(),
        })?;
    stream
        .into_vec()
        .await
        .map_err(|error| Refusal::TargetUnreadable {
            target: name.to_string(),
            error: error.to_string(),
        })
}
