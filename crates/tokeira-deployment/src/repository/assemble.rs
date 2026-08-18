//! Publication assembly from a committed deployment dir.
//!
//! One implementation serves every publisher — create's birth publication,
//! the lifecycle hooks, and the `publish` repair verb — so the claim and the
//! published inventory can never drift between them. Assembly reads only
//! committed files (the dir a rename or an envelope CAS has already made
//! authoritative); publish never reads mutable state itself.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::definition::ConfigurationIdentity;
use uuid::Uuid;

use super::{
    claim::{DefinitionSection, DeploymentClaim, DeploymentRef, EngineSection, Transition},
    error::PublishError,
    publish::PublicationInput,
};
use crate::{AuthorityTier, BUNDLE_MANIFEST_BASENAME, ProvisionerBundle};

/// The engine-computed identity facts a publisher supplies; everything else
/// is read from the committed dir.
#[derive(Debug, Clone)]
pub struct ClaimInputs {
    /// The composite configuration identity the engine computed.
    pub identity: ConfigurationIdentity,
    /// Bare companion names in served order.
    pub companions: Vec<String>,
    /// The committed transition.
    pub transition: Transition,
    /// The post-transition configuration revision.
    pub config_revision: u64,
}

/// The slice of `metadata.json` a publication needs. Deliberately tolerant
/// (no `deny_unknown_fields`): the file is owned by `tkr`, which may carry
/// fields publishers have no business reading.
#[derive(Debug, Deserialize)]
struct PublisherMetadata {
    name: String,
    id: Uuid,
    platform: PlatformId,
    definition: Option<RecordedDefinitionSlice>,
}

#[derive(Debug, Deserialize)]
struct RecordedDefinitionSlice {
    format: DefinitionFormatId,
    path: String,
}

/// Local-only files that never enter a publication: the engine pair publish
/// carries explicitly, the seat-local records, and all of `state/`.
const UNPUBLISHED: &[&str] = &["tkp", "metadata.json", "tokeirad.pid", "deployment.toml"];

/// Assemble the Deployment Claim from the committed dir plus the
/// engine-computed facts.
pub fn claim_from_dir(
    deployment_dir: &Path,
    inputs: &ClaimInputs,
) -> Result<DeploymentClaim, PublishError> {
    let metadata_path = deployment_dir.join("metadata.json");
    let metadata: PublisherMetadata = serde_json::from_slice(
        &std::fs::read(&metadata_path)
            .map_err(|error| other(format!("reading {}: {error}", metadata_path.display())))?,
    )
    .map_err(|error| {
        other(format!(
            "{} does not decode: {error}",
            metadata_path.display()
        ))
    })?;
    let definition = metadata.definition.ok_or_else(|| {
        other(format!(
            "{} records no definition; only definition-bearing deployments publish",
            metadata_path.display()
        ))
    })?;

    let manifest = read_manifest(deployment_dir)?;
    let build_authority = match manifest.authority.tier() {
        AuthorityTier::LocalDeveloper => "local-developer",
        AuthorityTier::TrustedCi => "trusted-ci",
    };

    Ok(DeploymentClaim {
        deployment: DeploymentRef {
            name: metadata.name,
            id: metadata.id,
        },
        platform: metadata.platform,
        format: definition.format,
        definition: DefinitionSection {
            root: definition.path,
            companions: inputs.companions.clone(),
            identity: inputs.identity.clone(),
        },
        engine: EngineSection {
            identity_digest: manifest.identity_digest().to_hex(),
            provisioner_version: manifest.provisioner_version.clone(),
            manifest: BUNDLE_MANIFEST_BASENAME.to_string(),
            build_authority: build_authority.to_string(),
        },
        transition: inputs.transition,
        config_revision: inputs.config_revision,
    })
}

/// Assemble the full publication input for `claim` from its committed dir:
/// the definition documents at their recorded names, every publishable
/// config-tree file at its relative path, the bundle manifest, and the
/// engine binaries resolved through their retention refs.
pub fn publication_input_from_dir(
    deployment_dir: &Path,
    claim: DeploymentClaim,
) -> Result<PublicationInput, PublishError> {
    let mut documents = Vec::new();
    let root_bytes = read_file(deployment_dir, &claim.definition.root)?;
    documents.push((claim.definition.root.clone(), root_bytes));
    for companion in &claim.definition.companions {
        let target = claim.companion_target(companion);
        documents.push((target.clone(), read_file(deployment_dir, &target)?));
    }

    // Config trees: every committed file that is neither a definition
    // document, engine material, nor a seat-local record. The exclusion set
    // is shared by every publisher, so consecutive publications agree on
    // what the config half contains.
    let mut config_tree = Vec::new();
    let mut stack = vec![deployment_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|error| other(format!("reading {}: {error}", dir.display())))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| other(format!("reading {}: {error}", dir.display())))?;
            let path = entry.path();
            let relative = path
                .strip_prefix(deployment_dir)
                .map_err(|error| other(error.to_string()))?
                .to_string_lossy()
                .into_owned();
            if path.is_dir() {
                // `state/` is the deployment's local machinery (envelope,
                // retained revisions, repository client state) — never
                // published content. Hidden dirs are staging artifacts.
                if relative != "state" && !relative.starts_with('.') {
                    stack.push(path);
                }
                continue;
            }
            if relative.starts_with('.')
                || UNPUBLISHED.contains(&relative.as_str())
                || relative == BUNDLE_MANIFEST_BASENAME
                || relative == claim.definition.root
                || claim
                    .definition
                    .companions
                    .iter()
                    .any(|companion| claim.companion_target(companion) == relative)
            {
                continue;
            }
            let bytes = std::fs::read(&path)
                .map_err(|error| other(format!("reading {}: {error}", path.display())))?;
            config_tree.push(relative.clone());
            documents.push((relative, bytes));
        }
    }
    config_tree.sort();

    let manifest = read_manifest(deployment_dir)?;
    let mut bundle_artifacts = Vec::new();
    for artifact in &manifest.artifacts {
        let reference = artifact.retrieval_ref.as_deref().ok_or_else(|| {
            other(format!(
                "artifact `{}` carries no retention ref; the committed engine cannot be published",
                artifact.target.0
            ))
        })?;
        let path: PathBuf = deployment_dir.join("state").join(reference);
        if !path.is_file() {
            return Err(other(format!(
                "artifact `{}` retention ref `{reference}` resolves to no file under state/",
                artifact.target.0
            )));
        }
        bundle_artifacts.push((artifact.target.0.clone(), path));
    }

    Ok(PublicationInput {
        claim,
        documents,
        config_tree,
        bundle_manifest: manifest,
        bundle_artifacts,
    })
}

fn read_manifest(deployment_dir: &Path) -> Result<ProvisionerBundle, PublishError> {
    let path = deployment_dir.join(BUNDLE_MANIFEST_BASENAME);
    serde_json::from_slice(
        &std::fs::read(&path)
            .map_err(|error| other(format!("reading {}: {error}", path.display())))?,
    )
    .map_err(|error| other(format!("{} does not decode: {error}", path.display())))
}

fn read_file(deployment_dir: &Path, relative: &str) -> Result<Vec<u8>, PublishError> {
    let path = deployment_dir.join(relative);
    std::fs::read(&path).map_err(|error| other(format!("reading {}: {error}", path.display())))
}

fn other(message: String) -> PublishError {
    PublishError::Other(message)
}
