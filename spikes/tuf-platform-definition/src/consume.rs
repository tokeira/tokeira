//! Consuming a definition set from a verified TUF repository.
//!
//! The consumer trusts exactly one thing out-of-band: the pinned root.json
//! bytes (the analog of the platform-source-set spec's Deployment Origin).
//! Everything else — which metadata is current, which targets exist, what
//! bytes they must hash to — arrives through the TUF verification chain
//! that `tough` enforces during `RepositoryLoader::load` and
//! `Repository::read_target`.
//!
//! On top of TUF's own guarantees the consumer enforces the product
//! invariant: the recomputed `sha256-set-v1` identity over the fetched
//! bytes, in the claim's served order, must equal the identity the
//! publisher signed into the set claim. A repository whose targets verify
//! individually but whose claim is inconsistent is refused — the claim is
//! the bridge to `config_history` and retarget comparison, so it must never
//! drift from the bytes.

use std::sync::Arc;

use anyhow::Context as _;
use tough::{IntoVec as _, Repository, RepositoryLoader, TargetName, TransportErrorKind};

use crate::{
    publish::{SET_CLAIM_KEY, SetClaim},
    set::{DefinitionSet, SetIdentity, VerifiedPartSources},
};

/// A definition set fetched and verified from a TUF repository — the shape
/// `tkr` would feed into `DefinitionSeed` at deployment create
/// (`apps/tkr/src/deployment_dir.rs:61`: root bytes + named part bytes).
#[derive(Debug)]
pub struct FetchedSet {
    /// The reconstructed definition set, parts in claimed served order.
    pub set: DefinitionSet,
    /// The claim as signed by the publisher.
    pub claim: SetClaim,
    /// The identity recomputed from fetched bytes (equal to
    /// `claim.identity`, or the fetch would have been refused).
    pub identity: SetIdentity,
}

impl FetchedSet {
    /// The verified parts behind the product's `SourceResolver` seam.
    pub fn part_sources(&self) -> VerifiedPartSources {
        VerifiedPartSources::new(self.set.parts.iter().cloned())
    }
}

/// Load a repository from base URLs through a transport, verifying against
/// the pinned `trusted_root` bytes.
///
/// `datastore` is where `tough` persists the metadata it has accepted, which
/// is what makes rollback detection hold across separate loads; a real
/// deployment would point this at deployment-local state.
pub async fn load_repository(
    trusted_root: &[u8],
    metadata_base: url::Url,
    targets_base: url::Url,
    transport: impl tough::Transport + 'static,
    datastore: Option<std::path::PathBuf>,
) -> anyhow::Result<Repository> {
    let mut loader =
        RepositoryLoader::new(&trusted_root, metadata_base, targets_base).transport(transport);
    if let Some(dir) = datastore {
        loader = loader.datastore(dir);
    }
    loader.load().await.context("TUF repository load refused")
}

/// Fetch and verify the complete definition set from a loaded repository.
pub async fn fetch_definition_set(repo: &Repository) -> anyhow::Result<FetchedSet> {
    // 1. Find the one target carrying the set claim.
    let mut claims = Vec::new();
    for (name, target) in repo.targets().signed.targets_iter() {
        if let Some(value) = target.custom.get(SET_CLAIM_KEY) {
            let claim: SetClaim =
                serde_json::from_value(value.clone()).context("decoding signed set claim")?;
            claims.push((name.clone(), claim));
        }
    }
    let (root_name, claim) = match claims.len() {
        1 => claims.remove(0),
        0 => anyhow::bail!("repository carries no definition-set claim"),
        n => anyhow::bail!("repository carries {n} definition-set claims; expected exactly one"),
    };
    anyhow::ensure!(
        claim.root == root_name.raw(),
        "set claim root `{}` does not match its carrying target `{}`",
        claim.root,
        root_name.raw()
    );

    // 2. Fetch the root document and every claimed part. read_target streams
    //    are hash-checked by tough against the signed targets metadata.
    let root = read_target_bytes(repo, &root_name).await?;
    let mut parts: Vec<(String, Arc<[u8]>)> = Vec::new();
    for part in &claim.parts {
        let file_name = format!("{part}.{}", claim.format);
        let target_name = TargetName::new(&file_name)
            .with_context(|| format!("claimed part name `{file_name}`"))?;
        let bytes = read_target_bytes(repo, &target_name).await?;
        parts.push((part.clone(), Arc::from(bytes.into_boxed_slice())));
    }

    // 3. Recompute the composite identity and hold it against the claim.
    let set = DefinitionSet {
        format: claim.format.clone(),
        root_name: claim.root.clone(),
        root,
        parts,
    };
    let identity = set.identity();
    anyhow::ensure!(
        identity == claim.identity,
        "definition-set identity mismatch: claim {}:{}, fetched {}:{}",
        claim.identity.algorithm,
        claim.identity.digest,
        identity.algorithm,
        identity.digest,
    );

    Ok(FetchedSet {
        set,
        claim,
        identity,
    })
}

async fn read_target_bytes(repo: &Repository, name: &TargetName) -> anyhow::Result<Vec<u8>> {
    let stream = repo
        .read_target(name)
        .await
        .with_context(|| format!("reading target `{}`", name.raw()))?
        .with_context(|| format!("target `{}` not found in repository", name.raw()))?;
    stream
        .into_vec()
        .await
        .with_context(|| format!("verifying target `{}`", name.raw()))
}

/// Classify a load error as "file was missing" — used by tests to prove the
/// transport reports absence in the way TUF's root-version walk requires.
pub fn is_file_not_found(err: &tough::error::Error) -> bool {
    matches!(
        err,
        tough::error::Error::Transport { source, .. }
            if source.kind() == TransportErrorKind::FileNotFound
    )
}
