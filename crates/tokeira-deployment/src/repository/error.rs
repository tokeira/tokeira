//! Typed repository errors with stable refusal names.
//!
//! Every refusal an operator can see carries what happened, why, and what to
//! do next in its `Display`; `--json` consumers key on the stable name from
//! [`Refusal::name`]. The names are the deployment-repository spec's claim
//! and error tables — a rename is a contract change.

use thiserror::Error;

/// A locator that cannot yield well-formed base URLs.
#[derive(Debug, Error)]
pub enum LocatorError {
    /// A local path that is not absolute or not representable as a URL.
    #[error("local repository path `{path}` is not an absolute, URL-representable directory")]
    LocalPath {
        /// The offending path.
        path: String,
    },
    /// Bucket/prefix shape violations (empty, slashes, control characters).
    #[error(
        "s3 locator `s3://{bucket}/{prefix}` is malformed: bucket and prefix must be non-empty, \
         control-free, and the prefix must carry no leading or trailing slash"
    )]
    S3Shape {
        /// Bucket as supplied.
        bucket: String,
        /// Prefix as supplied.
        prefix: String,
    },
    /// URL parsing failed for a shape-valid locator.
    #[error("s3 locator `s3://{bucket}/{prefix}` does not parse as a URL: {error}")]
    S3Parse {
        /// Bucket as supplied.
        bucket: String,
        /// Prefix as supplied.
        prefix: String,
        /// Parser detail.
        error: String,
    },
    /// Joining a sub-path onto the base failed.
    #[error("cannot join `{sub}` onto the repository base: {error}")]
    Join {
        /// The sub-path.
        sub: String,
        /// Parser detail.
        error: String,
    },
}

/// A verification refusal: the repository loaded (or partially loaded) but
/// the publication is not admissible. Stable-named per the spec's claim
/// contract.
#[derive(Debug, Error)]
pub enum Refusal {
    /// No target carries a Deployment Claim.
    #[error(
        "claim_missing: the publication carries no deployment claim; republish from a create or lifecycle transition"
    )]
    ClaimMissing,
    /// More than one target carries a claim.
    #[error(
        "claim_ambiguous: {count} targets carry deployment claims; a publication binds exactly one"
    )]
    ClaimAmbiguous {
        /// How many claims were found.
        count: usize,
    },
    /// The claim's root name disagrees with the target carrying it.
    #[error("claim_root_mismatch: claim names root `{claimed}` but rides target `{carrier}`")]
    ClaimRootMismatch {
        /// Root named inside the claim.
        claimed: String,
        /// Target the claim rode on.
        carrier: String,
    },
    /// The claim decodes but violates its schema.
    #[error("claim_invalid: the deployment claim does not decode: {error}")]
    ClaimInvalid {
        /// Decoder detail.
        error: String,
    },
    /// A claimed companion has no target.
    #[error(
        "claim_companion_missing: claimed companion `{name}` has no target `{target}` in the publication"
    )]
    ClaimCompanionMissing {
        /// Bare companion name.
        name: String,
        /// The target name that was expected.
        target: String,
    },
    /// Recomputed identity disagrees with the claimed identity.
    #[error(
        "identity_mismatch: claim states {claimed_algorithm}:{claimed_digest} but the fetched \
         bytes recompute to {computed_algorithm}:{computed_digest}; refuse the publication"
    )]
    IdentityMismatch {
        /// Claimed algorithm label.
        claimed_algorithm: String,
        /// Claimed digest.
        claimed_digest: String,
        /// Recomputed algorithm label.
        computed_algorithm: String,
        /// Recomputed digest.
        computed_digest: String,
    },
    /// The claim's engine manifest target is absent.
    #[error(
        "engine_manifest_missing: claim names bundle manifest target `{target}` which the publication does not carry"
    )]
    EngineManifestMissing {
        /// The manifest target name.
        target: String,
    },
    /// The fetched bundle manifest does not decode.
    #[error("engine_manifest_invalid: the bundle manifest does not decode: {error}")]
    EngineManifestInvalid {
        /// Decoder detail.
        error: String,
    },
    /// The claim's engine identity digest disagrees with the manifest's.
    #[error(
        "engine_identity_mismatch: claim binds engine identity {claimed} but the manifest carries {manifest}"
    )]
    EngineIdentityMismatch {
        /// Digest from the claim.
        claimed: String,
        /// Digest from the fetched manifest.
        manifest: String,
    },
    /// A manifest artifact's digest disagrees with its TUF target hash, or
    /// its retrieval ref does not name its target.
    #[error(
        "engine_artifact_mismatch: artifact `{target_triple}` disagrees with the publication \
         ({detail}); the bundle and the repository have diverged"
    )]
    EngineArtifactMismatch {
        /// The artifact's target triple.
        target_triple: String,
        /// What disagreed.
        detail: String,
    },
    /// The engine carries no artifact for the requesting host.
    #[error(
        "host_target_unsupported: the publication's engine carries no artifact for host target \
         `{host}`; available: {available}"
    )]
    HostTargetUnsupported {
        /// The requesting host triple.
        host: String,
        /// Comma-joined available triples.
        available: String,
    },
    /// A target read failed TUF verification or transport.
    #[error("target_unreadable: target `{target}` did not verify: {error}")]
    TargetUnreadable {
        /// The target name.
        target: String,
        /// Underlying detail.
        error: String,
    },
}

impl Refusal {
    /// The stable machine-readable refusal name (`--json` contract).
    pub fn name(&self) -> &'static str {
        match self {
            Self::ClaimMissing => "claim_missing",
            Self::ClaimAmbiguous { .. } => "claim_ambiguous",
            Self::ClaimRootMismatch { .. } => "claim_root_mismatch",
            Self::ClaimInvalid { .. } => "claim_invalid",
            Self::ClaimCompanionMissing { .. } => "claim_companion_missing",
            Self::IdentityMismatch { .. } => "identity_mismatch",
            Self::EngineManifestMissing { .. } => "engine_manifest_missing",
            Self::EngineManifestInvalid { .. } => "engine_manifest_invalid",
            Self::EngineIdentityMismatch { .. } => "engine_identity_mismatch",
            Self::EngineArtifactMismatch { .. } => "engine_artifact_mismatch",
            Self::HostTargetUnsupported { .. } => "host_target_unsupported",
            Self::TargetUnreadable { .. } => "target_unreadable",
        }
    }
}

/// Opening/loading failures ahead of claim enforcement.
#[derive(Debug, Error)]
pub enum OpenError {
    /// The pinned trust anchor is unusable before any fetch.
    #[error(
        "trust_anchor_invalid: the pinned root.json is unusable: {error}; re-pin from the publisher's trust anchor"
    )]
    TrustAnchor {
        /// Parser/verifier detail.
        error: String,
    },
    /// The pinned anchor bytes disagree with the recorded digest.
    #[error(
        "trust_anchor_digest_mismatch: state/repository/root.json hashes to {actual} but \
         metadata.json records {recorded}; the pinned file was replaced outside the client"
    )]
    TrustAnchorDigest {
        /// Digest of the on-disk bytes.
        actual: String,
        /// Digest recorded in metadata.json.
        recorded: String,
    },
    /// Locator could not yield URLs.
    #[error(transparent)]
    Locator(#[from] LocatorError),
    /// The TUF chain refused the load (signatures, expiry, rollback).
    #[error("repository_refused: {error}")]
    Verification {
        /// tough's own error rendering (names expiry/rollback/signature).
        error: String,
    },
}

/// Publish-side failures.
#[derive(Debug, Error)]
pub enum PublishError {
    /// Version race: the repository already advanced past the caller's view.
    #[error(
        "publication_conflict: create-only write refused at `{key}` — version {attempted} is \
         already (perhaps partially) written; retry the publication with expected version \
         {attempted}, which republishes the content idempotently at the next version"
    )]
    Conflict {
        /// The colliding object key.
        key: String,
        /// The version this publish attempted.
        attempted: u64,
    },
    /// An immutable object exists with different bytes.
    #[error(
        "immutable_divergence: `{key}` exists with different bytes; the repository holds a \
         conflicting publication and MUST NOT be overwritten"
    )]
    ImmutableDivergence {
        /// The diverging object key.
        key: String,
    },
    /// A non-hermetic engine cannot enter an S3 repository.
    #[error(
        "non_hermetic_engine: the engine has no pinned build container and cannot be published \
         to an S3 repository; build with the hermetic bundle path, or keep the deployment local"
    )]
    NonHermetic,
    /// Signing failed (key source, KMS).
    #[error("signing_failed: {role} could not sign: {error}")]
    Signing {
        /// The role being signed.
        role: &'static str,
        /// Key-source detail (names the KMS RSA constraint when relevant).
        error: String,
    },
    /// Locator could not yield URLs/paths.
    #[error(transparent)]
    Locator(#[from] LocatorError),
    /// Assembly/serialization/io detail.
    #[error("publish_failed: {0}")]
    Other(String),
}

/// Write-path failures shared by both homes.
#[derive(Debug, Error)]
pub enum WriteError {
    /// Create-only collision with differing bytes.
    #[error("`{key}` exists with different bytes")]
    Conflict {
        /// The colliding key.
        key: String,
    },
    /// Underlying transport/filesystem failure.
    #[error("writing `{key}`: {error}")]
    Io {
        /// The key being written.
        key: String,
        /// Underlying detail.
        error: String,
    },
}
