//! Portable, secret-free release Plan and Report schemas.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{ReleaseError, sha256_hex};

/// Current JSON schema used by release Plans and Reports.
pub const RELEASE_SCHEMA_VERSION: u32 = 1;

/// Canonical public repository identity without URL credentials.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIdentity {
    /// Repository slug, normally `owner/name`.
    pub slug: String,
    /// Canonical HTTPS remote with transport and user-info removed.
    pub remote: String,
}

impl RepositoryIdentity {
    /// Derive the canonical identity from a Git remote URL.
    ///
    /// SSH and HTTPS spellings of the same GitHub repository collapse to one identity,
    /// and any user-info (a token embedded in an HTTPS remote) is dropped before the
    /// value can reach a Plan, a trailer, or a diagnostic.
    pub fn from_remote(raw: &str) -> Result<Self, ReleaseError> {
        let raw = raw.trim();
        let https = if let Some(path) = raw.strip_prefix("git@github.com:") {
            format!("https://github.com/{path}")
        } else if let Some(path) = raw.strip_prefix("ssh://git@github.com/") {
            format!("https://github.com/{path}")
        } else if let Some(rest) = raw.strip_prefix("https://") {
            let without_user = rest.rsplit_once('@').map_or(rest, |(_, value)| value);
            format!("https://{without_user}")
        } else {
            return Err(ReleaseError::Workspace {
                reason: "origin remote must use GitHub HTTPS or SSH syntax".to_owned(),
            });
        };
        let path = https
            .strip_prefix("https://github.com/")
            .ok_or_else(|| ReleaseError::Workspace {
                reason: "origin is not a canonical GitHub repository".to_owned(),
            })?
            .trim_end_matches('/');
        let slug = path.strip_suffix(".git").unwrap_or(path).to_owned();
        if slug.split('/').filter(|part| !part.is_empty()).count() != 2 {
            return Err(ReleaseError::Workspace {
                reason: format!("origin does not name one owner/repository: {slug}"),
            });
        }
        Ok(Self {
            remote: format!("https://github.com/{slug}"),
            slug,
        })
    }
}

/// One admitted changelog fragment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FragmentIdentity {
    /// Workspace-relative fragment path.
    pub path: PathBuf,
    /// SHA-256 of the exact fragment bytes.
    pub sha256: String,
}

/// Exact changie artifact selected for this plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangieIdentity {
    /// Pinned changie version.
    pub version: String,
    /// Full upstream source revision.
    pub source_revision: String,
    /// Stable platform key.
    pub platform: String,
    /// Exact upstream archive name.
    pub asset: String,
    /// Exact upstream archive digest.
    pub asset_sha256: String,
}

/// Compiler and executor identities used by reproducibility gates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolchainIdentity {
    /// Rust toolchain version from the selected workspace.
    pub rust: String,
    /// Pinned Dagger engine version.
    pub dagger: String,
}

/// Registry state observed while planning.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlannedRegistryState {
    /// The package/version is not observable.
    Absent,
    /// The package/version is observable with this registry checksum.
    Existing { checksum: String },
}

/// One publishable package in dependency order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PackagePlan {
    /// Cargo package name.
    pub name: String,
    /// Workspace-relative manifest path.
    pub manifest_path: PathBuf,
    /// Unified source version before preparation.
    pub from_version: String,
    /// Unified version after preparation.
    pub target_version: String,
    /// Internal publishable prerequisites in lexical order.
    pub publishable_dependencies: Vec<String>,
    /// SHA-256 of the hermetic target-version `.crate` bytes built while planning.
    ///
    /// The Hermetic Tag Build must reproduce exactly these bytes before anything is
    /// pushed, and every registry checksum must equal them; a train whose bytes cannot
    /// match is refused before its first irreversible step.
    pub hermetic_sha256: String,
    /// Read-only crates.io state observed during planning.
    pub registry: PlannedRegistryState,
}

/// Kind of outward effect shown at the confirmation boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseEffectKind {
    /// Workspace source mutation.
    Source,
    /// Atomic branch and tag publication.
    Git,
    /// Package upload or verification.
    Registry,
    /// Release-note creation or verification.
    Release,
}

/// One deterministic outward effect displayed before apply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEffect {
    /// Stable effect category.
    pub kind: ReleaseEffectKind,
    /// Human-readable, secret-free description.
    pub summary: String,
}

/// Secret-free, serializable release intent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleasePlan {
    /// Exact supported Plan schema.
    pub schema_version: u32,
    /// Selected public repository.
    pub repository: RepositoryIdentity,
    /// Canonical local path used only for admission and apply.
    pub workspace_root: PathBuf,
    /// Full Git object ID of the admitted source base.
    pub base_commit: String,
    /// Stable target SemVer.
    pub target_version: String,
    /// Exact annotated tag name.
    pub tag: String,
    /// Complete publishable closure in deterministic order.
    pub packages: Vec<PackagePlan>,
    /// Complete fragment inventory in path order.
    pub fragments: Vec<FragmentIdentity>,
    /// Digest of the byte-exact shared changie config set.
    pub changelog_config_sha256: String,
    /// Pinned changie identity.
    pub changie_release: ChangieIdentity,
    /// Pinned build identities.
    pub toolchain: ToolchainIdentity,
    /// Digest of the planning-time note preview.
    ///
    /// Informational only: the version heading carries the changie batch date, so the
    /// preview advances with the calendar. The notes that count are derived from the
    /// tagged version file at apply and verify time and carried by the Release Report.
    pub release_notes_sha256: String,
    /// Ordered effects that require operator confirmation.
    pub effects: Vec<ReleaseEffect>,
    /// Digest of the portable, observation-independent Train Identity input.
    pub digest: String,
}

#[derive(Serialize)]
struct DigestPackage<'a> {
    name: &'a str,
    manifest_path: &'a PathBuf,
    from_version: &'a str,
    target_version: &'a str,
    publishable_dependencies: &'a [String],
    hermetic_sha256: &'a str,
}

/// The Train Identity input: everything that fixes *what* the train publishes.
///
/// Excluded on purpose: `workspace_root` (host detail), registry observations (they
/// advance as the train progresses), and `release_notes_sha256` (its version heading
/// is dated by the batch, so including it would make a Plan expire at midnight and a
/// pending train unresumable the next day). Included on purpose: the hermetic crate
/// checksums, which bind the identity to the exact bytes the train may publish.
#[derive(Serialize)]
struct PlanDigestInput<'a> {
    schema_version: u32,
    repository: &'a RepositoryIdentity,
    base_commit: &'a str,
    target_version: &'a str,
    tag: &'a str,
    packages: Vec<DigestPackage<'a>>,
    fragments: &'a [FragmentIdentity],
    changelog_config_sha256: &'a str,
    changie_release: &'a ChangieIdentity,
    toolchain: &'a ToolchainIdentity,
    effects: &'a [ReleaseEffect],
}

impl ReleasePlan {
    /// Serialize the complete local Plan with stable struct-field order.
    pub fn canonical_json(&self) -> Result<Vec<u8>, ReleaseError> {
        let mut bytes = serde_json::to_vec_pretty(self).map_err(|source| ReleaseError::Plan {
            reason: format!("could not serialize the plan: {source}"),
        })?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Compute the Train Identity digest, excluding host path and advancing observations.
    pub fn computed_digest(&self) -> Result<String, ReleaseError> {
        let packages = self
            .packages
            .iter()
            .map(|package| DigestPackage {
                name: &package.name,
                manifest_path: &package.manifest_path,
                from_version: &package.from_version,
                target_version: &package.target_version,
                publishable_dependencies: &package.publishable_dependencies,
                hermetic_sha256: &package.hermetic_sha256,
            })
            .collect();
        let input = PlanDigestInput {
            schema_version: self.schema_version,
            repository: &self.repository,
            base_commit: &self.base_commit,
            target_version: &self.target_version,
            tag: &self.tag,
            packages,
            fragments: &self.fragments,
            changelog_config_sha256: &self.changelog_config_sha256,
            changie_release: &self.changie_release,
            toolchain: &self.toolchain,
            effects: &self.effects,
        };
        let bytes = serde_json::to_vec(&input).map_err(|source| ReleaseError::Plan {
            reason: format!("could not canonicalize the plan identity: {source}"),
        })?;
        Ok(sha256_hex(&bytes))
    }

    /// Set the digest to the canonical identity digest.
    pub fn seal(&mut self) -> Result<(), ReleaseError> {
        self.digest = self.computed_digest()?;
        Ok(())
    }

    /// Reject unsupported schema or tampered Plan bytes before any mutation.
    pub fn validate_digest(&self) -> Result<(), ReleaseError> {
        if self.schema_version != RELEASE_SCHEMA_VERSION {
            return Err(ReleaseError::UnsupportedPlanSchema {
                observed: self.schema_version,
            });
        }
        let computed = self.computed_digest()?;
        if computed != self.digest {
            return Err(ReleaseError::Plan {
                reason: format!(
                    "digest mismatch: expected {}, observed {computed}",
                    self.digest
                ),
            });
        }
        Ok(())
    }
}

/// Durable identity that must match every resume observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainIdentity {
    /// Public repository identity.
    pub repository: RepositoryIdentity,
    /// Full admitted base commit.
    pub base_commit: String,
    /// Stable release version.
    pub target_version: String,
    /// Canonical Plan digest.
    pub plan_digest: String,
}

impl From<&ReleasePlan> for TrainIdentity {
    fn from(plan: &ReleasePlan) -> Self {
        Self {
            repository: plan.repository.clone(),
            base_commit: plan.base_commit.clone(),
            target_version: plan.target_version.clone(),
            plan_digest: plan.digest.clone(),
        }
    }
}

/// Outcome for one package/version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackageOutcome {
    /// Uploaded during this invocation and verified.
    Published,
    /// Already existed and matched the hermetic artifact.
    ExistingVerified,
    /// Upload outcome remains ambiguous inside the bounded observation window.
    Pending,
    /// A non-integrity package operation failed.
    Failed,
}

/// Secret-free evidence for one package/version.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PackageResult {
    /// Package name.
    pub name: String,
    /// Released version.
    pub version: String,
    /// Classified outcome.
    pub outcome: PackageOutcome,
    /// SHA-256 of the hermetic `.crate` archive when available.
    pub hermetic_sha256: Option<String>,
    /// SHA-256 of registry-downloaded bytes when available.
    pub downloaded_sha256: Option<String>,
    /// Registry checksum metadata when available.
    pub registry_sha256: Option<String>,
    /// Version-specific registry README URL when verified.
    pub readme_url: Option<String>,
}

/// Evidence for the annotated release tag and its commit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TagResult {
    /// Exact `v`-prefixed tag.
    pub tag: String,
    /// Peeled release commit object ID.
    pub commit: String,
    /// Whether the mutually consistent branch/tag pair is public.
    pub published: bool,
}

/// Release-object outcome after parity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseNotesOutcome {
    /// No release-object mutation was attempted.
    Absent,
    /// A matching immutable release already existed.
    ExistingVerified,
    /// This invocation created the immutable release.
    Created,
}

/// Evidence for the public release object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseNotesResult {
    /// Classified release-object outcome.
    pub outcome: ReleaseNotesOutcome,
    /// Digest of the exact notes bytes.
    pub sha256: String,
}

/// Stable diagnostic attached to a report without secret-bearing sources.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseDiagnostic {
    /// Stable machine-readable condition.
    pub code: String,
    /// Concise operator-facing summary.
    pub summary: String,
    /// Optional sanitized evidence.
    pub details: Option<String>,
}

/// Durable train state exposed to operators.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrainState {
    /// Plan exists but no public effect is durable.
    Planned,
    /// Preparation, build, or atomic Git publication failed before an upload.
    PrePublicationFailed,
    /// Git is public and registry or release-note work remains.
    PartiallyPublished,
    /// Immutable public bytes conflict with the train.
    TerminalMismatch,
    /// Git, every package, parity, and notes are complete.
    Complete,
}

/// Result of the registry-secret publish-and-parity invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishParityReport {
    /// Exact supported report schema.
    pub schema_version: u32,
    /// Durable train identity.
    pub train: TrainIdentity,
    /// Package evidence in dependency order.
    pub packages: Vec<PackageResult>,
    /// Atomic branch/tag evidence.
    pub tag: TagResult,
    /// Digest required of the later release-note invocation.
    pub release_notes_sha256: String,
    /// Secret-free diagnostics.
    pub diagnostics: Vec<ReleaseDiagnostic>,
}

/// Complete verification report after the structurally separate notes phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReport {
    /// Exact supported report schema.
    pub schema_version: u32,
    /// Durable train identity.
    pub train: TrainIdentity,
    /// Classified durable state.
    pub state: TrainState,
    /// Package evidence in dependency order.
    pub packages: Vec<PackageResult>,
    /// Atomic branch/tag evidence.
    pub tag: TagResult,
    /// Release-object evidence.
    pub release_notes: ReleaseNotesResult,
    /// Secret-free diagnostics.
    pub diagnostics: Vec<ReleaseDiagnostic>,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn remote_identity_removes_transport_details() {
        for remote in [
            "git@github.com:tokeira/tokeira.git",
            "ssh://git@github.com/tokeira/tokeira.git",
            "https://operator:token@github.com/tokeira/tokeira.git",
            "https://github.com/tokeira/tokeira/",
        ] {
            let identity = RepositoryIdentity::from_remote(remote).expect("GitHub remote");
            assert_eq!(identity.slug, "tokeira/tokeira");
            assert_eq!(identity.remote, "https://github.com/tokeira/tokeira");
        }
        assert!(RepositoryIdentity::from_remote("https://gitlab.com/tokeira/tokeira").is_err());
        assert!(RepositoryIdentity::from_remote("git@github.com:tokeira").is_err());
    }

    fn plan(
        root: PathBuf,
        registry: PlannedRegistryState,
        hermetic_sha256: &str,
        release_notes_sha256: &str,
    ) -> ReleasePlan {
        let mut plan = ReleasePlan {
            schema_version: RELEASE_SCHEMA_VERSION,
            repository: RepositoryIdentity {
                slug: "tokeira/tokeira".to_owned(),
                remote: "https://github.com/tokeira/tokeira".to_owned(),
            },
            workspace_root: root,
            base_commit: "a".repeat(40),
            target_version: "0.2.0".to_owned(),
            tag: "v0.2.0".to_owned(),
            packages: vec![PackagePlan {
                name: "crate-a".to_owned(),
                manifest_path: PathBuf::from("crate-a/Cargo.toml"),
                from_version: "0.1.0".to_owned(),
                target_version: "0.2.0".to_owned(),
                publishable_dependencies: Vec::new(),
                hermetic_sha256: hermetic_sha256.to_owned(),
                registry,
            }],
            fragments: Vec::new(),
            changelog_config_sha256: "b".repeat(64),
            changie_release: ChangieIdentity {
                version: "1.25.2".to_owned(),
                source_revision: "c".repeat(40),
                platform: "linux-x86_64".to_owned(),
                asset: "changie.tar.gz".to_owned(),
                asset_sha256: "d".repeat(64),
            },
            toolchain: ToolchainIdentity {
                rust: "1.97.1".to_owned(),
                dagger: "0.19.8".to_owned(),
            },
            release_notes_sha256: release_notes_sha256.to_owned(),
            effects: Vec::new(),
            digest: String::new(),
        };
        plan.seal().expect("valid canonical plan");
        plan
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 2: canonical Plan determinism and secret independence
        #[test]
        fn plan_digest_excludes_host_and_advancing_observations(
            root_a in "/[a-z]{1,8}",
            root_b in "/[a-z]{1,8}",
            checksum in "[0-9a-f]{64}",
            hermetic in "[0-9a-f]{64}",
            other_hermetic in "[0-9a-f]{64}",
            notes_today in "[0-9a-f]{64}",
            notes_tomorrow in "[0-9a-f]{64}",
            registry_token in proptest::collection::vec(any::<u8>(), 1..64),
        ) {
            // Host path, registry state, and the dated note preview may all differ
            // between planning and apply without changing what the train publishes.
            let absent = plan(
                PathBuf::from(root_a),
                PlannedRegistryState::Absent,
                &hermetic,
                &notes_today,
            );
            let existing = plan(
                PathBuf::from(root_b),
                PlannedRegistryState::Existing { checksum },
                &hermetic,
                &notes_tomorrow,
            );
            prop_assert_eq!(&absent.digest, &existing.digest);
            prop_assert!(absent.validate_digest().is_ok());
            prop_assert!(existing.validate_digest().is_ok());

            // Different crate bytes are a different train.
            let rebuilt = plan(
                PathBuf::from("/same"),
                PlannedRegistryState::Absent,
                &other_hermetic,
                &notes_today,
            );
            prop_assert_eq!(rebuilt.digest == absent.digest, other_hermetic == hermetic);

            // No credential byte can reach the Plan's canonical form.
            let canonical = absent.canonical_json().expect("canonical Plan JSON");
            let token = String::from_utf8_lossy(&registry_token).into_owned();
            prop_assert!(token.is_empty() || token.len() < 8 || !String::from_utf8_lossy(&canonical).contains(&token));
        }
    }
}
