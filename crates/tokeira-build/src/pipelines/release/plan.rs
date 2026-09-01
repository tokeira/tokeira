//! Source admission and canonical release Plan construction.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use cargo_metadata::{Metadata, semver::Version};
use serde::Deserialize;

use crate::CHANGIE_RELEASE;

use super::{
    ChangieIdentity, PackageOutcome, PackagePlan, PackageResult, PlannedRegistryState,
    RELEASE_SCHEMA_VERSION, ReleaseEffect, ReleaseEffectKind, ReleaseError, ReleasePlan,
    RepositoryIdentity, ToolchainIdentity, admit_changelog_config, admit_fragments,
    generate_release_notes, graph::package_by_id, publishable_packages, sha256_hex,
};

/// Checked non-Cargo TOML scalar that participates in Unified Version.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtraVersionField {
    /// Workspace-relative TOML file.
    pub path: PathBuf,
    /// Exact nested scalar key path.
    pub key: Vec<String>,
}

/// Optional Git-source `tkr` pin used only by an external consumer repository.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExternalTkrPin {
    /// HTTPS Git source for Tokeira.
    pub repository: String,
    /// Full 40-hex source revision.
    pub revision: String,
}

/// Strict repository-local release configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseConfig {
    /// Exact supported config schema.
    pub schema_version: u32,
    /// Branch updated by the atomic release push.
    pub release_branch: String,
    /// Checked repository-owned TOML version fields.
    #[serde(default)]
    pub extra_version_fields: Vec<ExtraVersionField>,
    /// External consumer bootstrap pin; forbidden for the Tokeira repository itself.
    pub tkr: Option<ExternalTkrPin>,
}

impl ReleaseConfig {
    /// Read and strictly validate the selected workspace configuration.
    pub fn load(
        workspace_root: &Path,
        repository: &RepositoryIdentity,
    ) -> Result<Self, ReleaseError> {
        let path = workspace_root.join(".tokeira-release.toml");
        let text = std::fs::read_to_string(&path).map_err(|source| ReleaseError::Workspace {
            reason: format!("could not read {}: {source}", path.display()),
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| ReleaseError::Workspace {
            reason: format!("invalid {}: {source}", path.display()),
        })?;
        if config.schema_version != RELEASE_SCHEMA_VERSION {
            return Err(ReleaseError::Workspace {
                reason: format!(
                    "unsupported release config schema {}",
                    config.schema_version
                ),
            });
        }
        if !valid_release_branch(&config.release_branch) {
            return Err(ReleaseError::Workspace {
                reason: "release_branch is not a valid explicit branch name".to_owned(),
            });
        }
        let mut version_fields = BTreeSet::new();
        for field in &config.extra_version_fields {
            let portable_path = !field.path.as_os_str().is_empty()
                && !field.path.is_absolute()
                && field
                    .path
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)));
            if !portable_path || field.key.is_empty() || field.key.iter().any(String::is_empty) {
                return Err(ReleaseError::Workspace {
                    reason: format!(
                        "extra version field must name a workspace-relative path and non-empty key: {} {:?}",
                        field.path.display(),
                        field.key
                    ),
                });
            }
            if !version_fields.insert((field.path.clone(), field.key.clone())) {
                return Err(ReleaseError::Workspace {
                    reason: format!(
                        "duplicate extra version field: {} {:?}",
                        field.path.display(),
                        field.key
                    ),
                });
            }
        }
        if repository.slug == "tokeira/tokeira" && config.tkr.is_some() {
            return Err(ReleaseError::Workspace {
                reason: "the in-tree Tokeira release config forbids a `tkr` table".to_owned(),
            });
        }
        if let Some(pin) = &config.tkr {
            let revision_is_valid = pin.revision.len() == 40
                && pin
                    .revision
                    .chars()
                    .all(|character| character.is_ascii_hexdigit());
            if !pin.repository.starts_with("https://") || !revision_is_valid {
                return Err(ReleaseError::Workspace {
                    reason: "external tkr pin requires an HTTPS source and full 40-hex revision"
                        .to_owned(),
                });
            }
        }
        Ok(config)
    }
}

fn valid_release_branch(branch: &str) -> bool {
    !branch.is_empty()
        && branch != "HEAD"
        && !branch.starts_with('-')
        && !branch.starts_with('.')
        && !branch.ends_with(['.', '/'])
        && !branch.contains("..")
        && !branch.contains("@{")
        && !branch.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
        && branch.split('/').all(|component| {
            !component.is_empty() && !component.starts_with('.') && !component.ends_with(".lock")
        })
}

/// Immutable bytes needed to preview final consumer notes during read-only planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedArtifact {
    /// SHA-256 of the hermetic target-version `.crate` bytes.
    pub sha256: String,
    /// Version-specific registry README URL.
    pub readme_url: String,
}

/// Secret-free inputs assembled before planning.
#[derive(Debug)]
pub struct ReleasePlanRequest<'a> {
    /// Canonical selected workspace.
    pub workspace_root: PathBuf,
    /// Stable greater target SemVer from the CLI.
    pub target_version: String,
    /// Explicit comparison base; the configured upstream is used when absent.
    pub base_ref: Option<String>,
    /// Canonical public repository identity.
    pub repository: RepositoryIdentity,
    /// Cargo metadata produced with the selected locked workspace.
    pub metadata: &'a Metadata,
    /// Direct external registry versions needed to package the publishable closure.
    pub external_dependencies: Vec<PackageIdentity>,
    /// Read-only hermetic package evidence used by the exact notes preview.
    pub planned_artifacts: BTreeMap<String, PlannedArtifact>,
    /// Exact changie version-file bytes produced by the isolated planning graph.
    pub version_body: String,
    /// Executor platform used to select the pinned changie archive.
    pub changie_platform: String,
    /// Pinned Rust and Dagger identities.
    pub toolchain: ToolchainIdentity,
}

/// Cleanliness and synchronization facts observed without mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitObservation {
    /// Full source commit at HEAD.
    pub head_commit: String,
    /// Full commit resolved from the selected base ref.
    pub base_commit: String,
    /// Whether tracked and untracked source state is clean.
    pub clean: bool,
    /// Whether HEAD is exactly the admitted base.
    pub up_to_date: bool,
}

/// Package/version identity used for public registry observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageIdentity {
    /// Cargo package name.
    pub name: String,
    /// Stable target version.
    pub version: String,
}

/// Read-only public registry observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryObservation {
    /// The version is not currently observable.
    Absent,
    /// The version exists with this registry checksum.
    Existing { checksum: String },
}

/// External read seam used by local Dagger and offline tests.
pub trait ReleaseObservations: Send + Sync {
    /// Observe clean and synchronized Git state for the request.
    fn git(&self, request: &ReleasePlanRequest<'_>) -> Result<GitObservation, ReleaseError>;
    /// Observe public package/version state without a registry credential.
    fn registry(&self, package: &PackageIdentity) -> Result<RegistryObservation, ReleaseError>;
}

/// Build and seal one deterministic, secret-free release Plan.
pub fn plan_release(
    request: &ReleasePlanRequest<'_>,
    observations: &dyn ReleaseObservations,
) -> Result<ReleasePlan, ReleaseError> {
    let workspace_root =
        request
            .workspace_root
            .canonicalize()
            .map_err(|source| ReleaseError::Workspace {
                reason: format!(
                    "could not canonicalize {}: {source}",
                    request.workspace_root.display()
                ),
            })?;
    let metadata_root = request
        .metadata
        .workspace_root
        .as_std_path()
        .canonicalize()
        .map_err(|source| ReleaseError::Workspace {
            reason: format!("could not canonicalize Cargo metadata root: {source}"),
        })?;
    if workspace_root != metadata_root {
        return Err(ReleaseError::WorkspaceMismatch {
            expected: workspace_root,
            observed: metadata_root,
        });
    }

    let config = ReleaseConfig::load(&workspace_root, &request.repository)?;
    let git = observations.git(request)?;
    if !git.clean {
        return Err(ReleaseError::DirtyWorkspace {
            commit: git.head_commit,
        });
    }
    if !git.up_to_date || git.head_commit != git.base_commit {
        return Err(ReleaseError::StaleWorkspace {
            head: git.head_commit,
            base: git.base_commit,
        });
    }
    for dependency in &request.external_dependencies {
        if matches!(
            observations.registry(dependency)?,
            RegistryObservation::Absent
        ) {
            return Err(ReleaseError::ExternalDependency {
                package: dependency.name.clone(),
                version: dependency.version.clone(),
            });
        }
    }
    let target =
        Version::parse(&request.target_version).map_err(|source| ReleaseError::TargetVersion {
            reason: source.to_string(),
        })?;
    if !target.pre.is_empty() || !target.build.is_empty() {
        return Err(ReleaseError::TargetVersion {
            reason: "release target must be stable SemVer without pre-release/build metadata"
                .to_owned(),
        });
    }

    let graph = publishable_packages(request.metadata)?;
    let mut versions = graph
        .iter()
        .filter_map(|node| package_by_id(request.metadata, &node.package_id))
        .map(|package| package.version.clone())
        .collect::<Vec<_>>();
    versions.sort();
    versions.dedup();
    if versions.len() != 1 {
        return Err(ReleaseError::NonUnifiedVersion {
            versions: versions.iter().map(ToString::to_string).collect(),
        });
    }
    let current = versions.first().ok_or_else(|| ReleaseError::Workspace {
        reason: "publish graph lost every package version".to_owned(),
    })?;
    if target <= *current {
        return Err(ReleaseError::TargetVersion {
            reason: format!("target {target} must be greater than unified version {current}"),
        });
    }

    let changelog_config_sha256 = admit_changelog_config(&workspace_root)?;
    let fragments = admit_fragments(&workspace_root)?;
    if fragments.is_empty() {
        return Err(ReleaseError::Changelog {
            path: PathBuf::from(".changes/unreleased"),
            reason: "a release requires at least one admitted fragment".to_owned(),
        });
    }
    let mut packages = Vec::with_capacity(graph.len());
    let mut note_packages = Vec::with_capacity(graph.len());
    for node in graph {
        let package = package_by_id(request.metadata, &node.package_id).ok_or_else(|| {
            ReleaseError::Workspace {
                reason: format!("Cargo package {} disappeared during planning", node.name),
            }
        })?;
        let relative_manifest = package
            .manifest_path
            .as_std_path()
            .strip_prefix(&workspace_root)
            .map_err(|_| ReleaseError::Workspace {
                reason: format!("manifest for {} is outside the workspace", node.name),
            })?
            .to_path_buf();
        let identity = PackageIdentity {
            name: node.name.clone(),
            version: target.to_string(),
        };
        let registry = match observations.registry(&identity)? {
            RegistryObservation::Absent => PlannedRegistryState::Absent,
            RegistryObservation::Existing { checksum } => {
                PlannedRegistryState::Existing { checksum }
            }
        };
        let artifact = request.planned_artifacts.get(&node.name).ok_or_else(|| {
            ReleaseError::PackageDryRun {
                reason: format!("missing hermetic planning artifact for {}", node.name),
            }
        })?;
        packages.push(PackagePlan {
            name: node.name.clone(),
            manifest_path: relative_manifest,
            from_version: current.to_string(),
            target_version: target.to_string(),
            publishable_dependencies: node.dependencies,
            registry,
        });
        note_packages.push(PackageResult {
            name: node.name,
            version: target.to_string(),
            outcome: PackageOutcome::ExistingVerified,
            hermetic_sha256: Some(artifact.sha256.clone()),
            downloaded_sha256: Some(artifact.sha256.clone()),
            registry_sha256: Some(artifact.sha256.clone()),
            readme_url: Some(artifact.readme_url.clone()),
        });
    }
    let notes = generate_release_notes(&request.version_body, &note_packages)?;
    let asset = CHANGIE_RELEASE
        .asset(&request.changie_platform)
        .ok_or_else(|| ReleaseError::Tool {
            reason: format!(
                "unsupported changie executor platform {}",
                request.changie_platform
            ),
        })?;
    let existing_count = packages
        .iter()
        .filter(|package| matches!(package.registry, PlannedRegistryState::Existing { .. }))
        .count();
    let tag = format!("v{target}");
    let mut effects = vec![
        ReleaseEffect {
            kind: ReleaseEffectKind::Source,
            summary: format!(
                "rewrite {} publishable packages to {target} and batch {} fragments",
                packages.len(),
                fragments.len()
            ),
        },
        ReleaseEffect {
            kind: ReleaseEffectKind::Git,
            summary: format!(
                "atomically publish branch {} and annotated tag {tag}",
                config.release_branch
            ),
        },
    ];
    effects.extend(packages.iter().map(|package| ReleaseEffect {
        kind: ReleaseEffectKind::Registry,
        summary: match &package.registry {
            PlannedRegistryState::Absent => {
                format!("publish and verify {} {target}", package.name)
            }
            PlannedRegistryState::Existing { .. } => {
                format!("download and verify existing {} {target}", package.name)
            }
        },
    }));
    effects.push(ReleaseEffect {
        kind: ReleaseEffectKind::Release,
        summary: format!(
            "create or verify release {tag} after parity ({} of {} packages already observed)",
            existing_count,
            packages.len()
        ),
    });
    let mut plan = ReleasePlan {
        schema_version: RELEASE_SCHEMA_VERSION,
        repository: request.repository.clone(),
        workspace_root,
        base_commit: git.base_commit,
        target_version: target.to_string(),
        tag,
        packages,
        fragments: fragments.iter().map(Into::into).collect(),
        changelog_config_sha256,
        changie_release: ChangieIdentity {
            version: CHANGIE_RELEASE.version.to_owned(),
            source_revision: CHANGIE_RELEASE.source_revision.to_owned(),
            platform: asset.platform.to_owned(),
            asset: asset.name.to_owned(),
            asset_sha256: asset.sha256.to_owned(),
        },
        toolchain: request.toolchain.clone(),
        release_notes_sha256: sha256_hex(&notes),
        effects,
        digest: String::new(),
    };
    plan.seal()?;
    Ok(plan)
}
