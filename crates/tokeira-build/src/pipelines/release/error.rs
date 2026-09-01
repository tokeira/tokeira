//! Typed release failures with stable operator-facing classifications.

use std::path::PathBuf;

use thiserror::Error;

/// A release refusal or executor failure.
#[derive(Debug, Error)]
pub enum ReleaseError {
    /// The requested Cargo workspace could not be selected uniquely.
    #[error("release workspace admission failed: {reason}")]
    Workspace { reason: String },
    /// The starting path belongs to more than one nested Cargo workspace.
    #[error("release workspace is ambiguous: {roots:?}")]
    AmbiguousWorkspace { roots: Vec<PathBuf> },
    /// The stored Plan and explicitly selected workspace differ.
    #[error("release workspace mismatch: expected {expected}, observed {observed}")]
    WorkspaceMismatch {
        expected: PathBuf,
        observed: PathBuf,
    },
    /// The selected source contains tracked or untracked mutations.
    #[error("release source is dirty at {commit}")]
    DirtyWorkspace { commit: String },
    /// The selected source does not identify the admitted base.
    #[error("release source is stale: head {head}, base {base}")]
    StaleWorkspace { head: String, base: String },
    /// The requested release version is invalid or does not advance the train.
    #[error("invalid release target version: {reason}")]
    TargetVersion { reason: String },
    /// Publishable packages do not share one source version.
    #[error("publishable packages do not have one unified version: {versions:?}")]
    NonUnifiedVersion { versions: Vec<String> },
    /// Internal publishable dependencies contain a cycle.
    #[error("publishable package dependency graph contains a cycle: {packages:?}")]
    PublishGraphCycle { packages: Vec<String> },
    /// Changelog configuration or a fragment is invalid.
    #[error("changelog admission failed for {path}: {reason}")]
    Changelog { path: PathBuf, reason: String },
    /// Repository changie configuration differs from the embedded shared contract.
    #[error("changelog configuration drift: expected {expected}, observed {observed}")]
    ChangelogConfigDrift { expected: String, observed: String },
    /// No pinned changie archive supports the selected host platform.
    #[error("unsupported changie tool platform {platform}; {remediation}")]
    UnsupportedToolPlatform {
        platform: String,
        remediation: String,
    },
    /// The pinned changie binary cannot be admitted.
    #[error("pinned changie tool admission failed: {reason}")]
    Tool { reason: String },
    /// Cargo packaging did not prove the publishable closure.
    #[error("package dry-run failed: {reason}")]
    PackageDryRun { reason: String },
    /// A serialized plan is unsupported, invalid, or has drifted.
    #[error("release plan admission failed: {reason}")]
    Plan { reason: String },
    /// Stored and recomputed canonical Plan identities differ.
    #[error("release Plan drifted: stored {stored}, recomputed {recomputed}")]
    PlanDrift { stored: String, recomputed: String },
    /// The explicitly selected Plan output could not be written atomically.
    #[error("release Plan output failed for {path}: {reason}")]
    PlanOutput { path: PathBuf, reason: String },
    /// The explicitly selected Report output could not be written atomically.
    #[error("release Report output failed for {path}: {reason}")]
    ReportOutput { path: PathBuf, reason: String },
    /// The exact Plan was not confirmed.
    #[error("release confirmation failed: {reason}")]
    Confirmation { reason: String },
    /// The operator explicitly declined the rendered Plan.
    #[error("the operator declined the exact release Plan")]
    ConfirmationDeclined,
    /// The named registry credential is unavailable.
    #[error("registry credential environment variable `{name}` is missing or empty")]
    CredentialMissing { name: String },
    /// The fixed release API credential is unavailable.
    #[error("release credential environment variable `GH_TOKEN` is missing or empty")]
    ReleaseCredentialMissing,
    /// A required package outside the selected workspace is unavailable.
    #[error("external dependency is unavailable: {package} {version}")]
    ExternalDependency { package: String, version: String },
    /// A pre-existing tag does not identify this train.
    #[error("release tag conflict for {tag}: expected {expected}, observed {observed}")]
    TagConflict {
        tag: String,
        expected: String,
        observed: String,
    },
    /// Resume observations do not contain one mutually consistent branch/tag pair.
    #[error(
        "remote release refs conflict: branch observed {branch_observed}; tag observed {tag_observed}"
    )]
    GitRefConflict {
        branch_observed: String,
        tag_observed: String,
    },
    /// An ambiguous upload did not become observable inside the bounded window.
    #[error("registry state remains pending for {package} {version}")]
    RegistryPending { package: String, version: String },
    /// The registry rejected an upload conclusively.
    #[error("registry publication failed for {package} {version}: {reason}")]
    RegistryPublish {
        package: String,
        version: String,
        reason: String,
    },
    /// Hermetic, downloaded, and registry checksums are not identical.
    #[error(
        "artifact mismatch for {package} {version}: hermetic={hermetic}, downloaded={downloaded}, registry={registry}"
    )]
    ArtifactMismatch {
        package: String,
        version: String,
        hermetic: String,
        downloaded: String,
        registry: String,
    },
    /// A release object exists with different immutable fields.
    #[error("release object conflict for {tag}: {reason}")]
    ReleaseConflict { tag: String, reason: String },
    /// Dagger or another owned execution boundary failed.
    #[error("release executor failed: {reason}")]
    Executor { reason: String },
}

impl ReleaseError {
    /// Stable machine code used by CLI JSON error output.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Workspace { .. } => "workspace_not_found",
            Self::AmbiguousWorkspace { .. } => "ambiguous_workspace",
            Self::WorkspaceMismatch { .. } => "workspace_mismatch",
            Self::DirtyWorkspace { .. } => "dirty_workspace",
            Self::StaleWorkspace { .. } => "stale_workspace",
            Self::TargetVersion { .. } => "invalid_target_version",
            Self::NonUnifiedVersion { .. } => "non_unified_workspace_version",
            Self::PublishGraphCycle { .. } => "invalid_publish_graph",
            Self::Changelog { .. } => "invalid_fragment",
            Self::ChangelogConfigDrift { .. } => "changelog_config_drift",
            Self::UnsupportedToolPlatform { .. } => "unsupported_tool_platform",
            Self::Tool { .. } => "tool_pin_drift",
            Self::PackageDryRun { .. } => "package_dry_run_failed",
            Self::Plan { .. } => "invalid_plan",
            Self::PlanDrift { .. } => "plan_drift",
            Self::PlanOutput { .. } => "plan_output_failed",
            Self::ReportOutput { .. } => "report_output_failed",
            Self::Confirmation { .. } => "confirmation_required",
            Self::ConfirmationDeclined => "declined",
            Self::CredentialMissing { .. } => "registry_credential_missing",
            Self::ReleaseCredentialMissing => "release_credential_missing",
            Self::ExternalDependency { .. } => "external_dependency_unavailable",
            Self::TagConflict { .. } => "tag_conflict",
            Self::GitRefConflict { .. } => "git_ref_conflict",
            Self::RegistryPending { .. } => "registry_state_pending",
            Self::RegistryPublish { .. } => "registry_publish_failed",
            Self::ArtifactMismatch { .. } => "artifact_mismatch",
            Self::ReleaseConflict { .. } => "release_conflict",
            Self::Executor { .. } => "executor_failed",
        }
    }

    /// Stable process status class for the CLI boundary.
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Workspace { .. }
            | Self::AmbiguousWorkspace { .. }
            | Self::WorkspaceMismatch { .. }
            | Self::DirtyWorkspace { .. }
            | Self::StaleWorkspace { .. }
            | Self::TargetVersion { .. }
            | Self::NonUnifiedVersion { .. }
            | Self::PublishGraphCycle { .. }
            | Self::Changelog { .. }
            | Self::ChangelogConfigDrift { .. }
            | Self::Plan { .. }
            | Self::PlanDrift { .. }
            | Self::PlanOutput { .. }
            | Self::ReportOutput { .. }
            | Self::Confirmation { .. }
            | Self::ConfirmationDeclined => 2,
            Self::UnsupportedToolPlatform { .. }
            | Self::Tool { .. }
            | Self::CredentialMissing { .. }
            | Self::ReleaseCredentialMissing => 3,
            Self::PackageDryRun { .. } | Self::ExternalDependency { .. } => 4,
            Self::TagConflict { .. }
            | Self::GitRefConflict { .. }
            | Self::ReleaseConflict { .. } => 5,
            Self::RegistryPending { .. } | Self::RegistryPublish { .. } => 6,
            Self::ArtifactMismatch { .. } => 7,
            Self::Executor { .. } => 8,
        }
    }
}
