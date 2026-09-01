//! Deterministic planning, preparation, publication, and verification for release trains.
//!
//! The train is one deliberate sequence: an operator plans against a clean, current
//! workspace and gets a secret-free Plan; confirms that exact Plan; the executor
//! prepares the tagged source, publishes the release branch and tag atomically,
//! publishes packages in dependency order with three-way parity, and only then, in a
//! separate invocation with a separate credential, creates the immutable release
//! notes. Every decision on that path is a pure function here; the Dagger executor
//! only observes and moves bytes.

mod apply;
mod dagger;
mod error;
mod fragment;
mod graph;
mod model;
mod notes;
mod plan;
mod prepare;
mod scripts;

pub use apply::{
    ObservedGitRef, RegistryCredential, ReleaseAdmission, ReleaseApiCredential,
    ReleaseDaggerClient, ReleaseNotesRequest, ReleasePublishRequest, ReleaseVerifyRequest,
    RemoteGitObservation, TrainPhaseFacts, admit_release_refs, classify_train_state,
    create_release_notes, gh_release_create_arguments, next_upload_at, publish_and_verify_release,
    registry_observation_delays, require_apply_admission, verify_published_refs, verify_release,
    verify_resume_refs,
};
pub use dagger::{
    ObservedReleaseInputs, PlannedReleaseArtifacts, ReleaseObjectObservation,
    observe_planned_artifacts, observe_release_inputs, observe_release_object,
    plan_release_with_dagger,
};
pub use error::ReleaseError;
pub use fragment::{
    AdmittedFragment, CANONICAL_CHANGELOG_CONFIG_SHA256, admit_changelog_config, admit_fragments,
    canonical_changelog_config_sha256, fragment_filename, render_version_body,
};
pub use graph::{
    PublishableNode, external_publish_dependencies, publishable_packages, stable_topological_order,
};
pub use model::{
    ChangieIdentity, FragmentIdentity, PackageOutcome, PackagePlan, PackageResult,
    PlannedRegistryState, PublishParityReport, RELEASE_SCHEMA_VERSION, ReleaseDiagnostic,
    ReleaseEffect, ReleaseEffectKind, ReleaseNotesOutcome, ReleaseNotesResult, ReleasePlan,
    ReleaseReport, RepositoryIdentity, TagResult, ToolchainIdentity, TrainIdentity, TrainState,
};
pub use notes::{generate_release_notes, verify_artifact_parity};
pub use plan::{
    ExternalTkrPin, ExtraVersionField, GitObservation, PackageIdentity, PlannedArtifact,
    RegistryObservation, ReleaseConfig, ReleaseObservations, ReleasePlanRequest, plan_release,
};
pub use prepare::{
    atomic_git_push_arguments, cargo_package_arguments, cargo_package_arguments_for_names,
    rewrite_extra_version_field, rewrite_manifest, rewrite_workspace_manifests,
};
pub use tokeira_deployment::sha256_hex;
