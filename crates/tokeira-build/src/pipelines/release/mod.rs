//! Deterministic planning, preparation, publication, and verification for release trains.

mod apply;
mod dagger;
mod digest;
mod error;
mod fragment;
mod graph;
mod model;
mod notes;
mod plan;
mod prepare;

pub use apply::{
    ObservedGitRef, RegistryCredential, ReleaseApiCredential, ReleaseDaggerClient,
    ReleaseNotesRequest, ReleasePublishRequest, ReleaseVerifyRequest, RemoteGitObservation,
    TrainPhaseFacts, classify_train_state, create_release_notes, gh_release_create_arguments,
    next_upload_at, publish_and_verify_release, registry_observation_delays,
    require_apply_admission, verify_release, verify_resume_refs,
};
pub use dagger::{
    ObservedReleaseInputs, PlannedReleaseArtifacts, ReleaseObjectObservation,
    observe_planned_artifacts, observe_release_inputs, observe_release_object,
    plan_release_with_dagger,
};
pub use digest::sha256_hex;
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
    PreparedRelease, ReleaseSource, atomic_git_push_arguments, cargo_package_arguments,
    prepare_release_source, rewrite_extra_version_field, rewrite_manifest,
};
