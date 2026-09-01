//! Build, source-assembly, publish, and mirror pipelines.
//!
//! The crate is intentionally platform-independent. Platform crates decide
//! which images exist and what remote references they should use.

mod arch;
mod changie_release;
mod closure;
pub mod compat_bump;
mod composition;
mod dagger_release;
mod discovery;
mod error;
mod snapshot;
mod toolchain;

pub mod pipelines;

/// Offline fakes (mock Dagger engine, deterministic artifact bytes) for
/// in-crate unit tests and, behind the `testing` feature, downstream
/// integration tests that exercise the real pipelines without a Dagger
/// engine.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use arch::Arch;
pub use changie_release::{CHANGIE_RELEASE, ChangieAsset, ChangieRelease};
pub use closure::{
    ClosureError, LockedDependency, ProvisionerClosure, resolve_source_closure,
    resolve_source_closure_for_packages,
};
pub use composition::{
    BoundProvisionerSource, CompositionError, GENERATED_PROVISIONER_BIN, GENERATED_ROOT_PACKAGE,
    GENERATED_ROOT_RELATIVE_PATH, SCOPED_WORKSPACE_RELATIVE_PATH, TKP_PACKAGE,
    assemble_bound_provisioner,
};
pub use dagger_release::{
    CI_FMT_NIGHTLY, DAGGER_ENGINE_BOOTSTRAP_COMMAND, DAGGER_RELEASE, DaggerRelease,
};
pub use discovery::{
    DefinitionFrontendPackageDescriptor, DiscoveryError, PackageCoordinates,
    PlatformPackageDescriptor, WorkspaceDescriptors, discover_workspace_descriptors,
};
pub use error::BuildError;
pub use pipelines::{
    build::{TokeiradBuildRequest, TokeiradBuildResult, build_tokeirad_image},
    ci::{
        CiBuildMode, CiBuildReport, CiBuildRequest, CiCheck, CiCheckReport, CiCheckRequest,
        CiCheckResult, DaggerClient, run_ci_build, run_ci_checks, workspace_bar_commands,
    },
    mirror::{MirrorRequest, MirroredReference, mirror_image},
    obtain::{ObtainedProvisioner, obtain_provisioner},
    provisioner::{ProvisionerBuildRequest, build_provisioner, engine_identity_for},
    publish::{PublishRequest, PublishResult, PublishedReference, RegistryPassword, publish_image},
    release::{
        AdmittedFragment, CANONICAL_CHANGELOG_CONFIG_SHA256, ChangieIdentity, ExternalTkrPin,
        ExtraVersionField, FragmentIdentity, GitObservation, ObservedGitRef, ObservedReleaseInputs,
        PackageIdentity, PackageOutcome, PackagePlan, PackageResult, PlannedArtifact,
        PlannedRegistryState, PlannedReleaseArtifacts, PreparedRelease, PublishParityReport,
        PublishableNode, RELEASE_SCHEMA_VERSION, RegistryCredential, RegistryObservation,
        ReleaseApiCredential, ReleaseConfig, ReleaseDaggerClient, ReleaseDiagnostic, ReleaseEffect,
        ReleaseEffectKind, ReleaseError, ReleaseNotesOutcome, ReleaseNotesRequest,
        ReleaseNotesResult, ReleaseObjectObservation, ReleaseObservations, ReleasePlan,
        ReleasePlanRequest, ReleasePublishRequest, ReleaseReport, ReleaseSource,
        ReleaseVerifyRequest, RemoteGitObservation, RepositoryIdentity, TagResult,
        ToolchainIdentity, TrainIdentity, TrainPhaseFacts, TrainState, admit_changelog_config,
        admit_fragments, atomic_git_push_arguments, canonical_changelog_config_sha256,
        cargo_package_arguments, classify_train_state, create_release_notes,
        external_publish_dependencies, fragment_filename, generate_release_notes,
        gh_release_create_arguments, next_upload_at, observe_planned_artifacts,
        observe_release_inputs, observe_release_object, plan_release, plan_release_with_dagger,
        prepare_release_source, publish_and_verify_release, publishable_packages,
        registry_observation_delays, render_version_body, require_apply_admission,
        rewrite_extra_version_field, rewrite_manifest, stable_topological_order,
        verify_artifact_parity, verify_release, verify_resume_refs,
    },
};
pub use snapshot::{
    SnapshotError, SnapshotRequest, SourceSnapshot, materialize_snapshot, snapshot_source_closure,
};
pub use tokeira_orchestrator::DefinitionSourceExtension;
pub use toolchain::rust_toolchain_version;
