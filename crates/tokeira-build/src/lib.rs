//! Image build, publish, and mirror pipelines.
//!
//! The crate is intentionally platform-independent. Platform crates decide
//! which images exist and what remote references they should use.

mod arch;
mod closure;
mod dagger;
mod dagger_default;
mod discovery;
mod error;
mod snapshot;
mod toolchain;

pub mod pipelines;

#[cfg(test)]
pub(crate) mod testing;

pub use arch::Arch;
pub use closure::{ClosureError, LockedDependency, ProvisionerClosure, resolve_source_closure};
pub use dagger::{ContainerRef, DaggerClient, DirectoryRef, FileRef, SecretRef};
pub use dagger_default::DefaultDaggerClient;
pub use discovery::{
    DEFINITION_FRONTEND_CONTRACT, DefinitionFrontendPackageDescriptor, DefinitionSourceExtension,
    DiscoveryError, PLATFORM_BINDING_CONTRACT, PackageCoordinates, PlatformLaunchClass,
    PlatformPackageDescriptor, WorkspaceDescriptors, discover_workspace_descriptors,
};
pub use error::BuildError;
pub use pipelines::{
    build::{TokeiradBuildRequest, TokeiradBuildResult, build_tokeirad_image},
    mirror::{MirrorRequest, MirroredReference, mirror_image},
    obtain::{ObtainedProvisioner, obtain_provisioner},
    provisioner::{ProvisionerBuildRequest, build_provisioner, engine_identity_for},
    publish::{PublishRequest, PublishResult, PublishedReference, RegistryPassword, publish_image},
};
pub use snapshot::{
    SnapshotError, SnapshotRequest, SourceSnapshot, materialize_snapshot, snapshot_source_closure,
};
pub use toolchain::rust_toolchain_version;
