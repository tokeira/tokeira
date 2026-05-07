//! Image build, publish, and mirror pipelines.
//!
//! The crate is intentionally platform-independent. Platform crates decide
//! which images exist and what remote references they should use.

mod arch;
mod dagger;
mod dagger_default;
mod error;
mod toolchain;

pub mod pipelines;

#[cfg(test)]
pub(crate) mod testing;

pub use arch::Arch;
pub use dagger::{ContainerRef, DaggerClient, DirectoryRef, FileRef, SecretRef};
pub use dagger_default::DefaultDaggerClient;
pub use error::BuildError;
pub use pipelines::{
    build::{TokeiradBuildRequest, TokeiradBuildResult, build_tokeirad_image},
    mirror::{MirrorRequest, MirroredReference, mirror_image},
    publish::{PublishRequest, PublishResult, PublishedReference, RegistryPassword, publish_image},
};
pub use toolchain::rust_toolchain_version;
