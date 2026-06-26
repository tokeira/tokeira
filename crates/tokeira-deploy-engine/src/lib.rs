//! Generic runtime lifecycle engine for images and services.
//!
//! Provides the [`Image`] and [`Service`] trait abstractions, the [`Platform`]
//! trait for manifest application, context types with a typed extension
//! mechanism, and the [`ServiceEngine`] that coordinates plan/apply operations.
//!
//! Deployment specializations implement this crate by describing desired
//! artifacts and workloads:
//!
//! - [`Image`] resolves the image reference a workload should run.
//! - [`Service`] emits provider-specific manifests for a workload.
//! - [`Platform`] applies those manifests to the target runtime, such as local
//!   Docker Compose, Kubernetes, ECS, or another scheduler.
//!
//! The engine compares desired manifests against persisted runtime state and
//! delegates the actual apply step to [`Platform`]. Provider handles and config
//! should be passed through [`ImageContext`] and [`ServiceContext`] extensions
//! rather than hard-coded in this crate.

mod engine;
mod error;
mod image;
mod platform;
mod service;

pub use engine::{PlannedServiceManifest, ServiceChange, ServiceChangeKind, ServiceEngine};
pub use error::RuntimeError;
pub use image::{
    DesiredImageRef, Image, ImageContext, ImageSourceType, WritebackTarget, validate_registry,
};
pub use platform::Platform;
pub use service::{Service, ServiceContext};

/// Public error alias for [`RuntimeError`].
pub type DeployError = RuntimeError;

/// Re-export RuntimeState from iac for convenience.
pub use tokeira_iac::RuntimeState;
