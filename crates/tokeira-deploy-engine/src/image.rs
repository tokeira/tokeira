//! Image lifecycle abstractions.
//!
//! An [`Image`] represents a named deployable artifact with a source type
//! and a desired reference. [`ImageContext`] carries persisted state and
//! typed extensions for resolution.

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use serde::{Deserialize, Serialize};

use crate::RuntimeError;

/// How an image is produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageSourceType {
    /// Built from source via a build pipeline.
    Build,
    /// Mirrored from an upstream registry.
    Mirror,
    /// Pulled from a registry as-is.
    Registry,
}

/// Resolved desired image reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredImageRef {
    /// Target repository name without registry host prefix.
    pub repository: String,
    /// Tag to resolve against.
    pub tag: String,
    /// Upstream source reference for mirrored images.
    pub upstream_ref: Option<String>,
}

/// A dotted config key that should receive a published or mirrored image ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritebackTarget {
    pub field: &'static str,
}

/// Context passed to [`Image::desired_ref`] for image resolution.
///
/// Extensions provide access to configuration and platform handles without
/// coupling this crate to specific implementations.
pub struct ImageContext {
    pub state: tokeira_iac::RuntimeState,
    extensions: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ImageContext {
    pub fn new(state: tokeira_iac::RuntimeState) -> Self {
        Self {
            state,
            extensions: HashMap::new(),
        }
    }

    /// Retrieve a typed extension by type.
    pub fn extension<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }

    /// Register a typed extension.
    pub fn set_extension<T: 'static + Send + Sync>(&mut self, value: T) {
        self.extensions.insert(TypeId::of::<T>(), Box::new(value));
    }
}

impl Default for ImageContext {
    fn default() -> Self {
        Self::new(tokeira_iac::RuntimeState::default())
    }
}

/// A named deployable artifact.
///
/// Implement this trait for every image a deployment needs to resolve before
/// applying services. Registry-only deployments can return the configured
/// reference directly. Build or mirror deployments can use [`ImageContext`]
/// extensions to access build systems, registries, credentials, or previously
/// persisted image state.
pub trait Image: Debug + Send + Sync {
    /// Stable image name.
    fn name(&self) -> &str;

    /// How this image is produced.
    fn source_type(&self) -> ImageSourceType;

    /// Compute the desired image reference given current state and context.
    fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError>;

    /// Return config fields populated when the image is published or mirrored.
    fn writeback_targets(&self, _ctx: &ImageContext) -> Vec<WritebackTarget> {
        Vec::new()
    }
}

/// Validate image registry uniqueness before callers consume resolved refs.
pub fn validate_registry(
    images: &[Box<dyn Image>],
    ctx: &ImageContext,
) -> Result<(), RuntimeError> {
    let mut names = HashSet::with_capacity(images.len());
    let mut repositories = HashSet::with_capacity(images.len());

    for image in images {
        let name = image.name();
        if !names.insert(name.to_string()) {
            return Err(RuntimeError::Image(format!(
                "image registry validation failed: duplicate name = {name}"
            )));
        }

        let desired = image.desired_ref(ctx)?;
        if !repositories.insert(desired.repository.clone()) {
            return Err(RuntimeError::Image(format!(
                "image registry validation failed: duplicate repository = {}",
                desired.repository
            )));
        }
    }

    Ok(())
}
