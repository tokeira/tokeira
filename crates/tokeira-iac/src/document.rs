//! Typed state documents.
//!
//! These implement [`tokeira_state::Validate`] so they can be persisted via
//! [`StateStore<T>`](tokeira_state::StateStore). The store treats them as
//! opaque JSON; this crate interprets the contents.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ResourceId, ResourceState};

/// Persisted infrastructure state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraState {
    pub version: u32,
    pub resources: BTreeMap<ResourceId, ResourceState>,
    pub outputs: BTreeMap<String, String>,
    pub last_applied: String,
}

impl Default for InfraState {
    fn default() -> Self {
        Self {
            version: 1,
            resources: BTreeMap::new(),
            outputs: BTreeMap::new(),
            last_applied: String::new(),
        }
    }
}

impl tokeira_state::Validate for InfraState {
    fn validate(&self) -> Result<(), tokeira_state::StateError> {
        for (id, rs) in &self.resources {
            if id.0.trim().is_empty() {
                return Err(tokeira_state::StateError::Corrupted(
                    "resource state contains an empty resource id".into(),
                ));
            }
            if rs.physical_id.trim().is_empty() {
                return Err(tokeira_state::StateError::Corrupted(format!(
                    "resource '{}' has an empty physical_id",
                    id.0
                )));
            }
        }
        Ok(())
    }
}

/// Persisted runtime state for images and services.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeState {
    pub version: u32,
    pub images: BTreeMap<String, ImageState>,
    pub services: BTreeMap<String, ServiceState>,
    pub last_applied: String,
}

impl tokeira_state::Validate for RuntimeState {
    fn validate(&self) -> Result<(), tokeira_state::StateError> {
        for (name, image) in &self.images {
            if name.trim().is_empty() {
                return Err(tokeira_state::StateError::Corrupted(
                    "image state contains an empty name".into(),
                ));
            }
            if image.resolved_ref.trim().is_empty() {
                return Err(tokeira_state::StateError::Corrupted(format!(
                    "image '{name}' has an empty resolved_ref"
                )));
            }
        }
        Ok(())
    }
}

impl RuntimeState {
    /// Look up a tracked image by name.
    pub fn image(&self, name: &str) -> Option<&ImageState> {
        self.images.get(name)
    }
}

/// Persisted state for a managed image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageState {
    pub name: String,
    pub resolved_ref: String,
    pub digest: Option<String>,
    pub published_at: String,
    pub source: ImageSource,
}

/// How an image was produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImageSource {
    Built,
    Mirrored { upstream_ref: String },
    PullThrough { upstream_ref: String },
}

/// Persisted state for a managed service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceState {
    pub name: String,
    pub module: String,
    pub manifest_count: usize,
    pub desired_hash: String,
    pub last_applied: String,
}

/// Typed state store for infrastructure state.
pub type InfraStateStore = tokeira_state::S3StateStore<InfraState>;

/// Typed state store for runtime state.
pub type RuntimeStateStore = tokeira_state::S3StateStore<RuntimeState>;
