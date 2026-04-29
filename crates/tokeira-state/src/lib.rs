//! Deployment state persistence.
//!
//! This crate provides the state contract used by the orchestration framework.
//! State documents are ordinary serde values that also implement [`Validate`].
//! The store validates documents after loading and before saving so callers do
//! not persist structurally invalid infrastructure or runtime state.
//!
//! There are two store implementations:
//!
//! - [`CasStore<T>`] is a single-document compare-and-swap store that
//!   delegates I/O to a [`StateBackend`] trait object. The orchestrator uses
//!   this with [`LocalBackend`] or any future backend adapter. It writes the
//!   full serialized document directly — no manifest/snapshot indirection.
//!
//! - [`S3StateStore<T>`] is the S3-native implementation with a manifest
//!   pointer, immutable snapshots, lease locks, checksum verification, and
//!   ETag-based CAS. Cloud deployments that want the full S3 state model
//!   should use this type directly.
//!
//! A custom deployment normally selects a [`StateBackend`] for each state
//! document it owns. Backends must treat manifest writes as compare-and-swap
//! operations and return [`StateError::Conflict`] when the caller's expected
//! version is stale.

mod backend;
mod cas_store;
mod error;
mod local;
mod manifest;
mod s3_store;

/// Validation hook called after deserializing a document.
///
/// Implementors check structural integrity without interpreting
/// domain-specific semantics.
pub trait Validate {
    fn validate(&self) -> Result<(), StateError>;
}

pub use backend::StateBackend;
pub use cas_store::CasStore;
pub use error::StateError;
pub use local::LocalBackend;
pub use manifest::{
    LockGuard, ManifestState, SnapshotRef, StateLeaseLock, StateManifest,
};
pub use s3_store::S3StateStore;

/// Backward-compatible alias. Prefer [`CasStore`] in new code.
pub type StateStore<T> = CasStore<T>;
