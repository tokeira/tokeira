//! Generated protobuf and gRPC bindings for Tokeira.
//!
//! This crate deliberately separates:
//!
//! - `public`: Temporal-compatible API packages and service constants
//! - `internal`: Tokeira-only runtime/admin packages
//! - `conversions`: small, explicit helpers between wire structs and `tokeira-types`
//!
//! Keeping these concerns together prevents the rest of the workspace from depending directly on
//! generated protobuf details.

pub mod internal;
pub mod public;
pub mod conversions;

pub use internal::{admin, runtime};
pub use public::{common, enums, operatorservice, workflowservice};
