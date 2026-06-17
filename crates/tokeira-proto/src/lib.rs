//! Generated protobuf and gRPC bindings for Tokeira.
//!
//! This crate deliberately separates:
//!
//! - `public`: Temporal-compatible API packages and service constants
//! - `internal`: Tokeira-only runtime/admin packages (tonic/prost — legacy)
//! - `connect`: Tokeira-internal controller surface (buffa + connect-rust)
//! - `conversions`: small, explicit helpers between wire structs and `tokeira-types`
//!
//! The `connect` module is the preferred path for controller ↔ runtime/edge/autoscaler
//! communication. It provides zero-copy view types and the connect-rust service traits.
//! The `internal` module retains the tonic/prost output for code that hasn't migrated.

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
// The tonic/prost-generated bindings (compiled from the upstream Temporal `.proto`s)
// trip style lints that are not actionable on generated code: large message/oneof
// enums, and doc-list formatting carried verbatim from the upstream proto comments.
#![allow(
    clippy::large_enum_variant,
    clippy::doc_lazy_continuation,
    clippy::doc_overindented_list_items
)]

pub mod conversions;
pub mod internal;
pub mod public;

/// Connect-rust generated types for the internal controller surface.
///
/// Provides buffa message types (owned + zero-copy views), connect-rust
/// service traits, and generated clients. Speaks Connect, gRPC, and
/// gRPC-Web on the same handlers.
pub mod connect {
    connectrpc::include_generated!("_connectrpc_controller.rs");
}

pub use internal::{admin, controller, runtime};
pub use public::{
    common, enums, failure, history, operatorservice, taskqueue, workflow, workflowservice,
};
