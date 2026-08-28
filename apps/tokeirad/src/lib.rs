//! Compatibility re-exports for the `tokeirad` binary and integration harnesses.
//!
//! Service construction and lifecycle ownership live in [`tokeira_engine`]. The
//! application package retains this library target so existing test harnesses can
//! migrate without changing imports in lockstep with the extraction.
//!
//! The `conformance` feature adds the `conformance` harness-assembly module:
//! the app layer is the only place the never-published conformance override
//! machinery is linked and installed into the engine's inert harness seams.

pub use tokeira_engine::*;

#[cfg(feature = "conformance")]
pub mod conformance;
#[cfg(feature = "conformance")]
mod conformance_grpc_authenticator;
#[cfg(feature = "conformance")]
mod conformance_nexus_authorizer;
