//! Generated Buffa/connect-rust bindings for the Tokeira conformance control API.
//!
//! Conformance-only: the control service (`tokeira-conformance-control`) uses
//! these bindings to receive Temporal `OverrideDynamicConfig` values delivered
//! to an out-of-process `tokeirad` (spec
//! `.kiro/specs/conformance-config-override/`). Never linked into a production
//! build — consumer crates depend on the conformance stack only under their
//! `conformance` Cargo feature.

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
// Generated connect-rpc service markers/servers carry no Debug derive.
#![allow(missing_debug_implementations)]

pub mod connect {
    connectrpc::include_generated!("_connectrpc_conformance.rs");
}
