//! Generated Buffa/connect-rust bindings for the Tokeira compatibility API.
//!
//! The SDK-facing Temporal API remains in `tokeira-proto`. This crate owns
//! Tokeira-specific compatibility metadata so operators can query richer build,
//! feature, and SDK state without extending upstream Temporal protos.

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
// Generated connect-rpc service markers/servers carry no Debug derive.
#![allow(missing_debug_implementations)]

pub mod connect {
    connectrpc::include_generated!("_connectrpc_compatibility.rs");
}
