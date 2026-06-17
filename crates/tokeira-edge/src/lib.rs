//! Public API compatibility shell for Tokeira.
//!
//! This crate should remain intentionally thin.
//! It admits and translates requests, but it should not implement durable workflow
//! semantics. If a change would alter workflow history ordering, retry semantics,
//! timer behavior, or task durability, that change almost certainly belongs in
//! `tokeira-kernel`, `tokeira-runtime`, or `tokeira-storage` instead.

// Advisory clippy lints accepted across this proto-translation crate:
// - `too_many_arguments`: request translators thread many wire fields by design.
// - `result_large_err`: fallible translators return tonic `Status`; boxing every
//   `Result` is churn without a measured win.
// - `type_complexity`: tower/tonic middleware and interceptor signatures are
//   inherently nested.
// - `needless_update`: `..Default::default()` on prost messages is deliberate
//   forward-compat — an upstream proto field addition cannot then break the build.
#![allow(
    clippy::too_many_arguments,
    clippy::result_large_err,
    clippy::type_complexity,
    clippy::needless_update
)]

pub mod batch_engine;
pub mod chasm_activity;
pub mod conformance;
pub mod errors;
pub mod grpc;
pub mod health_service;
pub mod history_wait;
pub mod http_proxy;
pub mod interceptors;
pub mod long_poll;
pub mod metrics;
pub mod namespace_cache;
pub mod operator_service;
pub mod pending_queries;
pub mod poller_registry;
pub mod request_id;
pub mod routing;
pub mod routing_cache;
pub mod translate;
pub mod workflow_service;

pub use batch_engine::*;
pub use conformance::*;
pub use errors::*;
pub use grpc::*;
pub use health_service::*;
pub use history_wait::*;
pub use http_proxy::*;
pub use interceptors::*;
pub use long_poll::*;
pub use metrics::*;
pub use namespace_cache::*;
pub use operator_service::*;
pub use pending_queries::*;
pub use poller_registry::*;
pub use request_id::*;
pub use routing::*;
pub use routing_cache::*;
pub use translate::*;
pub use workflow_service::*;
