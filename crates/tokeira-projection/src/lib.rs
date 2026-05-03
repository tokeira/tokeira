//! Projection worker and small visibility-oriented sink abstractions.
//!
//! Projection is intentionally separated from the correctness path. That means a
//! lagging projector is a quality problem, not a correctness failure.

#[cfg(feature = "dsql")]
pub mod dsql_store;
pub mod filter;
pub mod memory;
pub mod metrics;
pub mod query_service;
pub mod rollup;
pub mod sink;
pub mod store;
pub mod types;
pub mod visibility_api;
pub mod visibility_sink;
pub mod worker;

#[cfg(feature = "dsql")]
pub use dsql_store::*;
pub use filter::*;
pub use memory::*;
pub use metrics::*;
pub use query_service::*;
pub use rollup::*;
pub use sink::*;
pub use store::*;
pub use types::*;
pub use visibility_api::*;
pub use visibility_sink::*;
pub use worker::*;
