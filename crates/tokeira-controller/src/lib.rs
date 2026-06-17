//! Active-active placement controller primitives.

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
// Service methods return tonic `Status` errors; boxing every `Result` to satisfy
// `result_large_err` is churn without a measured win.
#![allow(clippy::result_large_err)]

pub mod config;
pub mod connect_service;
pub mod drain;
pub mod generation;
pub mod membership;
pub mod metrics;
pub mod placement;
pub mod service;

pub use config::*;
pub use connect_service::ConnectPlacementController;
pub use drain::*;
pub use generation::*;
pub use membership::*;
pub use placement::*;
pub use service::*;
