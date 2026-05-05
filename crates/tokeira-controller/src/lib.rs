//! Active-active placement controller primitives.

pub mod config;
pub mod drain;
pub mod generation;
pub mod membership;
pub mod placement;
pub mod service;

pub use config::*;
pub use drain::*;
pub use generation::*;
pub use membership::*;
pub use placement::*;
pub use service::*;
