//! Compatibility re-exports for the `tokeirad` binary and integration harnesses.
//!
//! Service construction and lifecycle ownership live in [`tokeira_engine`]. The
//! application package retains this library target so existing test harnesses can
//! migrate without changing imports in lockstep with the extraction.

pub use tokeira_engine::*;
