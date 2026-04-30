//! Storage interfaces and a simple in-memory development store.
//!
//! This crate intentionally does **not** contain a real DSQL implementation yet.
//! The goal is to make the contracts explicit before the production storage
//! layer is written.
//!
//! The in-memory store is useful for tests, examples, and Codex-driven feature
//! work. It now persists authoritative history, request-dedupe state, and a
//! transition audit log so semantic review can happen before DSQL lands.
//! It is *not* a concurrency or correctness reference for a real cluster.

pub mod api;
#[cfg(feature = "dsql")]
pub mod dsql;
pub mod memory;
pub mod metrics;

pub use api::*;
pub use memory::*;
pub use metrics::*;
