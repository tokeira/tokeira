//! Storage interfaces and a simple in-memory development store.
//!
//! The crate is organized around semantic persistence traits first, with
//! backends underneath them. Runtime code depends on `RunRepository`,
//! `LeaseRepository`, and `ProjectionLog`, not on any physical table layout.
//!
//! The in-memory store is useful for tests, examples, and Codex-driven feature
//! work. It now persists authoritative history, request-dedupe state, and a
//! transition audit log so semantic review can happen before DSQL lands.
//! It is *not* a concurrency or correctness reference for a real cluster.
//!
//! The optional `dsql` module contains the production-oriented Aurora DSQL
//! foundation: migrations, validation, connection management, and a DSQL
//! `RunRepository` implementation. It is feature-gated so most workspace tests
//! can run without SQLx/DSQL dependencies.

// Persistence entry points (commit, connection setup) thread many parameters by
// design (`too_many_arguments`); folding them into parameter structs is churn
// without a clarity win. The persistence-op enums carry one large variant whose
// boxing is a deliberate decision, not a style fix (`large_enum_variant`).
#![allow(clippy::too_many_arguments, clippy::large_enum_variant)]

pub mod api;
#[cfg(test)]
mod bug_condition_exploration_tests;
pub mod chasm;
#[cfg(feature = "dsql")]
pub mod dsql;
pub mod memory;
pub mod metrics;
#[cfg(test)]
mod preservation_property_tests;

pub use api::*;
pub use chasm::*;
pub use memory::*;
pub use metrics::*;
