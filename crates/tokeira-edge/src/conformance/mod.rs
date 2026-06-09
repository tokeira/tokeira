//! Tier-2 functional-conformance data models owned by `tokeira-edge`.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC
//! wire against a running `tokeirad`, then interprets the outcomes in a report joined
//! against the compatibility matrix (see
//! `.kiro/specs/temporal-functional-conformance`). This module groups the serializable
//! shapes that report consumes; behaviour (the recorder, the report join, the gates)
//! lives in later tasks, not here.
//!
//! - [`record`] — the wire-coverage record: `(wire_method, status_code, count)` rows the
//!   recorder emits over a run, each later resolved through
//!   `tokeira_compatibility::coverage::resolve`.
//! - [`recorder`] — the in-memory recorder that aggregates those `(wire_method,
//!   status_code)` observations live over a run and materializes them into a [`record`].
//! - [`layer`] — the tower `Layer`/`Service` that captures `(wire_method, status_code)`
//!   at the gRPC transport boundary and feeds the [`recorder`]; mounted on the server by
//!   `tokeirad` only under the conformance flag.
//! - [`ledger`] — the per-test ledger: each test's classified outcome and its
//!   category-keyed evidence.
//! - [`report`] — the wire-coverage report: joins the [`record`] observations against
//!   the compatibility matrix via `tokeira_compatibility::coverage::resolve`, marking
//!   each RPC `agrees` / `contradicts` / `uncovered` / `unknown-to-matrix` (task 9.1).
//! - [`test_report`] — the per-test classified report: joins the run's per-test
//!   outcomes (the task-8.2 distiller document) against the authored [`ledger`],
//!   producing one classified row per test for the report gates (task 9.2).
//! - [`pin`] — the pin-consistency gate: the fail-fast check that the fork's conformance
//!   branch is pinned at the Temporal tag matching `TEMPORAL_SERVER_COMPAT` and is not the
//!   fork's `main`, so the corpus can never be run against a release newer than the claim.

pub mod layer;
pub mod ledger;
pub mod pin;
pub mod record;
pub mod recorder;
pub mod report;
pub mod test_report;

pub use layer::*;
pub use ledger::*;
pub use pin::*;
pub use record::*;
pub use recorder::*;
pub use report::*;
pub use test_report::*;
