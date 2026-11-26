//! Independent, pure decisions derived from Temporal server v1.31.0.
//!
//! Expected behaviour must not inherit the implementation it evaluates. This crate
//! therefore cannot depend on Tokeira's kernel, runtime, edge, storage, or
//! compatibility policy, even transitively. Production crates may use it only as a
//! development dependency. The dependency test checks both directions.
//!
//! Inputs retain upstream policy and failure shapes; time is explicit nanoseconds
//! since the Unix epoch. No clock, storage, or service topology is modelled.
//! Ordinary rules await corpus calibration. Exact-time rules are source-only;
//! absent or provisional coverage cannot establish calibrated agreement.
#![deny(missing_docs)]

pub mod activity;
pub mod catalog;
