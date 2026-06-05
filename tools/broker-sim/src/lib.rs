//! Delivery-broker simulator library surface.
//!
//! The broker model and its pieces are exposed as a library so integration
//! tests (and the binary) can drive them. The binary (`main.rs`) is a thin CLI
//! shell over `run` below; the heavy lifting lives in these modules.
//!
//! See `.kiro/specs/delivery-broker-simulator/` for the requirements, design,
//! and the S1–S7 / L1–L4 invariants this model is built to falsify.

pub mod bug;
pub mod events;
pub mod exhaustive;
pub mod invariants;
pub mod model;
pub mod model_machine;
pub mod workload;
