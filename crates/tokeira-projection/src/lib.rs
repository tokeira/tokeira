//! Projection worker and small visibility-oriented sink abstractions.
//!
//! Projection is intentionally separated from the correctness path. That means a
//! lagging projector is a quality problem, not a correctness failure.

pub mod sink;
pub mod visibility;
pub mod worker;

pub use sink::*;
pub use visibility::*;
pub use worker::*;
