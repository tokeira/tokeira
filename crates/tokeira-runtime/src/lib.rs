//! Lane-based runtime orchestration.
//!
//! The runtime is where pure semantics meet scheduling and delivery. It should
//! stay much thinner than a full server process: its job is to serialize commands
//! for a run, persist transitions, and publish derived effects.
//!
//! The runtime deliberately does not assume that all runs are hot, that all work
//! is poll-driven, or that a lane owns a run forever.

pub mod broker;
pub mod lane;
pub mod runtime;

pub use broker::*;
pub use lane::*;
pub use runtime::*;
