//! Pure workflow transition engine.
//!
//! The kernel owns *semantic* correctness. It does not know where bytes are
//! stored, how workers poll, or how a runtime cluster is scaled. That is very
//! deliberate: if the kernel starts learning about transport or storage, every
//! later optimization becomes harder.
//!
//! A useful way to read this crate is:
//!
//! - `state` describes the durable input to a transition,
//! - `command` describes the thing that happened,
//! - `kernel` derives the authoritative new state,
//! - `transition` describes the side effects that downstream layers must honor.

// The command/event/transition enums carry one large authoritative variant. Boxing
// it to satisfy `large_enum_variant` is a perf decision for the transition hot path,
// not a style fix, so the advisory lint is allowed rather than acted on here.
#![allow(clippy::large_enum_variant)]

pub mod command;
pub mod event;
pub mod kernel;
pub mod state;
pub mod transition;

pub use command::*;
pub use event::*;
pub use kernel::*;
pub use state::*;
pub use transition::*;
