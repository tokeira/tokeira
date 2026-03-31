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
