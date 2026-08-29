//! The Python `.tkdp` definition frontend, executed by Pydantic's Monty.
//!
//! Operators author typed dataclass configuration, `config()`, and
//! `deployment(cfg, cx)` with a deliberately restricted `match` statement —
//! which Monty does not implement — lowered ahead of execution into
//! Monty-supported Python. The transient program (synthesized facade +
//! lowered source + driver) is executed by unmodified Monty and returns one
//! completed structural result; every reported position translates back to
//! the operator's file through a byte-covering source map. A `.tkdp`
//! deployment differs from a `.tkd` deployment in exactly one recorded fact:
//! its definition format.
//!
//! Monty is pinned to an exact git revision (crates.io `0.0.19` predates
//! in-sandbox dataclasses); the capability probes in `tests/probes.rs` hold
//! the pin to every behaviour this crate assumes, so a bump that breaks an
//! assumption fails loudly. Two sanctioned CPython deviations, stated rather
//! than accidental: strict match exhaustion (a config that matches nothing is
//! a defect, not a no-op) and exact variant identity (`type(x) is C`, not
//! `isinstance` — config variants form a closed algebraic set).

pub mod convert;
pub(crate) mod diagnostics;
pub(crate) mod facade;
pub mod frontend;
pub mod lower;
pub mod preflight;
pub mod program;
pub mod runner;
pub mod source_map;
mod syntax;

pub use frontend::{TkdpFrontend, frontend};
pub use preflight::{preflight_part_syntax, preflight_syntax};
pub use syntax::{SyntaxFinding, SyntaxValidation, validate_syntax};
