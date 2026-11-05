//! The wall clock Tokeira reads the current time from.
//!
//! Every decision about whether something is due — a timer, a timeout, a
//! retry, a lease, a token's freshness — compares a deadline that an earlier
//! step stamped against the current time. Both sides must read the same clock,
//! or a deadline stamped by one is judged by the other. Code that holds a
//! [`Clock`] reads it for both.
//!
//! Production uses [`SystemClock`], the host's UTC clock, everywhere. A harness
//! that runs nodes in simulated time, such as the turmoil simulator in
//! `tools/simulation/turmoil`, supplies a clock that follows the simulation
//! instead.
//!
//! This crate is where time enters the engine. Storage, the runtime, the edge
//! and the engine depend on it. The kernel and `tokeira-types` do not: the
//! kernel takes time from the commands it is given, which keeps it
//! deterministic, and leaving this crate out of its dependencies means it has
//! no clock to read.
//!
//! Times are [`time::OffsetDateTime`], the type every stored deadline and
//! every kernel command already carries.
//!
//! # Example
//!
//! A test that needs time to stand still can supply its own clock:
//!
//! ```
//! use std::sync::Arc;
//!
//! use time::{OffsetDateTime, macros::datetime};
//! use tokeira_clock::Clock;
//!
//! #[derive(Debug)]
//! struct Frozen(OffsetDateTime);
//!
//! impl Clock for Frozen {
//!     fn now(&self) -> OffsetDateTime {
//!         self.0
//!     }
//! }
//!
//! let clock: Arc<dyn Clock> = Arc::new(Frozen(datetime!(2026-01-01 00:00 UTC)));
//! assert_eq!(clock.now(), datetime!(2026-01-01 00:00 UTC));
//! ```

use std::{fmt, sync::Arc};

use time::OffsetDateTime;

/// A source of the current wall-clock time.
///
/// Implementations are shared across tasks and threads behind an
/// `Arc<dyn Clock>`. Nothing here promises that time moves forward: a
/// deadline check must hold however far apart two readings are.
pub trait Clock: Send + Sync + fmt::Debug {
    /// The current time, in UTC.
    fn now(&self) -> OffsetDateTime;
}

/// The host's UTC clock, which production uses everywhere.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// A shared [`SystemClock`], for constructors that default to it.
pub fn system_clock() -> Arc<dyn Clock> {
    Arc::new(SystemClock)
}
