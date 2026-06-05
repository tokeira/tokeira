//! Reusable discrete-event simulation harness for the Tokeira simulator family.
//!
//! This crate generalises the mechanics `tools/placement-sim` invented inline so
//! that each service simulator — the delivery-broker simulator first, the
//! admission-control and connection-management simulators later — can be built
//! on a shared, uniformly-disciplined substrate rather than re-deriving an event
//! loop, an RNG, and an invariant checker each time.
//!
//! The harness deliberately holds **no domain logic**: it knows nothing about
//! brokers, placement, admission, or connections. A consuming simulator supplies
//! its own model state, event type, invariants, faults, and signal names, and
//! drives them through these generic pieces:
//!
//! - [`rng::Rng`] — the deterministic `xorshift64` generator (all randomness).
//! - [`event::EventQueue`] / [`event::SimCtx`] — the `(time, seq)`-ordered event
//!   queue and the mutation context handed to a model during `handle`.
//! - [`invariant::InvariantRegistry`] — named safety/liveness invariants checked
//!   after every event (safety) or at quiescence (liveness).
//! - [`fault::FaultInjector`] — model-defined adversarial faults, enable/disable
//!   config, and injection counts.
//! - [`enumerate::run_bounded_exhaustive`] — bounded-exhaustive interleaving
//!   checker over a tiny [`enumerate::ExhaustiveModel`].
//! - [`stress::run_seed`] — the seeded stress runner over a [`stress::StressModel`].
//! - [`report::Report`] — cross-seed aggregation and `placement-sim`-style output.
//! - [`cli`] — shared CLI flag vocabulary plus model-specific extensions.
//!
//! Determinism is the load-bearing property: a `(seed, model, fault-config)`
//! triple must reproduce an identical event sequence and result, so failures are
//! always replayable. Nothing here reads a wall clock or performs real I/O.

pub mod cli;
pub mod enumerate;
pub mod event;
pub mod fault;
pub mod invariant;
pub mod report;
pub mod rng;
pub mod stress;

pub use cli::{CliArgs, CliSpec};
pub use enumerate::{run_bounded_exhaustive, Counterexample, EnumReport, ExhaustiveModel};
pub use event::{EventQueue, Scheduled, SimCtx};
pub use fault::{Fault, FaultConfig, FaultInjector};
pub use invariant::{Invariant, InvariantClass, InvariantOutcome, InvariantRegistry, Violation};
pub use report::{Report, SeedReport, SignalCounters};
pub use rng::Rng;
pub use stress::{run_seed, Failure, StressModel};
