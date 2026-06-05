//! Placement/membership simulator library surface.
//!
//! The placement model and its pieces are exposed as a library so integration
//! tests (and the binary in `main.rs`) can drive them. The simulator falsifies
//! the placement design thesis (architecture doc 035) — *DSQL owns truth;
//! runtime ownership is valid only by the current lease epoch; queue-home is
//! advisory; execution-home is the correctness boundary* — by re-modeling the
//! design as a pure deterministic state machine on the shared
//! [`sim_engine`](../engine) substrate, importing no server crate.
//!
//! Module map (mirrors the broker simulator):
//!
//! - [`model`] — domain identifiers, the authoritative `Dsql`, the advisory
//!   `Edge`/`Runtime` belief, config, and the home-assignment hashes.
//! - [`events`] — the event taxonomy (control plane, lease lifecycle, the
//!   deliberately two-phase data plane, edge repair, faults).
//! - [`model_machine`] — `PlacementModel`, the `StressModel` implementation.
//! - [`invariants`] — the I1–I6 safety invariants.
//! - [`workload`] — the reproducible client + fault schedule.
//! - [`exhaustive`] — the bounded-exhaustive safety-kernel checker.
//! - [`bug`] — the injectable `buggy-start-routing` defect.

pub mod bug;
pub mod events;
pub mod exhaustive;
pub mod invariants;
pub mod model;
pub mod model_machine;
pub mod workload;
