//! The placement model's event taxonomy.
//!
//! Every state change in the model is one of these events drained from the
//! engine's `(time, seq)`-ordered queue. They fall into a few groups:
//!
//! - **Control plane:** `ControllerObserve` (build + publish a snapshot) and
//!   `EdgeApplySnapshot` (edge accepts it after delivery latency).
//! - **Lease lifecycle:** `RuntimeAcquire` / `RuntimeRenew` / `RuntimeRelinquish`.
//! - **Data plane, deliberately multi-phase:** an `EdgeOp` resolves a route and
//!   schedules a `RuntimeHandle`; the runtime begins an OCC transaction and
//!   schedules a `CommitAttempt`; the fence is checked *there*, not at handle
//!   time. This temporal split is what makes the lease-change-mid-commit race
//!   observable (mirroring the broker model's reserve/commit split).
//! - **Edge repair:** `EdgeRepairAndRetry` → `EdgeRepairResolve` model a
//!   `RefreshBundle` RPC with latency, then re-issue the op.
//! - **Adversarial faults:** renewal suppression, crash/restart, and a two-phase
//!   drain (`DrainRoutingUpdate` then `DrainRelinquishPhase`) whose ordering
//!   invariant I6 protects.

use crate::model::{BundleId, ClientOp, ControllerId, Epoch, RoutingSnapshot, RuntimeId};

/// A model event. Wraps [`PlacementEventKind`]; the wrapper exists so the type
/// is a single named event for the engine's `StressModel::Event` and leaves room
/// for per-event metadata without disturbing the kind enum.
#[derive(Clone, Debug)]
pub struct PlacementEvent {
    /// What this event does.
    pub kind: PlacementEventKind,
}

impl PlacementEvent {
    /// Wrap a kind into an event.
    pub fn new(kind: PlacementEventKind) -> Self {
        Self { kind }
    }
}

/// The full event alphabet of the placement model.
#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum PlacementEventKind {
    /// A controller observes DSQL, publishes a snapshot (delivered later), and
    /// schedules acquisitions for any unowned bundle.
    ControllerObserve { controller: ControllerId },
    /// The edge accepts a delivered snapshot (monotone by generation).
    EdgeApplySnapshot { snapshot: RoutingSnapshot },
    /// A runtime attempts to acquire a bundle lease.
    RuntimeAcquire {
        runtime: RuntimeId,
        bundle: BundleId,
    },
    /// A runtime renews a lease it holds; reschedules itself.
    RuntimeRenew {
        runtime: RuntimeId,
        bundle: BundleId,
        epoch: Epoch,
    },
    /// A runtime relinquishes a lease (also the drain path's final step).
    RuntimeRelinquish {
        runtime: RuntimeId,
        bundle: BundleId,
        epoch: Epoch,
    },
    /// A client operation enters at the edge; routed by the advisory snapshot.
    EdgeOp { op: ClientOp, attempt: u8 },
    /// The chosen runtime handles the op: checks local ownership, then *begins*
    /// an OCC transaction and schedules the commit.
    RuntimeHandle {
        runtime: RuntimeId,
        op: ClientOp,
        bundle: BundleId,
        observed_epoch: Epoch,
        attempt: u8,
    },
    /// The delayed commit: the OCC fence is evaluated here, after the
    /// transaction's simulated duration, so a lease change in between is caught.
    CommitAttempt {
        runtime: RuntimeId,
        op: ClientOp,
        bundle: BundleId,
        observed_epoch: Epoch,
        attempt: u8,
        tx_id: u64,
    },
    /// Begin an edge route repair after a miss (models RefreshBundle latency).
    EdgeRepairAndRetry {
        op: ClientOp,
        bundle: BundleId,
        attempt: u8,
    },
    /// The repair resolves against live DSQL state and re-issues the op.
    EdgeRepairResolve {
        op: ClientOp,
        bundle: BundleId,
        attempt: u8,
    },
    /// Fault: suppress a runtime's renewals for a window (lease will lapse).
    DisableRenewals {
        runtime: RuntimeId,
        duration_ms: u64,
    },
    /// Re-enable a runtime's renewals.
    EnableRenewals { runtime: RuntimeId },
    /// Fault: crash a runtime; a fresh incarnation restarts after a delay.
    CrashRuntime {
        runtime: RuntimeId,
        restart_delay_ms: u64,
    },
    /// A crashed runtime's replacement incarnation comes up.
    RestartRuntime { old_runtime: RuntimeId },
    /// Begin a graceful drain: stop renewing, then update routing before
    /// relinquishing (the ordering I6 enforces).
    BeginDrain { runtime: RuntimeId },
    /// Drain phase 1: remove the draining runtime from the edge routing.
    DrainRoutingUpdate { runtime: RuntimeId },
    /// Drain phase 2: relinquish the drained bundles (only after phase 1).
    DrainRelinquishPhase { runtime: RuntimeId },
}
