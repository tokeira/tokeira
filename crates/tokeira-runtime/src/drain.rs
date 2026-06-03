//! Runtime-local two-phase drain state.
//!
//! Owns the in-memory flags that track whether this runtime node is draining and
//! whether it has reached a point where it is safe to terminate. The state is
//! purely local and advisory: it is reported to placement controllers via the
//! heartbeat and consulted by request admission, but it is not durable and holds
//! no correctness weight — durable ownership and the transition log decide
//! correctness, this only sequences a graceful shutdown.
//!
//! Two phases, monotonic in the safe-to-terminate direction:
//! `Active → Draining → SafeToTerminate`. `Draining` is entered when shutdown
//! begins (admission starts shedding new work); `SafeToTerminate` is reached only
//! once all owned work has wound down. The flags use atomics so admission and
//! heartbeat construction can read the state on hot paths without a lock.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

/// Runtime drain state reported to placement controllers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeDrainState {
    /// Normal operation; new work is admitted.
    Active,
    /// Shutdown requested; shedding new work while in-flight work winds down.
    Draining,
    /// All owned work has drained; the node can be terminated without loss.
    SafeToTerminate,
}

/// Shared flag used by request admission and heartbeat construction.
///
/// Cheap to read concurrently: both phases are atomics, so admission and
/// heartbeat paths observe the state without taking a lock.
#[derive(Debug, Default)]
pub struct RuntimeDrain {
    draining: AtomicBool,
    safe_to_terminate: AtomicBool,
}

impl RuntimeDrain {
    /// Enter the draining phase. Clears `safe_to_terminate` so a fresh drain never
    /// inherits a stale "safe" verdict from a prior cycle.
    pub fn begin(&self) {
        self.draining.store(true, Ordering::Release);
        self.safe_to_terminate.store(false, Ordering::Release);
    }

    /// Declare the node safe to terminate. Also asserts `draining` so the reported
    /// state is internally consistent even if `begin` was not called first.
    pub fn mark_safe_to_terminate(&self) {
        self.draining.store(true, Ordering::Release);
        self.safe_to_terminate.store(true, Ordering::Release);
    }

    /// Re-evaluate drain progress against the current work counts, promoting to
    /// safe-to-terminate only once draining is under way and no owned work remains
    /// (no owned bundles, no in-flight transitions, no pending WFT replies).
    /// Checking all three guards against terminating while any committed-but-unsent
    /// reply or mid-flight transition could still be lost.
    pub fn record_progress(
        &self,
        owned_bundle_count: usize,
        inflight_transition_count: usize,
        pending_wft_replies: usize,
    ) {
        if self.is_draining()
            && owned_bundle_count == 0
            && inflight_transition_count == 0
            && pending_wft_replies == 0
        {
            self.mark_safe_to_terminate();
        }
    }

    /// Current drain state, derived from the two flags.
    pub fn state(&self) -> RuntimeDrainState {
        if self.safe_to_terminate.load(Ordering::Acquire) {
            RuntimeDrainState::SafeToTerminate
        } else if self.draining.load(Ordering::Acquire) {
            RuntimeDrainState::Draining
        } else {
            RuntimeDrainState::Active
        }
    }

    /// Whether the node has begun draining (either `Draining` or
    /// `SafeToTerminate`).
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_only_reports_safe_when_all_work_is_drained() {
        let drain = RuntimeDrain::default();
        drain.begin();

        drain.record_progress(0, 1, 0);
        assert_eq!(drain.state(), RuntimeDrainState::Draining);

        drain.record_progress(0, 0, 0);
        assert_eq!(drain.state(), RuntimeDrainState::SafeToTerminate);
    }
}
