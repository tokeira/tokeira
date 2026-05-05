//! Runtime-local two-phase drain state.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

/// Runtime drain state reported to placement controllers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeDrainState {
    Active,
    Draining,
    SafeToTerminate,
}

/// Shared flag used by request admission and heartbeat construction.
#[derive(Debug, Default)]
pub struct RuntimeDrain {
    draining: AtomicBool,
    safe_to_terminate: AtomicBool,
}

impl RuntimeDrain {
    pub fn begin(&self) {
        self.draining.store(true, Ordering::Release);
        self.safe_to_terminate.store(false, Ordering::Release);
    }

    pub fn mark_safe_to_terminate(&self) {
        self.draining.store(true, Ordering::Release);
        self.safe_to_terminate.store(true, Ordering::Release);
    }

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

    pub fn state(&self) -> RuntimeDrainState {
        if self.safe_to_terminate.load(Ordering::Acquire) {
            RuntimeDrainState::SafeToTerminate
        } else if self.draining.load(Ordering::Acquire) {
            RuntimeDrainState::Draining
        } else {
            RuntimeDrainState::Active
        }
    }

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
