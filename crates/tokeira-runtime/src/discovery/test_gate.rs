//! Test-only pass boundary: hold the next pass while a commit loses publication.
//!
//! Holding begins only after an empty pass completes, leaving a registered poll
//! and the normal next-pass deadline intact. Production has no gate or branch.

use std::sync::atomic::{AtomicU8, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(crate) struct TestPassGate {
    state: AtomicU8,
    held: Notify,
}

impl TestPassGate {
    pub(crate) fn arm(&self) {
        self.state.store(1, Ordering::SeqCst);
    }

    pub(crate) fn completed(&self) {
        if self
            .state
            .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.held.notify_one();
        }
    }

    pub(crate) fn is_held(&self) -> bool {
        self.state.load(Ordering::SeqCst) == 2
    }

    pub(crate) async fn wait(&self) {
        self.held.notified().await;
    }

    pub(crate) fn release(&self) {
        self.state.store(0, Ordering::SeqCst);
    }
}
