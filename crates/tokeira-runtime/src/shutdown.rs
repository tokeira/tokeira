//! Cancellation and join ownership for runtime-scoped background work.
//!
//! A cancellation token by itself is not a shutdown boundary: it says work
//! should stop but gives the embedding host no proof that spans and guards have
//! actually finished. [`RuntimeShutdownHandle`] pairs cancellation with a
//! [`TaskTracker`], allowing explicit shutdown to
//! establish a real happens-before boundary before the host flushes telemetry.

use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Cloneable cancellation and join boundary for runtime-owned tasks.
#[derive(Clone, Debug)]
pub struct RuntimeShutdownHandle {
    inner: Arc<RuntimeShutdownInner>,
}

#[derive(Debug)]
struct RuntimeShutdownInner {
    cancel: CancellationToken,
    tracker: TaskTracker,
    component_cancellations: Mutex<Vec<CancellationToken>>,
}

impl RuntimeShutdownHandle {
    /// Create an open tracker that accepts startup tasks until shutdown begins.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RuntimeShutdownInner {
                cancel: CancellationToken::new(),
                tracker: TaskTracker::new(),
                component_cancellations: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Master cancellation token for loops owned directly by the caller.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// Spawn one task whose completion is owned by this shutdown boundary.
    #[must_use]
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner.tracker.spawn(future)
    }

    /// Adopt a task that an existing component constructor already spawned.
    ///
    /// The wrapper keeps the component join handle inside the tracker, so the
    /// shutdown boundary does not report completion merely because the caller
    /// dropped its original handle.
    #[must_use]
    pub fn track_join<T>(
        &self,
        task: JoinHandle<T>,
    ) -> JoinHandle<Result<T, tokio::task::JoinError>>
    where
        T: Send + 'static,
    {
        self.spawn(task)
    }

    /// Register a pre-existing component token for synchronous cancellation.
    pub(crate) fn register_cancellation(&self, token: CancellationToken) {
        self.inner
            .component_cancellations
            .lock()
            .expect("runtime cancellation registry poisoned")
            .push(token);
    }

    /// Close task registration after startup without cancelling existing work.
    pub fn close_registration(&self) {
        self.inner.tracker.close();
    }

    /// Synchronously stop accepting tracked work and cancel every registered loop.
    pub fn begin_shutdown(&self) {
        self.inner.tracker.close();
        self.inner.cancel.cancel();
        let cancellations = self
            .inner
            .component_cancellations
            .lock()
            .expect("runtime cancellation registry poisoned");
        for cancellation in cancellations.iter() {
            cancellation.cancel();
        }
    }

    /// Await tracked completion within one caller-owned absolute deadline.
    pub async fn wait(&self, deadline: Instant) -> Result<(), RuntimeShutdownError> {
        let now = Instant::now();
        if now >= deadline {
            return Err(RuntimeShutdownError::DeadlineExceeded {
                remaining_tasks: self.inner.tracker.len(),
            });
        }
        tokio::time::timeout(
            deadline.saturating_duration_since(now),
            self.inner.tracker.wait(),
        )
        .await
        .map_err(|_| RuntimeShutdownError::DeadlineExceeded {
            remaining_tasks: self.inner.tracker.len(),
        })
    }

    /// Number of tracked tasks that have not completed.
    pub fn remaining_tasks(&self) -> usize {
        self.inner.tracker.len()
    }
}

impl Default for RuntimeShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Failure to join runtime-owned work within the host's shutdown budget.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeShutdownError {
    /// Tracked work did not finish before the absolute deadline.
    #[error("timed out joining {remaining_tasks} runtime tasks")]
    DeadlineExceeded {
        /// Number of tasks still registered when the deadline elapsed.
        remaining_tasks: usize,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[tokio::test]
    async fn cancellation_then_wait_is_a_real_completion_boundary() {
        let shutdown = RuntimeShutdownHandle::new();
        let cancel = shutdown.cancellation_token();
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = Arc::clone(&finished);
        let _task = shutdown.spawn(async move {
            cancel.cancelled().await;
            task_finished.store(true, Ordering::Release);
        });

        shutdown.begin_shutdown();
        shutdown
            .wait(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .expect("tracked task joins");
        assert!(finished.load(Ordering::Acquire));
        assert_eq!(shutdown.remaining_tasks(), 0);
    }

    #[tokio::test]
    async fn deadline_reports_unfinished_work_without_blocking() {
        let shutdown = RuntimeShutdownHandle::new();
        let (_sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let _task = shutdown.spawn(async move {
            let _ = receiver.await;
        });
        shutdown.begin_shutdown();
        assert_eq!(
            shutdown.wait(Instant::now()).await,
            Err(RuntimeShutdownError::DeadlineExceeded { remaining_tasks: 1 })
        );
    }
}
