//! Cooperative shutdown primitive for observability-owned background tasks.
//!
//! The shared observability layer should not own process termination. Binaries
//! decide when they are shutting down and call `cancel`; background tasks then
//! exit their loops and return through their normal join handles.

use tokio_util::sync::CancellationToken;

/// Cooperative shutdown token for observability background tasks.
#[derive(Clone, Debug)]
pub struct ObservabilityShutdown {
    token: CancellationToken,
}

impl ObservabilityShutdown {
    /// Construct a new uncancelled shutdown token.
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Request cooperative shutdown.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Wait until shutdown has been requested.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// Clone the underlying token for tasks that need direct integration with
    /// APIs expecting `CancellationToken`.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Default for ObservabilityShutdown {
    fn default() -> Self {
        Self::new()
    }
}
