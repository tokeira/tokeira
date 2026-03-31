use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

use crate::errors::{EdgeError, EdgeResult};

/// Admission control for long-poll requests.
///
/// Long polls are primarily an edge concern: they tie up sockets, memory, and
/// scheduler slots. They should *not* cause the runtime to pin durable state or
/// the storage layer to pin DSQL sessions.
///
/// This gate therefore limits concurrency before the request reaches the broker/runtime.
#[derive(Clone, Debug)]
pub struct LongPollConfig {
    pub max_concurrent: usize,
    pub acquire_timeout: Duration,
}

impl Default for LongPollConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10_000,
            acquire_timeout: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LongPollGate {
    sem: Arc<Semaphore>,
    config: LongPollConfig,
}

#[derive(Debug)]
pub struct LongPollPermit {
    _permit: OwnedSemaphorePermit,
}

impl LongPollGate {
    pub fn new(config: LongPollConfig) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.sem.available_permits()
    }

    pub async fn acquire(&self) -> EdgeResult<LongPollPermit> {
        let sem = self.sem.clone();

        let permit = timeout(self.config.acquire_timeout, sem.acquire_owned())
            .await
            .map_err(|_| EdgeError::LongPollAdmissionTimeout)?
            .map_err(|_| EdgeError::TooManyLongPolls)?;

        Ok(LongPollPermit { _permit: permit })
    }
}
