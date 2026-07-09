//! Per-task-queue active-poller bookkeeping.
//!
//! `DescribeTaskQueue` reports which workers are currently polling a queue. That is
//! a transport-level observation — who holds a long poll open right now — not
//! authoritative state, so it lives at the edge and is rebuilt from live
//! connections rather than persisted. [`PollerRegistry::register`] records a poller
//! keyed by queue and returns a [`PollerGuard`] whose `Drop` deregisters it, so the
//! set tracks live polls without a separate reaper. It is intentionally ephemeral:
//! losing it on restart costs only a momentarily empty `DescribeTaskQueue`, never
//! correctness.

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use time::OffsetDateTime;
use tokeira_types::{QueueKey, WorkerIdentity};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivePoller {
    pub identity: WorkerIdentity,
    pub registered_at: OffsetDateTime,
}

#[derive(Debug, Default)]
struct PollerRegistryState {
    pollers: RwLock<HashMap<QueueKey, HashMap<u64, ActivePoller>>>,
    next_id: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub struct PollerRegistry {
    state: Arc<PollerRegistryState>,
}

impl PollerRegistry {
    pub fn register(&self, queue: QueueKey, identity: WorkerIdentity) -> PollerGuard {
        let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
        let poller = ActivePoller {
            identity,
            registered_at: OffsetDateTime::now_utc(),
        };

        self.state
            .pollers
            .write()
            .expect("poller registry poisoned")
            .entry(queue.clone())
            .or_default()
            .insert(id, poller);

        PollerGuard {
            state: self.state.clone(),
            queue,
            id,
        }
    }

    pub fn pollers(&self, queue: &QueueKey) -> Vec<ActivePoller> {
        self.state
            .pollers
            .read()
            .expect("poller registry poisoned")
            .get(queue)
            .map(|entries| entries.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub struct PollerGuard {
    state: Arc<PollerRegistryState>,
    queue: QueueKey,
    id: u64,
}

impl Drop for PollerGuard {
    fn drop(&mut self) {
        let mut pollers = self
            .state
            .pollers
            .write()
            .expect("poller registry poisoned");

        if let Some(entries) = pollers.get_mut(&self.queue) {
            entries.remove(&self.id);
            if entries.is_empty() {
                pollers.remove(&self.queue);
            }
        }
    }
}
