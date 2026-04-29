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

    pub fn has_active_poller(
        &self,
        queue: &QueueKey,
        worker_identity: &WorkerIdentity,
    ) -> bool {
        self.state
            .pollers
            .read()
            .expect("poller registry poisoned")
            .get(queue)
            .is_some_and(|entries| {
                entries
                    .values()
                    .any(|poller| &poller.identity == worker_identity)
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_types::{NamespaceId, TaskKind, TaskQueueName};

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn arb_queue_key() -> impl Strategy<Value = QueueKey> {
        arb_small_string().prop_map(|task_queue| QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName(task_queue),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        })
    }

    proptest! {
        #[test]
        fn property_has_active_poller_matches_live_registration(
            queue in arb_queue_key(),
            worker_identity in arb_small_string(),
            other_identity in arb_small_string(),
        ) {
            let registry = PollerRegistry::default();
            let worker_identity = WorkerIdentity(worker_identity);
            let other_identity = WorkerIdentity(other_identity);

            prop_assert!(!registry.has_active_poller(&queue, &worker_identity));

            let guard = registry.register(queue.clone(), worker_identity.clone());
            prop_assert!(registry.has_active_poller(&queue, &worker_identity));

            if worker_identity != other_identity {
                prop_assert!(!registry.has_active_poller(&queue, &other_identity));
            }

            drop(guard);
            prop_assert!(!registry.has_active_poller(&queue, &worker_identity));
        }
    }
}
