//! Worker registry tracking active worker versions per task queue.
//!
//! When a worker polls, the edge registers its deployment and build identifiers
//! here. The runtime consults the registry to resolve versioning metadata for
//! task routing, ensuring that workflow tasks are dispatched to workers running
//! compatible code.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use time::OffsetDateTime;
use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerIdentity};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerRegistrationKey {
    pub worker_identity: WorkerIdentity,
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerVersionMetadata {
    pub deployment: Option<DeploymentId>,
    pub build_id: Option<BuildId>,
    pub last_seen_at: Option<OffsetDateTime>,
}

#[derive(Debug)]
struct WorkerRegistration {
    metadata: WorkerVersionMetadata,
    generation: u64,
}

#[derive(Debug, Default)]
struct WorkerRegistryState {
    registrations: HashMap<WorkerRegistrationKey, WorkerRegistration>,
    next_generation: u64,
}

#[derive(Default, Clone, Debug)]
pub struct WorkerRegistry {
    inner: Arc<Mutex<WorkerRegistryState>>,
}

impl WorkerRegistry {
    pub fn register(&self, key: WorkerRegistrationKey, metadata: WorkerVersionMetadata) {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        let generation = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1);
        state.registrations.insert(
            key,
            WorkerRegistration {
                metadata,
                generation,
            },
        );
    }

    /// Record one poll and return a cancellation finalizer.
    ///
    /// A normally resolved poll calls [`WorkerRegistrationGuard::completed`] and
    /// leaves the timestamp available to the bounded recent-poller check. If the
    /// handler future is cancelled, dropping the guard removes the outstanding
    /// registration so deployment deletion does not treat a stopped poller as live.
    pub fn register_poll(
        &self,
        key: WorkerRegistrationKey,
        metadata: WorkerVersionMetadata,
    ) -> WorkerRegistrationGuard {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        let generation = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1);
        state.registrations.insert(
            key.clone(),
            WorkerRegistration {
                metadata,
                generation,
            },
        );
        WorkerRegistrationGuard {
            registry: self.clone(),
            key: Some(key),
            generation,
        }
    }

    fn remove_if_current(&self, key: &WorkerRegistrationKey, generation: u64) {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        // Workflow and activity long polls from one SDK worker share this key. A
        // cancelled older request must not erase an observation installed by a
        // newer request after it (`poller_history.go @ v1.31.0`).
        if state
            .registrations
            .get(key)
            .is_some_and(|registration| registration.generation == generation)
        {
            state.registrations.remove(key);
        }
    }

    pub fn lookup(&self, key: &WorkerRegistrationKey) -> WorkerVersionMetadata {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .registrations
            .get(key)
            .map(|registration| registration.metadata.clone())
            .unwrap_or_default()
    }

    pub fn has_recent_poller_for_deployment_version(
        &self,
        namespace_id: NamespaceId,
        deployment: &DeploymentId,
        build_id: &BuildId,
        now: OffsetDateTime,
        recent_window: time::Duration,
    ) -> bool {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .registrations
            .iter()
            .any(|(key, registration)| {
                let metadata = &registration.metadata;
                key.namespace_id == namespace_id
                    && metadata.deployment.as_ref() == Some(deployment)
                    && metadata.build_id.as_ref() == Some(build_id)
                    && metadata
                        .last_seen_at
                        .is_some_and(|last_seen| now - last_seen <= recent_window)
            })
    }
}

/// Poll-lifetime finalizer for deployment liveness.
///
/// Dropping an armed guard means the handler future was cancelled before a
/// normal result. Consuming it through [`Self::completed`] preserves the recent
/// observation for the configured poller-history window.
#[must_use = "call completed after a non-cancelled poll result"]
#[derive(Debug)]
pub struct WorkerRegistrationGuard {
    registry: WorkerRegistry,
    key: Option<WorkerRegistrationKey>,
    generation: u64,
}

impl WorkerRegistrationGuard {
    /// Mark a poll as normally resolved, retaining its recent observation.
    pub fn completed(mut self) {
        self.key.take();
    }
}

impl Drop for WorkerRegistrationGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.registry.remove_if_current(&key, self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerIdentity};

    use super::{WorkerRegistrationKey, WorkerRegistry, WorkerVersionMetadata};

    fn arb_small_string() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..8)
            .prop_map(|chars| chars.into_iter().collect())
    }

    proptest! {
        #[test]
        fn property_worker_registry_round_trip(
            worker_identity in arb_small_string(),
            task_queue in arb_small_string(),
            deployment in prop::option::of(arb_small_string()),
            build_id in prop::option::of(arb_small_string()),
            next_deployment in prop::option::of(arb_small_string()),
            next_build_id in prop::option::of(arb_small_string()),
        ) {
            let registry = WorkerRegistry::default();
            let key = WorkerRegistrationKey {
                worker_identity: WorkerIdentity(worker_identity),
                namespace_id: NamespaceId::new(),
                task_queue: TaskQueueName(task_queue),
            };

            prop_assert_eq!(registry.lookup(&key), WorkerVersionMetadata::default());

            let metadata = WorkerVersionMetadata {
                deployment: deployment.clone().map(DeploymentId),
                build_id: build_id.clone().map(BuildId),
                last_seen_at: None,
            };
            registry.register(key.clone(), metadata.clone());
            prop_assert_eq!(registry.lookup(&key), metadata);

            let updated = WorkerVersionMetadata {
                deployment: next_deployment.map(DeploymentId),
                build_id: next_build_id.map(BuildId),
                last_seen_at: None,
            };
            registry.register(key.clone(), updated.clone());
            prop_assert_eq!(registry.lookup(&key), updated);
        }
    }

    #[test]
    fn cancelled_poll_removes_liveness_while_normal_completion_retains_it() {
        let registry = WorkerRegistry::default();
        let key = WorkerRegistrationKey {
            worker_identity: WorkerIdentity("worker".to_string()),
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_string()),
        };
        let metadata = WorkerVersionMetadata {
            deployment: Some(DeploymentId("deployment".to_string())),
            build_id: Some(BuildId("build".to_string())),
            last_seen_at: None,
        };

        drop(registry.register_poll(key.clone(), metadata.clone()));
        assert_eq!(registry.lookup(&key), WorkerVersionMetadata::default());

        registry
            .register_poll(key.clone(), metadata.clone())
            .completed();
        assert_eq!(registry.lookup(&key), metadata);
    }

    #[test]
    fn stale_cancelled_poll_does_not_remove_a_newer_observation() {
        let registry = WorkerRegistry::default();
        let key = WorkerRegistrationKey {
            worker_identity: WorkerIdentity("worker".to_string()),
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("queue".to_string()),
        };
        let old = WorkerVersionMetadata {
            deployment: Some(DeploymentId("deployment".to_string())),
            build_id: Some(BuildId("old".to_string())),
            last_seen_at: None,
        };
        let latest = WorkerVersionMetadata {
            deployment: Some(DeploymentId("deployment".to_string())),
            build_id: Some(BuildId("latest".to_string())),
            last_seen_at: None,
        };

        let stale_guard = registry.register_poll(key.clone(), old);
        registry
            .register_poll(key.clone(), latest.clone())
            .completed();
        drop(stale_guard);

        assert_eq!(registry.lookup(&key), latest);
    }
}
