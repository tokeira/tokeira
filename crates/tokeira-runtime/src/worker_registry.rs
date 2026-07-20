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
}

#[derive(Debug, Default)]
struct WorkerRegistryState {
    registrations: HashMap<WorkerRegistrationKey, WorkerRegistration>,
}

#[derive(Default, Clone, Debug)]
pub struct WorkerRegistry {
    inner: Arc<Mutex<WorkerRegistryState>>,
}

impl WorkerRegistry {
    pub fn register(&self, key: WorkerRegistrationKey, metadata: WorkerVersionMetadata) {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        state
            .registrations
            .insert(key, WorkerRegistration { metadata });
    }

    /// Remove one worker's liveness observation after an explicit worker-shutdown
    /// request.
    ///
    /// Ordinary client cancellation does not call this method: v1.31.0 retains a
    /// poll admission for `PollerHistoryTTL`, which is observable by deployment
    /// deletion and scavenging. `CancelOutstandingWorkerPolls` is the distinct
    /// eager-removal path (`service/matching/matching_engine.go @ v1.31.0`).
    pub fn remove_worker(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        worker_identity: &WorkerIdentity,
    ) {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        state.registrations.retain(|key, _| {
            key.namespace_id != namespace_id
                || &key.task_queue != task_queue
                || &key.worker_identity != worker_identity
        });
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
    fn poll_observation_survives_request_cancellation_until_explicit_shutdown() {
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

        registry.register(key.clone(), metadata.clone());
        assert_eq!(registry.lookup(&key), metadata);
        registry.remove_worker(key.namespace_id, &key.task_queue, &key.worker_identity);
        assert_eq!(registry.lookup(&key), WorkerVersionMetadata::default());
    }
}
