//! Worker registry tracking active worker versions per task queue.
//!
//! When a worker polls, the edge registers its deployment and build identifiers
//! here. The runtime consults the registry to resolve versioning metadata for
//! task routing, ensuring that workflow tasks are dispatched to workers running
//! compatible code. One SDK identity may poll more than one physical Deployment
//! Version concurrently, so observations are retained per Version rather than
//! overwritten by identity; explicit worker shutdown removes every observation
//! for that identity on the task-queue family.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use time::OffsetDateTime;
use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskKind, TaskQueueName, WorkerIdentity};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerRegistrationKey {
    pub worker_identity: WorkerIdentity,
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
    pub task_kind: TaskKind,
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
    registrations: HashMap<WorkerRegistrationKey, Vec<WorkerRegistration>>,
}

#[derive(Default, Clone, Debug)]
pub struct WorkerRegistry {
    inner: Arc<Mutex<WorkerRegistryState>>,
}

impl WorkerRegistry {
    pub fn register(&self, key: WorkerRegistrationKey, metadata: WorkerVersionMetadata) {
        let mut state = self.inner.lock().expect("inner lock poisoned");
        let registrations = state.registrations.entry(key).or_default();
        // One SDK process commonly uses the same identity for workers on two
        // Deployment Versions. Temporal keeps those observations on separate
        // physical queues, so replacing by identity would erase the older
        // Version's live poller and falsely blackhole its queries
        // (`poller_history.go` and `checkQueryBlackholed`,
        // `service/matching/task_queue_partition_manager.go @ v1.31.0`). Move an
        // existing physical-Version observation to the end so `lookup` retains
        // its historical "most recently registered" contract.
        if let Some(index) = registrations.iter().position(|registration| {
            registration.metadata.deployment == metadata.deployment
                && registration.metadata.build_id == metadata.build_id
        }) {
            registrations.remove(index);
        }
        registrations.push(WorkerRegistration { metadata });
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
            .and_then(|registrations| registrations.last())
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
        self.has_recent_poller_for_deployment_version_on_task_queue(
            namespace_id,
            deployment,
            build_id,
            None,
            None,
            now,
            recent_window,
        )
    }

    /// Return whether a deployment version has a recent poll observation for
    /// one task-queue family and kind, with absent filters matching either.
    ///
    /// The kind filter matters for query blackhole detection: an activity
    /// poller cannot answer a workflow query even when it shares the same task
    /// queue family and deployment version (`checkQueryBlackholed`,
    /// `service/matching/task_queue_partition_manager.go @ v1.31.0`).
    pub fn has_recent_poller_for_deployment_version_on_task_queue(
        &self,
        namespace_id: NamespaceId,
        deployment: &DeploymentId,
        build_id: &BuildId,
        task_queue: Option<&TaskQueueName>,
        task_kind: Option<TaskKind>,
        now: OffsetDateTime,
        recent_window: time::Duration,
    ) -> bool {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .registrations
            .iter()
            .any(|(key, registrations)| {
                key.namespace_id == namespace_id
                    && task_queue.is_none_or(|queue| &key.task_queue == queue)
                    && task_kind.is_none_or(|kind| key.task_kind == kind)
                    && registrations.iter().any(|registration| {
                        let metadata = &registration.metadata;
                        metadata.deployment.as_ref() == Some(deployment)
                            && metadata.build_id.as_ref() == Some(build_id)
                            && metadata
                                .last_seen_at
                                .is_some_and(|last_seen| now - last_seen <= recent_window)
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_types::{
        BuildId, DeploymentId, NamespaceId, TaskKind, TaskQueueName, WorkerIdentity,
    };

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
                task_kind: TaskKind::Workflow,
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
            task_kind: TaskKind::Workflow,
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

    #[test]
    fn one_worker_identity_retains_observations_on_multiple_physical_versions() {
        let registry = WorkerRegistry::default();
        let namespace_id = NamespaceId::new();
        let key = WorkerRegistrationKey {
            worker_identity: WorkerIdentity("worker".to_string()),
            namespace_id,
            task_queue: TaskQueueName("queue".to_string()),
            task_kind: TaskKind::Workflow,
        };
        let now = time::OffsetDateTime::now_utc();
        for build_id in ["build-a", "build-b"] {
            registry.register(
                key.clone(),
                WorkerVersionMetadata {
                    deployment: Some(DeploymentId("deployment".to_string())),
                    build_id: Some(BuildId(build_id.to_string())),
                    last_seen_at: Some(now),
                },
            );
        }

        for build_id in ["build-a", "build-b"] {
            assert!(
                registry.has_recent_poller_for_deployment_version_on_task_queue(
                    namespace_id,
                    &DeploymentId("deployment".to_string()),
                    &BuildId(build_id.to_string()),
                    Some(&key.task_queue),
                    Some(TaskKind::Workflow),
                    now,
                    time::Duration::minutes(5),
                )
            );
        }
        assert_eq!(
            registry.lookup(&key).build_id,
            Some(BuildId("build-b".to_string()))
        );
    }
}
