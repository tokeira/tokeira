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

#[derive(Default, Clone)]
pub struct WorkerRegistry {
    inner: Arc<Mutex<HashMap<WorkerRegistrationKey, WorkerVersionMetadata>>>,
}

impl WorkerRegistry {
    pub fn register(&self, key: WorkerRegistrationKey, metadata: WorkerVersionMetadata) {
        self.inner.lock().unwrap().insert(key, metadata);
    }

    pub fn lookup(&self, key: &WorkerRegistrationKey) -> WorkerVersionMetadata {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    pub fn has_recent_poller_for_build_id(
        &self,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        build_id: &BuildId,
        now: OffsetDateTime,
        recent_window: time::Duration,
    ) -> bool {
        self.inner.lock().unwrap().iter().any(|(key, metadata)| {
            key.namespace_id == namespace_id
                && &key.task_queue == task_queue
                && metadata.deployment.is_none()
                && metadata.build_id.as_ref() == Some(build_id)
                && metadata
                    .last_seen_at
                    .is_some_and(|last_seen| now - last_seen <= recent_window)
        })
    }

    pub fn has_recent_poller_for_deployment_version(
        &self,
        namespace_id: NamespaceId,
        deployment: &DeploymentId,
        build_id: &BuildId,
        now: OffsetDateTime,
        recent_window: time::Duration,
    ) -> bool {
        self.inner.lock().unwrap().iter().any(|(key, metadata)| {
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
    use time::{Duration, OffsetDateTime};
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

    fn register_poller(
        registry: &WorkerRegistry,
        namespace_id: NamespaceId,
        task_queue: &TaskQueueName,
        worker: &str,
        deployment: Option<&str>,
        build_id: Option<&str>,
        last_seen_at: OffsetDateTime,
    ) {
        registry.register(
            WorkerRegistrationKey {
                worker_identity: WorkerIdentity(worker.to_string()),
                namespace_id,
                task_queue: task_queue.clone(),
            },
            WorkerVersionMetadata {
                deployment: deployment.map(|value| DeploymentId(value.to_string())),
                build_id: build_id.map(|value| BuildId(value.to_string())),
                last_seen_at: Some(last_seen_at),
            },
        );
    }

    #[test]
    fn recent_build_id_poller_matches_namespace_queue_and_build_id() {
        let registry = WorkerRegistry::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("queue-a".to_string());
        let now = OffsetDateTime::UNIX_EPOCH + Duration::minutes(10);
        register_poller(
            &registry,
            namespace_id,
            &task_queue,
            "worker-a",
            None,
            Some("build-a"),
            now - Duration::seconds(30),
        );

        assert!(registry.has_recent_poller_for_build_id(
            namespace_id,
            &task_queue,
            &BuildId("build-a".to_string()),
            now,
            Duration::minutes(5),
        ));
    }

    #[test]
    fn stale_build_id_poller_does_not_match() {
        let registry = WorkerRegistry::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("queue-a".to_string());
        let now = OffsetDateTime::UNIX_EPOCH + Duration::minutes(10);
        register_poller(
            &registry,
            namespace_id,
            &task_queue,
            "worker-a",
            None,
            Some("build-a"),
            now - Duration::minutes(10),
        );

        assert!(!registry.has_recent_poller_for_build_id(
            namespace_id,
            &task_queue,
            &BuildId("build-a".to_string()),
            now,
            Duration::minutes(5),
        ));
    }

    #[test]
    fn wrong_task_queue_or_build_id_does_not_match() {
        let registry = WorkerRegistry::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("queue-a".to_string());
        let now = OffsetDateTime::UNIX_EPOCH + Duration::minutes(10);
        register_poller(
            &registry,
            namespace_id,
            &task_queue,
            "worker-a",
            None,
            Some("build-a"),
            now,
        );

        assert!(!registry.has_recent_poller_for_build_id(
            namespace_id,
            &TaskQueueName("queue-b".to_string()),
            &BuildId("build-a".to_string()),
            now,
            Duration::minutes(5),
        ));
        assert!(!registry.has_recent_poller_for_build_id(
            namespace_id,
            &task_queue,
            &BuildId("build-b".to_string()),
            now,
            Duration::minutes(5),
        ));
    }

    #[test]
    fn deployment_based_poller_does_not_satisfy_build_id_only_validation() {
        let registry = WorkerRegistry::default();
        let namespace_id = NamespaceId::new();
        let task_queue = TaskQueueName("queue-a".to_string());
        let now = OffsetDateTime::UNIX_EPOCH + Duration::minutes(10);
        register_poller(
            &registry,
            namespace_id,
            &task_queue,
            "worker-a",
            Some("series-a"),
            Some("build-a"),
            now,
        );

        assert!(!registry.has_recent_poller_for_build_id(
            namespace_id,
            &task_queue,
            &BuildId("build-a".to_string()),
            now,
            Duration::minutes(5),
        ));
    }
}
