use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_types::{
        BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerIdentity,
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
            };

            prop_assert_eq!(registry.lookup(&key), WorkerVersionMetadata::default());

            let metadata = WorkerVersionMetadata {
                deployment: deployment.clone().map(DeploymentId),
                build_id: build_id.clone().map(BuildId),
            };
            registry.register(key.clone(), metadata.clone());
            prop_assert_eq!(registry.lookup(&key), metadata);

            let updated = WorkerVersionMetadata {
                deployment: next_deployment.map(DeploymentId),
                build_id: next_build_id.map(BuildId),
            };
            registry.register(key.clone(), updated.clone());
            prop_assert_eq!(registry.lookup(&key), updated);
        }
    }
}
