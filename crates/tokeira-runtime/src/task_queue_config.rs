use std::collections::BTreeMap;

use dashmap::DashMap;
use tokeira_types::{NamespaceId, TaskQueueName};

#[derive(Clone, Debug, PartialEq)]
pub struct TaskQueueConfigEntry {
    pub namespace_id: NamespaceId,
    pub task_queue: TaskQueueName,
    pub queue_rate_limit: Option<f32>,
    pub fairness_key_rate_limit_default: Option<f32>,
    pub fairness_weight_overrides: BTreeMap<String, f32>,
}

pub trait TaskQueueConfigStore: Send + Sync + 'static {
    fn get(
        &self,
        namespace_id: &NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<TaskQueueConfigEntry>;

    fn set(&self, entry: TaskQueueConfigEntry);

    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry>;
}

#[derive(Debug, Default)]
pub struct InMemoryTaskQueueConfigStore {
    entries: DashMap<(NamespaceId, TaskQueueName), TaskQueueConfigEntry>,
}

impl InMemoryTaskQueueConfigStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskQueueConfigStore for InMemoryTaskQueueConfigStore {
    fn get(
        &self,
        namespace_id: &NamespaceId,
        task_queue: &TaskQueueName,
    ) -> Option<TaskQueueConfigEntry> {
        self.entries
            .get(&(*namespace_id, task_queue.clone()))
            .map(|entry| entry.clone())
    }

    fn set(&self, entry: TaskQueueConfigEntry) {
        self.entries
            .insert((entry.namespace_id, entry.task_queue.clone()), entry);
    }

    fn list(&self, namespace_id: &NamespaceId) -> Vec<TaskQueueConfigEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.key().0 == *namespace_id)
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_round_trip_is_key_isolated() {
        let store = InMemoryTaskQueueConfigStore::new();
        let namespace_a = NamespaceId(uuid::Uuid::from_u128(1));
        let namespace_b = NamespaceId(uuid::Uuid::from_u128(2));
        let queue = TaskQueueName("main".to_string());
        let entry = TaskQueueConfigEntry {
            namespace_id: namespace_a,
            task_queue: queue.clone(),
            queue_rate_limit: Some(10.0),
            fairness_key_rate_limit_default: Some(5.0),
            fairness_weight_overrides: BTreeMap::from([("tenant-a".to_string(), 2.0)]),
        };

        store.set(entry.clone());

        assert_eq!(store.get(&namespace_a, &queue), Some(entry));
        assert_eq!(store.get(&namespace_b, &queue), None);
    }
}
