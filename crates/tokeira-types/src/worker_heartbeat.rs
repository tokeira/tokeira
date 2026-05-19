use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

use crate::{NamespaceId, TaskQueueName, WorkerIdentity};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerInstanceKey(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeatStatus(pub i32);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub namespace_id: NamespaceId,
    pub worker_instance_key: WorkerInstanceKey,
    pub task_queue: TaskQueueName,
    pub worker_identity: WorkerIdentity,
    pub last_seen: OffsetDateTime,
    pub status: WorkerHeartbeatStatus,
    pub build_id: Option<String>,
    pub deployment_name: Option<String>,
    pub sdk_name: Option<String>,
    pub sdk_version: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionReport {
    pub ttl_evicted: Vec<(NamespaceId, WorkerInstanceKey)>,
    pub capacity_evicted: Vec<(NamespaceId, WorkerInstanceKey)>,
    pub live: Vec<WorkerHeartbeat>,
    pub namespace_counts: Vec<(NamespaceId, usize)>,
    pub remaining: usize,
}

#[derive(Debug, Error)]
pub enum HeartbeatStoreError {
    #[error("heartbeat store backend error: {0}")]
    Backend(String),
}

pub trait HeartbeatStore: Send + Sync + 'static {
    fn insert(&self, heartbeat: WorkerHeartbeat) -> Result<(), HeartbeatStoreError>;

    fn get_worker(
        &self,
        namespace: &NamespaceId,
        worker_instance_key: &WorkerInstanceKey,
    ) -> Result<Option<WorkerHeartbeat>, HeartbeatStoreError>;

    fn list_workers(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError>;

    fn maintain(
        &self,
        now: OffsetDateTime,
        ttl: time::Duration,
        min_evict_age: time::Duration,
        max_entries: usize,
    ) -> Result<EvictionReport, HeartbeatStoreError>;
}
