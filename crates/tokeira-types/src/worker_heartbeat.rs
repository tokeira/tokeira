//! Shared worker-heartbeat observation types.
//!
//! The runtime stores this compact, process-local model for liveness and
//! inventory reads. The encoded heartbeat is deliberately opaque here: only
//! the compatibility edge interprets Temporal protobuf bytes, keeping runtime
//! and kernel crates independent of the public wire schema.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

use crate::{NamespaceId, TaskQueueName, WorkerIdentity};

/// Namespace-unique key assigned to one worker process.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerInstanceKey(pub String);

/// Raw Temporal worker-status enum value, including unknown future values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeatStatus(pub i32);

/// Latest process-local heartbeat observation for one worker instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    /// Namespace that admitted the heartbeat.
    pub namespace_id: NamespaceId,
    /// Namespace-unique worker process key.
    pub worker_instance_key: WorkerInstanceKey,
    /// Task queue reported by the worker.
    pub task_queue: TaskQueueName,
    /// SDK-supplied worker identity, which need not be unique.
    pub worker_identity: WorkerIdentity,
    /// Server receipt time used only for retention and liveness metrics.
    pub last_seen: OffsetDateTime,
    /// Raw worker-status enum value.
    pub status: WorkerHeartbeatStatus,
    /// Build identifier reported by a versioned worker.
    pub build_id: Option<String>,
    /// Deployment name reported by a versioned worker.
    pub deployment_name: Option<String>,
    /// SDK implementation name when supplied.
    pub sdk_name: Option<String>,
    /// SDK implementation version when supplied.
    pub sdk_version: Option<String>,
    /// Complete protobuf-encoded heartbeat for lossless edge responses.
    ///
    /// Runtime treats this as opaque observation data. Keeping the response
    /// image here avoids duplicating every upstream host/slot/poller type in
    /// the runtime-facing domain model.
    pub encoded_heartbeat: Vec<u8>,
}

/// Outcome of one heartbeat-store retention pass.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionReport {
    /// Keys removed because their last server observation exceeded the TTL.
    pub ttl_evicted: Vec<(NamespaceId, WorkerInstanceKey)>,
    /// Keys removed to reduce registry capacity.
    pub capacity_evicted: Vec<(NamespaceId, WorkerInstanceKey)>,
    /// Entries remaining after eviction.
    pub live: Vec<WorkerHeartbeat>,
    /// Remaining entry count grouped by namespace.
    pub namespace_counts: Vec<(NamespaceId, usize)>,
    /// Total entries remaining after eviction.
    pub remaining: usize,
}

/// Failure returned by a heartbeat observation store.
#[derive(Debug, Error)]
pub enum HeartbeatStoreError {
    /// A heartbeat batch was structurally invalid.
    #[error("invalid heartbeat observation: {0}")]
    Invalid(String),
    /// Store-specific backend failure.
    #[error("heartbeat store backend error: {0}")]
    Backend(String),
}

/// Process-local observation store used by heartbeat ingestion and inventory reads.
pub trait HeartbeatStore: Send + Sync + 'static {
    /// Upsert a live observation or apply its status-specific lifecycle action.
    fn insert(&self, heartbeat: WorkerHeartbeat) -> Result<(), HeartbeatStoreError>;

    /// Atomically apply a complete repeated-heartbeat request.
    ///
    /// Implementations validate the whole batch before mutation and make
    /// either every observation or none visible.
    fn insert_batch(&self, heartbeats: Vec<WorkerHeartbeat>) -> Result<(), HeartbeatStoreError>;

    /// Return the latest observation for an exact namespace and worker key.
    fn get_worker(
        &self,
        namespace: &NamespaceId,
        worker_instance_key: &WorkerInstanceKey,
    ) -> Result<Option<WorkerHeartbeat>, HeartbeatStoreError>;

    /// Return all current observations in one namespace.
    fn list_workers(
        &self,
        namespace: &NamespaceId,
    ) -> Result<Vec<WorkerHeartbeat>, HeartbeatStoreError>;

    /// Apply TTL and capacity retention using the caller-supplied clock.
    fn maintain(
        &self,
        now: OffsetDateTime,
        ttl: time::Duration,
        min_evict_age: time::Duration,
        max_entries: usize,
    ) -> Result<EvictionReport, HeartbeatStoreError>;
}
