use serde::{Deserialize, Serialize};

use crate::{LogicalTaskSeq, RunKey, ShardEpoch};

/// Workflow-task tokens carry enough information for the server to reject stale
/// completions after retries, failover, or delayed network delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskToken {
    pub run_key: RunKey,
    pub logical_seq: LogicalTaskSeq,
    pub started_event_id: i64,
    pub attempt: u32,
    pub shard_epoch: ShardEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskToken {
    pub run_key: RunKey,
    pub schedule_event_id: i64,
    pub started_event_id: i64,
    pub attempt: u32,
    pub shard_epoch: ShardEpoch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskToken {
    Workflow(WorkflowTaskToken),
    Activity(ActivityTaskToken),
}
