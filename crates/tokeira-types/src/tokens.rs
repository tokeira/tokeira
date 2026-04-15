//! Opaque fencing tokens that tie a poll response to its completion.
//!
//! When the broker dispatches a task it embeds a token containing the run key,
//! sequence, attempt, and shard epoch. The worker echoes this token back on
//! completion. The server validates every field to prevent stale completions
//! caused by retries, failover, or delayed network delivery.

use serde::{Deserialize, Serialize};

use crate::{LogicalTaskSeq, RunKey, ShardEpoch};

/// Opaque token that binds a workflow task to a specific
/// run, sequence, attempt, and shard epoch.
///
/// Workflow-task tokens carry enough information for the
/// server to reject stale completions after retries,
/// failover, or delayed network delivery. When a worker
/// completes a workflow task it echoes this token back; the
/// runtime validates every field before accepting the result.
///
/// See `docs/architecture/010-history-as-authority.md` for
/// the fencing model that relies on these tokens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskToken {
    /// Durable row key identifying the run.
    pub run_key: RunKey,
    /// Logical task sequence at the time the task was started.
    /// Used to detect completions for an outdated task.
    pub logical_seq: LogicalTaskSeq,
    /// History event ID of the `WorkflowTaskStarted` event.
    /// Anchors the token to a specific point in the history.
    pub started_event_id: i64,
    /// One-based attempt counter for this task. Incremented
    /// on each retry of the same logical task.
    pub attempt: u32,
    /// Shard epoch at the time the task was dispatched. A
    /// completion carrying a stale epoch is rejected after
    /// shard failover.
    pub shard_epoch: ShardEpoch,
}

/// Opaque token that binds an activity task to a specific
/// run, activity, attempt, and shard epoch.
///
/// Analogous to [`WorkflowTaskToken`] but for activity tasks.
/// The runtime validates this token when an activity reports
/// completion, failure, or heartbeat.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTaskToken {
    /// Durable row key identifying the parent run.
    pub run_key: RunKey,
    /// Application-level activity identifier (unique within
    /// the run).
    pub activity_id: String,
    /// History event ID of the `ActivityTaskScheduled` event.
    pub schedule_event_id: i64,
    /// One-based attempt counter. Incremented on each retry.
    pub attempt: u32,
    /// Shard epoch at the time the task was dispatched.
    pub shard_epoch: ShardEpoch,
}

/// Unified task token enum covering both task kinds.
///
/// The edge layer deserialises the opaque token bytes into
/// this enum to determine which validation path to follow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskToken {
    /// Token for a workflow task.
    Workflow(WorkflowTaskToken),
    /// Token for an activity task.
    Activity(ActivityTaskToken),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_task_token_roundtrip_fidelity() {
        let token = ActivityTaskToken {
            run_key: RunKey::new(),
            activity_id: "activity-1".to_string(),
            schedule_event_id: 42,
            attempt: 3,
            shard_epoch: ShardEpoch::ZERO,
        };

        let cloned = token.clone();
        assert_eq!(cloned, token);
        assert_eq!(cloned.activity_id, "activity-1");
        assert_eq!(cloned.schedule_event_id, 42);
        assert_eq!(cloned.attempt, 3);
        assert_eq!(cloned.shard_epoch, ShardEpoch::ZERO);
    }
}
