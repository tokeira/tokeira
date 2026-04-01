use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use tokeira_types::{
    ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, RetryPolicy, RunId, RunKey,
    SearchAttributes, StickyAffinity, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
};

/// Durable state for an open or closed workflow run.
///
/// This state is intentionally *summary shaped*. The authoritative event stream
/// is still history, but the runtime needs a compact, mutation-friendly view so
/// it can process commands without replaying the whole run every time.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowState {
    pub run_key: RunKey,
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueueName,

    pub status: ExecutionStatus,
    pub transition_seq: TransitionSeq,
    pub last_event_id: i64,
    pub next_workflow_task_seq: LogicalTaskSeq,
    pub pending_workflow_task: Option<PendingWorkflowTask>,
    pub sticky: Option<StickyAffinity>,

    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub workflow_task_timeout: Duration,
    pub retry_policy: Option<RetryPolicy>,
    pub attempt: u32,
    pub activities: BTreeMap<String, ActivityState>,
    pub timers: BTreeMap<String, TimerState>,
    pub children: BTreeMap<WorkflowId, ChildWorkflowState>,

    pub started_at: OffsetDateTime,
    pub closed_at: Option<OffsetDateTime>,
}

impl WorkflowState {
    pub fn is_open(&self) -> bool {
        self.status.is_open()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingWorkflowTask {
    pub logical_seq: LogicalTaskSeq,
    pub scheduled_event_id: i64,
    pub started_event_id: Option<i64>,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivityState {
    pub activity_id: String,
    pub schedule_event_id: i64,
    pub task_queue: TaskQueueName,
    pub attempt: u32,
    pub schedule_to_close_timeout: Option<Duration>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
    pub heartbeat_timeout: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimerState {
    pub timer_id: String,
    pub started_event_id: i64,
    pub fire_at: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChildWorkflowState {
    pub child_workflow_id: WorkflowId,
    pub child_run_id: Option<RunId>,
    pub initiated_event_id: i64,
    pub started_event_id: Option<i64>,
    pub parent_close_policy: ParentClosePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParentClosePolicy {
    Terminate,
    RequestCancel,
    Abandon,
}

/// Either the run does not yet exist or it already has durable state.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadedRun {
    Absent,
    Existing(WorkflowState),
}
