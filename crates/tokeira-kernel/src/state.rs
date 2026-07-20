use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Duration, OffsetDateTime};
use tokeira_types::{
    BuildId, DeploymentId, EventPrincipal, ExecutionStatus, Headers, LogicalTaskSeq, Memo,
    NamespaceId, Payload, Payloads, RetryPolicy, RunId, RunKey, SearchAttributes, StickyAffinity,
    TaskQueueName, TransitionSeq, WorkerIdentity, WorkflowId, WorkflowType,
};

use crate::command::{WorkerVersionStamp, WorkflowTaskFailedCause, WorkflowTaskWorkerVersion};

/// Identity of the last non-transient workflow-task problem, the kernel
/// analog of v1.31.0's `WorkflowExecutionInfo.LastWorkflowTaskFailure` oneof.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkflowTaskProblem {
    /// A `WorkflowTaskFailed` event was persisted with this cause.
    Failed(WorkflowTaskFailedCause),
    /// A `WorkflowTaskTimedOut(StartToClose)` event was persisted. v1.31.0
    /// only ever stores the start-to-close type — schedule-to-start timeouts
    /// never set the oneof (`workflow_task_state_machine.go:968, 270-305 @
    /// v1.31.0`).
    TimedOutStartToClose,
}

/// Durable state for an open or closed workflow run.
///
/// This state is intentionally *summary shaped*. The authoritative event stream
/// is still history, but the runtime needs a compact, mutation-friendly view so
/// it can process commands without replaying the whole run every time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowState {
    /// Composite storage key for this run.
    pub run_key: RunKey,
    /// Namespace that owns the execution.
    pub namespace_id: NamespaceId,
    /// User-assigned workflow identifier.
    pub workflow_id: WorkflowId,
    /// Unique identifier for this specific run.
    pub run_id: RunId,
    /// Workflow type name (maps to an SDK handler).
    pub workflow_type: WorkflowType,
    /// Task queue where workflow tasks are dispatched.
    pub task_queue: TaskQueueName,
    /// Optional deployment for versioned task routing.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier for versioned task routing.
    pub build_id: Option<BuildId>,
    /// Worker Deployment v2 versioning state for this execution.
    #[serde(default)]
    pub versioning_info: Option<WorkflowVersioningInfo>,
    /// Deployment name that completed the most recent versioned workflow task.
    #[serde(default)]
    pub worker_deployment_name: Option<String>,

    /// Current lifecycle status (Running, Paused, or a
    /// terminal state).
    pub status: ExecutionStatus,
    /// Optimistic-concurrency fence for committed transitions.
    /// See `docs/architecture/020-kernel.md`.
    pub transition_seq: TransitionSeq,
    /// Highest event ID assigned so far in this run.
    pub last_event_id: i64,
    /// Number of external-payload references across committed history.
    #[serde(default)]
    pub external_payload_count: i64,
    /// Sum of declared external-payload sizes across committed history.
    #[serde(default)]
    pub external_payload_size_bytes: i64,
    /// Next logical task sequence to assign when scheduling a
    /// workflow task.
    pub next_workflow_task_seq: LogicalTaskSeq,
    /// The currently pending workflow task, if any. At most
    /// one WFT is pending at a time.
    pub pending_workflow_task: Option<PendingWorkflowTask>,
    /// started_event_id of the most recently completed
    /// workflow task.
    pub previous_started_event_id: i64,
    /// Attempt number to assign to the next scheduled
    /// workflow task.
    pub workflow_task_attempt: u32,
    /// Consecutive workflow-task problems (failures and start-to-close
    /// timeouts) since the last successful WFT completion. Advances under
    /// exactly the same guard as the attempt counter in v1.31.0's
    /// `failWorkflowTask` (`workflow_task_state_machine.go:1010-1027 @
    /// v1.31.0`): non-sticky failures and non-sticky start-to-close
    /// timeouts, transient or not; never for schedule-to-start timeouts or
    /// while the run's real sticky queue is set. Reset to zero on WFT
    /// completion. Drives the `TemporalReportedProblems` derive on Describe.
    #[serde(default)]
    pub workflow_task_attempts_since_last_success: u32,
    /// Identity of the last NON-TRANSIENT workflow-task problem, mirroring
    /// v1.31.0's persisted `LastWorkflowTaskFailure` oneof — written only
    /// when the corresponding failed/timed-out event is actually emitted
    /// (`workflow_task_state_machine.go:907-911, 975-977 @ v1.31.0`).
    /// Transient problems advance the counter without touching this, so a
    /// threshold crossed by a transient retry's timeout still reports the
    /// attempt-1 cause. Cleared on WFT completion.
    #[serde(default)]
    pub last_workflow_task_problem: Option<WorkflowTaskProblem>,
    /// Sticky execution affinity recorded when a worker
    /// provides a `sticky_ttl`.
    pub sticky: Option<StickyAffinity>,
    /// Pause metadata when the workflow is paused.
    pub pause_info: Option<PauseInfo>,
    /// True after a cooperative cancellation has been requested but before the
    /// workflow has closed or ignored it.
    #[serde(default)]
    pub cancel_requested: bool,
    /// Monotonic stamp incremented on pause/unpause to
    /// invalidate in-flight workflow task deliveries.
    pub wft_stamp: u64,

    /// Unindexed key-value metadata attached to the execution.
    pub memo: Memo,
    /// Indexed attributes for visibility queries.
    pub search_attributes: SearchAttributes,
    /// Maximum wall-clock time for the entire execution chain.
    pub workflow_execution_timeout: Option<Duration>,
    /// Maximum wall-clock time for a single run.
    pub workflow_run_timeout: Option<Duration>,
    /// Maximum time a worker may hold a workflow task.
    pub workflow_task_timeout: Duration,
    /// Retry policy governing automatic retries on failure.
    pub retry_policy: Option<RetryPolicy>,
    /// Current retry attempt number (1-based).
    pub attempt: u32,
    /// Run ID of the very first run in the execution chain.
    pub first_execution_run_id: Option<RunId>,
    /// Run ID of the original execution in the chain.
    pub original_execution_run_id: Option<RunId>,
    /// Run ID this run was reset INTO, if it has been reset. Set on the BASE run
    /// of a reset; surfaced as `ExecutionInfo.ResetRunId` via DescribeMutableState
    /// (`UpdateResetRunID`, mutable_state_impl.go:1010 @ v1.31.0).
    pub reset_run_id: Option<RunId>,
    /// Parent run identity if this execution is a child.
    pub parent_run_key: Option<RunKey>,
    /// Parent workflow identity if this execution is a child.
    pub parent_workflow_id: Option<WorkflowId>,
    /// Parent run ID if this execution is a child.
    pub parent_run_id: Option<RunId>,
    /// Parent namespace if this execution is a child.
    pub parent_namespace_id: Option<NamespaceId>,
    /// Parent namespace NAME if this execution is a child, retained so a
    /// retry/continue-as-new/cron successor still authors the parent namespace
    /// name on its own `WorkflowExecutionStarted` event.
    #[serde(default)]
    pub parent_namespace_name: Option<String>,
    /// Parent initiation event ID if this execution is a child.
    pub parent_initiated_event_id: i64,
    /// Canonical root workflow ID for this run, authored from the start event
    /// when present and otherwise defaulted to this run's own execution.
    #[serde(default)]
    pub root_workflow_id: Option<WorkflowId>,
    /// Canonical root run ID for this run.
    #[serde(default)]
    pub root_run_id: Option<RunId>,
    /// Last successful predecessor completion result.
    pub last_completion_result: Option<Payloads>,
    /// Open activities keyed by activity ID.
    pub activities: BTreeMap<String, ActivityState>,
    /// Open timers keyed by timer ID.
    pub timers: BTreeMap<String, TimerState>,
    /// Open child workflows keyed by child workflow ID.
    pub children: BTreeMap<WorkflowId, ChildWorkflowState>,
    /// Pending external signal deliveries keyed by initiated
    /// event ID.
    pub pending_external_signals: BTreeMap<i64, PendingExternalSignal>,
    /// Pending external cancel deliveries keyed by initiated
    /// event ID.
    pub pending_external_cancels: BTreeMap<i64, PendingExternalCancel>,
    /// Pending workflow updates keyed by update ID.
    /// These are updates that have been accepted by the worker
    /// (WorkflowExecutionUpdateAccepted event written).
    pub pending_updates: BTreeMap<String, PendingUpdate>,
    /// Updates that have been admitted (submitted by the client)
    /// but not yet accepted by the worker. Tracked to reject
    /// duplicate update IDs.
    pub admitted_updates: std::collections::HashSet<String>,
    /// Pending Nexus operations keyed by operation ID.
    pub pending_nexus_operations: BTreeMap<String, PendingNexusOperation>,
    /// Completion callbacks attached to this execution.
    #[serde(default)]
    pub completion_callbacks: Vec<CompletionCallback>,
    /// SDK-authored summary/details metadata captured at start.
    #[serde(default)]
    pub user_metadata: Option<UserMetadata>,
    /// Links associated with the workflow start event.
    #[serde(default)]
    pub links: Vec<Link>,
    /// Start delay requested by the client. The runtime uses this to defer
    /// initial WFT dispatch; the kernel records it so replay and snapshots keep
    /// the accepted start contract durable.
    #[serde(default)]
    pub workflow_start_delay: Option<Duration>,
    /// Priority metadata inherited by workflow tasks unless a child command
    /// overrides it.
    #[serde(default)]
    pub priority: Option<Priority>,

    /// Timestamp when the first event was recorded.
    pub started_at: OffsetDateTime,
    /// Timestamp when the very first run in the execution
    /// chain started.
    pub first_run_started_at: Option<OffsetDateTime>,
    /// Timestamp when the execution reached a terminal state.
    /// `None` while the execution is still open.
    pub closed_at: Option<OffsetDateTime>,
    /// Result payload retained for terminal completion.
    pub close_result: Option<Payloads>,
    /// Opaque failure payload retained for terminal failure.
    pub close_failure: Option<Payload>,
    /// Maps each request id that authored an event on this run to the authoring
    /// event (mirrors `persistencespb.RequestIDInfo`,
    /// `WorkflowExecutionInfo.request_ids @ v1.31.0`). The start request id maps
    /// to the `WorkflowExecutionStarted` event; each request that attaches via a
    /// `UseExisting` conflict resolution maps to the `WorkflowExecutionOptionsUpdated`
    /// event it authored. Surfaced through `DescribeWorkflowExecution`'s
    /// `WorkflowExtendedInfo.request_id_infos`.
    #[serde(default)]
    pub request_id_infos: BTreeMap<String, RequestIdInfo>,
    /// Events admitted while a workflow task was started, held here (without
    /// event ids) until the WFT closes and they are flushed into history in
    /// admission order. Mirrors the v1.31.0 buffered-event model: `bufferEvent`
    /// decides eligibility (`historybuilder/event_store.go:263 @ v1.31.0`) and
    /// the flush happens at WFT close (`failWorkflowTask`,
    /// `workflow/util.go:26 @ v1.31.0`). Phase 1 buffers
    /// `WorkflowExecutionSignaled` and `WorkflowExecutionCancelRequested`
    /// (spec: `kernel-event-buffering`). Always empty for closed runs.
    #[serde(default)]
    pub buffered_events: Vec<BufferedEvent>,
}

/// A history event admitted while a workflow task was started, held durably on
/// [`WorkflowState`] without an event id until the WFT closes and it is
/// flushed into history (event ids are assigned only at flush). Buffered
/// events keep their admission timestamp: v1.31.0 buffered events are
/// fully-formed at admission except for the id
/// (`EventStore::add`, `historybuilder/event_store.go:74 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BufferedEvent {
    /// Wall-clock time the authoring command was admitted; the flushed
    /// history event carries this timestamp, not the flush time.
    pub admitted_at: OffsetDateTime,
    /// The event body to emit at flush.
    pub kind: crate::event::HistoryEventKind,
    /// Authenticated caller that admitted this event.
    ///
    /// Buffer flush may run under a different worker request, so attribution
    /// must be captured here rather than inherited at flush time
    /// (`mutable_state_impl.go:7105-7124 @ v1.31.0`).
    #[serde(default)]
    pub principal: Option<EventPrincipal>,
}

/// `EventType` wire numbers the kernel authors into [`RequestIdInfo::event_type`].
/// These mirror `temporal.api.enums.v1.EventType @ v1.31.0`. The kernel already
/// stores `EventType` i32 values in link references (see
/// [`LinkWorkflowEventReference`]), so this keeps that convention rather than
/// taking a `tokeira-proto` dependency (kernel purity, AGENTS §2).
pub const EVENT_TYPE_WORKFLOW_EXECUTION_STARTED: i32 = 1;
/// See [`EVENT_TYPE_WORKFLOW_EXECUTION_STARTED`]. `WorkflowExecutionOptionsUpdated`
/// is the event an attaching `UseExisting` request authors.
pub const EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED: i32 = 55;

/// Per-run record of which event a request id authored (mirrors
/// `persistencespb.RequestIDInfo`). Tokeira adopted the buffered-event model
/// (spec: `kernel-event-buffering`, superseding the earlier no-buffering
/// deviation), but request-id infos are only authored for start / options-
/// updated events — kinds the `bufferEvent` predicate never buffers
/// (`event_store.go:263 @ v1.31.0`) — so `buffered` remains `false` here; it
/// is retained for wire parity with the `DescribeWorkflowExecution` surface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestIdInfo {
    /// History event id that this request id authored.
    pub event_id: i64,
    /// Type of the authoring event (a `temporal.api.enums.v1.EventType` i32),
    /// e.g. `WorkflowExecutionStarted` for the starting request or
    /// `WorkflowExecutionOptionsUpdated` for an attached request.
    pub event_type: i32,
    /// Whether the authoring event is still buffered. Always `false` here
    /// (see the struct-level note).
    pub buffered: bool,
}

impl WorkflowState {
    /// Returns `true` when the execution is still in progress
    /// (`Running` or `Paused`).
    pub fn is_open(&self) -> bool {
        self.status.is_open()
    }

    /// Return the execution-scoped versioning override, if one is set.
    pub fn versioning_override(&self) -> Option<&VersioningOverride> {
        self.versioning_info
            .as_ref()
            .and_then(|info| info.versioning_override.as_ref())
    }

    /// Resolve the deployment version that currently controls dispatch.
    ///
    /// Precedence (v1.31.0 `GetEffectiveDeployment`,
    /// `service/history/workflow/util.go @ v1.31.0`): an in-flight transition
    /// wins, then a pinned override, then the worker-reported deployment — but
    /// only when the effective behavior is versioned. A run whose effective
    /// behavior is UNSPECIFIED is unversioned, so its deployment is nil even if
    /// a stale `deployment_version` lingers.
    pub fn effective_deployment(&self) -> Option<&WorkerDeploymentVersionRef> {
        let info = self.versioning_info.as_ref()?;
        if let Some(transition) = &info.version_transition {
            return Some(transition);
        }
        if let Some(VersioningOverride::Pinned { version }) = &info.versioning_override {
            return Some(version);
        }
        (self.effective_behavior() != VersioningBehavior::Unspecified)
            .then_some(info.deployment_version.as_ref())
            .flatten()
    }

    /// Resolve the versioning behavior that currently controls dispatch.
    ///
    /// Precedence (v1.31.0 `GetEffectiveVersioningBehavior`,
    /// `service/history/workflow/util.go @ v1.31.0`): an in-flight transition
    /// always reads as AUTO_UPGRADE; otherwise an override wins over the
    /// worker-reported behavior; otherwise the worker-reported behavior.
    pub fn effective_behavior(&self) -> VersioningBehavior {
        let Some(info) = self.versioning_info.as_ref() else {
            return VersioningBehavior::Unspecified;
        };
        if info.version_transition.is_some() {
            return VersioningBehavior::AutoUpgrade;
        }
        match &info.versioning_override {
            Some(VersioningOverride::Pinned { .. }) => VersioningBehavior::Pinned,
            Some(VersioningOverride::AutoUpgrade) => VersioningBehavior::AutoUpgrade,
            None => info.behavior,
        }
    }

    /// Start an in-flight transition toward a new Worker Deployment Version.
    ///
    /// Mirrors v1.31.0 `MutableState.StartDeploymentTransition`
    /// (`service/history/workflow/mutable_state_impl.go @ v1.31.0`). Pinned runs
    /// cannot transition — a pinned workflow must stay on its version, so a
    /// differing poller's task is dropped by the caller rather than moved.
    pub fn start_version_transition(
        &mut self,
        target: WorkerDeploymentVersionRef,
        revision_number: i64,
    ) -> Result<(), VersionTransitionError> {
        if self.effective_behavior() == VersioningBehavior::Pinned {
            return Err(VersionTransitionError::PinnedWorkflowCannotTransition);
        }

        let info = self
            .versioning_info
            .get_or_insert_with(WorkflowVersioningInfo::default);
        info.version_transition = Some(target);
        info.revision_number = revision_number;
        // Clear sticky affinity and reschedule any not-yet-started pending WFT:
        // the workflow's effective deployment just changed, so the pending task
        // must be re-dispatched to a poller on the new target deployment rather
        // than served from the old sticky queue. A WFT already started on the
        // old deployment is left to finish (started_event_id set) — the
        // transition completes when a task next completes on the target.
        self.sticky = None;
        if let Some(pending) = self.pending_workflow_task.as_mut()
            && pending.started_event_id.is_none()
        {
            pending.started_at = None;
            self.workflow_task_attempt = pending.attempt;
        }
        Ok(())
    }

    /// Apply worker-completed versioning fields from a workflow task completion.
    ///
    /// Mirrors v1.31.0 `afterAddWorkflowTaskCompletedEvent`
    /// (`service/history/workflow/workflow_task_state_machine.go @ v1.31.0`).
    pub fn apply_wft_versioning(
        &mut self,
        behavior: VersioningBehavior,
        deployment_version: Option<WorkerDeploymentVersionRef>,
        worker_deployment_name: Option<String>,
    ) {
        let info = self
            .versioning_info
            .get_or_insert_with(WorkflowVersioningInfo::default);
        // Complete the transition only when this WFT actually completed on the
        // transition's target deployment. A WFT that was already started when
        // the transition began can complete on the *old* deployment; that does
        // not finish the transition, and a fresh WFT is scheduled to drive it.
        if info.version_transition.as_ref() == deployment_version.as_ref() {
            info.version_transition = None;
        }
        info.behavior = behavior;
        if behavior == VersioningBehavior::Unspecified {
            // Unversioned workers do not carry a deployment version.
            info.deployment_version = None;
        } else {
            info.deployment_version = deployment_version;
        }
        // WorkerDeploymentName tracks the most recent completion's deployment
        // name regardless of behavior: v1.31.0 sets
        // `executionInfo.WorkerDeploymentName = attrs.GetWorkerDeploymentName()`
        // unconditionally, before the behavior branch
        // (`afterAddWorkflowTaskCompletedEvent`,
        // `service/history/workflow/workflow_task_state_machine.go @ v1.31.0`).
        // An unversioned worker that reports a deployment name still records it.
        self.worker_deployment_name = worker_deployment_name;
        self.compact_versioning_info();
    }

    /// Record the legacy worker stamp and its visibility-index values from a
    /// successful workflow-task completion.
    ///
    /// The values are durable derived state reconstructed from the completed
    /// event. v1.31.0 places the current unversioned sentinel first, preserves
    /// earlier concrete build IDs, and appends a newly observed stamp
    /// (`addBuildIdToLoadedSearchAttribute`,
    /// `service/history/workflow/mutable_state_impl.go @ v1.31.0`).
    pub fn apply_wft_worker_version(
        &mut self,
        worker_version: Option<&WorkflowTaskWorkerVersion>,
        behavior: VersioningBehavior,
    ) {
        let info = self
            .versioning_info
            .get_or_insert_with(WorkflowVersioningInfo::default);
        let stamp = worker_version.and_then(|version| version.stamp.as_ref());
        info.most_recent_worker_version_stamp = stamp.cloned();

        info.build_id_search_attributes
            .retain(|value| value != "unversioned");
        if behavior == VersioningBehavior::Unspecified
            && !stamp.is_some_and(|stamp| stamp.use_versioning)
        {
            info.build_id_search_attributes
                .insert(0, "unversioned".to_owned());
        }

        if let Some(stamp) = stamp.filter(|stamp| !stamp.build_id.is_empty()) {
            let prefix = if stamp.use_versioning {
                "versioned"
            } else {
                "unversioned"
            };
            let value = format!("{prefix}:{}", stamp.build_id);
            if !info.build_id_search_attributes.contains(&value) {
                info.build_id_search_attributes.push(value);
            }
        }
    }

    /// Update the execution-scoped versioning override while preserving any
    /// other populated versioning fields.
    pub fn set_versioning_override(&mut self, override_: Option<VersioningOverride>) {
        match override_ {
            Some(override_) => {
                // A pinned override immediately becomes the effective deployment,
                // so DescribeWorkflowExecution must expose its deployment name.
                // AUTO_UPGRADE deliberately leaves the last worker deployment name
                // unchanged (`updateVersioningOverride`,
                // `service/history/workflow/mutable_state_impl.go @ v1.31.0`).
                if let VersioningOverride::Pinned { version } = &override_ {
                    self.worker_deployment_name = Some(version.deployment_name.clone());
                }
                self.versioning_info
                    .get_or_insert_with(WorkflowVersioningInfo::default)
                    .versioning_override = Some(override_);
            }
            None => {
                if let Some(info) = self.versioning_info.as_mut() {
                    info.versioning_override = None;
                    // Clearing an override returns the workflow to the deployment
                    // last reported by a successful WFT, or to no deployment when
                    // no versioning state exists. This mirrors the same v1.31.0
                    // mutable-state transition cited above.
                    self.worker_deployment_name = info
                        .deployment_version
                        .as_ref()
                        .map(|version| version.deployment_name.clone());
                    self.compact_versioning_info();
                } else {
                    self.worker_deployment_name = None;
                }
            }
        }
    }

    fn compact_versioning_info(&mut self) {
        // Collapse an all-default versioning_info to None so unversioned runs
        // carry no versioning state. worker_deployment_name is a standalone
        // field (v1.31.0 `executionInfo.WorkerDeploymentName`) and is NOT
        // cleared here: an unversioned worker that reported a deployment name
        // retains it even when versioning_info compacts away.
        if self
            .versioning_info
            .as_ref()
            .is_some_and(WorkflowVersioningInfo::is_unversioned_default)
        {
            self.versioning_info = None;
        }
    }
}

/// Error returned by pure per-run versioning transitions.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum VersionTransitionError {
    /// Pinned workflows cannot start deployment-version transitions.
    #[error("pinned workflow cannot start a deployment-version transition")]
    PinnedWorkflowCannotTransition,
}

/// Authoritative record of a pending workflow task.
///
/// The kernel uses this to validate that starts, completions,
/// failures, and timeouts reference the correct task. At most
/// one `PendingWorkflowTask` exists per run at any time.
///
/// See `docs/architecture/020-kernel.md` §Pending workflow
/// task model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingWorkflowTask {
    /// Logical task sequence assigned at schedule time.
    pub logical_seq: LogicalTaskSeq,
    /// Event ID of the `WorkflowTaskScheduled` event.
    pub scheduled_event_id: i64,
    /// Wall-clock time when the task was scheduled.
    pub scheduled_at: OffsetDateTime,
    /// Event ID of the `WorkflowTaskStarted` event, or `None`
    /// if the task has not yet been picked up by a worker.
    pub started_event_id: Option<i64>,
    /// Wall-clock time when the task was started, if it has
    /// been picked up by a worker.
    pub started_at: Option<OffsetDateTime>,
    /// Number of times this task has been started (incremented
    /// on each start, including retries after failure/timeout).
    pub attempt: u32,
    /// Schedule-to-start deadline for a STICKY dispatch (sticky raise S2);
    /// `None` for normal dispatch (normal WFTs have no schedule-to-start
    /// timer @ v1.31.0). Captured at schedule time — a sticky RESET does not
    /// disarm it (the v1.31.0 timer fires regardless; stickytq leaf 2).
    #[serde(default)]
    pub schedule_to_start_deadline: Option<OffsetDateTime>,
    /// Which task mode this pending WFT runs in. A SPECULATIVE task persists
    /// nothing at schedule/start and either drops without trace or
    /// materializes late at completion — it is attempt 1, so the transient
    /// `attempt > 1` predicate cannot classify it and an explicit bit is
    /// required (`WORKFLOW_TASK_TYPE_SPECULATIVE`,
    /// `workflow_task_state_machine.go:676-748 @ v1.31.0`; spec
    /// speculative-wft K1). `#[serde(default)]` decodes pre-existing state
    /// as Normal.
    #[serde(default)]
    pub task_type: WorkflowTaskType,
}

/// Task mode for a pending workflow task (spec speculative-wft K1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowTaskType {
    /// Ordinary durable task: `WorkflowTaskScheduled`/`Started` persist as
    /// they happen (attempt-1) or virtually (transient attempt>1).
    #[default]
    Normal,
    /// Update-carrying task that persists NOTHING until completion decides
    /// drop (rejection-only) or late materialization
    /// (`WORKFLOW_TASK_TYPE_SPECULATIVE` @ v1.31.0).
    Speculative,
}

/// Durable state for a single open activity.
///
/// Carries the full set of parameters needed to re-dispatch
/// the activity after pause/unpause or retry. The `stamp`
/// field is a monotonic invalidation counter used to detect
/// stale deliveries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityState {
    /// User-assigned activity identifier.
    pub activity_id: String,
    /// Activity type name (maps to an SDK handler).
    pub activity_type: String,
    /// Event ID of the `ActivityTaskScheduled` event.
    pub schedule_event_id: i64,
    /// Task queue where the activity is dispatched.
    pub task_queue: TaskQueueName,
    /// Optional deployment override for this activity.
    pub deployment: Option<DeploymentId>,
    /// Optional build identifier override for this activity.
    pub build_id: Option<BuildId>,
    /// Arguments passed to the activity function.
    pub input: Payloads,
    /// Transport headers carried with the activity task.
    pub header: Option<Headers>,
    /// Current attempt number (1-based, incremented on retry).
    pub attempt: u32,
    /// Retry policy for this activity.
    pub retry_policy: Option<RetryPolicy>,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Maximum time from schedule to worker pickup.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time from worker pickup to completion.
    pub start_to_close_timeout: Option<Duration>,
    /// Maximum time between heartbeats.
    pub heartbeat_timeout: Option<Duration>,
    /// When the activity was originally scheduled.
    pub scheduled_at: OffsetDateTime,
    /// When the currently pending attempt was scheduled.
    ///
    /// Temporal returns this separately from the original schedule time on
    /// activity poll. Keeping it beside the durable activity info lets retries
    /// resume after restart without trusting the transient dispatch queue.
    #[serde(default)]
    pub current_attempt_scheduled_at: Option<OffsetDateTime>,
    /// When the activity was started by a worker, if it
    /// has started.
    pub started_at: Option<OffsetDateTime>,
    /// Event ID of the `ActivityTaskStarted` event, if the
    /// activity has been started.
    pub started_event_id: Option<i64>,
    /// Failure from the previous attempt, surfaced on the next
    /// `ActivityTaskStarted` event when the activity retries.
    pub last_failure: Option<Payload>,
    /// Identity of the worker that started the current attempt
    /// (`ai.StartedIdentity` @ v1.31.0; kernel raise K3). NOT cleared on
    /// retry — `UpdateActivityInfoForRetries` copies it into
    /// `retry_last_worker_identity` and leaves it in place
    /// (`workflow/activity.go:63-97 @ v1.31.0`); the next start overwrites.
    #[serde(default)]
    pub started_identity: Option<WorkerIdentity>,
    /// Identity bookkeeping for Describe's `LastWorkerIdentity` fallback
    /// (`ai.RetryLastWorkerIdentity` @ v1.31.0; kernel raise K3): written by
    /// heartbeats carrying a non-empty identity
    /// (`recordactivitytaskheartbeat/api.go:79-81`) and by retries from the
    /// failing attempt's `started_identity`.
    #[serde(default)]
    pub retry_last_worker_identity: Option<WorkerIdentity>,
    /// Latest worker heartbeat progress for this activity.
    ///
    /// Temporal stores this on mutable activity info and returns it on the next
    /// activity task start (`mutable_state_impl.go:1956`,
    /// `recordactivitytaskstarted/api.go:265 @ v1.31.0`). Keeping it on
    /// `ActivityState` makes heartbeat progress part of the durable run state
    /// rather than volatile timeout tracking.
    #[serde(default)]
    pub heartbeat_details: Option<Payloads>,
    /// A workflow requested cancellation of this (started) activity; the
    /// worker learns via its next heartbeat response and resolves
    /// cooperatively (`ai.CancelRequested` @ v1.31.0; kernel raise K4). This
    /// durable bit is the authority — the in-memory tracking flag is only a
    /// delivery aid.
    #[serde(default)]
    pub cancel_requested: bool,
    /// Pause metadata when the activity is individually
    /// paused.
    pub pause_info: Option<ActivityPauseInfo>,
    /// Monotonic stamp incremented on pause/unpause/option
    /// changes to invalidate in-flight deliveries.
    pub stamp: u64,
    /// Whether an explicit activity reset applies to the currently running
    /// incarnation. Heartbeat responses project this independently from
    /// cancellation and pause until retry preparation consumes it
    /// (`ActivityReset`, `service/history/workflow/activity.go @ v1.31.0`).
    #[serde(default)]
    pub activity_reset: bool,
    /// Deferred instruction to clear heartbeat details when retry preparation
    /// creates the next incarnation. A reset must not erase progress that the
    /// still-running worker may continue to heartbeat before it completes or
    /// fails (`ResetHeartbeats`, `service/history/workflow/activity.go @
    /// v1.31.0`).
    #[serde(default)]
    pub reset_heartbeats: bool,
}

/// Metadata recorded when a workflow is paused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PauseInfo {
    /// Wall-clock time the pause was applied.
    pub pause_time: OffsetDateTime,
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
    /// Request ID used for idempotent re-delivery of the
    /// pause command.
    pub request_id: String,
}

/// Metadata recorded when an individual activity is paused.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivityPauseInfo {
    /// Wall-clock time the pause was applied.
    pub pause_time: OffsetDateTime,
    /// Identity of the caller who issued the pause.
    pub identity: String,
    /// Human-readable reason for the pause.
    pub reason: String,
    /// Namespace workflow rule that caused the pause, or `None` for a manual request.
    #[serde(default)]
    pub rule_id: Option<String>,
}

/// Durable state for a single open timer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimerState {
    /// User-assigned timer identifier.
    pub timer_id: String,
    /// Event ID of the `TimerStarted` event.
    pub started_event_id: i64,
    /// Absolute wall-clock time when the timer should fire.
    pub fire_at: OffsetDateTime,
}

/// Durable state for a single open child workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChildWorkflowState {
    /// Workflow ID of the child.
    pub child_workflow_id: WorkflowId,
    /// Namespace that owns the child workflow.
    pub namespace_id: NamespaceId,
    /// Human-readable namespace name, if the edge supplied one.
    pub namespace: Option<String>,
    /// Workflow type of the child, retained for terminal events.
    pub workflow_type: WorkflowType,
    /// Header the child was started with, retained so the
    /// `ChildWorkflowExecutionStarted` event on the parent carries it
    /// (child_workflow_execution_started_event @ v1.31.0).
    #[serde(default)]
    pub header: Option<Headers>,
    /// Run ID assigned to the child, once started.
    pub child_run_id: Option<RunId>,
    /// Event ID of the initiation event in the parent's
    /// history.
    pub initiated_event_id: i64,
    /// Event ID of the `ChildWorkflowExecutionStarted` event,
    /// once the child has started.
    pub started_event_id: Option<i64>,
    /// What to do with this child when the parent closes.
    pub parent_close_policy: ParentClosePolicy,
}

/// What happens to a child workflow when its parent closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentClosePolicy {
    /// Forcibly terminate the child.
    Terminate,
    /// Send a cooperative cancel request to the child.
    RequestCancel,
    /// Leave the child running (detach).
    Abandon,
}

/// Tracks an in-flight signal to an external workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingExternalSignal {
    /// Event ID of the initiation event (used as map key).
    pub initiated_event_id: i64,
    /// Namespace ID of the signal target.
    pub target_namespace_id: NamespaceId,
    /// Human-readable namespace name of the target, if supplied.
    pub target_namespace: Option<String>,
    /// Workflow ID of the signal target.
    pub target_workflow_id: WorkflowId,
    /// Optional run ID of the signal target.
    pub target_run_id: Option<RunId>,
    /// Name of the signal being delivered.
    pub signal_name: String,
}

/// Tracks an in-flight cancel request to an external
/// workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingExternalCancel {
    /// Event ID of the initiation event (used as map key).
    pub initiated_event_id: i64,
    /// Namespace ID of the cancel target.
    pub target_namespace_id: NamespaceId,
    /// Human-readable namespace name of the target, if supplied.
    pub target_namespace: Option<String>,
    /// Workflow ID of the cancel target.
    pub target_workflow_id: WorkflowId,
    /// Optional run ID of the cancel target.
    pub target_run_id: Option<RunId>,
}

/// Tracks a workflow update that has been accepted but not
/// yet completed or rejected by the worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingUpdate {
    /// Caller-assigned update identifier.
    pub update_id: String,
    /// Event ID of the `WorkflowExecutionUpdateAccepted`
    /// event.
    pub accepted_event_id: i64,
    /// Name of the update handler.
    pub name: String,
}

/// Durable delivery state for a pending Nexus operation's cancellation request.
///
/// This is authoritative workflow state, not an outbound queue record. The runtime
/// performs delivery and supplies attempt outcomes/deadlines; the pure kernel only
/// advances this state and emits derived dispatch effects. The shape mirrors
/// `NexusOperationCancellationInfo` and the cancellation state machine in
/// `components/nexusoperations/statemachine.go:524-681 @ v1.31.0`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NexusOperationCancellation {
    /// Event ID of the `NexusOperationCancelRequested` history event.
    pub requested_event_id: i64,
    /// Time delivery first became eligible. This remains absent while cancellation
    /// waits for an operation that has not started.
    pub requested_time: Option<OffsetDateTime>,
    /// Current cancellation-delivery lifecycle state.
    pub state: NexusOperationCancellationState,
    /// Completed delivery attempts. Incremented only when an attempt reports an
    /// outcome, matching v1.31.0's `recordAttempt` ordering.
    pub attempt: u32,
    /// Completion time of the most recent delivery attempt.
    pub last_attempt_complete_time: Option<OffsetDateTime>,
    /// Failure from the most recent failed delivery attempt.
    pub last_attempt_failure: Option<Payload>,
    /// Runtime-computed deadline for retrying a retryable failure.
    pub next_attempt_at: Option<OffsetDateTime>,
}

/// Lifecycle of a Nexus cancellation request while its parent operation remains pending.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NexusOperationCancellationState {
    /// Cancellation was requested before the operation started, so no delivery is
    /// eligible yet.
    #[default]
    Unspecified,
    /// A delivery is queued or in flight.
    Scheduled,
    /// The last delivery failed retryably and awaits its runtime-supplied deadline.
    BackingOff,
    /// The handler acknowledged the cancellation request. This does not resolve the
    /// parent operation; its completion callback still owns the terminal outcome.
    Succeeded,
    /// Delivery failed non-retryably.
    Failed,
}

/// Tracks a Nexus operation that has been scheduled but not
/// yet reached a terminal state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingNexusOperation {
    /// Operation identifier.
    pub operation_id: String,
    /// Event ID of the `NexusOperationScheduled` event.
    pub scheduled_event_id: i64,
    /// Nexus endpoint name.
    pub endpoint: String,
    /// Nexus service name.
    pub service: String,
    /// Nexus operation name.
    pub operation: String,
    /// Maximum time from schedule to completion.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Caller's tolerance for the handler to start the operation; only applies
    /// while `started` is false. v1.31.0 wire field 16 on
    /// `PendingNexusOperationInfo`.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Caller's tolerance for an async operation to complete after starting;
    /// only applies once `started`. v1.31.0 wire field 17.
    pub start_to_close_timeout: Option<Duration>,
    /// When the operation was scheduled.
    pub scheduled_at: OffsetDateTime,
    /// Whether the operation has transitioned to async-started.
    pub started: bool,
    /// When the operation transitioned to started, anchoring the
    /// start-to-close deadline. `None` until `started` flips true. This is the
    /// authority the timeout scanner reads to fire start-to-close even across a
    /// shard takeover (the derived tracking index is rebuilt from this state).
    pub started_at: Option<OffsetDateTime>,
    /// Failed invocation attempts so far (0 until the first failure). Bumped by a
    /// retryable `NexusResolution::AttemptFailed`; the runtime reads it to compute
    /// the next backoff. Mirrors v1.31.0 `recordAttempt`
    /// (`components/nexusoperations/statemachine.go:91-94`). Derived backoff state,
    /// not replayed from history (no `EventAttemptFailed` event exists).
    #[serde(default)]
    pub attempt: u32,
    /// Failure of the most recent failed-but-retryable attempt — the handler's own
    /// failure (NexusHandlerFailureInfo + cause), *not* the NexusOperationFailureInfo
    /// wrapper used for the terminal `NexusOperationFailed` event. Surfaced on
    /// `DescribeWorkflowExecution.PendingNexusOperations[].LastAttemptFailure`. `None`
    /// while no attempt has failed and **cleared on re-dispatch** so Describe shows
    /// nothing while an attempt is in flight (v1.31.0 `recordAttempt`).
    #[serde(default)]
    pub last_attempt_failure: Option<Payload>,
    /// When a backing-off operation is next eligible for re-dispatch. `Some` iff the
    /// op is `BACKING_OFF`; set by the retryable-failure transition from a
    /// runtime-computed backoff (the kernel stays free of backoff math/config, like
    /// the `CompletionCallback` path). The Nexus-op retry scanner re-fires only once
    /// `now >= next_attempt_at`; the schedule-to-close timeout scanner dominates.
    #[serde(default)]
    pub next_attempt_at: Option<OffsetDateTime>,
    /// Handler-issued async operation token (from the `NexusOperationStarted`
    /// response), retained so cancel/get re-send it verbatim. v1.31.0 keeps this in
    /// the NexusOperation mutable-state data (not only on the event); tokeira must
    /// too, because the runtime cancel path works off this projection, which carries
    /// no history. Empty until the operation transitions to async-started.
    #[serde(default)]
    pub operation_token: String,
    /// The StartOperation input, retained so a backing-off operation can be
    /// re-dispatched without re-reading history: the retry transition reconstructs
    /// the `ScheduleNexusOperation` dispatch op from this projection (the runtime
    /// retry scanner works off committed state, which carries no events). Populated
    /// at schedule and rebuilt from the `NexusOperationScheduled` event on replay.
    #[serde(default)]
    pub input: Payloads,
    /// Cancellation-delivery lifecycle, present after the workflow requests
    /// cancellation and retained until the parent operation resolves.
    #[serde(default)]
    pub cancellation: Option<NexusOperationCancellation>,
}

/// Stored form of `WorkflowExecutionVersioningInfo` for one run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersioningInfo {
    /// SDK-declared behavior from the most recent completed workflow task.
    pub behavior: VersioningBehavior,
    /// Deployment version that completed the most recent workflow task.
    pub deployment_version: Option<WorkerDeploymentVersionRef>,
    /// Execution-scoped override with precedence over behavior.
    pub versioning_override: Option<VersioningOverride>,
    /// In-flight transition target while a task is moving to a new version.
    pub version_transition: Option<WorkerDeploymentVersionRef>,
    /// Monotonic routing-decision counter used as a dispatch staleness fence.
    pub revision_number: i64,
    /// Continue-as-new behavior for the first task of this run and retries.
    pub continue_as_new_initial_versioning_behavior: ContinueAsNewVersioningBehavior,
    /// Ordered values projected into the system-managed `BuildIds` search
    /// attribute. These are per-run and therefore do not leak across
    /// continue-as-new boundaries.
    #[serde(default)]
    pub build_id_search_attributes: Vec<String>,
    /// Most recent structured worker-version stamp from a completed WFT.
    #[serde(default)]
    pub most_recent_worker_version_stamp: Option<WorkerVersionStamp>,
}

impl WorkflowVersioningInfo {
    /// Whether this value carries routing-facing execution versioning data.
    ///
    /// Legacy BuildIds bookkeeping alone must not manufacture a non-empty
    /// `WorkflowExecutionInfo.versioning_info` for an unversioned run.
    pub fn has_execution_versioning_info(&self) -> bool {
        self.behavior != VersioningBehavior::Unspecified
            || self.deployment_version.is_some()
            || self.versioning_override.is_some()
            || self.version_transition.is_some()
            || self.revision_number != 0
            || self.continue_as_new_initial_versioning_behavior
                != ContinueAsNewVersioningBehavior::Unspecified
    }

    fn is_unversioned_default(&self) -> bool {
        !self.has_execution_versioning_info()
            && self.build_id_search_attributes.is_empty()
            && self.most_recent_worker_version_stamp.is_none()
    }
}

/// Worker Deployment Version reference carried by per-run versioning state.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkerDeploymentVersionRef {
    /// Worker Deployment name.
    pub deployment_name: String,
    /// Build identifier within the deployment.
    pub build_id: String,
}

/// Workflow versioning behavior stored for a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningBehavior {
    /// Unversioned legacy execution.
    #[default]
    Unspecified,
    /// Execution is pinned to its deployment version.
    Pinned,
    /// Execution automatically moves to the current target version.
    AutoUpgrade,
}

/// Continue-as-new initial versioning behavior stored for a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContinueAsNewVersioningBehavior {
    /// No explicit continue-as-new behavior was supplied.
    #[default]
    Unspecified,
    /// Start the new run with auto-upgrade behavior.
    AutoUpgrade,
    /// Start the new run using the ramping version.
    UseRampingVersion,
}

/// Execution-scoped worker versioning override configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningOverride {
    /// Pin the execution to a specific Worker Deployment Version.
    Pinned { version: WorkerDeploymentVersionRef },
    /// Force the execution into auto-upgrade behavior.
    AutoUpgrade,
}

/// SDK-authored summary/details metadata retained with the run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMetadata {
    /// Short UI-facing summary payload.
    #[serde(default)]
    pub summary: Option<Payload>,
    /// Longer UI-facing details payload.
    #[serde(default)]
    pub details: Option<Payload>,
}

/// Link metadata attached to a workflow start or signal event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Link {
    /// Reference to another workflow event.
    WorkflowEvent {
        namespace: String,
        workflow_id: String,
        run_id: String,
        reference: Option<LinkWorkflowEventReference>,
    },
    /// Reference to a batch job.
    BatchJob { job_id: String },
    /// Reference to an activity.
    Activity {
        namespace: String,
        activity_id: String,
        run_id: String,
    },
    /// Reference to a standalone Nexus operation.
    NexusOperation {
        namespace: String,
        operation_id: String,
        run_id: String,
    },
}

/// Optional discriminator for workflow-event links.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkWorkflowEventReference {
    /// Direct reference to a history event ID and type.
    Event { event_id: i64, event_type: i32 },
    /// Indirect reference through the request ID that authored an event.
    RequestId { request_id: String, event_type: i32 },
}

/// Completion callback configuration attached at workflow start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionCallback {
    /// Callback target and request headers.
    pub spec: CallbackSpec,
    /// Links associated with the callback itself.
    #[serde(default)]
    pub links: Vec<Link>,
    /// Event that triggers callback dispatch.
    #[serde(default)]
    pub trigger: CallbackTrigger,
    /// Time the callback was registered on the workflow.
    #[serde(default)]
    pub registration_time: Option<OffsetDateTime>,
    /// Durable callback lifecycle state.
    #[serde(default)]
    pub state: CallbackState,
    /// Number of dispatch attempts already made.
    #[serde(default)]
    pub attempt: u32,
    /// Last failure payload observed while dispatching this callback.
    #[serde(default)]
    pub last_attempt_failure: Option<Payload>,
    /// When a `BackingOff` callback is next eligible for a delivery retry.
    ///
    /// Set by `CompletionCallbackAttempted` on a retryable failure (the runtime
    /// computes it from the configured backoff policy and the attempt count, so the
    /// kernel stays free of backoff math and config). `None` for any non-backing-off
    /// state. The runtime's completion-callback scanner re-fires a `BackingOff`
    /// callback only once `now >= next_attempt_at`.
    #[serde(default)]
    pub next_attempt_at: Option<OffsetDateTime>,
    /// Wall-clock completion time of the latest delivery attempt.
    ///
    /// Every success or failure records this timestamp so
    /// `DescribeWorkflowExecution.CallbackInfo.last_attempt_complete_time`
    /// reflects the callback executor's latest observation
    /// (`components/callbacks/statemachine.go @ v1.31.0`). This trailing field
    /// preserves the prefix layout of callback state serialized before Tier 5.32.
    #[serde(default)]
    pub last_attempt_complete_time: Option<OffsetDateTime>,
}

/// Public callback target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackSpec {
    /// Nexus callback URL with caller-supplied headers.
    Nexus {
        url: String,
        #[serde(default)]
        header: BTreeMap<String, String>,
    },
}

/// Callback trigger condition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackTrigger {
    /// Dispatch when the workflow reaches any terminal state.
    #[default]
    WorkflowClosed,
}

/// Durable callback lifecycle state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallbackState {
    /// Callback is registered and waiting for the trigger.
    #[default]
    Standby,
    /// Callback has been queued or is currently executing.
    Scheduled,
    /// Callback failed with a retryable error and is backing off.
    BackingOff,
    /// Callback failed permanently.
    Failed,
    /// Callback completed successfully.
    Succeeded,
    /// Callback is blocked by server-side admission.
    Blocked,
}

/// Workflow priority metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Priority {
    /// Lower values are higher priority; zero means server default.
    pub priority_key: i32,
    /// Fairness key used by matching when queues are backed up.
    pub fairness_key: String,
    /// Relative fairness weight for `fairness_key`.
    pub fairness_weight: f32,
}

/// Options to apply when a start targets an existing running workflow.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnConflictOptions {
    /// Attach the start request ID to the running workflow.
    pub attach_request_id: bool,
    /// Attach supplied completion callbacks to the running workflow.
    pub attach_completion_callbacks: bool,
    /// Attach supplied links to the options-updated event.
    pub attach_links: bool,
}

/// Either the run does not yet exist or it already has durable state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LoadedRun {
    /// The run does not yet exist in durable storage. Only
    /// the `Start` command accepts this variant.
    Absent,
    /// The run exists and carries its current durable state.
    Existing(WorkflowState),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use serde::Serialize;
    use serde_json::Value;
    use tokeira_types::WorkerIdentity;

    use super::*;

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    fn version(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersionRef {
        WorkerDeploymentVersionRef {
            deployment_name: deployment_name.into(),
            build_id: build_id.into(),
        }
    }

    fn open_state() -> WorkflowState {
        WorkflowState {
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("workflow-type".into()),
            task_queue: TaskQueueName("workflow-task-queue".into()),
            deployment: None,
            build_id: None,
            versioning_info: None,
            worker_deployment_name: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 1,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo(BTreeMap::new()),
            search_attributes: SearchAttributes(BTreeMap::new()),
            workflow_execution_timeout: Some(Duration::minutes(10)),
            workflow_run_timeout: Some(Duration::minutes(5)),
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: Some(RunId::new()),
            original_execution_run_id: None,
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: now(),
            first_run_started_at: Some(now()),
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
        }
    }

    #[test]
    fn effective_versioning_precedence_prefers_transition_then_override_then_behavior() {
        let behavior_version = version("deployment", "behavior");
        let override_version = version("deployment", "override");
        let transition_version = version("deployment", "transition");
        let mut state = open_state();
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: Some(behavior_version.clone()),
            versioning_override: None,
            version_transition: None,
            revision_number: 7,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        });

        assert_eq!(state.effective_behavior(), VersioningBehavior::AutoUpgrade);
        assert_eq!(state.effective_deployment(), Some(&behavior_version));

        state.set_versioning_override(Some(VersioningOverride::Pinned {
            version: override_version.clone(),
        }));
        assert_eq!(state.effective_behavior(), VersioningBehavior::Pinned);
        assert_eq!(state.effective_deployment(), Some(&override_version));

        state
            .versioning_info
            .as_mut()
            .expect("versioning info")
            .version_transition = Some(transition_version.clone());
        assert_eq!(state.effective_behavior(), VersioningBehavior::AutoUpgrade);
        assert_eq!(state.effective_deployment(), Some(&transition_version));
    }

    #[test]
    fn auto_upgrade_override_takes_precedence_over_pinned_behavior() {
        let behavior_version = version("deployment", "behavior");
        let mut state = open_state();
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::Pinned,
            deployment_version: Some(behavior_version.clone()),
            versioning_override: Some(VersioningOverride::AutoUpgrade),
            version_transition: None,
            revision_number: 3,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        });

        assert_eq!(state.effective_behavior(), VersioningBehavior::AutoUpgrade);
        assert_eq!(state.effective_deployment(), Some(&behavior_version));
    }

    #[test]
    fn versioning_override_updates_the_described_worker_deployment_name() {
        let worker_version = version("worker-deployment", "worker-build");
        let pinned_version = version("pinned-deployment", "pinned-build");
        let mut state = open_state();
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: Some(worker_version),
            ..WorkflowVersioningInfo::default()
        });
        state.worker_deployment_name = Some("worker-deployment".to_string());

        state.set_versioning_override(Some(VersioningOverride::Pinned {
            version: pinned_version,
        }));
        assert_eq!(
            state.worker_deployment_name.as_deref(),
            Some("pinned-deployment")
        );

        state.set_versioning_override(Some(VersioningOverride::AutoUpgrade));
        assert_eq!(
            state.worker_deployment_name.as_deref(),
            Some("pinned-deployment")
        );

        state.set_versioning_override(None);
        assert_eq!(
            state.worker_deployment_name.as_deref(),
            Some("worker-deployment")
        );
    }

    #[test]
    fn pinned_workflow_rejects_start_version_transition_without_mutation() {
        let pinned_version = version("deployment", "pinned");
        let target_version = version("deployment", "target");
        let mut state = open_state();
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::Pinned,
            deployment_version: Some(pinned_version),
            versioning_override: None,
            version_transition: None,
            revision_number: 11,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        });
        let before = state.clone();

        let result = state.start_version_transition(target_version, 12);

        assert_eq!(
            result,
            Err(VersionTransitionError::PinnedWorkflowCannotTransition)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn start_version_transition_clears_sticky_and_reschedules_unstarted_wft() {
        let target_version = version("deployment", "target");
        let mut state = open_state();
        state.sticky = Some(StickyAffinity {
            worker_identity: WorkerIdentity("sticky-worker".into()),
            sticky_queue: TaskQueueName("queue-sticky".into()),
            schedule_to_start_timeout: Duration::seconds(5),
            expires_at: now() + Duration::seconds(30),
        });
        state.pending_workflow_task = Some(PendingWorkflowTask {
            task_type: WorkflowTaskType::Normal,
            schedule_to_start_deadline: None,
            logical_seq: LogicalTaskSeq(2),
            scheduled_event_id: 10,
            scheduled_at: now(),
            started_event_id: None,
            started_at: None,
            attempt: 4,
        });
        state.workflow_task_attempt = 5;

        state
            .start_version_transition(target_version.clone(), 42)
            .expect("unpinned workflow can transition");

        let info = state
            .versioning_info
            .expect("transition creates versioning info");
        assert_eq!(info.version_transition, Some(target_version));
        assert_eq!(info.revision_number, 42);
        assert_eq!(state.sticky, None);
        let pending = state.pending_workflow_task.expect("pending workflow task");
        assert_eq!(pending.started_event_id, None);
        assert_eq!(pending.started_at, None);
        assert_eq!(state.workflow_task_attempt, pending.attempt);
    }

    #[test]
    fn start_version_transition_leaves_started_wft_running() {
        let target_version = version("deployment", "target");
        let mut state = open_state();
        state.pending_workflow_task = Some(PendingWorkflowTask {
            task_type: WorkflowTaskType::Normal,
            schedule_to_start_deadline: None,
            logical_seq: LogicalTaskSeq(2),
            scheduled_event_id: 10,
            scheduled_at: now(),
            started_event_id: Some(11),
            started_at: Some(now()),
            attempt: 4,
        });
        state.workflow_task_attempt = 5;

        state
            .start_version_transition(target_version.clone(), 42)
            .expect("unpinned workflow can transition");

        let pending = state.pending_workflow_task.expect("pending workflow task");
        assert_eq!(pending.started_event_id, Some(11));
        assert_eq!(pending.started_at, Some(now()));
        assert_eq!(state.workflow_task_attempt, 5);
    }

    #[test]
    fn apply_wft_versioning_unspecified_clears_deployment_version_but_keeps_name() {
        let current_version = version("deployment", "current");
        let reported_version = version("deployment", "reported");
        let mut state = open_state();
        state.worker_deployment_name = Some("deployment".into());
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: Some(current_version),
            versioning_override: None,
            version_transition: None,
            revision_number: 6,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        });

        state.apply_wft_versioning(
            VersioningBehavior::Unspecified,
            Some(reported_version),
            Some("deployment".into()),
        );

        let info = state
            .versioning_info
            .expect("revision keeps info non-default");
        assert_eq!(info.behavior, VersioningBehavior::Unspecified);
        assert_eq!(info.deployment_version, None);
        // v1.31.0 records the reported deployment name even for unversioned
        // (UNSPECIFIED-behavior) completions.
        assert_eq!(state.worker_deployment_name, Some("deployment".into()));
    }

    #[test]
    fn apply_wft_versioning_clears_transition_only_on_matching_completion() {
        let transition_version = version("deployment", "transition");
        let other_version = version("deployment", "other");
        let mut state = open_state();
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::AutoUpgrade,
            deployment_version: None,
            versioning_override: None,
            version_transition: Some(transition_version.clone()),
            revision_number: 9,
            continue_as_new_initial_versioning_behavior:
                ContinueAsNewVersioningBehavior::Unspecified,
            ..WorkflowVersioningInfo::default()
        });

        state.apply_wft_versioning(
            VersioningBehavior::AutoUpgrade,
            Some(other_version.clone()),
            Some("deployment".into()),
        );
        let info = state.versioning_info.as_ref().expect("versioning info");
        assert_eq!(info.version_transition, Some(transition_version.clone()));
        assert_eq!(info.deployment_version, Some(other_version));
        assert_eq!(state.effective_deployment(), Some(&transition_version));

        state.apply_wft_versioning(
            VersioningBehavior::AutoUpgrade,
            Some(transition_version.clone()),
            Some("deployment".into()),
        );
        let info = state.versioning_info.expect("versioning info");
        assert_eq!(info.version_transition, None);
        assert_eq!(info.deployment_version, Some(transition_version));
        // v1.31.0 completion updates behavior/deployment and clears a matching
        // transition, but it does not assign `RevisionNumber`; that field is
        // set at transition start (`mutable_state_impl.go:9108`) or inherited
        // at start (`mutable_state_impl.go:2963`).
        assert_eq!(info.revision_number, 9);
    }

    #[test]
    fn workflow_state_deserializes_without_versioning_fields_as_unversioned() {
        let state = open_state();
        let mut value = serde_json::to_value(&state).expect("serialize state");
        let object = value.as_object_mut().expect("state serializes as object");
        object.remove("versioning_info");
        object.remove("worker_deployment_name");

        let migrated: WorkflowState = serde_json::from_value(Value::Object(object.clone()))
            .expect("missing versioning fields default");

        assert_eq!(migrated.versioning_info, None);
        assert_eq!(migrated.worker_deployment_name, None);
        assert_eq!(
            migrated.effective_behavior(),
            VersioningBehavior::Unspecified
        );
        assert_eq!(migrated.effective_deployment(), None);
    }

    #[test]
    fn legacy_buffered_event_postcard_documents_pre_baseline_decode_failure() {
        // Feature: authorization-foundation, Property 5: genuine bytes from
        // the pre-attribution trailing-field shape fail positional decoding.
        // Tokeira is pre-baseline, so this pins why a fresh database is
        // required rather than claiming a migration guarantee that postcard
        // does not provide (Requirement 7.7).
        #[derive(Serialize)]
        struct LegacyBufferedEvent {
            admitted_at: OffsetDateTime,
            kind: crate::event::HistoryEventKind,
        }

        let legacy = LegacyBufferedEvent {
            admitted_at: now(),
            kind: crate::event::HistoryEventKind::WorkflowExecutionSignaled {
                signal_name: "legacy".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request_id: "legacy-request".into(),
                identity: None,
            },
        };
        let bytes = postcard::to_allocvec(&legacy).expect("encode old buffered-event shape");
        let error = postcard::from_bytes::<BufferedEvent>(&bytes)
            .expect_err("postcard cannot default a missing trailing sequence element");

        assert!(matches!(error, postcard::Error::DeserializeUnexpectedEnd));
    }
}
